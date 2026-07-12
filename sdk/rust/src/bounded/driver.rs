//! Sequential bounded driver.
//!
//! One driver runs per started bounded instance on a **dedicated thread** (not
//! a work-pool worker): a `run`-sugar body may block on its inbox during an
//! interactive loop, and the structured path joins pool segment sub-tasks —
//! running the driver on the shared pool would tie up a worker indefinitely and
//! a one-worker pool would self-deadlock (the driver blocks on tasks only that
//! same worker could run). The driver:
//!
//! - reads the [`BoundedInstance`](super::instance::BoundedInstance) that
//!   `start_bounded_resumable` registered on the shared `OperativeRegistry`
//!   before the driver started — its cancel token + inbox (so `BoundedStop` /
//!   `RemOperative` / `max_duration` cancel it and the wire-dispatch path
//!   delivers post-start input) and, for a structured durable class, the
//!   crash-resume gate held until the Controller's Segment restores land;
//! - runs the body — `run`-sugar ([`run_sugar`]) or the structured linear chain
//!   ([`run_structured`], operations sequential, segments parallel within an
//!   operation, count-based barrier between them);
//! - converges every exit trigger on the single `BoundedCompleted` teardown,
//!   emitting status (`ok | error | cancelled`) plus the result payload.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam::channel::{unbounded, Receiver};

use super::compensation::{plan_compensation, SegmentRun, StageRecord, StagedOutput};
use super::instance::BoundedInstance;
use super::spec::BoundedSpec;
use super::{BoundedCollectSink, BoundedCtx, BoundedError, BoundedRunFn, RollbackMode};
use crate::channel::builder::BuildSegmentArgs;
use crate::channel::{ChannelKind, OperationId};
use crate::executor::work_stealing_pool::WorkStealingPool;
use crate::operative::topology::{OperationDescriptor, OperativeTopology, SegmentingMode};
use crate::segment::flusher::SegmentFlusher;
use crate::segment::segment_state_sink::{
    COMPENSATION_DONE_PAYLOAD_ID, COMPENSATION_INTENT_PAYLOAD_ID,
};
use crate::segment::SegmentSlot;
use crate::util::LockRecover;

/// Type-erased value moving between bounded stages.
type Value = Arc<dyn Any + Send + Sync>;

/// Upper bound on the wait for the controller's `OpCommitResponse` before the
/// run terminates abnormally — the same posture as any mid-run channel loss
/// (the un-acked op re-runs on resume, at-least-once).
const OP_COMMIT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
/// Re-check cadence during the ack wait so a cooperative cancel is observed
/// promptly (mirrors the restore gate's `RESUME_WAIT_CHUNK`).
const OP_COMMIT_ACK_CHUNK: Duration = Duration::from_millis(200);

/// One stage's parallel-join result: the inter-operation `batch` released to the
/// next stage, plus the per-segment record (which segments ran, with their
/// collected outputs) retained for a possible saga rollback.
struct StageOut {
    batch: Vec<Value>,
    segments: Vec<SegmentRun>,
}

/// Read-only context threaded through one structured chain run: the topology,
/// the resolved chain order, the running instance (its id + saga crash-resume
/// markers), the work pool, the pre-built per-stage segments, the rollback mode,
/// and whether the class is durable (gates the saga compensation records).
/// Bundled so the chain driver and the compensator take it by reference instead
/// of a long parameter list.
struct ChainCtx<'a> {
    topology: &'a Arc<OperativeTopology>,
    chain: &'a [OperationId],
    instance: &'a BoundedInstance,
    instance_id: &'a Arc<str>,
    pool: &'a Arc<WorkStealingPool>,
    stage_segments: &'a [Vec<Arc<SegmentSlot>>],
    rollback_mode: RollbackMode,
    persistence_enabled: bool,
}

impl ChainCtx<'_> {
    /// Whether this run journals its op boundaries — the single gate on both
    /// the `OpCommit` handshake and output staging. An opted-out class or a
    /// transport-less run (unit test / standalone bench, no coordinator) has no
    /// journal to gate a flush on, so its plugin output must keep going
    /// straight to the wire rather than being staged into an outbox nobody
    /// would ever flush.
    fn journaling(&self) -> bool {
        self.persistence_enabled && crate::executor::coordinator_installed()
    }
}

/// Spawn the bounded driver for one already-registered instance on a dedicated
/// thread. Fire-and-forget: the thread exits at terminal teardown. The caller
/// ([`ExecutorNode::start_bounded_resumable`](crate::executor::ExecutorNode))
/// builds + registers the [`BoundedInstance`] **synchronously**, before the
/// `AddOperative` frame is ACKed, so an inbox frame / `BoundedStop` / restore
/// arriving right after is never lost to a startup race.
pub(crate) fn spawn(
    spec: Arc<BoundedSpec>,
    instance: Arc<BoundedInstance>,
    inbox_rx: Receiver<Vec<u8>>,
    args: String,
    pool: Arc<WorkStealingPool>,
) {
    let name = format!("flezha-bounded-{}", instance.instance_id);
    let thread_instance = Arc::clone(&instance);
    let spawned = thread::Builder::new()
        .name(name)
        .spawn(move || run(spec, thread_instance, inbox_rx, args, pool));

    if let Err(e) = spawned {
        // Spawn failed: deregister this run's flusher entries and report a
        // terminal failure so the controller does not wait forever. The registry
        // entry is dropped by the controller's stop_operative on the completion.
        deregister_flushers(&instance.stage_segments);
        crate::executor::emit_bounded_completed(
            instance.instance_id.to_string(),
            "error".to_string(),
            format!("failed to spawn bounded driver thread: {e}"),
            Vec::new(),
        );
    }
}

fn run(
    spec: Arc<BoundedSpec>,
    instance: Arc<BoundedInstance>,
    inbox_rx: Receiver<Vec<u8>>,
    args: String,
    pool: Arc<WorkStealingPool>,
) {
    let finished = Arc::new(AtomicBool::new(false));

    // max_duration backstop: trip `cancel` once it elapses, unless the body
    // finished first.
    let watcher = spec
        .max_duration
        .map(|d| spawn_duration_watcher(d, Arc::clone(&instance.cancel), Arc::clone(&finished)));

    let (status, error, result) = match spec.topology.as_ref() {
        // Structured: hold on the crash-resume gate until the Controller's
        // Segment restores land, then drive the chain over the restored state.
        Some(_) => {
            instance.wait_for_restore();
            run_structured(&spec, &instance, &args, &pool)
        }
        // Run-sugar: no Segments, no restore — a crash re-runs from the top.
        None => run_sugar(
            spec.run,
            &instance.instance_id,
            &args,
            &instance.cancel,
            inbox_rx,
        ),
    };

    finished.store(true, Ordering::Release);
    if let Some(w) = watcher {
        let _ = w.join();
    }
    // Deregister this run's per-Segment flusher entries. The registry entry
    // itself stays until the controller's stop_operative teardown, so a
    // duplicate AddOperative before then is idempotent (no re-run).
    deregister_flushers(&instance.stage_segments);

    crate::executor::emit_bounded_completed(
        instance.instance_id.to_string(),
        status,
        error,
        result,
    );
}

/// Run-sugar path: a single implicit Operation/Segment whose body is `run`.
fn run_sugar(
    run: BoundedRunFn,
    instance_id: &Arc<str>,
    args: &str,
    cancel: &Arc<AtomicBool>,
    inbox_rx: Receiver<Vec<u8>>,
) -> (String, String, Vec<u8>) {
    let mut ctx = BoundedCtx::new(Arc::clone(instance_id), Arc::clone(cancel), inbox_rx);
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run(&mut ctx, args)));
    let (_completed, result) = ctx.into_result();
    classify(outcome, cancel, result)
}

/// Map a body outcome + cancellation state to the terminal completion tuple.
/// Precedence: a real failure (`Err`/panic) → `error`; otherwise a tripped
/// cancellation token → `cancelled`; otherwise `ok`.
fn classify(
    outcome: std::thread::Result<Result<(), BoundedError>>,
    cancel: &AtomicBool,
    result: Vec<u8>,
) -> (String, String, Vec<u8>) {
    match outcome {
        Ok(Ok(())) => {
            if cancel.load(Ordering::Acquire) {
                ("cancelled".to_string(), String::new(), result)
            } else {
                ("ok".to_string(), String::new(), result)
            }
        }
        Ok(Err(e)) => ("error".to_string(), e.to_string(), result),
        Err(panic) => (
            "error".to_string(),
            crate::operation::error::format_panic(panic),
            result,
        ),
    }
}

/// Structured path: run the linear chain sequentially. Operation `N+1` starts
/// only when every segment of operation `N` has finished (count-based barrier);
/// segments within an operation run in parallel on the pool.
fn run_structured(
    spec: &BoundedSpec,
    instance: &BoundedInstance,
    args: &str,
    pool: &Arc<WorkStealingPool>,
) -> (String, String, Vec<u8>) {
    let topology = spec
        .topology
        .as_ref()
        .expect("run_structured invoked only for a structured spec");

    // Segments were built + registered up front by `start_bounded_resumable`
    // (so restore could reach them and the resume gate could hold); a durable
    // class has them already restored by the Controller's `RestoreInitState`
    // frames. The run's flusher entries are deregistered in `run`'s teardown.
    let ctx = ChainCtx {
        topology,
        chain: &spec.chain,
        instance,
        instance_id: &instance.instance_id,
        pool,
        stage_segments: &instance.stage_segments,
        rollback_mode: spec.rollback_mode,
        persistence_enabled: spec.persistence_enabled,
    };
    drive_chain(&ctx, args, &instance.cancel)
}

/// Build every stage's Segments for a structured spec, in chain order. Called
/// synchronously by `start_bounded_resumable` so the Segments are registered
/// for restore and the resume gate is armed before the driver thread — or any
/// restore frame — runs. `instance` on each slot is a null `Weak` — the bounded
/// collecting path never routes through an `UnboundedInstance`. When the class
/// is durable, each Segment's applied Changes are flushed to the WAL as they run.
pub(crate) fn build_all_stage_segments(
    spec: &BoundedSpec,
    instance_id: &Arc<str>,
    pool: &Arc<WorkStealingPool>,
) -> Vec<Vec<Arc<SegmentSlot>>> {
    let topology = spec
        .topology
        .as_ref()
        .expect("build_all_stage_segments invoked only for a structured spec");
    let mut stage_segments: Vec<Vec<Arc<SegmentSlot>>> = Vec::with_capacity(spec.chain.len());
    for &op_id in &spec.chain {
        let op = topology
            .operation(op_id)
            .expect("chain operation present in topology");
        stage_segments.push(build_stage_segments(
            topology,
            instance_id,
            op_id,
            op,
            pool,
            spec.persistence_enabled,
        ));
    }
    stage_segments
}

/// Deregister every per-Segment flusher entry created for a run so the
/// process-global flusher does not accumulate dead entries across jobs.
fn deregister_flushers(stage_segments: &[Vec<Arc<SegmentSlot>>]) {
    for slots in stage_segments {
        for slot in slots {
            if let Some(id) = slot.flusher_entry_id {
                SegmentFlusher::instance().deregister(id);
            }
        }
    }
}

fn drive_chain(ctx: &ChainCtx, args: &str, cancel: &AtomicBool) -> (String, String, Vec<u8>) {
    // Crash-resume into an in-flight compensation (saga, §4.3): a non-empty
    // reconstruction means the Controller shipped back compensation-intent
    // markers. Without an op-journal that always means the operative had
    // terminated abnormally with (some of) its undo outstanding. WITH a
    // journal, intent markers also accumulate during normal forward progress,
    // so only a shipped compensation-done marker — written exclusively by an
    // undo pass — still proves compensation was in flight; otherwise the
    // journal decides where forward execution resumes (bounded-op-journal).
    let resumed = reconstruct_completed_from_intents(ctx);
    let journal = ctx.instance.op_journal_resume();
    if !resumed.is_empty() && (journal.is_none() || ctx.instance.any_compensation_done()) {
        run_compensation(ctx, &resumed);
        return (
            "error".to_string(),
            "resumed compensation after crash".to_string(),
            Vec::new(),
        );
    }

    let topology = ctx.topology;

    // Completed stages of the current (and only, in v1) pass, recorded in
    // completion order so an abnormal exit can roll them back in reverse. A
    // journal-gated resume pre-seeds it with the intent reconstruction so the
    // skipped (already-committed) operations still roll back if a later
    // stage terminates the run abnormally.
    let mut completed: Vec<StageRecord> = Vec::new();

    // Journal-gated resume: skip every operation recorded `op_committed` and
    // seed the first uncommitted one from the journaled handoff via the
    // channel codec. No journal ⇒ seed the first operation from the spawn
    // args via the entry channel's typed wrapper (re-run from the top).
    let (first_stage, mut batch): (usize, Vec<Value>) = match journal
        .as_ref()
        .filter(|resume| !resume.committed_op_idxs.is_empty())
    {
        Some(resume) => {
            let last = *resume
                .committed_op_idxs
                .iter()
                .max()
                .expect("non-empty committed set") as usize;
            match seed_from_journal(ctx, resume) {
                Ok(seeded) => {
                    completed = resumed;
                    (last + 1, seeded)
                }
                Err(e) => {
                    // The journaled handoff cannot be re-seeded — terminate
                    // abnormally and undo the committed operations.
                    run_compensation(ctx, &resumed);
                    return ("error".to_string(), e, Vec::new());
                }
            }
        }
        None => match seed_from_entry(topology, args) {
            Ok(seeded) => (0, seeded),
            Err(e) => return ("error".to_string(), e, Vec::new()),
        },
    };

    // `Some` once the chain terminates abnormally (error / mid-chain stop),
    // carrying the terminal status + error text.
    let mut abnormal: Option<(String, String)> = None;

    for (stage_idx, &op_id) in ctx.chain.iter().enumerate().skip(first_stage) {
        // A cooperative stop before a stage starts is partial progress: stop
        // advancing and roll back what completed.
        if cancel.load(Ordering::Acquire) {
            abnormal = Some(("cancelled".to_string(), String::new()));
            break;
        }
        let op = topology
            .operation(op_id)
            .expect("chain operation present in topology");
        let slots = &ctx.stage_segments[stage_idx];
        let partitioned = partition(op, slots.len(), &batch);
        match run_stage_parallel(
            topology,
            ctx.instance_id,
            slots,
            partitioned,
            ctx.pool,
            ctx.journaling(),
        ) {
            Ok(StageOut {
                batch: out_batch,
                segments,
            }) => {
                batch = out_batch;
                // Durable saga (§4.3): record this operation's completion — one
                // compensation-intent marker per segment that ran — so a crash
                // mid-compensation knows what still needs undo. Only for
                // compensable operations (a hookless op is never rolled back).
                if op.compensate_overridden {
                    for seg_run in &segments {
                        emit_compensation_record(
                            COMPENSATION_INTENT_PAYLOAD_ID,
                            &slots[seg_run.segment_index].segment_identity,
                            ctx.persistence_enabled,
                        );
                    }
                }
                // Op-boundary commit (bounded-op-journal): journal the stage's
                // completion + handoff on the controller and block until the
                // durable ack — the next operation must not race ahead of the
                // committed record. Ordered AFTER the intent markers so a
                // crash between marker and ack resolves as "op not committed
                // → re-run" (at-least-once).
                let commit = commit_op_boundary(ctx, stage_idx, op, &segments, slots, &batch);
                completed.push(StageRecord {
                    stage_idx,
                    segments,
                });
                if let Err(e) = commit {
                    let status = if cancel.load(Ordering::Acquire) {
                        ("cancelled".to_string(), String::new())
                    } else {
                        ("error".to_string(), e)
                    };
                    abnormal = Some(status);
                    break;
                }
            }
            Err(e) => {
                abnormal = Some(("error".to_string(), e.to_string()));
                break;
            }
        }
    }

    match abnormal {
        // Clean run to quiescence: every stage completed, so there is nothing
        // partial to undo — a clean `ok` never compensates.
        None => ("ok".to_string(), String::new(), Vec::new()),
        // Abnormal exit after partial progress: roll back the completed stages
        // in the rollback mode's order, then report the terminal status.
        Some((status, error)) => {
            run_compensation(ctx, &completed);
            (status, error, Vec::new())
        }
    }
}

/// Seed the first operation from the spawn args via the entry channel's
/// typed wrapper (a fresh run, or a resume with nothing journaled).
fn seed_from_entry(topology: &OperativeTopology, args: &str) -> Result<Vec<Value>, String> {
    let entry = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::Entry)
        .expect("structured bounded has an entry channel (validated at registration)");
    let wrap = entry
        .wrap_input
        .as_ref()
        .expect("entry channel carries a wrap_input");
    wrap(args.as_bytes().to_vec())
        .map(|v| vec![v])
        .map_err(|e| format!("seeding entry channel failed: {e}"))
}

/// Decode the journaled handoff batch back into typed values via each item's
/// producing-channel codec (`bounded-op-journal` crash-resume seeding).
fn seed_from_journal(
    ctx: &ChainCtx,
    resume: &super::instance::JournalResume,
) -> Result<Vec<Value>, String> {
    let mut seeded: Vec<Value> = Vec::with_capacity(resume.handoff.len());
    for (channel_id, bytes) in &resume.handoff {
        let codec = ctx
            .topology
            .channel(*channel_id)
            .and_then(|c| c.codec.as_ref())
            .ok_or_else(|| {
                format!(
                    "journaled handoff references channel id {channel_id} without an inter-op \
                     codec; cannot re-seed the resumed operation"
                )
            })?;
        let value = (codec.decode)(bytes.clone())
            .map_err(|e| format!("journaled handoff decode failed on channel {channel_id}: {e}"))?;
        seeded.push(value);
    }
    Ok(seeded)
}

/// Ship `OpCommit` for a completed stage and block until the controller's
/// durable `OpCommitResponse` (bounded-op-journal D3). The handoff batch is
/// serialized through the operation's output-channel codec (validated present
/// at registration; a terminal or plugin-bound output has no codec and ships
/// an empty handoff), and the stage's staged plugin output rides along as the
/// operation's outbox (`bounded-transactional-outbox`) — journaled with the
/// commit in one durable write, flushed by the controller only afterwards.
/// Skipped for opted-out classes and transport-less runs (unit tests /
/// standalone, where no coordinator is installed — mirroring the DLQ park's
/// sink gate); those never stage, so nothing is stranded. A failed or timed-out
/// ack is a terminal run error; the un-acked op re-runs on resume, and its
/// staged output — never journaled — is simply dropped (at-least-once).
fn commit_op_boundary(
    ctx: &ChainCtx,
    stage_idx: usize,
    op: &OperationDescriptor,
    segments: &[SegmentRun],
    slots: &[Arc<SegmentSlot>],
    batch: &[Value],
) -> Result<(), String> {
    if !ctx.journaling() {
        return Ok(());
    }

    let mut handoff: Vec<crate::pb::OpHandoffItem> = Vec::new();
    if let Some(channel_id) = op.output_channel_id {
        if let Some(codec) = ctx
            .topology
            .channel(channel_id)
            .and_then(|c| c.codec.as_ref())
        {
            for value in batch {
                let payload = (codec.encode)(value.as_ref()).map_err(|e| {
                    format!(
                        "op {stage_idx} handoff serialization failed on channel {channel_id}: {e}"
                    )
                })?;
                handoff.push(crate::pb::OpHandoffItem {
                    channel_id,
                    payload,
                });
            }
        }
    }

    let segments_ran: Vec<Vec<u8>> = segments
        .iter()
        .map(|seg_run| {
            slots[seg_run.segment_index]
                .segment_identity
                .as_ref()
                .clone()
        })
        .collect();

    ship_op_commit(
        ctx,
        stage_idx,
        crate::pb::OpCommit {
            operative_id: ctx.instance_id.to_string(),
            op_idx: stage_idx as u32,
            iter: 0,
            segments_ran,
            handoff,
            outbox: segments
                .iter()
                .flat_map(|seg_run| outbox_items(&slots[seg_run.segment_index], &seg_run.staged))
                .collect(),
            compensating: false,
        },
    )
}

/// One segment's staged plugin output as outbox entries, tagged with the
/// producing segment's wire identity and the segment-local emit order
/// (`seq_in_op`) — the controller mints the flush key from that tuple.
fn outbox_items(slot: &Arc<SegmentSlot>, staged: &[StagedOutput]) -> Vec<crate::pb::OpOutboxItem> {
    staged
        .iter()
        .enumerate()
        .map(|(seq_in_op, (channel, payload))| crate::pb::OpOutboxItem {
            segment_id: slot.segment_identity.as_ref().clone(),
            seq_in_op: seq_in_op as u32,
            channel: channel.clone(),
            payload: payload.clone(),
        })
        .collect()
}

/// Emit one `OpCommit` and block until the controller's durable ack. Shared by
/// the forward op boundary and the compensation undo record — both are "journal
/// this, then tell me it is durable" round-trips over the same handshake.
///
/// A forward commit aborts early on a cooperative cancel (the caller maps that
/// to the `cancelled` terminal status and rolls back). A compensation commit
/// does not: rollback IS the response to that cancel, and abandoning the undo
/// record because the token is set would drop the undo output it carries. It
/// waits out the full ack deadline instead.
fn ship_op_commit(
    ctx: &ChainCtx,
    stage_idx: usize,
    commit: crate::pb::OpCommit,
) -> Result<(), String> {
    let compensating = commit.compensating;
    let what = if compensating {
        "compensation"
    } else {
        "commit"
    };
    let reference_id = format!(
        "op-commit-{}-{}-{}",
        ctx.instance_id,
        stage_idx,
        crate::util::nonce_suffix()
    );
    let rx = crate::shutdown::register_pending_ack(reference_id.clone());
    crate::executor::emit_op_commit(reference_id.clone(), commit);

    // Chunked wait so a cooperative cancel is observed promptly; the caller
    // maps a cancel-tripped failure to the `cancelled` terminal status.
    let deadline = Instant::now() + OP_COMMIT_ACK_TIMEOUT;
    loop {
        match rx.recv_timeout(OP_COMMIT_ACK_CHUNK) {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => {
                return Err(format!("op {stage_idx} {what} refused by controller: {e}"));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if !compensating && ctx.instance.cancel.load(Ordering::Acquire) {
                    crate::shutdown::drop_pending_ack(&reference_id);
                    return Err(format!("op {stage_idx} {what} interrupted by stop"));
                }
                if Instant::now() >= deadline {
                    crate::shutdown::drop_pending_ack(&reference_id);
                    return Err(format!(
                        "op {stage_idx} {what} ack timed out after {}s",
                        OP_COMMIT_ACK_TIMEOUT.as_secs()
                    ));
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                crate::shutdown::drop_pending_ack(&reference_id);
                return Err(format!("op {stage_idx} {what} waiter dropped"));
            }
        }
    }
}

/// Roll back the completed stages of an abnormally-terminated chain. Completed
/// operations are compensated in reverse order (per [`plan_compensation`]) and,
/// within an operation, once per segment that ran. Operations without a
/// `compensate` hook are skipped. Best-effort: a failing hook is logged and the
/// remaining steps still run, so one broken undo cannot strand the rest.
fn run_compensation(ctx: &ChainCtx, completed: &[StageRecord]) {
    // v1 runs a single pass, so there are no committed iterations to keep; the
    // structured-loop follow-up threads them in and `plan_compensation` already
    // honours the mode difference.
    let plan = plan_compensation(ctx.rollback_mode, &[], completed);
    if plan.is_empty() {
        return;
    }
    for record in plan {
        let op_id = ctx.chain[record.stage_idx];
        let op = ctx
            .topology
            .operation(op_id)
            .expect("chain operation present in topology");
        // Hookless operations take no part in rollback.
        if !op.compensate_overridden {
            continue;
        }
        let slots = &ctx.stage_segments[record.stage_idx];
        for seg_run in &record.segments {
            let slot = &slots[seg_run.segment_index];
            // Crash-resume: a segment whose compensation-done marker was shipped
            // back already ran its undo — skip it (idempotent by author
            // contract, but re-running wastes work / double-emits records).
            if ctx.instance.has_compensation_done(&slot.segment_identity) {
                continue;
            }
            let outputs: Vec<Value> = seg_run.outputs.iter().map(Arc::clone).collect();
            // One sink per compensated segment: the undo output it emits is
            // staged under that segment's identity for the `compensated`
            // journal entry (bounded-transactional-outbox D5).
            let mut sink = BoundedSegmentSink::new(ctx.instance_id, ctx.journaling());
            {
                let mut state = slot.state.lock_recover();
                if let Err(e) = slot.dispatcher.compensate_collecting(
                    outputs,
                    &mut state,
                    ctx.topology,
                    &mut sink,
                ) {
                    eprintln!(
                        "[flezha] bounded {} compensation error: {e}",
                        ctx.instance_id
                    );
                }
            }
            // Journal this segment's undo output BEFORE the done marker: the
            // marker is what stops the hook re-running on resume, so a crash
            // between the two must fall on the side that re-runs the (idempotent)
            // hook rather than the side that loses its undo output. A hook that
            // emitted nothing needs no entry.
            if !sink.staged.is_empty() {
                let commit = crate::pb::OpCommit {
                    operative_id: ctx.instance_id.to_string(),
                    op_idx: record.stage_idx as u32,
                    iter: 0,
                    segments_ran: vec![slot.segment_identity.as_ref().clone()],
                    handoff: Vec::new(),
                    outbox: outbox_items(slot, &sink.staged),
                    compensating: true,
                };
                if let Err(e) = ship_op_commit(ctx, record.stage_idx, commit) {
                    eprintln!(
                        "[flezha] bounded {} undo-outbox journaling failed: {e}",
                        ctx.instance_id
                    );
                }
            }
            // Durable saga (§4.3): record this compensation's completion so a
            // later crash resumes the remainder and skips this segment.
            emit_compensation_record(
                COMPENSATION_DONE_PAYLOAD_ID,
                &slot.segment_identity,
                ctx.persistence_enabled,
            );
        }
    }
}

/// Rebuild the completed-stage records to compensate on a crash-resume from the
/// shipped-back compensation-intent markers: a Segment slot whose identity was
/// marked "intent" completed forward and needs undo. Outputs are empty — an
/// operation's collected inter-operation output was never durable, so a durable
/// compensable operation must persist its undo handle in State (which IS
/// restored), per the at-least-once author contract. Stages are gathered in
/// chain order; [`plan_compensation`] reverses them for the undo pass.
fn reconstruct_completed_from_intents(ctx: &ChainCtx) -> Vec<StageRecord> {
    let mut completed: Vec<StageRecord> = Vec::new();
    for (stage_idx, slots) in ctx.stage_segments.iter().enumerate() {
        let segments: Vec<SegmentRun> = slots
            .iter()
            .enumerate()
            .filter(|(_, slot)| ctx.instance.has_compensation_intent(&slot.segment_identity))
            .map(|(segment_index, _)| SegmentRun {
                segment_index,
                outputs: Vec::new(),
                staged: Vec::new(),
            })
            .collect();
        if !segments.is_empty() {
            completed.push(StageRecord {
                stage_idx,
                segments,
            });
        }
    }
    completed
}

/// Ship one saga compensation marker for a Segment through the reliable sink —
/// a no-op for an opted-out (non-durable) class, which never crash-resumes.
fn emit_compensation_record(payload_id: &str, segment_identity: &[u8], persistence_enabled: bool) {
    if persistence_enabled {
        crate::segment::segment_state_sink::send_compensation_record(payload_id, segment_identity);
    }
}

/// Build the SegmentSlots for one stage: a single slot in Single mode, one per
/// declared key in Static mode. Reuses the unbounded `build_segment` closure
/// so per-Segment state + identity are identical to an unbounded operative.
fn build_stage_segments(
    topology: &Arc<OperativeTopology>,
    instance_id: &Arc<str>,
    op_id: OperationId,
    op: &OperationDescriptor,
    pool: &Arc<WorkStealingPool>,
    persistence_enabled: bool,
) -> Vec<Arc<SegmentSlot>> {
    let build = |operation_key: Option<&str>, segment_index: usize| {
        (op.build_segment)(BuildSegmentArgs {
            topology: Arc::clone(topology),
            instance: Weak::new(),
            instance_id: Arc::clone(instance_id),
            operation_id: op_id,
            operation_key,
            segment_index,
            persistence_enabled,
            pool: Arc::clone(pool),
        })
    };
    match op.mode {
        SegmentingMode::Single => vec![build(None, 0)],
        SegmentingMode::Static => op
            .segment_keys
            .iter()
            .enumerate()
            .map(|(idx, key)| build(Some(key), idx))
            .collect(),
        // Rejected at registration; defensively yield no segments.
        SegmentingMode::Dynamic => Vec::new(),
    }
}

/// Partition an input batch across a stage's segments. Single mode routes every
/// item to segment 0; Static mode routes by the operation's `segment_for` keys.
fn partition(op: &OperationDescriptor, segment_count: usize, batch: &[Value]) -> Vec<Vec<Value>> {
    let mut buckets: Vec<Vec<Value>> = vec![Vec::new(); segment_count];
    match op.mode {
        SegmentingMode::Single => {
            if let Some(bucket) = buckets.first_mut() {
                bucket.extend(batch.iter().map(Arc::clone));
            }
        }
        SegmentingMode::Static => {
            let key_to_idx: HashMap<&str, usize> = op
                .segment_keys
                .iter()
                .enumerate()
                .map(|(i, k)| (*k, i))
                .collect();
            for item in batch {
                for key in (op.segment_for_erased)(item.as_ref()) {
                    if let Some(&idx) = key_to_idx.get(key) {
                        buckets[idx].push(Arc::clone(item));
                    }
                }
            }
        }
        SegmentingMode::Dynamic => {}
    }
    buckets
}

/// Run a stage's segments in parallel on the pool and join on a count-based
/// barrier: every dispatched item is processed across all segments, with no
/// segment mid-run, before the collected output is released to the next stage.
fn run_stage_parallel(
    topology: &Arc<OperativeTopology>,
    instance_id: &Arc<str>,
    slots: &[Arc<SegmentSlot>],
    partitioned: Vec<Vec<Value>>,
    pool: &Arc<WorkStealingPool>,
    staging: bool,
) -> Result<StageOut, BoundedError> {
    type SegmentOut = (Vec<Value>, Vec<StagedOutput>);
    let (tx, rx) = unbounded::<(usize, Result<SegmentOut, BoundedError>)>();
    let mut spawned = 0usize;
    for (seg_idx, (slot, items)) in slots.iter().zip(partitioned.into_iter()).enumerate() {
        if items.is_empty() {
            continue;
        }
        let slot = Arc::clone(slot);
        let topology = Arc::clone(topology);
        let instance_id = Arc::clone(instance_id);
        let tx = tx.clone();
        spawned += 1;
        pool.submit(Box::new(move || {
            let _ = tx.send((
                seg_idx,
                run_segment(&topology, &instance_id, &slot, items, staging),
            ));
        }));
    }
    drop(tx);

    // Wait for every segment (the barrier) before returning, even on failure,
    // so no segment is mid-run when the stage completes. Each segment's outputs
    // are kept under its index for both the next-stage batch and saga rollback.
    // A failing stage returns `Err` here, dropping every segment's staged
    // plugin output with it — a failed operation leaks nothing.
    let mut segments: Vec<SegmentRun> = Vec::new();
    let mut first_err: Option<BoundedError> = None;
    for _ in 0..spawned {
        match rx.recv() {
            Ok((seg_idx, Ok((outputs, staged)))) => segments.push(SegmentRun {
                segment_index: seg_idx,
                outputs,
                staged,
            }),
            Ok((_, Err(e))) => {
                first_err.get_or_insert(e);
            }
            Err(_) => {
                first_err.get_or_insert_with(|| BoundedError::from("bounded segment task dropped"));
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    // Deterministic next-stage batch: segments in index order (each segment's
    // own item order is preserved by `run_segment`).
    segments.sort_by_key(|s| s.segment_index);
    let batch: Vec<Value> = segments
        .iter()
        .flat_map(|s| s.outputs.iter().map(Arc::clone))
        .collect();
    Ok(StageOut { batch, segments })
}

/// Process one segment's items sequentially (per-segment ordering preserved),
/// running `Operation::run` via the monomorphized dispatcher's collecting path
/// and gathering the inter-operation output for the next stage plus the
/// plugin-bound output staged for the operation's outbox.
fn run_segment(
    topology: &OperativeTopology,
    instance_id: &Arc<str>,
    slot: &Arc<SegmentSlot>,
    items: Vec<Value>,
    staging: bool,
) -> Result<(Vec<Value>, Vec<StagedOutput>), BoundedError> {
    let mut sink = BoundedSegmentSink::new(instance_id, staging);
    let mut state = slot.state.lock_recover();
    for item in items {
        slot.dispatcher
            .run_collecting(item, &mut state, topology, &mut sink)?;
    }
    Ok((sink.collected, sink.staged))
}

/// Per-segment collecting sink for the structured driver. Inter-operation
/// output is buffered for the next stage. Plugin-bound output is **staged** for
/// the operation's transactional outbox when the run commits its op boundaries
/// (`staging`), so nothing reaches a plugin before the op's journal entry is
/// durable; otherwise — an opted-out class or a transport-less run, neither of
/// which journals — it is forwarded straight to the wire as before.
struct BoundedSegmentSink {
    instance_id: Arc<str>,
    collected: Vec<Value>,
    staged: Vec<StagedOutput>,
    staging: bool,
}

impl BoundedSegmentSink {
    fn new(instance_id: &Arc<str>, staging: bool) -> Self {
        Self {
            instance_id: Arc::clone(instance_id),
            collected: Vec::new(),
            staged: Vec::new(),
            staging,
        }
    }
}

impl BoundedCollectSink for BoundedSegmentSink {
    fn collect(&mut self, value: Value) {
        self.collected.push(value);
    }

    fn emit_plugin_bytes(&mut self, channel: &str, bytes: Vec<u8>) {
        if self.staging {
            self.staged.push((channel.to_string(), bytes));
        } else {
            crate::executor::emit_bounded_output(self.instance_id.to_string(), bytes);
        }
    }
}

/// Watcher thread for `max_duration`: trips `cancel` once the duration elapses,
/// unless the body finished first. Wakes every ≤200 ms to honour an early
/// finish promptly.
fn spawn_duration_watcher(
    duration: Duration,
    cancel: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("flezha-bounded-timer".to_string())
        .spawn(move || {
            let chunk = Duration::from_millis(200);
            let mut slept = Duration::ZERO;
            while slept < duration {
                if finished.load(Ordering::Acquire) {
                    return;
                }
                let nap = chunk.min(duration - slept);
                thread::sleep(nap);
                slept += nap;
            }
            if !finished.load(Ordering::Acquire) {
                cancel.store(true, Ordering::Release);
            }
        })
        .expect("spawning flezha-bounded-timer thread should not fail")
}
