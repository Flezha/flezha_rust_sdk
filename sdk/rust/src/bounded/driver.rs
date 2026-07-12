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
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crossbeam::channel::{unbounded, Receiver, RecvTimeoutError};

use super::compensation::{plan_compensation, SegmentRun, StageRecord, StagedOutput};
use super::instance::{BoundedInstance, InboxItem, JournalResume};
use super::spec::BoundedSpec;
use super::{BoundedCollectSink, BoundedCtx, BoundedError, BoundedRunFn, LoopMode, RollbackMode};
use crate::channel::builder::BuildSegmentArgs;
use crate::channel::{ChannelKind, OperationId, RetryPolicy};
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

/// Upper bound on the wait for the controller's `OpCommitResponse` /
/// `IterCommitResponse` before the run terminates abnormally — the same posture
/// as any mid-run channel loss (the un-acked op/iteration re-runs on resume,
/// at-least-once).
const OP_COMMIT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
/// Re-check cadence during the ack wait so a cooperative cancel is observed
/// promptly (mirrors the restore gate's `RESUME_WAIT_CHUNK`).
const OP_COMMIT_ACK_CHUNK: Duration = Duration::from_millis(200);

/// How long an inbox-driven loop waits at a committed iteration boundary before
/// it parks (eviction hysteresis, `bounded-durable-suspension`): a message
/// arriving inside the window continues the loop in place instead of paying a
/// park/rehydrate cycle. Overridable with `FLEZHA_BOUNDED_PARK_IDLE_MS`.
const PARK_IDLE_DEFAULT_MS: u64 = 5_000;

/// How a bounded run ended, as seen by the teardown.
enum RunOutcome {
    /// Terminal: emit `BoundedCompleted` with this `(status, error, result)`.
    Terminal(String, String, Vec<u8>),
    /// Suspended at a committed iteration boundary. `IterPark` is already on the
    /// wire; the run tears down WITHOUT a terminal completion, and the
    /// controller keeps the instance `suspended` until an input (or the wake
    /// deadline) rehydrates it.
    Parked,
}

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
    /// Loop shape: `Once` is the single pass, the others iterate the chain with
    /// a durable commit per iteration (`bounded-durable-loop`).
    loop_mode: LoopMode,
    /// Wake deadline armed when a `PerInput` loop parks.
    wake_after: Option<Duration>,
    /// Post-start input, fed from the wire dispatch path. A `PerInput` loop
    /// seeds each iteration from it.
    inbox: &'a Receiver<InboxItem>,
    /// Per-segment retry ladder applied inside a stage (`bounded-segment-retry`).
    retry: RetryPolicy,
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
    inbox_rx: Receiver<InboxItem>,
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
    inbox_rx: Receiver<InboxItem>,
    args: String,
    pool: Arc<WorkStealingPool>,
) {
    let finished = Arc::new(AtomicBool::new(false));

    // max_duration backstop: trip `cancel` once it elapses, unless the body
    // finished first.
    let watcher = spec
        .max_duration
        .map(|d| spawn_duration_watcher(d, Arc::clone(&instance.cancel), Arc::clone(&finished)));

    let outcome = match spec.topology.as_ref() {
        // Structured: hold on the crash-resume gate until the Controller's
        // Segment restores land, then drive the chain over the restored state.
        Some(_) => {
            instance.wait_for_restore();
            run_structured(&spec, &instance, &args, &pool, &inbox_rx)
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

    match outcome {
        RunOutcome::Terminal(status, error, result) => {
            crate::executor::emit_bounded_completed(
                instance.instance_id.to_string(),
                status,
                error,
                result,
            );
        }
        // Suspension is not a terminal outcome: the controller evicts the
        // instance on the already-shipped `IterPark` and keeps it resumable.
        RunOutcome::Parked => {}
    }
}

/// Run-sugar path: a single implicit Operation/Segment whose body is `run`.
fn run_sugar(
    run: BoundedRunFn,
    instance_id: &Arc<str>,
    args: &str,
    cancel: &Arc<AtomicBool>,
    inbox_rx: Receiver<InboxItem>,
) -> RunOutcome {
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
) -> RunOutcome {
    let (status, error) = match outcome {
        Ok(Ok(())) => {
            if cancel.load(Ordering::Acquire) {
                ("cancelled".to_string(), String::new())
            } else {
                ("ok".to_string(), String::new())
            }
        }
        Ok(Err(e)) => ("error".to_string(), e.to_string()),
        Err(panic) => (
            "error".to_string(),
            crate::operation::error::format_panic(panic),
        ),
    };
    RunOutcome::Terminal(status, error, result)
}

/// Structured path: run the linear chain sequentially. Operation `N+1` starts
/// only when every segment of operation `N` has finished (count-based barrier);
/// segments within an operation run in parallel on the pool.
fn run_structured(
    spec: &BoundedSpec,
    instance: &BoundedInstance,
    args: &str,
    pool: &Arc<WorkStealingPool>,
    inbox: &Receiver<InboxItem>,
) -> RunOutcome {
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
        loop_mode: spec.loop_mode,
        wake_after: spec.wake_after,
        inbox,
        retry: spec.retry,
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

fn drive_chain(ctx: &ChainCtx, args: &str, cancel: &AtomicBool) -> RunOutcome {
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
        run_compensation(ctx, &[], &resumed);
        return RunOutcome::Terminal(
            "error".to_string(),
            "resumed compensation after crash".to_string(),
            Vec::new(),
        );
    }

    let resume = journal.unwrap_or_default();

    // The journal's committed work, in the two granularities the driver needs:
    // the fully-committed iterations (`plan_compensation`'s `committed` input, so
    // keep-progress keeps them and all-or-nothing unwinds them —
    // `bounded-durable-loop` D2) and the stages of the still-in-flight iteration.
    // Both are empty for a fresh run and for a non-looping chain.
    let (mut committed, in_flight) = journaled_stages(ctx, &resume);

    let mut iter = resume.iter;
    let mut consumed_offset = resume.consumed_offset;
    // Terminal batch of the previous iteration, carried in memory by a
    // handoff-chained loop; on resume it comes back from the journal instead.
    let mut carry: Option<Vec<Value>> = None;

    // Resume state, consumed by the first pass: the operations of the in-flight
    // iteration the journal already records committed are skipped (`first_stage`)
    // but still recorded as completed, so a later failure rolls them back too.
    let mut resumed_stages = Some(in_flight);
    let mut first_stage = resume
        .committed_op_idxs
        .iter()
        .max()
        .map_or(0, |last| *last as usize + 1);

    loop {
        // Completed stages of this iteration, in completion order, so an abnormal
        // exit rolls them back in reverse.
        let mut completed: Vec<StageRecord> = resumed_stages.take().unwrap_or_default();

        let seed = if first_stage > 0 {
            // Mid-iteration resume: pick up at the first uncommitted operation.
            match seed_from_journal(ctx, &resume.handoff) {
                Ok(batch) => IterationSeed::Batch(batch, None),
                Err(e) => IterationSeed::Failed(e),
            }
        } else {
            next_iteration_seed(ctx, args, iter, &resume, carry.take(), cancel)
        };

        let batch = match seed {
            IterationSeed::Batch(batch, consumed) => {
                if let Some(offset) = consumed {
                    consumed_offset = offset;
                }
                batch
            }
            // The loop ran out of work (single pass done, chained loop released
            // an empty batch, inbox closed): a clean terminal.
            IterationSeed::Done => return terminal_or_cancelled(cancel),
            // The park record names the last COMMITTED iteration — `iter` is the
            // one it would have run next — so `parked(N)` always follows
            // `iter_committed(N)`, and a resume enters N+1 either way.
            IterationSeed::Park => return park(ctx, iter.saturating_sub(1)),
            IterationSeed::Cancelled => {
                return RunOutcome::Terminal("cancelled".to_string(), String::new(), Vec::new());
            }
            // The seed cannot be built (an unre-seedable journaled handoff, an
            // entry payload the channel rejects) — terminate abnormally and undo
            // what is outstanding.
            IterationSeed::Failed(e) => {
                run_compensation(ctx, &committed, &completed);
                return RunOutcome::Terminal("error".to_string(), e, Vec::new());
            }
        };

        match run_iteration(ctx, iter, first_stage, batch, cancel, &mut completed) {
            Ok(terminal_batch) => {
                // Iteration boundary (`bounded-durable-loop`): journal
                // `iter_committed`, snapshot the segments, flush the iteration's
                // staged outbox, release the consumed inbox prefix — then enter
                // the next iteration. A non-looping chain has no boundary and
                // finishes here, exactly as before.
                if ctx.loop_mode == LoopMode::Once {
                    return RunOutcome::Terminal("ok".to_string(), String::new(), Vec::new());
                }
                if let Err(e) = commit_iteration(ctx, iter, consumed_offset, &terminal_batch) {
                    let (status, error) = abnormal_status(cancel, e);
                    run_compensation(ctx, &committed, &completed);
                    return RunOutcome::Terminal(status, error, Vec::new());
                }
                committed.push(completed);
                carry = Some(terminal_batch);
                iter += 1;
                // Every iteration after the resumed one runs the whole chain.
                first_stage = 0;
            }
            Err((status, error)) => {
                run_compensation(ctx, &committed, &completed);
                return RunOutcome::Terminal(status, error, Vec::new());
            }
        }
    }
}

/// Run one iteration's stages from `first_stage` to the end of the chain,
/// recording each completed stage into `completed`. `Ok` carries the terminal
/// batch the last operation released; `Err` the terminal status of an abnormal
/// exit (a failing operation, a cooperative stop, a refused op commit).
fn run_iteration(
    ctx: &ChainCtx,
    iter: u32,
    first_stage: usize,
    mut batch: Vec<Value>,
    cancel: &AtomicBool,
    completed: &mut Vec<StageRecord>,
) -> Result<Vec<Value>, (String, String)> {
    for (stage_idx, &op_id) in ctx.chain.iter().enumerate().skip(first_stage) {
        // A cooperative stop before a stage starts is partial progress: stop
        // advancing and roll back what completed.
        if cancel.load(Ordering::Acquire) {
            return Err(("cancelled".to_string(), String::new()));
        }
        let op = ctx
            .topology
            .operation(op_id)
            .expect("chain operation present in topology");
        let slots = &ctx.stage_segments[stage_idx];
        let partitioned = partition(op, slots.len(), &batch);
        let StageOut {
            batch: out_batch,
            segments,
        } = run_stage_parallel(ctx, slots, partitioned)
            .map_err(|e| abnormal_status(cancel, e.to_string()))?;
        batch = out_batch;

        // Durable saga (§4.3): record this operation's completion — one
        // compensation-intent marker per segment that ran — so a crash
        // mid-compensation knows what still needs undo. Only for compensable
        // operations (a hookless op is never rolled back).
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
        // completion + handoff on the controller and block until the durable
        // ack — the next operation must not race ahead of the committed record.
        // Ordered AFTER the intent markers so a crash between marker and ack
        // resolves as "op not committed → re-run" (at-least-once).
        let commit = commit_op_boundary(ctx, stage_idx, iter, op, &segments, slots, &batch);
        completed.push(StageRecord {
            stage_idx,
            segments,
        });
        if let Err(e) = commit {
            return Err(abnormal_status(cancel, e));
        }
    }
    Ok(batch)
}

/// How the next iteration is seeded.
enum IterationSeed {
    /// Run the iteration over this batch; `Some(offset)` when an inbox item
    /// supplied it (the offset the iteration commit records as consumed).
    Batch(Vec<Value>, Option<u64>),
    /// The loop has no more work: terminate cleanly.
    Done,
    /// The inbox is empty at a committed boundary past the idle threshold:
    /// suspend (`bounded-durable-suspension`).
    Park,
    /// A cooperative stop arrived while waiting for input — nothing is in
    /// flight, so there is nothing to roll back.
    Cancelled,
    /// The seed could not be built; terminate abnormally.
    Failed(String),
}

/// Resolve the seed for a fresh iteration from the loop's shape: the spawn args
/// (single pass / first iteration), the next inbox item (`PerInput`), or the
/// previous iteration's terminal handoff (`Chained`).
fn next_iteration_seed(
    ctx: &ChainCtx,
    args: &str,
    iter: u32,
    resume: &JournalResume,
    carry: Option<Vec<Value>>,
    cancel: &AtomicBool,
) -> IterationSeed {
    match ctx.loop_mode {
        // Single pass, and the first iteration of a chained loop: the spawn args
        // through the entry channel.
        LoopMode::Once | LoopMode::Chained if iter == 0 => {
            seed_or_fail(seed_bytes(ctx.topology, args.as_bytes().to_vec()), None)
        }
        // A single pass has no second iteration.
        LoopMode::Once => IterationSeed::Done,

        // Handoff-chained: the previous iteration's terminal batch, in memory or
        // (after a crash) re-decoded from the journaled iteration commit. An
        // empty batch means the loop has converged.
        LoopMode::Chained => {
            let batch = match carry {
                Some(batch) => batch,
                None => match seed_from_journal(ctx, &resume.handoff) {
                    Ok(batch) => batch,
                    Err(e) => return IterationSeed::Failed(e),
                },
            };
            if batch.is_empty() {
                IterationSeed::Done
            } else {
                IterationSeed::Batch(batch, None)
            }
        }

        // Inbox-driven: one iteration per post-start input. An empty inbox past
        // the idle threshold parks the instance (durable classes only — an
        // opted-out one has no journal to park into and blocks in memory, as
        // before).
        LoopMode::PerInput => match wait_for_input(ctx, cancel) {
            Some(item) => seed_or_fail(
                seed_bytes(ctx.topology, item.payload),
                // Offset 0 = unjournaled input (opted-out class): keep the
                // recorded consumed offset where it is.
                (item.offset > 0).then_some(item.offset),
            ),
            None if cancel.load(Ordering::Acquire) => IterationSeed::Cancelled,
            None if ctx.journaling() => IterationSeed::Park,
            // Transport-less / opted-out: the inbox closed for good.
            None => IterationSeed::Done,
        },
    }
}

fn seed_or_fail(seeded: Result<Vec<Value>, String>, consumed: Option<u64>) -> IterationSeed {
    match seeded {
        Ok(batch) => IterationSeed::Batch(batch, consumed),
        Err(e) => IterationSeed::Failed(e),
    }
}

/// Block for the next inbox item. `None` on a cooperative stop, on a closed
/// inbox, or — for a journaling instance — once the inbox has been idle past
/// the hysteresis window (the caller then parks). Sleeps while idle; the
/// ≤200 ms wake granularity observes cancellation promptly.
fn wait_for_input(ctx: &ChainCtx, cancel: &AtomicBool) -> Option<InboxItem> {
    let deadline = Instant::now() + park_idle();
    loop {
        if cancel.load(Ordering::Acquire) {
            return None;
        }
        match ctx.inbox.recv_timeout(OP_COMMIT_ACK_CHUNK) {
            Ok(item) => return Some(item),
            Err(RecvTimeoutError::Timeout) => {
                if ctx.journaling() && Instant::now() >= deadline {
                    return None;
                }
            }
            Err(RecvTimeoutError::Disconnected) => return None,
        }
    }
}

/// Eviction hysteresis for an idle inbox-driven loop.
fn park_idle() -> Duration {
    let ms = std::env::var("FLEZHA_BOUNDED_PARK_IDLE_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(PARK_IDLE_DEFAULT_MS);
    Duration::from_millis(ms)
}

/// Suspend at the committed boundary of iteration `iter`: ship `IterPark` (with
/// the class's durable wake deadline, if any) and tear the run down without a
/// terminal completion. The controller journals `parked`, evicts the instance,
/// and rehydrates it on the next inbox append or at the deadline.
fn park(ctx: &ChainCtx, iter: u32) -> RunOutcome {
    let wake_deadline_ms = ctx.wake_after.map_or(0, |after| {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        (now + after).as_millis() as u64
    });
    crate::executor::emit_control_request(
        format!(
            "iter-park-{}-{}-{}",
            ctx.instance_id,
            iter,
            crate::util::nonce_suffix()
        ),
        crate::pb::command_request::Command::IterPark(crate::pb::IterPark {
            operative_id: ctx.instance_id.to_string(),
            iter,
            wake_deadline_ms,
        }),
    );
    RunOutcome::Parked
}

/// Terminal status of a loop that ran out of work: `cancelled` when a stop
/// tripped the token, else a clean `ok`.
fn terminal_or_cancelled(cancel: &AtomicBool) -> RunOutcome {
    let status = if cancel.load(Ordering::Acquire) {
        "cancelled"
    } else {
        "ok"
    };
    RunOutcome::Terminal(status.to_string(), String::new(), Vec::new())
}

/// A commit failure is `cancelled` when the token tripped (the stop is what
/// interrupted the ack wait), else a terminal error.
fn abnormal_status(cancel: &AtomicBool, error: String) -> (String, String) {
    if cancel.load(Ordering::Acquire) {
        ("cancelled".to_string(), String::new())
    } else {
        ("error".to_string(), error)
    }
}

/// Wrap one entry payload — the spawn args, or an inbox item's bytes — into the
/// entry channel's typed value.
fn seed_bytes(topology: &OperativeTopology, bytes: Vec<u8>) -> Result<Vec<Value>, String> {
    let entry = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::Entry)
        .expect("structured bounded has an entry channel (validated at registration)");
    let wrap = entry
        .wrap_input
        .as_ref()
        .expect("entry channel carries a wrap_input");
    wrap(bytes)
        .map(|v| vec![v])
        .map_err(|e| format!("seeding entry channel failed: {e}"))
}

/// Decode a journaled handoff batch back into typed values via each item's
/// producing-channel codec (`bounded-op-journal` crash-resume seeding).
fn seed_from_journal(ctx: &ChainCtx, handoff: &[(u32, Vec<u8>)]) -> Result<Vec<Value>, String> {
    let mut seeded: Vec<Value> = Vec::with_capacity(handoff.len());
    for (channel_id, bytes) in handoff {
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

/// Rebuild the journal's committed work as stage records, in the two
/// granularities the driver needs: the **fully-committed iterations** (below the
/// resume point — `plan_compensation`'s `committed` input, `bounded-durable-loop`
/// D2) and the stages of the **still-in-flight iteration**, which a resumed pass
/// skips but must still be able to roll back. One traversal, one grouping.
///
/// Segment outputs are empty, as on any crash-resume: an operation's collected
/// inter-operation output was never durable, so a durable compensable operation
/// persists its undo handle in Segment state (which IS restored), per the
/// at-least-once author contract.
fn journaled_stages(
    ctx: &ChainCtx,
    resume: &JournalResume,
) -> (Vec<Vec<StageRecord>>, Vec<StageRecord>) {
    let mut by_iter: BTreeMap<u32, Vec<StageRecord>> = BTreeMap::new();
    for op in &resume.committed_ops {
        if let Some(record) = stage_record(ctx, op.op_idx, &op.segments_ran) {
            by_iter.entry(op.iter).or_default().push(record);
        }
    }
    for stages in by_iter.values_mut() {
        // Completion order within an iteration is chain order.
        stages.sort_by_key(|s| s.stage_idx);
    }
    // The resume point splits the two: everything below it is committed progress.
    let in_flight = by_iter
        .split_off(&resume.iter)
        .remove(&resume.iter)
        .unwrap_or_default();
    (by_iter.into_values().collect(), in_flight)
}

/// One journaled op commit as a `StageRecord`, resolving each shipped-back wire
/// segment identity to its slot index. `None` when the operation is not in this
/// build's chain (a topology that changed under a journaled instance).
fn stage_record(ctx: &ChainCtx, op_idx: u32, segments_ran: &[Vec<u8>]) -> Option<StageRecord> {
    let stage_idx = op_idx as usize;
    let slots = ctx.stage_segments.get(stage_idx)?;
    let segments = segments_ran
        .iter()
        .filter_map(|id| {
            slots
                .iter()
                .position(|slot| slot.segment_identity.as_slice() == id.as_slice())
        })
        .map(|segment_index| SegmentRun {
            segment_index,
            outputs: Vec::new(),
            staged: Vec::new(),
        })
        .collect();
    Some(StageRecord {
        stage_idx,
        segments,
    })
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
    iter: u32,
    op: &OperationDescriptor,
    segments: &[SegmentRun],
    slots: &[Arc<SegmentSlot>],
    batch: &[Value],
) -> Result<(), String> {
    if !ctx.journaling() {
        return Ok(());
    }

    let handoff = encode_handoff(ctx, op.output_channel_id, batch)
        .map_err(|e| format!("op {stage_idx} {e}"))?;

    let segments_ran: Vec<Vec<u8>> = segments
        .iter()
        .map(|seg_run| {
            slots[seg_run.segment_index]
                .segment_identity
                .as_ref()
                .clone()
        })
        .collect();

    ship_and_await(
        ctx,
        stage_idx,
        "commit",
        true,
        crate::pb::command_request::Command::OpCommit(crate::pb::OpCommit {
            operative_id: ctx.instance_id.to_string(),
            op_idx: stage_idx as u32,
            iter,
            segments_ran,
            handoff,
            outbox: segments
                .iter()
                .flat_map(|seg_run| outbox_items(&slots[seg_run.segment_index], &seg_run.staged))
                .collect(),
            compensating: false,
        }),
    )
}

/// Ship `IterCommit` for a finished loop iteration and block until the
/// controller's durable `IterCommitResponse` (`bounded-durable-loop` D1): the
/// journal records `iter_committed(iter, consumed_offset)` with the iteration's
/// terminal handoff, the instance's segments are snapshotted at the boundary,
/// the iteration's staged outbox is flushed, and the consumed inbox prefix is
/// released. The next iteration starts only on the successful reply, so a crash
/// re-runs at most the whole in-flight iteration — never a committed one.
fn commit_iteration(
    ctx: &ChainCtx,
    iter: u32,
    consumed_offset: u64,
    batch: &[Value],
) -> Result<(), String> {
    if !ctx.journaling() {
        return Ok(());
    }

    // The terminal handoff re-seeds iteration N+1 of a handoff-chained loop
    // after a crash; an inbox-driven loop re-seeds from its inbox instead and
    // ships whatever the terminal operation released (empty when it has no
    // codec'd output channel).
    let terminal_channel = ctx
        .chain
        .last()
        .and_then(|&op_id| ctx.topology.operation(op_id))
        .and_then(|op| op.output_channel_id);
    let handoff = encode_handoff(ctx, terminal_channel, batch)
        .map_err(|e| format!("iteration {iter} {e}"))?;

    ship_and_await(
        ctx,
        iter as usize,
        "iteration commit",
        true,
        crate::pb::command_request::Command::IterCommit(crate::pb::IterCommit {
            operative_id: ctx.instance_id.to_string(),
            iter,
            consumed_offset,
            handoff,
        }),
    )
}

/// Serialize a released batch through its producing channel's inter-op codec.
/// Empty when the operation has no output channel (a terminal stage) or the
/// channel carries no codec (a plugin-bound output).
fn encode_handoff(
    ctx: &ChainCtx,
    channel_id: Option<u32>,
    batch: &[Value],
) -> Result<Vec<crate::pb::OpHandoffItem>, String> {
    let Some(channel_id) = channel_id else {
        return Ok(Vec::new());
    };
    let Some(codec) = ctx
        .topology
        .channel(channel_id)
        .and_then(|c| c.codec.as_ref())
    else {
        return Ok(Vec::new());
    };

    batch
        .iter()
        .map(|value| {
            (codec.encode)(value.as_ref())
                .map(|payload| crate::pb::OpHandoffItem {
                    channel_id,
                    payload,
                })
                .map_err(|e| format!("handoff serialization failed on channel {channel_id}: {e}"))
        })
        .collect()
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

/// Emit one durability handshake — `OpCommit` (forward op boundary or
/// compensation undo record) or `IterCommit` (iteration boundary) — and block
/// until the controller's durable ack. All three are "journal this, then tell me
/// it is durable" round-trips over the same reference-id-keyed reply path.
///
/// `abort_on_cancel` aborts the wait early on a cooperative cancel, which the
/// caller maps to the `cancelled` terminal status. The compensation undo record
/// passes `false`: rollback IS the response to that cancel, and abandoning the
/// record because the token is set would drop the undo output it carries — it
/// waits out the full ack deadline instead.
fn ship_and_await(
    ctx: &ChainCtx,
    idx: usize,
    what: &str,
    abort_on_cancel: bool,
    command: crate::pb::command_request::Command,
) -> Result<(), String> {
    let reference_id = format!(
        "op-commit-{}-{}-{}",
        ctx.instance_id,
        idx,
        crate::util::nonce_suffix()
    );
    let rx = crate::shutdown::register_pending_ack(reference_id.clone());
    crate::executor::emit_control_request(reference_id.clone(), command);

    // Chunked wait so a cooperative cancel is observed promptly.
    let deadline = Instant::now() + OP_COMMIT_ACK_TIMEOUT;
    loop {
        match rx.recv_timeout(OP_COMMIT_ACK_CHUNK) {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(e)) => {
                return Err(format!("{what} {idx} refused by controller: {e}"));
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if abort_on_cancel && ctx.instance.cancel.load(Ordering::Acquire) {
                    crate::shutdown::drop_pending_ack(&reference_id);
                    return Err(format!("{what} {idx} interrupted by stop"));
                }
                if Instant::now() >= deadline {
                    crate::shutdown::drop_pending_ack(&reference_id);
                    return Err(format!(
                        "{what} {idx} ack timed out after {}s",
                        OP_COMMIT_ACK_TIMEOUT.as_secs()
                    ));
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                crate::shutdown::drop_pending_ack(&reference_id);
                return Err(format!("{what} {idx} waiter dropped"));
            }
        }
    }
}

/// Roll back an abnormally-terminated chain. The in-flight iteration's completed
/// operations are always compensated, in reverse order (per
/// [`plan_compensation`]) and, within an operation, once per segment that ran;
/// `committed` holds the iterations the journal records as finished, which
/// keep-progress keeps and all-or-nothing unwinds after it. Operations without a
/// `compensate` hook are skipped. Best-effort: a failing hook is logged and the
/// remaining steps still run, so one broken undo cannot strand the rest.
fn run_compensation(ctx: &ChainCtx, committed: &[Vec<StageRecord>], in_flight: &[StageRecord]) {
    let plan = plan_compensation(ctx.rollback_mode, committed, in_flight);
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
                let commit = crate::pb::command_request::Command::OpCommit(crate::pb::OpCommit {
                    operative_id: ctx.instance_id.to_string(),
                    op_idx: record.stage_idx as u32,
                    iter: 0,
                    segments_ran: vec![slot.segment_identity.as_ref().clone()],
                    handoff: Vec::new(),
                    outbox: outbox_items(slot, &sink.staged),
                    compensating: true,
                });
                if let Err(e) = ship_and_await(ctx, record.stage_idx, "compensation", false, commit)
                {
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
/// A failing segment climbs its retry ladder in place ([`run_segment_retrying`])
/// — its siblings keep their completed results buffered and are never re-run.
fn run_stage_parallel(
    ctx: &ChainCtx,
    slots: &[Arc<SegmentSlot>],
    partitioned: Vec<Vec<Value>>,
) -> Result<StageOut, BoundedError> {
    let (tx, rx) = unbounded::<(usize, Result<SegmentOut, BoundedError>)>();
    let mut spawned = 0usize;
    for (seg_idx, (slot, items)) in slots.iter().zip(partitioned.into_iter()).enumerate() {
        if items.is_empty() {
            continue;
        }
        let job = SegmentJob {
            topology: Arc::clone(ctx.topology),
            instance_id: Arc::clone(ctx.instance_id),
            slot: Arc::clone(slot),
            staging: ctx.journaling(),
            retry: ctx.retry,
            cancel: Arc::clone(&ctx.instance.cancel),
        };
        let tx = tx.clone();
        spawned += 1;
        ctx.pool.submit(Box::new(move || {
            let _ = tx.send((seg_idx, job.run_with_retry(items)));
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
    // own item order is preserved by `SegmentJob::run`).
    segments.sort_by_key(|s| s.segment_index);
    let batch: Vec<Value> = segments
        .iter()
        .flat_map(|s| s.outputs.iter().map(Arc::clone))
        .collect();
    Ok(StageOut { batch, segments })
}

/// One segment's stage result: the inter-operation output collected for the
/// next stage plus the plugin-bound output staged for the operation's outbox.
type SegmentOut = (Vec<Value>, Vec<StagedOutput>);

/// One segment's share of a stage, moved onto a pool thread: everything needed
/// to run its batch — and to re-run it on a retry — off the driver thread.
struct SegmentJob {
    topology: Arc<OperativeTopology>,
    instance_id: Arc<str>,
    slot: Arc<SegmentSlot>,
    staging: bool,
    retry: RetryPolicy,
    cancel: Arc<AtomicBool>,
}

impl SegmentJob {
    /// Run the batch, climbing the class's retry ladder on a retryable failure
    /// (`bounded-segment-retry`): the segment re-runs its whole batch from the
    /// top — at-least-once, the same idempotency contract crash-resume imposes —
    /// with each attempt collecting into a fresh sink, so a failed attempt's
    /// output is discarded rather than double-counted. A non-retryable error, an
    /// exhausted ladder, or a cancellation during the backoff returns the error
    /// into the caller's abnormal path unchanged.
    fn run_with_retry(&self, items: Vec<Value>) -> Result<SegmentOut, BoundedError> {
        let max_attempts = self.retry.max_attempts.max(1);
        let mut items = Some(items);
        let mut attempt: u32 = 1;
        loop {
            let is_final = attempt >= max_attempts;
            // A non-final attempt hands the batch over by Arc clone so the
            // original survives for the re-run; the final attempt moves it.
            let attempt_items = match &items {
                Some(batch) if !is_final => batch.clone(),
                _ => items.take().expect("retry ladder: batch consumed twice"),
            };
            match self.run(attempt_items) {
                Ok(out) => return Ok(out),
                Err(e) if is_final || e.non_retryable => return Err(e),
                Err(e) => {
                    let delay = crate::segment::retry::note_failed_attempt(
                        &self.label(),
                        attempt,
                        self.retry,
                        &e,
                    );
                    // An external stop / `max_duration` during the wait abandons
                    // the ladder: the failure takes the abnormal-exit path.
                    if !crate::util::sleep_interruptible(delay, Some(&self.cancel)) {
                        return Err(e);
                    }
                    attempt += 1;
                }
            }
        }
    }

    /// One attempt: process the segment's items sequentially (per-segment
    /// ordering preserved), running `Operation::run` via the monomorphized
    /// dispatcher's collecting path and gathering the inter-operation output for
    /// the next stage plus the plugin-bound output staged for the operation's
    /// outbox.
    fn run(&self, items: Vec<Value>) -> Result<SegmentOut, BoundedError> {
        let mut sink = BoundedSegmentSink::new(&self.instance_id, self.staging);
        let mut state = self.slot.state.lock_recover();
        for item in items {
            self.slot
                .dispatcher
                .run_collecting(item, &mut state, &self.topology, &mut sink)?;
        }
        Ok((sink.collected, sink.staged))
    }

    /// The failing work, as the retry log names it.
    fn label(&self) -> String {
        let operation = self
            .topology
            .operation(self.slot.operation_id)
            .map_or("-", |op| op.name.as_ref());
        format!(
            "bounded {} operation {} segment {}",
            self.instance_id,
            operation,
            self.slot.operation_key.as_deref().unwrap_or("-")
        )
    }
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
/// unless the body finished first (the sleep wakes every ≤200 ms to honour an
/// early finish promptly).
fn spawn_duration_watcher(
    duration: Duration,
    cancel: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
) -> JoinHandle<()> {
    thread::Builder::new()
        .name("flezha-bounded-timer".to_string())
        .spawn(move || {
            if crate::util::sleep_interruptible(duration, Some(&finished)) {
                cancel.store(true, Ordering::Release);
            }
        })
        .expect("spawning flezha-bounded-timer thread should not fail")
}
