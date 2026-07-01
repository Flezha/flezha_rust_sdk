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
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam::channel::{unbounded, Receiver};

use super::compensation::{plan_compensation, SegmentRun, StageRecord};
use super::instance::BoundedInstance;
use super::spec::BoundedSpec;
use super::{BoundedCollectSink, BoundedCtx, BoundedError, BoundedRunFn, RollbackMode};
use crate::channel::builder::BuildSegmentArgs;
use crate::channel::{ChannelKind, OperationId};
use crate::executor::work_stealing_pool::WorkStealingPool;
use crate::operative::topology::{OperationDescriptor, OperativeTopology, SegmentingMode};
use crate::segment::flusher::SegmentFlusher;
use crate::segment::SegmentSlot;
use crate::util::LockRecover;

/// Type-erased value moving between bounded stages.
type Value = Arc<dyn Any + Send + Sync>;

/// One stage's parallel-join result: the inter-operation `batch` released to the
/// next stage, plus the per-segment record (which segments ran, with their
/// collected outputs) retained for a possible saga rollback.
struct StageOut {
    batch: Vec<Value>,
    segments: Vec<SegmentRun>,
}

/// Read-only context threaded through one structured chain run: the topology,
/// the resolved chain order, the instance id, the work pool, the pre-built
/// per-stage segments, and the rollback mode. Bundled so the chain driver and
/// the compensator take it by reference instead of a long parameter list.
struct ChainCtx<'a> {
    topology: &'a Arc<OperativeTopology>,
    chain: &'a [OperationId],
    instance_id: &'a Arc<str>,
    pool: &'a Arc<WorkStealingPool>,
    stage_segments: &'a [Vec<Arc<SegmentSlot>>],
    rollback_mode: RollbackMode,
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
        instance_id: &instance.instance_id,
        pool,
        stage_segments: &instance.stage_segments,
        rollback_mode: spec.rollback_mode,
    };
    drive_chain(&ctx, args, &instance.cancel)
}

/// Build every stage's Segments for a structured spec, in chain order. Called
/// synchronously by `start_bounded_resumable` so the Segments are registered
/// for restore and the resume gate is armed before the driver thread — or any
/// restore frame — runs. `instance` on each slot is a null `Weak` — the bounded
/// collecting path never routes through a `ContinuousInstance`. When the class
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
    let topology = ctx.topology;
    // Seed the first operation from the spawn args via the entry channel's
    // typed wrapper.
    let entry = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::Entry)
        .expect("structured bounded has an entry channel (validated at registration)");
    let wrap = entry
        .wrap_input
        .as_ref()
        .expect("entry channel carries a wrap_input");
    let mut batch: Vec<Value> = match wrap(args.as_bytes().to_vec()) {
        Ok(v) => vec![v],
        Err(e) => {
            return (
                "error".to_string(),
                format!("seeding entry channel failed: {e}"),
                Vec::new(),
            )
        }
    };

    // Completed stages of the current (and only, in v1) pass, recorded in
    // completion order so an abnormal exit can roll them back in reverse.
    let mut completed: Vec<StageRecord> = Vec::new();
    // `Some` once the chain terminates abnormally (error / mid-chain stop),
    // carrying the terminal status + error text.
    let mut abnormal: Option<(String, String)> = None;

    for (stage_idx, &op_id) in ctx.chain.iter().enumerate() {
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
        match run_stage_parallel(topology, ctx.instance_id, slots, partitioned, ctx.pool) {
            Ok(stage_out) => {
                batch = stage_out.batch;
                completed.push(StageRecord {
                    stage_idx,
                    segments: stage_out.segments,
                });
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
    let mut sink = BoundedSegmentSink {
        instance_id: Arc::clone(ctx.instance_id),
        collected: Vec::new(),
    };
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
            let outputs: Vec<Value> = seg_run.outputs.iter().map(Arc::clone).collect();
            let mut state = slot.state.lock_recover();
            if let Err(e) =
                slot.dispatcher
                    .compensate_collecting(outputs, &mut state, ctx.topology, &mut sink)
            {
                eprintln!(
                    "[flezha] bounded {} compensation error: {e}",
                    ctx.instance_id
                );
            }
        }
    }
}

/// Build the SegmentSlots for one stage: a single slot in Single mode, one per
/// declared key in Static mode. Reuses the continuous `build_segment` closure
/// so per-Segment state + identity are identical to a continuous operative.
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
) -> Result<StageOut, BoundedError> {
    let (tx, rx) = unbounded::<(usize, Result<Vec<Value>, BoundedError>)>();
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
            let _ = tx.send((seg_idx, run_segment(&topology, &instance_id, &slot, items)));
        }));
    }
    drop(tx);

    // Wait for every segment (the barrier) before returning, even on failure,
    // so no segment is mid-run when the stage completes. Each segment's outputs
    // are kept under its index for both the next-stage batch and saga rollback.
    let mut segments: Vec<SegmentRun> = Vec::new();
    let mut first_err: Option<BoundedError> = None;
    for _ in 0..spawned {
        match rx.recv() {
            Ok((seg_idx, Ok(outputs))) => segments.push(SegmentRun {
                segment_index: seg_idx,
                outputs,
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
/// and gathering inter-operation output for the next stage.
fn run_segment(
    topology: &OperativeTopology,
    instance_id: &Arc<str>,
    slot: &Arc<SegmentSlot>,
    items: Vec<Value>,
) -> Result<Vec<Value>, BoundedError> {
    let mut sink = BoundedSegmentSink {
        instance_id: Arc::clone(instance_id),
        collected: Vec::new(),
    };
    let mut state = slot.state.lock_recover();
    for item in items {
        slot.dispatcher
            .run_collecting(item, &mut state, topology, &mut sink)?;
    }
    Ok(sink.collected)
}

/// Per-segment collecting sink for the structured driver. Inter-operation
/// output is buffered for the next stage; plugin-bound output is forwarded to
/// the unit's wire via the coordinator (identical to `run`-sugar `ctx.emit`).
struct BoundedSegmentSink {
    instance_id: Arc<str>,
    collected: Vec<Value>,
}

impl BoundedCollectSink for BoundedSegmentSink {
    fn collect(&mut self, value: Value) {
        self.collected.push(value);
    }

    fn emit_plugin_bytes(&mut self, bytes: Vec<u8>) {
        crate::executor::emit_bounded_output(self.instance_id.to_string(), bytes);
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
