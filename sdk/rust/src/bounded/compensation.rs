//! Saga compensation planning for the bounded driver.
//!
//! The driver records each **completed** stage (the operation + the segments
//! that ran, with their collected outputs) as it advances. On abnormal
//! termination — an operation errors, or an external stop trips after partial
//! progress — [`plan_compensation`] turns those records into the exact rollback
//! order: completed operations are undone in **reverse**, and within an
//! operation each segment that ran is compensated once (segment order is
//! irrelevant — independent keys). A clean `ok` produces no records, so nothing
//! is compensated.
//!
//! The planner is **pure** so both rollback modes are testable without driving
//! the executor. v1 runs the structured chain as a single pass, so it always
//! produces exactly one in-flight iteration and zero committed iterations; the
//! `committed` parameter and the keep-progress branch are exercised by the unit
//! tests and will carry the structured-loop follow-up unchanged.

use std::any::Any;
use std::sync::Arc;

use super::RollbackMode;

/// Type-erased value a segment collected on its forward run, retained for a
/// possible compensation. Downcast back to `Arc<S::Output>` by
/// `OperationDispatcher::compensate_collecting`.
pub(crate) type Value = Arc<dyn Any + Send + Sync>;

/// One serialized plugin-bound output staged by a segment, in the order the
/// segment produced it: `(plugin channel name, payload bytes)`.
pub(crate) type StagedOutput = (String, Vec<u8>);

/// One segment's contribution to a completed stage: which segment ran, the
/// inter-operation outputs it collected, and the plugin-bound output it staged
/// for the operation's transactional outbox. Only segments that actually
/// processed input are recorded ("an effect"); empty-partition segments never
/// ran and are skipped.
pub(crate) struct SegmentRun {
    pub segment_index: usize,
    pub outputs: Vec<Value>,
    pub staged: Vec<StagedOutput>,
}

/// A completed operation in the chain, recorded so it can be rolled back.
/// `stage_idx` indexes both the chain and the driver's pre-built stage-segment
/// table, so the compensator resolves the operation descriptor and the segment
/// slots from it. `iter` is the loop iteration it ran in, which its `compensated`
/// journal entry is keyed by (0 for a non-looping chain, and for a record
/// rebuilt from compensation-intent markers, which carry no iteration).
pub(crate) struct StageRecord {
    pub stage_idx: usize,
    pub iter: u32,
    pub segments: Vec<SegmentRun>,
}

/// Resolve the rollback order for an abnormal termination.
///
/// `in_flight` is the interrupted iteration's completed stages, in completion
/// order; it is **always** undone (reverse order). `committed` holds fully
/// finished iterations, in completion order:
///
/// - **all-or-nothing** undoes everything — the in-flight iteration first, then
///   each committed iteration, all in reverse completion order, back to the
///   start.
/// - **keep-progress** undoes only the in-flight iteration; every committed
///   iteration is kept.
///
/// Within a single iteration (or a no-loop pass) the two modes coincide:
/// `committed` is empty, so both return the in-flight stages reversed.
pub(crate) fn plan_compensation<'a>(
    mode: RollbackMode,
    committed: &'a [Vec<StageRecord>],
    in_flight: &'a [StageRecord],
) -> Vec<&'a StageRecord> {
    let mut plan: Vec<&StageRecord> = in_flight.iter().rev().collect();
    if mode == RollbackMode::AllOrNothing {
        for iteration in committed.iter().rev() {
            plan.extend(iteration.iter().rev());
        }
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `StageRecord` with no retained outputs (the plan only orders
    /// stages; per-segment output handling is the dispatcher's job).
    fn stage(stage_idx: usize) -> StageRecord {
        StageRecord {
            stage_idx,
            iter: 0,
            segments: Vec::new(),
        }
    }

    fn order(plan: &[&StageRecord]) -> Vec<usize> {
        plan.iter().map(|s| s.stage_idx).collect()
    }

    #[test]
    fn no_progress_compensates_nothing() {
        let plan = plan_compensation(RollbackMode::AllOrNothing, &[], &[]);
        assert!(plan.is_empty());
        let plan = plan_compensation(RollbackMode::KeepProgress, &[], &[]);
        assert!(plan.is_empty());
    }

    #[test]
    fn single_pass_is_reverse_order_in_both_modes() {
        // A no-loop pass (committed empty): both modes undo the whole pass in
        // reverse — Op0, Op1, Op2 completed ⇒ compensate Op2, Op1, Op0.
        let in_flight = vec![stage(0), stage(1), stage(2)];
        for mode in [RollbackMode::AllOrNothing, RollbackMode::KeepProgress] {
            let plan = plan_compensation(mode, &[], &in_flight);
            assert_eq!(order(&plan), vec![2, 1, 0], "mode {mode:?}");
        }
    }

    #[test]
    fn all_or_nothing_undoes_committed_iterations_too() {
        // Two committed iterations (each a 2-stage pass) plus a partial third.
        let committed = vec![vec![stage(0), stage(1)], vec![stage(0), stage(1)]];
        let in_flight = vec![stage(0)];
        let plan = plan_compensation(RollbackMode::AllOrNothing, &committed, &in_flight);
        // Reverse completion order across every iteration, back to the start:
        // in-flight Op0, then iteration 2 (Op1, Op0), then iteration 1 (Op1, Op0).
        assert_eq!(order(&plan), vec![0, 1, 0, 1, 0]);
    }

    #[test]
    fn keep_progress_keeps_committed_iterations() {
        // Same shape, keep-progress: only the in-flight iteration unwinds; the
        // two finished iterations are kept.
        let committed = vec![vec![stage(0), stage(1)], vec![stage(0), stage(1)]];
        let in_flight = vec![stage(0)];
        let plan = plan_compensation(RollbackMode::KeepProgress, &committed, &in_flight);
        assert_eq!(order(&plan), vec![0]);
    }
}
