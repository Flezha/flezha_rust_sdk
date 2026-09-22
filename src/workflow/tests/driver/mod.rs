//! The one driver test. Its phases are ordered functions, not separate
//! `#[test]`s: they share the reconnect trigger, the global sink lock and
//! the class registry this function installs, so the order is a fact only
//! while one test owns it.

use std::sync::Arc;

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::fake_controller::*;
use super::fixtures::*;
use crate::runner::{install_reconnect_trigger, RunnerMessage};
use crate::workflow::*;

mod durable_loop;
mod entrance;
mod fan_in;
mod lifecycle;
mod retry;
mod saga;
mod task_journal;

// ---- single driver-observation test --------------------------------------
//
// Installs the process-global coordinator trigger so it can observe the
// EmitWorkflowOutput / EmitWorkflowCompleted messages the driver routes
// through it; the shared runner-state lock keeps that install exclusive
// against the other tests that install one. Scenarios run strictly
// sequentially (each drained to its completion before the next) so a
// single shared channel stays unambiguous.
#[test]
fn workflow_driver_scenarios() {
    let _runner_guard = crate::runner::env_test_guard();
    let (tx, rx) = crossbeam::channel::unbounded::<RunnerMessage>();
    install_reconnect_trigger(tx);

    // Durable workflow operatives in this test ship WAL frames through the
    // process-global sink; hold the shared sink lock for the whole test so
    // it never races the cell-state sink tests, and install a capturing
    // sink to observe the frames.
    let _sink_guard = crate::cell::state_sink::SINK_TEST_LOCK.lock().unwrap();
    let wal_sink: crate::cell::flusher::SendFrameFn = Arc::new(
        |payload_id: &str, frame: &[u8]| -> Result<(), crate::cell::flusher::SendError> {
            WAL_FRAMES
                .lock()
                .unwrap()
                .push((payload_id.to_string(), frame.to_vec()));
            Ok(())
        },
    );
    crate::cell::state_sink::install_global_sink(wal_sink);

    crate::register_class::<crate::WorkflowKind, EmitWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, FailWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, PanicWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, CompleteWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, LoopWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, TimedWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, TwoCompletions>().unwrap();
    crate::register_class::<crate::WorkflowKind, KeyedCompletions>().unwrap();
    crate::register_class::<crate::WorkflowKind, CompleteThenFail>().unwrap();
    crate::register_class::<crate::WorkflowKind, CompletingChat>().unwrap();
    crate::register_class::<crate::WorkflowKind, ChainWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, TreeWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, TreeSaga>().unwrap();
    crate::register_class::<crate::WorkflowKind, FanWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, FanOptOutWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, HybridWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, SagaWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, KeepProgressSaga>().unwrap();
    crate::register_class::<crate::WorkflowKind, CleanSaga>().unwrap();
    crate::register_class::<crate::WorkflowKind, FanSaga>().unwrap();
    crate::register_class::<crate::WorkflowKind, StopSaga>().unwrap();
    crate::register_class::<crate::WorkflowKind, ResumeSaga>().unwrap();
    crate::register_class::<crate::WorkflowKind, LeakWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, UndoSaga>().unwrap();
    crate::register_class::<crate::WorkflowKind, ChatLoop>().unwrap();
    crate::register_class::<crate::WorkflowKind, LoopSagaAllOrNothing>().unwrap();
    crate::register_class::<crate::WorkflowKind, LoopSagaKeepProgress>().unwrap();
    crate::register_class::<crate::WorkflowKind, RetryWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, RetryFailFastWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, RetrySlowBackoffWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, StepProbeWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, DiamondWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, DiamondSaga>().unwrap();
    crate::register_class::<crate::WorkflowKind, MergeWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, PanickingMergeWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, PanickingTransformWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, TwoChainsWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, FanEntryWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, ChainedChainsWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, JournaledCountdownWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, BudgetOnce>().unwrap();
    crate::register_class::<crate::WorkflowKind, ItemRetryWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, SiblingSaga>().unwrap();

    let mut node = RunnerNode::new("test-ns".to_string());

    // The phases run strictly in this order, each draining its own instances
    // to completion before the next one starts.
    lifecycle::lifecycle(&mut node, &rx);
    lifecycle::completions(&mut node, &rx);
    lifecycle::loop_stop_and_removal(&mut node, &rx);
    lifecycle::structured_chains(&mut node, &rx);
    saga::saga_compensation(&mut node, &rx);
    lifecycle::add_operative_routing(&mut node, &rx);
    task_journal::durable_wal_and_journal(&mut node, &rx);
    task_journal::task_journal_commits(&mut node, &rx);
    task_journal::transactional_outbox(&mut node, &rx);
    task_journal::resume_skip(&mut node, &rx);
    saga::saga_crash_resume(&mut node, &rx);
    fan_in::fan_in_branching(&mut node, &rx);
    fan_in::merge_mapping(&mut node, &rx);
    durable_loop::durable_loop(&mut node, &rx);
    durable_loop::loop_saga_and_rewind(&mut node, &rx);
    durable_loop::loop_completion(&mut node, &rx);
    retry::cell_retry(&mut node, &rx);
    retry::item_retry(&mut node, &rx);
    retry::retry_stop_and_stage(&mut node, &rx);
    saga::sibling_commit(&mut node, &rx);
    fan_in::branching_resume(&mut node, &rx);
    durable_loop::barrier_resend(&mut node, &rx);
    entrance::entrance(&mut node, &rx);
    entrance::once_waits(&mut node, &rx);
}

/// Respawn `test-chain-workflow` as a crash-resume carrying `journal` on the
/// bind (exactly what the AddOperative handler passes), release the restore
/// gate, and drain it to completion.
pub(super) fn resume_chain_with_journal(
    node: &RunnerNode,
    rx: &Receiver<RunnerMessage>,
    id: &str,
    journal: instance::JournalResume,
) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
    assert!(start_resumable(
        node,
        "test-chain-workflow".into(),
        id.into(),
        "",
        1,
        0,
        Some(journal)
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, id, b"ignored-args"));
    let s1_id = crate::cell::flusher::encode_cell_identity(id, "workflow_stage1", "");
    let (inst, _slot) =
        find_cell(node, &s1_id).expect("chain cell reachable by the restore handler");
    restored(&inst);
    drain(rx, id)
}
