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
mod fan_in;
mod proc_journal;
mod retry;
mod run_sugar;
mod saga;

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
    // it never races the entity-state sink tests, and install a capturing
    // sink to observe the frames.
    let _sink_guard = crate::entity::state_sink::SINK_TEST_LOCK.lock().unwrap();
    let wal_sink: crate::entity::flusher::SendFrameFn = Arc::new(
        |payload_id: &str, frame: &[u8]| -> Result<(), crate::entity::flusher::SendError> {
            WAL_FRAMES
                .lock()
                .unwrap()
                .push((payload_id.to_string(), frame.to_vec()));
            Ok(())
        },
    );
    crate::entity::state_sink::install_global_sink(wal_sink);

    crate::register_class::<crate::WorkflowKind, EmitWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, FailWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, PanicWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, CompleteWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, LoopWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, TimedWorkflow>().unwrap();
    crate::register_class::<crate::WorkflowKind, EchoStructured>().unwrap();
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
    crate::register_class::<crate::WorkflowKind, ResendChain>().unwrap();
    crate::register_class::<crate::WorkflowKind, ResendLoop>().unwrap();
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

    let mut node = RunnerNode::new("test-ns".to_string());

    // The phases run strictly in this order, each draining its own instances
    // to completion before the next one starts.
    run_sugar::run_sugar(&mut node, &rx);
    run_sugar::loop_stop_and_removal(&mut node, &rx);
    run_sugar::structured_chains(&mut node, &rx);
    saga::saga_compensation(&mut node, &rx);
    run_sugar::add_operative_routing(&mut node, &rx);
    proc_journal::durable_wal_and_journal(&mut node, &rx);
    proc_journal::proc_journal_commits(&mut node, &rx);
    proc_journal::transactional_outbox(&mut node, &rx);
    proc_journal::resume_skip(&mut node, &rx);
    saga::saga_crash_resume(&mut node, &rx);
    fan_in::fan_in_branching(&mut node, &rx);
    fan_in::merge_mapping(&mut node, &rx);
    durable_loop::durable_loop(&mut node, &rx);
    durable_loop::loop_saga_and_rewind(&mut node, &rx);
    retry::entity_retry(&mut node, &rx);
    retry::retry_stop_and_stage(&mut node, &rx);
    fan_in::branching_resume(&mut node, &rx);
    durable_loop::barrier_resend(&mut node, &rx);
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
    assert!(node.start_workflow_resumable(
        "test-chain-workflow".into(),
        id.into(),
        "ignored-args".into(),
        1,
        0,
        Some(journal)
    ));
    let s1_id = crate::entity::flusher::encode_entity_identity(id, "workflow_stage1", "");
    let (inst, _slot) = node
        .find_workflow_entity(&s1_id)
        .expect("chain entity reachable by the restore handler");
    inst.note_restored();
    drain(rx, id)
}
