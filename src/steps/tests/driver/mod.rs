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
use crate::steps::*;

mod durable_loop;
mod entrance;
mod fan_in;
mod keyed;
mod lifecycle;
mod retry;
mod saga;
mod task_journal;
mod timers;

// ---- single driver-observation test --------------------------------------
//
// Installs the process-global coordinator trigger so it can observe the
// control requests and `EmitRelayCompleted` messages the driver routes
// through it; the shared runner-state lock keeps that install exclusive
// against the other tests that install one. Scenarios run strictly
// sequentially (each drained to its completion before the next) so a
// single shared channel stays unambiguous.
#[test]
fn steps_driver_scenarios() {
    let _runner_guard = crate::runner::env_test_guard();
    let (tx, rx) = crossbeam::channel::unbounded::<RunnerMessage>();
    install_reconnect_trigger(tx);

    // Durable steps relays in this test ship WAL frames through the
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
    // An opted-out class's plugin output leaves on the connection its item
    // rode, which for a fed or re-seeded steps relay is the secondary.
    crate::cell::state_sink::set_secondary_stream(Some(capturing_wire()));

    crate::register_class::<EmitSteps>().unwrap();
    crate::register_class::<FailSteps>().unwrap();
    crate::register_class::<PanicSteps>().unwrap();
    crate::register_class::<CompleteSteps>().unwrap();
    crate::register_class::<LoopSteps>().unwrap();
    crate::register_class::<TimedSteps>().unwrap();
    crate::register_class::<TwoCompletions>().unwrap();
    crate::register_class::<KeyedCompletions>().unwrap();
    crate::register_class::<CompleteThenFail>().unwrap();
    crate::register_class::<CompletingChat>().unwrap();
    crate::register_class::<ChainSteps>().unwrap();
    crate::register_class::<TreeSteps>().unwrap();
    crate::register_class::<TreeSaga>().unwrap();
    crate::register_class::<FanSteps>().unwrap();
    crate::register_class::<FanOptOutSteps>().unwrap();
    crate::register_class::<HybridSteps>().unwrap();
    crate::register_class::<SagaSteps>().unwrap();
    crate::register_class::<KeepProgressSaga>().unwrap();
    crate::register_class::<CleanSaga>().unwrap();
    crate::register_class::<FanSaga>().unwrap();
    crate::register_class::<StopSaga>().unwrap();
    crate::register_class::<ResumeSaga>().unwrap();
    crate::register_class::<LeakSteps>().unwrap();
    crate::register_class::<UndoSaga>().unwrap();
    crate::register_class::<ChatLoop>().unwrap();
    crate::register_class::<PartLoop>().unwrap();
    crate::register_class::<LoopSagaUndoAll>().unwrap();
    crate::register_class::<LoopSagaKeepProgress>().unwrap();
    crate::register_class::<RetrySteps>().unwrap();
    crate::register_class::<RetryFailFastSteps>().unwrap();
    crate::register_class::<RetrySlowBackoffSteps>().unwrap();
    crate::register_class::<StepProbeSteps>().unwrap();
    crate::register_class::<DiamondSteps>().unwrap();
    crate::register_class::<DiamondSaga>().unwrap();
    crate::register_class::<MergeSteps>().unwrap();
    crate::register_class::<PanickingMergeSteps>().unwrap();
    crate::register_class::<PanickingTransformSteps>().unwrap();
    crate::register_class::<TwoChainsSteps>().unwrap();
    crate::register_class::<FanEntrySteps>().unwrap();
    crate::register_class::<ChainedChainsSteps>().unwrap();
    crate::register_class::<JournaledCountdownSteps>().unwrap();
    crate::register_class::<BudgetOnce>().unwrap();
    crate::register_class::<ItemRetrySteps>().unwrap();
    crate::register_class::<SiblingSaga>().unwrap();
    crate::register_class::<KeyedFlow>().unwrap();
    crate::register_class::<StaticOrderFlow>().unwrap();
    crate::register_class::<KeyedSaga>().unwrap();
    crate::register_class::<KeyedBlockFlow>().unwrap();
    crate::register_class::<CappedKeyedFlow>().unwrap();
    crate::register_class::<KeyedLoopUndoAll>().unwrap();
    crate::register_class::<KeyedLoopKeepProgress>().unwrap();
    crate::register_class::<KeyedEntryLoop>().unwrap();
    crate::register_class::<TimerLoop>().unwrap();
    crate::register_class::<ChainedTimerLoop>().unwrap();

    let mut node = RunnerNode::new("test-ns".to_string());

    // The phases run strictly in this order, each draining its own instances
    // to completion before the next one starts.
    lifecycle::lifecycle(&mut node, &rx);
    lifecycle::completions(&mut node, &rx);
    lifecycle::loop_stop_and_removal(&mut node, &rx);
    lifecycle::structured_chains(&mut node, &rx);
    saga::saga_compensation(&mut node, &rx);
    lifecycle::add_relay_routing(&mut node, &rx);
    task_journal::durable_wal_and_journal(&mut node, &rx);
    task_journal::task_journal_commits(&mut node, &rx);
    task_journal::record_outputs(&mut node, &rx);
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
    durable_loop::parts_before_the_answer(&mut node, &rx);
    entrance::entrance(&mut node, &rx);
    entrance::once_waits(&mut node, &rx);
    keyed::keyed_cells(&mut node, &rx);
    timers::timers(&mut node, &rx);
    crate::cell::state_sink::set_secondary_stream(None);
}

/// Respawn `test-chain-steps` as a crash-resume carrying `journal` on the
/// bind (exactly what the AddRelay handler passes), release the restore
/// gate, and drain it to completion.
pub(super) fn resume_chain_with_journal(
    node: &RunnerNode,
    rx: &Receiver<RunnerMessage>,
    id: &str,
    journal: instance::JournalResume,
) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
    assert!(start_resumable(
        node,
        "test-chain-steps".into(),
        id.into(),
        "",
        1,
        0,
        Some(journal)
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, id, b"ignored-args"));
    let s1_id = crate::cell::flusher::encode_cell_identity(id, "steps_stage1", "");
    let (inst, _slot) =
        find_cell(node, &s1_id).expect("chain cell reachable by the restore handler");
    restored(&inst);
    drain(rx, id)
}
