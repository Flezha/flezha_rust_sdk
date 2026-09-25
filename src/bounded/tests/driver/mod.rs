//! The one driver test. Its phases are ordered functions, not separate
//! `#[test]`s: they share the reconnect trigger, the global sink lock and
//! the class registry this function installs, so the order is a fact only
//! while one test owns it.

use std::sync::Arc;

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::fake_controller::*;
use super::fixtures::*;
use crate::bounded::*;
use crate::runner::{install_reconnect_trigger, RunnerMessage};

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
// control requests and `EmitBoundedCompleted` messages the driver routes
// through it; the shared runner-state lock keeps that install exclusive
// against the other tests that install one. Scenarios run strictly
// sequentially (each drained to its completion before the next) so a
// single shared channel stays unambiguous.
#[test]
fn bounded_driver_scenarios() {
    let _runner_guard = crate::runner::env_test_guard();
    let (tx, rx) = crossbeam::channel::unbounded::<RunnerMessage>();
    install_reconnect_trigger(tx);

    // Durable bounded operators in this test ship WAL frames through the
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
    // rode, which for a fed or re-seeded bounded operator is the secondary.
    crate::cell::state_sink::set_secondary_stream(Some(capturing_wire()));

    crate::register_class::<crate::BoundedKind, EmitBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, FailBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, PanicBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, CompleteBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, LoopBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, TimedBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, TwoCompletions>().unwrap();
    crate::register_class::<crate::BoundedKind, KeyedCompletions>().unwrap();
    crate::register_class::<crate::BoundedKind, CompleteThenFail>().unwrap();
    crate::register_class::<crate::BoundedKind, CompletingChat>().unwrap();
    crate::register_class::<crate::BoundedKind, ChainBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, TreeBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, TreeSaga>().unwrap();
    crate::register_class::<crate::BoundedKind, FanBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, FanOptOutBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, HybridBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, SagaBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, KeepProgressSaga>().unwrap();
    crate::register_class::<crate::BoundedKind, CleanSaga>().unwrap();
    crate::register_class::<crate::BoundedKind, FanSaga>().unwrap();
    crate::register_class::<crate::BoundedKind, StopSaga>().unwrap();
    crate::register_class::<crate::BoundedKind, ResumeSaga>().unwrap();
    crate::register_class::<crate::BoundedKind, LeakBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, UndoSaga>().unwrap();
    crate::register_class::<crate::BoundedKind, ChatLoop>().unwrap();
    crate::register_class::<crate::BoundedKind, LoopSagaAllOrNothing>().unwrap();
    crate::register_class::<crate::BoundedKind, LoopSagaKeepProgress>().unwrap();
    crate::register_class::<crate::BoundedKind, RetryBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, RetryFailFastBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, RetrySlowBackoffBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, StepProbeBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, DiamondBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, DiamondSaga>().unwrap();
    crate::register_class::<crate::BoundedKind, MergeBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, PanickingMergeBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, PanickingTransformBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, TwoChainsBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, FanEntryBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, ChainedChainsBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, JournaledCountdownBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, BudgetOnce>().unwrap();
    crate::register_class::<crate::BoundedKind, ItemRetryBounded>().unwrap();
    crate::register_class::<crate::BoundedKind, SiblingSaga>().unwrap();
    crate::register_class::<crate::BoundedKind, KeyedFlow>().unwrap();
    crate::register_class::<crate::BoundedKind, StaticOrderFlow>().unwrap();
    crate::register_class::<crate::BoundedKind, KeyedSaga>().unwrap();
    crate::register_class::<crate::BoundedKind, KeyedBlockFlow>().unwrap();
    crate::register_class::<crate::BoundedKind, CappedKeyedFlow>().unwrap();
    crate::register_class::<crate::BoundedKind, KeyedLoopAllOrNothing>().unwrap();
    crate::register_class::<crate::BoundedKind, KeyedLoopKeepProgress>().unwrap();
    crate::register_class::<crate::BoundedKind, KeyedEntryLoop>().unwrap();
    crate::register_class::<crate::BoundedKind, TimerLoop>().unwrap();
    crate::register_class::<crate::BoundedKind, ChainedTimerLoop>().unwrap();

    let mut node = RunnerNode::new("test-ns".to_string());

    // The phases run strictly in this order, each draining its own instances
    // to completion before the next one starts.
    lifecycle::lifecycle(&mut node, &rx);
    lifecycle::completions(&mut node, &rx);
    lifecycle::loop_stop_and_removal(&mut node, &rx);
    lifecycle::structured_chains(&mut node, &rx);
    saga::saga_compensation(&mut node, &rx);
    lifecycle::add_operator_routing(&mut node, &rx);
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
    entrance::entrance(&mut node, &rx);
    entrance::once_waits(&mut node, &rx);
    keyed::keyed_cells(&mut node, &rx);
    timers::timers(&mut node, &rx);
    crate::cell::state_sink::set_secondary_stream(None);
}

/// Respawn `test-chain-bounded` as a crash-resume carrying `journal` on the
/// bind (exactly what the AddOperator handler passes), release the restore
/// gate, and drain it to completion.
pub(super) fn resume_chain_with_journal(
    node: &RunnerNode,
    rx: &Receiver<RunnerMessage>,
    id: &str,
    journal: instance::JournalResume,
) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
    assert!(start_resumable(
        node,
        "test-chain-bounded".into(),
        id.into(),
        "",
        1,
        0,
        Some(journal)
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, id, b"ignored-args"));
    let s1_id = crate::cell::flusher::encode_cell_identity(id, "bounded_stage1", "");
    let (inst, _slot) =
        find_cell(node, &s1_id).expect("chain cell reachable by the restore handler");
    restored(&inst);
    drain(rx, id)
}
