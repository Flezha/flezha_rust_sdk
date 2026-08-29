//! Saga compensation: what a failed step unwinds, and what a crash-resume
//! finishes.

use std::sync::atomic::Ordering;

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use crate::runner::RunnerMessage;

pub(super) fn saga_compensation(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- §7b.1 saga: a failed step rolls back completed steps in reverse ---
    let _ = drain_comp_log();
    assert!(node.start_workflow("test-saga-workflow".into(), "saga-1".into(), "X".into()));
    let (_o, done) = drain(rx, "saga-1");
    assert_eq!(done.0, "error", "a failed terminal step ⇒ status error");
    assert!(done.1.contains("ship failed"));
    // reserve → charge completed; ship failed (not compensated). Undo runs
    // reverse: charge then reserve, each receiving its own output.
    assert_eq!(
        drain_comp_log(),
        vec![
            "undo-charge:charged:reserved:X".to_string(),
            "undo-reserve:reserved:X".to_string(),
        ],
    );

    // --- §7b.2 all-or-nothing: keep-progress unwinds an identical single
    // pass (the mode only diverges with committed iterations) — proves
    // rollback_mode() reaches the driver ---
    let _ = drain_comp_log();
    assert!(node.start_workflow(
        "test-keep-progress-saga".into(),
        "saga-kp".into(),
        "Y".into()
    ));
    let (_o, done) = drain(rx, "saga-kp");
    assert_eq!(done.0, "error");
    assert_eq!(
        drain_comp_log(),
        vec![
            "undo-charge:charged:reserved:Y".to_string(),
            "undo-reserve:reserved:Y".to_string(),
        ],
    );

    // --- §7b.1 clean completion does NOT compensate ---
    let _ = drain_comp_log();
    assert!(node.start_workflow("test-clean-saga".into(), "saga-ok".into(), "Z".into()));
    let (clean_outs, done) = drain(rx, "saga-ok");
    assert_eq!(done.0, "ok");
    assert_eq!(clean_outs, vec![b"echo:charged:reserved:Z".to_vec()]);
    assert!(
        drain_comp_log().is_empty(),
        "a clean ok must not compensate"
    );

    // --- §7b.1 per-entity compensation: once per entity that ran;
    // hookless SplitProc is skipped ---
    let _ = drain_comp_log();
    assert!(node.start_workflow("test-fan-saga".into(), "fan-saga".into(), "a,bb,ccc".into()));
    let (_o, done) = drain(rx, "fan-saga");
    assert_eq!(done.0, "error");
    let mut fan_comp = drain_comp_log();
    fan_comp.sort();
    // odd entity ran "a","ccc"; even ran "bb"; the hook fires once per
    // entity with that entity's own outputs. SplitProc (no hook) is skipped.
    assert_eq!(
        fan_comp,
        vec![
            "undo-fan:even:[even:bb]".to_string(),
            "undo-fan:odd:[odd:a,odd:ccc]".to_string(),
        ],
    );

    // --- §7b.1 forced-stop after partial progress compensates ---
    let _ = drain_comp_log();
    STAGE0_RAN.store(false, Ordering::SeqCst);
    STAGE0_RELEASE.store(false, Ordering::SeqCst);
    assert!(node.start_workflow("test-stop-saga".into(), "stop-saga".into(), "W".into()));
    while !STAGE0_RAN.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
    node.get_workflow("stop-saga")
        .expect("live instance accepts cancel")
        .request_cancel();
    STAGE0_RELEASE.store(true, Ordering::SeqCst);
    let (_o, done) = drain(rx, "stop-saga");
    assert_eq!(done.0, "cancelled", "cooperative stop ⇒ cancelled");
    // StopReserve completed before the stop landed; Echo never ran.
    assert_eq!(
        drain_comp_log(),
        vec!["undo-stop-reserve:reserved:W".to_string()],
    );
}

pub(super) fn saga_crash_resume(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- §4.3 durable saga records: a fresh abnormal run emits one
    // compensation_intent per completed compensable entity and one
    // compensation_done per compensated entity ---
    use crate::entity::state_sink::{
        COMPENSATION_DONE_PAYLOAD_ID as DONE, COMPENSATION_INTENT_PAYLOAD_ID as INTENT,
    };
    WAL_FRAMES.lock().unwrap().clear();
    let _ = drain_comp_log();
    // reserve + charge complete (both compensable ⇒ 2 intents); ship fails ⇒
    // both roll back (2 dones). Hookless ship_fails never completed ⇒ no record.
    assert!(node.start_workflow("test-saga-workflow".into(), "saga-rec".into(), "42".into()));
    let (_o, done) = drain(rx, "saga-rec");
    assert_eq!(done.0, "error");
    assert_eq!(
        count_wal_frames(INTENT),
        2,
        "one intent per completed compensable entity"
    );
    assert_eq!(count_wal_frames(DONE), 2, "one done per compensated entity");

    // --- §4.3 crash-resume finishes the remaining hooks, skipping the ones
    // already recorded done, and does NOT re-run the forward chain ---
    WAL_FRAMES.lock().unwrap().clear();
    let _ = drain_comp_log();
    // Respawn a durable saga expecting one restore: the driver parks on the
    // gate while we seed the markers the RestoreInitState handler would carry.
    assert!(node.start_workflow_resumable(
        "test-resume-saga".into(),
        "resume-saga".into(),
        "x".into(),
        1,
        0,
        None,
    ));
    let a_id = crate::entity::flusher::encode_entity_identity("resume-saga", "resume_undo_a", "");
    let b_id = crate::entity::flusher::encode_entity_identity("resume-saga", "resume_undo_b", "");
    let (saga_inst, _slot) = node
        .find_workflow_entity(&a_id)
        .expect("a durable workflow saga Entity is reachable by the restore handler");
    // Original run: A + B both completed (intent); B was already undone before
    // the crash (done). Resume must undo A and SKIP B — in reverse — without
    // re-running the forward chain (so no fresh intents, no `a:`/`b:` output).
    saga_inst.note_compensation_markers(&a_id, true, false);
    saga_inst.note_compensation_markers(&b_id, true, true);
    saga_inst.note_restored();
    let (outs, done) = drain(rx, "resume-saga");
    assert_eq!(
        done.0, "error",
        "a resumed compensation terminates with error"
    );
    assert!(
        outs.is_empty(),
        "compensation-resume does not re-run forward procs"
    );
    assert_eq!(
        drain_comp_log(),
        vec!["undo-A".to_string()],
        "A is undone; B (already done) is skipped"
    );
    assert_eq!(
        count_wal_frames(DONE),
        1,
        "only A emits a fresh compensation_done; B was skipped"
    );
    assert_eq!(
        count_wal_frames(INTENT),
        0,
        "a compensation-resume records no new intent (forward chain not re-run)"
    );
}
