//! Durability and the proc journal: what reaches the WAL, what a barrier
//! commits, and what a shipped journal skips.

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use super::resume_chain_with_journal;
use crate::runner::RunnerMessage;
use crate::workflow::*;

pub(super) fn durable_wal_and_journal(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- §5b.1/§7c.1 durable workflow actually writes WAL; opt-out writes nothing ---
    // FanWorkflow is durable-by-default; its FanProc applies a Change per item,
    // which the driver flushes as an entity_state_change frame to the sink.
    WAL_FRAMES.lock().unwrap().clear();
    assert!(node.start_workflow("test-fan-workflow".into(), "fan-wal".into(), "a,bb".into()));
    let (_o, done) = drain(rx, "fan-wal");
    assert_eq!(done.0, "ok");
    assert!(
        !WAL_FRAMES.lock().unwrap().is_empty(),
        "a durable workflow operative ships entity_state_change frames to the WAL"
    );

    // The opt-out twin does identical work with PERSISTENCE_ENABLED=false:
    // Changes drain-and-discard, nothing reaches the WAL.
    WAL_FRAMES.lock().unwrap().clear();
    assert!(node.start_workflow(
        "test-fan-optout-workflow".into(),
        "fan-optout".into(),
        "a,bb".into(),
    ));
    let (_o, done) = drain(rx, "fan-optout");
    assert_eq!(done.0, "ok");
    assert!(
        WAL_FRAMES.lock().unwrap().is_empty(),
        "an opt-out workflow operative writes nothing to the WAL"
    );

    // --- §5b.2 crash-resume gate: a durable structured workflow holds its
    // first Procedure until the Controller's Entity restores land, and its
    // Entities are reachable by the restore handler via wire identity ---
    WAL_FRAMES.lock().unwrap().clear();
    // Spawn as a "respawn" expecting one Entity restore: the driver builds
    // and registers its Entities, then parks on the resume gate — nothing
    // runs yet because no restore has landed.
    assert!(node.start_workflow_resumable(
        "test-fan-workflow".into(),
        "resume-inst".into(),
        "a,bb".into(),
        1,
        0,
        None,
    ));
    // The workflow Entities are reachable by wire identity: the stream
    // lookup only scans StreamInstance entities, so the restore handler
    // finds a workflow Entity by identity or not at all.
    let even_id =
        crate::entity::flusher::encode_entity_identity("resume-inst", "workflow_fan_op", "even");
    let (resume_instance, _slot) = node
        .find_workflow_entity(&even_id)
        .expect("a durable workflow Entity is reachable by the restore handler");
    // Release one restore, exactly as the RestoreInitState handler does after
    // applying a persisted Entity: the parked driver wakes and runs to
    // completion (resume-not-cold-start is proven end-to-end in the Elixir
    // integration test, which does the real WAL round-trip).
    resume_instance.note_restored();
    let (_o, done) = drain(rx, "resume-inst");
    assert_eq!(
        done.0, "ok",
        "the driver resumes once the expected Entity restore lands"
    );
}

pub(super) fn proc_journal_commits(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a durable structured chain commits every
    // proc boundary — one ProcCommit per stage, gated on the (fake
    // controller's) durable ack, carrying the codec-serialized handoff ---
    let _ = drain_proc_commits();
    assert!(node.start_workflow(
        "test-chain-workflow".into(),
        "chain-commit".into(),
        "x".into()
    ));
    let (outs, done) = drain(rx, "chain-commit");
    assert_eq!(done.0, "ok");
    assert_eq!(outs, vec![b"x-1-2".to_vec()]);
    let commits = drain_proc_commits();
    assert_eq!(
        commits.iter().map(|c| c.proc_idx).collect::<Vec<_>>(),
        vec![0, 1],
        "one gated ProcCommit per completed stage, in chain order"
    );
    assert!(commits.iter().all(|c| c.operative_id == "chain-commit"));
    // Stage 0 hands off on the codec'd `mid` channel (id 1: in=0, mid=1);
    // the terminal stage's output is plugin-bound (no codec) ⇒ empty.
    assert_eq!(commits[0].handoff.len(), 1);
    assert_eq!(commits[0].handoff[0].channel_id, 1);
    assert_eq!(
        postcard::from_bytes::<String>(&commits[0].handoff[0].payload).unwrap(),
        "x-1"
    );
    assert!(commits[1].handoff.is_empty());
    assert_eq!(
        commits[0].entities_ran.len(),
        1,
        "one entity ran in stage 0"
    );

    // workflow-transactional-outbox: the terminal stage's plugin output did
    // NOT go to the wire — it was staged and rode INSIDE that stage's
    // commit, keyed by the producing entity and its per-proc sequence.
    assert!(
        wire_emits_for("chain-commit").is_empty(),
        "a durable structured operative stages: nothing reaches the wire mid-proc"
    );
    assert!(
        commits[0].outbox.is_empty(),
        "stage 0's output is inter-proc (codec handoff), not plugin-bound"
    );
    assert_eq!(commits[1].outbox.len(), 1, "stage 1's plugin output staged");
    assert_eq!(commits[1].outbox[0].payload, b"x-1-2".to_vec());
    assert_eq!(commits[1].outbox[0].channel, "out");
    assert_eq!(commits[1].outbox[0].seq_in_proc, 0);
    assert!(!commits[1].outbox[0].entity_id.is_empty());
    assert!(commits.iter().all(|c| !c.compensating));

    // The run-sugar path is untouched: no procedures, no barrier to stage
    // against, so its emit goes straight to the wire (at-least-once).
    assert_eq!(
        wire_emits_for("workflow-ok"),
        vec![b"hello".to_vec()],
        "run-sugar keeps the direct wire path"
    );

    // Opt-out twin: no journal, no handshake — zero commits, and (having
    // nothing that could ever flush a staged outbox) it still emits direct.
    assert!(node.start_workflow(
        "test-fan-optout-workflow".into(),
        "optout-no-commit".into(),
        "a".into(),
    ));
    let (outs, done) = drain(rx, "optout-no-commit");
    assert_eq!(done.0, "ok");
    assert!(
        drain_proc_commits().is_empty(),
        "an opted-out class skips the proc-commit handshake entirely"
    );
    assert_eq!(
        wire_emits_for("optout-no-commit"),
        outs,
        "an opted-out class emits straight to the wire, as before"
    );
    assert_eq!(outs, vec![b"1:a".to_vec()]);
}

pub(super) fn transactional_outbox(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- workflow-transactional-outbox: a failed procedure leaks nothing ---
    assert!(node.start_workflow("test-leak-workflow".into(), "leak-1".into(), "x".into()));
    let (outs, done) = drain(rx, "leak-1");
    assert_eq!(done.0, "error");
    assert!(
        outs.is_empty() && wire_emits_for("leak-1").is_empty(),
        "the failing proc emitted before it failed; that output must never be delivered"
    );
    assert!(
        drain_proc_commits().is_empty(),
        "the proc never committed, so its staged output was never journaled"
    );

    // --- a compensate hook's undo output
    // is staged too, and rides a `compensating` commit for the procedure it
    // reverses (the forward output already flushed under its own key) ---
    assert!(node.start_workflow("test-undo-saga".into(), "undo-1".into(), "u".into()));
    let (outs, done) = drain(rx, "undo-1");
    assert_eq!(done.0, "error");
    assert!(
        wire_emits_for("undo-1").is_empty(),
        "undo output stages like forward output — never straight to the wire"
    );
    let commits = drain_proc_commits();
    let forward: Vec<_> = commits.iter().filter(|c| !c.compensating).collect();
    let undo: Vec<_> = commits.iter().filter(|c| c.compensating).collect();
    assert_eq!(forward.len(), 1, "only UndoEmit completed forward");
    assert_eq!(
        forward[0].outbox[0].payload,
        b"did:u".to_vec(),
        "the hybrid channel staged the forward copy"
    );
    assert_eq!(undo.len(), 1, "one compensated entity ⇒ one undo record");
    assert_eq!(undo[0].proc_idx, 0, "the compensated procedure's index");
    assert_eq!(undo[0].outbox.len(), 1);
    assert_eq!(undo[0].outbox[0].payload, b"undo:did:u".to_vec());
    assert_eq!(undo[0].outbox[0].channel, "mid");
    assert!(
        undo[0].handoff.is_empty(),
        "an undo record carries outbox only"
    );
    // Both the forward copy and the undo reached the plugins (the fake
    // controller flushes each commit's outbox on ack), in that order.
    assert_eq!(outs, vec![b"did:u".to_vec(), b"undo:did:u".to_vec()]);
}

pub(super) fn resume_skip(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- resume-skip: a shipped journal skips the committed stages and
    // seeds the next one from the journaled handoff ---
    let (outs, done) = resume_chain_with_journal(
        node,
        rx,
        "chain-resume",
        instance::JournalResume {
            committed_proc_idxs: vec![0],
            // The seed is the COMMITTING procedure's own handoff — keyed by
            // its proc_idx, so a fan-out's children each take their own
            // parent's edge (`workflow-branching`).
            proc_handoffs: vec![instance::EdgeHandoff {
                proc_idx: 0,
                handoff: vec![(1, postcard::to_allocvec(&"seed".to_string()).unwrap())],
            }],
            ..Default::default()
        },
    );
    assert_eq!(done.0, "ok");
    assert_eq!(
        outs,
        vec![b"seed-2".to_vec()],
        "stage 0 is skipped (no `-1` suffix, args unused); stage 1 runs \
         seeded from the journaled handoff"
    );
    assert_eq!(
        drain_proc_commits()
            .iter()
            .map(|c| c.proc_idx)
            .collect::<Vec<_>>(),
        vec![1],
        "only the resumed (uncommitted) stage re-commits"
    );

    // A journal recording EVERY stage committed resumes straight to a
    // clean completion — nothing re-runs, nothing re-commits.
    let (outs, done) = resume_chain_with_journal(
        node,
        rx,
        "chain-done",
        instance::JournalResume {
            committed_proc_idxs: vec![0, 1],
            ..Default::default()
        },
    );
    assert_eq!(done.0, "ok");
    assert!(outs.is_empty(), "a fully-committed chain re-runs nothing");
    assert!(drain_proc_commits().is_empty());
}
