//! Durability and the task journal: what reaches the WAL, what a barrier
//! commits, and what a shipped journal skips.

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use super::resume_chain_with_journal;
use crate::runner::RunnerMessage;
use crate::steps::*;

pub(super) fn durable_wal_and_journal(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- §5b.1/§7c.1 durable steps relay actually writes WAL; opt-out writes nothing ---
    // FanSteps is durable-by-default; its FanTask applies a Change per item,
    // which the driver flushes as a cell_state_change frame to the sink.
    WAL_FRAMES.lock().unwrap().clear();
    assert!(start_fed(node, "test-fan-steps", "fan-wal", b"a,bb"));
    let (_o, done) = drain(rx, "fan-wal");
    assert_eq!(done.0, "ok");
    assert!(
        !WAL_FRAMES.lock().unwrap().is_empty(),
        "a durable steps relay ships cell_state_change frames to the WAL"
    );

    // The opt-out twin does identical work with `Durable::Memory`:
    // Changes drain-and-discard, nothing reaches the WAL.
    WAL_FRAMES.lock().unwrap().clear();
    assert!(start_fed(
        node,
        "test-fan-optout-steps",
        "fan-optout",
        b"a,bb"
    ));
    let (_o, done) = drain(rx, "fan-optout");
    assert_eq!(done.0, "ok");
    assert!(
        WAL_FRAMES.lock().unwrap().is_empty(),
        "an opt-out steps relay writes nothing to the WAL"
    );

    // --- §5b.2 crash-resume gate: a durable structured steps relay holds its
    // first Tasklet until the Controller's Cell restores land, and its
    // Cells are reachable by the restore handler via wire identity ---
    WAL_FRAMES.lock().unwrap().clear();
    // Spawn as a "respawn" expecting one Cell restore: the driver builds
    // and registers its Cells, then parks on the resume gate — nothing
    // runs yet because no restore has landed.
    assert!(start_resumable(
        node,
        "test-fan-steps".into(),
        "resume-inst".into(),
        "",
        1,
        0,
        None,
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, "resume-inst", b"a,bb"));
    // The steps relay Cells are reachable by wire identity: the stream
    // lookup only scans RelayInstance cells, so the restore handler
    // finds a steps relay Cell by identity or not at all.
    let even_id = crate::cell::flusher::encode_cell_identity("resume-inst", "steps_fan_op", "even");
    let (resume_instance, _slot) = find_cell(node, &even_id)
        .expect("a durable steps Cell is reachable by the restore handler");
    // Release one restore, exactly as the RestoreInitState handler does after
    // applying a persisted Cell: the parked driver wakes and runs to
    // completion (resume-not-cold-start is proven end-to-end in the Elixir
    // integration test, which does the real WAL round-trip).
    restored(&resume_instance);
    let (_o, done) = drain(rx, "resume-inst");
    assert_eq!(
        done.0, "ok",
        "the driver resumes once the expected Cell restore lands"
    );
}

pub(super) fn task_journal_commits(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a durable structured chain commits every
    // task boundary — one TaskCommit per stage, gated on the (fake
    // controller's) durable ack, carrying the codec-serialized handoff ---
    let _ = drain_task_commits();
    assert!(start_fed(node, "test-chain-steps", "chain-commit", b"x"));
    let (outs, done) = drain(rx, "chain-commit");
    assert_eq!(done.0, "ok");
    assert_eq!(outs, vec![b"x-1-2".to_vec()]);
    let commits = drain_task_commits();
    assert_eq!(
        commits.iter().map(|c| c.task_idx).collect::<Vec<_>>(),
        vec![0, 1],
        "one gated TaskCommit per completed stage, in chain order"
    );
    assert!(commits.iter().all(|c| c.relay_id == "chain-commit"));
    // Stage 0 hands off on the codec'd `mid` channel (id 1: in=0, mid=1);
    // the terminal stage's output is plugin-bound (no codec) ⇒ empty.
    assert_eq!(commits[0].handoff.len(), 1);
    assert_eq!(commits[0].handoff[0].channel_id, 1);
    assert_eq!(
        postcard::from_bytes::<String>(&commits[0].handoff[0].payload).unwrap(),
        "x-1"
    );
    assert!(commits[1].handoff.is_empty());
    assert_eq!(commits[0].cells_ran.len(), 1, "one cell ran in stage 0");

    // The terminal stage's plugin output did NOT go to the wire: it rode that
    // stage's Cell's effect record, which the stage's commit names by seq and
    // channel, and was released only once the commit was acked.
    assert!(
        wire_emits_for("chain-commit").is_empty(),
        "a durable structured relay writes its output into its records"
    );
    assert!(
        commits[0].cells_ran[0].channels.is_empty(),
        "stage 0's output is inter-task (codec handoff), not plugin-bound"
    );
    let stage1 = &commits[1].cells_ran[0];
    assert_eq!(stage1.channels, ["out"], "stage 1's record wrote on `out`");
    let seq = stage1.seq.expect("stage 1's Cell wrote a record");
    assert_eq!(stage1.first_seq, Some(seq), "one item, one record");
    assert_eq!(released(&commits[1].cells_ran), [b"x-1-2".to_vec()]);
    assert!(commits.iter().all(|c| !c.compensating));

    // Opt-out twin: no journal, no handshake — zero commits, and (having
    // nothing that could ever flush a staged outbox) it still emits direct.
    assert!(start_fed(
        node,
        "test-fan-optout-steps",
        "optout-no-commit",
        b"a"
    ));
    let (outs, done) = drain(rx, "optout-no-commit");
    assert_eq!(done.0, "ok");
    assert!(
        drain_task_commits().is_empty(),
        "an opted-out class skips the task-commit handshake entirely"
    );
    assert_eq!(
        wire_emits_for("optout-no-commit"),
        outs,
        "an opted-out class emits straight to the wire, as before"
    );
    assert_eq!(outs, vec![b"1:a".to_vec()]);
}

pub(super) fn record_outputs(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a failed tasklet leaks nothing: no commit names its record ---
    assert!(start_fed(node, "test-leak-steps", "leak-1", b"x"));
    let (outs, done) = drain(rx, "leak-1");
    assert_eq!(done.0, "error");
    assert!(
        outs.is_empty() && wire_emits_for("leak-1").is_empty(),
        "the failing task emitted before it failed; that output must never be delivered"
    );
    assert!(
        drain_task_commits().is_empty(),
        "the task never committed, so no entry names its record"
    );

    // --- a compensate hook's undo output rides the compensating Cell's own
    // record, named by a `compensating` commit for the tasklet it reverses,
    // at a seq above the forward record's ---
    assert!(start_fed(node, "test-undo-saga", "undo-1", b"u"));
    let (outs, done) = drain(rx, "undo-1");
    assert_eq!(done.0, "error");
    assert!(
        wire_emits_for("undo-1").is_empty(),
        "undo output rides a record like forward output, never straight to the wire"
    );
    let commits = drain_task_commits();
    let forward: Vec<_> = commits.iter().filter(|c| !c.compensating).collect();
    let undo: Vec<_> = commits.iter().filter(|c| c.compensating).collect();
    assert_eq!(forward.len(), 1, "only UndoEmit completed forward");
    assert_eq!(
        released(&forward[0].cells_ran),
        [b"did:u".to_vec()],
        "the hybrid channel's forward copy rides the forward record"
    );
    assert_eq!(undo.len(), 1, "one compensated cell ⇒ one undo entry");
    assert_eq!(undo[0].task_idx, 0, "the compensated tasklet's index");
    assert_eq!(released(&undo[0].cells_ran), [b"undo:did:u".to_vec()]);
    assert_eq!(undo[0].cells_ran[0].channels, ["mid"]);
    assert_eq!(
        undo[0].cells_ran[0].cell_id,
        forward[0].cells_ran[0].cell_id
    );
    assert!(
        undo[0].cells_ran[0].seq > forward[0].cells_ran[0].seq,
        "the undo is a newer record of the same Cell"
    );
    assert!(
        undo[0].handoff.is_empty(),
        "an undo entry carries no handoff"
    );
    // Both the forward copy and the undo reached the plugins (the fake
    // controller releases each commit's records on ack), in that order.
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
            committed_task_idxs: vec![0],
            // The seed is the COMMITTING tasklet's own handoff — keyed by
            // its task_idx, so a fan-out's children each take their own
            // parent's edge (`steps-branching`).
            task_handoffs: vec![instance::EdgeHandoff {
                task_idx: 0,
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
        drain_task_commits()
            .iter()
            .map(|c| c.task_idx)
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
            committed_task_idxs: vec![0, 1],
            ..Default::default()
        },
    );
    assert_eq!(done.0, "ok");
    assert!(outs.is_empty(), "a fully-committed chain re-runs nothing");
    assert!(drain_task_commits().is_empty());
}
