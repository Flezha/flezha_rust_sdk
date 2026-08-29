//! The durable loop: one iteration per inbox item, resume at an iteration,
//! rewind, and the barrier re-send window.

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use crate::runner::RunnerMessage;
use crate::workflow::*;

pub(super) fn durable_loop(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- workflow-durable-loop: one iteration per inbox item, each ending in
    // a durable iteration commit; an idle inbox parks the instance ---
    std::env::set_var("FLEZHA_WORKFLOW_PARK_IDLE_MS", "300");
    let _ = drain_proc_commits();
    assert!(node.start_workflow("test-chat-loop".into(), "chat-1".into(), "ignored".into()));
    let chat = node
        .get_workflow("chat-1")
        .expect("the loop's instance is registered before its driver runs");
    // Two chat turns, at durable-inbox offsets 4 and 7 (the controller's
    // offsets are opaque to the driver — it only echoes them back).
    assert!(chat.push_inbox(b"hi".to_vec(), 4));
    assert!(chat.push_inbox(b"again".to_vec(), 7));

    let (outs, done) = drain(rx, "chat-1");
    assert_eq!(
        done.0, "parked",
        "an idle inbox at a committed boundary parks — NOT a terminal completion"
    );
    assert_eq!(
        outs,
        vec![b"hi-1-2".to_vec(), b"again-1-2".to_vec()],
        "one full pass of the chain per inbox item"
    );

    let iters = drain_iter_commits();
    assert_eq!(
        iters.iter().map(|c| c.iter).collect::<Vec<_>>(),
        vec![0, 1],
        "one iteration commit per turn, in order"
    );
    assert_eq!(
        iters.iter().map(|c| c.consumed_offset).collect::<Vec<_>>(),
        vec![4, 7],
        "each commit records the inbox offset its iteration consumed"
    );
    assert_eq!(
        drain_proc_commits()
            .iter()
            .map(|c| (c.iter, c.proc_idx))
            .collect::<Vec<_>>(),
        vec![(0, 0), (0, 1), (1, 0), (1, 1)],
        "every proc commit is stamped with its enclosing iteration"
    );

    let parks = drain_iter_parks();
    assert_eq!(parks.len(), 1, "exactly one park");
    assert_eq!(
        parks[0].iter, 1,
        "the park names the last COMMITTED iteration"
    );
    assert_eq!(
        parks[0].wake_deadline_ms, 0,
        "no wake_after declared ⇒ park until the next input"
    );

    // --- resume-at-iteration: a journal recording iterations 0-1 committed
    // enters iteration 2 — the committed turns never re-run ---
    let s1_id = crate::entity::flusher::encode_entity_identity("chat-2", "workflow_stage1", "");
    assert!(node.start_workflow_resumable(
        "test-chat-loop".into(),
        "chat-2".into(),
        "ignored".into(),
        1,
        0,
        Some(instance::JournalResume {
            iter: 2,
            consumed_offset: 7,
            committed_procs: vec![
                instance::CommittedProc {
                    proc_idx: 0,
                    iter: 0,
                    entities_ran: vec![s1_id.clone()],
                },
                instance::CommittedProc {
                    proc_idx: 1,
                    iter: 0,
                    entities_ran: Vec::new(),
                },
            ],
            ..Default::default()
        })
    ));
    let (chat2, _slot) = node
        .find_workflow_entity(&s1_id)
        .expect("the loop's Entity is reachable by the restore handler");
    chat2.note_restored();
    assert!(chat2.push_inbox(b"third".to_vec(), 9));

    let (outs, done) = drain(rx, "chat-2");
    assert_eq!(done.0, "parked");
    assert_eq!(
        outs,
        vec![b"third-1-2".to_vec()],
        "only the new turn runs; the journaled iterations do not re-run"
    );
    let iters = drain_iter_commits();
    assert_eq!(
        iters.iter().map(|c| c.iter).collect::<Vec<_>>(),
        vec![2],
        "the resumed run commits iteration 2 — never 0 or 1 again"
    );
    assert_eq!(iters[0].consumed_offset, 9);
}

pub(super) fn loop_saga_and_rewind(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- KeepProgress/AllOrNothing are backed by the JOURNAL — the
    // `committed` iterations a crash-resumed run rolls back come from
    // `iter_committed` records, not from memory ---
    for (factory, id, expected) in [
        // All-or-nothing: the failing turn unwinds itself AND the journaled
        // turn below it. `undo-loop:1` is the in-flight iteration (outputs
        // retained in memory); `undo-loop:0` the journal-rebuilt one.
        (
            "test-loop-saga-aon",
            "loop-saga-aon",
            vec!["undo-loop:1".to_string(), "undo-loop:0".to_string()],
        ),
        // Keep-progress: the committed turn is kept; only the in-flight one
        // unwinds.
        (
            "test-loop-saga-keep",
            "loop-saga-keep",
            vec!["undo-loop:1".to_string()],
        ),
    ] {
        let _ = drain_comp_log();
        let seg = crate::entity::flusher::encode_entity_identity(id, "loop_reserve", "");
        // Journal: iteration 0 committed both of its procedures.
        assert!(node.start_workflow_resumable(
            factory.into(),
            id.into(),
            "ignored".into(),
            1,
            0,
            Some(instance::JournalResume {
                iter: 1,
                consumed_offset: 2,
                committed_procs: vec![
                    instance::CommittedProc {
                        proc_idx: 0,
                        iter: 0,
                        entities_ran: vec![seg.clone()],
                    },
                    instance::CommittedProc {
                        proc_idx: 1,
                        iter: 0,
                        entities_ran: Vec::new(),
                    },
                ],
                ..Default::default()
            })
        ));
        let (inst, _slot) = node
            .find_workflow_entity(&seg)
            .expect("the loop saga's Entity is reachable by the restore handler");
        inst.note_restored();
        assert!(inst.push_inbox(b"boom".to_vec(), 5));

        let (_outs, done) = drain(rx, id);
        assert_eq!(done.0, "error", "the failing turn terminates the run");
        assert_eq!(
            drain_comp_log(),
            expected,
            "{factory}: rollback scope across a journaled iteration boundary"
        );
    }

    // --- workflow-loop-rewind: a journal carrying `rewind_undo` unwinds those
    // iterations — reverse iteration, reverse stage — BEFORE the loop resumes
    // forward at the rewound-to iteration ---
    let _ = drain_comp_log();
    let _ = drain_proc_commits();
    // Keep-progress deliberately: a rewind unwinds committed iterations by
    // definition, so the undo must be forced all-or-nothing whatever the
    // class's crash rollback mode says.
    let id = "loop-rewind";
    let seg = crate::entity::flusher::encode_entity_identity(id, "loop_reserve", "");
    // Rewound to iteration 0: iteration 0 stays committed, iterations 1 and 2
    // are outstanding undo, and the loop resumes at 1.
    let undo_op = |proc_idx: u32, iter: u32, entities_ran: Vec<Vec<u8>>| instance::CommittedProc {
        proc_idx,
        iter,
        entities_ran,
    };
    assert!(node.start_workflow_resumable(
        "test-loop-saga-keep".into(),
        id.into(),
        "ignored".into(),
        1,
        0,
        Some(instance::JournalResume {
            iter: 1,
            consumed_offset: 1,
            committed_procs: vec![undo_op(0, 0, vec![seg.clone()]), undo_op(1, 0, Vec::new())],
            rewind_undo: vec![
                undo_op(0, 1, vec![seg.clone()]),
                undo_op(1, 1, Vec::new()),
                undo_op(0, 2, vec![seg.clone()]),
                undo_op(1, 2, Vec::new()),
            ],
            ..Default::default()
        })
    ));
    let (inst, _slot) = node
        .find_workflow_entity(&seg)
        .expect("the loop saga's Entity is reachable by the restore handler");
    inst.note_restored();
    // The resumed turn fails, so the run reaches a terminal state (the fake
    // controller must stay in `drain` to ack the undo commits).
    assert!(inst.push_inbox(b"boom".to_vec(), 2));

    let (_outs, done) = drain(rx, id);
    assert_eq!(done.0, "error", "the resumed turn's own failure terminates");
    assert_eq!(
        drain_comp_log(),
        vec![
            "undo-loop:0".to_string(),
            "undo-loop:0".to_string(),
            "undo-loop:1".to_string()
        ],
        "both rewound iterations are undone first (journal-rebuilt ⇒ 0 retained \
         outputs), and only then does the resumed turn run and roll itself back \
         (in-flight ⇒ 1 retained output)"
    );
    let undo: Vec<(u32, u32)> = drain_proc_commits()
        .iter()
        .filter(|c| c.compensating)
        .map(|c| (c.proc_idx, c.iter))
        .collect();
    assert_eq!(
        undo,
        vec![(1, 2), (0, 2), (1, 1), (0, 1)],
        "reverse iteration, reverse stage — and the hookless terminal proc still \
         journals its (empty) `compensated` record, which is what drains it out \
         of the next fold's outstanding work"
    );

    std::env::remove_var("FLEZHA_WORKFLOW_PARK_IDLE_MS");
}

pub(super) fn barrier_resend(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- workflow-durable-path-throughput: an unacked barrier is re-sent
    // under the same reference_id, and the controller journals it once ---
    std::env::set_var("FLEZHA_WORKFLOW_ACK_TIMEOUT_MS", "300");
    let _ = drain_proc_commits();
    let _ = drain_barrier_sends();

    // The first ProcCommit's ack is lost: the re-send continues the chain.
    lose_acks("resend-1", 1);
    assert!(node.start_workflow("test-resend-chain".into(), "resend-1".into(), "x".into()));
    let (outs, done) = drain(rx, "resend-1");
    assert_eq!(
        (done.0.as_str(), done.1.as_str()),
        ("ok", ""),
        "a lost ack delays the chain, it does not end it"
    );
    assert_eq!(
        outs,
        vec![b"x-1-2".to_vec()],
        "the journaled outbox reaches the plugins once"
    );
    let sends = barrier_sends_of("resend-1");
    assert_eq!(sends.len(), 3, "stage 0 twice, stage 1 once");
    assert_eq!(
        sends[0], sends[1],
        "the re-send carries the same reference_id"
    );
    assert_ne!(sends[1], sends[2]);
    assert_eq!(
        drain_proc_commits()
            .iter()
            .map(|c| c.proc_idx)
            .collect::<Vec<_>>(),
        vec![0, 1],
        "each barrier is journaled once"
    );

    // No ack ever arrives: the bounded ladder fails the run.
    lose_acks("resend-2", usize::MAX);
    assert!(node.start_workflow("test-resend-chain".into(), "resend-2".into(), "y".into()));
    let (_outs, done) = drain(rx, "resend-2");
    assert_eq!(done.0, "error");
    assert!(
        done.1.contains("ack timed out after 3 attempt(s)"),
        "the exhausted ladder is the terminal error: {}",
        done.1
    );
    let sends = barrier_sends_of("resend-2");
    assert_eq!(sends.len(), 3, "the default ladder sends three times");
    assert!(sends.iter().all(|r| r == &sends[0]));
    let _ = drain_proc_commits();

    // A re-sent IterCommit is idempotent on the controller: the turn's
    // barriers each lose their first ack, and each is journaled once.
    std::env::set_var("FLEZHA_WORKFLOW_PARK_IDLE_MS", "300");
    let _ = drain_iter_commits();
    let _ = drain_iter_parks();
    lose_acks("resend-3", 3);
    assert!(node.start_workflow(
        "test-resend-loop".into(),
        "resend-3".into(),
        "ignored".into()
    ));
    let resend_loop = node
        .get_workflow("resend-3")
        .expect("the loop's instance is registered before its driver runs");
    assert!(resend_loop.push_inbox(b"hi".to_vec(), 4));
    let (outs, done) = drain(rx, "resend-3");
    assert_eq!(done.0, "parked");
    assert_eq!(outs, vec![b"hi-1-2".to_vec()]);
    assert_eq!(
        drain_iter_commits()
            .iter()
            .map(|c| (c.iter, c.consumed_offset))
            .collect::<Vec<_>>(),
        vec![(0, 4)],
        "one iter_committed for two sends"
    );
    assert_eq!(
        drain_proc_commits()
            .iter()
            .map(|c| (c.iter, c.proc_idx))
            .collect::<Vec<_>>(),
        vec![(0, 0), (0, 1)]
    );
    let sends = barrier_sends_of("resend-3");
    assert_eq!(sends.len(), 6, "every barrier of the turn was sent twice");
    assert!(
        sends.chunks(2).all(|pair| pair[0] == pair[1]),
        "each re-send carries its barrier's own reference_id"
    );
    std::env::remove_var("FLEZHA_WORKFLOW_ACK_TIMEOUT_MS");
}
