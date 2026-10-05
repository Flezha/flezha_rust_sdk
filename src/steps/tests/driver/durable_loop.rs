//! The durable loop: one iteration per inbox item, resume at an iteration,
//! rewind, and the barrier re-send window.

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use crate::runner::RunnerMessage;
use crate::steps::*;

pub(super) fn durable_loop(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- steps-durable-loop: one iteration per inbox item, each ending in
    // a durable iteration commit; an idle inbox parks the instance ---
    let _ = drain_task_commits();
    assert!(start(node, "test-chat-loop".into(), "chat-1".into(), "")
        .expect("init_values is a JSON object"));
    let chat =
        instance(node, "chat-1").expect("the loop's instance is registered before its driver runs");
    // Two chat turns, at input-log offsets 4 and 7 (the controller's
    // offsets are opaque to the driver — it only echoes them back).
    feed_at(&chat, b"hi", 4);
    feed_at(&chat, b"again", 7);

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
        vec![Some(4), Some(7)],
        "each commit records the input-log offset its iteration consumed"
    );
    assert_eq!(
        drain_task_commits()
            .iter()
            .map(|c| (c.iter, c.task_idx))
            .collect::<Vec<_>>(),
        vec![(0, 0), (0, 1), (1, 0), (1, 1)],
        "every task commit is stamped with its enclosing iteration"
    );

    let parks = drain_iter_parks();
    assert_eq!(parks.len(), 1, "exactly one park");
    assert_eq!(
        parks[0].iter, 1,
        "the park names the last COMMITTED iteration"
    );
    assert_eq!(
        parks[0].wake_deadline_ms, 0,
        "no timer pending ⇒ park until the next input"
    );

    // --- resume-at-iteration: a journal recording iterations 0-1 committed
    // enters iteration 2 — the committed turns never re-run ---
    let s1_id = crate::cell::flusher::encode_cell_identity("chat-2", "steps_stage1", "");
    assert!(start_resumable(
        node,
        "test-chat-loop".into(),
        "chat-2".into(),
        "",
        1,
        0,
        Some(instance::JournalResume {
            iter: 2,
            committed_tasks: vec![
                instance::CommittedTask {
                    task_idx: 0,
                    iter: 0,
                    cells_ran: vec![s1_id.clone()],
                    completion: None,
                    trigger: None,
                },
                instance::CommittedTask {
                    task_idx: 1,
                    iter: 0,
                    cells_ran: Vec::new(),
                    completion: None,
                    trigger: None,
                },
            ],
            ..Default::default()
        })
    )
    .expect("init_values is a JSON object"));
    let (chat2, _slot) =
        find_cell(node, &s1_id).expect("the loop's Cell is reachable by the restore handler");
    restored(&chat2);
    feed_at(&chat2, b"third", 9);

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
    assert_eq!(iters[0].consumed_offset, Some(9));
}

pub(super) fn loop_saga_and_rewind(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- KeepProgress/UndoAll are backed by the JOURNAL — the
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
        let seg = crate::cell::flusher::encode_cell_identity(id, "loop_reserve", "");
        // Journal: iteration 0 committed both of its tasklets.
        assert!(start_resumable(
            node,
            factory.into(),
            id.into(),
            "",
            1,
            0,
            Some(instance::JournalResume {
                iter: 1,
                committed_tasks: vec![
                    instance::CommittedTask {
                        task_idx: 0,
                        iter: 0,
                        cells_ran: vec![seg.clone()],
                        completion: None,
                        trigger: None,
                    },
                    instance::CommittedTask {
                        task_idx: 1,
                        iter: 0,
                        cells_ran: Vec::new(),
                        completion: None,
                        trigger: None,
                    },
                ],
                ..Default::default()
            })
        )
        .expect("init_values is a JSON object"));
        let (inst, _slot) = find_cell(node, &seg)
            .expect("the loop saga's Cell is reachable by the restore handler");
        restored(&inst);
        feed_at(&inst, b"boom", 5);

        let (_outs, done) = drain(rx, id);
        assert_eq!(done.0, "error", "the failing turn terminates the run");
        assert_eq!(
            drain_comp_log(),
            expected,
            "{factory}: rollback scope across a journaled iteration boundary"
        );
    }

    // --- steps-loop-rewind: a journal carrying `rewind_undo` unwinds those
    // iterations — reverse iteration, reverse stage — BEFORE the loop resumes
    // forward at the rewound-to iteration ---
    let _ = drain_comp_log();
    let _ = drain_task_commits();
    // Keep-progress deliberately: a rewind unwinds committed iterations by
    // definition, so the undo must be forced all-or-nothing whatever the
    // class's crash rollback mode says.
    let id = "loop-rewind";
    let seg = crate::cell::flusher::encode_cell_identity(id, "loop_reserve", "");
    // Rewound to iteration 0: iteration 0 stays committed, iterations 1 and 2
    // are outstanding undo, and the loop resumes at 1.
    let undo_op = |task_idx: u32, iter: u32, cells_ran: Vec<Vec<u8>>| instance::CommittedTask {
        task_idx,
        iter,
        cells_ran,
        completion: None,
        trigger: None,
    };
    assert!(start_resumable(
        node,
        "test-loop-saga-keep".into(),
        id.into(),
        "",
        1,
        0,
        Some(instance::JournalResume {
            iter: 1,
            committed_tasks: vec![undo_op(0, 0, vec![seg.clone()]), undo_op(1, 0, Vec::new())],
            rewind_undo: vec![
                undo_op(0, 1, vec![seg.clone()]),
                undo_op(1, 1, Vec::new()),
                undo_op(0, 2, vec![seg.clone()]),
                undo_op(1, 2, Vec::new()),
            ],
            ..Default::default()
        })
    )
    .expect("init_values is a JSON object"));
    let (inst, _slot) =
        find_cell(node, &seg).expect("the loop saga's Cell is reachable by the restore handler");
    restored(&inst);
    // The resumed turn fails, so the run reaches a terminal state (the fake
    // controller must stay in `drain` to ack the undo commits).
    feed_at(&inst, b"boom", 2);

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
    let undo: Vec<(u32, u32)> = drain_task_commits()
        .iter()
        .filter(|c| c.compensating)
        .map(|c| (c.task_idx, c.iter))
        .collect();
    assert_eq!(
        undo,
        vec![(1, 2), (0, 2), (1, 1), (0, 1)],
        "reverse iteration, reverse stage — and the hookless terminal task still \
         journals its (empty) `compensated` record, which is what drains it out \
         of the next fold's outstanding work"
    );
}

pub(super) fn loop_completion(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- steps-durable-loop: a tasklet's complete ends a durable loop at
    // its commit, journaled on both barriers, and the loop never parks ---
    let _ = drain_task_commits();
    let _ = drain_iter_commits();
    let _ = drain_iter_parks();
    assert!(
        start(node, "test-completing-chat".into(), "bye-1".into(), "")
            .expect("init_values is a JSON object")
    );
    let chat =
        instance(node, "bye-1").expect("the loop's instance is registered before its driver runs");
    feed_at(&chat, b"hi", 4);
    feed_at(&chat, b"bye", 5);

    let (outs, done) = drain(rx, "bye-1");
    assert_eq!(
        (done.0.as_str(), done.2.as_slice()),
        ("ok", &b"farewell"[..]),
        "the completing turn ends the instance with its result"
    );
    assert_eq!(
        outs,
        vec![b"hi-2".to_vec(), b"bye-2".to_vec()],
        "the rest of the completing iteration still runs"
    );
    assert!(
        drain_iter_parks().is_empty(),
        "a completed loop never parks"
    );
    assert_eq!(
        drain_iter_commits()
            .iter()
            .map(|c| (c.iter, c.completes, c.result.clone()))
            .collect::<Vec<_>>(),
        vec![(0, false, Vec::new()), (1, true, b"farewell".to_vec())]
    );
    assert_eq!(
        drain_task_commits()
            .iter()
            .map(|c| (c.iter, c.task_idx, c.completes))
            .collect::<Vec<_>>(),
        vec![(0, 0, false), (0, 1, false), (1, 0, true), (1, 1, false)],
        "the completing tasklet's own commit carries it too"
    );

    // --- resume from a completing iter_committed: ends at once, runs nothing ---
    let seg = crate::cell::flusher::encode_cell_identity("bye-2", "steps_chat_turn", "");
    assert!(start_resumable(
        node,
        "test-completing-chat".into(),
        "bye-2".into(),
        "",
        1,
        0,
        Some(instance::JournalResume {
            iter: 2,
            completion: Some(b"farewell".to_vec()),
            ..Default::default()
        })
    )
    .expect("init_values is a JSON object"));
    let (inst, _slot) =
        find_cell(node, &seg).expect("the loop's Cell is reachable by the restore handler");
    restored(&inst);
    let (outs, done) = drain(rx, "bye-2");
    assert_eq!(
        (done.0.as_str(), done.2.as_slice()),
        ("ok", &b"farewell"[..])
    );
    assert!(outs.is_empty());
    assert!(drain_task_commits().is_empty() && drain_iter_commits().is_empty());

    // --- a crash after the completing tasklet committed and before the
    // iteration did: the skipped tasklet's journaled completion still ends
    // the resumed iteration ---
    let id = "bye-3";
    let seg = crate::cell::flusher::encode_cell_identity(id, "steps_chat_turn", "");
    // Channel ids follow declaration order: `in` = 0, `mid` = 1.
    assert!(start_resumable(
        node,
        "test-completing-chat".into(),
        id.into(),
        "",
        1,
        0,
        Some(instance::JournalResume {
            iter: 1,
            committed_task_idxs: vec![0],
            task_handoffs: vec![instance::EdgeHandoff {
                task_idx: 0,
                handoff: vec![(1, postcard::to_allocvec(&"bye".to_string()).unwrap())],
            }],
            committed_tasks: vec![instance::CommittedTask {
                task_idx: 0,
                iter: 1,
                cells_ran: vec![seg.clone()],
                completion: Some(b"farewell".to_vec()),
                trigger: None,
            }],
            ..Default::default()
        })
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, id, b"ignored"));
    let (inst, _slot) =
        find_cell(node, &seg).expect("the loop's Cell is reachable by the restore handler");
    restored(&inst);
    let (outs, done) = drain(rx, id);
    assert_eq!(
        (done.0.as_str(), done.2.as_slice()),
        ("ok", &b"farewell"[..]),
        "the resumed iteration completes instead of waiting for input"
    );
    assert_eq!(
        outs,
        vec![b"bye-2".to_vec()],
        "only the uncommitted stage ran"
    );
    assert_eq!(
        drain_iter_commits()
            .iter()
            .map(|c| (c.iter, c.completes, c.result.clone()))
            .collect::<Vec<_>>(),
        vec![(1, true, b"farewell".to_vec())]
    );
    assert!(drain_iter_parks().is_empty());
    let _ = drain_task_commits();
}

pub(super) fn barrier_resend(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- steps-durable-path-throughput: an unacked barrier is re-sent
    // under the same reference_id, and the controller journals it once ---
    std::env::set_var("FLEZHA_STEPS_ACK_TIMEOUT_MS", "300");
    let _ = drain_task_commits();
    let _ = drain_barrier_sends();

    // The first TaskCommit's ack is lost: the re-send continues the chain.
    lose_acks("resend-1", 1);
    assert!(start_fed(node, "test-chain-steps", "resend-1", b"x"));
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
        drain_task_commits()
            .iter()
            .map(|c| c.task_idx)
            .collect::<Vec<_>>(),
        vec![0, 1],
        "each barrier is journaled once"
    );

    // No ack ever arrives: the bounded ladder fails the run.
    lose_acks("resend-2", usize::MAX);
    assert!(start_fed(node, "test-chain-steps", "resend-2", b"y"));
    let (_outs, done) = drain(rx, "resend-2");
    assert_eq!(done.0, "error");
    assert!(
        done.1.contains("ack timed out after 3 attempt(s)"),
        "the exhausted ladder is the terminal error: {}",
        done.1
    );
    let sends = barrier_sends_of("resend-2");
    assert_eq!(sends.len(), 3, "the constant ladder sends three times");
    assert!(sends.iter().all(|r| r == &sends[0]));
    let _ = drain_task_commits();

    // A re-sent IterCommit is idempotent on the controller: the turn's
    // barriers each lose their first ack, and each is journaled once.
    let _ = drain_iter_commits();
    let _ = drain_iter_parks();
    lose_acks("resend-3", 3);
    assert!(start(node, "test-chat-loop".into(), "resend-3".into(), "")
        .expect("init_values is a JSON object"));
    let resend_loop = instance(node, "resend-3")
        .expect("the loop's instance is registered before its driver runs");
    feed_at(&resend_loop, b"hi", 4);
    let (outs, done) = drain(rx, "resend-3");
    assert_eq!(done.0, "parked");
    assert_eq!(outs, vec![b"hi-1-2".to_vec()]);
    assert_eq!(
        drain_iter_commits()
            .iter()
            .map(|c| (c.iter, c.consumed_offset))
            .collect::<Vec<_>>(),
        vec![(0, Some(4))],
        "one iter_committed for two sends"
    );
    assert_eq!(
        drain_task_commits()
            .iter()
            .map(|c| (c.iter, c.task_idx))
            .collect::<Vec<_>>(),
        vec![(0, 0), (0, 1)]
    );
    let sends = barrier_sends_of("resend-3");
    assert_eq!(sends.len(), 6, "every barrier of the turn was sent twice");
    assert!(
        sends.chunks(2).all(|pair| pair[0] == pair[1]),
        "each re-send carries its barrier's own reference_id"
    );
    std::env::remove_var("FLEZHA_STEPS_ACK_TIMEOUT_MS");
}

pub(super) fn parts_before_the_answer(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- output-parts: a `per_input` stage's parts leave on the runner's
    // connection while it runs, numbered per turn, and the answer is still
    // released only at the turn's commit ---
    let (stream, mut observer) = flz_core::bench_support::stream_pair();
    observer
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .unwrap();
    crate::cell::state_sink::set_secondary_stream(Some(std::sync::Arc::clone(&stream)));
    assert!(start(node, "test-part-loop".into(), "part-1".into(), "")
        .expect("init_values is a JSON object"));
    for turn in ["x", "y"] {
        node.relay_dispatch_handle().dispatch(
            "part-1",
            turn.as_bytes().to_vec(),
            &Default::default(),
            std::sync::Arc::clone(&stream),
        );
    }
    // The next turn runs once this one's commit is acked, which `drain` does.
    let reader = std::thread::spawn(move || {
        std::iter::from_fn(|| {
            flz_transport::service::payloader::extract_payload(&mut observer).ok()
        })
        .filter(|(payload_id, _, _)| payload_id == flz_core::part::PART_PAYLOAD_ID)
        .take(4)
        .map(|(_, payload, meta)| {
            assert_eq!(meta["channel"], "answer");
            let text = String::from_utf8(payload).unwrap();
            format!("{text}:{}:{}", meta["flz.part"], meta["flz.part_attempt"])
        })
        .collect::<Vec<_>>()
    });
    let (outs, done) = drain(rx, "part-1");
    assert_eq!(
        reader.join().unwrap(),
        ["x-a:0:1", "x-b:1:1", "y-a:0:1", "y-b:1:1"]
    );
    assert_eq!(done.0, "parked");
    assert_eq!(outs, vec![b"done:x".to_vec(), b"done:y".to_vec()]);
    let _ = drain_task_commits();
    let _ = drain_iter_commits();
    let _ = drain_iter_parks();
    crate::cell::state_sink::set_secondary_stream(Some(capturing_wire()));
}
