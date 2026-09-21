//! Lifecycle and structured-chain scenarios: a single pass, a failure, a
//! completion and its tie-break, an inbox loop a tasklet ends, a stop, and
//! the `AddOperative` kind routing.

use std::sync::atomic::Ordering;

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use crate::command::CommandAction;
use crate::pb::{AddOperative, WorkflowStop};
use crate::runner::RunnerMessage;

pub(super) fn lifecycle(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a once pass: args echoed, status "ok" ---
    EMIT_RUNS.store(0, Ordering::SeqCst);
    assert!(node.start_workflow(
        "test-emit-workflow".into(),
        "workflow-ok".into(),
        "hello".into()
    ));
    let (outs, done) = drain(rx, "workflow-ok");
    assert_eq!(
        EMIT_RUNS.load(Ordering::SeqCst),
        1,
        "the tasklet runs exactly once"
    );
    assert_eq!(outs, vec![b"hello".to_vec()], "args echoed as output");
    assert_eq!((done.0.as_str(), done.1.as_str()), ("ok", ""));

    // Instance stays registered; a duplicate start is idempotent (no re-run).
    assert!(node.has_operative("workflow-ok"));
    assert!(
        node.get_stream("workflow-ok").is_none(),
        "a workflow is not a stream operative"
    );
    assert!(node.start_workflow(
        "test-emit-workflow".into(),
        "workflow-ok".into(),
        "hello".into()
    ));
    assert_eq!(
        EMIT_RUNS.load(Ordering::SeqCst),
        1,
        "duplicate start must not re-run"
    );

    // --- a non-retryable failure: terminal on its first run ---
    FAIL_RUNS.store(0, Ordering::SeqCst);
    assert!(node.start_workflow(
        "test-fail-workflow".into(),
        "workflow-err".into(),
        "".into()
    ));
    let (_o, done) = drain(rx, "workflow-err");
    assert_eq!(done.0, "error");
    assert!(done.1.contains("boom"));
    assert_eq!(
        FAIL_RUNS.load(Ordering::SeqCst),
        1,
        "a non-retryable error skips the retry ladder"
    );

    // --- a panic caught and reported as "error" ---
    assert!(node.start_workflow(
        "test-panic-workflow".into(),
        "workflow-panic".into(),
        "".into()
    ));
    let (_o, done) = drain(rx, "workflow-panic");
    assert_eq!(done.0, "error");
    assert!(done.1.contains("kaboom"));
}

pub(super) fn completions(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a tasklet's complete carries the result, journaled on its commit ---
    let _ = drain_task_commits();
    assert!(node.start_workflow(
        "test-complete-workflow".into(),
        "workflow-c".into(),
        "z".into()
    ));
    let (_o, done) = drain(rx, "workflow-c");
    assert_eq!(done.0, "ok");
    assert_eq!(
        done.2,
        b"result:z".to_vec(),
        "complete() result reaches WorkflowCompleted"
    );
    assert_eq!(
        completions_of(drain_task_commits(), "workflow-c"),
        vec![Some(b"result:z".to_vec())],
        "the completing tasklet's commit carries its result"
    );

    // --- two completing tasklets: the later one in commit order wins ---
    assert!(node.start_workflow(
        "test-two-completions".into(),
        "workflow-two".into(),
        "x".into()
    ));
    let (_o, done) = drain(rx, "workflow-two");
    assert_eq!((done.0.as_str(), done.2.as_slice()), ("ok", &b"second"[..]));
    assert_eq!(
        completions_of(drain_task_commits(), "workflow-two"),
        vec![Some(b"first".to_vec()), Some(b"second".to_vec())],
        "each tasklet journals its own completion"
    );

    // --- two cells of one tasklet: the greatest cell key wins ---
    assert!(node.start_workflow(
        "test-keyed-completions".into(),
        "workflow-keyed".into(),
        "x".into()
    ));
    let (_o, done) = drain(rx, "workflow-keyed");
    assert_eq!((done.0.as_str(), done.2.as_slice()), ("ok", &b"b"[..]));
    assert_eq!(
        completions_of(drain_task_commits(), "workflow-keyed"),
        vec![Some(b"b".to_vec())],
        "the tasklet's commit carries the one its greatest key asked for"
    );

    // --- a failure later in the iteration discards the completion ---
    assert!(node.start_workflow(
        "test-complete-then-fail".into(),
        "workflow-cf".into(),
        "x".into()
    ));
    let (_o, done) = drain(rx, "workflow-cf");
    assert_eq!(done.0, "error");
    assert!(done.1.contains("boom after complete"), "{}", done.1);
    assert!(done.2.is_empty(), "a failed iteration completes nothing");
    let _ = drain_task_commits();
}

/// The completion each of `id`'s forward task commits carried, in order.
fn completions_of(commits: Vec<crate::pb::TaskCommit>, id: &str) -> Vec<Option<Vec<u8>>> {
    commits
        .into_iter()
        .filter(|c| c.operative_id == id && !c.compensating)
        .map(|c| c.completes.then_some(c.result))
        .collect()
}

pub(super) fn loop_stop_and_removal(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- inbox loop: one iteration per input until a tasklet completes it ---
    assert!(node.start_workflow("test-loop-workflow".into(), "loop-1".into(), "".into()));
    for turn in [&b"alpha"[..], b"beta", b"quit"] {
        assert!(
            node.get_workflow("loop-1")
                .is_some_and(|i| i.push_inbox(turn.to_vec(), 0)),
            "inbox delivers to running instance"
        );
    }
    let (outs, done) = drain(rx, "loop-1");
    assert_eq!(outs, vec![b"got:alpha".to_vec(), b"got:beta".to_vec()]);
    assert_eq!(
        (done.0.as_str(), done.2.as_slice()),
        ("ok", &b""[..]),
        "a tasklet's complete ends the loop"
    );

    // --- external stop (WorkflowStop handler) → cancelled ---
    assert!(node.start_workflow("test-loop-workflow".into(), "loop-2".into(), "".into()));
    assert!(node
        .get_workflow("loop-2")
        .is_some_and(|i| i.push_inbox(b"one".to_vec(), 0)));
    // Drain the echo first so the instance is definitely live before stop.
    wait_for_output(rx, "loop-2", b"got:one");
    let stop = WorkflowStop {
        operative_id: "loop-2".into(),
    };
    let mut errs = Vec::new();
    stop.handle(node, "", &mut errs);
    assert!(errs.is_empty());
    let (_o, done) = drain(rx, "loop-2");
    assert_eq!(done.0, "cancelled", "WorkflowStop ⇒ cancelled");

    // --- hard removal (RemOperative → stop_operative) cancels a workflow ---
    assert!(node.start_workflow("test-loop-workflow".into(), "loop-3".into(), "".into()));
    assert!(node
        .get_workflow("loop-3")
        .is_some_and(|i| i.push_inbox(b"x".to_vec(), 0)));
    wait_for_output(rx, "loop-3", b"got:x");
    assert!(
        node.stop_operative("loop-3"),
        "stop_operative removes the workflow"
    );
    assert!(!node.has_operative("loop-3"));
    let (_o, done) = drain(rx, "loop-3");
    assert_eq!(
        done.0, "cancelled",
        "hard stop ⇒ driver cancels and tears down"
    );

    // --- max_duration backstop → cancelled ---
    assert!(node.start_workflow("test-timed-workflow".into(), "timed-1".into(), "".into()));
    let (_o, done) = drain(rx, "timed-1");
    assert_eq!(done.0, "cancelled", "max_duration elapse ⇒ cancelled");
}

pub(super) fn structured_chains(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- structured multi-task chain runs in order with a barrier ---
    assert!(node.start_workflow("test-chain-workflow".into(), "chain-1".into(), "x".into()));
    let (chain_outs, done) = drain(rx, "chain-1");
    assert_eq!(done.0, "ok");
    assert_eq!(
        chain_outs,
        vec![b"x-1-2".to_vec()],
        "Stage1 then Stage2, in order"
    );

    // --- structured parallel static cells + barrier + per-cell state ---
    assert!(node.start_workflow(
        "test-fan-workflow".into(),
        "fan-1".into(),
        "a,bb,ccc,dddd".into(),
    ));
    let (mut fan_outs, done) = drain(rx, "fan-1");
    assert_eq!(done.0, "ok");
    fan_outs.sort();
    // odd cell: "a"(1) then "ccc"(2); even cell: "bb"(1) then "dddd"(2).
    // Independent per-cell counters; the barrier collected all four.
    let mut expected: Vec<Vec<u8>> = vec![
        b"1:a".to_vec(),
        b"2:ccc".to_vec(),
        b"1:bb".to_vec(),
        b"2:dddd".to_vec(),
    ];
    expected.sort();
    assert_eq!(fan_outs, expected);

    // --- hybrid channel: inter-task output also routed to a plugin sink ---
    assert!(node.start_workflow("test-hybrid-workflow".into(), "hybrid-1".into(), "z".into()));
    let (mut hybrid_outs, done) = drain(rx, "hybrid-1");
    assert_eq!(done.0, "ok");
    hybrid_outs.sort();
    // "mid:h:z" from the hybrid sink on Stage1's output; "final:h:z" from
    // Stage2's terminal plugin — Stage1's value reached both Stage2 and the sink.
    let mut hybrid_expected = vec![b"final:h:z".to_vec(), b"mid:h:z".to_vec()];
    hybrid_expected.sort();
    assert_eq!(hybrid_outs, hybrid_expected);
}

pub(super) fn add_operative_routing(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- AddOperative --kind workflow routes to the workflow path ---
    let add = AddOperative {
        operative_id: "workflow-via-add".into(),
        factory_id: "test-emit-workflow".into(),
        init_values: "argz".into(),
        kind: "workflow".into(),
        resume_cell_count: 0,
        task_journal: None,
        ..Default::default()
    };
    let mut errors = Vec::new();
    add.handle(node, "", &mut errors);
    assert!(
        errors.is_empty(),
        "AddOperative --kind workflow should not error: {errors:?}"
    );
    let (outs, done) = drain(rx, "workflow-via-add");
    assert_eq!(outs, vec![b"argz".to_vec()]);
    assert_eq!(done.0, "ok");

    // --- AddOperative --kind workflow for an unregistered factory reports it ---
    let add_bad = AddOperative {
        operative_id: "workflow-bad".into(),
        factory_id: "test-unregistered".into(),
        init_values: "".into(),
        kind: "workflow".into(),
        resume_cell_count: 0,
        task_journal: None,
        ..Default::default()
    };
    let mut errors_bad = Vec::new();
    add_bad.handle(node, "", &mut errors_bad);
    assert_eq!(errors_bad.len(), 1);
    assert!(errors_bad[0].contains("WorkflowOperative class"));

    // --- AddOperative with an unknown kind literal is refused ---
    let add_unknown = AddOperative {
        operative_id: "kind-bad".into(),
        factory_id: "test-emit-workflow".into(),
        init_values: "".into(),
        kind: "continuous".into(),
        resume_cell_count: 0,
        task_journal: None,
        ..Default::default()
    };
    let mut errors_unknown = Vec::new();
    add_unknown.handle(node, "", &mut errors_unknown);
    assert_eq!(errors_unknown.len(), 1);
    assert!(errors_unknown[0].contains("unknown compute-unit kind 'continuous'"));
    assert!(node.get_workflow("kind-bad").is_none());
}
