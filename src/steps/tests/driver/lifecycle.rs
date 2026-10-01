//! Lifecycle and structured-chain scenarios: a single pass, a failure, a
//! completion and its tie-break, an inbox loop a tasklet ends, a stop, and
//! the `AddOperator` kind routing.

use std::sync::atomic::Ordering;

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use crate::command::CommandAction;
use crate::pb::{AddOperator, StepsStop};
use crate::runner::RunnerMessage;
use crate::ErrorModel;

pub(super) fn lifecycle(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a once pass: args echoed, status "ok" ---
    EMIT_RUNS.store(0, Ordering::SeqCst);
    assert!(start_fed(node, "test-emit-steps", "steps-ok", b"hello"));
    let (outs, done) = drain(rx, "steps-ok");
    assert_eq!(
        EMIT_RUNS.load(Ordering::SeqCst),
        1,
        "the tasklet runs exactly once"
    );
    assert_eq!(outs, vec![b"hello".to_vec()], "args echoed as output");
    assert_eq!((done.0.as_str(), done.1.as_str()), ("ok", ""));

    // Instance stays registered; a duplicate start is idempotent (no re-run).
    assert!(node.has_operator("steps-ok"));
    assert!(
        instance(node, "steps-ok").is_some_and(|i| i.steps.is_some()),
        "a steps instance carries its run"
    );
    assert!(start_fed(node, "test-emit-steps", "steps-ok", b"hello"));
    assert_eq!(
        EMIT_RUNS.load(Ordering::SeqCst),
        1,
        "duplicate start must not re-run"
    );

    // --- a non-retryable failure: terminal on its first run ---
    FAIL_RUNS.store(0, Ordering::SeqCst);
    assert!(start_fed(node, "test-fail-steps", "steps-err", b""));
    let (_o, done) = drain(rx, "steps-err");
    assert_eq!(done.0, "error");
    assert!(done.1.contains("boom"));
    assert_eq!(
        FAIL_RUNS.load(Ordering::SeqCst),
        1,
        "a non-retryable error skips the retry ladder"
    );

    // --- a panic caught and reported as "error" ---
    assert!(start_fed(node, "test-panic-steps", "steps-panic", b""));
    let (_o, done) = drain(rx, "steps-panic");
    assert_eq!(done.0, "error");
    assert!(done.1.contains("kaboom"));
}

pub(super) fn completions(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a tasklet's complete carries the result, journaled on its commit ---
    let _ = drain_task_commits();
    assert!(start_fed(node, "test-complete-steps", "steps-c", b"z"));
    let (_o, done) = drain(rx, "steps-c");
    assert_eq!(done.0, "ok");
    assert_eq!(
        done.2,
        b"result:z".to_vec(),
        "complete() result reaches OperatorCompleted"
    );
    assert_eq!(
        completions_of(drain_task_commits(), "steps-c"),
        vec![Some(b"result:z".to_vec())],
        "the completing tasklet's commit carries its result"
    );

    // --- two completing tasklets: the later one in commit order wins ---
    assert!(start_fed(node, "test-two-completions", "steps-two", b"x"));
    let (_o, done) = drain(rx, "steps-two");
    assert_eq!((done.0.as_str(), done.2.as_slice()), ("ok", &b"second"[..]));
    assert_eq!(
        completions_of(drain_task_commits(), "steps-two"),
        vec![Some(b"first".to_vec()), Some(b"second".to_vec())],
        "each tasklet journals its own completion"
    );

    // --- two cells of one tasklet: the greatest cell key wins ---
    assert!(start_fed(
        node,
        "test-keyed-completions",
        "steps-keyed",
        b"x"
    ));
    let (_o, done) = drain(rx, "steps-keyed");
    assert_eq!((done.0.as_str(), done.2.as_slice()), ("ok", &b"b"[..]));
    assert_eq!(
        completions_of(drain_task_commits(), "steps-keyed"),
        vec![Some(b"b".to_vec())],
        "the tasklet's commit carries the one its greatest key asked for"
    );

    // --- a failure later in the iteration discards the completion ---
    assert!(start_fed(
        node,
        "test-complete-then-fail",
        "steps-cf",
        b"x"
    ));
    let (_o, done) = drain(rx, "steps-cf");
    assert_eq!(done.0, "error");
    assert!(done.1.contains("boom after complete"), "{}", done.1);
    assert!(done.2.is_empty(), "a failed iteration completes nothing");
    let _ = drain_task_commits();
}

/// The completion each of `id`'s forward task commits carried, in order.
fn completions_of(commits: Vec<crate::pb::TaskCommit>, id: &str) -> Vec<Option<Vec<u8>>> {
    commits
        .into_iter()
        .filter(|c| c.operator_id == id && !c.compensating)
        .map(|c| c.completes.then_some(c.result))
        .collect()
}

pub(super) fn loop_stop_and_removal(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- inbox loop: one iteration per input until a tasklet completes it ---
    assert!(start(node, "test-loop-steps".into(), "loop-1".into(), "")
        .expect("init_values is a JSON object"));
    for turn in [&b"alpha"[..], b"beta", b"quit"] {
        assert!(
            feed(node, "loop-1", turn),
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

    // --- external stop (StepsStop handler) → cancelled ---
    assert!(start(node, "test-loop-steps".into(), "loop-2".into(), "")
        .expect("init_values is a JSON object"));
    assert!(feed(node, "loop-2", b"one"));
    // Drain the echo first so the instance is definitely live before stop.
    wait_for_output(rx, "loop-2", b"got:one");
    let stop = StepsStop {
        operator_id: "loop-2".into(),
    };
    let mut errs = Vec::new();
    stop.handle(node, "", &mut errs);
    assert!(errs.is_empty());
    let (_o, done) = drain(rx, "loop-2");
    assert_eq!(done.0, "cancelled", "StepsStop ⇒ cancelled");

    // --- hard removal (RemOperator → stop_operator) ends the run with no
    // completion: the controller removing an instance writes none ---
    assert!(start(node, "test-loop-steps".into(), "loop-3".into(), "")
        .expect("init_values is a JSON object"));
    assert!(feed(node, "loop-3", b"x"));
    wait_for_output(rx, "loop-3", b"got:x");
    assert!(
        node.stop_operator("loop-3"),
        "stop_operator removes the steps operator"
    );
    assert!(!node.has_operator("loop-3"));
    assert_no_completion(rx, "loop-3", std::time::Duration::from_millis(300));

    // --- max_duration backstop → cancelled ---
    assert!(
        start(node, "test-timed-steps".into(), "timed-1".into(), "")
            .expect("init_values is a JSON object")
    );
    let (_o, done) = drain(rx, "timed-1");
    assert_eq!(done.0, "cancelled", "max_duration elapse ⇒ cancelled");
}

pub(super) fn structured_chains(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- structured multi-task chain runs in order with a barrier ---
    assert!(start_fed(node, "test-chain-steps", "chain-1", b"x"));
    let (chain_outs, done) = drain(rx, "chain-1");
    assert_eq!(done.0, "ok");
    assert_eq!(
        chain_outs,
        vec![b"x-1-2".to_vec()],
        "Stage1 then Stage2, in order"
    );

    // --- structured parallel static cells + barrier + per-cell state ---
    assert!(start_fed(
        node,
        "test-fan-steps",
        "fan-1",
        b"a,bb,ccc,dddd"
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
    assert!(start_fed(node, "test-hybrid-steps", "hybrid-1", b"z"));
    let (mut hybrid_outs, done) = drain(rx, "hybrid-1");
    assert_eq!(done.0, "ok");
    hybrid_outs.sort();
    // "mid:h:z" from the hybrid sink on Stage1's output; "final:h:z" from
    // Stage2's terminal plugin — Stage1's value reached both Stage2 and the sink.
    let mut hybrid_expected = vec![b"final:h:z".to_vec(), b"mid:h:z".to_vec()];
    hybrid_expected.sort();
    assert_eq!(hybrid_outs, hybrid_expected);
}

pub(super) fn add_operator_routing(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- AddOperator --kind steps operator routes to the steps path; its
    // init_values are state, never the first message ---
    let add = AddOperator {
        operator_id: "steps-via-add".into(),
        factory_id: "test-emit-steps".into(),
        init_values: "{}".into(),
        run: "steps".into(),
        resume_cell_count: 0,
        task_journal: None,
        ..Default::default()
    };
    let mut errors = Vec::new();
    add.handle(node, "", &mut errors);
    assert!(
        errors.is_empty(),
        "AddOperator --kind steps should not error: {errors:?}"
    );
    assert!(feed(node, "steps-via-add", b"argz"));
    let (outs, done) = drain(rx, "steps-via-add");
    assert_eq!(outs, vec![b"argz".to_vec()]);
    assert_eq!(done.0, "ok");

    // --- init_values that are not a JSON object are refused at the bind ---
    let add_text = AddOperator {
        operator_id: "steps-text-init".into(),
        factory_id: "test-emit-steps".into(),
        init_values: "argz".into(),
        run: "steps".into(),
        ..Default::default()
    };
    let mut errors_text = Vec::new();
    add_text.handle(node, "", &mut errors_text);
    assert_eq!(errors_text.len(), 1, "{errors_text:?}");
    assert_eq!(
        errors_text[0].code(),
        "state.init_values_invalid_json",
        "{errors_text:?}"
    );
    assert!(
        instance(node, "steps-text-init").is_none(),
        "a refused document starts nothing"
    );

    // --- AddOperator for an unregistered factory reports it ---
    let add_bad = AddOperator {
        operator_id: "steps-bad".into(),
        factory_id: "test-unregistered".into(),
        ..Default::default()
    };
    let mut errors_bad = Vec::new();
    add_bad.handle(node, "", &mut errors_bad);
    assert_eq!(errors_bad.len(), 1);
    assert_eq!(
        errors_bad[0].code(),
        "runtime.class_not_registered",
        "{errors_bad:?}"
    );
    assert!(errors_bad[0].to_string().contains("test-unregistered"));

    // --- the class says how it runs: the wire's run string is not read ---
    let add_other_kind = AddOperator {
        operator_id: "kind-other".into(),
        factory_id: "test-emit-steps".into(),
        run: "flow".into(),
        ..Default::default()
    };
    let mut errors_other = Vec::new();
    add_other_kind.handle(node, "", &mut errors_other);
    assert!(errors_other.is_empty(), "{errors_other:?}");
    assert!(
        instance(node, "kind-other").is_some_and(|i| i.steps.is_some()),
        "a steps class binds a steps instance whatever the wire says"
    );
    assert!(node.stop_operator("kind-other"));
}
