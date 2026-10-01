//! The entrance scenarios: `init_values` as state before the first message,
//! a message running only its channel's chain, an entry fanning out through
//! its edges, a chained loop re-entering its chain, and how every loop mode
//! waits for a message it does not have.

use std::sync::atomic::Ordering;
use std::time::Duration;

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use crate::steps::instance;
use crate::command::CommandAction;
use crate::pb::StepsStop;
use crate::runner::RunnerMessage;

/// The next `count` outputs of `id`, in order, with every control request
/// answered on the way.
fn outputs(rx: &Receiver<RunnerMessage>, id: &str, count: usize) -> Vec<Vec<u8>> {
    pump(rx, id, |outs| outs.len() >= count)
}

fn stop(node: &mut RunnerNode, id: &str) {
    let mut errors = Vec::new();
    StepsStop {
        operator_id: id.into(),
    }
    .handle(node, "", &mut errors);
    assert!(errors.is_empty(), "{errors:?}");
}

pub(super) fn entrance(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- init_values is state for a steps operator, in place before its first message ---
    assert!(start(
        node,
        "test-budget-once".into(),
        "budget-1".into(),
        r#"{"entrance_budget":{"value":7}}"#
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, "budget-1", b"go"));
    let (outs, done) = drain(rx, "budget-1");
    assert_eq!(
        outs,
        vec![b"7:go".to_vec()],
        "the tasklet ran over the state init_values set, then the message"
    );
    assert_eq!(done.0, "ok");

    // --- a message runs only the chain of the channel it names; the join
    // waits for that chain's producer alone ---
    assert!(start(node, "test-two-chains".into(), "chains-1".into(), "")
        .expect("init_values is a JSON object"));
    assert!(feed_on(node, "chains-1", Some("a"), b"x"));
    assert_eq!(outputs(rx, "chains-1", 1), vec![b"j:a:x".to_vec()]);
    assert!(feed_on(node, "chains-1", Some("b"), b"y"));
    assert_eq!(outputs(rx, "chains-1", 1), vec![b"j:b:y".to_vec()]);
    stop(node, "chains-1");
    let (outs, done) = drain(rx, "chains-1");
    assert!(outs.is_empty(), "no other chain ran: {outs:?}");
    assert_eq!(done.0, "cancelled");

    // --- an entry feeding two roots dispatches both in the first level, each
    // through its own edge transform ---
    assert!(
        start(node, "test-fan-entry".into(), "fan-entry-1".into(), "")
            .expect("init_values is a JSON object")
    );
    assert!(feed(node, "fan-entry-1", b"x"));
    let (mut outs, done) = drain(rx, "fan-entry-1");
    outs.sort();
    assert_eq!(outs, vec![b"a:l:x".to_vec(), b"b:r:x".to_vec()]);
    assert_eq!(done.0, "ok");

    // --- a chained loop re-enters the chain its first message picked ---
    CHAINED_ROOTS.lock().unwrap().clear();
    assert!(
        start(node, "test-chained-chains".into(), "chained-1".into(), "")
            .expect("init_values is a JSON object")
    );
    assert!(feed_on(node, "chained-1", Some("b"), b"2"));
    let (_outs, done) = drain(rx, "chained-1");
    assert_eq!(done.0, "ok");
    assert_eq!(
        CHAINED_ROOTS.lock().unwrap().clone(),
        vec!["b", "b", "b"],
        "every pass entered through b's root; a's chain never ran"
    );

    // --- a durable chained loop commits its message's offset once, and no
    // offset for a carry-fed pass ---
    let _ = drain_iter_commits();
    assert!(start(
        node,
        "test-journaled-countdown".into(),
        "countdown-1".into(),
        ""
    )
    .expect("init_values is a JSON object"));
    let countdown = instance(node, "countdown-1")
        .expect("the loop's instance is registered before its driver runs");
    feed_at(&countdown, b"2", 0);
    let (_outs, done) = drain(rx, "countdown-1");
    assert_eq!(done.0, "ok");
    assert_eq!(
        drain_iter_commits()
            .iter()
            .filter(|c| c.operator_id == "countdown-1")
            .map(|c| c.consumed_offset)
            .collect::<Vec<_>>(),
        vec![Some(0), None, None],
        "the carry-fed passes consumed no journaled input"
    );
}

pub(super) fn once_waits(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a once run with an empty inbox parks after the idle window when it
    // journals, and resumes at pass 0 from parked(0) ---
    let _ = drain_iter_parks();
    EMIT_RUNS.store(0, Ordering::SeqCst);
    assert!(
        start(node, "test-emit-steps".into(), "once-parks".into(), "")
            .expect("init_values is a JSON object")
    );
    let (outs, done) = drain(rx, "once-parks");
    assert_eq!(
        done.0, "parked",
        "an empty inbox before the first pass parks"
    );
    assert!(outs.is_empty());
    assert_eq!(
        EMIT_RUNS.load(Ordering::SeqCst),
        0,
        "nothing ran over nothing"
    );
    let parks = drain_iter_parks();
    assert_eq!(parks.len(), 1);
    assert_eq!(parks[0].iter, 0, "the park names pass 0");
    assert!(node.stop_operator("once-parks"));
    assert!(start_resumable(
        node,
        "test-emit-steps".into(),
        "once-parks-2".into(),
        "",
        0,
        0,
        Some(instance::JournalResume {
            iter: 0,
            ..Default::default()
        })
    )
    .expect("init_values is a JSON object"));
    assert!(feed(node, "once-parks-2", b"wake"));
    let (outs, done) = drain(rx, "once-parks-2");
    assert_eq!(
        outs,
        vec![b"wake".to_vec()],
        "the resumed instance ran pass 0"
    );
    assert_eq!(done.0, "ok");

    // --- a once run that journals nothing waits for its message ---
    assert!(
        start(node, "test-budget-once".into(), "once-waits".into(), "")
            .expect("init_values is a JSON object")
    );
    match rx.recv_timeout(Duration::from_millis(400)) {
        Ok(RunnerMessage::EmitOperatorCompleted { operator_id, .. })
            if operator_id == "once-waits" =>
        {
            panic!("an opted-out once run ended instead of waiting")
        }
        _ => {}
    }
    assert!(feed(node, "once-waits", b"a"));
    let (outs, done) = drain(rx, "once-waits");
    assert_eq!(
        outs,
        vec![b"0:a".to_vec()],
        "the pass ran over the late message"
    );
    assert_eq!(done.0, "ok");

    // --- a stop during the wait completes as cancelled ---
    assert!(
        start(node, "test-budget-once".into(), "once-stop".into(), "")
            .expect("init_values is a JSON object")
    );
    stop(node, "once-stop");
    let (_outs, done) = drain(rx, "once-stop");
    assert_eq!(done.0, "cancelled");
}
