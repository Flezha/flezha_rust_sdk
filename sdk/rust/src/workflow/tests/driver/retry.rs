//! The per-entity retry ladder: recovery, exhaustion, opt-out, and what an
//! external stop does to a waiting backoff.

use std::sync::atomic::Ordering;

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use crate::runner::RunnerMessage;

pub(super) fn entity_retry(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- workflow-entity-retry: a transient entity failure recovers in
    // place — the sibling entity is NOT re-run and nothing compensates ---
    arm_flaky(1, false);
    assert!(node.start_workflow(
        "test-retry-workflow".into(),
        "retry-ok".into(),
        "a,bb".into()
    ));
    let (mut outs, done) = drain(rx, "retry-ok");
    assert_eq!(done.0, "ok", "the ladder absorbed the transient failure");
    outs.sort();
    assert_eq!(
        outs,
        vec![b"even:bb".to_vec(), b"odd:a".to_vec()],
        "each entity's output lands exactly once — a retried attempt's \
         staged output is discarded, not double-emitted"
    );
    assert_eq!(
        ODD_ATTEMPTS.load(Ordering::SeqCst),
        2,
        "the failing entity ran twice (fail, then retry)"
    );
    assert_eq!(
        EVEN_RUNS.load(Ordering::SeqCst),
        1,
        "the sibling entity stayed buffered at the barrier: no re-run"
    );
    assert!(
        drain_comp_log().is_empty(),
        "a recovered retry must not compensate"
    );

    // --- retry exhaustion fails the procedure exactly as before: the
    // original error surfaces and completed procedures compensate ---
    arm_flaky(99, false);
    assert!(node.start_workflow(
        "test-retry-workflow".into(),
        "retry-exhausted".into(),
        "a,bb".into()
    ));
    let (_o, done) = drain(rx, "retry-exhausted");
    assert_eq!(done.0, "error");
    assert!(
        done.1.contains("flaky attempt 3"),
        "the last attempt's error is the terminal one: {}",
        done.1
    );
    assert_eq!(
        ODD_ATTEMPTS.load(Ordering::SeqCst),
        3,
        "the default ladder is 3 attempts"
    );
    assert_eq!(
        drain_comp_log(),
        vec!["undo-retry-reserve".to_string()],
        "exhaustion runs the existing saga rollback"
    );

    // --- a non-retryable error skips the ladder entirely ---
    arm_flaky(99, true);
    assert!(node.start_workflow(
        "test-retry-workflow".into(),
        "retry-permanent".into(),
        "a,bb".into()
    ));
    let (_o, done) = drain(rx, "retry-permanent");
    assert_eq!(done.0, "error");
    assert_eq!(
        ODD_ATTEMPTS.load(Ordering::SeqCst),
        1,
        "ProcedureError::non_retryable ⇒ no retry"
    );
    assert_eq!(drain_comp_log(), vec!["undo-retry-reserve".to_string()]);

    // --- retry_attempts() = 0 restores the pre-change fail-fast ---
    arm_flaky(99, false);
    assert!(node.start_workflow(
        "test-retry-failfast-workflow".into(),
        "retry-failfast".into(),
        "a,bb".into()
    ));
    let (_o, done) = drain(rx, "retry-failfast");
    assert_eq!(done.0, "error");
    assert_eq!(
        ODD_ATTEMPTS.load(Ordering::SeqCst),
        1,
        "attempts = 0 ⇒ the first error is terminal"
    );
    assert_eq!(drain_comp_log(), vec!["undo-retry-reserve".to_string()]);
}

pub(super) fn retry_stop_and_stage(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- an external stop interrupts a waiting backoff (30 s base: the run
    // can only end inside `drain`'s deadline by observing the cancel in the
    // wait). The stop rides a helper thread because this thread is the fake
    // controller — it must stay in `drain` to ack the chain's proc commits ---
    arm_flaky(99, false);
    assert!(node.start_workflow(
        "test-retry-slow-workflow".into(),
        "retry-stop".into(),
        "a,bb".into()
    ));
    let stop_target = node
        .get_workflow("retry-stop")
        .expect("live instance accepts cancel");
    let stopper = std::thread::spawn(move || {
        while ODD_ATTEMPTS.load(Ordering::SeqCst) == 0 {
            std::thread::yield_now();
        }
        stop_target.request_cancel();
    });
    let (_o, done) = drain(rx, "retry-stop");
    stopper.join().expect("stopper thread");
    assert_eq!(done.0, "cancelled", "the stop wins over the pending retry");
    assert_eq!(
        ODD_ATTEMPTS.load(Ordering::SeqCst),
        1,
        "the interrupted attempt is not retried"
    );
    assert_eq!(
        drain_comp_log(),
        vec!["undo-retry-reserve".to_string()],
        "a cancelled ladder takes the normal abnormal-exit path"
    );

    // --- a stage step is an activation the SDK's workers run, and the
    // backoff between two attempts holds none of them: every entity of this
    // stage backs off at once, and there are twice as many entities as
    // workers, so a backoff that occupied its worker would leave the pool
    // starved and the core would answer with threads above the base set ---
    STEP_ATTEMPTS.lock().unwrap().clear();
    assert!(node.start_workflow(
        "test-step-probe-workflow".into(),
        "workflow-steps".into(),
        "go".into()
    ));
    let (outs, done) = drain(rx, "workflow-steps");
    assert_eq!((done.0.as_str(), done.1.as_str()), ("ok", ""));
    assert_eq!(
        outs.len(),
        probe_keys().len(),
        "every entity emitted once, after its own retry"
    );
    let attempts = STEP_ATTEMPTS.lock().unwrap().clone();
    assert_eq!(
        attempts.len(),
        probe_keys().len() * 2,
        "one failed attempt and one success per entity"
    );
    let base = crate::operative::poll_pool::base_worker_count();
    for (key, thread) in &attempts {
        let worker = thread.strip_prefix("flz-poll-").unwrap_or_else(|| {
            panic!("entity {key} ran its step on {thread}, not on an SDK poll worker")
        });
        assert!(
            worker.parse::<usize>().expect("poll worker id") < base,
            "the pool grew past its {base} base workers to serve {key}: a backoff was \
             holding a worker instead of being timed at the driver's barrier"
        );
    }
}
