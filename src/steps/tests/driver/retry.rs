//! The per-cell retry ladder: recovery, exhaustion, opt-out, and what an
//! external stop does to a waiting backoff.

use std::sync::atomic::Ordering;

use crossbeam::channel::Receiver;
use flz_core::runner::RunnerNode;

use super::super::fake_controller::*;
use super::super::fixtures::*;
use crate::runner::RunnerMessage;

pub(super) fn cell_retry(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- steps-cell-retry: a transient cell failure recovers in
    // place — the sibling cell is NOT re-run and nothing compensates ---
    arm_flaky(1, false);
    assert!(start_fed(node, "test-retry-steps", "retry-ok", b"a,bb"));
    let (mut outs, done) = drain(rx, "retry-ok");
    assert_eq!(done.0, "ok", "the ladder absorbed the transient failure");
    outs.sort();
    assert_eq!(
        outs,
        vec![b"even:bb".to_vec(), b"odd:a".to_vec()],
        "each cell's output lands exactly once — a retried attempt's \
         staged output is discarded, not double-emitted"
    );
    assert_eq!(
        ODD_ATTEMPTS.load(Ordering::SeqCst),
        2,
        "the failing cell ran twice (fail, then retry)"
    );
    assert_eq!(
        EVEN_RUNS.load(Ordering::SeqCst),
        1,
        "the sibling cell stayed buffered at the barrier: no re-run"
    );
    assert!(
        drain_comp_log().is_empty(),
        "a recovered retry must not compensate"
    );

    // --- retry exhaustion fails the tasklet exactly as before: the
    // original error surfaces and completed tasklets compensate ---
    arm_flaky(99, false);
    assert!(start_fed(
        node,
        "test-retry-steps",
        "retry-exhausted",
        b"a,bb"
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
    assert!(start_fed(
        node,
        "test-retry-steps",
        "retry-permanent",
        b"a,bb"
    ));
    let (_o, done) = drain(rx, "retry-permanent");
    assert_eq!(done.0, "error");
    assert_eq!(
        ODD_ATTEMPTS.load(Ordering::SeqCst),
        1,
        "TaskletError::non_retryable ⇒ no retry"
    );
    assert_eq!(drain_comp_log(), vec!["undo-retry-reserve".to_string()]);

    // --- retry_max(1) on the edge is fail-fast ---
    arm_flaky(99, false);
    assert!(start_fed(
        node,
        "test-retry-failfast-steps",
        "retry-failfast",
        b"a,bb"
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

pub(super) fn item_retry(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- a failed item is re-handed with the items after it; the item
    // before it already ran and is never run again ---
    ITEM_ATTEMPTS.lock().unwrap().clear();
    assert!(start_fed(
        node,
        "test-item-retry-steps",
        "item-retry",
        b"a,b,c"
    ));
    let (mut outs, done) = drain(rx, "item-retry");
    assert_eq!((done.0.as_str(), done.1.as_str()), ("ok", ""));
    outs.sort();
    assert_eq!(
        outs,
        vec![b"item:a".to_vec(), b"item:b".to_vec(), b"item:c".to_vec()]
    );
    let attempts = ITEM_ATTEMPTS.lock().unwrap().clone();
    let runs = |item: &str| attempts.iter().filter(|seen| *seen == item).count();
    assert_eq!(
        (runs("a"), runs("b"), runs("c")),
        (1, 2, 1),
        "only the failed item re-ran: {attempts:?}"
    );
}

pub(super) fn retry_stop_and_stage(node: &mut RunnerNode, rx: &Receiver<RunnerMessage>) {
    // --- an external stop interrupts a waiting backoff (30 s base: the run
    // can only end inside `drain`'s deadline by observing the cancel in the
    // wait). The stop rides a helper thread because this thread is the fake
    // controller — it must stay in `drain` to ack the chain's task commits ---
    arm_flaky(99, false);
    assert!(start_fed(
        node,
        "test-retry-slow-steps",
        "retry-stop",
        b"a,bb"
    ));
    let stop_target = instance(node, "retry-stop").expect("live instance accepts cancel");
    let odd_cell = stop_target
        .cells
        .iter()
        .flatten()
        .find(|slot| slot.cell_key.as_deref() == Some("odd"))
        .map(std::sync::Arc::clone)
        .expect("the flaky stage's odd cell");
    let stopper = std::thread::spawn(move || {
        while ODD_ATTEMPTS.load(Ordering::SeqCst) == 0 {
            std::thread::yield_now();
        }
        flz_core::steps::stop(&stop_target);
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
    assert!(
        odd_cell.pending_retry.lock().unwrap().is_empty(),
        "the stop dropped the items the backoff held"
    );
    assert_eq!(
        crate::runner::poll_core::global().cancel_retry(&odd_cell),
        0,
        "the stop disarmed the backoff's wake: nothing is left to promote"
    );

    // --- a stage step is an activation the SDK's workers run, and the
    // backoff between two attempts holds none of them: every cell of this
    // stage backs off at once, and there are twice as many cells as
    // workers, so a backoff that occupied its worker would leave the pool
    // starved and the core would answer with threads above the base set ---
    STEP_ATTEMPTS.lock().unwrap().clear();
    assert!(start_fed(
        node,
        "test-step-probe-steps",
        "steps-steps",
        b"go"
    ));
    let (outs, done) = drain(rx, "steps-steps");
    assert_eq!((done.0.as_str(), done.1.as_str()), ("ok", ""));
    assert_eq!(
        outs.len(),
        probe_keys().len(),
        "every cell emitted once, after its own retry"
    );
    let attempts = STEP_ATTEMPTS.lock().unwrap().clone();
    assert_eq!(
        attempts.len(),
        probe_keys().len() * 2,
        "one failed attempt and one success per cell"
    );
    let base = crate::relay::dispatcher::ensure_poll_workers().base;
    for (key, thread) in &attempts {
        let worker = thread.strip_prefix("flz-poll-").unwrap_or_else(|| {
            panic!("cell {key} ran its step on {thread}, not on an SDK poll worker")
        });
        assert!(
            worker.parse::<usize>().expect("poll worker id") < base,
            "the pool grew past its {base} base workers to serve {key}: a backoff was \
             holding a worker instead of being timed at the driver's barrier"
        );
    }
}
