//! The jobs-underway cap and the starvation signal: what the cap holds
//! back, what it still lets through, and when the core tells a host it
//! owes a thread.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flz_core::poll_core::{PollCore, Polled, Starvation};
use flz_transport::transport::FlezhaStream;

use super::fixtures::{build_slot_of, hold_for, payload, unix_stream, wait_until};
use super::support::*;
use crate::entity::timer::{TimerKind, TimerOp};
use crate::entity::EntitySlot;

// ------------------------------------- the jobs-underway cap and its signal

/// Spawn a starvation waiter and return once the thread has reached
/// `wait_starvation`, so work queued after this call arrives at a waiter that
/// already exists — the wake path rather than the immediate one. Nothing in
/// the core publishes a parked waiter, so the thread publishes its own entry.
fn starvation_waiter(core: &Arc<PollCore>) -> std::thread::JoinHandle<Starvation> {
    let entered = Arc::new(AtomicBool::new(false));
    let handle = {
        let core = Arc::clone(core);
        let entered = Arc::clone(&entered);
        std::thread::spawn(move || {
            entered.store(true, Ordering::SeqCst);
            core.wait_starvation(Duration::from_secs(5))
        })
    };
    wait_until("the starvation waiter to reach its wait", || {
        entered.load(Ordering::SeqCst)
    });
    handle
}

/// Hand out one activation per slot, each occupying a job, and return the
/// tokens still outstanding.
fn occupy(core: &PollCore, slots: &[Arc<EntitySlot>], stream: &Arc<FlezhaStream>) -> Vec<u64> {
    slots
        .iter()
        .map(|slot| queue_and_hand_out(core, slot, stream, 1).token)
        .collect()
}

/// The cap is the core's own limit on jobs underway, independent of how
/// many threads a host runs: at it, queued entity work stays queued —
/// pollers wait as if `ready` were empty — and one completion is enough
/// to release exactly one more.
#[test]
fn handout_stops_at_the_cap_and_resumes_on_completion() {
    let core = PollCore::with_max_inflight(2);
    let slots: Vec<Arc<EntitySlot>> = (0..3)
        .map(|i| build_slot(&format!("poll-cap-{i}")).1)
        .collect();
    let stream = unix_stream();

    let held = occupy(&core, &slots[..2], &stream);

    // The third entity's round is queued and runnable; the cap holds it.
    assert!(slots[2].mailbox.try_push(payload(0, &stream)).is_ok());
    assert!(slots[2].mailbox.claim_inflight());
    core.schedule(&slots[2]);
    assert!(
        matches!(core.poll(Duration::from_millis(100)), Polled::Timeout),
        "work was handed out past the cap"
    );

    core.complete(held[0]);
    let act = match core.poll(Duration::from_millis(500)) {
        Polled::Activation(a) => a,
        _ => panic!("a completion did not release the queued work"),
    };
    assert!(Arc::ptr_eq(act.slot(), &slots[2]));
    core.complete(act.token);
    core.complete(held[1]);
}

/// Command activations are barrier-bounded by the drivers that enqueue
/// them and answer a caller blocked on a rendezvous — holding one behind
/// capped entity work would deadlock that caller, so the cap never gates
/// them.
#[test]
fn a_command_activation_is_served_at_the_cap() {
    let core = PollCore::with_max_inflight(1);
    let (_i, slot) = build_slot("poll-cap-command");
    let stream = unix_stream();
    let held = occupy(&core, std::slice::from_ref(&slot), &stream);

    let (_ic, other) = build_slot("poll-cap-command-other");
    assert!(other.mailbox.try_push(payload(0, &stream)).is_ok());
    assert!(other.mailbox.claim_inflight());
    core.schedule(&other);

    let (reply, _rx) = crossbeam::channel::bounded(1);
    core.submit_direct(flz_core::poll_core::EnginePayload::Command {
        target: flz_core::host::entity_ref_of(&other),
        change_seq: 0,
        factory: "poll_core_exclusivity_op".into(),
        kind: flz_core::schema::ActivationKind::SnapshotRequest,
        reply,
    });

    match core.poll(Duration::from_millis(500)) {
        Polled::Activation(act) => assert!(
            act.items().is_empty(),
            "the command activation must be served ahead of the capped work"
        ),
        _ => panic!("the cap gated a command activation"),
    }
    core.complete(held[0]);
}

/// Starvation is queued work with nobody left to take it: the host is
/// told, and decides for itself what to do about it (the Rust arm grows
/// its pool). Nothing is queued here until the waiter is parked, so the
/// wake is the signal and not a pre-existing condition.
#[test]
fn starvation_wakes_when_work_is_queued_with_no_poller() {
    let core = Arc::new(PollCore::new());
    let (_i, slot) = build_slot("poll-starve-queued");
    let stream = unix_stream();

    let supervisor = starvation_waiter(&core);

    let started = Instant::now();
    assert!(slot.mailbox.try_push(payload(0, &stream)).is_ok());
    assert!(slot.mailbox.claim_inflight());
    core.schedule(&slot);

    assert!(
        matches!(supervisor.join().unwrap(), Starvation::Observed),
        "queued work did not read as starvation"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the waiter timed out instead"
    );
}

/// A direct activation queued with nobody to take it is starvation too,
/// and unlike entity work the cap does not gate it: a workflow driver is
/// blocked on its step, so "every worker is inside user code" must buy a
/// thread rather than wedge the stage.
#[test]
fn starvation_wakes_at_a_queued_direct_activation() {
    let core = Arc::new(PollCore::with_max_inflight(1));
    let (_i, slot) = build_slot("poll-starve-direct");
    let stream = unix_stream();
    // At the cap: only the direct activation can make this starvation.
    let _held = occupy(&core, std::slice::from_ref(&slot), &stream);

    let supervisor = starvation_waiter(&core);

    let (reply, _rx) = crossbeam::channel::bounded(1);
    core.submit_direct(flz_core::poll_core::EnginePayload::Command {
        target: flz_core::host::entity_ref_of(&slot),
        change_seq: 0,
        factory: "poll_core_exclusivity_op".into(),
        kind: flz_core::schema::ActivationKind::SnapshotRequest,
        reply,
    });

    assert!(
        matches!(supervisor.join().unwrap(), Starvation::Observed),
        "a queued direct activation did not read as starvation"
    );
}

/// A deferred wake coming due with no poller to promote it is starvation
/// too — otherwise "every worker blocked in user code" would hold a timer
/// firing until one of them finished.
#[test]
fn starvation_wakes_at_a_due_deferred_wake() {
    let core = Arc::new(PollCore::new());
    let (_i, slot) = build_slot("poll-starve-wake");
    slot.timers
        .lock()
        .unwrap()
        .apply(&TimerOp::register(TimerKind::ProcessingTime, 0));
    assert!(slot.mailbox.claim_inflight());
    // Due 150 ms out: the waiter must shorten its own wait to it.
    core.arm_timer(&slot, flz_core::util::wall_now_ms() + 150, unix_stream());

    let started = Instant::now();
    assert!(
        matches!(
            core.wait_starvation(Duration::from_secs(5)),
            Starvation::Observed
        ),
        "a due wake with no poller did not read as starvation"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "the waiter woke before the entry was due"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

/// A poller inside `poll` is about to take the work, so nothing is
/// starved — that is what keeps a healthy runner from growing threads it
/// does not need.
#[test]
fn no_starvation_while_a_poller_is_waiting() {
    let core = Arc::new(PollCore::new());
    let (_i, slot) = build_slot("poll-starve-served");
    let stream = unix_stream();

    let poller = {
        let core = Arc::clone(&core);
        std::thread::spawn(move || core.poll(Duration::from_secs(2)))
    };
    wait_until("the poller to enter poll", || core.pollers_inside() >= 1);
    assert!(slot.mailbox.try_push(payload(0, &stream)).is_ok());
    assert!(slot.mailbox.claim_inflight());
    core.schedule(&slot);

    assert!(
        matches!(
            core.wait_starvation(Duration::from_millis(300)),
            Starvation::Timeout
        ),
        "work a waiting poller can serve must not read as starvation"
    );
    match poller.join().unwrap() {
        Polled::Activation(act) => {
            core.complete(act.token);
        }
        _ => panic!("the waiting poller did not take the queued work"),
    }
}

/// A saturated runner is BUSY, not starved: the queue is never empty and
/// the workers are outside `poll` most of the time, yet they keep coming
/// back for more. Reporting that as starvation would answer a runner that
/// is working at capacity with ever more threads — measured as a
/// throughput collapse, which is why the signal requires a dwell with no
/// `poll` entry at all rather than a snapshot of "nobody is polling".
#[test]
fn a_saturated_runner_does_not_read_as_starved() {
    let core = Arc::new(PollCore::new());
    let slots: Vec<Arc<EntitySlot>> = (0..4)
        .map(|i| build_slot(&format!("poll-saturated-{i}")).1)
        .collect();
    let stop = Arc::new(AtomicBool::new(false));

    // Keeps every entity's queue non-empty for the whole run.
    let feeder = {
        let (core, slots, stop) = (Arc::clone(&core), slots.clone(), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                for slot in &slots {
                    crate::runner::poll_core_tests::fixtures::push_input(&core, slot, 1);
                }
                hold_for(Duration::from_millis(1));
            }
        })
    };
    // One worker spending most of its time in "user code" — so
    // `pollers_inside` reads zero at almost any instant — but re-entering
    // `poll` every few milliseconds.
    let worker = {
        let (core, stop) = (Arc::clone(&core), Arc::clone(&stop));
        std::thread::spawn(move || {
            while !stop.load(Ordering::Acquire) {
                if let Polled::Activation(act) = core.poll(Duration::from_millis(20)) {
                    // "User code" running: the worker is outside `poll`.
                    hold_for(Duration::from_millis(5));
                    core.complete(act.token);
                }
            }
        })
    };

    let verdict = core.wait_starvation(Duration::from_millis(600));
    stop.store(true, Ordering::Release);
    feeder.join().unwrap();
    worker.join().unwrap();
    assert!(
        matches!(verdict, Starvation::Timeout),
        "a runner working through a full queue must not read as starved"
    );
}

/// Shutdown is not delayed by a backoff: there is no sleep left to run
/// out, so a poller observes it immediately.
#[test]
fn shutdown_during_a_deferred_window_is_immediate() {
    let core = PollCore::new();
    let (_i, slot) = build_slot_of::<SlowRetryOp>("poll-defer-shutdown");
    let stream = unix_stream();

    let act = queue_and_hand_out(&core, &slot, &stream, 1);
    let (token, _slot, items) = act.into_work().expect("work activation");
    core.retry_after_failure(token, items, "boom".into());

    core.shutdown();
    let started = Instant::now();
    assert!(matches!(
        core.poll(Duration::from_secs(30)),
        Polled::ShuttingDown
    ));
    assert!(started.elapsed() < Duration::from_millis(200));
}
