//! Handout exclusivity and ordering, and the retry ladder: one Entity is
//! outstanding at most once, its items keep their order across a re-hand,
//! and a backoff defers the re-hand without holding a thread.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flz_core::poll_core::{Answered, PollCore, Polled};

use super::fixtures::{build_slot_of, payload, unix_stream, wait_until};
use super::support::*;
use crate::entity::{EntityItem, EntitySlot};
use crate::operative::instance::StreamInstance;

/// The spec pin: two workers polling concurrently while two entities each
/// have 100 queued inputs never both hold the same entity, and each
/// entity's inputs arrive in enqueue order across successive activations.
#[test]
fn same_entity_never_outstanding_twice_and_order_holds() {
    const PER_ENTITY: usize = 100;
    let core = Arc::new(PollCore::new());
    let (_ia, slot_a) = build_slot("poll-excl-a");
    let (_ib, slot_b) = build_slot("poll-excl-b");
    let stream = unix_stream();

    // Per-slot "held by a worker" flags keyed by slot pointer identity.
    let held: Arc<Vec<(usize, AtomicBool)>> = Arc::new(vec![
        (Arc::as_ptr(&slot_a) as usize, AtomicBool::new(false)),
        (Arc::as_ptr(&slot_b) as usize, AtomicBool::new(false)),
    ]);
    let seen: Arc<Vec<(usize, std::sync::Mutex<Vec<usize>>)>> = Arc::new(vec![
        (Arc::as_ptr(&slot_a) as usize, std::sync::Mutex::new(vec![])),
        (Arc::as_ptr(&slot_b) as usize, std::sync::Mutex::new(vec![])),
    ]);
    let total = Arc::new(AtomicUsize::new(0));

    let mut workers = Vec::new();
    for _ in 0..2 {
        let core = Arc::clone(&core);
        let held = Arc::clone(&held);
        let seen = Arc::clone(&seen);
        let total = Arc::clone(&total);
        workers.push(std::thread::spawn(move || loop {
            match core.poll(Duration::from_millis(50)) {
                Polled::Activation(act) => {
                    let key = Arc::as_ptr(act.slot()) as usize;
                    let flag = &held.iter().find(|(k, _)| *k == key).unwrap().1;
                    assert!(
                        !flag.swap(true, Ordering::AcqRel),
                        "entity handed out while already outstanding"
                    );
                    let log = &seen.iter().find(|(k, _)| *k == key).unwrap().1;
                    for work in act.items() {
                        let EntityItem::Input(p) = &work.item else {
                            panic!("unexpected timer item")
                        };
                        let ordinal = *p.value.downcast_ref::<usize>().unwrap();
                        log.lock().unwrap().push(ordinal);
                        total.fetch_add(1, Ordering::Relaxed);
                    }
                    flag.store(false, Ordering::Release);
                    core.complete(act.token);
                }
                Polled::Timeout => {}
                Polled::ShuttingDown => return,
            }
        }));
    }

    // Interleave pushes across the two entities, scheduling a round on
    // every winning inflight CAS — the producer protocol verbatim.
    for i in 0..PER_ENTITY {
        for slot in [&slot_a, &slot_b] {
            assert!(slot.mailbox.try_push(payload(i, &stream)).is_ok());
            if slot.mailbox.claim_inflight() {
                core.schedule(slot);
            }
        }
    }

    wait_until("every activation to arrive", || {
        total.load(Ordering::Relaxed) >= PER_ENTITY * 2
    });
    core.shutdown();
    for w in workers {
        w.join().unwrap();
    }

    let expected: Vec<usize> = (0..PER_ENTITY).collect();
    for (_, log) in seen.iter() {
        assert_eq!(*log.lock().unwrap(), expected, "per-entity order violated");
    }
}

/// Completion releases the entity: a batch queued behind an outstanding
/// activation is handed out only after `complete`.
#[test]
fn completion_releases_the_entity() {
    let core = PollCore::new();
    let (_i, slot) = build_slot("poll-release");
    let stream = unix_stream();

    let first = queue_and_hand_out(&core, &slot, &stream, 1);

    // Arrives mid-flight: the producer's CAS fails (round still held),
    // so nothing is scheduled — and poll must NOT hand the entity out.
    assert!(slot.mailbox.try_push(payload(1, &stream)).is_ok());
    assert!(!slot.mailbox.claim_inflight());
    match core.poll(Duration::from_millis(50)) {
        Polled::Timeout => {}
        _ => panic!("entity handed out while outstanding"),
    }

    core.complete(first.token);
    match core.poll(Duration::from_millis(500)) {
        Polled::Activation(second) => {
            assert_eq!(second.items().len(), 1);
            core.complete(second.token);
        }
        _ => panic!("queued input not handed out after completion"),
    }
}

/// The engine's ladder: a retryable failure re-hands the same item with
/// a bumped attempt, the tail queued behind it in order; the attempt
/// that reaches the budget is marked final.
#[test]
fn retry_after_failure_reholds_head_and_tail_in_order() {
    let core = PollCore::new();
    let (_i, slot) = build_slot("poll-retry");
    let stream = unix_stream();

    let act = queue_and_hand_out(&core, &slot, &stream, 3);
    assert_eq!(act.items().len(), 3);
    assert_eq!(act.items()[0].attempt, 1);
    assert!(!act.items()[0].is_final);

    // Item 0 processed fine; item 1 failed retryably: hand back 1 + 2.
    let (token, _slot, items) = act.into_work().expect("work activation");
    let passback: Vec<_> = items.into_iter().skip(1).collect();
    core.retry_after_failure(token, passback, "boom".into());

    let act = match core.poll(Duration::from_millis(500)) {
        Polled::Activation(a) => a,
        _ => panic!("re-hand did not arrive"),
    };
    assert_eq!(act.items().len(), 2);
    assert_eq!(ordinals(&act), vec![1, 2], "head-of-line order violated");
    assert_eq!(act.items()[0].attempt, 2);
    assert_eq!(act.items()[1].attempt, 1, "tail attempts must not bump");
    // Default policy is 3 attempts: one more failure reaches the budget.
    let (token, _slot, items) = act.into_work().expect("work activation");
    let passback: Vec<_> = items.into_iter().collect();
    core.retry_after_failure(token, passback, "boom again".into());
    let act = match core.poll(Duration::from_millis(2_000)) {
        Polled::Activation(a) => a,
        _ => panic!("second re-hand did not arrive"),
    };
    assert_eq!(act.items()[0].attempt, 3);
    assert!(
        act.items()[0].is_final,
        "budget-reaching attempt must be final"
    );
    core.complete(act.token);
}

/// A parked head must not take the unprocessed tail with it: the host
/// stops at the failed item and the core re-hands the rest — same order,
/// no attempt bump, no backoff (the tail never failed). This is exactly
/// how a healthy payload queued behind a poison one stays deliverable.
#[test]
fn abandon_returns_the_tail_untouched() {
    let core = PollCore::new();
    let (_i, slot) = build_slot("poll-rehand");
    let stream = unix_stream();

    let act = queue_and_hand_out(&core, &slot, &stream, 3);
    assert_eq!(act.items().len(), 3);

    // Item 0 parked at its final attempt; 1 and 2 never ran.
    let started = Instant::now();
    let (token, _slot, items) = act.into_work().expect("work activation");
    let tail: Vec<_> = items.into_iter().skip(1).collect();
    core.abandon(token, tail);

    let act = match core.poll(Duration::from_millis(500)) {
        Polled::Activation(a) => a,
        _ => panic!("unprocessed tail was dropped"),
    };
    assert!(
        started.elapsed() < Duration::from_millis(400),
        "a re-hand is not a retry: no backoff may delay it"
    );
    assert_eq!(ordinals(&act), vec![1, 2], "tail order violated");
    assert_eq!(act.items()[0].attempt, 1, "a re-hand bumps no attempt");
    assert_eq!(act.items()[1].attempt, 1);
    core.complete(act.token);

    // An empty tail is a plain completion: the entity is released.
    assert!(slot.mailbox.try_push(payload(9, &stream)).is_ok());
    assert!(slot.mailbox.claim_inflight());
    core.schedule(&slot);
    let act = match core.poll(Duration::from_millis(100)) {
        Polled::Activation(a) => a,
        _ => panic!("expected an activation"),
    };
    core.abandon(act.token, Vec::new());
    assert!(slot.mailbox.try_push(payload(10, &stream)).is_ok());
    assert!(slot.mailbox.claim_inflight());
    core.schedule(&slot);
    match core.poll(Duration::from_millis(500)) {
        Polled::Activation(a) => {
            core.complete(a.token);
        }
        _ => panic!("entity not released after an empty re-hand"),
    }
}

/// The third answer, on the path it exists for: a host that took a batch
/// and could not finish it re-hands EVERYTHING untouched. Per-entity
/// exclusivity is the handout, so without this the entity would never
/// appear in another poll result for the life of the process.
#[test]
fn abandon_rehands_the_whole_batch_and_releases_the_entity() {
    let core = PollCore::new();
    let (_i, slot) = build_slot("poll-abandon");
    let stream = unix_stream();

    let act = queue_and_hand_out(&core, &slot, &stream, 3);
    let (token, _slot, items) = act.into_work().expect("work activation");
    assert_eq!(core.outstanding_count(), 1);
    assert_eq!(core.abandon(token, items), Answered::Released);
    assert_eq!(core.outstanding_count(), 0);

    let act = match core.poll(Duration::from_millis(500)) {
        Polled::Activation(a) => a,
        _ => panic!("an abandoned entity must be pollable again"),
    };
    assert_eq!(
        ordinals(&act),
        vec![0, 1, 2],
        "items must come back in order"
    );
    for item in act.items() {
        assert_eq!(item.attempt, 1, "an abandon consumes no ladder attempt");
        assert!(!item.is_final);
    }
    core.complete(act.token);
}

/// A token the core does not hold is a host defect — a double answer, or
/// one from a context that never held it. Reporting it is what turns a
/// silent second release (which would re-arm an entity another worker is
/// already draining) into a caught bug.
#[test]
fn a_second_answer_is_rejected_not_silently_ignored() {
    let core = PollCore::new();
    let (_i, slot) = build_slot("poll-double-answer");
    let stream = unix_stream();

    let act = queue_and_hand_out(&core, &slot, &stream, 1);
    let token = act.token;
    drop(act);
    assert_eq!(core.complete(token), Answered::Released);

    assert_eq!(core.complete(token), Answered::UnknownToken);
    assert_eq!(core.abandon(token, Vec::new()), Answered::UnknownToken);
    assert_eq!(
        core.retry_after_failure(token, Vec::new(), "boom".into()),
        Answered::UnknownToken
    );
    assert_eq!(
        core.abandon(u64::MAX, Vec::new()),
        Answered::UnknownToken,
        "a token never handed out is refused the same way"
    );
}

/// Handout liveness, the two values the heartbeat carries: one wedged
/// Entity is invisible to the runner-wide dispatch counter, and these are
/// what make it visible. Reporting only — nothing here reclaims.
#[test]
fn handout_liveness_reports_the_count_and_the_oldest_age() {
    let core = PollCore::new();
    let (_ia, first) = build_slot("poll-liveness-a");
    let (_ib, second) = build_slot("poll-liveness-b");
    let stream = unix_stream();

    assert_eq!(core.outstanding_count(), 0);
    assert_eq!(
        core.outstanding_oldest_age_ms(),
        0,
        "an idle core reports zero"
    );

    let old = queue_and_hand_out(&core, &first, &stream, 1);
    // The first handout ages on the clock before the second is issued.
    wait_until("the first handout to age", || {
        core.outstanding_oldest_age_ms() >= 60
    });
    let young = queue_and_hand_out(&core, &second, &stream, 1);

    assert_eq!(core.outstanding_count(), 2);
    let age = core.outstanding_oldest_age_ms();
    assert!(
        age >= 60,
        "the OLDEST handout's age is what is reported, got {age}ms"
    );

    core.complete(old.token);
    assert_eq!(core.outstanding_count(), 1);
    assert!(
        core.outstanding_oldest_age_ms() < age,
        "the remaining handout is younger than the one just answered"
    );
    core.complete(young.token);
    assert_eq!(core.outstanding_oldest_age_ms(), 0);
}

// ------------------------------------------------ backoff without a sleep

/// The retry backoff holds no thread: the completing call returns at once,
/// the entity stays handed out until its due time — a producer enqueueing
/// meanwhile cannot claim a round — and the re-hand then leads with the
/// held-back items, ahead of what arrived during the window.
#[test]
fn a_backoff_defers_the_re_hand_without_holding_a_thread() {
    let core = PollCore::new();
    let (_i, slot) = build_slot_of::<SlowRetryOp>("poll-defer");
    let stream = unix_stream();

    let act = queue_and_hand_out(&core, &slot, &stream, 2);
    let (token, _slot, items) = act.into_work().expect("work activation");

    let started = Instant::now();
    core.retry_after_failure(token, items, "boom".into());
    assert!(
        started.elapsed() < Duration::from_millis(200),
        "the completing thread was held for the backoff"
    );

    // A producer during the window: it enqueues, but the entity is still
    // handed out, so no round can be claimed and no poller may serve it.
    assert!(slot.mailbox.try_push(payload(9, &stream)).is_ok());
    assert!(!slot.mailbox.claim_inflight());
    assert!(matches!(
        core.poll(Duration::from_millis(300)),
        Polled::Timeout
    ));

    let act = match core.poll(Duration::from_secs(6)) {
        Polled::Activation(a) => a,
        _ => panic!("the deferred re-hand never came due"),
    };
    assert_eq!(
        ordinals(&act),
        vec![0, 1, 9],
        "the holdback leads, in order, ahead of the window's arrival"
    );
    assert_eq!(act.items()[0].attempt, 2);
    core.complete(act.token);
}

/// Correlated failure — every entity failing at once — leaves pollers free
/// to serve the entities whose work would succeed. This is the whole point
/// of not sleeping a worker: with the pool sized to the CPU count, N
/// deferred entities would otherwise be N workers doing nothing.
#[test]
fn correlated_failure_leaves_pollers_serving_healthy_entities() {
    let core = PollCore::new();
    let (_ih, healthy) = build_slot("poll-correlated-healthy");
    let stream = unix_stream();

    let failing: Vec<(Arc<StreamInstance>, Arc<EntitySlot>)> = (0..4)
        .map(|i| build_slot_of::<SlowRetryOp>(&format!("poll-correlated-{i}")))
        .collect();
    for (_instance, slot) in &failing {
        let act = queue_and_hand_out(&core, slot, &stream, 1);
        let (token, _slot, items) = act.into_work().expect("work activation");
        core.retry_after_failure(token, items, "dependency is down".into());
    }

    let act = queue_and_hand_out(&core, &healthy, &stream, 1);
    assert!(
        Arc::ptr_eq(act.slot(), &healthy),
        "a healthy entity was starved by the deferred ones"
    );
    core.complete(act.token);
}
