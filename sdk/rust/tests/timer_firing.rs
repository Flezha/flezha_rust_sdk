//! Both trigger paths: an event-time firing rides the watermark, a
//! processing-time one rides the wall clock, and each arrives through the
//! Entity mailbox rather than a direct `on_timer` call.

use std::sync::Arc;
use std::time::Duration;

use flezha_runner::entity::state_sink;
use flezha_runner::entity::timer::{TimerKind, TimerOp};

mod common;
use common::*;

// ------------------------------------------------------- event-time firing

/// Spec: "Crossing the high-water fires in order" — 100 then 200, both BEFORE
/// the input that crossed them, with 300 still pending.
#[test]
fn crossing_the_high_water_fires_in_order_before_the_input() {
    let _guard = timer_test_guard();
    take_order();

    let (handle, instance) = setup::<WindowOperative>("timers-window-order");
    let stream = dummy_stream();
    dispatch_input(&handle, "timers-window-order", "in", tick(50), &stream);
    assert!(wait_until(
        || order_snapshot().len() == 1,
        Duration::from_secs(5)
    ));
    dispatch_input(&handle, "timers-window-order", "in", tick(250), &stream);

    assert!(
        wait_until(|| order_snapshot().len() >= 4, Duration::from_secs(5)),
        "the two firings and the two runs must all land, got {:?}",
        order_snapshot()
    );
    assert_eq!(
        take_order(),
        vec![
            "run@50".to_string(),
            "timer@100".to_string(),
            "timer@200".to_string(),
            "run@250".to_string(),
        ]
    );
    assert_eq!(
        pending(&instance.entities[0][0], TimerKind::EventTime),
        vec![300],
        "300 is beyond the new high-water and stays pending"
    );
}

/// Spec: "A quiet Entity does not fire" — event time is a property of the
/// input, so an Entity nothing arrives at has no clock to cross.
#[test]
fn a_quiet_entity_never_fires_its_event_time_timer() {
    let _guard = timer_test_guard();
    take_order();

    let (handle, instance) = setup::<WindowOperative>("timers-window-quiet");
    let stream = dummy_stream();
    dispatch_input(&handle, "timers-window-quiet", "in", tick(50), &stream);
    assert!(wait_until(
        || pending(&instance.entities[0][0], TimerKind::EventTime).len() == 3,
        Duration::from_secs(5)
    ));

    // Wall time passes; nothing arrives.
    assert!(stays_quiet(
        || order_snapshot().len() > 1,
        Duration::from_millis(200)
    ));

    assert_eq!(
        take_order(),
        vec!["run@50".to_string()],
        "no firing, however much wall-clock time passes"
    );
}

// -------------------------------------------- processing-time firing

/// The processing-time path end to end: an input arms a deadline, nothing
/// else arrives, and `on_timer` runs with the SCHEDULED timestamp — not the
/// wall clock at which a poller got round to promoting it. Also spec "Timers
/// are per Entity": only the key that registered fires.
#[test]
fn a_processing_time_timer_fires_from_the_wake_list_with_its_scheduled_time() {
    let _guard = timer_test_guard();
    take_firings();

    let (handle, _instance) = setup::<SessionOperative>("timers-session-fire");
    let stream = dummy_stream();
    let before = wall_now_ms();
    dispatch_input(
        &handle,
        "timers-session-fire",
        "in",
        b"alice".to_vec(),
        &stream,
    );

    assert!(
        wait_until(|| !firings_snapshot().is_empty(), Duration::from_secs(5)),
        "the deadline must fire"
    );
    // A stray second firing gets its chance to appear and does not — a
    // per-Entity timer must not fan out.
    assert!(stays_quiet(
        || firings_snapshot().len() > 1,
        Duration::from_millis(100)
    ));
    let fired = take_firings();
    assert_eq!(fired.len(), 1, "one Entity, one timer, one firing");
    let (key, kind, fired_at) = &fired[0];
    assert_eq!(key, "alice", "the firing carries the registering Entity");
    assert_eq!(*kind, "processing_time");
    let after = wall_now_ms();
    assert!(
        *fired_at >= before + TIMEOUT_MS && *fired_at <= after,
        "fired_at is the scheduled instant (now + {TIMEOUT_MS}ms), not the wall clock at firing"
    );
}

/// Spec (`sdk-poll-core`) "A due wake that cannot be delivered stays on the
/// wake list": the poller promoting a due timer neither serves the saturated
/// consumer inline nor parks on it. It re-records the entry and returns, and
/// the firing lands on a later pass once room appears — the firing is delayed,
/// never dropped, and no thread is held inside `poll` while it waits.
#[test]
fn a_firing_into_a_saturated_consumer_waits_for_room_without_holding_a_poller() {
    let _guard = timer_test_guard();
    take_firings();
    let (sock, _peer) = std::os::unix::net::UnixStream::pair().expect("pair");
    state_sink::set_secondary_stream(Some(Arc::new(
        flezha_runner::transport::FlezhaStream::Unix(sock),
    )));

    let (handle, instance) = setup::<SaturatedSessionOperative>("timers-saturated");
    let registry = Arc::clone(instance.dynamic_registries[0].as_ref().expect("dynamic"));
    let slot = registry.get_or_create("alice").expect("entity");
    let stream = dummy_stream();

    // Inflight held with no drainer, then both slots filled: nothing pops
    // until whoever pushes next serves the round.
    assert!(slot.mailbox.claim_inflight());
    for _ in 0..2 {
        dispatch_input(
            &handle,
            "timers-saturated",
            "in",
            b"alice".to_vec(),
            &stream,
        );
    }
    assert!(slot.mailbox.is_full());

    // A deadline already in the past: the next poll pass promotes it.
    slot.timers
        .lock()
        .unwrap()
        .apply(&TimerOp::register(TimerKind::ProcessingTime, 1));
    flezha_runner::entity::timer::rearm_restored(&slot);

    // Held, not delivered and not lost: with no room the promotion pass keeps
    // re-recording the entry. (The retired behaviour ran the round inline here
    // and the firing landed with the mailbox still full.)
    assert!(
        stays_quiet(
            || !firings_snapshot().is_empty(),
            Duration::from_millis(200)
        ),
        "a firing must not be delivered into a mailbox with no room"
    );
    assert!(slot.mailbox.is_full(), "the mailbox is still saturated");

    // Room appears and the round gets a drainer: the deferred firing lands.
    assert!(slot.mailbox.pop().is_some());
    slot.mailbox.release_inflight();
    dispatch_input(
        &handle,
        "timers-saturated",
        "in",
        b"alice".to_vec(),
        &stream,
    );

    assert!(
        wait_until(|| !firings_snapshot().is_empty(), Duration::from_secs(5)),
        "the deferred firing must land once the mailbox has room"
    );

    // The two queued inputs re-armed the deadline; let it fire before the
    // next case counts firings.
    take_firings();
    assert!(wait_until(
        || !firings_snapshot().is_empty(),
        Duration::from_millis(TIMEOUT_MS * 3)
    ));
    take_firings();
    state_sink::set_secondary_stream(None);
}
