//! The deferred-wake list: what entering `poll` promotes, what a full
//! mailbox defers instead of parking on, and how a wake is scoped,
//! disarmed and discarded.

use std::sync::Arc;
use std::time::{Duration, Instant};

use flz_core::poll_core::{PollCore, Polled, Starvation};

use super::fixtures::{unix_stream, wait_until};
use super::support::*;
use crate::entity::timer::{TimerKind, TimerOp};
use crate::entity::EntityItem;

// ------------------------------------------------ the deferred-wake list

/// Entering `poll` promotes whatever came due: timers armed in the past
/// are on the Entity's mailbox by the time that call returns, in ascending
/// scheduled order however they were armed (what a restore's overdue set
/// relies on), each carrying its scheduled timestamp as the invocation's
/// `now`.
#[test]
fn poll_entry_promotes_due_timers_in_ascending_order() {
    let core = PollCore::new();
    let (_i, slot) = build_slot("poll-wake-entry");
    let at = flz_core::util::wall_now_ms() - 1;
    arm_due_timer(&core, &slot, at);
    // Armed second, due first: order comes from the list, not arrival.
    slot.timers
        .lock()
        .unwrap()
        .apply(&TimerOp::register(TimerKind::ProcessingTime, at - 1));
    core.arm_timer(&slot, at - 1, unix_stream());
    assert_eq!(core.timers_armed(), 2);

    // Nothing is queued on this core, so the call promotes and times out.
    assert!(matches!(
        core.poll(Duration::from_millis(0)),
        Polled::Timeout
    ));
    assert_eq!(core.timers_armed(), 0, "a fired entry leaves the list");

    let act = core.try_assemble(&slot).expect("both firings");
    let fired: Vec<u64> = act
        .items()
        .iter()
        .map(|w| w.now.expect("a firing carries its scheduled at"))
        .collect();
    assert_eq!(
        fired,
        vec![at - 1, at],
        "ascending scheduled order; fired_at is the scheduled timestamp"
    );
    core.complete(act.token);
}

/// A due firing whose target mailbox is full must not park the promoter
/// inside `poll`: a thread there is counted by `pollers_inside`, which
/// makes the starvation wait answer "a thread is about to take work". A
/// single-poller host (every C-ABI host, which can install no inline-serve
/// hook) would then wedge permanently with the supervisor blind to it.
///
/// Now the pass returns, the entry stays on the list, and the firing lands
/// once room appears — still carrying the timestamp it was ARMED with,
/// which is what the pending set cancels by.
#[test]
fn a_due_firing_into_a_full_mailbox_defers_instead_of_parking() {
    let core = PollCore::new();
    let (_i, slot) = full_slot("poll-wake-full-timer");
    let at = flz_core::util::wall_now_ms() - 1;
    slot.timers
        .lock()
        .unwrap()
        .apply(&TimerOp::register(TimerKind::ProcessingTime, at));
    core.arm_timer(&slot, at, unix_stream());
    assert_eq!(core.timers_armed(), 1);

    // The pass RETURNS. Before the fix this call never came back.
    let started = Instant::now();
    assert!(matches!(
        core.poll(Duration::from_millis(0)),
        Polled::Timeout
    ));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the promoter must not wait for mailbox room"
    );
    assert_eq!(
        core.timers_armed(),
        1,
        "the undeliverable firing stays on the wake list"
    );

    // And the condition is reportable: a due entry no pass could service
    // is starvation with no dwell, which is what buys the host a thread.
    assert!(
        matches!(
            core.wait_starvation(Duration::from_secs(5)),
            Starvation::Observed
        ),
        "a wake nobody can deliver must read as starvation"
    );

    // Room appears; a later pass lands it.
    assert!(slot.mailbox.pop().is_some());
    wait_until(
        "the deferred firing to land once the mailbox has room",
        || {
            let _ = core.poll(Duration::from_millis(10));
            core.timers_armed() == 0
        },
    );
    let (token, fired_at) = assemble_timer(&core, &slot);
    assert_eq!(
        fired_at, at,
        "a deferred firing keeps its ARMED timestamp, not the retry's due time"
    );
    core.complete(token);
}

/// The same rule for the other wake a saturated mailbox can meet. A close
/// silently dropped would hold the interval's outputs until the next
/// input — exactly what `checkpoint_max_delay_ms` exists to prevent — so
/// it defers rather than parking or vanishing.
#[test]
fn a_due_interval_close_into_a_full_mailbox_defers_instead_of_parking() {
    let core = PollCore::new();
    let (_i, slot) = full_slot("poll-wake-full-close");
    core.arm_interval_close(&slot, flz_core::util::wall_now_ms() - 1);

    let started = Instant::now();
    assert!(matches!(
        core.poll(Duration::from_millis(0)),
        Polled::Timeout
    ));
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "the promoter must not wait for mailbox room"
    );
    assert_eq!(slot.mailbox.len(), 1, "nothing was admitted");

    assert!(slot.mailbox.pop().is_some());
    wait_until(
        "the deferred close to land once the mailbox has room",
        || {
            let _ = core.poll(Duration::from_millis(10));
            slot.mailbox.len() == 1
        },
    );
    assert!(matches!(
        slot.mailbox.pop(),
        Some(EntityItem::CloseInterval)
    ));
}

/// An entry armed while a poller is already blocked shortens that
/// poller's wait: it is promoted promptly instead of at the
/// caller-supplied timeout, and no thread was created to notice.
#[test]
fn a_wake_armed_during_a_wait_shortens_it() {
    let core = Arc::new(PollCore::new());
    let (_i, slot) = build_slot("poll-wake-during");
    let poller = {
        let core = Arc::clone(&core);
        std::thread::spawn(move || core.poll(Duration::from_secs(30)))
    };
    // The poller is inside `poll` before the overdue deadline is armed.
    wait_until("the poller to enter poll", || core.pollers_inside() >= 1);
    let started = Instant::now();
    arm_due_timer(&core, &slot, flz_core::util::wall_now_ms() - 1);

    wait_until("a blocked poller to promote the entry", || {
        core.timers_armed() == 0
    });
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the wait was not shortened to the nearest due entry"
    );
    assert!(!slot.mailbox.is_empty(), "the firing reached the mailbox");

    core.shutdown();
    assert!(matches!(poller.join().unwrap(), Polled::ShuttingDown));
}

/// Two Operative instances arming the same procedure id, entity key and
/// scheduled time hold two entries, and one instance's cancellation
/// leaves the other's firing alone — the identity in the list key carries
/// the instance.
#[test]
fn timers_are_scoped_to_the_arming_instance() {
    let core = PollCore::new();
    let (_ia, a) = build_slot("poll-scope-a");
    let (_ib, b) = build_slot("poll-scope-b");
    let at = flz_core::util::wall_now_ms() - 1;
    arm_due_timer(&core, &a, at);
    arm_due_timer(&core, &b, at);
    assert_eq!(
        core.timers_armed(),
        2,
        "same procedure and key, different instances: two entries"
    );

    core.disarm_timer(&a.entity_identity, at);
    assert_eq!(core.timers_armed(), 1);
    assert!(matches!(
        core.poll(Duration::from_millis(0)),
        Polled::Timeout
    ));

    assert!(
        a.mailbox.is_empty(),
        "the disarmed instance's timer must not fire"
    );
    let (token, now) = assemble_timer(&core, &b);
    assert_eq!(now, at);
    core.complete(token);
}

/// The list's bookkeeping: arming is an idempotent replace, a disarm takes
/// exactly one entry, and discarding an Entity takes every entry it owns
/// and no other's — what the dynamic-mode reaper relies on.
#[test]
fn the_wake_list_arms_disarms_and_discards_by_entity() {
    let core = PollCore::new();
    let (_ia, a) = build_slot("poll-armed-a");
    let (_ib, b) = build_slot("poll-armed-b");
    // Far enough out that nothing promotes mid-test.
    let base = flz_core::util::wall_now_ms() + 600_000;

    core.arm_timer(&a, base, unix_stream());
    core.arm_timer(&a, base + 10, unix_stream());
    core.arm_timer(&b, base + 5, unix_stream());
    assert_eq!(core.timers_armed(), 3);

    core.arm_timer(&a, base + 10, unix_stream());
    assert_eq!(core.timers_armed(), 3, "re-arming replaces, it never adds");

    core.disarm_timer(&a.entity_identity, base + 10);
    assert_eq!(core.timers_armed(), 2, "a disarm removes exactly one entry");
    core.disarm_timer(&a.entity_identity, base + 10);
    assert_eq!(core.timers_armed(), 2, "and is idempotent");

    core.discard_entity_timers(&a.entity_identity);
    assert_eq!(
        core.timers_armed(),
        1,
        "discarding an Entity takes every entry it owns and no other's"
    );
    core.discard_entity_timers(&b.entity_identity);
    assert_eq!(core.timers_armed(), 0);
}
