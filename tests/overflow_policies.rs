//! What each overflow policy does when a mailbox fills: DropNothing keeps
//! every item, DropNewest and DropOldest each shed their own end, and every
//! outcome is counted.

use flezha_runner::{OverflowPolicy, UnboundedOperator};

mod common;
use common::prelude::*;

/// `cell::drops_newest_total` and `drops_oldest_total` are PROCESS-wide: a
/// case that asserts on its own delta and a case that sheds concurrently
/// cannot both be running, or the second's drops are read as the first's.
/// Every case here that produces or measures a shed takes this.
static DROP_AGGREGATE_LOCK: Mutex<()> = Mutex::new(());

fn serial_drops() -> std::sync::MutexGuard<'static, ()> {
    DROP_AGGREGATE_LOCK
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

// ----- Section 4 / 7 / 8 scenario coverage: DropNothing stress + counters -----

static DROP_NOTHING_STRESS_RECEIVED: AtomicUsize = AtomicUsize::new(0);

test_tasklet!(DropNothingStressTasklet, "drop_nothing_stress_tasklet", Vec<u8> => (), (), cap 16, |_input, _state, _ctx| {
    // Tiny pause so producers actually get to park rather than
    // repeatedly winning the push race uncontested.
    for _ in 0..32 {
        std::hint::spin_loop();
    }
    DROP_NOTHING_STRESS_RECEIVED.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct DropNothingStressOperator;
impl UnboundedOperator for DropNothingStressOperator {
    fn factory_id() -> &'static str {
        "drop_nothing_stress_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<DropNothingStressTasklet>();
    }
}

/// Spec scenario "Parked producer does not block its consumer's drain"
/// (`DropNothing parking does not violate sticky per-cell ordering`)
/// stress-tested with 100 producer threads × 1000 items each on a
/// capacity-16 mailbox. Asserts every push eventually delivers and that
/// the per-Cell `pushes_total` matches the total emitted; this
/// covers Tasks 4.4, 5.6, 6.4, 7.7, 8.1, and 9.2 in one run.
#[test]
fn drop_nothing_stress_100_producers_no_loss() {
    DROP_NOTHING_STRESS_RECEIVED.store(0, Ordering::Release);
    let (handle, instance) = setup::<DropNothingStressOperator>("block-stress-instance");
    let stream = dummy_stream();

    let producers = 100usize;
    let per_producer = 1_000usize;
    let total = producers * per_producer;

    let mut threads = Vec::new();
    for tid in 0..producers {
        let handle = handle.clone();
        let stream = Arc::clone(&stream);
        threads.push(std::thread::spawn(move || {
            for i in 0..per_producer {
                let mut md = HashMap::new();
                md.insert("channel".to_string(), "input".to_string());
                let payload = format!("{tid}-{i}").into_bytes();
                handle.dispatch("block-stress-instance", payload, &md, Arc::clone(&stream));
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }

    assert!(
        wait_until(
            || DROP_NOTHING_STRESS_RECEIVED.load(Ordering::Acquire) == total,
            Duration::from_secs(60)
        ),
        "block-stress: only {} of {} delivered",
        DROP_NOTHING_STRESS_RECEIVED.load(Ordering::Acquire),
        total
    );

    // Per-Cell counters are populated through the producer-side
    // enqueue path. With DropNothing policy and a capacity-16 mailbox under
    // 100×1000 contention, every successful push bumps `pushes_total`
    // and at least one park event bumps `pushes_blocked_total`.
    let slot = &instance.cells[0][0];
    let counters = slot.counters();
    assert_eq!(
        counters.pushes_total.load(Ordering::Relaxed) as usize,
        total
    );
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
    assert!(
        counters.pushes_blocked_total.load(Ordering::Relaxed) > 0,
        "block-stress: producers should have parked at least once \
         under capacity-16 contention"
    );
}

// ----- DropNewest / DropOldest scenarios + per-counter coverage -----

static DROP_NEWEST_RECEIVED: AtomicUsize = AtomicUsize::new(0);

test_tasklet!(DropNewestTasklet, "drop_newest_tasklet", Vec<u8> => (), (), cap 2, |_input, _state, _ctx| {
    DROP_NEWEST_RECEIVED.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct DropNewestOperator;
impl UnboundedOperator for DropNewestOperator {
    fn factory_id() -> &'static str {
        "drop_newest_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNewest)
            .into::<DropNewestTasklet>();
    }
}

/// Spec scenarios "DropNewest discards the incoming item",
/// "An entry channel still drops when it says so", "DropNewest increments
/// only drops_newest_total". Pre-marks the slot's inflight CAS so no drain
/// runs during the test — that keeps the cap-2 mailbox saturated while
/// we exercise the DropNewest path deterministically.
#[test]
fn drop_newest_discards_when_full_and_only_bumps_newest_counter() {
    let _serial = serial_drops();
    DROP_NEWEST_RECEIVED.store(0, Ordering::Release);
    let drops_newest_before = flezha_runner::cell::drops_newest_total();
    let (handle, instance) = setup::<DropNewestOperator>("drop-newest-instance");
    let slot = Arc::clone(&instance.cells[0][0]);

    // Pre-acquire inflight: subsequent enqueue paths skip submit_drain
    // because their claim_inflight returns false. Items pile up in
    // the bounded mailbox without being popped.
    assert!(
        slot.mailbox.claim_inflight(),
        "fresh slot must yield inflight ownership"
    );

    let stream = dummy_stream();
    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());

    // First two pushes succeed and saturate the capacity-2 mailbox.
    for tag in [b"a".to_vec(), b"b".to_vec()] {
        handle.dispatch("drop-newest-instance", tag, &md, Arc::clone(&stream));
    }
    assert_eq!(slot.mailbox.len(), 2, "mailbox should be saturated");

    // Five more pushes hit the full mailbox; each increments
    // drops_newest_total and discards the payload.
    for _ in 0..5 {
        handle.dispatch(
            "drop-newest-instance",
            b"drop".to_vec(),
            &md,
            Arc::clone(&stream),
        );
    }

    let counters = slot.counters();
    assert_eq!(
        counters.pushes_total.load(Ordering::Relaxed),
        2,
        "only the two pre-saturation pushes succeed",
    );
    assert_eq!(
        counters.drops_newest_total.load(Ordering::Relaxed),
        5,
        "expected 5 DropNewest events"
    );
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.pushes_blocked_total.load(Ordering::Relaxed), 0);
    // The per-Cell counter stays in the process; the aggregate beside it is
    // what the data heartbeat carries, and a drop nobody outside can count is
    // the defect this closes.
    assert_eq!(
        flezha_runner::cell::drops_newest_total() - drops_newest_before,
        5,
        "the process-wide aggregate the heartbeat reports must move with it"
    );

    // Release inflight so the slot is in a consistent state at drop;
    // any residual items go away with the test's pool.
    slot.mailbox.release_inflight();
}

static DROP_OLDEST_OBSERVED: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

test_tasklet!(DropOldestTasklet, "drop_oldest_tasklet", Vec<u8> => (), (), cap 2, |input, _state, _ctx| {
    DROP_OLDEST_OBSERVED.lock().unwrap().push(input);
    Ok(())
});

struct DropOldestOperator;
impl UnboundedOperator for DropOldestOperator {
    fn factory_id() -> &'static str {
        "drop_oldest_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropOldest)
            .into::<DropOldestTasklet>();
    }
}

/// Spec scenario "DropOldest replaces the head item": producer emits
/// past capacity, oldest payload is evicted, newest is enqueued, the
/// consumer's `drops_oldest_total` increments per overflow event.
#[test]
fn drop_oldest_evicts_head_and_bumps_oldest_counter() {
    let _serial = serial_drops();
    DROP_OLDEST_OBSERVED.lock().unwrap().clear();
    let drops_oldest_before = flezha_runner::cell::drops_oldest_total();
    let (handle, instance) = setup::<DropOldestOperator>("drop-oldest-instance");
    let slot = Arc::clone(&instance.cells[0][0]);

    // Pre-acquire inflight so no drain runs during the deterministic
    // push sequence below; pop / dispatch happens after the assertions.
    assert!(slot.mailbox.claim_inflight());

    let stream = dummy_stream();
    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());

    for label in [
        b"old".to_vec(),
        b"mid".to_vec(),
        b"new1".to_vec(),
        b"new2".to_vec(),
    ] {
        handle.dispatch("drop-oldest-instance", label, &md, Arc::clone(&stream));
    }

    // After 4 pushes into a cap-2 mailbox under DropOldest:
    //   pushes_total counts all 4 — a force-push that admits IS a successful
    //   enqueue by that counter's definition, and counting only the
    //   pre-saturation two made `processed` exceed `admitted` for this policy;
    //   drops_oldest_total counts the 2 force_push evictions.
    let counters = slot.counters();
    assert_eq!(counters.pushes_total.load(Ordering::Relaxed), 4);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 2);
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.pushes_blocked_total.load(Ordering::Relaxed), 0);
    assert_eq!(
        flezha_runner::cell::drops_oldest_total() - drops_oldest_before,
        2,
        "the process-wide aggregate the heartbeat reports must move with it"
    );

    // Surface the surviving pair without going through the WS pool: pop
    // the mailbox manually and verify recency wins.
    let mut surviving = Vec::new();
    while let Some(flezha_runner::cell::CellItem::Input(payload)) = slot.mailbox.pop() {
        let any_ref: &dyn std::any::Any = &*payload.value;
        let bytes = any_ref
            .downcast_ref::<Vec<u8>>()
            .expect("entry payload is Vec<u8>");
        surviving.push(bytes.clone());
    }
    slot.mailbox.release_inflight();
    assert_eq!(surviving, vec![b"new1".to_vec(), b"new2".to_vec()]);
}

/// Spec (`operator-tasklet-cell-model`) "A replay slice is never shed": a
/// correction's answer may not depend on arrival timing, so a slice member
/// takes the `DropNothing` path whatever the channel declared. A dropped
/// member would leave `replay_open` set and `replay_depth` short, the slice
/// would still close on the end-of-slice broadcast, and the correction would
/// fold whatever arrived — a wrong answer with no error anywhere.
#[test]
fn a_replay_slice_member_is_never_shed_by_a_dropping_channel() {
    let _serial = serial_drops();
    DROP_NEWEST_RECEIVED.store(0, Ordering::Release);
    let (handle, instance) = setup::<DropNewestOperator>("drop-newest-replay-instance");
    let slot = Arc::clone(&instance.cells[0][0]);
    assert!(slot.mailbox.claim_inflight());

    let stream = dummy_stream();
    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    for tag in [b"a".to_vec(), b"b".to_vec()] {
        handle.dispatch("drop-newest-replay-instance", tag, &md, Arc::clone(&stream));
    }
    assert_eq!(slot.mailbox.len(), 2, "the mailbox is saturated");

    // A slice member into the same full mailbox. A live input here would be
    // dropped on the spot; this one waits.
    let mut replay_md = md.clone();
    replay_md.insert("flz.replay".to_string(), "1".to_string());
    let member = {
        let handle = handle.clone();
        let stream = Arc::clone(&stream);
        std::thread::spawn(move || {
            handle.dispatch(
                "drop-newest-replay-instance",
                b"member".to_vec(),
                &replay_md,
                stream,
            )
        })
    };

    // The member is parked on the full mailbox once its push is counted blocked.
    assert!(wait_until(
        || slot.counters().pushes_blocked_total.load(Ordering::Relaxed) >= 1,
        Duration::from_secs(5)
    ));
    assert!(!member.is_finished(), "the member waits for room");
    assert_eq!(
        slot.counters().drops_newest_total.load(Ordering::Relaxed),
        0,
        "a slice member is never shed"
    );

    // Room appears and the member lands.
    assert!(slot.mailbox.pop().is_some());
    member.join().expect("the member is admitted");
    assert_eq!(
        slot.counters().drops_newest_total.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        slot.mailbox.len(),
        2,
        "the slice member joined the queue rather than replacing one"
    );
    slot.mailbox.release_inflight();
}

/// Spec scenario "Counters present at runtime": fresh CellSlot
/// exposes a counters() accessor with all four atomics at 0.
#[test]
fn fresh_cell_counters_start_at_zero() {
    let (_handle, instance) = setup::<DropOldestOperator>("counters-fresh-instance");
    let counters = instance.cells[0][0].counters();
    assert_eq!(counters.pushes_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.pushes_blocked_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
}

static DROP_OLDEST_PROCESSED: AtomicUsize = AtomicUsize::new(0);

test_tasklet!(DropOldestStressTasklet, "drop_oldest_stress_tasklet", Vec<u8> => (), (), cap 2, |_input, _state, _ctx| {
    // Slow the drain just enough that producers genuinely race it, which is
    // what makes `force_push` fire and what made the counters disagree.
    for _ in 0..64 {
        std::hint::spin_loop();
    }
    DROP_OLDEST_PROCESSED.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct DropOldestStressOperator;
impl UnboundedOperator for DropOldestStressOperator {
    fn factory_id() -> &'static str {
        "drop_oldest_stress_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropOldest)
            .into::<DropOldestStressTasklet>();
    }
}

/// The measurement the counter fix exists for: under real contention a
/// `DropOldest` channel's **admitted** count must never come out below what the
/// consumer **processed**.
///
/// `force_push` admits the item — that is the whole policy — so `pushes_total`
/// must count that half as well as `try_push`, or an item that entered through
/// an eviction is processed while never counted as admitted. A unit assertion
/// cannot show the inversion: it needs producers actually racing a live drain,
/// which is the only thing that makes `force_push` fire.
///
/// Four rounds, each on a fresh instance, printing its own numbers.
#[test]
fn drop_oldest_stress_admitted_never_below_processed() {
    let _serial = serial_drops();
    let producers = 8usize;
    let per_producer = 2_000usize;
    let sent = producers * per_producer;

    for round in 1..=4 {
        DROP_OLDEST_PROCESSED.store(0, Ordering::Release);
        let id = format!("drop-oldest-stress-{round}");
        let (handle, instance) = setup::<DropOldestStressOperator>(&id);
        let stream = dummy_stream();

        let mut threads = Vec::new();
        for tid in 0..producers {
            let handle = handle.clone();
            let stream = Arc::clone(&stream);
            let id = id.clone();
            threads.push(std::thread::spawn(move || {
                let mut md = HashMap::new();
                md.insert("channel".to_string(), "input".to_string());
                for i in 0..per_producer {
                    let payload = format!("{tid}-{i}").into_bytes();
                    handle.dispatch(&id, payload, &md, Arc::clone(&stream));
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }

        // Let the queue drain and the last admitted item be served, so
        // `remaining` is really zero rather than "not yet served": the empty
        // mailbox is the settle, the count going quiet after it is the proof.
        let slot = Arc::clone(&instance.cells[0][0]);
        assert!(
            wait_until(|| slot.mailbox.is_empty(), Duration::from_secs(60)),
            "round {round}: the mailbox never drained"
        );
        let drained = Instant::now();
        loop {
            let seen = DROP_OLDEST_PROCESSED.load(Ordering::Acquire);
            if stays_quiet(
                || DROP_OLDEST_PROCESSED.load(Ordering::Acquire) != seen,
                Duration::from_millis(50),
            ) {
                break;
            }
            assert!(
                drained.elapsed() < Duration::from_secs(60),
                "round {round}: the dispatch count never settled"
            );
        }

        let counters = slot.counters();
        let admitted = counters.pushes_total.load(Ordering::Relaxed) as usize;
        let dropped = counters.drops_oldest_total.load(Ordering::Relaxed) as usize;
        let processed = DROP_OLDEST_PROCESSED.load(Ordering::Acquire);
        let remaining = slot.mailbox.len();
        println!(
            "drop_oldest round {round}: sent={sent} admitted={admitted} \
             processed={processed} dropped={dropped} remaining={remaining}"
        );

        // The probe's finding, inverted back: nothing can be processed that was
        // never admitted.
        assert!(
            processed <= admitted,
            "round {round}: processed ({processed}) exceeded admitted ({admitted}) \
             — the DropOldest admission is uncounted again"
        );
        // And every send IS an admission under this policy, whichever half of
        // the push admitted it.
        assert_eq!(
            admitted, sent,
            "round {round}: a force-push that admitted was not counted"
        );
        // The gap between what was admitted and what came out is exactly what
        // the drop counter is for. It may over-state: a `force_push` that finds
        // the queue already drained admits without evicting, and the policy
        // still applied, so the counter still moves.
        assert!(
            admitted - processed - remaining <= dropped,
            "round {round}: {} items vanished unaccounted (dropped={dropped})",
            admitted - processed - remaining
        );
        // A round that never evicted would prove nothing.
        assert!(
            dropped > 0,
            "round {round}: no eviction happened — the probe did not saturate"
        );
    }
}

// ----- Inter-tasklet default-Block coverage -----

static INTER_TASKLET_DOWNSTREAM: AtomicUsize = AtomicUsize::new(0);

test_tasklet!(InterTaskletProducer, "inter_tasklet_producer", String => String, (), |input, _state, ctx| {
    ctx.emit::<String>(input)?;
    Ok(())
});

test_tasklet!(InterTaskletConsumer, "inter_tasklet_consumer", String => (), (), |_input, _state, _ctx| {
    INTER_TASKLET_DOWNSTREAM.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct InterTaskletDefaultBlockOperator;
impl UnboundedOperator for InterTaskletDefaultBlockOperator {
    fn factory_id() -> &'static str {
        "inter_tasklet_default_block_operator"
    }
    fn channels(c: &ChannelBuilder) {
        // No `.on_full(...)` — exercises the default `Block` for inter-tasklet.
        c.input::<String>("input")
            .into::<InterTaskletProducer>()
            .named("hop")
            .into::<InterTaskletConsumer>();
    }
}

// ----- Sticky ordering preserved under bounded operator Block (BoundedOperator 8.3) -----

static STICKY_BOUNDED_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static STICKY_BOUNDED_MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static STICKY_BOUNDED_DELIVERED: AtomicUsize = AtomicUsize::new(0);

test_tasklet!(StickyBoundedTasklet, "sticky_bounded_tasklet", Vec<u8> => (), (), cap 16, |_input, _state, _ctx| {
    let cur = STICKY_BOUNDED_CONCURRENT.fetch_add(1, Ordering::AcqRel) + 1;
    let mut prev = STICKY_BOUNDED_MAX_CONCURRENT.load(Ordering::Acquire);
    while cur > prev {
        match STICKY_BOUNDED_MAX_CONCURRENT.compare_exchange_weak(
            prev,
            cur,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(p) => prev = p,
        }
    }
    for _ in 0..32 {
        std::hint::spin_loop();
    }
    STICKY_BOUNDED_CONCURRENT.fetch_sub(1, Ordering::AcqRel);
    STICKY_BOUNDED_DELIVERED.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct StickyBoundedOperator;
impl UnboundedOperator for StickyBoundedOperator {
    fn factory_id() -> &'static str {
        "sticky_bounded_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<StickyBoundedTasklet>();
    }
}

/// Spec scenario "Same cell is never concurrent" re-validated
/// under bounded mailbox capacity 16 with a 10K input burst and a
/// `DropNothing` entry policy. No drop / no concurrency violation; the inflight CAS
/// guarantees mutual exclusion regardless of how many producers are
/// parked at the entry edge.
#[test]
fn sticky_ordering_holds_under_drop_nothing_entry() {
    STICKY_BOUNDED_CONCURRENT.store(0, Ordering::Release);
    STICKY_BOUNDED_MAX_CONCURRENT.store(0, Ordering::Release);
    STICKY_BOUNDED_DELIVERED.store(0, Ordering::Release);

    let (handle, instance) = setup::<StickyBoundedOperator>("sticky-bounded-instance");
    let stream = dummy_stream();

    let producers = 4usize;
    let total = 10_000usize;
    let per_producer = total / producers;

    let mut threads = Vec::new();
    for tid in 0..producers {
        let handle = handle.clone();
        let stream = Arc::clone(&stream);
        threads.push(std::thread::spawn(move || {
            for i in 0..per_producer {
                let mut md = HashMap::new();
                md.insert("channel".to_string(), "input".to_string());
                handle.dispatch(
                    "sticky-bounded-instance",
                    format!("{tid}-{i}").into_bytes(),
                    &md,
                    Arc::clone(&stream),
                );
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }

    assert!(wait_until(
        || STICKY_BOUNDED_DELIVERED.load(Ordering::Acquire) == total,
        Duration::from_secs(60)
    ));

    assert_eq!(
        STICKY_BOUNDED_MAX_CONCURRENT.load(Ordering::Acquire),
        1,
        "bounded-mailbox single-cell allowed concurrent dispatch"
    );

    let counters = instance.cells[0][0].counters();
    assert_eq!(
        counters.pushes_total.load(Ordering::Relaxed) as usize,
        total
    );
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
}

/// Spec scenarios "Default inter-tasklet policy is Block" and
/// "Successful push increments pushes_total". 200 inputs flow through a
/// producer → consumer pipeline; the inter-tasklet channel uses the
/// default DropNothing policy. Even if the consumer briefly stalls, no item
/// is lost and `pushes_total` on the consumer Cell equals 200.
#[test]
fn default_inter_tasklet_policy_is_block_and_pushes_total_increments() {
    INTER_TASKLET_DOWNSTREAM.store(0, Ordering::Release);
    let (handle, instance) =
        setup::<InterTaskletDefaultBlockOperator>("inter-tasklet-default-instance");
    let stream = dummy_stream();

    let n = 200usize;
    for i in 0..n {
        let mut md = HashMap::new();
        md.insert("channel".to_string(), "input".to_string());
        handle.dispatch(
            "inter-tasklet-default-instance",
            format!("{i}").into_bytes(),
            &md,
            Arc::clone(&stream),
        );
    }

    assert!(wait_until(
        || INTER_TASKLET_DOWNSTREAM.load(Ordering::Acquire) == n,
        Duration::from_secs(10)
    ));

    // Consumer Tasklet is tasklet id 1 (producer is 0); single-mode cell.
    let consumer_cells = &instance.cells[1];
    let counters = consumer_cells[0].counters();
    assert_eq!(
        counters.pushes_total.load(Ordering::Relaxed) as usize,
        n,
        "every inter-tasklet emit must bump consumer's pushes_total"
    );
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
}
