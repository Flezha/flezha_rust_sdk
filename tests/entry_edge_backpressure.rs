//! The entry edge under load: what the wire reader does when the first
//! Entity's mailbox is full, and what the runner reports while it waits.

mod common;
use common::prelude::*;

// ----- Wire-reader behavior at the entry edge (Section 9) -----

/// The two wire-reader scenarios share `WIRE_A_DELIVERED` /
/// `WIRE_B_DELIVERED` statics through their Procedure impls. Serialize them
/// so cargo's per-binary parallelism doesn't cross-contaminate the
/// counter resets.
static WIRE_TEST_LOCK: Mutex<()> = Mutex::new(());
static WIRE_A_DELIVERED: AtomicUsize = AtomicUsize::new(0);
static WIRE_B_DELIVERED: AtomicUsize = AtomicUsize::new(0);

test_procedure!(WireSlowAProcedure, "wire_slow_a_procedure", Vec<u8> => (), (), cap 2, |_input, _state, _ctx| {
    WIRE_A_DELIVERED.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

test_procedure!(WireFastBProcedure, "wire_fast_b_procedure", Vec<u8> => (), (), |_input, _state, _ctx| {
    WIRE_B_DELIVERED.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct WireOperativeADefault;
impl StreamOperative for WireOperativeADefault {
    fn factory_id() -> &'static str {
        "wire_operative_a_default"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input").into::<WireSlowAProcedure>();
    }
}

struct WireOperativeABlock;
impl StreamOperative for WireOperativeABlock {
    fn factory_id() -> &'static str {
        "wire_operative_a_block"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<WireSlowAProcedure>();
    }
}

struct WireOperativeADropNewest;
impl StreamOperative for WireOperativeADropNewest {
    fn factory_id() -> &'static str {
        "wire_operative_a_drop_newest"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNewest)
            .into::<WireSlowAProcedure>();
    }
}

struct WireOperativeB;
impl StreamOperative for WireOperativeB {
    fn factory_id() -> &'static str {
        "wire_operative_b"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input").into::<WireFastBProcedure>();
    }
}

/// Spec scenario "A declared drop policy keeps the wire reader running":
/// when StreamOperative A's entry mailbox is artificially saturated under a
/// declared `DropNewest`, the simulated wire-reader continues dispatching
/// frames to sibling StreamOperative B without delay.
#[test]
fn declared_drop_newest_entry_does_not_stall_sibling_operatives() {
    let _serial = WIRE_TEST_LOCK.lock().unwrap();
    WIRE_A_DELIVERED.store(0, Ordering::Release);
    WIRE_B_DELIVERED.store(0, Ordering::Release);

    let registry = Arc::new(OperativeRegistry::new());

    let topology_a = build_topology_for::<WireOperativeADropNewest>().expect("topology a");
    let topology_b = build_topology_for::<WireOperativeB>().expect("topology b");
    let instance_a = StreamInstance::new(topology_a, Arc::from("wire-a-drop"));
    let instance_b = StreamInstance::new(topology_b, Arc::from("wire-b-drop"));
    registry.register_stream(Arc::clone(&instance_a));
    registry.register_stream(Arc::clone(&instance_b));

    // Saturate A's mailbox; pre-acquire inflight so A's drain does not
    // make room for new frames during the test.
    let slot_a = Arc::clone(&instance_a.entities[0][0]);
    assert!(slot_a.mailbox.claim_inflight());

    let handle = OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();
    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());

    // Wire-reader-shaped loop: alternate frames addressed to A and B
    // 50 times each. A's mailbox saturates at cap 2 → subsequent A
    // frames hit the declared DropNewest and the loop continues without delay.
    let start = Instant::now();
    for _ in 0..50 {
        handle.dispatch("wire-a-drop", b"a-frame".to_vec(), &md, Arc::clone(&stream));
        handle.dispatch("wire-b-drop", b"b-frame".to_vec(), &md, Arc::clone(&stream));
    }
    let dispatch_elapsed = start.elapsed();

    // The dispatch loop never parks: 100 dispatch calls must complete
    // far quicker than would be possible if the reader had blocked.
    assert!(
        dispatch_elapsed < Duration::from_millis(500),
        "declared-drop dispatch took {dispatch_elapsed:?}; loop should not park"
    );

    // Release A's inflight + drain B → B's 50 frames all land.
    slot_a.mailbox.release_inflight();
    assert!(wait_until(
        || WIRE_B_DELIVERED.load(Ordering::Acquire) == 50,
        Duration::from_secs(5)
    ));

    // A's counters: pushes_total = 2 (the cap) + drops_newest_total = 48.
    let counters_a = slot_a.counters();
    assert_eq!(counters_a.pushes_total.load(Ordering::Relaxed), 2);
    assert_eq!(counters_a.drops_newest_total.load(Ordering::Relaxed), 48);
    assert_eq!(counters_a.pushes_blocked_total.load(Ordering::Relaxed), 0);
}

/// Spec scenarios "Default entry policy discards nothing" and "Default entry
/// parks the reader and loses nothing": an entry channel declared without
/// `.on_full(...)` waits exactly like a declared `DropNothing`.
#[test]
fn default_entry_policy_waits_and_loses_nothing() {
    entry_drop_nothing_parks_reader_until_pool_drains::<WireOperativeADefault>("default");
}

#[test]
fn declared_entry_drop_nothing_parks_the_reader_until_the_pool_drains() {
    entry_drop_nothing_parks_reader_until_pool_drains::<WireOperativeABlock>("block");
}

/// Shared body of the entry-edge `Block` scenarios: when StreamOperative A's
/// entry channel saturates while A's round is queued but not yet served, the
/// wire-reader thread parks on the push. It never runs A's procedures itself
/// — serving a consumer inline is a pool thread's move; a reader doing it
/// would run one operative's work while every sibling on the connection waits
/// — so B's frame, queued behind the parked push, waits too. Once the pool
/// serves A's round, a pop unparks the reader and every frame lands, none
/// lost and none counted as dropped.
fn entry_drop_nothing_parks_reader_until_pool_drains<A: StreamOperative>(tag: &str) {
    let _serial = WIRE_TEST_LOCK.lock().unwrap();
    WIRE_A_DELIVERED.store(0, Ordering::Release);
    WIRE_B_DELIVERED.store(0, Ordering::Release);

    let registry = Arc::new(OperativeRegistry::new());

    let id_a = format!("wire-a-{tag}");
    let id_b = format!("wire-b-{tag}");
    let topology_a = build_topology_for::<A>().expect("topology a");
    let topology_b = build_topology_for::<WireOperativeB>().expect("topology b");
    let instance_a = StreamInstance::new(topology_a, Arc::from(id_a.as_str()));
    let instance_b = StreamInstance::new(topology_b, Arc::from(id_b.as_str()));
    registry.register_stream(Arc::clone(&instance_a));
    registry.register_stream(Arc::clone(&instance_b));

    // A's round is marked inflight but not handed to the pool, so nothing
    // drains A until the test schedules it below.
    let slot_a = Arc::clone(&instance_a.entities[0][0]);
    assert!(slot_a.mailbox.claim_inflight());
    let blocked_before = flezha_runner::entity::pushes_blocked_total();

    let handle = OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();

    let reader_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let completed_clone = Arc::clone(&reader_completed);
    let handle_clone = handle.clone();
    let stream_clone = Arc::clone(&stream);
    let reader = std::thread::spawn(move || {
        let mut md = HashMap::new();
        md.insert("channel".to_string(), "input".to_string());
        for _ in 0..3 {
            handle_clone.dispatch(&id_a, b"a-frame".to_vec(), &md, Arc::clone(&stream_clone));
        }
        handle_clone.dispatch(&id_b, b"b-frame".to_vec(), &md, Arc::clone(&stream_clone));
        completed_clone.store(true, Ordering::Release);
    });

    // The third A push finds the cap-2 mailbox full: the reader parks there,
    // runs nothing, and B's frame behind it has not been dispatched. Two
    // counted wait iterations (the park is bounded at 50 ms) prove it stayed.
    assert!(wait_until(
        || {
            slot_a
                .counters()
                .pushes_blocked_total
                .load(Ordering::Relaxed)
                >= 2
        },
        Duration::from_secs(5)
    ));
    assert!(
        !reader_completed.load(Ordering::Acquire),
        "the wire reader must park on A's saturated Block mailbox, not drain it inline"
    );
    assert_eq!(WIRE_A_DELIVERED.load(Ordering::Acquire), 0);
    assert_eq!(WIRE_B_DELIVERED.load(Ordering::Acquire), 0);
    assert!(
        slot_a
            .counters()
            .pushes_blocked_total
            .load(Ordering::Relaxed)
            >= 1
    );
    // Per WAIT ITERATION, not once per episode. A counter that bumped once
    // would leave this runner showing a frozen dispatch count beside a frozen
    // block count — byte for byte the picture a wedged runner shows, which is
    // the distinction the controller's stall classifier hangs off. The park is
    // bounded at 50 ms, so 300 ms of blocking is several iterations.
    let climbed = flezha_runner::entity::pushes_blocked_total() - blocked_before;
    assert!(
        climbed > 1,
        "a sustained block episode must keep the counter moving; it advanced {climbed}"
    );

    // Hand A's queued round to the pool: its first pop unparks the reader.
    flz_core::poll_core::global().schedule(&slot_a);

    assert!(
        wait_until(
            || reader_completed.load(Ordering::Acquire),
            Duration::from_secs(5)
        ),
        "wire reader stayed parked after the pool drained A"
    );
    reader.join().expect("reader thread panicked");

    assert!(wait_until(
        || {
            WIRE_A_DELIVERED.load(Ordering::Acquire) == 3
                && WIRE_B_DELIVERED.load(Ordering::Acquire) == 1
        },
        Duration::from_secs(5)
    ));

    let counters_a = slot_a.counters();
    assert_eq!(counters_a.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters_a.drops_oldest_total.load(Ordering::Relaxed), 0);
}
