//! Section 3 scenario tests covering:
//! - Sticky per-segment ordering (umbrella scenario "Same segment is
//!   never concurrent" + "Per-segment input order is preserved")
//! - Parallel segments (umbrella scenario "Different segments of the
//!   same operation run in parallel")
//! - Per-operative namespace isolation (umbrella scenario "Same-named
//!   channels on different operatives are independent")
//! - `emit_with_meta` rejection on inter-operation channels

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bi::OperativeDispatchHandle;
use flezha_executor::channel::ChannelBuilder;
use flezha_executor::operation::{Operation, OperationCtx, OperationError};
use flezha_executor::operative::builder::build_topology_for;
use flezha_executor::operative::registry::OperativeRegistry;
use flezha_executor::operative::UnboundedInstance;
use flezha_executor::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_executor::{__dispatch_internals as bi, OverflowPolicy, UnboundedOperative};

mod common;
use common::{dispatch_input, dummy_stream, setup, wait_until};

// ----- Per-segment ordering (single mode) -----

static APPEND_OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());

test_operation!(AppendOperation, "append_operation", String => String, (), |input, _state, _ctx| {
    APPEND_OBSERVED.lock().unwrap().push(input);
    Ok(())
});

struct AppendOperative;
impl UnboundedOperative for AppendOperative {
    fn factory_id() -> &'static str {
        "append_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<AppendOperation>();
    }
}

#[test]
fn per_segment_input_order_preserved() {
    APPEND_OBSERVED.lock().unwrap().clear();
    let (handle, _instance) = setup::<AppendOperative>("append-instance", 8);
    let stream = dummy_stream();

    let n = 1000usize;
    for i in 0..n {
        dispatch_input(
            &handle,
            "append-instance",
            "input",
            format!("{}", i).into_bytes(),
            &stream,
        );
    }

    assert!(
        wait_until(
            || APPEND_OBSERVED.lock().unwrap().len() == n,
            Duration::from_secs(5)
        ),
        "drain did not complete: {} of {}",
        APPEND_OBSERVED.lock().unwrap().len(),
        n
    );

    let observed = APPEND_OBSERVED.lock().unwrap().clone();
    for (i, s) in observed.iter().enumerate() {
        assert_eq!(*s, format!("{}", i), "out-of-order at {}", i);
    }
}

// ----- Sticky non-concurrent dispatch on the same segment -----

static APPEND_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static APPEND_MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);

test_operation!(ConcurrencyProbeOperation, "concurrency_probe_operation", String => String, (), |_input, _state, _ctx| {
    let cur = APPEND_CONCURRENT.fetch_add(1, Ordering::AcqRel) + 1;
    let mut prev = APPEND_MAX_CONCURRENT.load(Ordering::Acquire);
    while cur > prev {
        match APPEND_MAX_CONCURRENT.compare_exchange_weak(
            prev,
            cur,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => break,
            Err(p) => prev = p,
        }
    }
    for _ in 0..50 {
        std::hint::spin_loop();
    }
    APPEND_CONCURRENT.fetch_sub(1, Ordering::AcqRel);
    Ok(())
});

struct ConcurrencyProbeOperative;
impl UnboundedOperative for ConcurrencyProbeOperative {
    fn factory_id() -> &'static str {
        "concurrency_probe_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input")
            .into::<ConcurrencyProbeOperation>();
    }
}

#[test]
fn same_segment_never_runs_concurrently() {
    APPEND_CONCURRENT.store(0, Ordering::Release);
    APPEND_MAX_CONCURRENT.store(0, Ordering::Release);

    let topology = build_topology_for::<ConcurrencyProbeOperative>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(8));
    let registry = Arc::new(OperativeRegistry::new());
    let instance = UnboundedInstance::new(topology, Arc::from("conc-instance"), Arc::clone(&pool));
    registry.register_unbounded(instance);
    let handle = OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();

    let total = 2_000usize;
    let producers = 4usize;
    let mut threads = Vec::new();
    for _ in 0..producers {
        let handle = handle.clone();
        let stream = Arc::clone(&stream);
        threads.push(std::thread::spawn(move || {
            for i in 0..(total / producers) {
                let mut md = HashMap::new();
                md.insert("channel".to_string(), "input".to_string());
                handle.dispatch(
                    "conc-instance",
                    format!("{}", i).into_bytes(),
                    &md,
                    Arc::clone(&stream),
                );
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }

    // Wait for drain.
    assert!(wait_until(
        || APPEND_CONCURRENT.load(Ordering::Acquire) == 0 && pool_idle(&pool),
        Duration::from_secs(10)
    ));

    assert_eq!(
        APPEND_MAX_CONCURRENT.load(Ordering::Acquire),
        1,
        "single-segment mailbox allowed concurrent dispatch"
    );
}

fn pool_idle(_pool: &bi::WorkStealingPool) -> bool {
    // Heuristic: short sleep then assume idle. The drain loops
    // self-exit when the queue empties; the pool itself doesn't
    // expose an "idle" predicate.
    std::thread::sleep(Duration::from_millis(50));
    true
}

// ----- Parallel segments of the same operation -----

use std::sync::OnceLock;

fn parallel_observed() -> &'static Mutex<HashMap<&'static str, Vec<String>>> {
    static CELL: OnceLock<Mutex<HashMap<&'static str, Vec<String>>>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Default)]
struct ParallelStaticOperation;
impl Operation for ParallelStaticOperation {
    type Input = String;
    type Output = String;
    type State = String;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "parallel_static_operation"
    }
    fn init_state(
        operation_key: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> String {
        match operation_key {
            Some("a") => "a".to_string(),
            Some("b") => "b".to_string(),
            Some("c") => "c".to_string(),
            Some("d") => "d".to_string(),
            _ => "?".to_string(),
        }
    }
    fn operation_keys() -> Vec<&'static str> {
        vec!["a", "b", "c", "d"]
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(input: &String) -> Vec<&'static str> {
        match input.chars().next() {
            Some('a') => vec!["a"],
            Some('b') => vec!["b"],
            Some('c') => vec!["c"],
            _ => vec!["d"],
        }
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, ParallelStaticOperation>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        let key: &'static str = match state.as_str() {
            "a" => "a",
            "b" => "b",
            "c" => "c",
            "d" => "d",
            _ => "?",
        };
        parallel_observed()
            .lock()
            .unwrap()
            .entry(key)
            .or_default()
            .push(input);
        Ok(())
    }
}

struct ParallelOperative;
impl UnboundedOperative for ParallelOperative {
    fn factory_id() -> &'static str {
        "parallel_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<ParallelStaticOperation>();
    }
}

#[test]
fn static_segments_dispatch_independently() {
    parallel_observed().lock().unwrap().clear();
    let (handle, _instance) = setup::<ParallelOperative>("parallel-instance", 8);
    let stream = dummy_stream();

    let prefixes = ["a", "b", "c", "d"];
    let per_prefix = 100usize;
    for &p in &prefixes {
        for i in 0..per_prefix {
            dispatch_input(
                &handle,
                "parallel-instance",
                "input",
                format!("{}{}", p, i).into_bytes(),
                &stream,
            );
        }
    }

    assert!(wait_until(
        || {
            let map = parallel_observed().lock().unwrap();
            prefixes
                .iter()
                .all(|k| map.get(*k).map(|v| v.len() == per_prefix).unwrap_or(false))
        },
        Duration::from_secs(5)
    ));

    let map = parallel_observed().lock().unwrap();
    for &p in &prefixes {
        let v = map.get(p).expect("segment observed");
        assert_eq!(v.len(), per_prefix);
        for (i, s) in v.iter().enumerate() {
            assert_eq!(*s, format!("{}{}", p, i), "{} segment out of order", p);
        }
    }
}

// ----- Per-operative namespace isolation -----

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct CountingState {
    n: usize,
}

test_operation!(CountingOperation, "counting_operation", String => String, CountingState, |_input, _state, _ctx| {
    Ok(())
});

struct UnboundedA;
impl UnboundedOperative for UnboundedA {
    fn factory_id() -> &'static str {
        "ns_operative_a"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CountingOperation>();
    }
}

struct UnboundedB;
impl UnboundedOperative for UnboundedB {
    fn factory_id() -> &'static str {
        "ns_operative_b"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CountingOperation>();
    }
}

#[test]
fn per_operative_namespace_isolation() {
    let topology_a = build_topology_for::<UnboundedA>().expect("topology a");
    let topology_b = build_topology_for::<UnboundedB>().expect("topology b");
    assert_ne!(topology_a.factory_id, topology_b.factory_id);

    // Same channel name on each is a separate ChannelDescriptor with id 0
    // in its own topology — interning is per-operative.
    assert_eq!(topology_a.lookup_channel_id("input"), Some(0));
    assert_eq!(topology_b.lookup_channel_id("input"), Some(0));
    assert!(!std::ptr::eq(
        Arc::as_ptr(&topology_a),
        Arc::as_ptr(&topology_b),
    ));
}

// ----- emit_with_meta rejection on inter-operation channels -----

static META_REJECT_OBSERVED: Mutex<Option<String>> = Mutex::new(None);

test_operation!(MetaProducerOperation, "meta_producer_operation", String => String, (), |input, _state, ctx| {
    let mut meta = HashMap::new();
    meta.insert("k".to_string(), "v".to_string());
    let res = ctx.emit_with_meta::<String>(input, meta);
    if let Err(err) = res {
        *META_REJECT_OBSERVED.lock().unwrap() = Some(err.to_string());
    }
    Ok(())
});

test_operation!(MetaConsumerOperation, "meta_consumer_operation", String => (), (), |_input, _state, _ctx| {
    Ok(())
});

struct MetaRejectOperative;
impl UnboundedOperative for MetaRejectOperative {
    fn factory_id() -> &'static str {
        "meta_reject_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<MetaProducerOperation>();
        c.channel::<String>("hop")
            .from::<MetaProducerOperation>()
            .into::<MetaConsumerOperation>();
    }
}

#[test]
fn emit_with_meta_rejected_on_inter_operation() {
    *META_REJECT_OBSERVED.lock().unwrap() = None;
    let (handle, _instance) = setup::<MetaRejectOperative>("meta-instance", 2);
    let stream = dummy_stream();

    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    handle.dispatch("meta-instance", b"hello".to_vec(), &md, Arc::clone(&stream));

    assert!(wait_until(
        || META_REJECT_OBSERVED.lock().unwrap().is_some(),
        Duration::from_secs(2)
    ));
    let msg = META_REJECT_OBSERVED.lock().unwrap().clone().unwrap();
    assert!(
        msg.contains("inter-operation"),
        "expected inter-operation rejection, got {msg}"
    );
}

// ----- Section 4 / 7 / 8 scenario coverage: Block stress + counters -----

static BLOCK_STRESS_RECEIVED: AtomicUsize = AtomicUsize::new(0);

test_operation!(BlockStressOperation, "block_stress_operation", Vec<u8> => (), (), cap 16, |_input, _state, _ctx| {
    // Tiny pause so producers actually get to park rather than
    // repeatedly winning the push race uncontested.
    for _ in 0..32 {
        std::hint::spin_loop();
    }
    BLOCK_STRESS_RECEIVED.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct BlockStressOperative;
impl UnboundedOperative for BlockStressOperative {
    fn factory_id() -> &'static str {
        "block_stress_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<BlockStressOperation>();
    }
}

/// Spec scenario "Parked producer does not block its consumer's drain"
/// (`Block parking does not violate sticky per-segment ordering`)
/// stress-tested with 100 producer threads × 1000 items each on a
/// capacity-16 mailbox. Asserts every push eventually delivers and that
/// the per-Segment `pushes_total` matches the total emitted; this
/// covers Tasks 4.4, 5.6, 6.4, 7.7, 8.1, and 9.2 in one run.
#[test]
fn block_stress_100_producers_no_loss() {
    BLOCK_STRESS_RECEIVED.store(0, Ordering::Release);
    let (handle, instance) = setup::<BlockStressOperative>("block-stress-instance", 4);
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
            || BLOCK_STRESS_RECEIVED.load(Ordering::Acquire) == total,
            Duration::from_secs(60)
        ),
        "block-stress: only {} of {} delivered",
        BLOCK_STRESS_RECEIVED.load(Ordering::Acquire),
        total
    );

    // Per-Segment counters are populated through the producer-side
    // enqueue path. With Block policy and a capacity-16 mailbox under
    // 100×1000 contention, every successful push bumps `pushes_total`
    // and at least one park event bumps `pushes_blocked_total`.
    let slot = &instance.segments[0][0];
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

test_operation!(DropNewestOperation, "drop_newest_operation", Vec<u8> => (), (), cap 2, |_input, _state, _ctx| {
    DROP_NEWEST_RECEIVED.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct DropNewestOperative;
impl UnboundedOperative for DropNewestOperative {
    fn factory_id() -> &'static str {
        "drop_newest_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input").into::<DropNewestOperation>();
    }
}

/// Spec scenarios "DropNewest discards the incoming item",
/// "Default entry policy is DropNewest", "DropNewest increments only
/// drops_newest_total". Pre-marks the slot's inflight CAS so no drain
/// runs during the test — that keeps the cap-2 mailbox saturated while
/// we exercise the DropNewest path deterministically.
#[test]
fn drop_newest_discards_when_full_and_only_bumps_newest_counter() {
    DROP_NEWEST_RECEIVED.store(0, Ordering::Release);
    let (handle, instance) = setup::<DropNewestOperative>("drop-newest-instance", 1);
    let slot = Arc::clone(&instance.segments[0][0]);

    // Pre-acquire inflight: subsequent enqueue paths skip submit_drain
    // because their try_mark_inflight returns false. Items pile up in
    // the bounded mailbox without being popped.
    assert!(
        slot.mailbox.try_mark_inflight(),
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

    // Release inflight so the slot is in a consistent state at drop;
    // any residual items go away with the test's pool.
    slot.mailbox.release_inflight();
}

static DROP_OLDEST_OBSERVED: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());

test_operation!(DropOldestOperation, "drop_oldest_operation", Vec<u8> => (), (), cap 2, |input, _state, _ctx| {
    DROP_OLDEST_OBSERVED.lock().unwrap().push(input);
    Ok(())
});

struct DropOldestOperative;
impl UnboundedOperative for DropOldestOperative {
    fn factory_id() -> &'static str {
        "drop_oldest_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropOldest)
            .into::<DropOldestOperation>();
    }
}

/// Spec scenario "DropOldest replaces the head item": producer emits
/// past capacity, oldest payload is evicted, newest is enqueued, the
/// consumer's `drops_oldest_total` increments per overflow event.
#[test]
fn drop_oldest_evicts_head_and_bumps_oldest_counter() {
    DROP_OLDEST_OBSERVED.lock().unwrap().clear();
    let (handle, instance) = setup::<DropOldestOperative>("drop-oldest-instance", 1);
    let slot = Arc::clone(&instance.segments[0][0]);

    // Pre-acquire inflight so no drain runs during the deterministic
    // push sequence below; pop / dispatch happens after the assertions.
    assert!(slot.mailbox.try_mark_inflight());

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
    //   pushes_total counts the 2 pre-saturation successes;
    //   drops_oldest_total counts the 2 force_push evictions.
    let counters = slot.counters();
    assert_eq!(counters.pushes_total.load(Ordering::Relaxed), 2);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 2);
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.pushes_blocked_total.load(Ordering::Relaxed), 0);

    // Surface the surviving pair without going through the WS pool: pop
    // the mailbox manually and verify recency wins.
    let mut surviving = Vec::new();
    while let Some(payload) = slot.mailbox.pop() {
        let any_ref: &dyn std::any::Any = &*payload.value;
        let bytes = any_ref
            .downcast_ref::<Vec<u8>>()
            .expect("entry payload is Vec<u8>");
        surviving.push(bytes.clone());
    }
    slot.mailbox.release_inflight();
    assert_eq!(surviving, vec![b"new1".to_vec(), b"new2".to_vec()]);
}

/// Spec scenario "Counters present at runtime": fresh SegmentSlot
/// exposes a counters() accessor with all four atomics at 0.
#[test]
fn fresh_segment_counters_start_at_zero() {
    let (_handle, instance) = setup::<DropOldestOperative>("counters-fresh-instance", 1);
    let counters = instance.segments[0][0].counters();
    assert_eq!(counters.pushes_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.pushes_blocked_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
}

// ----- Inter-operation default-Block coverage -----

static INTER_OPERATION_DOWNSTREAM: AtomicUsize = AtomicUsize::new(0);

test_operation!(InterOperationProducer, "inter_operation_producer", String => String, (), |input, _state, ctx| {
    ctx.emit::<String>(input)?;
    Ok(())
});

test_operation!(InterOperationConsumer, "inter_operation_consumer", String => (), (), |_input, _state, _ctx| {
    INTER_OPERATION_DOWNSTREAM.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct InterOperationDefaultBlockOperative;
impl UnboundedOperative for InterOperationDefaultBlockOperative {
    fn factory_id() -> &'static str {
        "inter_operation_default_block_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<InterOperationProducer>();
        // No `.on_full(...)` — exercises the default `Block` for inter-operation.
        c.channel::<String>("hop")
            .from::<InterOperationProducer>()
            .into::<InterOperationConsumer>();
    }
}

// ----- Sticky ordering preserved under bounded Block (BoundedOperative 8.3) -----

static STICKY_BOUNDED_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static STICKY_BOUNDED_MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static STICKY_BOUNDED_DELIVERED: AtomicUsize = AtomicUsize::new(0);

test_operation!(StickyBoundedOperation, "sticky_bounded_operation", Vec<u8> => (), (), cap 16, |_input, _state, _ctx| {
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

struct StickyBoundedOperative;
impl UnboundedOperative for StickyBoundedOperative {
    fn factory_id() -> &'static str {
        "sticky_bounded_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<StickyBoundedOperation>();
    }
}

/// Spec scenario "Same segment is never concurrent" re-validated
/// under bounded mailbox capacity 16 with a 10K input burst and Block
/// entry policy. No drop / no concurrency violation; the inflight CAS
/// guarantees mutual exclusion regardless of how many producers are
/// parked at the entry edge.
#[test]
fn sticky_ordering_holds_under_bounded_block() {
    STICKY_BOUNDED_CONCURRENT.store(0, Ordering::Release);
    STICKY_BOUNDED_MAX_CONCURRENT.store(0, Ordering::Release);
    STICKY_BOUNDED_DELIVERED.store(0, Ordering::Release);

    let (handle, instance) = setup::<StickyBoundedOperative>("sticky-bounded-instance", 8);
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
        "bounded-mailbox single-segment allowed concurrent dispatch"
    );

    let counters = instance.segments[0][0].counters();
    assert_eq!(
        counters.pushes_total.load(Ordering::Relaxed) as usize,
        total
    );
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
}

// ----- Wire-reader behavior at the entry edge (Section 9) -----

/// The two wire-reader scenarios share `WIRE_A_DELIVERED` /
/// `WIRE_B_DELIVERED` statics through their Operation impls. Serialize them
/// so cargo's per-binary parallelism doesn't cross-contaminate the
/// counter resets.
static WIRE_TEST_LOCK: Mutex<()> = Mutex::new(());
static WIRE_A_DELIVERED: AtomicUsize = AtomicUsize::new(0);
static WIRE_B_DELIVERED: AtomicUsize = AtomicUsize::new(0);

test_operation!(WireSlowAOperation, "wire_slow_a_operation", Vec<u8> => (), (), cap 2, |_input, _state, _ctx| {
    WIRE_A_DELIVERED.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

test_operation!(WireFastBOperation, "wire_fast_b_operation", Vec<u8> => (), (), |_input, _state, _ctx| {
    WIRE_B_DELIVERED.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct WireOperativeADefault;
impl UnboundedOperative for WireOperativeADefault {
    fn factory_id() -> &'static str {
        "wire_operative_a_default"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input").into::<WireSlowAOperation>();
    }
}

struct WireOperativeABlock;
impl UnboundedOperative for WireOperativeABlock {
    fn factory_id() -> &'static str {
        "wire_operative_a_block"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<WireSlowAOperation>();
    }
}

struct WireOperativeB;
impl UnboundedOperative for WireOperativeB {
    fn factory_id() -> &'static str {
        "wire_operative_b"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input").into::<WireFastBOperation>();
    }
}

/// Spec scenario "Default entry policy keeps wire reader running":
/// when UnboundedOperative A's entry mailbox is artificially saturated under default
/// `DropNewest`, the simulated wire-reader continues dispatching frames
/// to sibling UnboundedOperative B without delay.
#[test]
fn default_entry_policy_does_not_stall_sibling_operatives() {
    let _serial = WIRE_TEST_LOCK.lock().unwrap();
    WIRE_A_DELIVERED.store(0, Ordering::Release);
    WIRE_B_DELIVERED.store(0, Ordering::Release);

    let pool = Arc::new(bi::WorkStealingPool::with_size(2));
    let registry = Arc::new(OperativeRegistry::new());

    let topology_a = build_topology_for::<WireOperativeADefault>().expect("topology a");
    let topology_b = build_topology_for::<WireOperativeB>().expect("topology b");
    let instance_a =
        UnboundedInstance::new(topology_a, Arc::from("wire-a-default"), Arc::clone(&pool));
    let instance_b =
        UnboundedInstance::new(topology_b, Arc::from("wire-b-default"), Arc::clone(&pool));
    registry.register_unbounded(Arc::clone(&instance_a));
    registry.register_unbounded(Arc::clone(&instance_b));

    // Saturate A's mailbox; pre-acquire inflight so A's drain does not
    // make room for new frames during the test.
    let slot_a = Arc::clone(&instance_a.segments[0][0]);
    assert!(slot_a.mailbox.try_mark_inflight());

    let handle = OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();
    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());

    // Wire-reader-shaped loop: alternate frames addressed to A and B
    // 50 times each. A's mailbox saturates at cap 2 → subsequent A
    // frames hit DropNewest and the loop continues without delay.
    let start = Instant::now();
    for _ in 0..50 {
        handle.dispatch(
            "wire-a-default",
            b"a-frame".to_vec(),
            &md,
            Arc::clone(&stream),
        );
        handle.dispatch(
            "wire-b-default",
            b"b-frame".to_vec(),
            &md,
            Arc::clone(&stream),
        );
    }
    let dispatch_elapsed = start.elapsed();

    // The dispatch loop never parks: 100 dispatch calls must complete
    // far quicker than would be possible if the reader had blocked.
    assert!(
        dispatch_elapsed < Duration::from_millis(500),
        "default-entry dispatch took {dispatch_elapsed:?}; loop should not park"
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

/// Spec scenario "Opt-in entry Block drains inline instead of stalling":
/// when UnboundedOperative A's explicit `OverflowPolicy::Block` entry channel
/// saturates while A's drain exists only as a scheduled-but-not-running
/// pool bounded (inflight marked, no active drainer), the wire-reader
/// thread drains A inline rather than parking — sibling UnboundedOperative B keeps
/// flowing and no frame is lost. Parking here was the 2026-06-10 prod
/// self-deadlock.
#[test]
fn opt_in_entry_block_drains_inline_without_stalling_siblings() {
    let _serial = WIRE_TEST_LOCK.lock().unwrap();
    WIRE_A_DELIVERED.store(0, Ordering::Release);
    WIRE_B_DELIVERED.store(0, Ordering::Release);

    let pool = Arc::new(bi::WorkStealingPool::with_size(2));
    let registry = Arc::new(OperativeRegistry::new());

    let topology_a = build_topology_for::<WireOperativeABlock>().expect("topology a");
    let topology_b = build_topology_for::<WireOperativeB>().expect("topology b");
    let instance_a =
        UnboundedInstance::new(topology_a, Arc::from("wire-a-block"), Arc::clone(&pool));
    let instance_b =
        UnboundedInstance::new(topology_b, Arc::from("wire-b-block"), Arc::clone(&pool));
    registry.register_unbounded(Arc::clone(&instance_a));
    registry.register_unbounded(Arc::clone(&instance_b));

    // Phantom drain: inflight held but no thread is draining, so only the
    // producer itself can free the full mailbox.
    let slot_a = Arc::clone(&instance_a.segments[0][0]);
    assert!(slot_a.mailbox.try_mark_inflight());

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
            handle_clone.dispatch(
                "wire-a-block",
                b"a-frame".to_vec(),
                &md,
                Arc::clone(&stream_clone),
            );
        }
        handle_clone.dispatch(
            "wire-b-block",
            b"b-frame".to_vec(),
            &md,
            Arc::clone(&stream_clone),
        );
        completed_clone.store(true, Ordering::Release);
    });

    // The reader must complete on its own: the third A push finds the
    // cap-2 mailbox full and drains it inline instead of parking.
    assert!(
        wait_until(
            || reader_completed.load(Ordering::Acquire),
            Duration::from_secs(5)
        ),
        "wire reader stalled on A's saturated Block mailbox"
    );
    reader.join().expect("reader thread panicked");

    // The full push was recorded as blocked, then every frame landed.
    assert!(
        slot_a
            .counters()
            .pushes_blocked_total
            .load(Ordering::Relaxed)
            >= 1
    );
    assert!(wait_until(
        || {
            WIRE_A_DELIVERED.load(Ordering::Acquire) == 3
                && WIRE_B_DELIVERED.load(Ordering::Acquire) == 1
        },
        Duration::from_secs(5)
    ));
}

/// Spec scenarios "Default inter-operation policy is Block" and
/// "Successful push increments pushes_total". 200 inputs flow through a
/// producer → consumer pipeline; the inter-operation channel uses the
/// default Block policy. Even if the consumer briefly stalls, no item
/// is lost and `pushes_total` on the consumer Segment equals 200.
#[test]
fn default_inter_operation_policy_is_block_and_pushes_total_increments() {
    INTER_OPERATION_DOWNSTREAM.store(0, Ordering::Release);
    let (handle, instance) =
        setup::<InterOperationDefaultBlockOperative>("inter-operation-default-instance", 4);
    let stream = dummy_stream();

    let n = 200usize;
    for i in 0..n {
        let mut md = HashMap::new();
        md.insert("channel".to_string(), "input".to_string());
        handle.dispatch(
            "inter-operation-default-instance",
            format!("{i}").into_bytes(),
            &md,
            Arc::clone(&stream),
        );
    }

    assert!(wait_until(
        || INTER_OPERATION_DOWNSTREAM.load(Ordering::Acquire) == n,
        Duration::from_secs(10)
    ));

    // Consumer Operation is operation id 1 (producer is 0); single-mode segment.
    let consumer_segments = &instance.segments[1];
    let counters = consumer_segments[0].counters();
    assert_eq!(
        counters.pushes_total.load(Ordering::Relaxed) as usize,
        n,
        "every inter-operation emit must bump consumer's pushes_total"
    );
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
}

// ----- Flush-before-emit ordering (durability fix) -----
//
// A persistence-enabled Operation applies a Change then emits. The change's
// WAL frame must reach the durable sink BEFORE the output frame is
// written, so a hard kill can never lose a change whose output already
// shipped. Both signals are recorded on this single ordered log; the WAL
// `send` and the plugin serializer run on the same dispatch thread inside
// one `run`, so the recorded order is deterministic.

static FLUSH_ORDER_LOG: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

/// Serializes every test whose dispatches produce WAL frames: the
/// flush-order test installs a process-global recording sink, so a
/// concurrent WAL-producing test would leak "wal" entries into its log.
static WAL_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(serde::Serialize, serde::Deserialize)]
struct AddChange(i64);
impl flezha_executor::state::Change for AddChange {}

#[derive(Default)]
struct FlushOrderOperation;
impl Operation for FlushOrderOperation {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = AddChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "flush_order_operation"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
        0
    }
    fn apply_change(state: &mut i64, change: &AddChange) {
        *state += change.0;
    }
    fn run(
        &mut self,
        _: String,
        mut state: StateHandle<'_, FlushOrderOperation>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(AddChange(1));
        let total = *state;
        ctx.emit::<String>(format!("{total}"))?;
        Ok(())
    }
}

test_operation!(FlushOrderSinkOperation, "flush_order_sink_operation", String => (), (), |_input, _state, _ctx| {
    Ok(())
});

struct FlushOrderOperative;
impl UnboundedOperative for FlushOrderOperative {
    fn factory_id() -> &'static str {
        "flush_order_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<FlushOrderOperation>();
        c.channel::<String>("out")
            .from::<FlushOrderOperation>()
            .also_to_plugin_with("probe", |_v: &String| {
                FLUSH_ORDER_LOG.lock().unwrap().push("emit");
                Vec::new()
            })
            .into::<FlushOrderSinkOperation>();
    }
}

#[test]
fn change_is_flushed_before_output_is_emitted() {
    use flezha_executor::segment::flusher::{SendError, SendFrameFn};
    use flezha_executor::segment::segment_state_sink;

    let _serial = WAL_TEST_LOCK.lock().unwrap();
    FLUSH_ORDER_LOG.lock().unwrap().clear();
    // WAL sink records every shipped change frame on the shared log.
    let sink: SendFrameFn = Arc::new(
        |_payload_id: &str, _frame: &[u8]| -> Result<(), SendError> {
            FLUSH_ORDER_LOG.lock().unwrap().push("wal");
            Ok(())
        },
    );
    segment_state_sink::install_global_sink(sink);

    let (handle, instance) = setup::<FlushOrderOperative>("flush-order-instance", 2);
    assert!(
        instance.persistence_enabled(),
        "operative must be persistent"
    );
    let stream = dummy_stream();

    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    handle.dispatch(
        "flush-order-instance",
        b"go".to_vec(),
        &md,
        Arc::clone(&stream),
    );

    assert!(
        wait_until(
            || FLUSH_ORDER_LOG.lock().unwrap().len() == 2,
            Duration::from_secs(5)
        ),
        "expected both wal+emit signals, got {:?}",
        FLUSH_ORDER_LOG.lock().unwrap().clone()
    );
    assert_eq!(
        FLUSH_ORDER_LOG.lock().unwrap().clone(),
        vec!["wal", "emit"],
        "change must reach the WAL path before the output frame is written"
    );
}

static PANIC_AFTER: AtomicUsize = AtomicUsize::new(0);

test_operation!(PanicOperation, "panic_operation", String => (), (), |input, _state, _ctx| {
    if input == "boom" {
        panic!("induced operation panic");
    }
    PANIC_AFTER.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct PanicOperative;
impl UnboundedOperative for PanicOperative {
    fn factory_id() -> &'static str {
        "panic_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<PanicOperation>();
    }
}

#[test]
fn operation_panic_does_not_wedge_the_segment() {
    PANIC_AFTER.store(0, Ordering::Release);
    // The exhausted retry budget parks the poison payload. With no global
    // sink the park drops immediately (transport-less fallback), but a
    // concurrently-running sink test may have installed a non-acking sink
    // — then the park (correctly) holds the segment until acked. Ack it
    // out-of-band: park ids are `<segment_label>#<process nonce>`, and
    // this binary parks only here, so a small nonce sweep always hits.
    let _acker = std::thread::spawn(|| {
        for _ in 0..200 {
            for nonce in 0..32 {
                flezha_executor::segment::park::complete_ack(&format!(
                    "panic-instance/panic_operation/#{nonce}"
                ));
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    });
    let (handle, instance) = setup::<PanicOperative>("panic-instance", 4);
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "panic-instance",
        "input",
        b"boom".to_vec(),
        &stream,
    );
    dispatch_input(
        &handle,
        "panic-instance",
        "input",
        b"after".to_vec(),
        &stream,
    );

    assert!(
        wait_until(
            || PANIC_AFTER.load(Ordering::Acquire) == 1,
            Duration::from_secs(5)
        ),
        "segment wedged after a operation panic: 'after' was never processed"
    );

    assert!(
        instance.segments[0][0].state.lock().is_ok(),
        "segment state mutex was poisoned by the operation panic"
    );
}

// ----- Block-policy producer on a one-worker pool must not self-deadlock -----
//
// The single pool worker, mid-producer-dispatch, emits into a full Block
// consumer mailbox whose drain bounded sits queued behind the producer's own
// bounded. Parking the worker there is a permanent self-deadlock (the freeze
// that took prod down on 2026-06-10); the Block arm must drain the full
// slot inline instead.

static BLOCKED_CONSUMED: AtomicUsize = AtomicUsize::new(0);

test_operation!(BlockProducerOperation, "block_producer_operation", String => String, (), |_input, _state, ctx| {
    for i in 0..8 {
        ctx.emit(format!("v{}", i))?;
    }
    Ok(())
});

test_operation!(BlockedConsumerOperation, "blocked_consumer_operation", String => String, (), cap 1, |_input, _state, _ctx| {
    BLOCKED_CONSUMED.fetch_add(1, Ordering::AcqRel);
    Ok(())
});

struct BlockDeadlockOperative;
impl UnboundedOperative for BlockDeadlockOperative {
    fn factory_id() -> &'static str {
        "block_deadlock_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<BlockProducerOperation>();
        // Inter-operation channel: OverflowPolicy defaults to Block.
        c.channel::<String>("blocked_out")
            .from::<BlockProducerOperation>()
            .into::<BlockedConsumerOperation>();
    }
}

#[test]
fn block_policy_emit_on_single_worker_pool_does_not_deadlock() {
    BLOCKED_CONSUMED.store(0, Ordering::Release);
    let (handle, _instance) = setup::<BlockDeadlockOperative>("block-deadlock-instance", 1);
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "block-deadlock-instance",
        "input",
        b"go".to_vec(),
        &stream,
    );

    assert!(
        wait_until(
            || BLOCKED_CONSUMED.load(Ordering::Acquire) == 8,
            Duration::from_secs(10)
        ),
        "Block-policy emit deadlocked the one-worker pool: {} of 8 consumed",
        BLOCKED_CONSUMED.load(Ordering::Acquire)
    );
}

// ----- Output identity: keyed-channel stamps, byte-identity, replay -----

use std::io::Read;
use std::os::unix::net::UnixStream;

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

/// Socketpair whose write half is the dispatch stream and whose read half
/// lets the test observe emitted plugin frames.
fn stream_pair() -> (Arc<FlezhaStream>, UnixStream) {
    let (a, b) = UnixStream::pair().expect("UnixStream::pair");
    b.set_nonblocking(true).expect("set_nonblocking");
    (Arc::new(FlezhaStream::Unix(a)), b)
}

struct WireFrame {
    raw: Vec<u8>,
    message: Vec<u8>,
    metadata: HashMap<String, String>,
}

/// Parse complete payloader frames from a byte buffer:
/// `[payload_id(32)][payload_size(4)][message_size(4)][message]
///  [metadata_size(2)][metadata]`, metadata pairs
/// `[key_len(1)][val_len(1)][key][value]`.
fn parse_frames(mut bytes: &[u8]) -> Vec<WireFrame> {
    let mut out = Vec::new();
    while bytes.len() >= 36 {
        let payload_size = u32::from_be_bytes(bytes[32..36].try_into().unwrap()) as usize;
        if bytes.len() < 36 + payload_size {
            break;
        }
        let raw = bytes[..36 + payload_size].to_vec();
        let body = &bytes[36..36 + payload_size];
        let message_size = u32::from_be_bytes(body[..4].try_into().unwrap()) as usize;
        let message = body[4..4 + message_size].to_vec();
        let meta_size =
            u16::from_be_bytes(body[4 + message_size..6 + message_size].try_into().unwrap())
                as usize;
        let mut meta_bytes = &body[6 + message_size..6 + message_size + meta_size];
        let mut metadata = HashMap::new();
        while meta_bytes.len() >= 2 {
            let klen = meta_bytes[0] as usize;
            let vlen = meta_bytes[1] as usize;
            let key = String::from_utf8_lossy(&meta_bytes[2..2 + klen]).to_string();
            let val = String::from_utf8_lossy(&meta_bytes[2 + klen..2 + klen + vlen]).to_string();
            metadata.insert(key, val);
            meta_bytes = &meta_bytes[2 + klen + vlen..];
        }
        out.push(WireFrame {
            raw,
            message,
            metadata,
        });
        bytes = &bytes[36 + payload_size..];
    }
    out
}

/// Read from the observer socket until `expected` frames parse (or panic
/// after `timeout`).
fn read_frames(sock: &mut UnixStream, expected: usize, timeout: Duration) -> Vec<WireFrame> {
    let deadline = Instant::now() + timeout;
    let mut buf = Vec::new();
    loop {
        let mut tmp = [0u8; 4096];
        match sock.read(&mut tmp) {
            Ok(0) => {}
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => panic!("frame read failed: {e}"),
        }
        let frames = parse_frames(&buf);
        if frames.len() >= expected {
            return frames;
        }
        assert!(
            Instant::now() < deadline,
            "timed out with {} of {} frames",
            frames.len(),
            expected
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TickChange;
impl flezha_executor::state::Change for TickChange {}

/// Keyed operation: applies one change per input, emits the input under
/// itself as the natural key.
#[derive(Default)]
struct KeyedStampOperation;
impl Operation for KeyedStampOperation {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = TickChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "keyed_stamp_operation"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
        0
    }
    fn apply_change(state: &mut i64, _change: &TickChange) {
        *state += 1;
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(TickChange);
        ctx.emit_keyed::<String>(input.clone(), input)?;
        Ok(())
    }
}

struct KeyedStampOperative;
impl UnboundedOperative for KeyedStampOperative {
    fn factory_id() -> &'static str {
        "keyed_stamp_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<KeyedStampOperation>();
        c.channel::<String>("out")
            .from::<KeyedStampOperation>()
            .keyed()
            .to_plugin("kafka");
    }
}

/// Spec "Identity stamps on keyed-channel output frames": every keyed
/// plugin emit carries `flz.segment` / `flz.seq` / `flz.key` alongside the
/// `channel` stamp; `flz.seq` reflects the durable change seq after the
/// flush-before-emit drain plus the emit ordinal.
#[test]
fn keyed_channel_emits_carry_identity_stamps() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    let (handle, _instance) = setup::<KeyedStampOperative>("keyed-stamps-instance", 2);
    let (stream, mut observer) = stream_pair();

    dispatch_input(
        &handle,
        "keyed-stamps-instance",
        "input",
        b"x".to_vec(),
        &stream,
    );
    dispatch_input(
        &handle,
        "keyed-stamps-instance",
        "input",
        b"y".to_vec(),
        &stream,
    );

    let frames = read_frames(&mut observer, 2, Duration::from_secs(5));
    let f1 = &frames[0];
    assert_eq!(f1.message, b"x".to_vec());
    assert_eq!(f1.metadata.get("channel").unwrap(), "out");
    assert_eq!(
        f1.metadata.get("flz.segment").unwrap(),
        "keyed-stamps-instance/keyed_stamp_operation/"
    );
    assert_eq!(f1.metadata.get("flz.seq").unwrap(), "1.0");
    assert_eq!(f1.metadata.get("flz.key").unwrap(), "x");

    let f2 = &frames[1];
    assert_eq!(f2.metadata.get("flz.seq").unwrap(), "2.0");
    assert_eq!(f2.metadata.get("flz.key").unwrap(), "y");
}

/// Spec "Non-keyed channels are unchanged and cost-free": a non-keyed
/// plugin emit produces the exact pre-identity wire frame — byte-identical
/// to `payloader::build_from_binary` with only the `channel` stamp.
#[test]
fn non_keyed_frame_is_byte_identical_to_prechange_layout() {
    // MetaProducerOperation-free operative: reuse FlushOrderOperative's shape
    // via a minimal pure-plugin operative.
    struct PlainPluginOperative;
    impl UnboundedOperative for PlainPluginOperative {
        fn factory_id() -> &'static str {
            "plain_plugin_operative"
        }
        fn channels(c: &mut ChannelBuilder) {
            c.input::<String>("input").into::<PlainPluginOperation>();
            c.channel::<String>("out")
                .from::<PlainPluginOperation>()
                .to_plugin("kafka");
        }
    }

    let (handle, _instance) = setup::<PlainPluginOperative>("plain-plugin-instance", 2);
    let (stream, mut observer) = stream_pair();

    dispatch_input(
        &handle,
        "plain-plugin-instance",
        "input",
        b"payload".to_vec(),
        &stream,
    );

    let frames = read_frames(&mut observer, 1, Duration::from_secs(5));
    let mut expected_meta = HashMap::new();
    expected_meta.insert("channel".to_string(), "out".to_string());
    let expected = payloader::build_from_binary("plain-plugin-instance", b"payload", expected_meta);
    assert_eq!(
        frames[0].raw, expected,
        "non-keyed plugin frame must be byte-identical to the pre-change layout"
    );
    assert!(
        !frames[0].metadata.keys().any(|k| k.starts_with("flz.")),
        "non-keyed frames carry no identity stamps"
    );
}

test_operation!(PlainPluginOperation, "plain_plugin_operation", String => String, (), |input, _state, ctx| {
    ctx.emit::<String>(input)?;
    Ok(())
});

/// Build one controller-format WAL record:
/// `[seq u64 BE][len u32 BE][header 16B][change][crc32 u32 BE]`,
/// `len = 16 + change.len()`, trailing crc over `header + change`.
fn wal_record<S: Operation>(seq: u64, change_bytes: &[u8]) -> Vec<u8> {
    let header =
        flezha_executor::segment::header::HeaderTemplate::for_operation::<S>().stamp(change_bytes);
    let mut record = Vec::new();
    record.extend_from_slice(&seq.to_be_bytes());
    record.extend_from_slice(&((16 + change_bytes.len()) as u32).to_be_bytes());
    record.extend_from_slice(&header);
    record.extend_from_slice(change_bytes);
    let mut crc_input = header.to_vec();
    crc_input.extend_from_slice(change_bytes);
    record.extend_from_slice(&crc32fast::hash(&crc_input).to_be_bytes());
    record
}

/// Spec "Stamps present and replay-stable" (crash-restore e2e): after a
/// simulated crash, a fresh instance restored via the runtime's
/// `restore_state` + change_seq re-arm replays the same input and emits a
/// frame whose `flz.*` stamps (and payload) are identical to the original.
#[test]
fn keyed_stamps_replay_identically_after_crash_restore() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    // Original run: two inputs, two changes, two stamped frames.
    let (handle_a, _instance_a) = setup::<KeyedStampOperative>("restore-stamps-instance", 2);
    let (stream_a, mut observer_a) = stream_pair();
    dispatch_input(
        &handle_a,
        "restore-stamps-instance",
        "input",
        b"x".to_vec(),
        &stream_a,
    );
    dispatch_input(
        &handle_a,
        "restore-stamps-instance",
        "input",
        b"y".to_vec(),
        &stream_a,
    );
    let original = read_frames(&mut observer_a, 2, Duration::from_secs(5));

    // Crash after "x"'s change (seq 0) became durable; "y" is replayed.
    // Fresh instance, same identity: restore state from the WAL and
    // re-arm change_seq exactly like `RestoreInitState` does.
    let (handle_b, instance_b) = setup::<KeyedStampOperative>("restore-stamps-instance", 2);
    let slot = Arc::clone(&instance_b.segments[0][0]);
    let change_bytes = postcard::to_allocvec(&TickChange).expect("encode change");
    let change_log = wal_record::<KeyedStampOperation>(0, &change_bytes);
    let terminal = {
        let mut guard = slot.state.lock().unwrap();
        slot.dispatcher
            .restore_state(&mut guard, &[], &change_log, None)
            .expect("restore_state")
    };
    assert_eq!(terminal, 0);
    slot.change_seq
        .store(terminal + 1, std::sync::atomic::Ordering::Release);

    let (stream_b, mut observer_b) = stream_pair();
    dispatch_input(
        &handle_b,
        "restore-stamps-instance",
        "input",
        b"y".to_vec(),
        &stream_b,
    );
    let replayed = read_frames(&mut observer_b, 1, Duration::from_secs(5));

    let orig = &original[1];
    let replay = &replayed[0];
    assert_eq!(replay.message, orig.message);
    for stamp in ["flz.segment", "flz.seq", "flz.key"] {
        assert_eq!(
            replay.metadata.get(stamp),
            orig.metadata.get(stamp),
            "{stamp} must be replay-stable across crash-restore"
        );
    }
    assert_eq!(replay.metadata.get("flz.seq").unwrap(), "2.0");
}

// ----- Effectively-once tier: atomic effect record on the state sink -----

static TIER_SINK_FRAMES: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

#[derive(Default)]
struct TierStampOperation;
impl Operation for TierStampOperation {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = TickChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tier_stamp_operation"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
        0
    }
    fn apply_change(state: &mut i64, _change: &TickChange) {
        *state += 1;
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(TickChange);
        ctx.emit_keyed::<String>(input.clone(), input)?;
        Ok(())
    }
}

struct TierOperative;
impl UnboundedOperative for TierOperative {
    fn factory_id() -> &'static str {
        "tier_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.effectively_once();
        c.input::<String>("input").into::<TierStampOperation>();
        c.channel::<String>("out")
            .from::<TierStampOperation>()
            .keyed()
            .to_plugin("kafka");
    }
}

/// Spec "Emits leave the primary stream in tier mode" + "Offset
/// round-trips": an effectively-once invocation's keyed emit travels inside
/// a `segment_effect_record` on the reliable state sink — carrying the
/// change, the output (channel/key/payload), and the `flz.offset` the
/// input arrived with — and no output frame appears on the primary stream.
#[test]
fn tier_emits_ride_the_state_sink_as_one_effect_record() {
    use flezha_executor::segment::flusher::{EffectRecordBody, SendError, SendFrameFn};
    use flezha_executor::segment::segment_state_sink;

    let _serial = WAL_TEST_LOCK.lock().unwrap();
    TIER_SINK_FRAMES.lock().unwrap().clear();
    let sink: SendFrameFn = Arc::new(|payload_id: &str, frame: &[u8]| -> Result<(), SendError> {
        TIER_SINK_FRAMES
            .lock()
            .unwrap()
            .push((payload_id.to_string(), frame.to_vec()));
        Ok(())
    });
    segment_state_sink::install_global_sink(sink);

    let (handle, _instance) = setup::<TierOperative>("tier-instance", 2);
    let (stream, mut observer) = stream_pair();

    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    md.insert("flz.offset".to_string(), "4711".to_string());
    handle.dispatch(
        "tier-instance",
        b"BTC/1m".to_vec(),
        &md,
        Arc::clone(&stream),
    );

    // The background flusher drains the effect entry within the linger window.
    assert!(
        wait_until(
            || {
                TIER_SINK_FRAMES
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(pid, _)| pid == "segment_effect_record")
            },
            Duration::from_secs(5)
        ),
        "no segment_effect_record reached the state sink"
    );

    let frames = TIER_SINK_FRAMES.lock().unwrap().clone();
    let (_, frame) = frames
        .iter()
        .find(|(pid, _)| pid == "segment_effect_record")
        .unwrap()
        .clone();

    // Outer frame layout is the state-change layout; header format_id = 2.
    let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    let header_off = 2 + pid_len + 8;
    assert_eq!(frame[header_off + 9], 2, "effect record header format_id");
    let body_len_off = header_off + 16;
    let body_len =
        u32::from_be_bytes(frame[body_len_off..body_len_off + 4].try_into().unwrap()) as usize;
    let body: EffectRecordBody =
        postcard::from_bytes(&frame[body_len_off + 4..body_len_off + 4 + body_len]).unwrap();

    assert_eq!(body.consumed_offset, Some(4711), "offset must round-trip");
    assert_eq!(body.changes.len(), 1, "the invocation's change rides along");
    assert_eq!(body.outputs.len(), 1);
    assert_eq!(body.outputs[0].channel, "out");
    assert_eq!(body.outputs[0].key, "BTC/1m");
    assert_eq!(body.outputs[0].payload, b"BTC/1m".to_vec());

    // And the primary stream carried nothing.
    std::thread::sleep(Duration::from_millis(50));
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        match observer.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) => panic!("observer read failed: {e}"),
        }
    }
    assert!(
        parse_frames(&buf).is_empty(),
        "tier emit leaked onto the primary stream"
    );
}

/// Spec "Replay after crash" (SDK half): the restored consumed-floor makes
/// the Segment skip re-forwarded inputs it already durably consumed, and
/// process everything above — never double-applied, never lost.
#[test]
fn tier_replay_skips_inputs_at_or_below_the_consumed_floor() {
    use flezha_executor::segment::flusher::{SendError, SendFrameFn};
    use flezha_executor::segment::segment_state_sink;

    let _serial = WAL_TEST_LOCK.lock().unwrap();
    TIER_SINK_FRAMES.lock().unwrap().clear();
    let sink: SendFrameFn = Arc::new(|payload_id: &str, frame: &[u8]| -> Result<(), SendError> {
        TIER_SINK_FRAMES
            .lock()
            .unwrap()
            .push((payload_id.to_string(), frame.to_vec()));
        Ok(())
    });
    segment_state_sink::install_global_sink(sink);

    let (handle, instance) = setup::<TierOperative>("tier-replay-instance", 2);
    let (stream, _observer) = stream_pair();

    // Restore left this Segment durably consumed through offset 4711.
    instance.segments[0][0]
        .consumed_floor
        .store(4711, std::sync::atomic::Ordering::Release);

    let dispatch_with_offset = |offset: &str, body: &[u8]| {
        let mut md = HashMap::new();
        md.insert("channel".to_string(), "input".to_string());
        md.insert("flz.offset".to_string(), offset.to_string());
        handle.dispatch(
            "tier-replay-instance",
            body.to_vec(),
            &md,
            Arc::clone(&stream),
        );
    };

    // Re-forwarded tail: 4710 and 4711 already consumed, 4712 is new.
    dispatch_with_offset("4710", b"old-1");
    dispatch_with_offset("4711", b"old-2");
    dispatch_with_offset("4712", b"new-1");

    assert!(
        wait_until(
            || {
                TIER_SINK_FRAMES
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(pid, _)| pid == "segment_effect_record")
                    .count()
                    == 1
            },
            Duration::from_secs(5)
        ),
        "exactly the above-floor input must produce an effect record"
    );

    // A linger window proves no late frames appear for the skipped offsets.
    std::thread::sleep(Duration::from_millis(50));
    let effect_count = TIER_SINK_FRAMES
        .lock()
        .unwrap()
        .iter()
        .filter(|(pid, _)| pid == "segment_effect_record")
        .count();
    assert_eq!(effect_count, 1, "skipped offsets must not re-apply");
}

// ----- LogicalClock: ctx.now() == durable change seq, restore re-arm -----

static LOGICAL_OBSERVED: Mutex<Vec<u64>> = Mutex::new(Vec::new());

#[derive(Default)]
struct LogicalNowOperation;
impl Operation for LogicalNowOperation {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = TickChange;
    type Clock = flezha_executor::state::LogicalClock;
    type Config = ();
    fn name() -> &'static str {
        "logical_now_operation"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
        0
    }
    fn apply_change(state: &mut i64, _change: &TickChange) {
        *state += 1;
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        LOGICAL_OBSERVED.lock().unwrap().push(ctx.now());
        state.apply(TickChange);
        // Emit so flush-before-emit ships the change deterministically
        // (the background flusher's linger would otherwise race the test).
        ctx.emit::<String>(input)?;
        Ok(())
    }
}

test_operation!(LogicalNowSinkOperation, "logical_now_sink_operation", String => (), (), |_input, _state, _ctx| {
    Ok(())
});

struct LogicalNowOperative;
impl UnboundedOperative for LogicalNowOperative {
    fn factory_id() -> &'static str {
        "logical_now_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<LogicalNowOperation>();
        c.channel::<String>("hop")
            .from::<LogicalNowOperation>()
            .into::<LogicalNowSinkOperation>();
    }
}

/// Spec "LogicalClock serves the durable change sequence": ctx.now() is
/// the change seq at dispatch entry — advancing with applied changes —
/// and a restored instance continues from the re-armed seq rather than a
/// reset counter.
#[test]
fn logical_clock_serves_change_seq_and_survives_restore_rearm() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    LOGICAL_OBSERVED.lock().unwrap().clear();
    let (handle, instance) = setup::<LogicalNowOperative>("logical-now-instance", 2);
    let stream = dummy_stream();

    for payload in [b"a".to_vec(), b"b".to_vec()] {
        dispatch_input(&handle, "logical-now-instance", "input", payload, &stream);
    }
    assert!(wait_until(
        || LOGICAL_OBSERVED.lock().unwrap().len() == 2,
        Duration::from_secs(5)
    ));
    assert_eq!(*LOGICAL_OBSERVED.lock().unwrap(), vec![0, 1]);
    assert_eq!(
        instance.segments[0][0].change_seq.load(Ordering::Acquire),
        2
    );

    // Crash-restore: fresh instance, restore both changes (seqs 0, 1) and
    // re-arm; the next invocation's logical time continues at 2.
    LOGICAL_OBSERVED.lock().unwrap().clear();
    let (handle_b, instance_b) = setup::<LogicalNowOperative>("logical-now-instance", 2);
    let slot = Arc::clone(&instance_b.segments[0][0]);
    let change_bytes = postcard::to_allocvec(&TickChange).expect("encode change");
    let mut change_log = wal_record::<LogicalNowOperation>(0, &change_bytes);
    change_log.extend_from_slice(&wal_record::<LogicalNowOperation>(1, &change_bytes));
    let terminal = {
        let mut guard = slot.state.lock().unwrap();
        slot.dispatcher
            .restore_state(&mut guard, &[], &change_log, None)
            .expect("restore_state")
    };
    assert_eq!(terminal, 1);
    slot.change_seq
        .store(terminal + 1, std::sync::atomic::Ordering::Release);

    dispatch_input(
        &handle_b,
        "logical-now-instance",
        "input",
        b"c".to_vec(),
        &stream,
    );
    assert!(wait_until(
        || LOGICAL_OBSERVED.lock().unwrap().len() == 1,
        Duration::from_secs(5)
    ));
    assert_eq!(
        *LOGICAL_OBSERVED.lock().unwrap(),
        vec![2],
        "logical time must continue from the re-armed seq, not reset"
    );
}

// ----- Late-input correction: the event-time-ordered replay drain -----
//
// The load-bearing behaviour of `unbounded-late-input-correction`. Re-forwarding
// a replayed slice in ARRIVAL order would put the late trade back after the
// trades that overtook it and recompute exactly the wrong answer the correction
// exists to repair. So the Segment holds the slice until the controller closes
// it, orders it by extracted event time, and only then dispatches.

static REPLAY_OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct TradeTime;
impl flezha_executor::operation::TimeExtractor<String> for TradeTime {
    fn extract(input: &String) -> u64 {
        input.parse().unwrap_or(0)
    }
}

#[derive(Default)]
struct ReplayOperation;
impl Operation for ReplayOperation {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = flezha_executor::operation::EventTimeFrom<TradeTime>;
    type Config = ();
    fn name() -> &'static str {
        "replay_operation"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        _ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        REPLAY_OBSERVED.lock().unwrap().push(input);
        Ok(())
    }
}

struct ReplayOperative;
impl UnboundedOperative for ReplayOperative {
    fn factory_id() -> &'static str {
        "replay_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.effectively_once();
        c.correction_window(600_000);
        c.input::<String>("input").into::<ReplayOperation>();
        c.channel::<String>("out")
            .from::<ReplayOperation>()
            .keyed()
            .to_plugin("kafka");
    }
}

/// Dispatch one wire frame with explicit metadata (the replay marks ride there).
fn dispatch_marked(
    handle: &OperativeDispatchHandle,
    payload: &str,
    stream: &Arc<FlezhaStream>,
    marks: &[(&str, &str)],
) {
    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    for (k, v) in marks {
        md.insert(k.to_string(), v.to_string());
    }
    handle.dispatch(
        "replay-instance",
        payload.as_bytes().to_vec(),
        &md,
        Arc::clone(stream),
    );
}

#[test]
fn a_replayed_slice_dispatches_in_event_time_order() {
    REPLAY_OBSERVED.lock().unwrap().clear();
    let (handle, _instance) = setup::<ReplayOperative>("replay-instance", 4);
    let stream = dummy_stream();

    // The controller re-forwards in offset (arrival) order — the late "200"
    // having arrived after "300" is exactly why the correction is running.
    for (offset, event_time) in [(1, "300"), (2, "100"), (3, "200")] {
        dispatch_marked(
            &handle,
            event_time,
            &stream,
            &[("flz.replay", "1"), ("flz.offset", &offset.to_string())],
        );
    }

    // Held, not dispatched: the slice is still open, so ordering it would be
    // guessing.
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        REPLAY_OBSERVED.lock().unwrap().is_empty(),
        "an open slice must not dispatch"
    );

    // The controller closes the slice. A broadcast, not a mark on the last
    // input — an input reaches one Segment by key, so a mark would strand every
    // other Segment's buffered slice.
    dispatch_marked(&handle, "", &stream, &[("flz.replay_end", "1")]);

    // Post-slice live traffic. It must land AFTER the whole slice even though
    // it arrives while the slice is still buffered: the slice is older, and a
    // live input that jumped it would re-create the mis-ordering the correction
    // just repaired.
    dispatch_marked(&handle, "600", &stream, &[("flz.offset", "4")]);

    assert!(
        wait_until(
            || REPLAY_OBSERVED.lock().unwrap().len() == 4,
            Duration::from_secs(5)
        ),
        "closed slice did not drain: {:?}",
        REPLAY_OBSERVED.lock().unwrap()
    );

    assert_eq!(
        REPLAY_OBSERVED.lock().unwrap().as_slice(),
        ["100", "200", "300", "600"],
        "the slice must fold in event-time order, with live traffic after it"
    );
}

static HIGHWATER_OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());

#[derive(Default)]
struct HighWaterOperation;
impl Operation for HighWaterOperation {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = flezha_executor::operation::EventTimeFrom<TradeTime>;
    type Config = ();
    fn name() -> &'static str {
        "high_water_operation"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        _ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        HIGHWATER_OBSERVED.lock().unwrap().push(input);
        Ok(())
    }
}

struct HighWaterOperative;
impl UnboundedOperative for HighWaterOperative {
    fn factory_id() -> &'static str {
        "high_water_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.effectively_once();
        c.correction_window(600_000);
        c.input::<String>("input").into::<HighWaterOperation>();
        c.channel::<String>("out")
            .from::<HighWaterOperation>()
            .keyed()
            .to_plugin("kafka");
    }
}

fn dispatch_hw(
    handle: &OperativeDispatchHandle,
    payload: &str,
    stream: &Arc<FlezhaStream>,
    marks: &[(&str, &str)],
) {
    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    for (k, v) in marks {
        md.insert(k.to_string(), v.to_string());
    }
    handle.dispatch(
        "high-water-instance",
        payload.as_bytes().to_vec(),
        &md,
        Arc::clone(stream),
    );
}

#[test]
fn a_replay_does_not_lower_the_event_time_high_water() {
    // Regression: `dispatch_replay_slice` used to reset the high-water to -1 and
    // rebuild it from the slice alone. Every replay is preceded by a restore that
    // already set the high-water from the state it restored — so on a crash-resume
    // whose unconsumed tail carries a LOWER max event time than the recovered
    // state, that reset silently lowered the high-water, and a genuinely late input
    // afterwards would read as on-time. A missed correction, and a silent one.
    //
    // Asserted on the high-water itself rather than on dispatch behaviour: with no
    // controller transport installed a correctable input falls back to being
    // processed as an arrival, so the observable dispatch would not distinguish the
    // two. The invariant is about the water mark.
    HIGHWATER_OBSERVED.lock().unwrap().clear();
    let (handle, instance) = setup::<HighWaterOperative>("high-water-instance", 4);
    let stream = dummy_stream();

    // Live traffic establishes a high-water of 900.
    dispatch_hw(&handle, "900", &stream, &[("flz.offset", "1")]);
    assert!(wait_until(
        || HIGHWATER_OBSERVED.lock().unwrap().len() == 1,
        Duration::from_secs(5)
    ));

    let slot = Arc::clone(&instance.segments[0][0]);
    assert_eq!(slot.event_time_high_water.load(Ordering::Acquire), 900);

    // A replayed slice whose newest event time (800) is BEHIND the high-water.
    dispatch_hw(
        &handle,
        "800",
        &stream,
        &[("flz.replay", "1"), ("flz.offset", "2")],
    );
    dispatch_hw(&handle, "", &stream, &[("flz.replay_end", "1")]);
    assert!(wait_until(
        || HIGHWATER_OBSERVED.lock().unwrap().len() == 2,
        Duration::from_secs(5)
    ));

    assert_eq!(
        slot.event_time_high_water.load(Ordering::Acquire),
        900,
        "the replay lowered the high-water; a later input at 850 would now read as on-time"
    );
}
