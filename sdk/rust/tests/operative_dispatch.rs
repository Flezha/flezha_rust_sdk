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
use flezha_executor::operative::ContinuousInstance;
use flezha_executor::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_executor::{__dispatch_internals as bi, ContinuousOperative, OverflowPolicy};

mod common;
use common::{dispatch_input, dummy_stream, setup, wait_until};

// ----- Per-segment ordering (single mode) -----

static APPEND_OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());

test_operation!(AppendOperation, "append_operation", String => String, (), |input, _state, _ctx| {
    APPEND_OBSERVED.lock().unwrap().push(input);
    Ok(())
});

struct AppendOperative;
impl ContinuousOperative for AppendOperative {
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
impl ContinuousOperative for ConcurrencyProbeOperative {
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
    let instance = ContinuousInstance::new(topology, Arc::from("conc-instance"), Arc::clone(&pool));
    registry.register_continuous(instance);
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
impl ContinuousOperative for ParallelOperative {
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

struct ContinuousA;
impl ContinuousOperative for ContinuousA {
    fn factory_id() -> &'static str {
        "ns_operative_a"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CountingOperation>();
    }
}

struct ContinuousB;
impl ContinuousOperative for ContinuousB {
    fn factory_id() -> &'static str {
        "ns_operative_b"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CountingOperation>();
    }
}

#[test]
fn per_operative_namespace_isolation() {
    let topology_a = build_topology_for::<ContinuousA>().expect("topology a");
    let topology_b = build_topology_for::<ContinuousB>().expect("topology b");
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
impl ContinuousOperative for MetaRejectOperative {
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
impl ContinuousOperative for BlockStressOperative {
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
impl ContinuousOperative for DropNewestOperative {
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
impl ContinuousOperative for DropOldestOperative {
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
impl ContinuousOperative for InterOperationDefaultBlockOperative {
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
impl ContinuousOperative for StickyBoundedOperative {
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
impl ContinuousOperative for WireOperativeADefault {
    fn factory_id() -> &'static str {
        "wire_operative_a_default"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input").into::<WireSlowAOperation>();
    }
}

struct WireOperativeABlock;
impl ContinuousOperative for WireOperativeABlock {
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
impl ContinuousOperative for WireOperativeB {
    fn factory_id() -> &'static str {
        "wire_operative_b"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input").into::<WireFastBOperation>();
    }
}

/// Spec scenario "Default entry policy keeps wire reader running":
/// when ContinuousOperative A's entry mailbox is artificially saturated under default
/// `DropNewest`, the simulated wire-reader continues dispatching frames
/// to sibling ContinuousOperative B without delay.
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
        ContinuousInstance::new(topology_a, Arc::from("wire-a-default"), Arc::clone(&pool));
    let instance_b =
        ContinuousInstance::new(topology_b, Arc::from("wire-b-default"), Arc::clone(&pool));
    registry.register_continuous(Arc::clone(&instance_a));
    registry.register_continuous(Arc::clone(&instance_b));

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
/// when ContinuousOperative A's explicit `OverflowPolicy::Block` entry channel
/// saturates while A's drain exists only as a scheduled-but-not-running
/// pool bounded (inflight marked, no active drainer), the wire-reader
/// thread drains A inline rather than parking — sibling ContinuousOperative B keeps
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
        ContinuousInstance::new(topology_a, Arc::from("wire-a-block"), Arc::clone(&pool));
    let instance_b =
        ContinuousInstance::new(topology_b, Arc::from("wire-b-block"), Arc::clone(&pool));
    registry.register_continuous(Arc::clone(&instance_a));
    registry.register_continuous(Arc::clone(&instance_b));

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
impl ContinuousOperative for FlushOrderOperative {
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

    FLUSH_ORDER_LOG.lock().unwrap().clear();
    // WAL sink records every shipped change frame on the shared log.
    let sink: SendFrameFn = Arc::new(|_frame: &[u8]| -> Result<(), SendError> {
        FLUSH_ORDER_LOG.lock().unwrap().push("wal");
        Ok(())
    });
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
impl ContinuousOperative for PanicOperative {
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
impl ContinuousOperative for BlockDeadlockOperative {
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
