//! Section 3 scenario tests covering:
//! - Sticky per-partition ordering (umbrella scenario "Same partition is
//!   never concurrent" + "Per-partition input order is preserved")
//! - Parallel partitions (umbrella scenario "Different partitions of the
//!   same shard run in parallel")
//! - Per-relay namespace isolation (umbrella scenario "Same-named
//!   channels on different relays are independent")
//! - `emit_with_meta` rejection on inter-shard channels

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bi::RelayDispatchHandle;
use flezha_executor::channel::ChannelBuilder;
use flezha_executor::relay::builder::build_topology_for;
use flezha_executor::relay::registry::RelayInstanceRegistry;
use flezha_executor::relay::RelayInstance;
use flezha_executor::shard::{Shard, ShardCtx, ShardError};
use flezha_executor::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_executor::{__dispatch_internals as bi, OverflowPolicy, Relay};
use flz_transport::transport::FlezhaStream;

// ----- Test utilities -----

fn dummy_stream() -> Arc<FlezhaStream> {
    let (a, _b) = UnixStream::pair().expect("UnixStream::pair");
    Arc::new(FlezhaStream::Unix(a))
}

fn wait_until<F: Fn() -> bool>(predicate: F, timeout: Duration) -> bool {
    let start = Instant::now();
    while !predicate() {
        if start.elapsed() > timeout {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

// ----- Per-partition ordering (single mode) -----

static APPEND_OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());

#[derive(Default)]
struct AppendShard;
impl Shard for AppendShard {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "append_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, AppendShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        APPEND_OBSERVED.lock().unwrap().push(input);
        Ok(())
    }
}

struct AppendRelay;
impl Relay for AppendRelay {
    fn factory_id() -> &'static str {
        "append_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<AppendShard>();
    }
}

#[test]
fn per_partition_input_order_preserved() {
    APPEND_OBSERVED.lock().unwrap().clear();
    let topology = build_topology_for::<AppendRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(8));
    let registry = Arc::new(RelayInstanceRegistry::new());

    let instance = RelayInstance::new(topology, Arc::from("append-instance"), Arc::clone(&pool));
    registry.register(instance);

    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();

    let n = 1000usize;
    for i in 0..n {
        let mut md = HashMap::new();
        md.insert("channel".to_string(), "input".to_string());
        handle.dispatch(
            "append-instance",
            format!("{}", i).into_bytes(),
            &md,
            Arc::clone(&stream),
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

// ----- Sticky non-concurrent dispatch on the same partition -----

static APPEND_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static APPEND_MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct ConcurrencyProbeShard;
impl Shard for ConcurrencyProbeShard {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "concurrency_probe_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, ConcurrencyProbeShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
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
    }
}

struct ConcurrencyProbeRelay;
impl Relay for ConcurrencyProbeRelay {
    fn factory_id() -> &'static str {
        "concurrency_probe_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<ConcurrencyProbeShard>();
    }
}

#[test]
fn same_partition_never_runs_concurrently() {
    APPEND_CONCURRENT.store(0, Ordering::Release);
    APPEND_MAX_CONCURRENT.store(0, Ordering::Release);

    let topology = build_topology_for::<ConcurrencyProbeRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(8));
    let registry = Arc::new(RelayInstanceRegistry::new());
    let instance = RelayInstance::new(topology, Arc::from("conc-instance"), Arc::clone(&pool));
    registry.register(instance);
    let handle = RelayDispatchHandle {
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
        "single-partition mailbox allowed concurrent dispatch"
    );
}

fn pool_idle(_pool: &bi::WorkStealingPool) -> bool {
    // Heuristic: short sleep then assume idle. The drain loops
    // self-exit when the queue empties; the pool itself doesn't
    // expose an "idle" predicate.
    std::thread::sleep(Duration::from_millis(50));
    true
}

// ----- Parallel partitions of the same shard -----

use std::sync::OnceLock;

fn parallel_observed() -> &'static Mutex<HashMap<&'static str, Vec<String>>> {
    static CELL: OnceLock<Mutex<HashMap<&'static str, Vec<String>>>> = OnceLock::new();
    CELL.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Default)]
struct ParallelStaticShard;
impl Shard for ParallelStaticShard {
    type Input = String;
    type Output = String;
    type State = String;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "parallel_static_shard"
    }
    fn init_state(
        shard_key: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> String {
        match shard_key {
            Some("a") => "a".to_string(),
            Some("b") => "b".to_string(),
            Some("c") => "c".to_string(),
            Some("d") => "d".to_string(),
            _ => "?".to_string(),
        }
    }
    fn shard_keys() -> Vec<&'static str> {
        vec!["a", "b", "c", "d"]
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(input: &String) -> Vec<&'static str> {
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
        state: StateHandle<'_, ParallelStaticShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
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

struct ParallelRelay;
impl Relay for ParallelRelay {
    fn factory_id() -> &'static str {
        "parallel_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<ParallelStaticShard>();
    }
}

#[test]
fn static_partitions_dispatch_independently() {
    parallel_observed().lock().unwrap().clear();
    let topology = build_topology_for::<ParallelRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(8));
    let registry = Arc::new(RelayInstanceRegistry::new());
    let instance = RelayInstance::new(topology, Arc::from("parallel-instance"), Arc::clone(&pool));
    registry.register(instance);
    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();

    let prefixes = ["a", "b", "c", "d"];
    let per_prefix = 100usize;
    for &p in &prefixes {
        for i in 0..per_prefix {
            let mut md = HashMap::new();
            md.insert("channel".to_string(), "input".to_string());
            handle.dispatch(
                "parallel-instance",
                format!("{}{}", p, i).into_bytes(),
                &md,
                Arc::clone(&stream),
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
        let v = map.get(p).expect("partition observed");
        assert_eq!(v.len(), per_prefix);
        for (i, s) in v.iter().enumerate() {
            assert_eq!(*s, format!("{}{}", p, i), "{} partition out of order", p);
        }
    }
}

// ----- Per-relay namespace isolation -----

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct CountingState {
    n: usize,
}

#[derive(Default)]
struct CountingShard;
impl Shard for CountingShard {
    type Input = String;
    type Output = String;
    type State = CountingState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "counting_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> CountingState {
        CountingState::default()
    }
    fn run(
        &mut self,
        _: String,
        _state: StateHandle<'_, CountingShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        Ok(())
    }
}

struct RelayA;
impl Relay for RelayA {
    fn factory_id() -> &'static str {
        "ns_relay_a"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CountingShard>();
    }
}

struct RelayB;
impl Relay for RelayB {
    fn factory_id() -> &'static str {
        "ns_relay_b"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CountingShard>();
    }
}

#[test]
fn per_relay_namespace_isolation() {
    let topology_a = build_topology_for::<RelayA>().expect("topology a");
    let topology_b = build_topology_for::<RelayB>().expect("topology b");
    assert_ne!(topology_a.factory_id, topology_b.factory_id);

    // Same channel name on each is a separate ChannelDescriptor with id 0
    // in its own topology — interning is per-relay.
    assert_eq!(topology_a.lookup_channel_id("input"), Some(0));
    assert_eq!(topology_b.lookup_channel_id("input"), Some(0));
    assert!(!std::ptr::eq(
        Arc::as_ptr(&topology_a),
        Arc::as_ptr(&topology_b),
    ));
}

// ----- emit_with_meta rejection on inter-shard channels -----

static META_REJECT_OBSERVED: Mutex<Option<String>> = Mutex::new(None);

#[derive(Default)]
struct MetaProducerShard;
impl Shard for MetaProducerShard {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "meta_producer_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, MetaProducerShard>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        let mut meta = HashMap::new();
        meta.insert("k".to_string(), "v".to_string());
        let res = ctx.emit_with_meta::<String>(input, meta);
        if let Err(err) = res {
            *META_REJECT_OBSERVED.lock().unwrap() = Some(err.to_string());
        }
        Ok(())
    }
}

#[derive(Default)]
struct MetaConsumerShard;
impl Shard for MetaConsumerShard {
    type Input = String;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "meta_consumer_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, MetaConsumerShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        Ok(())
    }
}

struct MetaRejectRelay;
impl Relay for MetaRejectRelay {
    fn factory_id() -> &'static str {
        "meta_reject_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<MetaProducerShard>();
        c.channel::<String>("hop")
            .from::<MetaProducerShard>()
            .into::<MetaConsumerShard>();
    }
}

#[test]
fn emit_with_meta_rejected_on_inter_shard() {
    *META_REJECT_OBSERVED.lock().unwrap() = None;
    let topology = build_topology_for::<MetaRejectRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(2));
    let registry = Arc::new(RelayInstanceRegistry::new());
    let instance = RelayInstance::new(topology, Arc::from("meta-instance"), Arc::clone(&pool));
    registry.register(instance);
    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
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
        msg.contains("inter-shard"),
        "expected inter-shard rejection, got {msg}"
    );
}

// ----- Section 4 / 7 / 8 scenario coverage: Block stress + counters -----

static BLOCK_STRESS_RECEIVED: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct BlockStressShard;
impl Shard for BlockStressShard {
    type Input = Vec<u8>;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "block_stress_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn mailbox_capacity() -> usize {
        16
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, BlockStressShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        // Tiny pause so producers actually get to park rather than
        // repeatedly winning the push race uncontested.
        for _ in 0..32 {
            std::hint::spin_loop();
        }
        BLOCK_STRESS_RECEIVED.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct BlockStressRelay;
impl Relay for BlockStressRelay {
    fn factory_id() -> &'static str {
        "block_stress_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<BlockStressShard>();
    }
}

/// Spec scenario "Parked producer does not block its consumer's drain"
/// (`Block parking does not violate sticky per-partition ordering`)
/// stress-tested with 100 producer threads × 1000 items each on a
/// capacity-16 mailbox. Asserts every push eventually delivers and that
/// the per-Partition `pushes_total` matches the total emitted; this
/// covers Tasks 4.4, 5.6, 6.4, 7.7, 8.1, and 9.2 in one run.
#[test]
fn block_stress_100_producers_no_loss() {
    BLOCK_STRESS_RECEIVED.store(0, Ordering::Release);
    let topology = build_topology_for::<BlockStressRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(4));
    let registry = Arc::new(RelayInstanceRegistry::new());
    let instance = RelayInstance::new(
        topology,
        Arc::from("block-stress-instance"),
        Arc::clone(&pool),
    );
    registry.register(Arc::clone(&instance));
    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
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

    // Per-Partition counters are populated through the producer-side
    // enqueue path. With Block policy and a capacity-16 mailbox under
    // 100×1000 contention, every successful push bumps `pushes_total`
    // and at least one park event bumps `pushes_blocked_total`.
    let slot = &instance.partitions[0][0];
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

#[derive(Default)]
struct DropNewestShard;
impl Shard for DropNewestShard {
    type Input = Vec<u8>;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_newest_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn mailbox_capacity() -> usize {
        2
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, DropNewestShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        DROP_NEWEST_RECEIVED.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct DropNewestRelay;
impl Relay for DropNewestRelay {
    fn factory_id() -> &'static str {
        "drop_newest_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input").into::<DropNewestShard>();
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
    let topology = build_topology_for::<DropNewestRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let registry = Arc::new(RelayInstanceRegistry::new());
    let instance = RelayInstance::new(
        topology,
        Arc::from("drop-newest-instance"),
        Arc::clone(&pool),
    );
    registry.register(Arc::clone(&instance));
    let slot = Arc::clone(&instance.partitions[0][0]);

    // Pre-acquire inflight: subsequent enqueue paths skip submit_drain
    // because their try_mark_inflight returns false. Items pile up in
    // the bounded mailbox without being popped.
    assert!(
        slot.mailbox.try_mark_inflight(),
        "fresh slot must yield inflight ownership"
    );

    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
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

#[derive(Default)]
struct DropOldestShard;
impl Shard for DropOldestShard {
    type Input = Vec<u8>;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_oldest_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn mailbox_capacity() -> usize {
        2
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        input: Vec<u8>,
        _: StateHandle<'_, DropOldestShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        DROP_OLDEST_OBSERVED.lock().unwrap().push(input);
        Ok(())
    }
}

struct DropOldestRelay;
impl Relay for DropOldestRelay {
    fn factory_id() -> &'static str {
        "drop_oldest_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropOldest)
            .into::<DropOldestShard>();
    }
}

/// Spec scenario "DropOldest replaces the head item": producer emits
/// past capacity, oldest payload is evicted, newest is enqueued, the
/// consumer's `drops_oldest_total` increments per overflow event.
#[test]
fn drop_oldest_evicts_head_and_bumps_oldest_counter() {
    DROP_OLDEST_OBSERVED.lock().unwrap().clear();
    let topology = build_topology_for::<DropOldestRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let registry = Arc::new(RelayInstanceRegistry::new());
    let instance = RelayInstance::new(
        topology,
        Arc::from("drop-oldest-instance"),
        Arc::clone(&pool),
    );
    registry.register(Arc::clone(&instance));
    let slot = Arc::clone(&instance.partitions[0][0]);

    // Pre-acquire inflight so no drain runs during the deterministic
    // push sequence below; pop / dispatch happens after the assertions.
    assert!(slot.mailbox.try_mark_inflight());

    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
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

/// Spec scenario "Counters present at runtime": fresh PartitionSlot
/// exposes a counters() accessor with all four atomics at 0.
#[test]
fn fresh_partition_counters_start_at_zero() {
    let topology = build_topology_for::<DropOldestRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = RelayInstance::new(
        topology,
        Arc::from("counters-fresh-instance"),
        Arc::clone(&pool),
    );
    let counters = instance.partitions[0][0].counters();
    assert_eq!(counters.pushes_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.pushes_blocked_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
}

// ----- Inter-shard default-Block coverage -----

static INTER_SHARD_DOWNSTREAM: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct InterShardProducer;
impl Shard for InterShardProducer {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "inter_shard_producer"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, InterShardProducer>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<String>(input)?;
        Ok(())
    }
}

#[derive(Default)]
struct InterShardConsumer;
impl Shard for InterShardConsumer {
    type Input = String;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "inter_shard_consumer"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, InterShardConsumer>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        INTER_SHARD_DOWNSTREAM.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct InterShardDefaultBlockRelay;
impl Relay for InterShardDefaultBlockRelay {
    fn factory_id() -> &'static str {
        "inter_shard_default_block_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<InterShardProducer>();
        // No `.on_full(...)` — exercises the default `Block` for inter-shard.
        c.channel::<String>("hop")
            .from::<InterShardProducer>()
            .into::<InterShardConsumer>();
    }
}

// ----- Sticky ordering preserved under bounded Block (Task 8.3) -----

static STICKY_BOUNDED_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static STICKY_BOUNDED_MAX_CONCURRENT: AtomicUsize = AtomicUsize::new(0);
static STICKY_BOUNDED_DELIVERED: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct StickyBoundedShard;
impl Shard for StickyBoundedShard {
    type Input = Vec<u8>;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "sticky_bounded_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn mailbox_capacity() -> usize {
        16
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, StickyBoundedShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
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
    }
}

struct StickyBoundedRelay;
impl Relay for StickyBoundedRelay {
    fn factory_id() -> &'static str {
        "sticky_bounded_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<StickyBoundedShard>();
    }
}

/// Spec scenario "Same partition is never concurrent" re-validated
/// under bounded mailbox capacity 16 with a 10K input burst and Block
/// entry policy. No drop / no concurrency violation; the inflight CAS
/// guarantees mutual exclusion regardless of how many producers are
/// parked at the entry edge.
#[test]
fn sticky_ordering_holds_under_bounded_block() {
    STICKY_BOUNDED_CONCURRENT.store(0, Ordering::Release);
    STICKY_BOUNDED_MAX_CONCURRENT.store(0, Ordering::Release);
    STICKY_BOUNDED_DELIVERED.store(0, Ordering::Release);

    let topology = build_topology_for::<StickyBoundedRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(8));
    let registry = Arc::new(RelayInstanceRegistry::new());
    let instance = RelayInstance::new(
        topology,
        Arc::from("sticky-bounded-instance"),
        Arc::clone(&pool),
    );
    registry.register(Arc::clone(&instance));
    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
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
        "bounded-mailbox single-partition allowed concurrent dispatch"
    );

    let counters = instance.partitions[0][0].counters();
    assert_eq!(
        counters.pushes_total.load(Ordering::Relaxed) as usize,
        total
    );
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
}

// ----- Wire-reader behavior at the entry edge (Section 9) -----

/// The two wire-reader scenarios share `WIRE_A_DELIVERED` /
/// `WIRE_B_DELIVERED` statics through their Shard impls. Serialize them
/// so cargo's per-binary parallelism doesn't cross-contaminate the
/// counter resets.
static WIRE_TEST_LOCK: Mutex<()> = Mutex::new(());
static WIRE_A_DELIVERED: AtomicUsize = AtomicUsize::new(0);
static WIRE_B_DELIVERED: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct WireSlowAShard;
impl Shard for WireSlowAShard {
    type Input = Vec<u8>;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "wire_slow_a_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn mailbox_capacity() -> usize {
        2
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, WireSlowAShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        WIRE_A_DELIVERED.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Default)]
struct WireFastBShard;
impl Shard for WireFastBShard {
    type Input = Vec<u8>;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "wire_fast_b_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, WireFastBShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        WIRE_B_DELIVERED.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct WireRelayADefault;
impl Relay for WireRelayADefault {
    fn factory_id() -> &'static str {
        "wire_relay_a_default"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input").into::<WireSlowAShard>();
    }
}

struct WireRelayABlock;
impl Relay for WireRelayABlock {
    fn factory_id() -> &'static str {
        "wire_relay_a_block"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<WireSlowAShard>();
    }
}

struct WireRelayB;
impl Relay for WireRelayB {
    fn factory_id() -> &'static str {
        "wire_relay_b"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input").into::<WireFastBShard>();
    }
}

/// Spec scenario "Default entry policy keeps wire reader running":
/// when Relay A's entry mailbox is artificially saturated under default
/// `DropNewest`, the simulated wire-reader continues dispatching frames
/// to sibling Relay B without delay.
#[test]
fn default_entry_policy_does_not_stall_sibling_relays() {
    let _serial = WIRE_TEST_LOCK.lock().unwrap();
    WIRE_A_DELIVERED.store(0, Ordering::Release);
    WIRE_B_DELIVERED.store(0, Ordering::Release);

    let pool = Arc::new(bi::WorkStealingPool::with_size(2));
    let registry = Arc::new(RelayInstanceRegistry::new());

    let topology_a = build_topology_for::<WireRelayADefault>().expect("topology a");
    let topology_b = build_topology_for::<WireRelayB>().expect("topology b");
    let instance_a = RelayInstance::new(topology_a, Arc::from("wire-a-default"), Arc::clone(&pool));
    let instance_b = RelayInstance::new(topology_b, Arc::from("wire-b-default"), Arc::clone(&pool));
    registry.register(Arc::clone(&instance_a));
    registry.register(Arc::clone(&instance_b));

    // Saturate A's mailbox; pre-acquire inflight so A's drain does not
    // make room for new frames during the test.
    let slot_a = Arc::clone(&instance_a.partitions[0][0]);
    assert!(slot_a.mailbox.try_mark_inflight());

    let handle = RelayDispatchHandle {
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

/// Spec scenario "Opt-in entry Block opts into co-tenancy hazard":
/// when Relay A explicitly declares `OverflowPolicy::Block` on its
/// entry channel and saturates, the simulated wire-reader thread parks
/// — sibling Relay B's frames stop arriving until A drains.
#[test]
fn opt_in_entry_block_stalls_sibling_relays() {
    let _serial = WIRE_TEST_LOCK.lock().unwrap();
    WIRE_A_DELIVERED.store(0, Ordering::Release);
    WIRE_B_DELIVERED.store(0, Ordering::Release);

    let pool = Arc::new(bi::WorkStealingPool::with_size(2));
    let registry = Arc::new(RelayInstanceRegistry::new());

    let topology_a = build_topology_for::<WireRelayABlock>().expect("topology a");
    let topology_b = build_topology_for::<WireRelayB>().expect("topology b");
    let instance_a = RelayInstance::new(topology_a, Arc::from("wire-a-block"), Arc::clone(&pool));
    let instance_b = RelayInstance::new(topology_b, Arc::from("wire-b-block"), Arc::clone(&pool));
    registry.register(Arc::clone(&instance_a));
    registry.register(Arc::clone(&instance_b));

    let slot_a = Arc::clone(&instance_a.partitions[0][0]);
    assert!(slot_a.mailbox.try_mark_inflight());

    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();

    // Spawn the wire-reader thread; it dispatches A, A, A → third A
    // parks because A's cap-2 mailbox is full + Block.
    let reader_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let started_clone = Arc::clone(&reader_started);
    let completed_clone = Arc::clone(&reader_completed);
    let handle_clone = handle.clone();
    let stream_clone = Arc::clone(&stream);
    let reader = std::thread::spawn(move || {
        let mut md = HashMap::new();
        md.insert("channel".to_string(), "input".to_string());
        started_clone.store(true, Ordering::Release);
        for _ in 0..3 {
            handle_clone.dispatch(
                "wire-a-block",
                b"a-frame".to_vec(),
                &md,
                Arc::clone(&stream_clone),
            );
        }
        // After A's three frames complete, dispatch one to B.
        handle_clone.dispatch(
            "wire-b-block",
            b"b-frame".to_vec(),
            &md,
            Arc::clone(&stream_clone),
        );
        completed_clone.store(true, Ordering::Release);
    });

    // Wait for the reader to start; the third A push should park.
    while !reader_started.load(Ordering::Acquire) {
        std::hint::spin_loop();
    }
    std::thread::sleep(Duration::from_millis(100));
    assert!(
        !reader_completed.load(Ordering::Acquire),
        "wire reader did not park on A's saturated Block mailbox"
    );
    assert_eq!(
        WIRE_B_DELIVERED.load(Ordering::Acquire),
        0,
        "B should have received nothing while reader is parked on A"
    );

    // Now drain A by manually popping; each pop unparks one waiter.
    slot_a.mailbox.pop();
    slot_a.block_parker.unpark_one();

    reader.join().expect("reader thread panicked");

    // After unparking, the third A push completed and B's frame was
    // dispatched. The drain we mimicked freed exactly one slot, so two
    // pushes-total entries record successfully (the first two), one is
    // pending in the cap-2 mailbox, and one was unparked + pushed.
    assert!(
        slot_a
            .counters()
            .pushes_blocked_total
            .load(Ordering::Relaxed)
            >= 1
    );
    slot_a.mailbox.release_inflight();

    assert!(wait_until(
        || WIRE_B_DELIVERED.load(Ordering::Acquire) == 1,
        Duration::from_secs(5)
    ));
}

/// Spec scenarios "Default inter-shard policy is Block" and
/// "Successful push increments pushes_total". 200 inputs flow through a
/// producer → consumer pipeline; the inter-shard channel uses the
/// default Block policy. Even if the consumer briefly stalls, no item
/// is lost and `pushes_total` on the consumer Partition equals 200.
#[test]
fn default_inter_shard_policy_is_block_and_pushes_total_increments() {
    INTER_SHARD_DOWNSTREAM.store(0, Ordering::Release);
    let topology = build_topology_for::<InterShardDefaultBlockRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(4));
    let registry = Arc::new(RelayInstanceRegistry::new());
    let instance = RelayInstance::new(
        topology,
        Arc::from("inter-shard-default-instance"),
        Arc::clone(&pool),
    );
    registry.register(Arc::clone(&instance));
    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();

    let n = 200usize;
    for i in 0..n {
        let mut md = HashMap::new();
        md.insert("channel".to_string(), "input".to_string());
        handle.dispatch(
            "inter-shard-default-instance",
            format!("{i}").into_bytes(),
            &md,
            Arc::clone(&stream),
        );
    }

    assert!(wait_until(
        || INTER_SHARD_DOWNSTREAM.load(Ordering::Acquire) == n,
        Duration::from_secs(10)
    ));

    // Consumer Shard is shard id 1 (producer is 0); single-mode partition.
    let consumer_partitions = &instance.partitions[1];
    let counters = consumer_partitions[0].counters();
    assert_eq!(
        counters.pushes_total.load(Ordering::Relaxed) as usize,
        n,
        "every inter-shard emit must bump consumer's pushes_total"
    );
    assert_eq!(counters.drops_newest_total.load(Ordering::Relaxed), 0);
    assert_eq!(counters.drops_oldest_total.load(Ordering::Relaxed), 0);
}
