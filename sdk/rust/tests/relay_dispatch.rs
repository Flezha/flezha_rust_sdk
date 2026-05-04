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

use flezha_compute_engine::__dispatch_internals as bi;
use flezha_compute_engine::channel::ChannelBuilder;
use flezha_compute_engine::relay::builder::build_topology_for;
use flezha_compute_engine::relay::registry::RelayInstanceRegistry;
use flezha_compute_engine::relay::RelayInstance;
use flezha_compute_engine::shard::{Shard, ShardCtx, ShardError};
use flezha_compute_engine::Relay;
use flz_transport::transport::FlezhaStream;
use bi::RelayDispatchHandle;

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
    fn name() -> &'static str {
        "append_shard"
    }
    fn init_state(_: Option<&str>) -> () {}
    fn run(
        &mut self,
        input: String,
        _: &mut (),
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

    let instance =
        RelayInstance::new(topology, Arc::from("append-instance"), Arc::clone(&pool));
    registry.register(instance);

    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
        pool: Arc::clone(&pool),
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
    fn name() -> &'static str {
        "concurrency_probe_shard"
    }
    fn init_state(_: Option<&str>) -> () {}
    fn run(
        &mut self,
        _: String,
        _: &mut (),
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
    let instance =
        RelayInstance::new(topology, Arc::from("conc-instance"), Arc::clone(&pool));
    registry.register(instance);
    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
        pool: Arc::clone(&pool),
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
        || APPEND_CONCURRENT.load(Ordering::Acquire) == 0
            && pool_idle(&pool),
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
    type State = &'static str;
    fn name() -> &'static str {
        "parallel_static_shard"
    }
    fn init_state(shard_key: Option<&str>) -> &'static str {
        match shard_key {
            Some("a") => "a",
            Some("b") => "b",
            Some("c") => "c",
            Some("d") => "d",
            _ => "?",
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
        state: &mut &'static str,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        parallel_observed()
            .lock()
            .unwrap()
            .entry(*state)
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
    let instance =
        RelayInstance::new(topology, Arc::from("parallel-instance"), Arc::clone(&pool));
    registry.register(instance);
    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
        pool: Arc::clone(&pool),
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
            prefixes.iter().all(|k| {
                map.get(*k)
                    .map(|v| v.len() == per_prefix)
                    .unwrap_or(false)
            })
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

#[derive(Default)]
struct CountingShard;
impl Shard for CountingShard {
    type Input = String;
    type Output = String;
    type State = AtomicUsize;
    fn name() -> &'static str {
        "counting_shard"
    }
    fn init_state(_: Option<&str>) -> AtomicUsize {
        AtomicUsize::new(0)
    }
    fn run(
        &mut self,
        _: String,
        state: &mut AtomicUsize,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        state.fetch_add(1, Ordering::AcqRel);
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
    fn name() -> &'static str {
        "meta_producer_shard"
    }
    fn init_state(_: Option<&str>) -> () {}
    fn run(
        &mut self,
        input: String,
        _: &mut (),
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
    fn name() -> &'static str {
        "meta_consumer_shard"
    }
    fn init_state(_: Option<&str>) -> () {}
    fn run(&mut self, _: String, _: &mut (), _: &mut ShardCtx<'_>) -> Result<(), ShardError> {
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
    let instance =
        RelayInstance::new(topology, Arc::from("meta-instance"), Arc::clone(&pool));
    registry.register(instance);
    let handle = RelayDispatchHandle {
        registry: Arc::clone(&registry),
        pool: Arc::clone(&pool),
    };
    let stream = dummy_stream();

    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    handle.dispatch(
        "meta-instance",
        b"hello".to_vec(),
        &md,
        Arc::clone(&stream),
    );

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
