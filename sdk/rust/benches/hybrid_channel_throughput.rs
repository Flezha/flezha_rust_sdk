//! Per-emit cost of hybrid channels (`also_to_plugin_with`) vs. pure
//! inter-shard and pure plugin-bound channels. Establishes that the
//! hybrid path is `inter-shard fan-out + serializer call`, with the
//! serializer paying the same fixed cost regardless of consumer count.
//!
//! Five bench configurations:
//! - `pure_inter_shard_1c`  — pure inter-shard, 1 consumer
//! - `pure_inter_shard_3c`  — pure inter-shard, 3 consumers
//! - `pure_plugin_bound`    — pure `to_plugin(...)` channel
//! - `hybrid_1c`            — hybrid, 1 consumer + plugin sink
//! - `hybrid_3c`            — hybrid, 3 consumers + plugin sink
//!
//! Each iteration drives one emit per relay through the live
//! dispatcher (`RelayDispatchHandle::dispatch` on a shared work-stealing
//! pool). The consumer Shards count receipts atomically so `iter_custom`
//! waits for full drain before stopping the timer.

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use flezha_executor::channel::ChannelBuilder;
use flezha_executor::relay::builder::build_topology_for;
use flezha_executor::relay::registry::RelayInstanceRegistry;
use flezha_executor::relay::RelayInstance;
use flezha_executor::shard::{Shard, ShardCtx, ShardError};
use flezha_executor::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_executor::{__dispatch_internals as bi, OverflowPolicy, Relay};
use flz_transport::transport::FlezhaStream;

// ----- shared bench helpers -----

fn pool_size_from_env_or_cpus() -> usize {
    std::env::var("FLEZHA_WORKER_POOL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(2)
        })
}

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
        std::thread::sleep(Duration::from_micros(50));
    }
    true
}

const MSGS_PER_ITER: usize = 5_000;

// Each bench configuration uses its own atomic counter so concurrent
// criterion runs don't cross-contaminate results.
static PURE_1C_COUNT: AtomicUsize = AtomicUsize::new(0);
static PURE_3C_COUNT: AtomicUsize = AtomicUsize::new(0);
static PLUGIN_BOUND_PRODUCE_COUNT: AtomicUsize = AtomicUsize::new(0);
static HYBRID_1C_COUNT: AtomicUsize = AtomicUsize::new(0);
static HYBRID_3C_COUNT: AtomicUsize = AtomicUsize::new(0);

// ---------- pure inter-shard, 1 consumer ----------

#[derive(Default)]
struct Pure1Producer;
impl Shard for Pure1Producer {
    type Input = Vec<u8>;
    type Output = u32;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "pure1_producer"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<u32>(1)
    }
}

#[derive(Default)]
struct Pure1Consumer;
impl Shard for Pure1Consumer {
    type Input = u32;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "pure1_consumer"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: u32,
        _: StateHandle<'_, Self>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        PURE_1C_COUNT.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct Pure1Relay;
impl Relay for Pure1Relay {
    fn factory_id() -> &'static str {
        "bench_pure_inter_shard_1c"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<Pure1Producer>();
        c.channel::<u32>("out")
            .from::<Pure1Producer>()
            .into::<Pure1Consumer>();
    }
}

// ---------- pure inter-shard, 3 consumers ----------

#[derive(Default)]
struct Pure3Producer;
impl Shard for Pure3Producer {
    type Input = Vec<u8>;
    type Output = u32;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "pure3_producer"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<u32>(1)
    }
}

macro_rules! pure3_consumer {
    ($name:ident, $static:literal) => {
        #[derive(Default)]
        struct $name;
        impl Shard for $name {
            type Input = u32;
            type Output = ();
            type State = ();
            type Change = NoChange;
            type Clock = WallClock;
            type Config = ();
            fn name() -> &'static str {
                $static
            }
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
            fn run(
                &mut self,
                _: u32,
                _: StateHandle<'_, Self>,
                _: &mut ShardCtx<'_>,
            ) -> Result<(), ShardError> {
                PURE_3C_COUNT.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    };
}

pure3_consumer!(Pure3ConsumerA, "pure3_a");
pure3_consumer!(Pure3ConsumerB, "pure3_b");
pure3_consumer!(Pure3ConsumerC, "pure3_c");

struct Pure3Relay;
impl Relay for Pure3Relay {
    fn factory_id() -> &'static str {
        "bench_pure_inter_shard_3c"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<Pure3Producer>();
        c.channel::<u32>("out")
            .from::<Pure3Producer>()
            .into::<Pure3ConsumerA>()
            .into::<Pure3ConsumerB>()
            .into::<Pure3ConsumerC>();
    }
}

// ---------- pure plugin-bound (`to_plugin`) ----------

#[derive(Default)]
struct PluginBoundProducer;
impl Shard for PluginBoundProducer {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "plugin_bound_producer"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        PLUGIN_BOUND_PRODUCE_COUNT.fetch_add(1, Ordering::Relaxed);
        ctx.emit::<Vec<u8>>(b"x".to_vec())
    }
}

struct PluginBoundRelay;
impl Relay for PluginBoundRelay {
    fn factory_id() -> &'static str {
        "bench_pure_plugin_bound"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<PluginBoundProducer>();
        c.channel::<Vec<u8>>("out")
            .from::<PluginBoundProducer>()
            .to_plugin("bench_plugin_bound");
    }
}

// ---------- hybrid 1 consumer ----------

#[derive(Default)]
struct Hybrid1Producer;
impl Shard for Hybrid1Producer {
    type Input = Vec<u8>;
    type Output = u32;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid1_producer"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<u32>(1)
    }
}

#[derive(Default)]
struct Hybrid1Consumer;
impl Shard for Hybrid1Consumer {
    type Input = u32;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid1_consumer"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: u32,
        _: StateHandle<'_, Self>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        HYBRID_1C_COUNT.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct Hybrid1Relay;
impl Relay for Hybrid1Relay {
    fn factory_id() -> &'static str {
        "bench_hybrid_1c"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<Hybrid1Producer>();
        c.channel::<u32>("out")
            .from::<Hybrid1Producer>()
            .also_to_plugin_with("bench_hybrid_plugin_1c", |v: &u32| v.to_be_bytes().to_vec())
            .into::<Hybrid1Consumer>();
    }
}

// ---------- hybrid 3 consumers ----------

#[derive(Default)]
struct Hybrid3Producer;
impl Shard for Hybrid3Producer {
    type Input = Vec<u8>;
    type Output = u32;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid3_producer"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        ctx.emit::<u32>(1)
    }
}

macro_rules! hybrid3_consumer {
    ($name:ident, $static:literal) => {
        #[derive(Default)]
        struct $name;
        impl Shard for $name {
            type Input = u32;
            type Output = ();
            type State = ();
            type Change = NoChange;
            type Clock = WallClock;
            type Config = ();
            fn name() -> &'static str {
                $static
            }
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
            fn run(
                &mut self,
                _: u32,
                _: StateHandle<'_, Self>,
                _: &mut ShardCtx<'_>,
            ) -> Result<(), ShardError> {
                HYBRID_3C_COUNT.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    };
}

hybrid3_consumer!(Hybrid3ConsumerA, "hybrid3_a");
hybrid3_consumer!(Hybrid3ConsumerB, "hybrid3_b");
hybrid3_consumer!(Hybrid3ConsumerC, "hybrid3_c");

struct Hybrid3Relay;
impl Relay for Hybrid3Relay {
    fn factory_id() -> &'static str {
        "bench_hybrid_3c"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<Hybrid3Producer>();
        c.channel::<u32>("out")
            .from::<Hybrid3Producer>()
            .also_to_plugin_with("bench_hybrid_plugin_3c", |v: &u32| v.to_be_bytes().to_vec())
            .into::<Hybrid3ConsumerA>()
            .into::<Hybrid3ConsumerB>()
            .into::<Hybrid3ConsumerC>();
    }
}

// ----- bench harness -----

fn run_bench<R: Relay>(
    c: &mut Criterion,
    name: &'static str,
    instance_id: &'static str,
    counter: &'static AtomicUsize,
    expected_per_msg: usize,
) {
    let pool = Arc::new(bi::WorkStealingPool::with_size(pool_size_from_env_or_cpus()));
    let topology = build_topology_for::<R>().expect("topology");
    let registry = Arc::new(RelayInstanceRegistry::new());
    let inst = RelayInstance::new(topology, Arc::from(instance_id), Arc::clone(&pool));
    registry.register(inst);
    let handle = bi::RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();
    let payload: Vec<u8> = b"x".repeat(8);

    let mut group = c.benchmark_group("hybrid_channel_throughput");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(MSGS_PER_ITER as u64));
    group.bench_function(name, |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                counter.store(0, Ordering::Release);
                let target = MSGS_PER_ITER * expected_per_msg;
                let start = Instant::now();
                for _ in 0..MSGS_PER_ITER {
                    let mut md = HashMap::new();
                    md.insert("channel".to_string(), "input".to_string());
                    handle.dispatch(instance_id, payload.clone(), &md, Arc::clone(&stream));
                }
                let drained = wait_until(
                    || counter.load(Ordering::Acquire) == target,
                    Duration::from_secs(15),
                );
                elapsed += start.elapsed();
                assert!(
                    drained,
                    "{name} drain timed out: {} of {}",
                    counter.load(Ordering::Acquire),
                    target
                );
            }
            elapsed
        });
    });
    group.finish();
}

fn pure_inter_shard_1c(c: &mut Criterion) {
    run_bench::<Pure1Relay>(c, "pure_inter_shard_1c", "bench-pure-1c", &PURE_1C_COUNT, 1);
}

fn pure_inter_shard_3c(c: &mut Criterion) {
    run_bench::<Pure3Relay>(c, "pure_inter_shard_3c", "bench-pure-3c", &PURE_3C_COUNT, 3);
}

fn pure_plugin_bound(c: &mut Criterion) {
    run_bench::<PluginBoundRelay>(
        c,
        "pure_plugin_bound",
        "bench-plugin-bound",
        &PLUGIN_BOUND_PRODUCE_COUNT,
        1,
    );
}

fn hybrid_1c(c: &mut Criterion) {
    run_bench::<Hybrid1Relay>(c, "hybrid_1c", "bench-hybrid-1c", &HYBRID_1C_COUNT, 1);
}

fn hybrid_3c(c: &mut Criterion) {
    run_bench::<Hybrid3Relay>(c, "hybrid_3c", "bench-hybrid-3c", &HYBRID_3C_COUNT, 3);
}

criterion_group!(
    benches,
    pure_inter_shard_1c,
    pure_inter_shard_3c,
    pure_plugin_bound,
    hybrid_1c,
    hybrid_3c,
);
criterion_main!(benches);
