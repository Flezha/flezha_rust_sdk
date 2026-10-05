//! Per-emit cost of hybrid channels (`sink_with`) vs. pure
//! inter-tasklet and pure plugin-bound channels. Establishes that the
//! hybrid path is `inter-tasklet fan-out + serializer call`, with the
//! serializer paying the same fixed cost regardless of consumer count.
//!
//! Five bench configurations:
//! - `pure_inter_tasklet_1c`  — pure inter-tasklet, 1 consumer
//! - `pure_inter_tasklet_3c`  — pure inter-tasklet, 3 consumers
//! - `pure_plugin_bound`    — pure `output(...)` channel
//! - `hybrid_1c`            — hybrid, 1 consumer + plugin sink
//! - `hybrid_3c`            — hybrid, 3 consumers + plugin sink
//!
//! Each iteration drives one emit per relay through the live
//! dispatcher (`RelayDispatchHandle::dispatch` on the shared poll-worker
//! pool). The consumer Tasklets count receipts atomically so `iter_custom`
//! waits for full drain before stopping the timer.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::relay::builder::build_topology_for;
use flezha_runner::relay::registry::RelayRegistry;
use flezha_runner::relay::RelayInstance;
use flezha_runner::tasklet::{Tasklet, TaskletError};
use flezha_runner::{__dispatch_internals as bi, Ctx, OverflowPolicy, Relay};
use flz_core::bench_support::{fan_in, stream_pair, Settle};

const MSGS_PER_ITER: usize = 5_000;

// Each bench configuration uses its own atomic counter so concurrent
// criterion runs don't cross-contaminate results.
static PURE_1C_COUNT: Settle = Settle::new();
static PURE_3C_COUNT: Settle = Settle::new();
static PLUGIN_BOUND_PRODUCE_COUNT: Settle = Settle::new();
static HYBRID_1C_COUNT: Settle = Settle::new();
static HYBRID_3C_COUNT: Settle = Settle::new();

// ---------- pure inter-tasklet, 1 consumer ----------

#[derive(Default)]
struct Pure1Producer;
#[flezha_runner::tasklet(name = "pure1_producer")]
impl Tasklet for Pure1Producer {
    type Input = Vec<u8>;
    type Output = u32;
    fn run(&mut self, _: Vec<u8>, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(1)
    }
}

#[derive(Default)]
struct Pure1Consumer;
#[flezha_runner::tasklet(name = "pure1_consumer")]
impl Tasklet for Pure1Consumer {
    type Input = u32;
    fn run(&mut self, _: u32, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        PURE_1C_COUNT.add(1);
        Ok(())
    }
}

struct Pure1Relay;
impl Relay for Pure1Relay {
    fn factory_id() -> &'static str {
        "bench_pure_inter_tasklet_1c"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<Pure1Producer>()
            .named("out")
            .into::<Pure1Consumer>();
    }
}

// ---------- pure inter-tasklet, 3 consumers ----------

#[derive(Default)]
struct Pure3Producer;
#[flezha_runner::tasklet(name = "pure3_producer")]
impl Tasklet for Pure3Producer {
    type Input = Vec<u8>;
    type Output = u32;
    fn run(&mut self, _: Vec<u8>, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(1)
    }
}

macro_rules! pure3_consumer {
    ($name:ident, $static:tt) => {
        #[derive(Default)]
        struct $name;
        #[flezha_runner::tasklet(name = $static)]
        impl Tasklet for $name {
            type Input = u32;
            fn run(&mut self, _: u32, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
                PURE_3C_COUNT.add(1);
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
        "bench_pure_inter_tasklet_3c"
    }
    fn channels(c: &ChannelBuilder) {
        let out = c
            .input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<Pure3Producer>()
            .named("out");
        out.into::<Pure3ConsumerA>();
        out.into::<Pure3ConsumerB>();
        out.into::<Pure3ConsumerC>();
    }
}

// ---------- pure plugin-bound (`output`) ----------

#[derive(Default)]
struct PluginBoundProducer;
#[flezha_runner::tasklet(name = "plugin_bound_producer")]
impl Tasklet for PluginBoundProducer {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    fn run(&mut self, _: Vec<u8>, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        PLUGIN_BOUND_PRODUCE_COUNT.add(1);
        ctx.emit(b"x".to_vec())
    }
}

struct PluginBoundRelay;
impl Relay for PluginBoundRelay {
    fn factory_id() -> &'static str {
        "bench_pure_plugin_bound"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<PluginBoundProducer>()
            .sink_named("out");
    }
}

// ---------- hybrid 1 consumer ----------

#[derive(Default)]
struct Hybrid1Producer;
#[flezha_runner::tasklet(name = "hybrid1_producer")]
impl Tasklet for Hybrid1Producer {
    type Input = Vec<u8>;
    type Output = u32;
    fn run(&mut self, _: Vec<u8>, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(1)
    }
}

#[derive(Default)]
struct Hybrid1Consumer;
#[flezha_runner::tasklet(name = "hybrid1_consumer")]
impl Tasklet for Hybrid1Consumer {
    type Input = u32;
    fn run(&mut self, _: u32, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        HYBRID_1C_COUNT.add(1);
        Ok(())
    }
}

struct Hybrid1Relay;
impl Relay for Hybrid1Relay {
    fn factory_id() -> &'static str {
        "bench_hybrid_1c"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<Hybrid1Producer>()
            .sink_named_with("out", |v: &u32| v.to_be_bytes().to_vec())
            .into::<Hybrid1Consumer>();
    }
}

// ---------- hybrid 3 consumers ----------

#[derive(Default)]
struct Hybrid3Producer;
#[flezha_runner::tasklet(name = "hybrid3_producer")]
impl Tasklet for Hybrid3Producer {
    type Input = Vec<u8>;
    type Output = u32;
    fn run(&mut self, _: Vec<u8>, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(1)
    }
}

macro_rules! hybrid3_consumer {
    ($name:ident, $static:tt) => {
        #[derive(Default)]
        struct $name;
        #[flezha_runner::tasklet(name = $static)]
        impl Tasklet for $name {
            type Input = u32;
            fn run(&mut self, _: u32, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
                HYBRID_3C_COUNT.add(1);
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
    fn channels(c: &ChannelBuilder) {
        let out = c
            .input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<Hybrid3Producer>()
            .sink_named_with("out", |v: &u32| v.to_be_bytes().to_vec());
        out.into::<Hybrid3ConsumerA>();
        out.into::<Hybrid3ConsumerB>();
        out.into::<Hybrid3ConsumerC>();
    }
}

// ----- bench harness -----

fn run_bench<R: Relay>(
    c: &mut Criterion,
    name: &'static str,
    instance_id: &'static str,
    counter: &'static Settle,
    expected_per_msg: usize,
) {
    let topology = build_topology_for::<R>().expect("topology");
    let registry = Arc::new(RelayRegistry::new());
    let inst = RelayInstance::new(topology, Arc::from(instance_id));
    registry.register(inst);
    let handle = bi::RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = stream_pair().0;
    let payload: Vec<u8> = b"x".repeat(8);
    let ids = [instance_id.to_string()];

    let mut group = c.benchmark_group("hybrid_channel_throughput");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(MSGS_PER_ITER as u64));
    group.bench_function(name, |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                counter.reset();
                let target = MSGS_PER_ITER * expected_per_msg;
                let start = Instant::now();
                fan_in(
                    &handle,
                    &ids,
                    MSGS_PER_ITER,
                    &[payload.as_slice()],
                    None,
                    &stream,
                );
                let drained = counter.wait_for(target, Duration::from_secs(15));
                elapsed += start.elapsed();
                assert!(
                    drained,
                    "{name} drain timed out: {} of {}",
                    counter.current(),
                    target
                );
            }
            elapsed
        });
    });
    group.finish();
}

fn pure_inter_tasklet_1c(c: &mut Criterion) {
    run_bench::<Pure1Relay>(
        c,
        "pure_inter_tasklet_1c",
        "bench-pure-1c",
        &PURE_1C_COUNT,
        1,
    );
}

fn pure_inter_tasklet_3c(c: &mut Criterion) {
    run_bench::<Pure3Relay>(
        c,
        "pure_inter_tasklet_3c",
        "bench-pure-3c",
        &PURE_3C_COUNT,
        3,
    );
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
    pure_inter_tasklet_1c,
    pure_inter_tasklet_3c,
    pure_plugin_bound,
    hybrid_1c,
    hybrid_3c,
);
criterion_main!(benches);
