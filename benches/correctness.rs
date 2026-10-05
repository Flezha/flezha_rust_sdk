//! Correctness microbenches per `relay-bench-harness/design.md`
//! Decision 8 (per-message dispatch-overhead budget) +
//! `rust-sdk-dispatch` Requirement "Per-Message Dispatch-Overhead Budget".
//!
//! Two functions:
//! - `throughput_single_cell` — single relay × single tasklet ×
//!   single cell. Measures the per-message dispatch budget on a
//!   pinned producer thread.
//! - `throughput_static_fanout` — single relay × single static-mode
//!   tasklet with 4 cell keys, `cells_for` returning all 4
//!   keys per input. Characterizes the fan-out cost.
//!
//! The sync-op invariant ("≤ 4 sync ops per message" on the
//! single-cell path; "4 + N × 3" for fan-out width N) is an
//! analytical bound enforced by code review, per
//! `relay-bench-harness/design.md` Decision 8 / risk note 4 and
//! `relay-bounded-mailbox/design.md` Decision 7. The 4-op budget
//! covers: (1) registry/topology lookup, (2) ArrayQueue try_push,
//! (3) `pushes_total` Relaxed counter, (4) inflight CAS. The fan-out
//! per-target cost is 3 ops (try_push + counter + inflight CAS), so
//! width N adds `N × 3` on top of the shared registry lookup.
//!
//! These benches report raw msg/s; CI compares against
//! `SINGLE_CELL_FLOOR_MSG_PER_SEC` in `docs/operations/benchmarking.md` as a
//! diagnostic, not a hard gate.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::relay::builder::build_topology_for;
use flezha_runner::relay::registry::RelayRegistry;
use flezha_runner::relay::RelayInstance;
use flezha_runner::tasklet::{Tasklet, TaskletError};
use flezha_runner::{
    __dispatch_internals as bi, Ctx, Exactly, OverflowPolicy, Relay, RelaySettings, Run,
};
use flz_core::bench_support::{
    allocations, drained_pair, fan_in, report, stream_pair, Counting, Settle,
};

/// Allocation counting for rows 3 and 4: at most one `#[global_allocator]` per
/// binary, so the static lives here and the machinery in `bench_support`.
#[global_allocator]
static GLOBAL: Counting = Counting;

// ----- Single-cell microbench -----

static SINGLE_COUNTER: Settle = Settle::new();

#[derive(Default)]
struct SingleCellTasklet;
#[flezha_runner::tasklet(name = "single_cell_tasklet")]
impl Tasklet for SingleCellTasklet {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    fn run(&mut self, _: Vec<u8>, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        SINGLE_COUNTER.add(1);
        Ok(())
    }
}

struct SingleCellRelay;
impl Relay for SingleCellRelay {
    fn factory_id() -> &'static str {
        "single_cell_relay"
    }
    fn channels(c: &ChannelBuilder) {
        // Opt the entry channel into Block so the bench measures the
        // dispatch hot path without DropNewest-induced losses (the
        // microbench bursts 100K msgs at default capacity 1024). Drops
        // would skew the throughput number; Block makes the producer
        // park briefly until the consumer drains, which is the desired
        // measurement under sustained back-pressure.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<SingleCellTasklet>();
    }
}

fn throughput_single_cell(c: &mut Criterion) {
    let msgs_per_iter = 100_000usize;

    let topology = build_topology_for::<SingleCellRelay>().expect("topology");
    let registry = Arc::new(RelayRegistry::new());
    let instance = RelayInstance::new(Arc::clone(&topology), Arc::from("single-cell-bench"));
    registry.register(instance);

    let handle = bi::RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = stream_pair().0;
    let payload: Vec<u8> = b"x".repeat(64);

    let ids = ["single-cell-bench".to_string()];

    let mut group = c.benchmark_group("throughput_single_cell");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(msgs_per_iter as u64));
    group.bench_function("throughput_single_cell", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                SINGLE_COUNTER.reset();
                let start = Instant::now();
                fan_in(
                    &handle,
                    &ids,
                    msgs_per_iter,
                    &[payload.as_slice()],
                    None,
                    &stream,
                );
                let drained = SINGLE_COUNTER.wait_for(msgs_per_iter, Duration::from_secs(30));
                elapsed += start.elapsed();
                assert!(
                    drained,
                    "single-cell microbench drain timed out: {} of {}",
                    SINGLE_COUNTER.current(),
                    msgs_per_iter
                );
            }
            elapsed
        });
    });
    group.finish();
}

// ----- Static-mode cell fan-out microbench (fan-out width 4) -----

static FANOUT_COUNTER: Settle = Settle::new();

const FANOUT_KEYS: &[&str] = &["k0", "k1", "k2", "k3"];

#[derive(Default)]
struct FanoutTasklet;
#[flezha_runner::tasklet(name = "fanout_tasklet")]
impl Tasklet for FanoutTasklet {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    fn cell_keys() -> Vec<&'static str> {
        FANOUT_KEYS.to_vec()
    }
    fn cells_for(_: &Vec<u8>) -> Vec<&'static str> {
        FANOUT_KEYS.to_vec()
    }
    fn run(&mut self, _: Vec<u8>, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        FANOUT_COUNTER.add(1);
        Ok(())
    }
}

struct StaticFanoutRelay;
impl Relay for StaticFanoutRelay {
    fn factory_id() -> &'static str {
        "static_fanout_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<FanoutTasklet>();
    }
}

fn throughput_static_fanout(c: &mut Criterion) {
    let inputs_per_iter = 50_000usize;
    let fan_out = FANOUT_KEYS.len();
    let expected_runs = inputs_per_iter * fan_out;

    let topology = build_topology_for::<StaticFanoutRelay>().expect("topology");
    let registry = Arc::new(RelayRegistry::new());
    let instance = RelayInstance::new(Arc::clone(&topology), Arc::from("static-fanout-bench"));
    registry.register(instance);

    let handle = bi::RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = stream_pair().0;
    let payload: Vec<u8> = b"x".repeat(64);

    let ids = ["static-fanout-bench".to_string()];

    let mut group = c.benchmark_group("throughput_static_fanout");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(inputs_per_iter as u64));
    group.bench_function("throughput_static_fanout", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                FANOUT_COUNTER.reset();
                let start = Instant::now();
                fan_in(
                    &handle,
                    &ids,
                    inputs_per_iter,
                    &[payload.as_slice()],
                    None,
                    &stream,
                );
                let drained = FANOUT_COUNTER.wait_for(expected_runs, Duration::from_secs(30));
                elapsed += start.elapsed();
                assert!(
                    drained,
                    "static-fanout microbench drain timed out: {} of {}",
                    FANOUT_COUNTER.current(),
                    expected_runs
                );
            }
            elapsed
        });
    });
    group.finish();
}

// ----- Effectively-once tier: tier-on vs tier-off emit throughput -----
//
// Two identical stateful keyed-emit relays, differing only in the
// `effectively_once()` declaration. The pair publishes the tier's cost
// (`effectively-once-output` §6.3); the tier-off number doubles as the
// zero-delta guard for the default path.

use flezha_runner::cell::flusher::{SendError, SendFrameFn};
use flezha_runner::cell::state_sink;
use serde::{Deserialize, Serialize};

static TIER_OFF_COUNTER: Settle = Settle::new();
static TIER_ON_COUNTER: Settle = Settle::new();

#[derive(Serialize, Deserialize)]
struct BumpChange;
impl flezha_runner::state::Change<i64> for BumpChange {
    fn apply_to(&self, state: &mut i64) {
        *state += 1;
    }
}

macro_rules! tier_bench_tasklet {
    ($task:ident, $name:tt, $counter:ident) => {
        #[derive(Default)]
        struct $task;
        #[flezha_runner::tasklet(name = $name)]
        impl Tasklet for $task {
            type Input = Vec<u8>;
            type Output = Vec<u8>;
            type State = i64;
            type Change = BumpChange;
            fn run(&mut self, input: Vec<u8>, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
                ctx.change(BumpChange);
                ctx.emit_keyed("k".to_string(), input)?;
                $counter.add(1);
                Ok(())
            }
        }
    };
}

tier_bench_tasklet!(TierOffTasklet, "tier_off_tasklet", TIER_OFF_COUNTER);
tier_bench_tasklet!(TierOnTasklet, "tier_on_tasklet", TIER_ON_COUNTER);

struct TierOffRelay;
impl Relay for TierOffRelay {
    fn factory_id() -> &'static str {
        "tier_off_bench_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<TierOffTasklet>()
            .keyed()
            .output_named("out");
    }
}

struct TierOnRelay;
impl Relay for TierOnRelay {
    fn factory_id() -> &'static str {
        "tier_on_bench_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<TierOnTasklet>()
            .keyed()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Flow {
                exactly: Exactly::effectively_once(),
            },
            ..RelaySettings::default()
        }
    }
}

fn throughput_effectively_once_tiers(c: &mut Criterion) {
    let msgs_per_iter = 50_000usize;

    // Discard state frames: the bench measures the SDK-side emit paths,
    // not the controller.
    let sink: SendFrameFn =
        Arc::new(|_pid: &str, _frame: &[u8]| -> Result<(), SendError> { Ok(()) });
    state_sink::install_global_sink(sink);

    let registry = Arc::new(RelayRegistry::new());

    for (factory, id) in [
        (
            build_topology_for::<TierOffRelay>().expect("topology"),
            "tier-off-bench",
        ),
        (
            build_topology_for::<TierOnRelay>().expect("topology"),
            "tier-on-bench",
        ),
    ] {
        let instance = RelayInstance::new(factory, Arc::from(id));
        registry.register(instance);
    }

    let handle = bi::RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = drained_pair();
    let payload: Vec<u8> = b"x".repeat(64);

    let mut group = c.benchmark_group("throughput_effectively_once_tiers");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(msgs_per_iter as u64));

    for (bench_name, instance_id, counter) in [
        ("tier_off", "tier-off-bench", &TIER_OFF_COUNTER),
        ("tier_on", "tier-on-bench", &TIER_ON_COUNTER),
    ] {
        let ids = [instance_id.to_string()];
        group.bench_function(bench_name, |b| {
            b.iter_custom(|iters| {
                let allocs_before = allocations();
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    counter.reset();
                    let start = Instant::now();
                    fan_in(
                        &handle,
                        &ids,
                        msgs_per_iter,
                        &[payload.as_slice()],
                        Some("flz.offset"),
                        &stream,
                    );
                    let drained = counter.wait_for(msgs_per_iter, Duration::from_secs(30));
                    elapsed += start.elapsed();
                    assert!(
                        drained,
                        "{bench_name} drain timed out: {} of {}",
                        counter.current(),
                        msgs_per_iter
                    );
                }
                report(
                    bench_name,
                    allocs_before,
                    (iters as usize * msgs_per_iter) as u64,
                );
                elapsed
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    throughput_single_cell,
    throughput_static_fanout,
    throughput_effectively_once_tiers
);
criterion_main!(benches);
