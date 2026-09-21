//! Throughput benches under the operative-mvp-rust model.
//!
//! Two shapes (per `operative-mvp-rust/design.md` Decision 6 +
//! `operative-bench-harness/design.md` Decisions 1-12):
//! - `regression_100x1x1x10k` — apples-to-apples regression check
//!   (100 operatives × 1 tasklet × 1 cell × 10K msg).
//! - `cell_stress_8x4x8x20k/<workers>` — cell-stress
//!   check (8 operatives × 4 tasklets × 8 cells × 20K msg/operative,
//!   fan-out factor 8 on the entry tasklet), labelled with the SDK's base
//!   worker count (`FLEZHA_WORKER_POOL_SIZE`, default: host parallelism).
//!
//! Both rely on the `bench-internals` feature for access to the
//! `OperativeDispatchHandle`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::operative::registry::OperativeRegistry;
use flezha_runner::operative::StreamInstance;
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::tasklet::{Tasklet, TaskletCtx, TaskletError};
use flezha_runner::{__dispatch_internals as bi, OverflowPolicy, StreamOperative};
use flz_core::bench_support::{
    allocations, fan_in, measurement_secs_default, report, stream_pair, Counting, Settle,
};

/// Allocation counting for rows 1 and 2: at most one `#[global_allocator]` per
/// binary, so the static lives here and the machinery in `bench_support`.
#[global_allocator]
static GLOBAL: Counting = Counting;

/// The SDK's base worker count for this run — what the cell-stress
/// benchmark id is labelled with. The pool is process-global and sized at
/// first use, so a run measures one size; sweep by re-running under different
/// `FLEZHA_WORKER_POOL_SIZE` values.
fn worker_count_from_env_or_cpus() -> usize {
    std::env::var("FLEZHA_WORKER_POOL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(2)
        })
}

// ----- Regression bench: 100 operatives × 1 tasklet × 1 cell × 10K msg -----

static REG_COUNTER: Settle = Settle::new();

#[derive(Default)]
struct RegressionTasklet;
impl Tasklet for RegressionTasklet {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "regression_tasklet"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, RegressionTasklet>,
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        REG_COUNTER.add(1);
        Ok(())
    }
}

struct RegressionOperative;
impl StreamOperative for RegressionOperative {
    fn factory_id() -> &'static str {
        "regression_bench_operative"
    }
    fn channels(c: &ChannelBuilder) {
        // Opt the entry channel into Block: the bench bursts 10K msgs
        // per operative against the default capacity-1024 mailbox; under
        // DropNewest the producer side would drop frames mid-burst and
        // the throughput number would silently misrepresent the
        // dispatch hot path.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<RegressionTasklet>();
    }
}

fn regression_bench(c: &mut Criterion) {
    let n_operatives = 100usize;
    let msgs_per_operative = 10_000usize;
    let total = n_operatives * msgs_per_operative;

    let topology = build_topology_for::<RegressionOperative>().expect("topology");
    let registry = Arc::new(OperativeRegistry::new());

    let instance_ids: Vec<String> = (0..n_operatives)
        .map(|i| format!("regression-{}", i))
        .collect();
    for id in &instance_ids {
        let inst = StreamInstance::new(Arc::clone(&topology), Arc::from(id.as_str()));
        registry.register_stream(inst);
    }

    let handle = bi::OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = stream_pair().0;
    let payload: Vec<u8> = b"x".repeat(64);

    let mut group = c.benchmark_group("operative_dispatch");
    group.measurement_time(Duration::from_secs(measurement_secs_default(60)));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(total as u64));
    group.bench_function("regression_100x1x1x10k", |b| {
        b.iter_custom(|iters| {
            let allocs_before = allocations();
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                REG_COUNTER.reset();
                let start = Instant::now();
                fan_in(
                    &handle,
                    &instance_ids,
                    msgs_per_operative,
                    &[payload.as_slice()],
                    None,
                    &stream,
                );
                let drained = REG_COUNTER.wait_for(total, Duration::from_secs(60));
                elapsed += start.elapsed();
                assert!(
                    drained,
                    "regression bench drain timed out: {} of {}",
                    REG_COUNTER.current(),
                    total
                );
            }
            report(
                "regression_100x1x1x10k",
                allocs_before,
                (iters as usize * total) as u64,
            );
            elapsed
        });
    });
    group.finish();
}

// ----- Cell-stress bench: 4 tasklets × 8 cells, fan-out 8 -----

static PART_COUNTER: Settle = Settle::new();

const STRESS_KEYS: &[&str] = &["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"];

// Entry-tasklet routing: fan out every input to all 8 cells per
// `operative-bench-harness/design.md` Decision 7 / workflow §1.1.
fn entry_cells_for(_: &Vec<u8>) -> Vec<&'static str> {
    STRESS_KEYS.to_vec()
}

// Downstream-tasklet routing: pin each emit to a single cell so the
// fan-out factor stays bounded at 8 (entry) and each pipeline stage runs
// once per upstream emit. Per workflow §1.1 ("cells_for returning all 8
// keys for the entry tasklet") only the entry tasklet fans out N-wide; B/C/D
// have 8 cells each (Decision 7) but route to one.
fn pipeline_cells_for(_: &Vec<u8>) -> Vec<&'static str> {
    vec!["p0"]
}

#[derive(Default)]
struct StressBenchTaskletA;
impl Tasklet for StressBenchTaskletA {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_bench_a"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn cell_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const CELLS_FOR_OVERRIDDEN: bool = true;
    fn cells_for(input: &Vec<u8>) -> Vec<&'static str> {
        entry_cells_for(input)
    }
    fn run(
        &mut self,
        input: Vec<u8>,
        _: StateHandle<'_, StressBenchTaskletA>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        PART_COUNTER.add(1);
        ctx.emit::<Vec<u8>>(input)
    }
}

#[derive(Default)]
struct StressBenchTaskletB;
impl Tasklet for StressBenchTaskletB {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_bench_b"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn cell_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const CELLS_FOR_OVERRIDDEN: bool = true;
    fn cells_for(input: &Vec<u8>) -> Vec<&'static str> {
        pipeline_cells_for(input)
    }
    fn run(
        &mut self,
        input: Vec<u8>,
        _: StateHandle<'_, StressBenchTaskletB>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        PART_COUNTER.add(1);
        ctx.emit::<Vec<u8>>(input)
    }
}

#[derive(Default)]
struct StressBenchTaskletC;
impl Tasklet for StressBenchTaskletC {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_bench_c"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn cell_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const CELLS_FOR_OVERRIDDEN: bool = true;
    fn cells_for(input: &Vec<u8>) -> Vec<&'static str> {
        pipeline_cells_for(input)
    }
    fn run(
        &mut self,
        input: Vec<u8>,
        _: StateHandle<'_, StressBenchTaskletC>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        PART_COUNTER.add(1);
        ctx.emit::<Vec<u8>>(input)
    }
}

#[derive(Default)]
struct StressBenchTaskletD;
impl Tasklet for StressBenchTaskletD {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_bench_d"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn cell_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const CELLS_FOR_OVERRIDDEN: bool = true;
    fn cells_for(input: &Vec<u8>) -> Vec<&'static str> {
        pipeline_cells_for(input)
    }
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, StressBenchTaskletD>,
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        PART_COUNTER.add(1);
        Ok(())
    }
}

struct CellStressBenchOperative;
impl StreamOperative for CellStressBenchOperative {
    fn factory_id() -> &'static str {
        "cell_stress_bench_operative"
    }
    fn channels(c: &ChannelBuilder) {
        // Opt the entry channel into Block to avoid DropNewest losses
        // during the 20K-msg-per-operative burst against the cap-1024
        // mailbox. Inter-tasklet channels keep their default Block
        // policy, which preserves ordering through the pipeline.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<StressBenchTaskletA>()
            .named("a_to_b")
            .into::<StressBenchTaskletB>()
            .named("b_to_c")
            .into::<StressBenchTaskletC>()
            .named("c_to_d")
            .into::<StressBenchTaskletD>();
    }
}

fn cell_stress_bench(c: &mut Criterion) {
    let n_operatives = 8usize;
    let msgs_per_operative = std::env::var("FLEZHA_BENCH_MSGS_PER_OPERATIVE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000usize);
    let measurement_secs = measurement_secs_default(30);
    let inputs_total = n_operatives * msgs_per_operative;
    let fan_out = STRESS_KEYS.len();
    let tasklets = 4usize;
    // Each input fans out to all 8 cells of the entry tasklet, and each
    // emit to a downstream tasklet fans out the same way → total Tasklet::run
    // invocations = inputs × fan_out × tasklets.
    let expected_runs = inputs_total * fan_out * tasklets;

    let topology = build_topology_for::<CellStressBenchOperative>().expect("topology");
    let payload: Vec<u8> = b"x".repeat(64);
    let stream = stream_pair().0;

    let mut group = c.benchmark_group("cell_stress_8x4x8x20k");
    group.measurement_time(Duration::from_secs(measurement_secs));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(expected_runs as u64));

    {
        let workers = worker_count_from_env_or_cpus();
        let registry = Arc::new(OperativeRegistry::new());
        let instance_ids: Vec<String> = (0..n_operatives)
            .map(|i| format!("cell-stress-{}", i))
            .collect();
        for id in &instance_ids {
            let inst = StreamInstance::new(Arc::clone(&topology), Arc::from(id.as_str()));
            registry.register_stream(inst);
        }
        let handle = bi::OperativeDispatchHandle {
            registry: Arc::clone(&registry),
        };

        group.bench_with_input(BenchmarkId::from_parameter(workers), &workers, |b, _| {
            b.iter_custom(|iters| {
                let allocs_before = allocations();
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    PART_COUNTER.reset();
                    let start = Instant::now();
                    fan_in(
                        &handle,
                        &instance_ids,
                        msgs_per_operative,
                        &[payload.as_slice()],
                        None,
                        &stream,
                    );
                    let drained = PART_COUNTER.wait_for(expected_runs, Duration::from_secs(120));
                    elapsed += start.elapsed();
                    assert!(
                        drained,
                        "cell stress drain timed out: {} of {}",
                        PART_COUNTER.current(),
                        expected_runs
                    );
                }
                report(
                    "cell_stress_8x4x8x20k",
                    allocs_before,
                    (iters as usize * expected_runs) as u64,
                );
                elapsed
            });
        });
    }
    group.finish();
}

criterion_group!(benches, regression_bench, cell_stress_bench);
criterion_main!(benches);
