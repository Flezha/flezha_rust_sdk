//! Throughput benches under the operative-mvp-rust model.
//!
//! Two shapes (per `operative-mvp-rust/design.md` Decision 6 +
//! `operative-bench-harness/design.md` Decisions 1-12):
//! - `regression_100x1x1x10k` — apples-to-apples regression check
//!   (100 operatives × 1 operation × 1 segment × 10K msg).
//! - `segmenting_stress_8x4x8x20k/<pool>` — segmenting-stress
//!   check (8 operatives × 4 operations × 8 segments × 20K msg/operative,
//!   fan-out factor 8 on the entry operation). Sweeps pool sizes from
//!   `FLEZHA_BENCH_POOL_SIZES` (default: a single host-detected size).
//!
//! Both rely on the `bench-internals` feature for access to the
//! `OperativeDispatchHandle` and the work-stealing pool.

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operation::{Operation, OperationCtx, OperationError};
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::operative::registry::OperativeRegistry;
use flezha_runner::operative::UnboundedInstance;
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::{OverflowPolicy, UnboundedOperative, __dispatch_internals as bi};
use flz_transport::transport::FlezhaStream;

// ----- Shared bench helpers (Decision 10: duplicated locally per bench file) -----

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
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

// ----- Regression bench: 100 operatives × 1 operation × 1 segment × 10K msg -----

static REG_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct RegressionOperation;
impl Operation for RegressionOperation {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "regression_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, RegressionOperation>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        REG_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct RegressionOperative;
impl UnboundedOperative for RegressionOperative {
    fn factory_id() -> &'static str {
        "regression_bench_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        // Opt the entry channel into Block: the bench bursts 10K msgs
        // per operative against the default capacity-1024 mailbox; under
        // DropNewest the producer side would drop frames mid-burst and
        // the throughput number would silently misrepresent the
        // dispatch hot path.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<RegressionOperation>();
    }
}

fn regression_bench(c: &mut Criterion) {
    let n_operatives = 100usize;
    let msgs_per_operative = 10_000usize;
    let total = n_operatives * msgs_per_operative;

    let pool = Arc::new(bi::WorkStealingPool::with_size(pool_size_from_env_or_cpus()));
    let topology = build_topology_for::<RegressionOperative>().expect("topology");
    let registry = Arc::new(OperativeRegistry::new());

    let instance_ids: Vec<String> = (0..n_operatives)
        .map(|i| format!("regression-{}", i))
        .collect();
    for id in &instance_ids {
        let inst = UnboundedInstance::new(
            Arc::clone(&topology),
            Arc::from(id.as_str()),
            Arc::clone(&pool),
        );
        registry.register_unbounded(inst);
    }

    let handle = bi::OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();
    let payload: Vec<u8> = b"x".repeat(64);

    let mut group = c.benchmark_group("operative_dispatch");
    group.measurement_time(Duration::from_secs(60));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(total as u64));
    group.bench_function("regression_100x1x1x10k", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                REG_COUNTER.store(0, Ordering::Release);
                let start = Instant::now();
                for id in &instance_ids {
                    for _ in 0..msgs_per_operative {
                        let mut md = HashMap::new();
                        md.insert("channel".to_string(), "input".to_string());
                        handle.dispatch(id, payload.clone(), &md, Arc::clone(&stream));
                    }
                }
                let drained = wait_until(
                    || REG_COUNTER.load(Ordering::Acquire) == total,
                    Duration::from_secs(60),
                );
                elapsed += start.elapsed();
                assert!(
                    drained,
                    "regression bench drain timed out: {} of {}",
                    REG_COUNTER.load(Ordering::Acquire),
                    total
                );
            }
            elapsed
        });
    });
    group.finish();
}

// ----- Segmenting-stress bench: 4 operations × 8 segments, fan-out 8 -----

static PART_COUNTER: AtomicUsize = AtomicUsize::new(0);

const STRESS_KEYS: &[&str] = &["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"];

// Entry-operation routing: fan out every input to all 8 segments per
// `operative-bench-harness/design.md` Decision 7 / bounded §1.1.
fn entry_segment_for(_: &Vec<u8>) -> Vec<&'static str> {
    STRESS_KEYS.to_vec()
}

// Downstream-operation routing: pin each emit to a single segment so the
// fan-out factor stays bounded at 8 (entry) and each pipeline stage runs
// once per upstream emit. Per bounded §1.1 ("segment_for returning all 8
// keys for the entry operation") only the entry operation fans out N-wide; B/C/D
// have 8 segments each (Decision 7) but route to one.
fn pipeline_segment_for(_: &Vec<u8>) -> Vec<&'static str> {
    vec!["p0"]
}

#[derive(Default)]
struct StressBenchOperationA;
impl Operation for StressBenchOperationA {
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
    fn operation_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(input: &Vec<u8>) -> Vec<&'static str> {
        entry_segment_for(input)
    }
    fn run(
        &mut self,
        input: Vec<u8>,
        _: StateHandle<'_, StressBenchOperationA>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        PART_COUNTER.fetch_add(1, Ordering::Relaxed);
        ctx.emit::<Vec<u8>>(input)
    }
}

#[derive(Default)]
struct StressBenchOperationB;
impl Operation for StressBenchOperationB {
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
    fn operation_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(input: &Vec<u8>) -> Vec<&'static str> {
        pipeline_segment_for(input)
    }
    fn run(
        &mut self,
        input: Vec<u8>,
        _: StateHandle<'_, StressBenchOperationB>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        PART_COUNTER.fetch_add(1, Ordering::Relaxed);
        ctx.emit::<Vec<u8>>(input)
    }
}

#[derive(Default)]
struct StressBenchOperationC;
impl Operation for StressBenchOperationC {
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
    fn operation_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(input: &Vec<u8>) -> Vec<&'static str> {
        pipeline_segment_for(input)
    }
    fn run(
        &mut self,
        input: Vec<u8>,
        _: StateHandle<'_, StressBenchOperationC>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        PART_COUNTER.fetch_add(1, Ordering::Relaxed);
        ctx.emit::<Vec<u8>>(input)
    }
}

#[derive(Default)]
struct StressBenchOperationD;
impl Operation for StressBenchOperationD {
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
    fn operation_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(input: &Vec<u8>) -> Vec<&'static str> {
        pipeline_segment_for(input)
    }
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, StressBenchOperationD>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        PART_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct SegmentingStressBenchOperative;
impl UnboundedOperative for SegmentingStressBenchOperative {
    fn factory_id() -> &'static str {
        "segmenting_stress_bench_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        // Opt the entry channel into Block to avoid DropNewest losses
        // during the 20K-msg-per-operative burst against the cap-1024
        // mailbox. Inter-operation channels keep their default Block
        // policy, which preserves ordering through the pipeline.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<StressBenchOperationA>();
        c.channel::<Vec<u8>>("a_to_b")
            .from::<StressBenchOperationA>()
            .into::<StressBenchOperationB>();
        c.channel::<Vec<u8>>("b_to_c")
            .from::<StressBenchOperationB>()
            .into::<StressBenchOperationC>();
        c.channel::<Vec<u8>>("c_to_d")
            .from::<StressBenchOperationC>()
            .into::<StressBenchOperationD>();
    }
}

fn segmenting_stress_bench(c: &mut Criterion) {
    let n_operatives = 8usize;
    let msgs_per_operative = std::env::var("FLEZHA_BENCH_MSGS_PER_OPERATIVE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000usize);
    let measurement_secs = std::env::var("FLEZHA_BENCH_MEASUREMENT_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30u64);
    let inputs_total = n_operatives * msgs_per_operative;
    let fan_out = STRESS_KEYS.len();
    let operations = 4usize;
    // Each input fans out to all 8 segments of the entry operation, and each
    // emit to a downstream operation fans out the same way → total Operation::run
    // invocations = inputs × fan_out × operations.
    let expected_runs = inputs_total * fan_out * operations;

    let pool_sizes: Vec<usize> = match std::env::var("FLEZHA_BENCH_POOL_SIZES") {
        Ok(s) => {
            let parsed: Vec<usize> = s.split(',').filter_map(|p| p.trim().parse().ok()).collect();
            if parsed.is_empty() {
                vec![pool_size_from_env_or_cpus()]
            } else {
                parsed
            }
        }
        Err(_) => vec![pool_size_from_env_or_cpus()],
    };

    let topology = build_topology_for::<SegmentingStressBenchOperative>().expect("topology");
    let payload: Vec<u8> = b"x".repeat(64);
    let stream = dummy_stream();

    let mut group = c.benchmark_group("segmenting_stress_8x4x8x20k");
    group.measurement_time(Duration::from_secs(measurement_secs));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(expected_runs as u64));

    for pool_size in pool_sizes {
        let pool = Arc::new(bi::WorkStealingPool::with_size(pool_size));
        let registry = Arc::new(OperativeRegistry::new());
        let instance_ids: Vec<String> = (0..n_operatives)
            .map(|i| format!("segmenting-stress-{}", i))
            .collect();
        for id in &instance_ids {
            let inst = UnboundedInstance::new(
                Arc::clone(&topology),
                Arc::from(id.as_str()),
                Arc::clone(&pool),
            );
            registry.register_unbounded(inst);
        }
        let handle = bi::OperativeDispatchHandle {
            registry: Arc::clone(&registry),
        };

        group.bench_with_input(
            BenchmarkId::from_parameter(pool_size),
            &pool_size,
            |b, _| {
                b.iter_custom(|iters| {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        PART_COUNTER.store(0, Ordering::Release);
                        let start = Instant::now();
                        for id in &instance_ids {
                            for _ in 0..msgs_per_operative {
                                let mut md = HashMap::new();
                                md.insert("channel".to_string(), "input".to_string());
                                handle.dispatch(id, payload.clone(), &md, Arc::clone(&stream));
                            }
                        }
                        let drained = wait_until(
                            || PART_COUNTER.load(Ordering::Acquire) == expected_runs,
                            Duration::from_secs(120),
                        );
                        elapsed += start.elapsed();
                        assert!(
                            drained,
                            "segmenting stress drain timed out: {} of {}",
                            PART_COUNTER.load(Ordering::Acquire),
                            expected_runs
                        );
                    }
                    elapsed
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, regression_bench, segmenting_stress_bench);
criterion_main!(benches);
