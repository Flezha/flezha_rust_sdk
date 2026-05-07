//! Throughput benches under the relay-mvp-rust model.
//!
//! Two shapes (per `relay-mvp-rust/design.md` Decision 6 +
//! `relay-bench-harness/design.md` Decisions 1-12):
//! - `regression_100x1x1x10k` — apples-to-apples regression check
//!   (100 relays × 1 shard × 1 partition × 10K msg).
//! - `partitioning_stress_8x4x8x20k/<pool>` — partitioning-stress
//!   check (8 relays × 4 shards × 8 partitions × 20K msg/relay,
//!   fan-out factor 8 on the entry shard). Sweeps pool sizes from
//!   `FLEZHA_BENCH_POOL_SIZES` (default: a single host-detected size).
//!
//! Both rely on the `bench-internals` feature for access to the
//! `RelayDispatchHandle` and the work-stealing pool.

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

use flezha_compute_engine::__dispatch_internals as bi;
use flezha_compute_engine::channel::ChannelBuilder;
use flezha_compute_engine::relay::builder::build_topology_for;
use flezha_compute_engine::relay::registry::RelayInstanceRegistry;
use flezha_compute_engine::relay::RelayInstance;
use flezha_compute_engine::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_compute_engine::shard::{Shard, ShardCtx, ShardError};
use flezha_compute_engine::{OverflowPolicy, Relay};
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

// ----- Regression bench: 100 relays × 1 shard × 1 partition × 10K msg -----

static REG_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct RegressionShard;
impl Shard for RegressionShard {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "regression_shard"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, RegressionShard>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        REG_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct RegressionRelay;
impl Relay for RegressionRelay {
    fn factory_id() -> &'static str {
        "regression_bench_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        // Opt the entry channel into Block: the bench bursts 10K msgs
        // per relay against the default capacity-1024 mailbox; under
        // DropNewest the producer side would drop frames mid-burst and
        // the throughput number would silently misrepresent the
        // dispatch hot path.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<RegressionShard>();
    }
}

fn regression_bench(c: &mut Criterion) {
    let n_relays = 100usize;
    let msgs_per_relay = 10_000usize;
    let total = n_relays * msgs_per_relay;

    let pool = Arc::new(bi::WorkStealingPool::with_size(pool_size_from_env_or_cpus()));
    let topology = build_topology_for::<RegressionRelay>().expect("topology");
    let registry = Arc::new(RelayInstanceRegistry::new());

    let instance_ids: Vec<String> = (0..n_relays)
        .map(|i| format!("regression-{}", i))
        .collect();
    for id in &instance_ids {
        let inst = RelayInstance::new(
            Arc::clone(&topology),
            Arc::from(id.as_str()),
            Arc::clone(&pool),
        );
        registry.register(inst);
    }

    let handle = bi::RelayDispatchHandle {
        registry: Arc::clone(&registry),
        pool: Arc::clone(&pool),
    };
    let stream = dummy_stream();
    let payload: Vec<u8> = b"x".repeat(64);

    let mut group = c.benchmark_group("relay_dispatch");
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
                    for _ in 0..msgs_per_relay {
                        let mut md = HashMap::new();
                        md.insert("channel".to_string(), "input".to_string());
                        handle.dispatch(
                            id,
                            payload.clone(),
                            &md,
                            Arc::clone(&stream),
                        );
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

// ----- Partitioning-stress bench: 4 shards × 8 partitions, fan-out 8 -----

static PART_COUNTER: AtomicUsize = AtomicUsize::new(0);

const STRESS_KEYS: &[&str] = &["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"];

// Entry-shard routing: fan out every input to all 8 partitions per
// `relay-bench-harness/design.md` Decision 7 / task §1.1.
fn entry_partition_for(_: &Vec<u8>) -> Vec<&'static str> {
    STRESS_KEYS.to_vec()
}

// Downstream-shard routing: pin each emit to a single partition so the
// fan-out factor stays bounded at 8 (entry) and each pipeline stage runs
// once per upstream emit. Per task §1.1 ("partition_for returning all 8
// keys for the entry shard") only the entry shard fans out N-wide; B/C/D
// have 8 partitions each (Decision 7) but route to one.
fn pipeline_partition_for(_: &Vec<u8>) -> Vec<&'static str> {
    vec!["p0"]
}

#[derive(Default)]
struct StressBenchShardA;
impl Shard for StressBenchShardA {
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
    fn shard_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(input: &Vec<u8>) -> Vec<&'static str> {
        entry_partition_for(input)
    }
    fn run(
        &mut self,
        input: Vec<u8>,
        _: StateHandle<'_, StressBenchShardA>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        PART_COUNTER.fetch_add(1, Ordering::Relaxed);
        ctx.emit::<Vec<u8>>(input)
    }
}

#[derive(Default)]
struct StressBenchShardB;
impl Shard for StressBenchShardB {
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
    fn shard_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(input: &Vec<u8>) -> Vec<&'static str> {
        pipeline_partition_for(input)
    }
    fn run(
        &mut self,
        input: Vec<u8>,
        _: StateHandle<'_, StressBenchShardB>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        PART_COUNTER.fetch_add(1, Ordering::Relaxed);
        ctx.emit::<Vec<u8>>(input)
    }
}

#[derive(Default)]
struct StressBenchShardC;
impl Shard for StressBenchShardC {
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
    fn shard_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(input: &Vec<u8>) -> Vec<&'static str> {
        pipeline_partition_for(input)
    }
    fn run(
        &mut self,
        input: Vec<u8>,
        _: StateHandle<'_, StressBenchShardC>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        PART_COUNTER.fetch_add(1, Ordering::Relaxed);
        ctx.emit::<Vec<u8>>(input)
    }
}

#[derive(Default)]
struct StressBenchShardD;
impl Shard for StressBenchShardD {
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
    fn shard_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(input: &Vec<u8>) -> Vec<&'static str> {
        pipeline_partition_for(input)
    }
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, StressBenchShardD>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        PART_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct PartitioningStressBenchRelay;
impl Relay for PartitioningStressBenchRelay {
    fn factory_id() -> &'static str {
        "partitioning_stress_bench_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        // Opt the entry channel into Block to avoid DropNewest losses
        // during the 20K-msg-per-relay burst against the cap-1024
        // mailbox. Inter-shard channels keep their default Block
        // policy, which preserves ordering through the pipeline.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<StressBenchShardA>();
        c.channel::<Vec<u8>>("a_to_b")
            .from::<StressBenchShardA>()
            .into::<StressBenchShardB>();
        c.channel::<Vec<u8>>("b_to_c")
            .from::<StressBenchShardB>()
            .into::<StressBenchShardC>();
        c.channel::<Vec<u8>>("c_to_d")
            .from::<StressBenchShardC>()
            .into::<StressBenchShardD>();
    }
}

fn partitioning_stress_bench(c: &mut Criterion) {
    let n_relays = 8usize;
    let msgs_per_relay = 20_000usize;
    let inputs_total = n_relays * msgs_per_relay;
    let fan_out = STRESS_KEYS.len();
    let shards = 4usize;
    // Each input fans out to all 8 partitions of the entry shard, and each
    // emit to a downstream shard fans out the same way → total Shard::run
    // invocations = inputs × fan_out × shards.
    let expected_runs = inputs_total * fan_out * shards;

    let pool_sizes: Vec<usize> = match std::env::var("FLEZHA_BENCH_POOL_SIZES") {
        Ok(s) => {
            let parsed: Vec<usize> = s
                .split(',')
                .filter_map(|p| p.trim().parse().ok())
                .collect();
            if parsed.is_empty() {
                vec![pool_size_from_env_or_cpus()]
            } else {
                parsed
            }
        }
        Err(_) => vec![pool_size_from_env_or_cpus()],
    };

    let topology = build_topology_for::<PartitioningStressBenchRelay>().expect("topology");
    let payload: Vec<u8> = b"x".repeat(64);
    let stream = dummy_stream();

    let mut group = c.benchmark_group("partitioning_stress_8x4x8x20k");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(expected_runs as u64));

    for pool_size in pool_sizes {
        let pool = Arc::new(bi::WorkStealingPool::with_size(pool_size));
        let registry = Arc::new(RelayInstanceRegistry::new());
        let instance_ids: Vec<String> = (0..n_relays)
            .map(|i| format!("partitioning-stress-{}", i))
            .collect();
        for id in &instance_ids {
            let inst = RelayInstance::new(
                Arc::clone(&topology),
                Arc::from(id.as_str()),
                Arc::clone(&pool),
            );
            registry.register(inst);
        }
        let handle = bi::RelayDispatchHandle {
            registry: Arc::clone(&registry),
            pool: Arc::clone(&pool),
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
                            for _ in 0..msgs_per_relay {
                                let mut md = HashMap::new();
                                md.insert("channel".to_string(), "input".to_string());
                                handle.dispatch(
                                    id,
                                    payload.clone(),
                                    &md,
                                    Arc::clone(&stream),
                                );
                            }
                        }
                        let drained = wait_until(
                            || PART_COUNTER.load(Ordering::Acquire) == expected_runs,
                            Duration::from_secs(120),
                        );
                        elapsed += start.elapsed();
                        assert!(
                            drained,
                            "partitioning stress drain timed out: {} of {}",
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

criterion_group!(benches, regression_bench, partitioning_stress_bench);
criterion_main!(benches);
