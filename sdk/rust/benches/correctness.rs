//! Correctness microbenches per `operative-bench-harness/design.md`
//! Decision 8 (per-message dispatch-overhead budget) +
//! `rust-sdk-dispatch` Requirement "Per-Message Dispatch-Overhead Budget".
//!
//! Two functions:
//! - `throughput_single_segment` — single operative × single operation ×
//!   single segment. Measures the per-message dispatch budget on a
//!   pinned producer thread.
//! - `throughput_static_fanout` — single operative × single static-mode
//!   operation with 4 segment keys, `segment_for` returning all 4
//!   keys per input. Characterizes the fan-out cost.
//!
//! The sync-op invariant ("≤ 4 sync ops per message" on the
//! single-segment path; "4 + N × 3" for fan-out width N) is an
//! analytical bound enforced by code review, per
//! `operative-bench-harness/design.md` Decision 8 / risk note 4 and
//! `operative-bounded-mailbox/design.md` Decision 7. The 4-op budget
//! covers: (1) registry/topology lookup, (2) ArrayQueue try_push,
//! (3) `pushes_total` Relaxed counter, (4) inflight CAS. The fan-out
//! per-target cost is 3 ops (try_push + counter + inflight CAS), so
//! width N adds `N × 3` on top of the shared registry lookup.
//!
//! These benches report raw msg/s; CI compares against
//! `SINGLE_SEGMENT_FLOOR_MSG_PER_SEC` in `docs/benchmarking.md` as a
//! diagnostic, not a hard gate.

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use flezha_executor::channel::ChannelBuilder;
use flezha_executor::operative::builder::build_topology_for;
use flezha_executor::operative::registry::OperativeRegistry;
use flezha_executor::operative::ContinuousInstance;
use flezha_executor::operation::{Operation, OperationCtx, OperationError};
use flezha_executor::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_executor::{__dispatch_internals as bi, OverflowPolicy, ContinuousOperative};
use flz_transport::transport::FlezhaStream;

// Helpers — duplicated from `dispatch.rs` per `operative-bench-harness/design.md`
// Decision 10 (two copies of 5 lines beats a shared `common.rs` for trivial gain).

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

// ----- Single-segment microbench -----

static SINGLE_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct SingleSegmentOperation;
impl Operation for SingleSegmentOperation {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "single_segment_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, SingleSegmentOperation>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        SINGLE_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct SingleSegmentOperative;
impl ContinuousOperative for SingleSegmentOperative {
    fn factory_id() -> &'static str {
        "single_segment_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        // Opt the entry channel into Block so the bench measures the
        // dispatch hot path without DropNewest-induced losses (the
        // microbench bursts 100K msgs at default capacity 1024). Drops
        // would skew the throughput number; Block makes the producer
        // park briefly until the consumer drains, which is the desired
        // measurement under sustained back-pressure.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<SingleSegmentOperation>();
    }
}

fn throughput_single_segment(c: &mut Criterion) {
    let msgs_per_iter = 100_000usize;

    let pool = Arc::new(bi::WorkStealingPool::with_size(pool_size_from_env_or_cpus()));
    let topology = build_topology_for::<SingleSegmentOperative>().expect("topology");
    let registry = Arc::new(OperativeRegistry::new());
    let instance = ContinuousInstance::new(
        Arc::clone(&topology),
        Arc::from("single-segment-bench"),
        Arc::clone(&pool),
    );
    registry.register_continuous(instance);

    let handle = bi::OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();
    let payload: Vec<u8> = b"x".repeat(64);

    let mut group = c.benchmark_group("throughput_single_segment");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(msgs_per_iter as u64));
    group.bench_function("throughput_single_segment", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                SINGLE_COUNTER.store(0, Ordering::Release);
                let start = Instant::now();
                for _ in 0..msgs_per_iter {
                    let mut md = HashMap::new();
                    md.insert("channel".to_string(), "input".to_string());
                    handle.dispatch(
                        "single-segment-bench",
                        payload.clone(),
                        &md,
                        Arc::clone(&stream),
                    );
                }
                let drained = wait_until(
                    || SINGLE_COUNTER.load(Ordering::Acquire) == msgs_per_iter,
                    Duration::from_secs(30),
                );
                elapsed += start.elapsed();
                assert!(
                    drained,
                    "single-segment microbench drain timed out: {} of {}",
                    SINGLE_COUNTER.load(Ordering::Acquire),
                    msgs_per_iter
                );
            }
            elapsed
        });
    });
    group.finish();
}

// ----- Static-mode segment fan-out microbench (fan-out width 4) -----

static FANOUT_COUNTER: AtomicUsize = AtomicUsize::new(0);

const FANOUT_KEYS: &[&str] = &["k0", "k1", "k2", "k3"];

#[derive(Default)]
struct FanoutOperation;
impl Operation for FanoutOperation {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "fanout_operation"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn operation_keys() -> Vec<&'static str> {
        FANOUT_KEYS.to_vec()
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(_: &Vec<u8>) -> Vec<&'static str> {
        FANOUT_KEYS.to_vec()
    }
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, FanoutOperation>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        FANOUT_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct StaticFanoutOperative;
impl ContinuousOperative for StaticFanoutOperative {
    fn factory_id() -> &'static str {
        "static_fanout_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<FanoutOperation>();
    }
}

fn throughput_static_fanout(c: &mut Criterion) {
    let inputs_per_iter = 50_000usize;
    let fan_out = FANOUT_KEYS.len();
    let expected_runs = inputs_per_iter * fan_out;

    let pool = Arc::new(bi::WorkStealingPool::with_size(pool_size_from_env_or_cpus()));
    let topology = build_topology_for::<StaticFanoutOperative>().expect("topology");
    let registry = Arc::new(OperativeRegistry::new());
    let instance = ContinuousInstance::new(
        Arc::clone(&topology),
        Arc::from("static-fanout-bench"),
        Arc::clone(&pool),
    );
    registry.register_continuous(instance);

    let handle = bi::OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();
    let payload: Vec<u8> = b"x".repeat(64);

    let mut group = c.benchmark_group("throughput_static_fanout");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(inputs_per_iter as u64));
    group.bench_function("throughput_static_fanout", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                FANOUT_COUNTER.store(0, Ordering::Release);
                let start = Instant::now();
                for _ in 0..inputs_per_iter {
                    let mut md = HashMap::new();
                    md.insert("channel".to_string(), "input".to_string());
                    handle.dispatch(
                        "static-fanout-bench",
                        payload.clone(),
                        &md,
                        Arc::clone(&stream),
                    );
                }
                let drained = wait_until(
                    || FANOUT_COUNTER.load(Ordering::Acquire) == expected_runs,
                    Duration::from_secs(30),
                );
                elapsed += start.elapsed();
                assert!(
                    drained,
                    "static-fanout microbench drain timed out: {} of {}",
                    FANOUT_COUNTER.load(Ordering::Acquire),
                    expected_runs
                );
            }
            elapsed
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    throughput_single_segment,
    throughput_static_fanout
);
criterion_main!(benches);
