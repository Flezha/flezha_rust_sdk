//! `steady_state_1k_entities` (dynamic entity mode): 1,000 active
//! dynamic Entities, no eviction, inputs round-robined across the keys.
//! Measures the dynamic routing hot path (one `entity_key` + one DashMap
//! lookup per input). Needs the `bench-internals` feature.

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::operative::registry::OperativeRegistry;
use flezha_runner::operative::UnboundedInstance;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::{OverflowPolicy, UnboundedOperative, __dispatch_internals as bi};
use flz_transport::transport::FlezhaStream;

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

static DYN_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// Dynamic Procedure with eviction OFF (default `entity_idle_ttl == None`),
/// so the steady-state bench measures routing only, not reaping.
#[derive(Default)]
struct SteadyDynProcedure;
impl Procedure for SteadyDynProcedure {
    type Input = Vec<u8>;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "steady_dyn_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        DYN_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct SteadyDynOperative;
impl UnboundedOperative for SteadyDynOperative {
    fn factory_id() -> &'static str {
        "steady_dyn_operative"
    }
    fn channels(c: &ChannelBuilder) {
        // First 8 bytes (hex-ish) are the key; cheap and deterministic.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .key_by(|input: &Vec<u8>| {
                String::from_utf8_lossy(&input[..input.len().min(8)]).into_owned()
            })
            .into::<SteadyDynProcedure>();
    }
}

fn steady_state_bench(c: &mut Criterion) {
    let n_keys = 1_000usize;
    let msgs = std::env::var("FLEZHA_BENCH_DYN_MSGS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000usize);

    let pool = Arc::new(bi::WorkStealingPool::with_size(pool_size_from_env_or_cpus()));
    let topology = build_topology_for::<SteadyDynOperative>().expect("topology");
    let registry = Arc::new(OperativeRegistry::new());
    registry.register_unbounded(UnboundedInstance::new(
        Arc::clone(&topology),
        Arc::from("steady-dyn-0"),
        Arc::clone(&pool),
    ));
    let handle = bi::OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();

    // Pre-build the per-key payloads (8-byte key prefix) so the measured
    // loop pays only dispatch cost, not formatting.
    let payloads: Vec<Vec<u8>> = (0..n_keys)
        .map(|i| format!("{:08}", i).into_bytes())
        .collect();

    let mut group = c.benchmark_group("dynamic_entity");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(msgs as u64));
    group.bench_function("steady_state_1k_entities", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                DYN_COUNTER.store(0, Ordering::Release);
                let start = Instant::now();
                for i in 0..msgs {
                    let mut md = HashMap::new();
                    md.insert("channel".to_string(), "input".to_string());
                    handle.dispatch(
                        "steady-dyn-0",
                        payloads[i % n_keys].clone(),
                        &md,
                        Arc::clone(&stream),
                    );
                }
                let drained = wait_until(
                    || DYN_COUNTER.load(Ordering::Acquire) == msgs,
                    Duration::from_secs(60),
                );
                elapsed += start.elapsed();
                assert!(drained, "steady-state drain timed out");
            }
            elapsed
        });
    });
    group.finish();
}

criterion_group!(benches, steady_state_bench);
criterion_main!(benches);
