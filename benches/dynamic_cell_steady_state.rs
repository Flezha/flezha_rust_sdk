//! `steady_state_1k_cells` (dynamic cell mode): 1,000 active
//! dynamic Cells, no eviction, inputs round-robined across the keys.
//! Measures the dynamic routing hot path (one `cell_key` + one DashMap
//! lookup per input). Needs the `bench-internals` feature.

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

static DYN_COUNTER: Settle = Settle::new();

/// Dynamic Tasklet with eviction OFF (default `idle_ttl == None`),
/// so the steady-state bench measures routing only, not reaping.
#[derive(Default)]
struct SteadyDynTasklet;
#[flezha_runner::tasklet(name = "steady_dyn_tasklet")]
impl Tasklet for SteadyDynTasklet {
    type Input = Vec<u8>;
    fn run(&mut self, _: Vec<u8>, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        DYN_COUNTER.add(1);
        Ok(())
    }
}

struct SteadyDynRelay;
impl Relay for SteadyDynRelay {
    fn factory_id() -> &'static str {
        "steady_dyn_relay"
    }
    fn channels(c: &ChannelBuilder) {
        // First 8 bytes (hex-ish) are the key; cheap and deterministic.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::DropNothing)
            .key_by(|input: &Vec<u8>| {
                String::from_utf8_lossy(&input[..input.len().min(8)]).into_owned()
            })
            .into::<SteadyDynTasklet>();
    }
}

fn steady_state_bench(c: &mut Criterion) {
    let n_keys = 1_000usize;
    let msgs = std::env::var("FLEZHA_BENCH_DYN_MSGS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200_000usize);

    let topology = build_topology_for::<SteadyDynRelay>().expect("topology");
    let registry = Arc::new(RelayRegistry::new());
    registry.register(RelayInstance::new(
        Arc::clone(&topology),
        Arc::from("steady-dyn-0"),
    ));
    let handle = bi::RelayDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = stream_pair().0;

    // Pre-build the per-key payloads (8-byte key prefix) so the measured
    // loop pays only dispatch cost, not formatting.
    let payloads: Vec<Vec<u8>> = (0..n_keys)
        .map(|i| format!("{:08}", i).into_bytes())
        .collect();
    let feed: Vec<&[u8]> = payloads.iter().map(|payload| payload.as_slice()).collect();
    let ids = ["steady-dyn-0".to_string()];

    let mut group = c.benchmark_group("dynamic_cell");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(msgs as u64));
    group.bench_function("steady_state_1k_cells", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                DYN_COUNTER.reset();
                let start = Instant::now();
                fan_in(&handle, &ids, msgs, &feed, None, &stream);
                let drained = DYN_COUNTER.wait_for(msgs, Duration::from_secs(60));
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
