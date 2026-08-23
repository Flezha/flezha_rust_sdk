//! Correctness microbenches per `operative-bench-harness/design.md`
//! Decision 8 (per-message dispatch-overhead budget) +
//! `rust-sdk-dispatch` Requirement "Per-Message Dispatch-Overhead Budget".
//!
//! Two functions:
//! - `throughput_single_entity` — single operative × single procedure ×
//!   single entity. Measures the per-message dispatch budget on a
//!   pinned producer thread.
//! - `throughput_static_fanout` — single operative × single static-mode
//!   procedure with 4 entity keys, `entities_for` returning all 4
//!   keys per input. Characterizes the fan-out cost.
//!
//! The sync-op invariant ("≤ 4 sync procs per message" on the
//! single-entity path; "4 + N × 3" for fan-out width N) is an
//! analytical bound enforced by code review, per
//! `operative-bench-harness/design.md` Decision 8 / risk note 4 and
//! `operative-bounded-mailbox/design.md` Decision 7. The 4-op budget
//! covers: (1) registry/topology lookup, (2) ArrayQueue try_push,
//! (3) `pushes_total` Relaxed counter, (4) inflight CAS. The fan-out
//! per-target cost is 3 procs (try_push + counter + inflight CAS), so
//! width N adds `N × 3` on top of the shared registry lookup.
//!
//! These benches report raw msg/s; CI compares against
//! `SINGLE_ENTITY_FLOOR_MSG_PER_SEC` in `docs/operations/benchmarking.md` as a
//! diagnostic, not a hard gate.

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::operative::registry::OperativeRegistry;
use flezha_runner::operative::StreamInstance;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::{__dispatch_internals as bi, OverflowPolicy, StreamOperative};
use flz_transport::transport::FlezhaStream;

// Helpers — duplicated from `dispatch.rs` per `operative-bench-harness/design.md`
// Decision 10 (two copies of 5 lines beats a shared `common.rs` for trivial gain).

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

// ----- Single-entity microbench -----

static SINGLE_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct SingleEntityProcedure;
impl Procedure for SingleEntityProcedure {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "single_entity_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, SingleEntityProcedure>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        SINGLE_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct SingleEntityOperative;
impl StreamOperative for SingleEntityOperative {
    fn factory_id() -> &'static str {
        "single_entity_operative"
    }
    fn channels(c: &ChannelBuilder) {
        // Opt the entry channel into Block so the bench measures the
        // dispatch hot path without DropNewest-induced losses (the
        // microbench bursts 100K msgs at default capacity 1024). Drops
        // would skew the throughput number; Block makes the producer
        // park briefly until the consumer drains, which is the desired
        // measurement under sustained back-pressure.
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<SingleEntityProcedure>();
    }
}

fn throughput_single_entity(c: &mut Criterion) {
    let msgs_per_iter = 100_000usize;

    let topology = build_topology_for::<SingleEntityOperative>().expect("topology");
    let registry = Arc::new(OperativeRegistry::new());
    let instance = StreamInstance::new(Arc::clone(&topology), Arc::from("single-entity-bench"));
    registry.register_stream(instance);

    let handle = bi::OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = dummy_stream();
    let payload: Vec<u8> = b"x".repeat(64);

    let mut group = c.benchmark_group("throughput_single_entity");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(msgs_per_iter as u64));
    group.bench_function("throughput_single_entity", |b| {
        b.iter_custom(|iters| {
            let mut elapsed = Duration::ZERO;
            for _ in 0..iters {
                SINGLE_COUNTER.store(0, Ordering::Release);
                let start = Instant::now();
                for _ in 0..msgs_per_iter {
                    let mut md = HashMap::new();
                    md.insert("channel".to_string(), "input".to_string());
                    handle.dispatch(
                        "single-entity-bench",
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
                    "single-entity microbench drain timed out: {} of {}",
                    SINGLE_COUNTER.load(Ordering::Acquire),
                    msgs_per_iter
                );
            }
            elapsed
        });
    });
    group.finish();
}

// ----- Static-mode entity fan-out microbench (fan-out width 4) -----

static FANOUT_COUNTER: AtomicUsize = AtomicUsize::new(0);

const FANOUT_KEYS: &[&str] = &["k0", "k1", "k2", "k3"];

#[derive(Default)]
struct FanoutProcedure;
impl Procedure for FanoutProcedure {
    type Input = Vec<u8>;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "fanout_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn entity_keys() -> Vec<&'static str> {
        FANOUT_KEYS.to_vec()
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_: &Vec<u8>) -> Vec<&'static str> {
        FANOUT_KEYS.to_vec()
    }
    fn run(
        &mut self,
        _: Vec<u8>,
        _: StateHandle<'_, FanoutProcedure>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        FANOUT_COUNTER.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

struct StaticFanoutOperative;
impl StreamOperative for StaticFanoutOperative {
    fn factory_id() -> &'static str {
        "static_fanout_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<FanoutProcedure>();
    }
}

fn throughput_static_fanout(c: &mut Criterion) {
    let inputs_per_iter = 50_000usize;
    let fan_out = FANOUT_KEYS.len();
    let expected_runs = inputs_per_iter * fan_out;

    let topology = build_topology_for::<StaticFanoutOperative>().expect("topology");
    let registry = Arc::new(OperativeRegistry::new());
    let instance = StreamInstance::new(Arc::clone(&topology), Arc::from("static-fanout-bench"));
    registry.register_stream(instance);

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

// ----- Effectively-once tier: tier-on vs tier-off emit throughput -----
//
// Two identical stateful keyed-emit operatives, differing only in the
// `effectively_once()` declaration. The pair publishes the tier's cost
// (`effectively-once-output` §6.3); the tier-off number doubles as the
// zero-delta guard for the default path.

use flezha_runner::entity::flusher::{SendError, SendFrameFn};
use flezha_runner::entity::state_sink;
use serde::{Deserialize, Serialize};

static TIER_OFF_COUNTER: AtomicUsize = AtomicUsize::new(0);
static TIER_ON_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Serialize, Deserialize)]
struct BumpChange;
impl flezha_runner::state::Change for BumpChange {}

macro_rules! tier_bench_procedure {
    ($proc:ident, $name:literal, $counter:ident) => {
        #[derive(Default)]
        struct $proc;
        impl Procedure for $proc {
            type Input = Vec<u8>;
            type Output = Vec<u8>;
            type State = i64;
            type Change = BumpChange;
            type Clock = WallClock;
            type Config = ();
            fn name() -> &'static str {
                $name
            }
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
                0
            }
            fn apply_change(state: &mut i64, _: &BumpChange) {
                *state += 1;
            }
            fn run(
                &mut self,
                input: Vec<u8>,
                mut state: StateHandle<'_, Self>,
                ctx: &mut ProcedureCtx<'_>,
            ) -> Result<(), ProcedureError> {
                state.apply(BumpChange);
                ctx.emit_keyed::<Vec<u8>>("k".to_string(), input)?;
                $counter.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    };
}

tier_bench_procedure!(TierOffProcedure, "tier_off_procedure", TIER_OFF_COUNTER);
tier_bench_procedure!(TierOnProcedure, "tier_on_procedure", TIER_ON_COUNTER);

struct TierOffOperative;
impl StreamOperative for TierOffOperative {
    fn factory_id() -> &'static str {
        "tier_off_bench_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<TierOffProcedure>()
            .named("out")
            .keyed()
            .sink("bench");
    }
}

struct TierOnOperative;
impl StreamOperative for TierOnOperative {
    fn factory_id() -> &'static str {
        "tier_on_bench_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.input::<Vec<u8>>("input")
            .on_full(OverflowPolicy::Block)
            .into::<TierOnProcedure>()
            .named("out")
            .keyed()
            .sink("bench");
    }
}

/// Socketpair whose read half is drained by a background thread, so the
/// tier-off variant's primary-stream emits never block on a full buffer.
fn drained_stream() -> Arc<FlezhaStream> {
    let (a, mut b) = UnixStream::pair().expect("UnixStream::pair");
    std::thread::spawn(move || {
        use std::io::Read;
        let mut buf = [0u8; 65536];
        while b.read(&mut buf).map(|n| n > 0).unwrap_or(false) {}
    });
    Arc::new(FlezhaStream::Unix(a))
}

fn throughput_effectively_once_tiers(c: &mut Criterion) {
    let msgs_per_iter = 50_000usize;

    // Discard state frames: the bench measures the SDK-side emit paths,
    // not the controller.
    let sink: SendFrameFn =
        Arc::new(|_pid: &str, _frame: &[u8]| -> Result<(), SendError> { Ok(()) });
    state_sink::install_global_sink(sink);

    let registry = Arc::new(OperativeRegistry::new());

    for (factory, id) in [
        (
            build_topology_for::<TierOffOperative>().expect("topology"),
            "tier-off-bench",
        ),
        (
            build_topology_for::<TierOnOperative>().expect("topology"),
            "tier-on-bench",
        ),
    ] {
        let instance = StreamInstance::new(factory, Arc::from(id));
        registry.register_stream(instance);
    }

    let handle = bi::OperativeDispatchHandle {
        registry: Arc::clone(&registry),
    };
    let stream = drained_stream();
    let payload: Vec<u8> = b"x".repeat(64);

    let mut group = c.benchmark_group("throughput_effectively_once_tiers");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(10);
    group.throughput(criterion::Throughput::Elements(msgs_per_iter as u64));

    for (bench_name, instance_id, counter) in [
        ("tier_off", "tier-off-bench", &TIER_OFF_COUNTER),
        ("tier_on", "tier-on-bench", &TIER_ON_COUNTER),
    ] {
        group.bench_function(bench_name, |b| {
            b.iter_custom(|iters| {
                let mut elapsed = Duration::ZERO;
                for _ in 0..iters {
                    counter.store(0, Ordering::Release);
                    let start = Instant::now();
                    for i in 0..msgs_per_iter {
                        let mut md = HashMap::new();
                        md.insert("channel".to_string(), "input".to_string());
                        md.insert("flz.offset".to_string(), i.to_string());
                        handle.dispatch(instance_id, payload.clone(), &md, Arc::clone(&stream));
                    }
                    let drained = wait_until(
                        || counter.load(Ordering::Acquire) == msgs_per_iter,
                        Duration::from_secs(30),
                    );
                    elapsed += start.elapsed();
                    assert!(
                        drained,
                        "{bench_name} drain timed out: {} of {}",
                        counter.load(Ordering::Acquire),
                        msgs_per_iter
                    );
                }
                elapsed
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    throughput_single_entity,
    throughput_static_fanout,
    throughput_effectively_once_tiers
);
criterion_main!(benches);
