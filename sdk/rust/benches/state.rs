//! `state_apply_throughput` Criterion bench per design.md Decision 10.
//!
//! Records nanoseconds-per-`state.apply(...)` call across the
//! `[SmallChange (8B), LargeChange (1KB)] × [NoCoalesceKey, WithCoalesceKey]
//! × [no_prealloc, prealloc_1024]` matrix. Asserts the umbrella's
//! "Vec::push only" hot-path commitment.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::{Deserialize, Serialize};

use flezha_compute_engine::state::{Change, CoalesceKey};

#[derive(Serialize, Deserialize, Clone)]
struct SmallChange {
    delta: i64,
}
impl Change for SmallChange {}

#[derive(Serialize, Deserialize, Clone)]
struct SmallChangeKeyed {
    delta: i64,
    key: u64,
}
impl Change for SmallChangeKeyed {
    fn coalesce_key(&self) -> Option<CoalesceKey> {
        Some(CoalesceKey::from_hash(&self.key))
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct LargeChange {
    payload: Vec<u8>,
}
impl Change for LargeChange {}

#[derive(Serialize, Deserialize, Clone)]
struct LargeChangeKeyed {
    payload: Vec<u8>,
    key: u64,
}
impl Change for LargeChangeKeyed {
    fn coalesce_key(&self) -> Option<CoalesceKey> {
        Some(CoalesceKey::from_hash(&self.key))
    }
}

/// Apply 1024 changes to a buffer (with optional pre-allocation) and
/// pretend-apply by mutating an `i64` counter. This mirrors the
/// `StateHandle::apply` hot path: one `Vec::push` plus a tight user
/// closure for `apply_change`.
fn run_dimension<C: Change + Clone>(
    c: &mut Criterion,
    label: &str,
    sample: C,
    prealloc: Option<usize>,
) {
    let mut group = c.benchmark_group("state_apply");
    group.throughput(Throughput::Elements(1024));
    group.bench_function(BenchmarkId::new(label, prealloc.unwrap_or(0)), |b| {
        b.iter(|| {
            let mut state: i64 = 0;
            let mut buffer: Vec<C> = match prealloc {
                Some(n) => Vec::with_capacity(n),
                None => Vec::new(),
            };
            for _ in 0..1024 {
                state = state.wrapping_add(1);
                buffer.push(sample.clone());
            }
            criterion::black_box((state, buffer));
        });
    });
    group.finish();
}

fn state_apply_throughput(c: &mut Criterion) {
    run_dimension::<SmallChange>(
        c,
        "small_no_key_no_prealloc",
        SmallChange { delta: 1 },
        None,
    );
    run_dimension::<SmallChange>(
        c,
        "small_no_key_prealloc_1024",
        SmallChange { delta: 1 },
        Some(1024),
    );
    run_dimension::<SmallChangeKeyed>(
        c,
        "small_keyed_no_prealloc",
        SmallChangeKeyed { delta: 1, key: 7 },
        None,
    );
    run_dimension::<SmallChangeKeyed>(
        c,
        "small_keyed_prealloc_1024",
        SmallChangeKeyed { delta: 1, key: 7 },
        Some(1024),
    );
    run_dimension::<LargeChange>(
        c,
        "large_no_key_no_prealloc",
        LargeChange { payload: vec![0u8; 1024] },
        None,
    );
    run_dimension::<LargeChange>(
        c,
        "large_no_key_prealloc_1024",
        LargeChange { payload: vec![0u8; 1024] },
        Some(1024),
    );
    run_dimension::<LargeChangeKeyed>(
        c,
        "large_keyed_no_prealloc",
        LargeChangeKeyed { payload: vec![0u8; 1024], key: 7 },
        None,
    );
    run_dimension::<LargeChangeKeyed>(
        c,
        "large_keyed_prealloc_1024",
        LargeChangeKeyed { payload: vec![0u8; 1024], key: 7 },
        Some(1024),
    );
}

criterion_group!(benches, state_apply_throughput);
criterion_main!(benches);
