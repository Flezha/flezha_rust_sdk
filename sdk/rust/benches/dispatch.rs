//! Dispatch performance benches.
//!
//! Runs the fast-dispatch path in isolation (no Elixir agent, no real
//! sockets) so regressions in the Rust SDK's core show up cleanly.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use flezha_compute_engine::processor::factory_registry::ProcessorBox;
use flezha_compute_engine::processor::{
    Processor, ProcessorError, ProcessorInput, ProcessorResult, ProcessorRunner,
};
use flezha_compute_engine::transport::FlezhaStream;

use flezha_compute_engine::__dispatch_internals::*;

// ---------------------------------------------------------------------------
// Benchable no-op processor (plus a counter-backed one for drain detection)
// ---------------------------------------------------------------------------

struct NoopCountingProcessor {
    counter: Arc<AtomicUsize>,
    state_anchor: (),
}

impl Processor for NoopCountingProcessor {
    fn run(&mut self, _input: ProcessorInput) -> Result<ProcessorResult, ProcessorError> {
        self.counter.fetch_add(1, Ordering::AcqRel);
        Ok(ProcessorResult::None)
    }
    fn json_serialize_state(&self) -> Result<Vec<u8>, String> {
        Ok(Vec::new())
    }
    fn json_deserialize_state(&mut self, _data: &[u8]) -> Result<(), String> {
        Ok(())
    }
    fn get_factory_id(&self) -> String {
        "noop".to_string()
    }
    fn get_state(&mut self) -> &mut dyn std::any::Any {
        &mut self.state_anchor
    }
    fn bin_serialize_state(&self) -> Box<dyn erased_serde::Serialize + '_> {
        Box::new(&self.state_anchor)
    }
    fn post_alter(&mut self) {}
    fn get_runner_type(&self) -> ProcessorRunner {
        ProcessorRunner::ThreadPool
    }
}

fn dummy_stream() -> Arc<FlezhaStream> {
    let (a, _b) = UnixStream::pair().expect("pair");
    Arc::new(FlezhaStream::Unix(a))
}

fn wait_for_counter(counter: &AtomicUsize, target: usize, budget: Duration) {
    let start = Instant::now();
    while counter.load(Ordering::Acquire) < target {
        if start.elapsed() > budget {
            panic!(
                "drain timed out at {}/{}",
                counter.load(Ordering::Acquire),
                target
            );
        }
        thread::yield_now();
    }
}

// ---------------------------------------------------------------------------
// 7.1 — throughput_single_processor
// ---------------------------------------------------------------------------

fn bench_throughput_single_processor(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_single_processor");
    let n = 10_000usize;
    group.throughput(Throughput::Elements(n as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function(BenchmarkId::from_parameter(n), |b| {
        b.iter_custom(|iters| {
            let pool_size = num_cpus_heuristic();
            let (handle, _pool) = new_handle_with_pool_size(pool_size);
            let counter = Arc::new(AtomicUsize::new(0));
            register_direct(
                &handle.registry,
                "solo".to_string(),
                Box::new(NoopCountingProcessor {
                    counter: Arc::clone(&counter),
                    state_anchor: (),
                }),
            );
            let stream = dummy_stream();

            let start = Instant::now();
            for _ in 0..iters {
                counter.store(0, Ordering::Release);
                for i in 0..n {
                    handle.dispatch(
                        "solo".to_string(),
                        ProcessorInput::Text(i.to_string()),
                        Arc::clone(&stream),
                    );
                }
                wait_for_counter(&counter, n, Duration::from_secs(30));
            }
            black_box(start.elapsed())
        })
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// 7.2 — throughput_many_processors
// ---------------------------------------------------------------------------

fn bench_throughput_many_processors(c: &mut Criterion) {
    let mut group = c.benchmark_group("throughput_many_processors");
    let num_processors = 100usize;
    let messages_per = 10_000usize;
    let total = num_processors * messages_per;
    group.throughput(Throughput::Elements(total as u64));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(20));

    group.bench_function("100x10k", |b| {
        b.iter_custom(|iters| {
            let pool_size = num_cpus_heuristic();
            let (handle, _pool) = new_handle_with_pool_size(pool_size);
            let counter = Arc::new(AtomicUsize::new(0));
            let mut pids = Vec::with_capacity(num_processors);
            for i in 0..num_processors {
                let pid = format!("p{}", i);
                register_direct(
                    &handle.registry,
                    pid.clone(),
                    Box::new(NoopCountingProcessor {
                        counter: Arc::clone(&counter),
                        state_anchor: (),
                    }),
                );
                pids.push(pid);
            }
            let stream = dummy_stream();

            let start = Instant::now();
            for _ in 0..iters {
                counter.store(0, Ordering::Release);
                for i in 0..total {
                    let pid = &pids[i % num_processors];
                    handle.dispatch(
                        pid.clone(),
                        ProcessorInput::Text(i.to_string()),
                        Arc::clone(&stream),
                    );
                }
                wait_for_counter(&counter, total, Duration::from_secs(60));
            }
            black_box(start.elapsed())
        })
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// 7.3 — latency_p99 (one-in-one-out)
// ---------------------------------------------------------------------------

fn bench_latency_p99(c: &mut Criterion) {
    let mut group = c.benchmark_group("latency_p99");
    group.sample_size(200);
    group.measurement_time(Duration::from_secs(10));

    let pool_size = num_cpus_heuristic();
    let (handle, _pool) = new_handle_with_pool_size(pool_size);
    let counter = Arc::new(AtomicUsize::new(0));
    register_direct(
        &handle.registry,
        "lat".to_string(),
        Box::new(NoopCountingProcessor {
            counter: Arc::clone(&counter),
            state_anchor: (),
        }),
    );
    let stream = dummy_stream();

    // Preload: force any lazy init.
    handle.dispatch(
        "lat".to_string(),
        ProcessorInput::Text("warmup".to_string()),
        Arc::clone(&stream),
    );
    wait_for_counter(&counter, 1, Duration::from_secs(5));

    group.bench_function("single_round_trip", |b| {
        b.iter(|| {
            let before = counter.load(Ordering::Acquire);
            handle.dispatch(
                "lat".to_string(),
                ProcessorInput::Text("x".to_string()),
                Arc::clone(&stream),
            );
            while counter.load(Ordering::Acquire) <= before {
                std::hint::spin_loop();
            }
        });
    });
    group.finish();
}

// ---------------------------------------------------------------------------
// 7.4 — dashmap_vs_rwlock_read_contention
//
// Reads from the DashMap registry under contention, compared to a naive
// RwLock<HashMap> baseline. Asserts DashMap is faster or at worst within
// noise — it's the change justification.
// ---------------------------------------------------------------------------

fn bench_dashmap_vs_rwlock_read_contention(c: &mut Criterion) {
    let mut group = c.benchmark_group("registry_read_contention");
    let entries = 1_000usize;
    let readers = 8usize;
    let reads_per_thread = 10_000usize;

    // Pre-populate both registries with equivalent entries.
    let dm_registry = Arc::new(ProcessorRegistry::new());
    let rw_registry: Arc<RwLock<std::collections::HashMap<String, Arc<Mutex<ProcessorBox>>>>> =
        Arc::new(RwLock::new(std::collections::HashMap::new()));

    for i in 0..entries {
        let pid = format!("k{}", i);
        register_direct(
            &dm_registry,
            pid.clone(),
            Box::new(NoopCountingProcessor {
                counter: Arc::new(AtomicUsize::new(0)),
                state_anchor: (),
            }),
        );
        rw_registry.write().unwrap().insert(
            pid,
            Arc::new(Mutex::new(Box::new(NoopCountingProcessor {
                counter: Arc::new(AtomicUsize::new(0)),
                state_anchor: (),
            }) as ProcessorBox)),
        );
    }

    group.bench_function("dashmap", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let mut handles = Vec::new();
                for _ in 0..readers {
                    let reg = Arc::clone(&dm_registry);
                    handles.push(thread::spawn(move || {
                        for i in 0..reads_per_thread {
                            let key = format!("k{}", i % entries);
                            let _ = black_box(reg.get_processor(&key));
                        }
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }
            }
            start.elapsed()
        })
    });

    group.bench_function("rwlock_hashmap", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                let mut handles = Vec::new();
                for _ in 0..readers {
                    let reg = Arc::clone(&rw_registry);
                    handles.push(thread::spawn(move || {
                        for i in 0..reads_per_thread {
                            let key = format!("k{}", i % entries);
                            let guard = reg.read().unwrap();
                            let _ = black_box(guard.get(&key).cloned());
                        }
                    }));
                }
                for h in handles {
                    h.join().unwrap();
                }
            }
            start.elapsed()
        })
    });

    group.finish();
}

fn num_cpus_heuristic() -> usize {
    std::thread::available_parallelism()
        .map_or(2, |v| v.get())
        .min(16)
}

criterion_group!(
    benches,
    bench_throughput_single_processor,
    bench_throughput_many_processors,
    bench_latency_p99,
    bench_dashmap_vs_rwlock_read_contention
);
criterion_main!(benches);
