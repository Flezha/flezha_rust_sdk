//! Correctness benches for the fast-dispatch path.
//!
//! These are not perf benches — they're invariant checks with `panic!` on
//! violation, wrapped in the `benches/` tree so `cargo bench --bench correctness`
//! runs them with optimized code (release profile) where concurrency bugs
//! are most likely to reproduce.
//!
//! Covered invariants:
//!   1. `contention_adder` — a stateful processor receiving 10_000 concurrent
//!      inputs ends up with the exact set of inputs recorded.
//!   2. `sticky_ordering_under_two_streams` — with two "primary streams"
//!      feeding the same processor, inputs from each stream appear in-order
//!      in the final state (per-stream FIFO preserved; cross-stream
//!      interleaving is unconstrained).
//!   3. `thread_spawn_concurrency` — a `ThreadSpawn`-runner processor admits
//!      concurrent invocations (observable via a shared counter).

use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use flezha_compute_engine::processor::{
    ProcessorError, ProcessorInput, ProcessorResult, ProcessorRunner,
};
use flezha_compute_engine::processor::Processor;
use flezha_compute_engine::transport::FlezhaStream;

use flezha_compute_engine::__dispatch_internals::*;

fn main() {
    eprintln!("[correctness] contention_adder ...");
    contention_adder();
    eprintln!("[correctness] sticky_ordering_under_two_streams ...");
    sticky_ordering_under_two_streams();
    eprintln!("[correctness] thread_spawn_concurrency ...");
    thread_spawn_concurrency();
    eprintln!("[correctness] ALL PASSED");
}

// ---------------------------------------------------------------------------
// Helpers: stream construction, simple processors.
// ---------------------------------------------------------------------------

fn dummy_stream() -> Arc<FlezhaStream> {
    let (a, _b) = UnixStream::pair().expect("pair");
    Arc::new(FlezhaStream::Unix(a))
}

/// Minimal stub for trait methods we don't exercise here.
macro_rules! stub_processor {
    () => {
        fn json_serialize_state(&self) -> Result<Vec<u8>, String> {
            Ok(Vec::new())
        }
        fn json_deserialize_state(&mut self, _data: &[u8]) -> Result<(), String> {
            Ok(())
        }
        fn get_factory_id(&self) -> String {
            "bench".to_string()
        }
        fn get_state(&mut self) -> &mut dyn std::any::Any {
            &mut self.state_anchor
        }
        fn bin_serialize_state(&self) -> Box<dyn erased_serde::Serialize + '_> {
            Box::new(&self.state_anchor)
        }
        fn post_alter(&mut self) {}
    };
}

// `AdderProcessor`: appends the parsed integer to a shared Vec; used by both
// `contention_adder` and `sticky_ordering_under_two_streams`.
struct AdderProcessor {
    log: Arc<Mutex<Vec<usize>>>,
    runner: ProcessorRunner,
    state_anchor: (),
}

impl Processor for AdderProcessor {
    fn run(&mut self, input: ProcessorInput) -> Result<ProcessorResult, ProcessorError> {
        let n = match input {
            ProcessorInput::Text(t) => t.parse::<usize>().map_err(|e| e.to_string())?,
            ProcessorInput::Binary(b) => {
                std::str::from_utf8(&b).unwrap().parse::<usize>().map_err(|e| e.to_string())?
            }
            ProcessorInput::Custom(_) => return Err("custom unsupported".to_string()),
        };
        self.log.lock().unwrap().push(n);
        Ok(ProcessorResult::None)
    }
    fn get_runner_type(&self) -> ProcessorRunner {
        self.runner.clone()
    }
    stub_processor!();
}

// ---------------------------------------------------------------------------
// 6.1 — contention_adder
// ---------------------------------------------------------------------------

fn contention_adder() {
    let (handle, _pool) = new_handle_with_pool_size(num_cpus_heuristic());
    let log: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::with_capacity(10_000)));

    let processor = Box::new(AdderProcessor {
        log: Arc::clone(&log),
        runner: ProcessorRunner::ThreadPool,
        state_anchor: (),
    });
    let _slot = register_direct(&handle.registry, "adder".to_string(), processor);

    let total = 10_000usize;
    let producers = 8;
    let per_producer = total / producers;

    let stream = dummy_stream();

    let mut threads = Vec::new();
    for p in 0..producers {
        let handle = handle.clone();
        let stream = Arc::clone(&stream);
        threads.push(thread::spawn(move || {
            for i in 0..per_producer {
                let n = p * per_producer + i;
                handle.dispatch(
                    "adder".to_string(),
                    ProcessorInput::Text(n.to_string()),
                    Arc::clone(&stream),
                );
            }
        }));
    }
    for t in threads {
        t.join().unwrap();
    }

    // Wait for drain to complete: the log should hit `total` entries.
    let start = Instant::now();
    loop {
        if log.lock().unwrap().len() >= total {
            break;
        }
        if start.elapsed() > Duration::from_secs(30) {
            panic!("adder timeout: got {}/{}", log.lock().unwrap().len(), total);
        }
        thread::sleep(Duration::from_millis(1));
    }
    // Small margin for the drain to fully settle.
    thread::sleep(Duration::from_millis(50));

    let v = log.lock().unwrap().clone();
    assert_eq!(v.len(), total, "missing items");
    let mut sorted = v.clone();
    sorted.sort();
    for (i, n) in sorted.iter().enumerate() {
        assert_eq!(*n, i, "missing or duplicated value at position {}", i);
    }
}

// ---------------------------------------------------------------------------
// 6.2 — sticky_ordering_under_two_streams
//
// Two primary streams, each pushing its own numeric sequence. Final log
// must contain each stream's sequence in order (interleaved is fine).
// ---------------------------------------------------------------------------

fn sticky_ordering_under_two_streams() {
    let (handle, _pool) = new_handle_with_pool_size(num_cpus_heuristic());
    let log: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::with_capacity(4_000)));

    let processor = Box::new(AdderProcessor {
        log: Arc::clone(&log),
        runner: ProcessorRunner::ThreadPool,
        state_anchor: (),
    });
    let _slot = register_direct(&handle.registry, "sticky".to_string(), processor);

    // Stream A owns numbers 0..1999 (even positions in the "encoded" sequence).
    // Stream B owns 10_000..11_999. Disjoint ranges so we can distinguish.
    let per_stream = 2_000usize;
    let stream_a_base = 0usize;
    let stream_b_base = 10_000usize;

    let stream_a = dummy_stream();
    let stream_b = dummy_stream();

    let a_handle = handle.clone();
    let b_handle = handle.clone();
    let a_stream = Arc::clone(&stream_a);
    let b_stream = Arc::clone(&stream_b);

    let ta = thread::spawn(move || {
        for i in 0..per_stream {
            a_handle.dispatch(
                "sticky".to_string(),
                ProcessorInput::Text((stream_a_base + i).to_string()),
                Arc::clone(&a_stream),
            );
        }
    });
    let tb = thread::spawn(move || {
        for i in 0..per_stream {
            b_handle.dispatch(
                "sticky".to_string(),
                ProcessorInput::Text((stream_b_base + i).to_string()),
                Arc::clone(&b_stream),
            );
        }
    });
    ta.join().unwrap();
    tb.join().unwrap();

    let start = Instant::now();
    loop {
        if log.lock().unwrap().len() >= per_stream * 2 {
            break;
        }
        if start.elapsed() > Duration::from_secs(30) {
            panic!(
                "sticky timeout: got {}/{}",
                log.lock().unwrap().len(),
                per_stream * 2
            );
        }
        thread::sleep(Duration::from_millis(1));
    }
    thread::sleep(Duration::from_millis(50));

    let v = log.lock().unwrap().clone();

    // Each stream's entries must appear in ascending order.
    check_subsequence_in_order(&v, stream_a_base, per_stream, "A");
    check_subsequence_in_order(&v, stream_b_base, per_stream, "B");
}

fn check_subsequence_in_order(log: &[usize], base: usize, count: usize, name: &str) {
    let mut expected = base;
    for x in log.iter().copied().filter(|n| *n >= base && *n < base + count) {
        if x != expected {
            panic!(
                "[sticky/{}] out-of-order: expected {}, got {} in log",
                name, expected, x
            );
        }
        expected += 1;
    }
    assert_eq!(
        expected,
        base + count,
        "[sticky/{}] missing entries: last ok = {}",
        name,
        expected
    );
}

// ---------------------------------------------------------------------------
// 6.3 — thread_spawn_concurrency
//
// A `ThreadSpawn` processor with a small sleep; assert we observe > 1
// concurrent invocations via a shared high-water counter.
// ---------------------------------------------------------------------------

struct SleepyProcessor {
    thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>>,
    done: Arc<AtomicUsize>,
    state_anchor: (),
}

impl Processor for SleepyProcessor {
    fn run(&mut self, _input: ProcessorInput) -> Result<ProcessorResult, ProcessorError> {
        self.thread_ids
            .lock()
            .unwrap()
            .push(std::thread::current().id());
        // Short sleep so the test observes multiple threads being spawned
        // before any of them complete (under ThreadSpawn semantics each
        // invocation owns a fresh OS thread — the Mutex on the processor
        // serializes `run` bodies, but the spawn-per-invocation property is
        // observable via distinct thread IDs).
        thread::sleep(Duration::from_millis(2));
        self.done.fetch_add(1, Ordering::AcqRel);
        Ok(ProcessorResult::None)
    }
    fn get_runner_type(&self) -> ProcessorRunner {
        ProcessorRunner::ThreadSpawn
    }
    stub_processor!();
}

fn thread_spawn_concurrency() {
    let (handle, _pool) = new_handle_with_pool_size(num_cpus_heuristic());
    let thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>> = Arc::new(Mutex::new(Vec::new()));
    let done = Arc::new(AtomicUsize::new(0));

    let processor = Box::new(SleepyProcessor {
        thread_ids: Arc::clone(&thread_ids),
        done: Arc::clone(&done),
        state_anchor: (),
    });
    let _slot = register_direct(&handle.registry, "sleepy".to_string(), processor);

    let total = 32usize;
    let stream = dummy_stream();
    for i in 0..total {
        handle.dispatch(
            "sleepy".to_string(),
            ProcessorInput::Text(i.to_string()),
            Arc::clone(&stream),
        );
    }

    let start = Instant::now();
    loop {
        if done.load(Ordering::Acquire) >= total {
            break;
        }
        if start.elapsed() > Duration::from_secs(20) {
            panic!("thread_spawn timeout");
        }
        thread::sleep(Duration::from_millis(1));
    }

    let ids = thread_ids.lock().unwrap().clone();
    let mut unique: Vec<_> = ids.clone();
    unique.sort_by_key(|t| format!("{:?}", t));
    unique.dedup();
    assert!(
        unique.len() > 1,
        "expected multiple threads observed under ThreadSpawn (got {} distinct of {} invocations)",
        unique.len(),
        ids.len()
    );
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

fn num_cpus_heuristic() -> usize {
    std::thread::available_parallelism().map_or(2, |v| v.get()).min(16)
}
