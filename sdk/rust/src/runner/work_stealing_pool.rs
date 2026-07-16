//! Chase-Lev work-stealing pool used by the fast-dispatch path.
//!
//! Design:
//! - One `Injector<Task>` shared by all producers.
//! - N worker threads, each owning its own `Worker<Task>` deque.
//! - Each worker also holds a `Vec<Stealer<Task>>` for its siblings.
//! - Workers park via `thread::park_timeout` when idle; `submit()` unparks one.
//! - A short spin before parking keeps bursty dispatch snappy.
//!
//! Lifecycle: `WorkStealingPool::new()` (or `with_size`) spawns the workers.
//! Dropping the pool sets `shutdown = true` and unparks every worker so they
//! exit their loop. Workers are joined implicitly via `JoinHandle::drop` is
//! NOT a join — so we `join()` explicitly in `Drop` to avoid leaking threads.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle, Thread};
use std::time::Duration;
use std::{env, iter};

use crossbeam_deque::{Injector, Stealer, Worker};

pub type Task = Box<dyn FnOnce() + Send + 'static>;

struct Shared {
    injector: Injector<Task>,
    stealers: Vec<Stealer<Task>>,
    workers_parked: Vec<AtomicBool>,
    /// Round-robin hint used by `submit` to spread wakeups across workers.
    next_wake: AtomicUsize,
    shutdown: AtomicBool,
}

pub struct WorkStealingPool {
    shared: Arc<Shared>,
    /// Kept so `submit` can unpark a specific worker; filled at construction.
    worker_threads: Arc<Vec<Thread>>,
    handles: Vec<Option<JoinHandle<()>>>,
}

impl WorkStealingPool {
    pub fn new() -> Self {
        Self::with_size(number_of_workers())
    }

    pub fn with_size(worker_count: usize) -> Self {
        assert!(worker_count > 0, "worker_count must be > 0");

        let workers: Vec<Worker<Task>> = (0..worker_count).map(|_| Worker::new_fifo()).collect();
        let stealers: Vec<Stealer<Task>> = workers.iter().map(|w| w.stealer()).collect();
        let workers_parked: Vec<AtomicBool> =
            (0..worker_count).map(|_| AtomicBool::new(false)).collect();

        let shared = Arc::new(Shared {
            injector: Injector::new(),
            stealers,
            workers_parked,
            next_wake: AtomicUsize::new(0),
            shutdown: AtomicBool::new(false),
        });

        // Spawn workers; grab their Thread refs before returning.
        let mut handles: Vec<Option<JoinHandle<()>>> = Vec::with_capacity(worker_count);
        let mut worker_threads: Vec<Thread> = Vec::with_capacity(worker_count);

        // We need to give the wake logic a way to unpark a specific worker.
        // One-shot channels let workers publish their own `Thread::current()`
        // handle before they start work.
        let mut thread_rx: Vec<crossbeam::channel::Receiver<Thread>> =
            Vec::with_capacity(worker_count);

        for (idx, worker) in workers.into_iter().enumerate() {
            let shared = Arc::clone(&shared);
            let (tx, rx) = crossbeam::channel::bounded::<Thread>(1);
            thread_rx.push(rx);

            let handle = thread::Builder::new()
                .name(format!("flezha-ws-{}", idx))
                .spawn(move || {
                    let _ = tx.send(thread::current());
                    run_worker(idx, worker, shared);
                })
                .expect("failed to spawn work-stealing worker");
            handles.push(Some(handle));
        }

        for rx in thread_rx {
            let t = rx.recv().expect("worker thread handle not sent");
            worker_threads.push(t);
        }

        WorkStealingPool {
            shared,
            worker_threads: Arc::new(worker_threads),
            handles,
        }
    }

    /// Submit a task. Pushes onto the global injector and wakes one parked worker.
    pub fn submit(&self, task: Task) {
        self.shared.injector.push(task);
        wake_one(&self.shared, &self.worker_threads);
    }

    pub fn worker_count(&self) -> usize {
        self.shared.stealers.len()
    }
}

impl Default for WorkStealingPool {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for WorkStealingPool {
    fn drop(&mut self) {
        self.shared.shutdown.store(true, Ordering::SeqCst);
        for t in self.worker_threads.iter() {
            t.unpark();
        }
        for h in self.handles.iter_mut() {
            if let Some(handle) = h.take() {
                let _ = handle.join();
            }
        }
    }
}

fn wake_one(shared: &Shared, threads: &[Thread]) {
    let n = shared.workers_parked.len();
    if n == 0 {
        return;
    }
    let start = shared.next_wake.fetch_add(1, Ordering::Relaxed) % n;
    for offset in 0..n {
        let idx = (start + offset) % n;
        if shared.workers_parked[idx]
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            threads[idx].unpark();
            return;
        }
    }
    // No worker parked: either all are busy or all are spinning. Either way
    // the task will be picked up without an explicit unpark.
}

fn run_worker(idx: usize, local: Worker<Task>, shared: Arc<Shared>) {
    let spin_limit = 64u32;
    let park_timeout = Duration::from_millis(50);

    loop {
        if shared.shutdown.load(Ordering::Acquire) {
            break;
        }

        if let Some(task) = find_task(&local, &shared) {
            (task)();
            continue;
        }

        // Short spin before parking — bursty producers often push another
        // task within a few hundred nanoseconds.
        let mut spun = 0u32;
        let mut got_task = false;
        while spun < spin_limit {
            if let Some(task) = find_task(&local, &shared) {
                (task)();
                got_task = true;
                break;
            }
            if shared.shutdown.load(Ordering::Acquire) {
                return;
            }
            spun += 1;
            std::hint::spin_loop();
        }
        if got_task {
            continue;
        }

        // Park. Mark first, re-check (covers producer racing with mark).
        shared.workers_parked[idx].store(true, Ordering::Release);
        if let Some(task) = find_task(&local, &shared) {
            shared.workers_parked[idx].store(false, Ordering::Release);
            (task)();
            continue;
        }
        thread::park_timeout(park_timeout);
        shared.workers_parked[idx].store(false, Ordering::Release);
    }
}

fn find_task(local: &Worker<Task>, shared: &Shared) -> Option<Task> {
    if let Some(t) = local.pop() {
        return Some(t);
    }
    iter::repeat_with(|| {
        shared
            .injector
            .steal_batch_and_pop(local)
            .or_else(|| shared.stealers.iter().map(|s| s.steal()).collect())
    })
    .find(|s| !s.is_retry())
    .and_then(|s| s.success())
}

/// Determines the number of workers for the pool.
///
/// 1. `FLEZHA_WORKER_POOL_SIZE` env var, if set and valid.
/// 2. `std::thread::available_parallelism()`, else 2.
fn number_of_workers() -> usize {
    let key = "FLEZHA_WORKER_POOL_SIZE";
    let default_workers = std::thread::available_parallelism().map_or(2, |v| v.get());

    let numb = match env::var(key) {
        Ok(val) => val.parse().unwrap_or(default_workers),
        Err(_) => default_workers,
    };

    if numb < 1 {
        panic!("Invalid number of workers specified in {}: {}", key, numb);
    }
    if numb > u16::MAX as usize {
        panic!("Number of workers exceeds maximum limit: {}", u16::MAX);
    }
    numb
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use super::*;

    #[test]
    fn submit_tasks_complete() {
        let pool = WorkStealingPool::with_size(4);
        let counter = Arc::new(AtomicUsize::new(0));
        let total = 10_000usize;

        let (done_tx, done_rx) = crossbeam::channel::bounded::<()>(total);
        for _ in 0..total {
            let counter = Arc::clone(&counter);
            let done_tx = done_tx.clone();
            pool.submit(Box::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                let _ = done_tx.send(());
            }));
        }

        for _ in 0..total {
            done_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("task drop");
        }
        assert_eq!(counter.load(Ordering::Relaxed), total);
    }

    #[test]
    fn basic_throughput_smoke() {
        let pool = WorkStealingPool::with_size(4);
        let counter = Arc::new(AtomicUsize::new(0));
        let total = 100_000usize;
        let (done_tx, done_rx) = crossbeam::channel::bounded::<()>(total);

        let start = Instant::now();
        for _ in 0..total {
            let counter = Arc::clone(&counter);
            let done_tx = done_tx.clone();
            pool.submit(Box::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                let _ = done_tx.send(());
            }));
        }
        for _ in 0..total {
            done_rx
                .recv_timeout(Duration::from_secs(30))
                .expect("task drop");
        }
        let elapsed = start.elapsed();
        eprintln!(
            "[ws-pool smoke] {} tasks in {:?} => {:.0} tasks/s",
            total,
            elapsed,
            total as f64 / elapsed.as_secs_f64()
        );
        assert_eq!(counter.load(Ordering::Relaxed), total);
    }
}
