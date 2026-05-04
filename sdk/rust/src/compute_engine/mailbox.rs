//! Per-processor mailbox: lock-free FIFO + inflight flag.
//!
//! Invariant (sticky ordering):
//!   For a given mailbox, at most one worker thread is inside `run_drain`
//!   at any wall-clock instant. Items are processed in FIFO arrival order.
//!
//! Wake protocol:
//!   - Producers call `push_and_maybe_submit(item, pool, drain_fn)`:
//!       1. enqueue the item (lock-free push on `SegQueue`);
//!       2. CAS `inflight false→true`. On success, the caller becomes the
//!          "owner" and submits a drain closure to the work-stealing pool.
//!          On failure someone else already owns the drain and will pick
//!          up the new item.
//!   - The drain loop pops items until the queue is empty, then stores
//!     `inflight = false`, then re-checks the queue. If the queue became
//!     non-empty between the last pop and the store (a concurrent push
//!     beat us), we re-CAS to resume ownership; otherwise we exit.
//!
//! The re-check-and-re-CAS closes the classic missed-wake race:
//!
//!   T1 (drainer):                    T2 (producer):
//!     pop() -> None                    push(item)
//!                                      CAS false→true -> FAILS (still true)
//!     store(inflight = false)
//!     pop() -> Some(item)  <-- rescues the missed wake
//!
//! This is the standard "actor dispatch" pattern used in Erlang's run queue
//! and Akka's Dispatcher.

use crossbeam::queue::SegQueue;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::compute_engine::work_stealing_pool::{Task, WorkStealingPool};

/// A single item on the wire for a processor — the work the drain loop will
/// hand to `processor::executor::exec_agent`. We keep the fields opaque to
/// the mailbox so this module is decoupled from `ProcessorInput` internals.
pub struct WorkItem<Payload> {
    pub payload: Payload,
}

/// Per-processor mailbox.
///
/// `Payload` is the user-defined work item shape. For Flezha, it's
/// `(ProcessorInput, Arc<FlezhaStream>)` — see
/// `compute_engine::mailbox_runtime::ProcessorPayload`.
pub struct Mailbox<Payload: Send + 'static> {
    queue: SegQueue<WorkItem<Payload>>,
    inflight: AtomicBool,
}

impl<Payload: Send + 'static> Mailbox<Payload> {
    pub fn new() -> Self {
        Mailbox {
            queue: SegQueue::new(),
            inflight: AtomicBool::new(false),
        }
    }

    /// Enqueue an item and, if nobody is draining, submit a per-item drain.
    ///
    /// `drain_fn` is invoked once per popped item. Preserved for callers
    /// that don't need batching (e.g. correctness tests).
    pub fn push_and_maybe_submit<F>(
        self: &Arc<Self>,
        payload: Payload,
        pool: &WorkStealingPool,
        drain_fn: F,
    ) where
        F: Fn(Payload) + Send + Sync + 'static,
    {
        self.queue.push(WorkItem { payload });

        if self
            .inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let this = Arc::clone(self);
            let drain = Arc::new(drain_fn);
            let task: Task = Box::new(move || {
                this.run_drain(&*drain);
            });
            pool.submit(task);
        }
    }

    /// Enqueue an item and, if nobody is draining, submit a BATCH drain.
    ///
    /// `drain_fn` receives a scratch `&mut BatchContext<Payload>` and is
    /// expected to call `ctx.next()` in a loop to pull items, doing per-item
    /// work (e.g. running the processor), and then flushing any accumulated
    /// side effects (e.g. one big `write_all` back to the agent) before
    /// returning. Between calls to `drain_fn`, the mailbox performs the
    /// same missed-wake guard as the per-item variant.
    ///
    /// This is what the fast-dispatch hot path uses so 10k messages hitting
    /// one processor during a burst produce ONE syscall, not 10k.
    pub fn push_and_maybe_submit_batched<F>(
        self: &Arc<Self>,
        payload: Payload,
        pool: &WorkStealingPool,
        drain_fn: F,
    ) where
        F: Fn(&mut BatchContext<Payload>) + Send + Sync + 'static,
    {
        self.queue.push(WorkItem { payload });

        if self
            .inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let this = Arc::clone(self);
            let drain = Arc::new(drain_fn);
            let task: Task = Box::new(move || {
                this.run_drain_batched(&*drain);
            });
            pool.submit(task);
        }
    }

    fn run_drain<F>(self: Arc<Self>, drain_fn: &F)
    where
        F: Fn(Payload) + Send + Sync + 'static,
    {
        loop {
            while let Some(item) = self.queue.pop() {
                drain_fn(item.payload);
            }
            self.inflight.store(false, Ordering::Release);
            if self.queue.is_empty() {
                return;
            }
            if self
                .inflight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                continue;
            }
            return;
        }
    }

    fn run_drain_batched<F>(self: Arc<Self>, drain_fn: &F)
    where
        F: Fn(&mut BatchContext<Payload>) + Send + Sync + 'static,
    {
        loop {
            let mut ctx = BatchContext { mbox: &self };
            drain_fn(&mut ctx);

            self.inflight.store(false, Ordering::Release);
            if self.queue.is_empty() {
                return;
            }
            if self
                .inflight
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                continue;
            }
            return;
        }
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    // ---- Low-level primitives for callers that want to manage the drain
    //      task themselves (avoiding per-dispatch closure allocation). ----

    /// Push a payload without allocating/submitting anything. Pair with
    /// `try_mark_inflight` to decide whether THIS caller should schedule
    /// the drain.
    #[inline]
    pub fn push_only(&self, payload: Payload) {
        self.queue.push(WorkItem { payload });
    }

    /// CAS inflight false→true. Returns true on success (caller owns the
    /// drain responsibility) and false if someone else is already draining.
    #[inline]
    pub fn try_mark_inflight(&self) -> bool {
        self.inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Clear the inflight flag. Caller is responsible for re-checking the
    /// queue and re-acquiring inflight if more items arrived.
    #[inline]
    pub fn release_inflight(&self) {
        self.inflight.store(false, Ordering::Release);
    }

    /// Pop the next item. Returns None if empty.
    #[inline]
    pub fn pop(&self) -> Option<Payload> {
        self.queue.pop().map(|item| item.payload)
    }
}

/// Handle to a currently-draining mailbox. The drain closure pulls items
/// via `next()` until `None`, does per-item work + a single flush of any
/// accumulated bytes before returning.
pub struct BatchContext<'a, Payload: Send + 'static> {
    mbox: &'a Mailbox<Payload>,
}

impl<'a, Payload: Send + 'static> BatchContext<'a, Payload> {
    #[inline]
    pub fn next(&mut self) -> Option<Payload> {
        self.mbox.queue.pop().map(|item| item.payload)
    }
}

impl<Payload: Send + 'static> Default for Mailbox<Payload> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn single_producer_drains_in_order() {
        let pool = WorkStealingPool::with_size(2);
        let mbox: Arc<Mailbox<usize>> = Arc::new(Mailbox::new());
        let seen: Arc<std::sync::Mutex<Vec<usize>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let done = Arc::new(AtomicUsize::new(0));

        let seen_clone = Arc::clone(&seen);
        let done_clone = Arc::clone(&done);
        let drain = move |n: usize| {
            seen_clone.lock().unwrap().push(n);
            done_clone.fetch_add(1, Ordering::AcqRel);
        };
        let drain = Arc::new(drain);

        let n = 1000usize;
        for i in 0..n {
            let drain = Arc::clone(&drain);
            mbox.push_and_maybe_submit(i, &pool, move |p| drain(p));
        }

        let start = std::time::Instant::now();
        while done.load(Ordering::Acquire) < n {
            if start.elapsed() > Duration::from_secs(5) {
                panic!("drain timeout");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let v = seen.lock().unwrap().clone();
        assert_eq!(v.len(), n);
        for i in 0..n {
            assert_eq!(v[i], i, "out-of-order at {}", i);
        }
    }

    #[test]
    fn concurrent_pushes_never_overlap_drain() {
        let pool = WorkStealingPool::with_size(4);
        let mbox: Arc<Mailbox<usize>> = Arc::new(Mailbox::new());
        let concurrent = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicUsize::new(0));

        let c = Arc::clone(&concurrent);
        let m = Arc::clone(&max_seen);
        let d = Arc::clone(&done);
        let drain = Arc::new(move |_n: usize| {
            let cur = c.fetch_add(1, Ordering::AcqRel) + 1;
            // bump max-seen
            let mut prev = m.load(Ordering::Acquire);
            while cur > prev {
                match m.compare_exchange_weak(prev, cur, Ordering::AcqRel, Ordering::Acquire) {
                    Ok(_) => break,
                    Err(p) => prev = p,
                }
            }
            // tiny pause to widen the window
            for _ in 0..10 {
                std::hint::spin_loop();
            }
            c.fetch_sub(1, Ordering::AcqRel);
            d.fetch_add(1, Ordering::AcqRel);
        });

        let total = 2_000usize;
        let producers = 4;
        let per_producer = total / producers;

        let mut handles = Vec::new();
        for _ in 0..producers {
            let mbox = Arc::clone(&mbox);
            let pool_ref = &pool as *const _;
            let drain = Arc::clone(&drain);
            // SAFETY: pool outlives all producer threads (joined below).
            let pool_ref = unsafe { &*pool_ref };
            handles.push(std::thread::spawn(move || {
                for i in 0..per_producer {
                    let drain = Arc::clone(&drain);
                    mbox.push_and_maybe_submit(i, pool_ref, move |p| drain(p));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let start = std::time::Instant::now();
        while done.load(Ordering::Acquire) < total {
            if start.elapsed() > Duration::from_secs(10) {
                panic!("concurrent drain timeout");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(
            max_seen.load(Ordering::Acquire),
            1,
            "mailbox allowed concurrent drain!"
        );
    }
}
