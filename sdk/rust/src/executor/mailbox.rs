//! Per-relay mailbox: bounded lock-free FIFO + inflight flag + counters.
//!
//! Invariants:
//!
//! - **Sticky ordering**: for a given mailbox, at most one worker thread is
//!   inside the drain loop at any wall-clock instant. Items are processed in
//!   FIFO arrival order. The inflight CAS guarantees mutual exclusion.
//! - **Bounded capacity**: the queue is a fixed-size [`ArrayQueue`] sized at
//!   construction (`Mailbox::with_capacity`). Pushes that would exceed the
//!   cap return the rejected payload to the caller; the caller decides what
//!   to do based on the channel's declared `OverflowPolicy` (Block, drop the
//!   newest, or replace the oldest via `force_push`).
//!
//! Wake protocol (unchanged from the unbounded shape):
//!
//!   - Producer calls `try_push(item)` → on success, CASes `inflight false→true`.
//!     On a winning CAS the caller submits a drain task to the work-stealing
//!     pool; on a losing CAS another drain is already in flight and will pop
//!     the new item.
//!   - The drain loop pops items until the queue is empty, stores
//!     `inflight = false`, then re-checks `is_empty`. If a producer's push
//!     raced our store (their CAS failed because we still held inflight, but
//!     their push made it onto the queue), the re-check sees the new item and
//!     we re-CAS to resume ownership.
//!
//!   T1 (drainer):                    T2 (producer):
//!     pop() -> None                    push(item)            // succeeds
//!                                      CAS false→true -> FAILS (still true)
//!     store(inflight = false)
//!     pop() -> Some(item)  <-- rescues the missed wake
//!
//! This is the standard "actor dispatch" pattern used in Erlang's run queue
//! and Akka's Dispatcher; it remains correct under [`ArrayQueue`]'s
//! lock-free push/pop because the queue's internal ordering guarantees that
//! a successful push is observable to a subsequent `is_empty()` /  `pop()`
//! on any thread.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam::queue::ArrayQueue;

use crate::executor::work_stealing_pool::{Task, WorkStealingPool};
use crate::partition::PartitionCounters;

/// A single item on the wire for a relay — the work the drain loop will
/// dispatch via the partition's `Dispatcher`. The wrapper keeps the queue
/// type opaque to the rest of the SDK and lets us recover the rejected
/// payload from a failed `ArrayQueue::push` without an extra clone.
pub struct WorkItem<Payload> {
    pub payload: Payload,
}

/// Returned by the convenience `push_and_maybe_submit*` helpers when the
/// underlying bounded queue rejects the push. The caller can recover the
/// payload from the wrapper.
#[derive(Debug)]
pub struct BackpressureError<Payload> {
    pub payload: Payload,
}

impl<Payload> std::fmt::Display for BackpressureError<Payload> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("mailbox full: push rejected by bounded queue")
    }
}

impl<Payload: std::fmt::Debug> std::error::Error for BackpressureError<Payload> {}

/// Per-Partition mailbox: a bounded `ArrayQueue<WorkItem<Payload>>` plus an
/// `AtomicBool` inflight flag and a [`PartitionCounters`] block.
///
/// `Payload` is the user-defined work item shape. For Flezha, it's
/// [`crate::partition::PartitionPayload`].
pub struct Mailbox<Payload: Send + 'static> {
    queue: ArrayQueue<WorkItem<Payload>>,
    inflight: AtomicBool,
    counters: PartitionCounters,
}

impl<Payload: Send + 'static> Mailbox<Payload> {
    /// Allocate a bounded mailbox with the given capacity. Capacity must
    /// be `>= 1`; the `ArrayQueue` constructor panics on 0, so callers
    /// (i.e. `relay::builder::resolve_mailbox_capacity`) must validate
    /// the value before calling here.
    pub fn with_capacity(capacity: usize) -> Self {
        Mailbox {
            queue: ArrayQueue::new(capacity),
            inflight: AtomicBool::new(false),
            counters: PartitionCounters::default(),
        }
    }

    /// Enqueue an item and, if nobody is draining, submit a per-item drain.
    ///
    /// `drain_fn` is invoked once per popped item. Preserved for callers
    /// that don't need batching (e.g. correctness tests). Returns
    /// `Err(BackpressureError { payload })` when the bounded queue rejects
    /// the push; callers wanting block / drop semantics should use the
    /// low-level `try_push` / `force_push` primitives instead.
    pub fn push_and_maybe_submit<F>(
        self: &Arc<Self>,
        payload: Payload,
        pool: &WorkStealingPool,
        drain_fn: F,
    ) -> Result<(), BackpressureError<Payload>>
    where
        F: Fn(Payload) + Send + Sync + 'static,
    {
        self.try_push(payload)
            .map_err(|payload| BackpressureError { payload })?;

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
        Ok(())
    }

    /// Enqueue an item and, if nobody is draining, submit a BATCH drain.
    ///
    /// `drain_fn` receives a scratch `&mut BatchContext<Payload>` and is
    /// expected to call `ctx.next()` in a loop to pull items, doing per-item
    /// work (e.g. running the relay), and then flushing any accumulated
    /// side effects (e.g. one big `write_all` back to the controller) before
    /// returning. Between calls to `drain_fn`, the mailbox performs the
    /// same missed-wake guard as the per-item variant.
    ///
    /// This is what the fast-dispatch hot path uses so 10k messages hitting
    /// one relay during a burst produce ONE syscall, not 10k. Returns
    /// `Err(BackpressureError { payload })` when the bounded queue rejects
    /// the push.
    pub fn push_and_maybe_submit_batched<F>(
        self: &Arc<Self>,
        payload: Payload,
        pool: &WorkStealingPool,
        drain_fn: F,
    ) -> Result<(), BackpressureError<Payload>>
    where
        F: Fn(&mut BatchContext<Payload>) + Send + Sync + 'static,
    {
        self.try_push(payload)
            .map_err(|payload| BackpressureError { payload })?;

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
        Ok(())
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

    /// Bounded capacity declared at construction. Constant for the
    /// lifetime of the mailbox.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.queue.capacity()
    }

    /// Snapshot of the queue's current length. Approximate under MPMC
    /// contention; exposed for diagnostics, not as a synchronization
    /// primitive.
    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// True when the bounded queue is at capacity. Used by Block-policy
    /// producers to recheck saturation after registering on the parker's
    /// wait list, closing the missed-wake window between a failed
    /// `try_push` and the deque push inside `BlockParker::wait_one`.
    #[inline]
    pub fn is_full(&self) -> bool {
        self.queue.len() >= self.queue.capacity()
    }

    /// Per-Partition counter set. Producer-side enqueue paths bump these
    /// on push success / block / drop events; readers (future metrics
    /// export) use [`Ordering::Relaxed`] loads.
    #[inline]
    pub fn counters(&self) -> &PartitionCounters {
        &self.counters
    }

    // ---- Low-level primitives for callers that want to manage the drain
    //      task themselves (avoiding per-dispatch closure allocation). ----

    /// Try to enqueue a payload onto the bounded queue without scheduling
    /// any drain. Returns the rejected payload on full so the caller can
    /// apply the channel's overflow policy (block-and-retry, drop-newest,
    /// drop-oldest via [`Self::force_push`]).
    #[inline]
    pub fn try_push(&self, payload: Payload) -> Result<(), Payload> {
        self.queue
            .push(WorkItem { payload })
            .map_err(|item| item.payload)
    }

    /// Atomically pop the oldest item if the queue is full, then push the
    /// new payload. Returns the dropped payload to the caller for
    /// accounting (`drops_oldest_total` counter increments). Best-effort
    /// under MPMC contention per Decision 9.
    #[inline]
    pub fn force_push(&self, payload: Payload) -> Option<Payload> {
        self.queue
            .force_push(WorkItem { payload })
            .map(|item| item.payload)
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;

    /// Capacity sized well above the test workload so the bounded queue
    /// never rejects in this pure ordering check; bounded-mailbox
    /// behavior under saturation has its own scenario tests in
    /// `tests/relay_dispatch.rs`.
    const TEST_CAPACITY: usize = 8192;

    #[test]
    fn single_producer_drains_in_order() {
        let pool = WorkStealingPool::with_size(2);
        let mbox: Arc<Mailbox<usize>> = Arc::new(Mailbox::with_capacity(TEST_CAPACITY));
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
            mbox.push_and_maybe_submit(i, &pool, move |p| drain(p))
                .expect("test capacity too small");
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
        for (i, val) in v.iter().enumerate().take(n) {
            assert_eq!(*val, i, "out-of-order at {}", i);
        }
    }

    #[test]
    fn concurrent_pushes_never_overlap_drain() {
        let pool = WorkStealingPool::with_size(4);
        let mbox: Arc<Mailbox<usize>> = Arc::new(Mailbox::with_capacity(TEST_CAPACITY));
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
                    mbox.push_and_maybe_submit(i, pool_ref, move |p| drain(p))
                        .expect("test capacity too small");
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

    #[test]
    fn try_push_returns_payload_when_full() {
        let mbox: Arc<Mailbox<usize>> = Arc::new(Mailbox::with_capacity(2));
        assert_eq!(mbox.try_push(1), Ok(()));
        assert_eq!(mbox.try_push(2), Ok(()));
        assert_eq!(mbox.try_push(3), Err(3));
        // Drain one, room is back.
        assert_eq!(mbox.pop(), Some(1));
        assert_eq!(mbox.try_push(3), Ok(()));
    }

    #[test]
    fn force_push_returns_dropped_oldest() {
        let mbox: Arc<Mailbox<usize>> = Arc::new(Mailbox::with_capacity(2));
        assert_eq!(mbox.try_push(1), Ok(()));
        assert_eq!(mbox.try_push(2), Ok(()));
        // Full: force_push pops the oldest (1) and accepts 3.
        assert_eq!(mbox.force_push(3), Some(1));
        assert_eq!(mbox.pop(), Some(2));
        assert_eq!(mbox.pop(), Some(3));
        assert_eq!(mbox.pop(), None);
    }

    #[test]
    fn capacity_and_len_track_pushes() {
        let mbox: Arc<Mailbox<usize>> = Arc::new(Mailbox::with_capacity(4));
        assert_eq!(mbox.capacity(), 4);
        assert_eq!(mbox.len(), 0);
        assert!(mbox.is_empty());
        mbox.try_push(10).unwrap();
        mbox.try_push(20).unwrap();
        assert_eq!(mbox.len(), 2);
        assert!(!mbox.is_empty());
    }
}
