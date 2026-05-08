//! `PartitionSlot`: per-(shard, partition) state + bounded mailbox +
//! dispatcher + producer-block parker + per-Partition counters.
//!
//! The inflight CAS on `Mailbox` guarantees that at most one drain task
//! is active per slot; under that exclusion the typed dispatcher
//! downcasts state and runs `Shard::run`. The mailbox is bounded
//! (`crossbeam::queue::ArrayQueue`); when the channel's declared
//! `OverflowPolicy` is `Block`, producers park on the slot's
//! [`BlockParker`] and the drain loop unparks one waiter per popped
//! item. Per-Partition push / drop / block counters live on
//! [`PartitionCounters`] and are bumped on the producer side.
//!
//! State persistence (`relay-state-persistence-rust`):
//!
//! Each `PartitionSlot` whose Shard's `Change` type is not `NoChange`
//! also carries a type-erased flush buffer (`Arc<Mutex<Box<dyn FlushableBuffer>>>`),
//! a per-instance `persistence_enabled: bool` flag, and a `change_seq:
//! AtomicU64` counter. The per-process [`flusher::PartitionFlusher`]
//! drains all registered buffers on its linger cadence.

pub mod flusher;
pub mod header;
pub mod partition_state_sink;
pub mod restore;

pub use restore::{RestoreError, RestoreInitPayload};

use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, Weak};

use crossbeam::sync::{Parker, Unparker};
use flz_transport::transport::FlezhaStream;

use crate::channel::ShardId;
use crate::executor::mailbox::Mailbox;
use crate::executor::work_stealing_pool::WorkStealingPool;

/// Erased payload moving through the inter-shard mailbox. The `value`
/// is `Arc<dyn Any + Send + Sync>` (the consumer downcasts to its own
/// `S::Input`); the `stream` is the secondary FlezhaStream of the
/// connection that the original wire input arrived on, propagated
/// downstream so any plugin-bound emit lands on the same connection.
pub struct PartitionPayload {
    pub value: Arc<dyn Any + Send + Sync>,
    pub stream: Arc<FlezhaStream>,
}

pub type PartitionMailbox = Arc<Mailbox<PartitionPayload>>;

/// Producer-side observability for one Partition. All counters are
/// monotonic, written by producers under [`std::sync::atomic::Ordering::Relaxed`],
/// and read by future metrics export at its leisure (Decision 5).
#[derive(Debug, Default)]
pub struct PartitionCounters {
    /// Number of payloads successfully enqueued onto this Partition's
    /// mailbox.
    pub pushes_total: AtomicU64,
    /// Number of times a `Block`-policy producer parked waiting for the
    /// consumer to free a slot. Multiple parks per push (under
    /// multi-producer contention) each increment.
    pub pushes_blocked_total: AtomicU64,
    /// Number of incoming payloads discarded under `DropNewest`.
    pub drops_newest_total: AtomicU64,
    /// Number of head-of-queue payloads evicted by `DropOldest`'s
    /// `force_push`.
    pub drops_oldest_total: AtomicU64,
}

/// Multi-waiter, multi-unparker FIFO parker used by `Block`-policy
/// producers when the consumer's mailbox is saturated.
///
/// Implementation: `Mutex<VecDeque<Unparker>>` per Decision 3's note. A
/// producer parks by creating a fresh `Parker`, pushing its `Unparker`
/// to the back of the deque, **dropping the lock**, and then calling
/// `parker.park()`. A consumer pops one waiter from the front under the
/// lock and unparks it. The lock is never held across `park()`, so the
/// deque mutation window is short and uncontended on the consumer side
/// (the inflight CAS guarantees a single drainer per slot).
///
/// FIFO ordering at the API level is best-effort once threads start
/// observing their own wakeups; thread scheduling can reorder which
/// woken thread re-attempts the push first.
pub struct BlockParker {
    waiters: Mutex<VecDeque<Unparker>>,
}

impl BlockParker {
    pub fn new() -> Self {
        Self {
            waiters: Mutex::new(VecDeque::new()),
        }
    }

    /// Park the calling thread on this slot's wait list. The lock is
    /// dropped before [`Parker::park`] so a consumer pop can unpark us.
    pub fn wait_one(&self) {
        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        {
            let mut deque = self.waiters.lock().expect("BlockParker mutex poisoned");
            deque.push_back(unparker);
        }
        parker.park();
    }

    /// Wake the oldest waiter, if any. Called by the consumer's drain
    /// loop after a successful pop.
    pub fn unpark_one(&self) {
        let unparker = {
            let mut deque = self.waiters.lock().expect("BlockParker mutex poisoned");
            deque.pop_front()
        };
        if let Some(u) = unparker {
            u.unpark();
        }
    }

    /// Number of currently parked waiters. Approximate under contention;
    /// exposed for diagnostics only.
    pub fn waiter_count(&self) -> usize {
        self.waiters
            .lock()
            .map(|d| d.len())
            .unwrap_or(0)
    }
}

impl Default for BlockParker {
    fn default() -> Self {
        Self::new()
    }
}

pub struct PartitionSlot {
    pub state: Mutex<Box<dyn Any + Send>>,
    pub mailbox: PartitionMailbox,
    pub dispatcher: Arc<dyn ShardDispatcher>,
    pub shard_id: ShardId,
    pub shard_key: Option<Arc<str>>,
    pub pool: Arc<WorkStealingPool>,
    pub instance: Weak<crate::relay::instance::RelayInstance>,
    pub topology: Arc<crate::relay::topology::RelayTopology>,
    /// FIFO wait list for `Block`-policy producers parked on a full
    /// mailbox. Drained one entry at a time by `drain_partition` after
    /// every successful pop.
    pub block_parker: BlockParker,
    /// Per-Partition flush buffer holding `Vec<S::Change>` behind the
    /// type-erased `FlushableBuffer` trait object. The dispatcher locks
    /// this Mutex for the duration of `Shard::run` and constructs a
    /// `StateHandle` from the borrowed Vec inside; the per-process
    /// flusher locks it on each linger tick to drain.
    pub flush_buffer: Arc<Mutex<Box<dyn flusher::FlushableBuffer>>>,
    /// Frozen-at-instance-start persistence flag. Resolved by
    /// `relay::persistence::resolve_persistence_enabled` from the
    /// executor override + Relay class const at `RelayInstance::new`
    /// time and propagated here.
    pub persistence_enabled: bool,
    /// Per-Partition WAL seq counter. The flusher fetches-and-adds once
    /// per drained record; `send_frame` failures decrement to preserve
    /// monotonicity (no gaps in the controller's WAL view).
    pub change_seq: Arc<std::sync::atomic::AtomicU64>,
    /// Stable per-Partition identity (`(relay_class, shard_id, partition_key)`)
    /// encoded for the wire by `flusher::encode_partition_identity`. The
    /// flusher reads this verbatim into each frame's
    /// `partition_id_bytes` field; the controller decodes the same structure
    /// to route the frame to the right per-Partition WAL.
    pub partition_identity: Arc<Vec<u8>>,
}

impl PartitionSlot {
    /// Convenience accessor for the per-Partition counter set. Future
    /// observability code walks the registry, calls `slot.mailbox().counters()`
    /// (or this shorthand), and reads the four atomics with `Relaxed`
    /// loads — no internal-state poke required.
    pub fn counters(&self) -> &PartitionCounters {
        self.mailbox.counters()
    }
}

pub trait ShardDispatcher: Send + Sync + 'static {
    fn dispatch(
        &self,
        payload: PartitionPayload,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn flusher::FlushableBuffer>,
        instance: &Weak<crate::relay::instance::RelayInstance>,
    );

    fn shard_id(&self) -> ShardId;
}

/// Forward to the section-2 dispatcher implementation.
pub fn build_partition_slot<S: crate::shard::Shard>(
    args: crate::channel::builder::BuildPartitionArgs<'_>,
) -> Arc<PartitionSlot> {
    crate::relay::dispatcher::build_partition_slot::<S>(args)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    #[test]
    fn block_parker_unpark_releases_waiter() {
        let parker = Arc::new(BlockParker::new());
        let p = Arc::clone(&parker);
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s = Arc::clone(&started);
        let t = std::thread::spawn(move || {
            s.store(true, Ordering::Release);
            p.wait_one();
        });
        // Wait for the thread to register itself before unparking, so we
        // exercise the wake-after-park path rather than the no-op
        // unpark-before-park (Parker handles both, but we want the
        // observable path here).
        while !started.load(Ordering::Acquire) {
            std::hint::spin_loop();
        }
        // Brief pause to let the producer reach `parker.park()`.
        std::thread::sleep(Duration::from_millis(10));
        parker.unpark_one();
        let join_start = Instant::now();
        loop {
            if t.is_finished() {
                break;
            }
            if join_start.elapsed() > Duration::from_secs(2) {
                panic!("waiter not unparked");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        t.join().unwrap();
    }

    #[test]
    fn block_parker_unpark_with_no_waiters_is_noop() {
        let parker = BlockParker::new();
        // Should not block, panic, or otherwise misbehave.
        parker.unpark_one();
        parker.unpark_one();
        assert_eq!(parker.waiter_count(), 0);
    }

    #[test]
    fn block_parker_fifo_order_at_unpark_time() {
        // Push three unparkers in order A, B, C; verify pop_front sees
        // A, B, C. Test the deque-level ordering only — once threads
        // observe their wakeups, scheduling can reorder them.
        let parker = BlockParker::new();
        // Use the internal API by parking three threads serialized via
        // sleeps so we can assert the wait_count grows monotonically.
        let p = Arc::new(parker);
        let order = Arc::new(Mutex::new(Vec::<u8>::new()));
        let mut threads = Vec::new();
        for tag in [b'A', b'B', b'C'] {
            let p = Arc::clone(&p);
            let order = Arc::clone(&order);
            threads.push(std::thread::spawn(move || {
                p.wait_one();
                order.lock().unwrap().push(tag);
            }));
            // Sequence the parkers so they enter the deque in order.
            std::thread::sleep(Duration::from_millis(20));
        }
        // All three should be parked now.
        let start = Instant::now();
        while p.waiter_count() < 3 {
            if start.elapsed() > Duration::from_secs(2) {
                panic!("waiters did not all park; count = {}", p.waiter_count());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        for _ in 0..3 {
            p.unpark_one();
            std::thread::sleep(Duration::from_millis(10));
        }
        for t in threads {
            t.join().unwrap();
        }
        let observed = order.lock().unwrap().clone();
        assert_eq!(observed, vec![b'A', b'B', b'C']);
    }
}
