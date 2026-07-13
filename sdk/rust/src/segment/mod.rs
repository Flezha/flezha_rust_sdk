//! `SegmentSlot`: per-(operation, segment) state + bounded mailbox +
//! dispatcher + producer-block parker + per-Segment counters.
//!
//! The inflight CAS on `Mailbox` guarantees that at most one drain task
//! is active per slot; under that exclusion the typed dispatcher
//! downcasts state and runs `Operation::run`. The mailbox is bounded
//! (`crossbeam::queue::ArrayQueue`); when the channel's declared
//! `OverflowPolicy` is `Block`, producers park on the slot's
//! [`BlockParker`] and the drain loop unparks one waiter per popped
//! item. Per-Segment push / drop / block counters live on
//! [`SegmentCounters`] and are bumped on the producer side.
//!
//! State persistence (`operative-state-persistence-rust`):
//!
//! Each `SegmentSlot` whose Operation's `Change` type is not `NoChange`
//! also carries a type-erased flush buffer (`Arc<Mutex<Box<dyn FlushableBuffer>>>`),
//! a per-instance `persistence_enabled: bool` flag, and a `change_seq:
//! AtomicU64` counter. The per-process [`flusher::SegmentFlusher`]
//! drains all registered buffers on its linger cadence.

pub mod correction;
pub mod credit;
pub mod dynamic;
pub mod flusher;
pub mod header;
pub mod park;
pub mod restore;
pub mod retry;
pub mod segment_state_sink;

use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use crossbeam::sync::{Parker, Unparker};
use flz_transport::transport::FlezhaStream;
pub use restore::{RestoreError, RestoreInitPayload};

use crate::channel::OperationId;
use crate::executor::mailbox::Mailbox;
use crate::executor::work_stealing_pool::WorkStealingPool;

/// Erased payload moving through the inter-operation mailbox. The `value`
/// is `Arc<dyn Any + Send + Sync>` (the consumer downcasts to its own
/// `S::Input`); the `stream` is the secondary FlezhaStream of the
/// connection that the original wire input arrived on, propagated
/// downstream so any plugin-bound emit lands on the same connection.
pub struct SegmentPayload {
    pub value: Arc<dyn Any + Send + Sync>,
    pub stream: Arc<FlezhaStream>,
    /// Durable input-log offset this payload derives from (`flz.offset` on
    /// the inbound frame, inherited across inter-operation emits). `None`
    /// outside the effectively-once tier; the invocation's atomic effect
    /// record carries it as the consumed-offset.
    pub consumed_offset: Option<u64>,
    /// Set when the controller re-forwarded this payload as part of a replayed
    /// slice (`flz.replay` on the inbound frame). The drain holds such payloads
    /// until the slice closes, orders them by event time, and only then
    /// dispatches — an arrival-ordered replay would recompute the same wrong
    /// answer the correction exists to repair.
    ///
    /// Never inherited by an inter-operation emit: a downstream Operation
    /// receives the corrected outputs already in order, and a slice it could
    /// never see the end of would hold its drain forever.
    pub replay: bool,
}

/// Construct a [`SegmentPayload`], cloning both shared handles. Shared by
/// the inter-operation emit path and the inbound wire-dispatch path.
pub fn make_segment_payload(
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
) -> SegmentPayload {
    SegmentPayload {
        value: Arc::clone(value),
        stream: Arc::clone(stream),
        consumed_offset,
        replay,
    }
}

pub type SegmentMailbox = Arc<Mailbox<SegmentPayload>>;

/// Producer-side observability for one Segment. All counters are
/// monotonic, written by producers under [`std::sync::atomic::Ordering::Relaxed`],
/// and read by future metrics export at its leisure (Decision 5).
#[derive(Debug, Default)]
pub struct SegmentCounters {
    /// Number of payloads successfully enqueued onto this Segment's
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
/// to the back of the deque, **dropping the lock**, **re-checking that
/// the mailbox is still full**, and only then calling `parker.park()`.
/// A consumer pops one waiter from the front under the lock and unparks
/// it. The lock is never held across `park()`, so the deque mutation
/// window is short and uncontended on the consumer side (the inflight
/// CAS guarantees a single drainer per slot).
///
/// The post-register recheck closes the window where drain could pop
/// every item and exit between the producer's failed `try_push` and the
/// deque push. Without it, a producer that loses that race parks into a
/// deque that no future drain will read, and only the next external
/// push to the same segment would rescue it — which never happens in
/// the steady-state burst case where every producer is mid-`wait_one`.
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

    /// Register on this slot's wait list and park the calling thread.
    /// The lock is dropped before [`Parker::park`] so a consumer pop can
    /// unpark us.
    ///
    /// After registering, the producer **rechecks** `mailbox.is_full()`.
    /// If a drain freed a slot (or fully drained and exited) between the
    /// caller's failed `try_push` and our deque push, the recheck observes
    /// `!is_full()` and we return immediately without parking; the outer
    /// `enqueue` loop will retry `try_push`. Without this recheck, a
    /// producer that loses that race parks forever — see the missed-wake
    /// note on [`BlockParker`].
    pub fn wait_one<P: Send + 'static>(&self, mailbox: &Mailbox<P>) {
        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        {
            let mut deque = self.waiters.lock().expect("BlockParker mutex poisoned");
            deque.push_back(unparker);
        }
        if !mailbox.is_full() {
            return;
        }
        parker.park();
    }

    /// [`wait_one`] with a bounded park. Used by `Block`-policy producers
    /// so that even an unforeseen lost-wake degrades to a retry loop with
    /// warnings instead of a permanent silent freeze.
    pub fn wait_one_timeout<P: Send + 'static>(&self, mailbox: &Mailbox<P>, timeout: Duration) {
        let parker = Parker::new();
        let unparker = parker.unparker().clone();
        {
            let mut deque = self.waiters.lock().expect("BlockParker mutex poisoned");
            deque.push_back(unparker);
        }
        if !mailbox.is_full() {
            return;
        }
        parker.park_timeout(timeout);
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
        self.waiters.lock().map(|d| d.len()).unwrap_or(0)
    }
}

impl Default for BlockParker {
    fn default() -> Self {
        Self::new()
    }
}

/// Process-global count of payloads dispatched through `drain_segment`.
/// Shipped in every `data_heartbeat` frame's metadata so the controller can
/// tell "executor alive but dispatch wedged" apart from "executor healthy" —
/// the 2026-06-10 freeze was invisible to every heartbeat-level signal.
pub static DISPATCHES_COMPLETED: AtomicU64 = AtomicU64::new(0);

/// Process-global count of Block-policy park episodes (producer found a full
/// mailbox with an active drainer elsewhere). Also shipped in heartbeat
/// metadata; a climbing value flags sustained backpressure.
pub static BLOCKED_PARK_EPISODES: AtomicU64 = AtomicU64::new(0);

pub fn dispatches_completed() -> u64 {
    DISPATCHES_COMPLETED.load(Ordering::Relaxed)
}

pub fn blocked_park_episodes() -> u64 {
    BLOCKED_PARK_EPISODES.load(Ordering::Relaxed)
}

pub struct SegmentSlot {
    pub state: Mutex<Box<dyn Any + Send>>,
    pub mailbox: SegmentMailbox,
    pub dispatcher: Arc<dyn OperationDispatcher>,
    pub operation_id: OperationId,
    pub operation_key: Option<Arc<str>>,
    pub pool: Arc<WorkStealingPool>,
    pub instance: Weak<crate::operative::instance::UnboundedInstance>,
    pub topology: Arc<crate::operative::topology::OperativeTopology>,
    /// FIFO wait list for `Block`-policy producers parked on a full
    /// mailbox. Drained one entry at a time by `drain_segment` after
    /// every successful pop.
    pub block_parker: BlockParker,
    /// Single-ACTIVE-drainer gate. `mailbox`'s inflight flag means "a drain
    /// is scheduled or running" (it is held by a queued pool task that may
    /// not be running yet); this flag is true only while a thread is inside
    /// `drain_segment`'s pop loop. Block-policy producers use it to drain
    /// a full slot inline instead of parking — a parked pool worker can be
    /// the only thread able to run the slot's queued drain task, which is a
    /// permanent self-deadlock on a one-worker pool.
    pub drain_active: AtomicBool,
    /// Per-Segment flush buffer holding `Vec<S::Change>` behind the
    /// type-erased `FlushableBuffer` trait object. The dispatcher locks
    /// this Mutex for the duration of `Operation::run` and constructs a
    /// `StateHandle` from the borrowed Vec inside; the per-process
    /// flusher locks it on each linger tick to drain.
    pub flush_buffer: Arc<Mutex<Box<dyn flusher::FlushableBuffer>>>,
    /// Frozen-at-instance-start persistence flag. Resolved by
    /// `operative::persistence::resolve_persistence_enabled` from the
    /// executor override + UnboundedOperative class const at `UnboundedInstance::new`
    /// time and propagated here.
    pub persistence_enabled: bool,
    /// Per-Segment WAL seq counter. The flusher fetches-and-adds once
    /// per drained record; the seq is baked into the frame before it is
    /// handed to the sink, so a frame buffered during a reconnect replays
    /// with its original seq — the controller's WAL view stays gap-free.
    pub change_seq: Arc<std::sync::atomic::AtomicU64>,
    /// Effectively-once tier: highest input-log offset this Segment
    /// durably consumed before a restore (max `consumed_offset` across the
    /// restored effect records; `-1` = none). `drain_segment` skips
    /// re-forwarded inputs at or below it so a crash-replay never
    /// double-applies. `None`-offset payloads (every non-tier input) pay
    /// one branch.
    pub consumed_floor: std::sync::atomic::AtomicI64,
    /// Late-input correction: the highest event time this Segment has
    /// dispatched (`-1` = none yet). Only an `EventTimeFrom` clock advances
    /// it. An input behind it is *late*; how far behind decides whether it
    /// is correctable or falls to the beyond-window policy. Rebuilt on
    /// restore from the snapshot header so a resumed Segment does not
    /// mistake its whole history for a flood of late inputs. Shared with the
    /// dispatcher, which is the only place the extractor can run.
    pub event_time_high_water: Arc<AtomicI64>,
    /// Late-input correction: payloads of a replayed slice (`flz.replay`),
    /// held so the drain can order them by event time before dispatching any.
    /// Empty whenever no replay is in flight, which is the overwhelmingly
    /// common case — a normal drain pays one `is_empty` check.
    pub replay_buffer: Mutex<Vec<SegmentPayload>>,
    /// True from the moment a replayed payload is enqueued until the
    /// controller's end-of-slice broadcast closes the slice. The drain flushes
    /// the buffer only once it is false, so a mailbox that momentarily runs dry
    /// mid-slice cannot dispatch half an ordering.
    ///
    /// It is set on the frame-handling thread (in `enqueue`) and cleared by the
    /// broadcast, which arrives on that same thread after every slice frame —
    /// so the close can never overtake a member it was meant to include.
    pub replay_open: AtomicBool,
    /// Number of payloads currently held in `replay_buffer`. A plain counter so
    /// the drain's hot path can ask "is a slice pending?" with one relaxed load
    /// instead of taking the buffer's mutex on every payload.
    pub replay_depth: AtomicU64,
    /// Stable per-Segment identity (`(operative_class, operation_id, segment_key)`)
    /// encoded for the wire by `flusher::encode_segment_identity`. The
    /// flusher reads this verbatim into each frame's
    /// `segment_id_bytes` field; the controller decodes the same structure
    /// to route the frame to the right per-Segment WAL.
    pub segment_identity: Arc<Vec<u8>>,
    /// Monotonic millis-since-process-start of the last successful push
    /// (`dynamic::now_millis`). Read by the dynamic-mode reaper to decide
    /// idle eviction; inert for single- and static-mode Segments.
    pub last_activity_at: AtomicI64,
    /// Flusher registration id, captured when this Segment's flush
    /// buffer was registered (`Some` iff `Change != NoChange`). The
    /// dynamic-mode reaper passes it to `SegmentFlusher::deregister` on
    /// eviction so an evicted Segment's buffer is no longer drained.
    pub flusher_entry_id: Option<crate::segment::flusher::FlusherEntryId>,
}

impl SegmentSlot {
    /// Convenience accessor for the per-Segment counter set. Future
    /// observability code walks the registry, calls `slot.mailbox().counters()`
    /// (or this shorthand), and reads the four atomics with `Relaxed`
    /// loads — no internal-state poke required.
    pub fn counters(&self) -> &SegmentCounters {
        self.mailbox.counters()
    }
}

pub trait OperationDispatcher: Send + Sync + 'static {
    fn dispatch(
        &self,
        payload: SegmentPayload,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn flusher::FlushableBuffer>,
        instance: &Weak<crate::operative::instance::UnboundedInstance>,
    );

    fn operation_id(&self) -> OperationId;

    /// The payload's event time, or `None` when this Operation's clock is not
    /// `EventTimeFrom` — i.e. when there is no event time to order by.
    ///
    /// The type-erased seam that lets `drain_segment`, which sees only an
    /// opaque `SegmentPayload`, sort a replayed slice by a key that only the
    /// user's `TimeExtractor` can produce. Same erasure trick as
    /// `compensate_collecting`.
    fn event_time_of(&self, payload: &SegmentPayload) -> Option<u64>;

    /// Bounded-driver entry: run `Operation::run` once on `input` against the
    /// type-erased segment `state`, collecting inter-operation output and
    /// routing plugin-bound output through `sink` — instead of the unbounded
    /// push-dispatch mailbox path. Used only by the sequential bounded driver
    /// (`crate::bounded`); unbounded dispatch never calls it. `topology`
    /// resolves the producer-edge channel's plugin serializer. Changes are
    /// folded into in-memory state via `StateHandle::apply`; Phase-3 durability
    /// wires the WAL flush here.
    fn run_collecting(
        &self,
        input: Arc<dyn Any + Send + Sync>,
        state: &mut Box<dyn Any + Send>,
        topology: &crate::operative::topology::OperativeTopology,
        sink: &mut dyn crate::bounded::BoundedCollectSink,
    ) -> Result<(), crate::bounded::BoundedError>;

    /// Bounded-saga rollback entry: invoke `Operation::compensate` once for this
    /// segment against its type-erased `state`, handing back the `outputs` the
    /// segment collected during its forward run (downcast to `Arc<S::Output>`)
    /// and routing any compensation emit through `sink`. Used only by the
    /// bounded driver's reverse-order rollback; unbounded dispatch never calls
    /// it. `topology` resolves the producer-edge channel's plugin serializer.
    fn compensate_collecting(
        &self,
        outputs: Vec<Arc<dyn Any + Send + Sync>>,
        state: &mut Box<dyn Any + Send>,
        topology: &crate::operative::topology::OperativeTopology,
        sink: &mut dyn crate::bounded::BoundedCollectSink,
    ) -> Result<(), crate::bounded::BoundedError>;

    /// Warm-restart: rebuild the type-erased segment `state` in place from a
    /// persisted `RestoreInitState` payload (snapshot + WAL change-log),
    /// returning the terminal seq applied. Type-erased entry point so the
    /// control-channel `RestoreInitState` handler — which has no `S` — can
    /// re-seed a live segment after an executor restart. Mirrors the
    /// downcast pattern used by `dispatch`.
    fn restore_state(
        &self,
        state: &mut Box<dyn Any + Send>,
        snapshot_bytes: &[u8],
        change_log_bytes: &[u8],
        operation_key: Option<&str>,
    ) -> Result<u64, crate::segment::restore::RestoreError>;
}

/// Forward to the section-2 dispatcher implementation.
pub fn build_segment_slot<S: crate::operation::Operation>(
    args: crate::channel::builder::BuildSegmentArgs<'_>,
) -> Arc<SegmentSlot> {
    crate::operative::dispatcher::build_segment_slot::<S>(args)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::{Duration, Instant};

    use super::*;

    /// Build a capacity-1 mailbox pre-filled so `is_full()` reports true.
    /// Required because [`BlockParker::wait_one`] rechecks the mailbox
    /// after registering and returns without parking when it isn't full.
    fn saturated_mailbox() -> Arc<Mailbox<u8>> {
        let mbox = Arc::new(Mailbox::<u8>::with_capacity(1));
        mbox.try_push(0).expect("pre-fill");
        mbox
    }

    #[test]
    fn block_parker_unpark_releases_waiter() {
        let parker = Arc::new(BlockParker::new());
        let mbox = saturated_mailbox();
        let p = Arc::clone(&parker);
        let m = Arc::clone(&mbox);
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s = Arc::clone(&started);
        let t = std::thread::spawn(move || {
            s.store(true, Ordering::Release);
            p.wait_one(&m);
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
        let mbox = saturated_mailbox();
        // Use the internal API by parking three threads serialized via
        // sleeps so we can assert the wait_count grows monotonically.
        let p = Arc::new(parker);
        let order = Arc::new(Mutex::new(Vec::<u8>::new()));
        let mut threads = Vec::new();
        for tag in [b'A', b'B', b'C'] {
            let p = Arc::clone(&p);
            let m = Arc::clone(&mbox);
            let order = Arc::clone(&order);
            threads.push(std::thread::spawn(move || {
                p.wait_one(&m);
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

    /// Regression for the missed-wake deadlock: if drain pops every item
    /// and exits between a producer's failed `try_push` and its deque
    /// push inside `wait_one`, the producer must NOT block forever. The
    /// fix re-checks `mailbox.is_full()` after registering and returns
    /// immediately when the mailbox has space.
    #[test]
    fn wait_one_does_not_block_when_mailbox_has_space() {
        let parker = BlockParker::new();
        let mbox = Arc::new(Mailbox::<u8>::with_capacity(2));
        // Mailbox has 2 free slots — wait_one must NOT park.
        let start = Instant::now();
        parker.wait_one(&mbox);
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(50),
            "wait_one parked despite non-full mailbox; elapsed = {:?}",
            elapsed
        );
        // The unparker we registered is still in the deque; harmless,
        // a future drain pops it and unparks a Parker that's already
        // been dropped.
        assert_eq!(parker.waiter_count(), 1);
    }
}
