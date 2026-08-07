//! Per-Entity timers: the pending set, its durable ops, and the
//! processing-time wheel.
//!
//! A timer is state, not a scheduler entry. Registration, cancellation and
//! firing each append a [`TimerOp`] to the Entity's durable record stream, so
//! the pending set is rebuilt by the ordinary change-log fold on restore — no
//! side-file, no crash marker, no completeness oracle. Ops ride the WAL two
//! ways, both of which the controller already stores verbatim:
//!
//!   * outside the effectively-once tier, as a `format_id = 3` record whose
//!     body is a postcard `Vec<TimerOp>`;
//!   * inside it, as `EffectRecordBody::timers`, so a firing's effects and the
//!     removal of the timer that produced them land in ONE record.
//!
//! Event-time timers need nothing here: they fire off the Entity's event-time
//! high-water, on the dispatch path. Only the processing-time kind needs a
//! clock of its own — one [`TimerWheel`] per Operative, whose expiry pushes a
//! [`TimerFire`] onto the target Entity's existing mailbox so `on_timer` is
//! serialized against `run` by the single-active-drainer gate already there.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use dashmap::DashMap;
use flz_transport::transport::FlezhaStream;
use serde::{Deserialize, Serialize};

use crate::channel::{OverflowPolicy, ProcedureId};
use crate::entity::EntitySlot;
use crate::util::LockRecover;

/// Which clock a timer is scheduled against. Serialized as a postcard unit
/// enum (`0` / `1`) inside every [`TimerOp`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TimerKind {
    /// Fires when the Entity's event-time high-water reaches the scheduled
    /// timestamp. A quiet Entity never fires one — nothing advances its clock.
    EventTime,
    /// Fires when wall-clock time reaches the scheduled timestamp, via the
    /// Operative's [`TimerWheel`].
    ProcessingTime,
}

impl TimerKind {
    pub fn as_str(self) -> &'static str {
        match self {
            TimerKind::EventTime => "event_time",
            TimerKind::ProcessingTime => "processing_time",
        }
    }
}

/// One durable mutation of an Entity's pending-timer set. Field order is the
/// wire contract — postcard is field-order sensitive and the controller's
/// `Flezha.State.Wire` decodes the same layout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimerOp {
    pub kind: TimerKind,
    pub scheduled_at: u64,
    /// `true` adds the timer to the pending set, `false` removes it. A firing
    /// records a removal, so a crash after the firing cannot re-fire it.
    pub register: bool,
}

impl TimerOp {
    pub fn register(kind: TimerKind, scheduled_at: u64) -> Self {
        Self {
            kind,
            scheduled_at,
            register: true,
        }
    }

    pub fn cancel(kind: TimerKind, scheduled_at: u64) -> Self {
        Self {
            kind,
            scheduled_at,
            register: false,
        }
    }
}

/// An Entity's pending timers: one ordered set per kind. Pending timers are a
/// **set**, so registering the same kind and timestamp twice is idempotent.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingTimers {
    event_time: BTreeSet<u64>,
    processing_time: BTreeSet<u64>,
}

impl PendingTimers {
    fn set_mut(&mut self, kind: TimerKind) -> &mut BTreeSet<u64> {
        match kind {
            TimerKind::EventTime => &mut self.event_time,
            TimerKind::ProcessingTime => &mut self.processing_time,
        }
    }

    fn set(&self, kind: TimerKind) -> &BTreeSet<u64> {
        match kind {
            TimerKind::EventTime => &self.event_time,
            TimerKind::ProcessingTime => &self.processing_time,
        }
    }

    /// Fold one op. The single mutation seam: registration, cancellation,
    /// firing and the restore replay all go through it, so the live set and a
    /// rebuilt one cannot drift. Returns whether the set actually changed.
    pub fn apply(&mut self, op: &TimerOp) -> bool {
        if op.register {
            self.set_mut(op.kind).insert(op.scheduled_at)
        } else {
            self.set_mut(op.kind).remove(&op.scheduled_at)
        }
    }

    pub fn contains(&self, kind: TimerKind, scheduled_at: u64) -> bool {
        self.set(kind).contains(&scheduled_at)
    }

    pub fn len(&self) -> usize {
        self.event_time.len() + self.processing_time.len()
    }

    pub fn is_empty(&self) -> bool {
        self.event_time.is_empty() && self.processing_time.is_empty()
    }

    /// Scheduled timestamps at or below `upto`, ascending — what a high-water
    /// crossing (event time) or a wheel tick (processing time) must fire.
    pub fn due(&self, kind: TimerKind, upto: u64) -> Vec<u64> {
        self.set(kind).range(..=upto).copied().collect()
    }

    /// Every pending timer of one kind, ascending.
    pub fn all(&self, kind: TimerKind) -> Vec<u64> {
        self.set(kind).iter().copied().collect()
    }

    /// One kind expressed as registrations — what the restore re-arm hands the
    /// wheel, and half of what a compaction re-states.
    pub fn register_ops(&self, kind: TimerKind) -> Vec<TimerOp> {
        self.all(kind)
            .into_iter()
            .map(|at| TimerOp::register(kind, at))
            .collect()
    }

    /// The full set expressed as registrations. A compaction re-states the
    /// pending timers this way at a seq ABOVE the snapshot's, so truncating
    /// the ops the snapshot covers cannot lose them.
    pub fn as_register_ops(&self) -> Vec<TimerOp> {
        let mut ops = self.register_ops(TimerKind::EventTime);
        ops.extend(self.register_ops(TimerKind::ProcessingTime));
        ops
    }
}

/// A processing-time timer the wheel has released onto an Entity's mailbox.
/// Carries the secondary stream captured when the timer was armed so
/// `on_timer`'s plugin emits leave on the same connection an input's would.
pub struct TimerFire {
    pub scheduled_at: u64,
    pub stream: Arc<FlezhaStream>,
}

// ----------------------------------------------------------------- session

/// What a Procedure declares about its timers, resolved once per Procedure
/// rather than at every invocation: the bound, the names a refusal reports, and
/// whether its Clock can serve an event-time deadline at all. Carries the
/// counter set too, so a session costs no registry lookup.
///
/// [`ProcedureCtx`]: crate::procedure::ProcedureCtx
#[derive(Clone)]
pub struct TimerPolicy {
    max_per_entity: usize,
    procedure: &'static str,
    event_time_clock: bool,
    clock: &'static str,
    counters: Arc<TimerCounters>,
}

impl TimerPolicy {
    pub fn for_procedure<S: crate::procedure::Procedure>() -> Self {
        Self {
            max_per_entity: S::max_timers_per_entity(),
            procedure: S::name(),
            event_time_clock: <S::Clock as crate::procedure::Clock>::IS_EVENT_TIME,
            clock: <S::Clock as crate::procedure::Clock>::type_name(),
            counters: counters_for(S::name()),
        }
    }

    /// Open a session over a COPY of `pending`, optionally seeded with the
    /// removal of the timer that is firing.
    pub fn session(
        &self,
        pending: &PendingTimers,
        now: u64,
        firing: Option<(TimerKind, u64)>,
    ) -> RefCell<TimerSession> {
        let mut session = TimerSession {
            pending: pending.clone(),
            ops: Vec::new(),
            policy: self.clone(),
            now,
            firing: false,
        };
        if let Some((kind, scheduled_at)) = firing {
            session.open_firing(kind, scheduled_at);
        }
        RefCell::new(session)
    }
}

/// The timer surface for ONE invocation, borrowed by [`ProcedureCtx`].
///
/// It works on a **copy** of the Entity's pending set: the retry ladder re-runs
/// a failed attempt, and a registration that attempt made must not survive it.
/// The dispatcher calls [`Self::commit`] only on the attempt that succeeded —
/// which is also where the counters move, so a refused-then-retried invocation
/// is counted once.
///
/// [`ProcedureCtx`]: crate::procedure::ProcedureCtx
pub struct TimerSession {
    pending: PendingTimers,
    ops: Vec<TimerOp>,
    policy: TimerPolicy,
    /// The invocation's `ctx.now()`. [`Self::register_after`] is anchored to
    /// it, which is what makes a replayed invocation register the same
    /// absolute timestamp the live one did.
    now: u64,
    /// Set when the session opens on a firing: op 0 is the fired timer's
    /// removal, so [`Self::commit`] counts it as a firing rather than an author
    /// cancel.
    firing: bool,
}

impl TimerSession {
    /// Seed the session with the removal of the timer that is firing, so it
    /// rides the SAME durable record as whatever `on_timer` goes on to apply
    /// and emit. A crash between the two is therefore impossible: either both
    /// landed, or the timer is still pending and the whole firing repeats.
    fn open_firing(&mut self, kind: TimerKind, scheduled_at: u64) {
        let op = TimerOp::cancel(kind, scheduled_at);
        self.pending.apply(&op);
        self.ops.push(op);
        self.firing = true;
    }

    pub fn now(&self) -> u64 {
        self.now
    }

    /// Register an event-time timer at an absolute timestamp.
    pub fn register_at_event_time(
        &mut self,
        scheduled_at: u64,
    ) -> Result<u64, crate::procedure::ProcedureError> {
        if !self.policy.event_time_clock {
            self.refused();
            return Err(
                crate::procedure::ProcedureError::EventTimeTimerWithoutEventTimeClock {
                    procedure: self.policy.procedure.to_string(),
                    clock: self.policy.clock.to_string(),
                },
            );
        }
        self.register(TimerKind::EventTime, scheduled_at)
    }

    /// Register a processing-time timer `delay` after `ctx.now()`.
    pub fn register_after(
        &mut self,
        delay: Duration,
    ) -> Result<u64, crate::procedure::ProcedureError> {
        let at = self.now.saturating_add(delay.as_millis() as u64);
        self.register(TimerKind::ProcessingTime, at)
    }

    fn register(
        &mut self,
        kind: TimerKind,
        scheduled_at: u64,
    ) -> Result<u64, crate::procedure::ProcedureError> {
        if !self.pending.contains(kind, scheduled_at)
            && self.pending.len() >= self.policy.max_per_entity
        {
            self.refused();
            return Err(crate::procedure::ProcedureError::TimerCapExceeded {
                procedure: self.policy.procedure.to_string(),
                max: self.policy.max_per_entity,
            });
        }
        let op = TimerOp::register(kind, scheduled_at);
        if self.pending.apply(&op) {
            self.ops.push(op);
        }
        Ok(scheduled_at)
    }

    /// Cancel a pending timer by its scheduled timestamp. `false` when none
    /// was pending — a cancellation nobody registered changes nothing.
    pub fn cancel(&mut self, kind: TimerKind, scheduled_at: u64) -> bool {
        let op = TimerOp::cancel(kind, scheduled_at);
        let changed = self.pending.apply(&op);
        if changed {
            self.ops.push(op);
        }
        changed
    }

    fn refused(&self) {
        self.policy
            .counters
            .refused_total
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Publish the session onto the Entity's live pending set and return the
    /// ops to make durable (and to arm/disarm on the wheel). Called by the
    /// dispatcher only for the attempt that completed.
    pub fn commit(self, live: &mut PendingTimers) -> Vec<TimerOp> {
        let counters = &self.policy.counters;
        for (idx, op) in self.ops.iter().enumerate() {
            if op.register {
                counters.registered_total.fetch_add(1, Ordering::Relaxed);
                counters.pending.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            counters.pending.fetch_sub(1, Ordering::Relaxed);
            if self.firing && idx == 0 {
                counters.fired_total.fetch_add(1, Ordering::Relaxed);
                TIMERS_FIRED_TOTAL.fetch_add(1, Ordering::Relaxed);
            } else {
                counters.cancelled_total.fetch_add(1, Ordering::Relaxed);
            }
        }
        *live = self.pending;
        self.ops
    }
}

// ---------------------------------------------------------------- counters

/// Per-Procedure timer counters. Monotonic totals plus a signed `pending`
/// gauge, all `Relaxed` — read by observability at its leisure, exactly like
/// [`crate::entity::EntityCounters`].
#[derive(Debug, Default)]
pub struct TimerCounters {
    pub pending: AtomicI64,
    pub registered_total: AtomicU64,
    pub fired_total: AtomicU64,
    pub cancelled_total: AtomicU64,
    pub refused_total: AtomicU64,
}

/// Immutable view of one Procedure's counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimerCountersView {
    pub procedure: &'static str,
    pub pending: i64,
    pub registered_total: u64,
    pub fired_total: u64,
    pub cancelled_total: u64,
    pub refused_total: u64,
}

static PER_PROCEDURE: std::sync::OnceLock<DashMap<&'static str, Arc<TimerCounters>>> =
    std::sync::OnceLock::new();

fn registry() -> &'static DashMap<&'static str, Arc<TimerCounters>> {
    PER_PROCEDURE.get_or_init(DashMap::new)
}

/// The counter set for one Procedure, created on first use.
pub fn counters_for(procedure: &'static str) -> Arc<TimerCounters> {
    if let Some(existing) = registry().get(procedure) {
        return Arc::clone(existing.value());
    }
    Arc::clone(
        registry()
            .entry(procedure)
            .or_insert_with(|| Arc::new(TimerCounters::default()))
            .value(),
    )
}

/// Every Procedure's counters, for `flz`-side observability and tests.
pub fn counters_snapshot() -> Vec<TimerCountersView> {
    let mut out: Vec<TimerCountersView> = registry()
        .iter()
        .map(|e| {
            let c = e.value();
            TimerCountersView {
                procedure: e.key(),
                pending: c.pending.load(Ordering::Relaxed),
                registered_total: c.registered_total.load(Ordering::Relaxed),
                fired_total: c.fired_total.load(Ordering::Relaxed),
                cancelled_total: c.cancelled_total.load(Ordering::Relaxed),
                refused_total: c.refused_total.load(Ordering::Relaxed),
            }
        })
        .collect();
    out.sort_by_key(|v| v.procedure);
    out
}

/// Process-global count of timers that have fired. Shipped in every
/// `data_heartbeat` frame beside `dispatches` / `blocked_parks`.
pub static TIMERS_FIRED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Process-global count of processing-time timers currently armed across every
/// Operative's wheel — the occupancy a runaway registration shows up in before
/// it is fatal.
pub static TIMERS_ARMED: AtomicI64 = AtomicI64::new(0);

pub fn timers_fired_total() -> u64 {
    TIMERS_FIRED_TOTAL.load(Ordering::Relaxed)
}

pub fn timers_armed() -> i64 {
    TIMERS_ARMED.load(Ordering::Relaxed)
}

// ------------------------------------------------------------------- wheel

/// Wheel key: fire order is by deadline, ties broken by Entity identity so the
/// order is total (and therefore reproducible).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ArmedAt {
    at: u64,
    procedure: ProcedureId,
    entity_key: String,
}

struct Armed {
    /// Weak so an idle-evicted Entity's timers die with it: the upgrade fails
    /// and the firing is dropped, which is the D6 contract.
    slot: Weak<EntitySlot>,
    stream: Arc<FlezhaStream>,
}

#[derive(Default)]
struct WheelInner {
    armed: BTreeMap<ArmedAt, Armed>,
    stop: bool,
}

/// One processing-time timer wheel per Operative. Expiry pushes onto the
/// target Entity's mailbox rather than invoking `on_timer` itself — that is
/// what makes firing obey the same FIFO ordering, the same back-pressure and
/// the same single-drainer exclusion an input already does, with no second
/// concurrency mechanism.
pub struct TimerWheel {
    inner: Mutex<WheelInner>,
    signal: Condvar,
}

/// Longest the wheel thread sleeps with nothing armed. Bounded so a stop
/// request is honoured promptly even without a signal.
const WHEEL_IDLE_TICK: Duration = Duration::from_millis(200);

impl TimerWheel {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(WheelInner::default()),
            signal: Condvar::new(),
        })
    }

    /// Arm one processing-time timer for an Entity. Re-arming the same
    /// `(procedure, key, scheduled_at)` replaces the entry — pending timers
    /// are a set, so this is the idempotence the surface promises.
    pub fn arm(
        &self,
        procedure: ProcedureId,
        entity_key: &str,
        scheduled_at: u64,
        slot: &Arc<EntitySlot>,
        stream: Arc<FlezhaStream>,
    ) {
        let key = ArmedAt {
            at: scheduled_at,
            procedure,
            entity_key: entity_key.to_string(),
        };
        let mut inner = self.inner.lock_recover();
        let replaced = inner.armed.insert(
            key,
            Armed {
                slot: Arc::downgrade(slot),
                stream,
            },
        );
        if replaced.is_none() {
            TIMERS_ARMED.fetch_add(1, Ordering::Relaxed);
        }
        drop(inner);
        self.signal.notify_all();
    }

    /// Disarm one timer. Idempotent — a firing that already popped the entry
    /// makes this a no-op.
    pub fn disarm(&self, procedure: ProcedureId, entity_key: &str, scheduled_at: u64) {
        let key = ArmedAt {
            at: scheduled_at,
            procedure,
            entity_key: entity_key.to_string(),
        };
        let mut inner = self.inner.lock_recover();
        if inner.armed.remove(&key).is_some() {
            TIMERS_ARMED.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Drop every timer armed for one Entity. Called when the dynamic-mode
    /// reaper evicts it, so the wheel releases the memory at eviction time
    /// rather than at each timer's deadline.
    pub fn discard_entity(&self, procedure: ProcedureId, entity_key: &str) {
        let mut inner = self.inner.lock_recover();
        let doomed: Vec<ArmedAt> = inner
            .armed
            .keys()
            .filter(|k| k.procedure == procedure && k.entity_key == entity_key)
            .cloned()
            .collect();
        for key in doomed {
            inner.armed.remove(&key);
            TIMERS_ARMED.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Number of timers currently armed on this wheel.
    pub fn occupancy(&self) -> usize {
        self.inner.lock_recover().armed.len()
    }

    /// Stop the wheel thread. Idempotent.
    pub fn stop(&self) {
        self.inner.lock_recover().stop = true;
        self.signal.notify_all();
    }

    /// Pop every entry due at `now`, ascending. Returns the fires to deliver
    /// and how long the caller should wait before asking again.
    fn take_due(&self, now: u64) -> (Vec<(Arc<EntitySlot>, TimerFire)>, Option<Duration>) {
        let mut inner = self.inner.lock_recover();
        let mut fires = Vec::new();
        while let Some((key, _)) = inner.armed.iter().next() {
            if key.at > now {
                let wait = Duration::from_millis(key.at - now);
                return (fires, Some(wait.min(WHEEL_IDLE_TICK)));
            }
            let key = key.clone();
            let armed = inner.armed.remove(&key).expect("entry observed above");
            TIMERS_ARMED.fetch_sub(1, Ordering::Relaxed);
            if let Some(slot) = armed.slot.upgrade() {
                fires.push((
                    slot,
                    TimerFire {
                        scheduled_at: key.at,
                        stream: armed.stream,
                    },
                ));
            }
        }
        (fires, None)
    }

    fn run_loop(self: Arc<Self>) {
        loop {
            {
                let inner = self.inner.lock_recover();
                if inner.stop {
                    return;
                }
            }
            let now = crate::procedure::clock::wall_now_ms();
            let (fires, wait) = self.take_due(now);
            for (slot, fire) in fires {
                // Block policy: a saturated mailbox delays the firing (and
                // drains the slot inline) rather than dropping it — a timer
                // silently lost is a bug the author cannot see.
                crate::operative::dispatcher::enqueue(
                    &slot,
                    crate::entity::EntityItem::Timer(fire),
                    OverflowPolicy::Block,
                );
            }
            let mut inner = self.inner.lock_recover();
            if inner.stop {
                return;
            }
            let wait = wait.unwrap_or(WHEEL_IDLE_TICK);
            let (guard, _) = self
                .signal
                .wait_timeout(inner, wait)
                .unwrap_or_else(|p| p.into_inner());
            inner = guard;
            if inner.stop {
                return;
            }
        }
    }
}

/// Spawn the wheel's thread. One per Operative instance, created lazily on the
/// first processing-time registration so an Operative that never uses timers
/// never pays for a thread.
pub fn spawn_wheel(wheel: Arc<TimerWheel>) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("flezha-timer-wheel".to_string())
        .spawn(move || wheel.run_loop())
        .expect("spawning flezha-timer-wheel thread should not fail")
}

// ------------------------------------------------ arming from committed ops

/// Reflect an invocation's committed timer ops onto the Operative's wheel.
/// Event-time timers need nothing here — the Entity's high-water fires them —
/// so a Procedure that only uses those never causes a wheel thread to exist.
pub(crate) fn apply_ops(slot: &Arc<EntitySlot>, ops: &[TimerOp], stream: &Arc<FlezhaStream>) {
    if !ops.iter().any(|op| op.kind == TimerKind::ProcessingTime) {
        return;
    }
    let Some(instance) = slot.instance.upgrade() else {
        return;
    };
    let wheel = instance.timer_wheel();
    let key = slot.entity_key.as_deref().unwrap_or("");
    for op in ops.iter().filter(|op| op.kind == TimerKind::ProcessingTime) {
        if op.register {
            wheel.arm(
                slot.procedure_id,
                key,
                op.scheduled_at,
                slot,
                Arc::clone(stream),
            );
        } else {
            wheel.disarm(slot.procedure_id, key, op.scheduled_at);
        }
    }
}

/// Re-arm a restored Entity's processing-time timers. The pending set came back
/// through the change-log fold; anything already past its deadline fires on the
/// wheel's next pass, in ascending order. Event-time timers need nothing — the
/// restored high-water is their trigger.
pub fn rearm_restored(slot: &Arc<EntitySlot>) {
    let ops = slot
        .timers
        .lock_recover()
        .register_ops(TimerKind::ProcessingTime);
    if ops.is_empty() {
        return;
    }
    // A restore has no originating input to inherit a connection from; the live
    // secondary stream is the same one the flusher ships on.
    let Some(stream) = crate::entity::state_sink::secondary_stream() else {
        tracing::debug!(
            target: "flezha::timer",
            entity = slot.dispatcher.entity_label(),
            pending = ops.len(),
            "restored processing-time timers not re-armed: no secondary stream installed"
        );
        return;
    };
    apply_ops(slot, &ops, &stream);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registration_is_idempotent_and_cancellation_removes() {
        let mut pending = PendingTimers::default();
        assert!(pending.apply(&TimerOp::register(TimerKind::ProcessingTime, 31_000)));
        assert!(!pending.apply(&TimerOp::register(TimerKind::ProcessingTime, 31_000)));
        assert_eq!(pending.len(), 1);
        assert!(pending.apply(&TimerOp::cancel(TimerKind::ProcessingTime, 31_000)));
        assert!(pending.is_empty());
    }

    #[test]
    fn the_two_kinds_are_separate_sets() {
        let mut pending = PendingTimers::default();
        pending.apply(&TimerOp::register(TimerKind::EventTime, 100));
        pending.apply(&TimerOp::register(TimerKind::ProcessingTime, 100));
        assert_eq!(pending.len(), 2);
        pending.apply(&TimerOp::cancel(TimerKind::EventTime, 100));
        assert!(pending.contains(TimerKind::ProcessingTime, 100));
        assert!(!pending.contains(TimerKind::EventTime, 100));
    }

    #[test]
    fn due_is_ascending_and_inclusive_of_the_bound() {
        let mut pending = PendingTimers::default();
        for at in [300u64, 100, 200] {
            pending.apply(&TimerOp::register(TimerKind::EventTime, at));
        }
        assert_eq!(pending.due(TimerKind::EventTime, 200), vec![100, 200]);
        assert_eq!(pending.due(TimerKind::EventTime, 99), Vec::<u64>::new());
    }

    /// The compaction re-statement must reproduce the set exactly: it is the
    /// only record of the timers once the ops below the snapshot are truncated.
    #[test]
    fn register_ops_round_trip_the_whole_set() {
        let mut pending = PendingTimers::default();
        pending.apply(&TimerOp::register(TimerKind::EventTime, 5));
        pending.apply(&TimerOp::register(TimerKind::ProcessingTime, 9));
        pending.apply(&TimerOp::register(TimerKind::ProcessingTime, 7));

        let mut rebuilt = PendingTimers::default();
        for op in pending.as_register_ops() {
            rebuilt.apply(&op);
        }
        assert_eq!(rebuilt, pending);
    }

    /// The wheel's own contract, without a dispatcher: entries fire in
    /// deadline order, a disarm removes exactly one, and discarding an Entity
    /// takes every entry it owns.
    #[test]
    fn the_wheel_arms_disarms_and_discards_by_entity() {
        let armed_before = timers_armed();
        let wheel = TimerWheel::new();
        assert_eq!(wheel.occupancy(), 0);

        // The wheel holds a Weak to the slot, so this test exercises the
        // bookkeeping — the delivery half is covered end-to-end by
        // `tests/entity_timers.rs`, which owns real slots.
        let key = ArmedAt {
            at: 10,
            procedure: 0,
            entity_key: "a".to_string(),
        };
        let later = ArmedAt {
            at: 20,
            procedure: 0,
            entity_key: "a".to_string(),
        };
        let other = ArmedAt {
            at: 15,
            procedure: 0,
            entity_key: "b".to_string(),
        };
        let (sock, _peer) = std::os::unix::net::UnixStream::pair().expect("UnixStream::pair");
        let stream = Arc::new(FlezhaStream::Unix(sock));
        for k in [&key, &later, &other] {
            wheel.inner.lock_recover().armed.insert(
                k.clone(),
                Armed {
                    slot: Weak::new(),
                    stream: Arc::clone(&stream),
                },
            );
            TIMERS_ARMED.fetch_add(1, Ordering::Relaxed);
        }
        assert_eq!(wheel.occupancy(), 3);

        wheel.disarm(0, "a", 20);
        assert_eq!(wheel.occupancy(), 2, "a disarm removes exactly one entry");

        wheel.discard_entity(0, "a");
        assert_eq!(
            wheel.occupancy(),
            1,
            "discarding an Entity takes every entry it owns and no other's"
        );

        wheel.discard_entity(0, "b");
        assert_eq!(wheel.occupancy(), 0);
        assert_eq!(
            timers_armed(),
            armed_before,
            "the process-wide gauge nets out"
        );
    }

    /// The counters an operator reads: a registration, a cancellation, a firing
    /// and a refusal each move exactly one of them.
    #[test]
    fn the_per_procedure_counters_follow_the_committed_ops() {
        let policy = TimerPolicy {
            max_per_entity: 1,
            procedure: "counter_probe_procedure",
            event_time_clock: false,
            clock: "WallClock",
            counters: counters_for("counter_probe_procedure"),
        };
        let mut live = PendingTimers::default();

        let session = policy.session(&live, 1_000, None);
        session
            .borrow_mut()
            .register_after(Duration::from_secs(30))
            .expect("first registration fits the cap");
        session
            .borrow_mut()
            .register_after(Duration::from_secs(60))
            .expect_err("the second exceeds it");
        session.into_inner().commit(&mut live);

        let session = policy.session(&live, 1_000, None);
        assert!(session
            .borrow_mut()
            .cancel(TimerKind::ProcessingTime, 31_000));
        session.into_inner().commit(&mut live);

        let session = policy.session(&live, 9_000, Some((TimerKind::ProcessingTime, 9_000)));
        session.into_inner().commit(&mut live);

        let view = counters_snapshot()
            .into_iter()
            .find(|v| v.procedure == "counter_probe_procedure")
            .expect("the Procedure appears in the registry on first use");
        assert_eq!(view.registered_total, 1);
        assert_eq!(view.cancelled_total, 1);
        assert_eq!(view.refused_total, 1);
        assert_eq!(view.fired_total, 1, "a firing is not counted as a cancel");
        assert_eq!(
            view.pending, -1,
            "the firing removed a timer this probe never registered"
        );
    }

    /// Pins the postcard bytes the controller's `Flezha.State.Wire`
    /// decodes — the two sides share this fixture, so an encoding change
    /// breaks loudly here rather than silently on the wire.
    #[test]
    fn timer_op_postcard_bytes_are_the_cross_language_fixture() {
        let ops = vec![
            TimerOp::register(TimerKind::ProcessingTime, 31_000),
            TimerOp::cancel(TimerKind::EventTime, 100),
        ];
        assert_eq!(
            postcard::to_allocvec(&ops).unwrap(),
            vec![
                0x02, // 2 ops
                0x01, 0x98, 0xF2, 0x01, 0x01, // ProcessingTime, 31000, register
                0x00, 0x64, 0x00, // EventTime, 100, cancel
            ]
        );
    }
}
