use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flz_core::util::{fnv1a128, FNV128_OFFSET_BASIS};

use crate::entity::timer::{TimerKind, TimerSession};
use crate::procedure::error::ProcedureError;
use crate::procedure::spawn::{self, IfRunning, SpawnHandle, SpawnOutcome};

/// Reserved metadata namespace: runtime identity stamps (`flz.entity`,
/// `flz.seq`, `flz.key`) live here; user-supplied keys with this prefix
/// are rejected at emit time so stamps can never be spoofed or clobbered.
pub const RESERVED_METADATA_PREFIX: &str = "flz.";

/// Per-invocation context for a Procedure's `run` method. Borrows the runner's
/// per-entity routing tables. Not Send/Sync — lives only across one
/// `Procedure::run` call.
///
/// The dispatcher stamps `output_channel_id` (cached at operative-build time)
/// so emit incurs zero channel-name lookups on the hot path. It also
/// evaluates the Procedure's declared `Clock` once at dispatch entry —
/// [`Self::now`] serves that single value for the whole invocation — and
/// carries the replay-stable inputs [`Self::stable_id`] derives from.
pub struct ProcedureCtx<'a> {
    pub(crate) emitter: &'a mut dyn EmitSink,
    now: u64,
    invocation_seq: u64,
    entity_identity: &'a [u8],
    /// The key this Entity was routed by, `None` in single-entity mode.
    /// Served by [`Self::entity_key`] and used as the default key of a
    /// keyed-channel emit that supplies none.
    entity_key: Option<&'a str>,
    stable_id_ordinal: Cell<u32>,
    checkpoint: bool,
    replay: bool,
    /// Timer surface for this invocation. `None` where timers do not apply —
    /// a workflow operative's stage (it has its own wake model) and a
    /// `compensate` hook — so the refusal is structural, not a runtime check.
    timers: Option<&'a RefCell<TimerSession>>,
}

/// Replay-stable 128-bit identifier returned by [`ProcedureCtx::stable_id`].
/// Derived by a frozen FNV-1a-128 over
/// `entity identity ‖ change seq ‖ per-invocation call ordinal` — the
/// algorithm is fixed by spec, independent of any library version.
/// `Display` renders 32 lowercase hex chars. The drop-in replacement for
/// `Uuid::new_v4()` in operative code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct StableId(pub u128);

impl std::fmt::Display for StableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl StableId {
    pub(crate) fn derive(entity_identity: &[u8], seq: u64, ordinal: u32) -> Self {
        let mut hash = fnv1a128(FNV128_OFFSET_BASIS, entity_identity);
        hash = fnv1a128(hash, &seq.to_be_bytes());
        hash = fnv1a128(hash, &ordinal.to_be_bytes());
        StableId(hash)
    }
}

/// Internal trait the runtime implements per-entity. Lets ProcedureCtx stay
/// non-generic while the underlying dispatcher carries the typed wiring.
pub(crate) trait EmitSink {
    /// Emit one value on this Procedure's output channel. Where it goes —
    /// consumer mailboxes, the plugin wire, a workflow stage handoff, or
    /// several of those — is the channel's shape, resolved inside the
    /// implementation. `meta` and `key` arrive already validated by
    /// [`ProcedureCtx::route`].
    ///
    /// One method, because the shape is one question: the authoring surface
    /// declares egress with a single `sink(...)`, and a channel is the
    /// plugin-bound terminal by having no consumers rather than by a second
    /// declaration.
    fn emit(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), ProcedureError>;

    fn channel_name(&self) -> &str;

    /// True iff the channel carries a `sink(...)`. User metadata rides the
    /// plugin frame, so a channel without one has nowhere to put it.
    fn has_plugin_sink(&self) -> bool {
        false
    }

    /// True iff the output channel is declared `.keyed()` — every plugin
    /// emit must then carry a natural key via `emit_keyed`.
    fn is_keyed(&self) -> bool {
        false
    }
}

impl<'a> ProcedureCtx<'a> {
    pub(crate) fn new(
        emitter: &'a mut dyn EmitSink,
        now: u64,
        invocation_seq: u64,
        entity_identity: &'a [u8],
    ) -> Self {
        Self {
            emitter,
            now,
            invocation_seq,
            entity_identity,
            entity_key: None,
            stable_id_ordinal: Cell::new(0),
            checkpoint: false,
            replay: false,
            timers: None,
        }
    }

    /// Attach the key this Entity was routed by. Set by every dispatch path
    /// from the Entity's own slot; left off where there is no Entity key
    /// (single-entity mode), which is what keeps the keyed-channel refusal
    /// alive exactly there.
    pub(crate) fn with_entity_key(mut self, entity_key: Option<&'a str>) -> Self {
        self.entity_key = entity_key;
        self
    }

    /// Attach this invocation's timer session. Set by the stream dispatcher
    /// for `run` and `on_timer`; every other construction site leaves it off.
    pub(crate) fn with_timers(mut self, timers: &'a RefCell<TimerSession>) -> Self {
        self.timers = Some(timers);
        self
    }

    /// Mark the invocation as a replay of already-processed input. Set by the
    /// dispatcher from the entity payload; it is what makes
    /// [`Self::spawn_workflow`] a live-processing-only side effect.
    pub(crate) fn during_replay(mut self, replay: bool) -> Self {
        self.replay = replay;
        self
    }

    /// Close the current checkpoint interval: every input folded in since the
    /// last checkpoint — this one included — is acknowledged, and the state
    /// they accumulated is made durable, as one atomic record.
    ///
    /// Only meaningful under `ChannelBuilder::checkpoint_ack()`. An operative
    /// on the default per-input granularity already acknowledges every input,
    /// so the call is a no-op there rather than an error — a checkpointing
    /// Procedure stays correct if its operative later drops the declaration.
    ///
    /// Call it where the accumulated state is genuinely coherent (a closed
    /// window, a completed batch): the interval you leave open is the work a
    /// crash replays and the input log retains.
    pub fn checkpoint(&mut self) {
        self.checkpoint = true;
    }

    /// Whether `run` closed the interval with [`Self::checkpoint`].
    pub(crate) fn checkpointed(&self) -> bool {
        self.checkpoint
    }

    /// The invocation's time value, computed once by the runtime from the
    /// Procedure's declared `Clock` before `run` executes: epoch
    /// milliseconds for `WallClock`, the extracted input timestamp for
    /// `EventTimeFrom`, the durable change seq for `LogicalClock`. Every
    /// call within one invocation returns the same value. This is the
    /// determinism-contract replacement for `SystemTime::now()`.
    pub fn now(&self) -> u64 {
        self.now
    }

    /// The key this Entity was routed by — the one `init_state` was handed —
    /// or `None` for a single-entity Procedure. It is part of the
    /// invocation's identity, so reading it is replay-stable; a keyed-channel
    /// emit that supplies no key of its own is emitted under it.
    pub fn entity_key(&self) -> Option<&str> {
        self.entity_key
    }

    /// A deterministic, replay-stable 128-bit identifier: derived from the
    /// Entity identity, the invocation's durable change seq, and a
    /// per-invocation call ordinal. Repeated calls in one invocation
    /// differ; the n-th call in a replayed invocation equals the n-th
    /// call in the original. The determinism-contract replacement for
    /// `Uuid::new_v4()`.
    pub fn stable_id(&self) -> StableId {
        let ordinal = self.stable_id_ordinal.get();
        self.stable_id_ordinal.set(ordinal + 1);
        StableId::derive(self.entity_identity, self.invocation_seq, ordinal)
    }

    /// Ask the controller for a workflow instance: start `factory_id` under
    /// `instance_id` if that id is free, otherwise apply `if_running`
    /// ([`IfRunning::Deliver`] feeds the running instance, [`IfRunning::Error`]
    /// reports [`SpawnError::AlreadyRunning`]). `payload` is the instance's
    /// first input on a spawn, one more input on a delivery.
    ///
    /// Returns immediately. The request cannot fault this procedure: a refusal,
    /// an unreachable controller, or an unknown factory surfaces only through
    /// the returned [`SpawnHandle`] — or, if the handle is dropped unobserved,
    /// as one warning line.
    ///
    /// Concurrent requests for one `instance_id` race-resolve controller-side:
    /// exactly one spawns and every other payload is delivered to it.
    ///
    /// Spawning is a side effect of **live** processing. Replayed input — a
    /// crash-resume or a late-input correction re-running this invocation —
    /// issues no request and answers [`SpawnOutcome::SkippedOnReplay`], so a
    /// replay never doubles the workers a run created.
    ///
    /// The timer calls on this same context behave the OPPOSITE way and
    /// deliberately so: a timer is internal state, so a replay registers it,
    /// where skipping would rebuild a pending set the live run never had.
    pub fn spawn_workflow(
        &self,
        instance_id: impl Into<String>,
        factory_id: impl Into<String>,
        payload: impl Into<Vec<u8>>,
        if_running: IfRunning,
    ) -> SpawnHandle {
        let (instance_id, factory_id) = (instance_id.into(), factory_id.into());
        if self.replay {
            return SpawnHandle::resolved(
                instance_id,
                factory_id,
                Ok(SpawnOutcome::SkippedOnReplay),
            );
        }
        spawn::request(instance_id, factory_id, payload.into(), if_running)
    }

    /// Register an **event-time** timer at an absolute timestamp, returning
    /// that timestamp. It fires when this Entity's event-time high-water
    /// reaches it — so an Entity that stops receiving input never fires one,
    /// however much wall-clock time passes. That is inherent to event time;
    /// [`Self::timer_after`] is the answer to "nothing has arrived".
    ///
    /// Refused with [`ProcedureError::EventTimeTimerWithoutEventTimeClock`]
    /// unless the Procedure declares `type Clock = EventTimeFrom<_>`: without
    /// one there is no high-water to fire against.
    ///
    /// Registering the same timestamp twice is idempotent — pending timers are
    /// a set. Handle the firing in [`Procedure::on_timer`].
    ///
    /// [`Procedure::on_timer`]: crate::procedure::Procedure::on_timer
    pub fn timer_at_event_time(&self, scheduled_at: u64) -> Result<u64, ProcedureError> {
        Ok(self
            .timers()?
            .borrow_mut()
            .register_at_event_time(scheduled_at)?)
    }

    /// Register a **processing-time** timer `delay` after [`Self::now`],
    /// returning the absolute timestamp it is scheduled at.
    ///
    /// The anchor is `ctx.now()` — the declared Clock — not an ambient read,
    /// which is what makes a replayed invocation register the same absolute
    /// timestamp the live one did. Under an `EventTimeFrom` clock the anchor is
    /// therefore the input's event time; a stream that is caught up schedules
    /// the same instant a wall clock would, and one replaying history schedules
    /// a past instant, which fires at once.
    pub fn timer_after(&self, delay: Duration) -> Result<u64, ProcedureError> {
        Ok(self.timers()?.borrow_mut().register_after(delay)?)
    }

    /// Cancel a pending event-time timer by the timestamp
    /// [`Self::timer_at_event_time`] returned. `false` if none was pending.
    pub fn cancel_event_time_timer(&self, scheduled_at: u64) -> Result<bool, ProcedureError> {
        Ok(self
            .timers()?
            .borrow_mut()
            .cancel(TimerKind::EventTime, scheduled_at))
    }

    /// Cancel a pending processing-time timer by the timestamp
    /// [`Self::timer_after`] returned. `false` if none was pending.
    pub fn cancel_processing_time_timer(&self, scheduled_at: u64) -> Result<bool, ProcedureError> {
        Ok(self
            .timers()?
            .borrow_mut()
            .cancel(TimerKind::ProcessingTime, scheduled_at))
    }

    fn timers(&self) -> Result<&'a RefCell<TimerSession>, ProcedureError> {
        self.timers.ok_or(ProcedureError::TimersUnavailable)
    }

    /// Emit a typed value on the producer-edge channel for this Procedure.
    /// `C` must match `Procedure::Output`; the channel id was cached at
    /// operative-build time from the producer's output-stream wiring
    /// (`.into::<S>()`), so this call performs no name lookups.
    ///
    /// On a `.keyed()` channel the emit carries this Entity's own key
    /// ([`Self::entity_key`]) — equal to the routing key by construction, so
    /// it cannot drift from it. A single-entity Procedure has no such key and
    /// fails with `KeyRequiredOnKeyedChannel`; an output whose natural key
    /// differs from the Entity's uses [`Self::emit_keyed`].
    pub fn emit<C>(&mut self, value: C) -> Result<(), ProcedureError>
    where
        C: 'static + Send + Sync,
    {
        self.route(Arc::new(value), None, None)
    }

    /// Emit with user-attached string-keyed metadata. Valid on any channel
    /// carrying a `sink(...)`. Pure inter-procedure channels with
    /// no plugin sink return `MetadataNotAllowedOnInterProcedureChannel`.
    /// Keys in the reserved `flz.` namespace are rejected.
    pub fn emit_with_meta<C>(
        &mut self,
        value: C,
        meta: HashMap<String, String>,
    ) -> Result<(), ProcedureError>
    where
        C: 'static + Send + Sync,
    {
        self.route(Arc::new(value), Some(meta), None)
    }

    /// Emit on a `.keyed()` channel with the output's natural key, for the
    /// output whose key differs from the Entity's own — [`Self::emit`]
    /// already carries that one. The runtime stamps `flz.entity` /
    /// `flz.seq` / `flz.key` on the plugin frame; the key must be
    /// replay-stable by the determinism contract (derived from
    /// input/state/ctx, never from ambient sources). Fails with
    /// `KeyNotAllowedOnNonKeyedChannel` on channels not declared `.keyed()`.
    pub fn emit_keyed<C>(&mut self, key: impl Into<String>, value: C) -> Result<(), ProcedureError>
    where
        C: 'static + Send + Sync,
    {
        self.route(Arc::new(value), None, Some(key.into()))
    }

    /// [`Self::emit_keyed`] with user metadata. Identity stamps are
    /// appended after user metadata so they can never be clobbered; keys
    /// in the reserved `flz.` namespace are rejected.
    pub fn emit_keyed_with_meta<C>(
        &mut self,
        key: impl Into<String>,
        value: C,
        meta: HashMap<String, String>,
    ) -> Result<(), ProcedureError>
    where
        C: 'static + Send + Sync,
    {
        self.route(Arc::new(value), Some(meta), Some(key.into()))
    }

    /// Single emit funnel: enforces the keyed-channel contract (the
    /// channel declaration is the single source of truth for whether a
    /// key is required), rejects reserved `flz.` metadata keys and metadata
    /// with nowhere to ride, then hands the value to the sink. Supplying no
    /// key on a keyed channel resolves to this Entity's own key here, so the
    /// defaulted emit and an explicit `emit_keyed` are one path from this
    /// point on and their stamping cannot diverge.
    fn route(
        &mut self,
        arc: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), ProcedureError> {
        let key = match (self.emitter.is_keyed(), key) {
            (true, None) => Some(
                self.entity_key
                    .ok_or_else(|| ProcedureError::KeyRequiredOnKeyedChannel {
                        channel: self.emitter.channel_name().to_string(),
                    })?
                    .to_string(),
            ),
            (false, Some(_)) => {
                return Err(ProcedureError::KeyNotAllowedOnNonKeyedChannel {
                    channel: self.emitter.channel_name().to_string(),
                })
            }
            (_, key) => key,
        };
        if let Some(meta) = &meta {
            if let Some(reserved) = meta
                .keys()
                .find(|k| k.starts_with(RESERVED_METADATA_PREFIX))
            {
                return Err(ProcedureError::ReservedMetadataKey {
                    key: reserved.clone(),
                });
            }
        }
        if meta.is_some() && !self.emitter.has_plugin_sink() {
            return Err(ProcedureError::MetadataNotAllowedOnInterProcedureChannel {
                channel: self.emitter.channel_name().to_string(),
            });
        }
        self.emitter.emit(arc, meta, key)
    }
}
