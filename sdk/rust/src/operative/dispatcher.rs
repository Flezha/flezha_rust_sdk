//! Per-Procedure typed dispatcher.
//!
//! `ProcedureDispatcherImpl<S>` carries S statically so:
//! - mailbox payloads are downcast to `Arc<S::Input>` without allocation
//! - the entity's state is downcast to `(S, S::State)` and threaded
//!   through `S::run` with a typed signature
//! - the emit sink already knows the cached output channel id
//!
//! The dispatcher trait object stores a single `Arc<dyn ProcedureDispatcher>`
//! per `EntitySlot`. The inflight CAS on the mailbox guarantees that
//! `dispatch(...)` is never called concurrently for the same slot, so
//! the per-entity `(S, S::State)` is safely accessible behind a
//! `Mutex` lock that is always uncontended in practice.

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use flz_transport::transport::FlezhaStream;

use crate::channel::builder::BuildEntityArgs;
use crate::channel::{ChannelId, ChannelKind, ProcedureId, RetryPolicy};
use crate::entity::flusher::{EntityFlusher, FlushableBuffer, TypedBodies, TypedBuffer};
use crate::entity::header::HeaderTemplate;
use crate::entity::timer::{PendingTimers, TimerKind, TimerOp, TimerPolicy};
use crate::entity::{EntityItem, EntityPayload, EntitySlot, ProcedureDispatcher};
use crate::operative::instance::StreamInstance;
use crate::operative::topology::OperativeTopology;
use crate::procedure::change::NoChange;
use crate::procedure::ctx::EmitSink;
use crate::procedure::state_handle::StateHandle;
use crate::procedure::{Procedure, ProcedureCtx, ProcedureError};
use crate::util::LockRecover;
use crate::workflow::{WorkflowCollectSink, WorkflowError};

/// Per-entity holder for a typed Procedure instance + its State. Boxed and
/// type-erased into `EntitySlot.state`.
pub struct EntityState<S: Procedure> {
    pub procedure: S,
    pub state: S::State,
}

/// Build the per-Procedure serialize/deserialize state fn pointers used by
/// `serialize_operative_state` and `apply_init_values`. The closure captures
/// S statically; `serialize_state` downcasts the type-erased state to
/// `EntityState<S>` and emits `serde_json::Value` via `S::State:
/// Serialize`. `deserialize_state` parses a `serde_json::Value` into
/// `S::State` and wraps it with a fresh `S::default()` to match what the
/// dispatcher expects when it locks the Mutex.
pub(crate) fn build_state_fns<S: Procedure>() -> (
    crate::channel::builder::SerializeStateFn,
    crate::channel::builder::DeserializeStateFn,
) {
    let serialize: crate::channel::builder::SerializeStateFn = Arc::new(
        |state: &Box<dyn Any + Send>| -> Result<serde_json::Value, String> {
            let typed = state
                .downcast_ref::<EntityState<S>>()
                .ok_or_else(|| "serialize_state: state type mismatch".to_string())?;
            serde_json::to_value(&typed.state).map_err(|e| format!("serialize_state: {}", e))
        },
    );
    let deserialize: crate::channel::builder::DeserializeStateFn = Arc::new(
        |value: &serde_json::Value| -> Result<Box<dyn Any + Send>, String> {
            let state: S::State = serde_json::from_value(value.clone())
                .map_err(|e| format!("deserialize_state: {}", e))?;
            let typed = EntityState::<S> {
                procedure: S::default(),
                state,
            };
            Ok(Box::new(typed) as Box<dyn Any + Send>)
        },
    );
    (serialize, deserialize)
}

/// The producer- and consumer-edge facts an Entity's dispatcher resolves once
/// at construction, so emit pays zero topology lookups on the hot path.
struct EdgeFacts {
    output_channel_id: Option<ChannelId>,
    output_channel_kind: ChannelKind,
    output_channel_name: Option<Arc<str>>,
    /// `.keyed()` opt-in of the producer-edge channel.
    output_channel_keyed: bool,
    /// Input-channel name stamped on dead-letter entries; `None` for
    /// procedures without a wired input channel.
    input_channel_name: Option<Arc<str>>,
    /// Retry-then-park policy of the input channel (defaults when unwired).
    retry: RetryPolicy,
}

/// Typed dispatcher. Caches its edge facts at construction so emit pays zero
/// name-lookup cost on the hot path.
pub struct ProcedureDispatcherImpl<S: Procedure> {
    procedure_id: ProcedureId,
    edges: EdgeFacts,
    /// Readable Entity identity (`instance/procedure/entity_key`) stamped
    /// as `flz.entity` on keyed-channel emits.
    entity_label: Arc<str>,
    /// The key this Entity was routed by, `None` in single-entity mode.
    /// Served to every invocation's `ProcedureCtx`, where it is both
    /// `ctx.entity_key()` and the default key of a keyed-channel emit.
    entity_key: Option<Arc<str>>,
    /// Flush-before-emit wiring, mirrored from the owning `EntitySlot`
    /// so the emit sink can drain this entity's just-applied changes to
    /// the durable WAL path before its output frame is written.
    flush: FlushWiring,
    /// What this Procedure declares about its timers, resolved once.
    timers: TimerPolicy,
    _phantom: std::marker::PhantomData<fn() -> S>,
}

/// Per-entity state the dispatcher needs to drive flush-before-emit.
/// Cloned cheaply (Arc handles + a Copy header) into the emit sink.
#[derive(Clone)]
struct FlushWiring {
    entity_identity: Arc<Vec<u8>>,
    change_seq: Arc<AtomicU64>,
    header: HeaderTemplate,
    persistence_enabled: bool,
    /// Effectively-once tier (`OperativeTopology::effectively_once`): the
    /// invocation's plugin emits are captured into an atomic effect record
    /// on the reliable state sink instead of the primary stream.
    effectively_once: bool,
    /// Checkpoint ack-granularity (`AckGranularity::Checkpoint`): the
    /// consumed-offset advances only when the interval closes, and the
    /// changes it applies — with its outputs, under
    /// `CheckpointOutputs::AtCheckpoint` — are held in the flush buffer
    /// until then. `None` under the per-input default.
    checkpoint: Option<crate::operative::topology::CheckpointPolicy>,
    /// Late-input correction policy, `None` unless the class declared a
    /// `correction_window()`. Admission and the high-water live in the
    /// engine's assembly (`runner::poll_core`), which reads this through
    /// `ProcedureDispatcher::correction_policy`.
    correction: Option<crate::operative::topology::CorrectionPolicy>,
}

impl<S: Procedure> ProcedureDispatcherImpl<S> {
    fn new(
        procedure_id: ProcedureId,
        edges: EdgeFacts,
        entity_label: Arc<str>,
        entity_key: Option<Arc<str>>,
        flush: FlushWiring,
    ) -> Self {
        Self {
            procedure_id,
            edges,
            entity_label,
            entity_key,
            flush,
            timers: crate::entity::timer::timer_policy_for::<S>(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Fresh per-invocation output-capture cell — `Some` only under the
    /// effectively-once tier (plugin emits then form the atomic effect
    /// record instead of leaving on a wire path).
    fn effect_capture(&self) -> Option<RefCell<Vec<crate::entity::flusher::OutputRecord>>> {
        self.flush
            .effectively_once
            .then(|| RefCell::new(Vec::new()))
    }

    /// Per-invocation determinism inputs, computed once at entry: the
    /// declared Clock's value (served by `ctx.now()`) and the durable
    /// change seq (`stable_id` derivation). `input` is `None` only for
    /// `compensate`. Wall time is lazy — only WallClock pays the
    /// `SystemTime::now()`.
    fn invocation_inputs(&self, input: Option<&S::Input>) -> (u64, u64) {
        let seq = self.flush.change_seq.load(Ordering::Relaxed);
        let now = <S::Clock as crate::procedure::ClockFor<S::Input>>::evaluate(
            input,
            crate::procedure::clock::wall_now_ms,
            || seq,
        );
        (now, seq)
    }

    /// Ship whatever the buffer holds now, on this Entity's identity, seq and
    /// header — the inline half of the drain the background flusher otherwise
    /// performs.
    fn flush_now(&self, buffer: &mut dyn FlushableBuffer) {
        crate::entity::flusher::flush_one(
            buffer,
            &self.flush.entity_identity,
            &self.flush.change_seq,
            &self.flush.header,
            self.flush.persistence_enabled,
        );
    }

    /// Ship the Changes (and, under the effectively-once tier, the captured
    /// outputs as one atomic effect entry) a collecting-path proc applied — or
    /// drain and discard when the class opted out. The workflow analog of the
    /// stream `dispatch` flush hook; both feed `flusher::flush_one`.
    fn flush_collected(
        &self,
        buffer: RefCell<Vec<S::Change>>,
        outputs: Vec<crate::entity::flusher::OutputRecord>,
        event_time: u64,
    ) {
        let changes = buffer.into_inner();
        let mut buf = if self.flush.effectively_once {
            if changes.is_empty() && outputs.is_empty() {
                return;
            }
            // Workflow inputs arrive via args/inbox, not the input-log —
            // no consumed-offset to carry.
            TypedBuffer::<S> {
                effects: vec![crate::entity::flusher::EffectEntry {
                    changes,
                    outputs,
                    consumed_offset: None,
                    dlq: None,
                    event_time,
                    timers: Vec::new(),
                }],
                ..Default::default()
            }
        } else {
            TypedBuffer::<S> {
                changes,
                ..Default::default()
            }
        };
        self.flush_now(&mut buf);
    }

    /// The changes one invocation applies into, seeded with a previous failed
    /// attempt's: those state mutations stayed applied (at-least-once), so
    /// durable state must not diverge from memory. The cell built over them is
    /// shared with the flush-before-emit hook — both run on this one dispatch
    /// thread inside one invocation, so the `RefCell` borrows never overlap.
    fn carry_over(&self, buffer: &mut TypedBuffer<S>) -> Vec<S::Change> {
        let mut carried = std::mem::take(&mut buffer.changes);
        carried.append(&mut buffer.pending_attempt);
        carried
    }

    /// Drain this entity's pending changes to the durable WAL path before the
    /// sink writes the matching output frame, so change N reaches the
    /// controller's WAL before N's output reaches Kafka. `None` when there is
    /// nothing to order against: a `NoChange` / non-persistent operative keeps
    /// the zero-cost inline emit, and under the tier the changes and the
    /// outputs ship in ONE effect record.
    fn flush_before_emit<'a>(
        &self,
        cell: &'a RefCell<Vec<S::Change>>,
    ) -> Option<FlushBeforeEmit<'a>> {
        if self.flush.effectively_once
            || !self.flush.persistence_enabled
            || !flush_before_emit_enabled()
        {
            return None;
        }
        let identity = Arc::clone(&self.flush.entity_identity);
        let seq = Arc::clone(&self.flush.change_seq);
        let header = self.flush.header;
        Some(Box::new(move || {
            let mut buf = TypedBuffer::<S> {
                changes: std::mem::take(&mut *cell.borrow_mut()),
                ..Default::default()
            };
            crate::entity::flusher::flush_one(&mut buf, &identity, &seq, &header, true);
        }))
    }

    /// The emit sink one invocation writes through, on this Entity's producer
    /// edge and the connection its item rode in on.
    fn emit_sink<'a>(
        &'a self,
        scope: &EntityScope<'a, S>,
        consumed_offset: Option<u64>,
        effect_outputs: Option<&'a RefCell<Vec<crate::entity::flusher::OutputRecord>>>,
        cell: &'a RefCell<Vec<S::Change>>,
    ) -> EntityEmitSink<'a> {
        EntityEmitSink {
            producer: scope.slot,
            output_channel_id: self.edges.output_channel_id,
            output_channel_name: self.edges.output_channel_name.as_deref(),
            keyed: self.edges.output_channel_keyed,
            entity_label: &self.entity_label,
            change_seq: &self.flush.change_seq,
            emit_ordinal: 0,
            instance: scope.instance.upgrade(),
            stream: Arc::clone(scope.stream),
            flush_before_emit: self.flush_before_emit(cell),
            consumed_offset,
            effect_outputs,
        }
    }

    /// The call into the author's code, contained: `run` on an input or
    /// `on_timer` on a firing, inside the emit sink and the timer session both
    /// share. `Ok((ops, checkpointed))` is a completed attempt and what its
    /// session committed; `Err` is the failure text and whether the ladder may
    /// retry it. The panic containment is what keeps a poison payload from
    /// poisoning the held `state` lock.
    fn call_contained(
        &self,
        call: Call<S>,
        now: u64,
        scope: &mut EntityScope<'_, S>,
        cell: &RefCell<Vec<S::Change>>,
        captured: Option<&RefCell<Vec<crate::entity::flusher::OutputRecord>>>,
    ) -> Result<(Vec<TimerOp>, bool), (String, bool)> {
        let invocation_seq = self.flush.change_seq.load(Ordering::Relaxed);
        let replay = call.replay();
        // The removal is op 0 for a firing, so it rides the same durable
        // record as whatever `on_timer` goes on to apply and emit.
        let firing = call.firing();
        let mut sink = self.emit_sink(scope, call.consumed_offset(), captured, cell);
        let state = &mut *scope.state;
        let live_timers = &mut *scope.timers;
        let session = self.timers.session(live_timers, now, firing);
        let mut ctx =
            ProcedureCtx::new(&mut sink, now, invocation_seq, &self.flush.entity_identity)
                .with_entity_key(self.entity_key.as_deref())
                .during_replay(replay)
                .with_timers(&session);
        let handle = StateHandle::<S>::new(&mut state.state, cell);

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match call {
            Call::Input { input, .. } => state.procedure.run(input, handle, &mut ctx),
            Call::Timer { fired_at, kind } => {
                state.procedure.on_timer(fired_at, kind, handle, &mut ctx)
            }
        }));
        match outcome {
            Ok(Ok(())) => {
                let checkpointed = ctx.checkpointed();
                Ok((session.into_inner().commit(live_timers), checkpointed))
            }
            failed => Err(classify_failure(failed)),
        }
    }

    /// What one attempt leaves for the settle. `Err` is a retryable non-final
    /// failure: nothing ships and the engine's ladder re-hands the item. A
    /// failed attempt's captured tier outputs never ship either — the
    /// invocation did not complete — so they are dropped here, not folded.
    fn resolve_attempt(
        &self,
        outcome: Result<(Vec<TimerOp>, bool), (String, bool)>,
        firing: Option<(TimerKind, u64)>,
        now: u64,
        is_final: bool,
        scope: &mut EntityScope<'_, S>,
        captured: Option<RefCell<Vec<crate::entity::flusher::OutputRecord>>>,
    ) -> Result<Settled, String> {
        let (error, retryable) = match outcome {
            Ok((timers, checkpointed)) => {
                return Ok(Settled {
                    timers,
                    outputs: captured.map(RefCell::into_inner).unwrap_or_default(),
                    checkpointed,
                    park_error: None,
                })
            }
            Err(failure) => failure,
        };
        if retryable && !is_final {
            return Err(error);
        }
        // A firing that exhausted its budget still records the removal: a
        // poison timer must park once, not fire forever — the same reason a
        // park closes a checkpoint interval.
        let timers = match firing {
            Some(_) => self
                .timers
                .session(scope.timers, now, firing)
                .into_inner()
                .commit(scope.timers),
            None => Vec::new(),
        };
        Ok(Settled {
            timers,
            outputs: Vec::new(),
            checkpointed: false,
            park_error: Some(error),
        })
    }

    /// The dead-letter entry a park records: the input channel that fed the
    /// poison, the attempts it exhausted, and the payload bytes — a final
    /// attempt captured them before the value moved into the call, a
    /// re-handable one still holds the input and extracts them only here.
    fn park_entry(
        &self,
        error: String,
        attempt: u32,
        parked_payload_bytes: Option<Vec<u8>>,
        retained_input: Option<&Arc<S::Input>>,
    ) -> crate::entity::flusher::DlqEntryBody {
        crate::entity::park::PARKED_TOTAL.fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "[flezha] procedure {} parking poison payload to DLQ after {} attempt(s): {}",
            S::name(),
            attempt,
            error
        );
        crate::entity::flusher::DlqEntryBody {
            channel: self
                .edges
                .input_channel_name
                .as_deref()
                .unwrap_or("<unwired>")
                .to_string(),
            error,
            attempts: attempt,
            payload: parked_payload_bytes.or_else(|| {
                retained_input.and_then(|arc| crate::entity::park::extract_payload_bytes(&**arc))
            }),
        }
    }

    /// Settle one attempt into this Entity's buffer through the one settlement
    /// rule the erased host also calls, then act on what it hands back: a
    /// class with no flusher entry has nothing that would ever drain its timer
    /// ops, so it drains them here, and a tier-off park holds the Entity until
    /// the controller acks the dead-letter append.
    fn settle(
        &self,
        buffer: &mut TypedBuffer<S>,
        effects: crate::entity::flusher::InvocationEffects<S::Change>,
        dlq: Option<crate::entity::flusher::DlqEntryBody>,
    ) {
        let consumed_offset = effects.offset;
        let tier_off_dlq = crate::entity::flusher::settle_effects::<TypedBodies<S>>(
            buffer,
            crate::entity::flusher::SettlePolicy {
                effectively_once: self.flush.effectively_once,
                checkpoint: self.flush.checkpoint,
                buffered: true,
            },
            effects,
            dlq,
        );
        if !buffer.timer_ops.is_empty() && !flusher_registered::<S>(self.flush.effectively_once) {
            self.flush_now(buffer);
        }
        if let Some(entry) = tier_off_dlq {
            // Park-then-advance, never a drop: ship the park frame and hold
            // this entity (the drain thread) until the controller acks the
            // dead-letter append as durable.
            crate::entity::park::park_tier_off(
                &self.flush.entity_identity,
                &self.entity_label,
                consumed_offset,
                entry,
            );
        }
    }

    /// Run ONE ATTEMPT of one invocation — `run` on an input, or `on_timer`
    /// on a firing — through the emit sink, the timer session and the durable
    /// record it produces. The two differ only in the call into the author's
    /// code; everything a failure has to be contained by is shared, which is
    /// what makes a poison timer park exactly like a poison input.
    ///
    /// The retry ladder's counting, backoff and re-hand live in the engine
    /// (`runner::poll_core`): a retryable non-final failure returns
    /// `Err(error)` and leaves nothing shipped; a non-retryable or final
    /// failure parks here (the park rides this entity's buffer) and returns
    /// the timer ops the attempt committed, consuming the item.
    fn invoke_once(
        &self,
        invocation: Invocation<'_, S>,
        mut scope: EntityScope<'_, S>,
    ) -> Result<Vec<TimerOp>, String> {
        let Invocation {
            call,
            now,
            attempt,
            is_final,
            parked_payload_bytes,
            retained_input,
        } = invocation;
        let offset = call.consumed_offset();
        let firing = call.firing();
        let cell = RefCell::new(self.carry_over(scope.buffer));
        let captured = self.effect_capture();

        let outcome = self.call_contained(call, now, &mut scope, &cell, captured.as_ref());
        let mut settled =
            match self.resolve_attempt(outcome, firing, now, is_final, &mut scope, captured) {
                Ok(settled) => settled,
                // Keep the attempt's changes out of the flusher's reach until
                // the item resolves — the next attempt re-takes them.
                Err(error) => {
                    scope.buffer.pending_attempt = cell.into_inner();
                    return Err(error);
                }
            };

        let dlq = settled
            .park_error
            .take()
            .map(|error| self.park_entry(error, attempt, parked_payload_bytes, retained_input));
        let committed = settled.timers.clone();
        let effects = settled.into_effects(cell.into_inner(), offset, now);
        self.settle(scope.buffer, effects, dlq);
        Ok(committed)
    }
}

/// The call into the author's code one invocation makes. The retry ladder, the
/// emit sink, the effect record and the DLQ park are the same machinery either
/// way — only this differs, which is why they share one code path.
enum Call<S: Procedure> {
    Input {
        input: S::Input,
        /// Durable input-log offset this invocation acknowledges. A firing
        /// carries none — it consumes no input — which is why it lives on the
        /// variant rather than beside it.
        consumed_offset: Option<u64>,
        replay: bool,
    },
    Timer {
        fired_at: u64,
        kind: TimerKind,
    },
}

impl<S: Procedure> Call<S> {
    fn consumed_offset(&self) -> Option<u64> {
        match self {
            Call::Input {
                consumed_offset, ..
            } => *consumed_offset,
            Call::Timer { .. } => None,
        }
    }

    fn replay(&self) -> bool {
        matches!(self, Call::Input { replay: true, .. })
    }

    /// The firing this invocation is, if it is one — what seeds the timer
    /// session and what a park still has to record.
    fn firing(&self) -> Option<(TimerKind, u64)> {
        match self {
            Call::Timer { fired_at, kind } => Some((*kind, *fired_at)),
            Call::Input { .. } => None,
        }
    }
}

/// One attempt of one invocation: the call, and the ladder facts that decide
/// whether a failure re-hands the item or parks it.
struct Invocation<'a, S: Procedure> {
    call: Call<S>,
    /// The invocation's `ctx.now()`: the declared Clock's value for an input,
    /// the scheduled timestamp for a firing.
    now: u64,
    attempt: u32,
    is_final: bool,
    parked_payload_bytes: Option<Vec<u8>>,
    retained_input: Option<&'a Arc<S::Input>>,
}

impl<'a, S: Procedure> Invocation<'a, S> {
    /// A due firing. It consumes no input, so it acknowledges no offset and
    /// has no payload a park could record.
    fn firing(fire: &'a flz_core::timer::TimerFire, attempt: u32, is_final: bool) -> Self {
        Self {
            call: Call::Timer {
                fired_at: fire.scheduled_at,
                kind: fire.kind,
            },
            now: fire.scheduled_at,
            attempt,
            is_final,
            parked_payload_bytes: None,
            retained_input: None,
        }
    }

    /// The last attempt on an input: the value moves into `run` — zero-copy
    /// when the Arc is uniquely held, a value clone when fan-out kept it
    /// shared — so its park bytes are captured first, because a park still has
    /// to record what poisoned the Entity.
    fn last_input(
        input: Arc<S::Input>,
        consumed_offset: Option<u64>,
        replay: bool,
        now: u64,
        attempt: u32,
    ) -> Self {
        Self {
            parked_payload_bytes: crate::entity::park::extract_payload_bytes(&*input),
            call: Call::Input {
                input: Arc::unwrap_or_clone(input),
                consumed_offset,
                replay,
            },
            now,
            attempt,
            is_final: true,
            retained_input: None,
        }
    }

    /// A re-handable attempt on an input: it runs against a copy so the
    /// original survives for the engine's re-hand, and pays the park-bytes
    /// extraction only if it parks after all.
    fn retryable_input(
        input: &'a Arc<S::Input>,
        consumed_offset: Option<u64>,
        replay: bool,
        now: u64,
        attempt: u32,
    ) -> Self {
        Self {
            call: Call::Input {
                input: (**input).clone(),
                consumed_offset,
                replay,
            },
            now,
            attempt,
            is_final: false,
            parked_payload_bytes: None,
            retained_input: Some(input),
        }
    }
}

/// The Entity's live state for one dispatch, behind the slot locks the engine
/// already holds.
struct EntityScope<'a, S: Procedure> {
    slot: &'a Arc<EntitySlot>,
    state: &'a mut EntityState<S>,
    buffer: &'a mut TypedBuffer<S>,
    timers: &'a mut PendingTimers,
    /// The connection this invocation's emits ride — the one its item came in
    /// on, inherited by any processing-time timer it arms.
    stream: &'a Arc<FlezhaStream>,
    instance: &'a Weak<StreamInstance>,
}

/// What one settling attempt left. `park_error` is set when the item parks —
/// so the park rides the same record as the consumed-offset advance it
/// acknowledges.
struct Settled {
    timers: Vec<TimerOp>,
    outputs: Vec<crate::entity::flusher::OutputRecord>,
    checkpointed: bool,
    park_error: Option<String>,
}

impl Settled {
    /// The invocation's effects, as the shared settlement rule takes them.
    /// `parked` is left false on purpose: the tier arm re-derives it from the
    /// park entry, so the two callers of `settle_effects` cannot disagree.
    fn into_effects<C>(
        self,
        changes: Vec<C>,
        offset: Option<u64>,
        event_time: u64,
    ) -> crate::entity::flusher::InvocationEffects<C> {
        crate::entity::flusher::InvocationEffects {
            changes,
            timers: self.timers,
            outputs: self.outputs,
            offset,
            event_time,
            checkpointed: self.checkpointed,
            parked: false,
        }
    }
}

/// How one collecting entry point differs from the other: the error prefix,
/// whether the author's own retryability classification survives (a
/// compensation has no ladder of its own), and whether keyed plugin emits are
/// captured into the invocation's atomic effect record.
#[derive(Clone, Copy)]
struct CollectShape {
    prefix: &'static str,
    author_classifies: bool,
    captures_effects: bool,
}

/// The workflow error a failed collecting call reports: `{prefix} {procedure}
/// failed: {cause}` for a returned error, `panicked` for an unwind. The
/// author's own classification rides through only where the entry point has a
/// retry ladder of its own — a compensation has none, and a panic never
/// classifies, because the author never got to.
fn collect_failure(
    shape: CollectShape,
    name: &str,
    failed: Result<Result<(), ProcedureError>, Box<dyn Any + Send>>,
) -> WorkflowError {
    match failed {
        Ok(Err(err)) => WorkflowError::new(
            format!("{} {} failed: {}", shape.prefix, name, err),
            shape.author_classifies && err.is_non_retryable(),
        ),
        Err(panic) => WorkflowError::new(
            format!(
                "{} {} panicked: {}",
                shape.prefix,
                name,
                crate::procedure::error::format_panic(panic)
            ),
            false,
        ),
        Ok(Ok(())) => unreachable!("handled above"),
    }
}

/// The error text of a failed attempt and whether the ladder may retry it. A
/// panic is retryable by default — the author never got to classify it.
fn classify_failure(
    failed: Result<Result<(), ProcedureError>, Box<dyn Any + Send>>,
) -> (String, bool) {
    match failed {
        Ok(Err(err)) => {
            let retryable = !err.is_non_retryable();
            (err.to_string(), retryable)
        }
        Err(panic) => (crate::procedure::error::format_panic(panic), true),
        Ok(Ok(())) => unreachable!("handled above"),
    }
}

impl<S: Procedure> ProcedureDispatcherImpl<S> {
    /// One due firing. Assembly validated it, but the pending set is the
    /// authority: re-check under the held locks, because a restore between
    /// handout and dispatch can rebuild it.
    fn dispatch_firing(
        &self,
        fire: flz_core::timer::TimerFire,
        attempt: u32,
        is_final: bool,
        scope: EntityScope<'_, S>,
    ) -> crate::entity::ItemDisposition {
        use crate::entity::ItemDisposition;
        if !scope.timers.contains(fire.kind, fire.scheduled_at) {
            return ItemDisposition::Done { ops: Vec::new() };
        }
        match self.invoke_once(Invocation::firing(&fire, attempt, is_final), scope) {
            Ok(ops) => ItemDisposition::Done { ops },
            Err(error) => ItemDisposition::RetryLater {
                item: EntityItem::Timer(fire),
                error,
            },
        }
    }

    /// One input. A final attempt parks instead of retrying, so it consumes
    /// the item either way; a re-handable one gives the untouched payload back
    /// to the engine's ladder.
    fn dispatch_input(
        &self,
        payload: EntityPayload,
        now: Option<u64>,
        attempt: u32,
        is_final: bool,
        scope: EntityScope<'_, S>,
    ) -> crate::entity::ItemDisposition {
        use crate::entity::ItemDisposition;
        let EntityPayload {
            value,
            stream,
            consumed_offset,
            replay,
        } = payload;
        let Some(input) = Self::typed_input(value) else {
            return ItemDisposition::Done { ops: Vec::new() };
        };
        // Engine-stamped event time, or the declared clock at entry.
        let now = now.unwrap_or_else(|| self.invocation_inputs(Some(&input)).0);

        if is_final {
            let last = Invocation::last_input(input, consumed_offset, replay, now, attempt);
            let ops = self.invoke_once(last, scope);
            debug_assert!(ops.is_ok(), "a final attempt parks instead of retrying");
            return ItemDisposition::Done {
                ops: ops.unwrap_or_default(),
            };
        }
        let again = Invocation::retryable_input(&input, consumed_offset, replay, now, attempt);
        match self.invoke_once(again, scope) {
            Ok(ops) => ItemDisposition::Done { ops },
            Err(error) => ItemDisposition::RetryLater {
                item: EntityItem::Input(EntityPayload {
                    value: input as Arc<dyn Any + Send + Sync>,
                    stream,
                    consumed_offset,
                    replay,
                }),
                error,
            },
        }
    }

    /// The typed input inside a mailbox payload, `None` with the mismatch
    /// reported. Consumers with different Input types in the same
    /// StreamOperative downcast independently, because the channel wiring
    /// guarantees each only ever receives its own type.
    fn typed_input(value: Arc<dyn Any + Send + Sync>) -> Option<Arc<S::Input>> {
        match Arc::downcast::<S::Input>(value) {
            Ok(arc) => Some(arc),
            Err(_) => {
                eprintln!(
                    "[flezha] dispatcher: payload type mismatch for procedure {}",
                    S::name()
                );
                None
            }
        }
    }
}

impl<S: Procedure> ProcedureDispatcher for ProcedureDispatcherImpl<S> {
    fn dispatch_item(
        &self,
        work: crate::runner::poll_core::ItemAttempt,
        slot: &Arc<EntitySlot>,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn FlushableBuffer>,
        timers: &mut PendingTimers,
        instance: &Weak<StreamInstance>,
    ) -> crate::entity::ItemDisposition {
        use crate::entity::ItemDisposition;

        let typed_state = state
            .downcast_mut::<EntityState<S>>()
            .expect("dispatcher: state type mismatch for procedure");
        let typed_buffer = flush_buffer
            .as_any_mut()
            .downcast_mut::<TypedBuffer<S>>()
            .expect("dispatcher: flush buffer type mismatch for procedure");

        let crate::runner::poll_core::ItemAttempt {
            item,
            now,
            attempt,
            is_final,
        } = work;
        // A `CloseInterval` is consumed by the batch assembler, which closes
        // the interval without an invocation — it never reaches dispatch, and
        // it is the one item with no connection of its own.
        let stream = match &item {
            EntityItem::Input(payload) => Arc::clone(&payload.stream),
            EntityItem::Timer(fire) => Arc::clone(&fire.stream),
            EntityItem::CloseInterval => return ItemDisposition::Done { ops: Vec::new() },
        };
        let scope = EntityScope {
            slot,
            state: typed_state,
            buffer: typed_buffer,
            timers,
            stream: &stream,
            instance,
        };
        match item {
            EntityItem::Timer(fire) => self.dispatch_firing(fire, attempt, is_final, scope),
            EntityItem::Input(payload) => {
                self.dispatch_input(payload, now, attempt, is_final, scope)
            }
            EntityItem::CloseInterval => unreachable!(),
        }
    }

    fn close_interval(
        &self,
        flush_buffer: &mut Box<dyn flz_core::flusher::FlushableBuffer>,
    ) -> bool {
        // Only a checkpoint class has an interval to close.
        if self.flush.checkpoint.is_none() {
            return false;
        }
        let Some(buffer) = flush_buffer
            .as_any_mut()
            .downcast_mut::<crate::entity::flusher::TypedBuffer<S>>()
        else {
            return false;
        };
        buffer.close_interval()
    }

    fn interval_deadline(
        &self,
        flush_buffer: &mut Box<dyn flz_core::flusher::FlushableBuffer>,
    ) -> Option<u64> {
        let policy = self.flush.checkpoint?;
        let buffer = flush_buffer
            .as_any_mut()
            .downcast_mut::<crate::entity::flusher::TypedBuffer<S>>()?;
        buffer.staged.deadline_ms(policy)
    }

    fn procedure_id(&self) -> ProcedureId {
        self.procedure_id
    }

    fn procedure_name(&self) -> &'static str {
        S::name()
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.edges.retry
    }

    fn is_event_time(&self) -> bool {
        <S::Clock as crate::procedure::Clock>::IS_EVENT_TIME
    }

    fn correction_policy(&self) -> Option<crate::operative::topology::CorrectionPolicy> {
        self.flush.correction
    }

    fn entity_label(&self) -> &str {
        &self.entity_label
    }

    fn event_time_of(&self, payload: &EntityPayload) -> Option<u64> {
        if !<S::Clock as crate::procedure::Clock>::IS_EVENT_TIME {
            return None;
        }
        let input = payload.value.downcast_ref::<S::Input>()?;
        Some(
            <S::Clock as crate::procedure::ClockFor<S::Input>>::evaluate(
                Some(input),
                crate::procedure::clock::wall_now_ms,
                || 0,
            ),
        )
    }

    fn run_collecting(
        &self,
        input: Arc<dyn Any + Send + Sync>,
        state: &mut Box<dyn Any + Send>,
        topology: &OperativeTopology,
        sink: &mut dyn WorkflowCollectSink,
    ) -> Result<(), WorkflowError> {
        let input_arc = Arc::downcast::<S::Input>(input).map_err(|_| {
            WorkflowError::from(format!(
                "workflow procedure {} received an input of the wrong type",
                S::name()
            ))
        })?;
        let input: S::Input = Arc::unwrap_or_clone(input_arc);

        let typed_state = state
            .downcast_mut::<EntityState<S>>()
            .expect("run_collecting: state type mismatch for procedure");
        let inputs = self.invocation_inputs(Some(&input));
        self.collect_invoke(
            typed_state,
            topology,
            sink,
            inputs,
            CollectShape {
                prefix: "procedure",
                author_classifies: true,
                captures_effects: true,
            },
            |procedure, handle, ctx| procedure.run(input, handle, ctx),
        )
    }

    fn compensate_collecting(
        &self,
        outputs: Vec<Arc<dyn Any + Send + Sync>>,
        state: &mut Box<dyn Any + Send>,
        topology: &OperativeTopology,
        sink: &mut dyn WorkflowCollectSink,
    ) -> Result<(), WorkflowError> {
        // Recover the typed outputs the entity collected on its forward run.
        let mut typed_outputs: Vec<Arc<S::Output>> = Vec::with_capacity(outputs.len());
        for value in outputs {
            match Arc::downcast::<S::Output>(value) {
                Ok(out) => typed_outputs.push(out),
                Err(_) => {
                    return Err(WorkflowError::from(format!(
                        "compensate: collected output of the wrong type for procedure {}",
                        S::name()
                    )))
                }
            }
        }

        let typed_state = state
            .downcast_mut::<EntityState<S>>()
            .expect("compensate_collecting: state type mismatch for procedure");
        // Compensation has no current input: EventTimeFrom yields 0 here. It
        // also keeps the pre-tier emit contract (keyed emits are refused),
        // which is why it captures no effects.
        let inputs = self.invocation_inputs(None);
        self.collect_invoke(
            typed_state,
            topology,
            sink,
            inputs,
            CollectShape {
                prefix: "compensation for procedure",
                author_classifies: false,
                captures_effects: false,
            },
            |procedure, handle, ctx| procedure.compensate(typed_outputs, handle, ctx),
        )
    }

    fn restore_state(
        &self,
        state: &mut Box<dyn Any + Send>,
        timers: &mut PendingTimers,
        snapshot_header: &[u8],
        snapshot_bytes: &[u8],
        change_log_bytes: &[u8],
        entity_key: Option<&str>,
    ) -> Result<flz_core::restore::RestoreOutcome, flz_core::header::RestoreError> {
        use crate::entity::restore::{
            init_entity_with_payload, iter_change_log_records, RestoreInitPayload,
        };

        let payload = RestoreInitPayload {
            snapshot_header,
            snapshot_bytes,
            change_log_bytes,
        };

        // Rebuild the typed state from snapshot + change-log replay. This runs
        // the same path as cold-start `init_state` + `apply_change`, so a
        // counter that summed to N comes back at N rather than 0.
        let (new_state, restored_timers, migrated) =
            init_entity_with_payload::<S>(entity_key, Some(&payload))?;

        let typed = state
            .downcast_mut::<EntityState<S>>()
            .expect("restore_state: state type mismatch for procedure");
        typed.state = new_state;
        // Pending timers are folded out of the same log, so a resumed Entity
        // owes exactly the timers it owed before the crash — no marker, no
        // side-file, no re-registration by the author.
        *timers = restored_timers;

        // Terminal seq = highest WAL seq applied, so the caller can re-arm the
        // entity's change_seq and post-restore appends continue contiguously
        // (no controller-side epoch realign / truncation).
        let mut terminal: u64 = 0;
        for record in iter_change_log_records(change_log_bytes) {
            terminal = terminal.max(record?.seq);
        }
        Ok(flz_core::restore::RestoreOutcome {
            terminal_seq: terminal,
            migrated,
        })
    }

    fn snapshot_frame(
        &self,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn FlushableBuffer>,
        timers: &PendingTimers,
    ) -> Result<(Vec<u8>, u64, usize), flz_core::Error> {
        let seq = self.drain_for_snapshot(flush_buffer, timers);
        let typed = state.downcast_ref::<EntityState<S>>().ok_or_else(|| {
            flz_core::Error::State("snapshot_frame: state type mismatch for procedure".to_string())
        })?;
        // Postcard — the codec `entity/restore.rs` decodes — NOT the JSON
        // migration serializer `build_state_fns` produces.
        let body = postcard::to_allocvec(&typed.state).map_err(|e| {
            flz_core::Error::State(format!("snapshot_frame: postcard state encode failed: {e}"))
        })?;
        let header = self.flush.header.stamp(&body);
        let frame = crate::entity::flusher::build_entity_frame(
            &self.flush.entity_identity,
            seq,
            &header,
            &body,
        );
        let size = body.len();
        Ok((frame, seq, size))
    }
}

impl<S: Procedure> ProcedureDispatcherImpl<S> {
    /// One collecting-path invocation — a workflow `run` or its `compensate`
    /// — through the collect sink, the change buffer and the WAL flush all of
    /// them owe. `body` is the call into the author's code; `shape` carries
    /// the only three things the two entry points disagree about.
    fn collect_invoke<F>(
        &self,
        typed_state: &mut EntityState<S>,
        topology: &OperativeTopology,
        sink: &mut dyn WorkflowCollectSink,
        inputs: (u64, u64),
        shape: CollectShape,
        body: F,
    ) -> Result<(), WorkflowError>
    where
        F: FnOnce(&mut S, StateHandle<'_, S>, &mut ProcedureCtx<'_>) -> Result<(), ProcedureError>,
    {
        let (now, invocation_seq) = inputs;
        // Per-run change buffer: `StateHandle::apply` records Changes here; the
        // flush after the run ships them to the WAL when the class is durable.
        let buffer: RefCell<Vec<S::Change>> = RefCell::new(Vec::new());
        // Tier mode: keyed plugin emits are captured for the invocation's
        // atomic effect record instead of the workflow plugin wire.
        let effect_outputs = shape
            .captures_effects
            .then(|| self.effect_capture())
            .flatten();

        let mut collect_sink = self.collect_sink(sink, topology, effect_outputs.as_ref());
        let mut ctx = ProcedureCtx::new(
            &mut collect_sink,
            now,
            invocation_seq,
            &self.flush.entity_identity,
        )
        .with_entity_key(self.entity_key.as_deref());
        let handle = StateHandle::<S>::new(&mut typed_state.state, &buffer);

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            body(&mut typed_state.procedure, handle, &mut ctx)
        }));

        // Ship the applied Changes to the WAL regardless of outcome, so the WAL
        // reflects the state applied before any failure (at-least-once; the saga
        // compensates a partial run).
        let outputs = effect_outputs.map(RefCell::into_inner).unwrap_or_default();
        self.flush_collected(buffer, outputs, now);

        match outcome {
            Ok(Ok(())) => Ok(()),
            failed => Err(collect_failure(shape, S::name(), failed)),
        }
    }

    /// The collect sink one workflow invocation writes through, on this
    /// Entity's producer edge.
    fn collect_sink<'a>(
        &'a self,
        sink: &'a mut dyn WorkflowCollectSink,
        topology: &'a OperativeTopology,
        effect_outputs: Option<&'a RefCell<Vec<crate::entity::flusher::OutputRecord>>>,
    ) -> WorkflowCollectAdapter<'a> {
        WorkflowCollectAdapter {
            sink,
            output_channel_id: self.edges.output_channel_id,
            output_channel_kind: self.edges.output_channel_kind,
            output_channel_name: self.edges.output_channel_name.as_deref(),
            topology,
            effect_outputs,
        }
    }

    /// Drain the buffer, then re-state the pending timers ABOVE the seq the
    /// snapshot carries. The drain comes first, so the snapshot's state and
    /// its seq are captured together and `change_seq - 1` is exactly the
    /// highest record the snapshot folds — which is what lets the controller's
    /// `truncate_up_to`, keeping records strictly above it, drop exactly what
    /// the snapshot folds and no more. The timers are re-stated AFTER the seq
    /// is read, landing at `snapshot_seq + 1`, which that truncation keeps; a
    /// crash between the two leaves an untruncated log that still holds the
    /// originals, so neither order can lose a timer.
    fn drain_for_snapshot(
        &self,
        flush_buffer: &mut Box<dyn FlushableBuffer>,
        timers: &PendingTimers,
    ) -> u64 {
        self.flush_now(&mut **flush_buffer);
        let seq = self
            .flush
            .change_seq
            .load(Ordering::Acquire)
            .saturating_sub(1);
        if !timers.is_empty() {
            flush_buffer
                .as_any_mut()
                .downcast_mut::<TypedBuffer<S>>()
                .expect("snapshot_frame: flush buffer type mismatch for procedure")
                .timer_ops = timers.as_register_ops();
            self.flush_now(&mut **flush_buffer);
        }
        seq
    }
}

/// Synchronous drain of the producing entity's pending changes onto
/// the durable WAL path, invoked before each output frame is written.
type FlushBeforeEmit<'a> = Box<dyn FnMut() + 'a>;

/// Per-invocation emit sink, constructed fresh inside each `dispatch`
/// call. Holds an upgraded `Arc<StreamInstance>` for the duration of the
/// run; if the instance has been dropped, every emit returns
/// `UnknownChannel`.
struct EntityEmitSink<'a> {
    /// The Entity this invocation belongs to. An emit it cannot deliver is
    /// held against this slot and this slot is handed no further work — the
    /// backpressure the parked producing thread used to express.
    producer: &'a Arc<EntitySlot>,
    output_channel_id: Option<ChannelId>,
    output_channel_name: Option<&'a str>,
    /// `.keyed()` opt-in for the output channel. Non-keyed channels take
    /// the pre-identity emit path byte-for-byte (no stamps, no extra work).
    keyed: bool,
    /// `flz.entity` stamp value for keyed emits.
    entity_label: &'a str,
    /// Durable change seq, read at stamp time (after flush_pending, so
    /// the just-applied changes are counted) for the `flz.seq` stamp.
    change_seq: &'a AtomicU64,
    /// Emit ordinal within this invocation; second half of `flz.seq`.
    emit_ordinal: u32,
    instance: Option<Arc<StreamInstance>>,
    stream: Arc<FlezhaStream>,
    /// `Some` for persistence-enabled entities with the flush-before-emit
    /// knob on. Called at the top of every emit so this run's just-applied
    /// changes are durably shipped before the output frame leaves.
    flush_before_emit: Option<FlushBeforeEmit<'a>>,
    /// Input-log offset of the invocation's input, inherited by
    /// inter-procedure emits so downstream Entities' effect records carry
    /// the origin offset.
    consumed_offset: Option<u64>,
    /// `Some` in effectively-once tier mode: plugin emits are pushed here
    /// (for the invocation's atomic effect record) instead of being written
    /// to the primary stream.
    effect_outputs: Option<&'a RefCell<Vec<crate::entity::flusher::OutputRecord>>>,
}

impl<'a> EntityEmitSink<'a> {
    /// Drain pending changes onto the WAL path before an output frame is
    /// written. No-op for non-persistent / `NoChange` operatives.
    fn flush_pending(&mut self) {
        if let Some(flush) = self.flush_before_emit.as_mut() {
            flush();
        }
    }

    /// `UnknownChannel` carrying the cached output channel name (or
    /// `<unknown>` when unset), for the resolve failure paths.
    fn unknown_channel(&self) -> ProcedureError {
        ProcedureError::UnknownChannel {
            channel: self
                .output_channel_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| "<unknown>".to_string()),
        }
    }

    /// Resolve the channel descriptor + StreamInstance for the cached
    /// output channel. Returns `UnknownChannel` if either has been
    /// dropped or is unset.
    fn resolve_channel(&self) -> Result<(Arc<StreamInstance>, ChannelId), ProcedureError> {
        let cid = self
            .output_channel_id
            .ok_or_else(|| self.unknown_channel())?;
        let instance = self
            .instance
            .as_ref()
            .ok_or_else(|| self.unknown_channel())?
            .clone();
        Ok((instance, cid))
    }

    /// Append the output-identity stamps for a keyed-channel plugin frame.
    /// Runs after user metadata is merged so stamps can never be
    /// clobbered; no-op (and no cost) for non-keyed channels.
    fn stamp_identity(&mut self, metadata: &mut HashMap<String, String>, key: Option<String>) {
        if self.keyed {
            apply_identity_stamps(
                metadata,
                self.entity_label,
                self.change_seq.load(Ordering::Relaxed),
                &mut self.emit_ordinal,
                key,
            );
        }
    }

    /// Plugin-sink tail: serialize once inside `catch_unwind` and forward the
    /// bytes through the plugin-wire framing path. A serializer panic surfaces
    /// as `ProcedureError::PluginSerializerPanic` to the producing Procedure;
    /// the dispatcher thread is unaffected.
    fn emit_plugin_sink(
        &mut self,
        instance: &StreamInstance,
        descriptor: &crate::channel::ChannelDescriptor,
        sink: &crate::channel::PluginSink,
        value: &Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), ProcedureError> {
        let serializer = Arc::clone(&sink.serializer);
        let value_for_panic = Arc::clone(value);
        let bytes_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (serializer)(value_for_panic.as_ref())
        }));
        let bytes = match bytes_result {
            Ok(b) => b,
            Err(panic) => {
                return Err(ProcedureError::PluginSerializerPanic {
                    channel: descriptor.name.to_string(),
                    message: crate::procedure::error::format_panic(panic),
                });
            }
        };

        if let Some(cell) = self.effect_outputs {
            push_effect_output(cell, &descriptor.name, key, meta, bytes);
            return Ok(());
        }

        let mut metadata = channel_metadata(meta, &descriptor.name);
        self.stamp_identity(&mut metadata, key);

        send_plugin_frame(&self.stream, &instance.instance_id, &bytes, &metadata);
        Ok(())
    }
}

impl<'a> EmitSink for EntityEmitSink<'a> {
    /// Fan out to the consumer mailboxes, then to the plugin sink if the
    /// channel has one. The plugin-bound terminal is the same path with an
    /// empty consumer list, which is why there is no second arm for it.
    fn emit(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), ProcedureError> {
        self.flush_pending();
        let (instance, cid) = self.resolve_channel()?;
        let descriptor =
            instance
                .topology
                .channel(cid)
                .ok_or_else(|| ProcedureError::UnknownChannel {
                    channel: format!("<channel id {}>", cid),
                })?;
        // An inter-procedure emit is never a replay member: the downstream
        // Procedure receives the corrected outputs already in order, and a
        // slice it could never see the end of would hold its drain forever.
        flz_core::routing::route_to_consumers(
            &instance,
            descriptor,
            &value,
            &self.stream,
            self.consumed_offset,
            false,
            Some(self.producer),
        );
        if let Some(sink) = descriptor.plugin_sink.as_ref() {
            self.emit_plugin_sink(&instance, descriptor, sink, &value, meta, key)?;
        }
        Ok(())
    }

    fn channel_name(&self) -> &str {
        self.output_channel_name.unwrap_or("<unset>")
    }

    fn has_plugin_sink(&self) -> bool {
        // Resolving the descriptor each time keeps the trait surface small.
        // `route` only asks when the emit carries metadata, which is rare, so
        // the ordinary emit pays nothing for this lookup.
        self.instance
            .as_ref()
            .and_then(|inst| {
                self.output_channel_id
                    .and_then(|cid| inst.topology.channel(cid))
            })
            .map(|d| d.plugin_sink.is_some())
            .unwrap_or(false)
    }

    fn is_keyed(&self) -> bool {
        self.keyed
    }
}

/// Emit sink for the workflow sequential driver. Inter-procedure output is
/// **collected** for handoff to the next stage at the barrier; plugin-bound
/// output is serialized and routed to the wire via the `WorkflowCollectSink`.
/// Constructed fresh per `run_collecting` call, so it never threads through
/// the stream push-dispatch mailbox path.
struct WorkflowCollectAdapter<'a> {
    sink: &'a mut dyn WorkflowCollectSink,
    output_channel_id: Option<ChannelId>,
    output_channel_kind: ChannelKind,
    output_channel_name: Option<&'a str>,
    topology: &'a OperativeTopology,
    /// `Some` under the effectively-once tier: plugin emits (keyed
    /// included) are captured into the invocation's atomic effect record
    /// instead of the metadata-less workflow plugin wire.
    effect_outputs: Option<&'a RefCell<Vec<crate::entity::flusher::OutputRecord>>>,
}

impl<'a> WorkflowCollectAdapter<'a> {
    fn unknown_channel(&self) -> ProcedureError {
        ProcedureError::UnknownChannel {
            channel: self
                .output_channel_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| "<unknown>".to_string()),
        }
    }

    /// Serialize a value with the producer-edge channel's plugin sink and
    /// route the bytes: into the invocation's effect record under the tier,
    /// else to the workflow plugin wire.
    fn route_plugin(
        &mut self,
        value: &Arc<dyn Any + Send + Sync>,
        key: Option<String>,
        meta: Option<HashMap<String, String>>,
    ) -> Result<(), ProcedureError> {
        let cid = self
            .output_channel_id
            .ok_or_else(|| self.unknown_channel())?;
        let descriptor =
            self.topology
                .channel(cid)
                .ok_or_else(|| ProcedureError::UnknownChannel {
                    channel: format!("<channel id {}>", cid),
                })?;
        let serializer = descriptor
            .plugin_sink
            .as_ref()
            .map(|s| &s.serializer)
            .ok_or_else(|| {
                ProcedureError::Custom(format!(
                    "workflow plugin channel `{}` is missing a serializer",
                    descriptor.name
                ))
            })?;
        let bytes = serializer(&**value);
        if let Some(cell) = self.effect_outputs {
            push_effect_output(cell, &descriptor.name, key, meta, bytes);
        } else {
            self.sink.emit_plugin_bytes(&descriptor.name, bytes);
        }
        Ok(())
    }
}

impl<'a> WorkflowCollectAdapter<'a> {
    /// The workflow plugin wire (`emit_plugin_bytes`) carries no metadata,
    /// so `.keyed()` identity stamps cannot ride it. Reject rather than
    /// silently dropping the identity contract — except under the
    /// effectively-once tier, where emits ride the effect record (which
    /// carries the key durably).
    fn is_plugin_bound(&self) -> bool {
        matches!(self.output_channel_kind, ChannelKind::PluginBound)
    }

    fn reject_key(&self, key: &Option<String>) -> Result<(), ProcedureError> {
        if self.effect_outputs.is_none() && key.is_some() {
            return Err(ProcedureError::Custom(format!(
                "keyed output identity is not supported on workflow operatives \
                 (channel `{}`): the workflow plugin wire carries no metadata",
                self.channel_name()
            )));
        }
        Ok(())
    }
}

impl<'a> EmitSink for WorkflowCollectAdapter<'a> {
    fn emit(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), ProcedureError> {
        self.reject_key(&key)?;
        if self.has_plugin_sink() {
            self.route_plugin(&value, key, meta)?;
        }
        // The one place a workflow channel's shape still matters: a
        // plugin-bound channel has no consumer to hand a stage batch to, so
        // collecting there would seed the next stage with a terminal output.
        if !self.is_plugin_bound() {
            self.sink.collect(value);
        }
        Ok(())
    }

    fn channel_name(&self) -> &str {
        self.output_channel_name.unwrap_or("<workflow>")
    }

    fn has_plugin_sink(&self) -> bool {
        self.output_channel_id
            .and_then(|cid| self.topology.channel(cid))
            .map(|d| d.plugin_sink.is_some())
            .unwrap_or(false)
    }

    fn is_keyed(&self) -> bool {
        self.output_channel_id
            .and_then(|cid| self.topology.channel(cid))
            .map(|d| d.keyed)
            .unwrap_or(false)
    }
}

/// Apply the channel's `OverflowPolicy` when pushing onto an Entity's
/// bounded mailbox. Bumps the appropriate per-Entity counter on every
/// outcome and submits a drain task on success / DropOldest. On Block,
/// parks the calling thread on the slot's `Waiters` and retries
/// until the push succeeds (Decision 3).
/// Build the emit wire metadata: drop any caller-supplied reserved
/// `channel` key, then stamp the descriptor's channel name.
fn channel_metadata(
    meta: Option<HashMap<String, String>>,
    channel: &str,
) -> HashMap<String, String> {
    let mut metadata = meta.unwrap_or_default();
    metadata.remove("channel");
    metadata.insert("channel".to_string(), channel.to_string());
    metadata
}

/// Output-identity stamps for one keyed-channel plugin frame: the emitting
/// Entity (`flz.entity`), the replay-stable output sequence
/// `<change_seq>.<emit_ordinal>` (`flz.seq`), and the caller's natural key
/// (`flz.key`). Applied after user metadata so stamps win; advances the
/// per-invocation emit ordinal. Shared with the test harness so
/// `replay_emits_equal` compares the same stamp bytes the wire carries.
pub(crate) fn apply_identity_stamps(
    metadata: &mut HashMap<String, String>,
    entity_label: &str,
    change_seq: u64,
    emit_ordinal: &mut u32,
    key: Option<String>,
) {
    metadata.insert("flz.entity".to_string(), entity_label.to_string());
    metadata.insert(
        "flz.seq".to_string(),
        format!("{}.{}", change_seq, emit_ordinal),
    );
    *emit_ordinal += 1;
    if let Some(k) = key {
        metadata.insert("flz.key".to_string(), k);
    }
}

/// Tier-mode capture (shared by the stream emit sink and the workflow
/// collect adapter): push one plugin output into the invocation's atomic
/// effect record instead of any wire path. Key-sorted user metadata keeps
/// the record bytes replay-deterministic (HashMap iteration order is not);
/// the reserved `channel` key is dropped like `channel_metadata` does, and
/// identity stamps are derived controller-side from the durable record.
fn push_effect_output(
    cell: &RefCell<Vec<crate::entity::flusher::OutputRecord>>,
    channel: &str,
    key: Option<String>,
    meta: Option<HashMap<String, String>>,
    payload: Vec<u8>,
) {
    let mut metadata: Vec<(String, String)> = meta
        .unwrap_or_default()
        .into_iter()
        .filter(|(k, _)| k != "channel")
        .collect();
    metadata.sort();
    cell.borrow_mut()
        .push(crate::entity::flusher::OutputRecord {
            channel: channel.to_string(),
            key: key.unwrap_or_default(),
            metadata,
            payload,
        });
}

const ENV_FLUSH_BEFORE_EMIT: &str = "FLEZHA_FLUSH_BEFORE_EMIT";

/// Whether flush-before-emit is enabled for persistence-enabled entities.
/// Defaults to ON (durability over latency); set `FLEZHA_FLUSH_BEFORE_EMIT`
/// to `0`/`false`/`off`/`no` to revert to the inline-emit fast path.
fn flush_before_emit_enabled() -> bool {
    match std::env::var(ENV_FLUSH_BEFORE_EMIT) {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
    }
}
use flz_core::routing::send_plugin_frame;

/// Answers a handout exactly once, whatever becomes of the worker holding it.
///
/// Per-entity exclusivity IS the handout: an activation that is never answered
/// retires its entity for the life of the process, silently. `Drop` closes
/// every way that could happen — an early return, a `?`, a panic unwinding
/// past the dispatcher's own guards — by abandoning, which puts the items this
/// worker did not finish back untouched and makes the entity pollable again.
struct Handout<'a> {
    core: &'a crate::runner::poll_core::PollCore,
    token: u64,
    /// What the worker has not finished. The item being dispatched stays here
    /// until it resolves — the dispatch runs on a shallow clone — so an unwind
    /// mid-item re-hands it rather than losing it.
    remaining: std::collections::VecDeque<crate::runner::poll_core::ItemAttempt>,
    answered: bool,
}

impl<'a> Handout<'a> {
    fn new(
        core: &'a crate::runner::poll_core::PollCore,
        token: u64,
        items: Vec<crate::runner::poll_core::ItemAttempt>,
    ) -> Self {
        Self {
            core,
            token,
            remaining: items.into(),
            answered: false,
        }
    }

    fn next_item(&self) -> Option<crate::runner::poll_core::ItemAttempt> {
        self.remaining.front().cloned()
    }

    fn item_done(&mut self) {
        self.remaining.pop_front();
    }

    /// The head failed retryably: it and the untouched tail ride the ladder.
    fn retry(mut self, head: crate::runner::poll_core::ItemAttempt, error: String) {
        self.remaining.pop_front();
        let mut rest = Vec::with_capacity(self.remaining.len() + 1);
        rest.push(head);
        rest.extend(self.remaining.drain(..));
        self.answered = true;
        self.core.retry_after_failure(self.token, rest, error);
    }

    fn complete(mut self) {
        self.answered = true;
        self.core.complete(self.token);
    }

    /// End the batch early and hand the untouched tail back. Only for items
    /// this worker did NOT run — re-handing one it already ran would execute
    /// it twice.
    fn hand_back(&mut self) {
        self.answered = true;
        let items: Vec<_> = self.remaining.drain(..).collect();
        self.core.abandon(self.token, items);
    }
}

impl Drop for Handout<'_> {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        tracing::error!(
            target: "flezha::dispatch",
            token = self.token,
            unfinished = self.remaining.len(),
            panicking = std::thread::panicking(),
            "a poll worker left an activation unanswered; abandoning it so its entity keeps serving"
        );
        self.hand_back();
    }
}

/// The connection one item rides — inherited by any processing-time timer it
/// arms, so a firing's plugin emits leave the same way an input's would. A
/// `CloseInterval` has no connection of its own and takes the live secondary;
/// with none live the close cannot ship at all.
fn item_stream(item: &EntityItem) -> Option<Arc<FlezhaStream>> {
    match item {
        EntityItem::Input(payload) => Some(Arc::clone(&payload.stream)),
        EntityItem::Timer(fire) => Some(Arc::clone(&fire.stream)),
        EntityItem::CloseInterval => flz_core::state_sink::secondary_stream(),
    }
}

/// One item through the typed dispatch, holding the slot's three locks in
/// dispatch order (state, then flush_buffer, then timers).
fn dispatch_locked(
    slot: &Arc<EntitySlot>,
    work: crate::runner::poll_core::ItemAttempt,
) -> crate::entity::ItemDisposition {
    let mut state_guard = slot.state.lock_recover();
    let mut buffer_guard = slot.flush_buffer.lock_recover();
    let mut timer_guard = slot.timers.lock_recover();
    let instance_weak = Weak::clone(&slot.instance);
    slot.dispatcher.dispatch_item(
        work,
        slot,
        &mut state_guard,
        &mut buffer_guard,
        &mut timer_guard,
        &instance_weak,
    )
}

/// What a completed item owes outside the locks: arming can fire an
/// already-due timer straight back onto this slot's mailbox, and the drain
/// that would run it needs them released. Answers whether the batch must stop
/// here — this item's emit could not be delivered, and the parked producer
/// this replaces stopped here too (its thread never returned into the batch),
/// so the untouched tail goes back to the head of the entity's line and runs
/// once the push lands. Without that the holdback would grow by a whole batch.
fn complete_item(slot: &Arc<EntitySlot>, ops: &[TimerOp], stream: &Arc<FlezhaStream>) -> bool {
    crate::entity::timer::apply_ops(slot, ops, stream);
    flz_core::timer::arm_interval_deadline(slot);
    crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
    slot.push_deferred.load(Ordering::Acquire)
}

/// The Entity work one activation carries, or `None` when there is none to run
/// here: a workflow step carries its own reply, so it releases this worker the
/// moment it answers (a retry backoff is timed by the driver at its barrier,
/// never slept here), and command/prepare payloads are host-only — a typed
/// worker never receives one, because typed and host classes do not share a
/// process.
fn entity_work(
    payload: crate::runner::poll_core::EnginePayload,
) -> Option<(Arc<EntitySlot>, Vec<crate::runner::poll_core::ItemAttempt>)> {
    use crate::runner::poll_core::EnginePayload;
    match payload {
        EnginePayload::Work { slot, items } => Some((slot, items)),
        EnginePayload::WorkflowStep(step) => {
            flz_core::workflow::step::run_workflow_step(step);
            None
        }
        _ => None,
    }
}

/// Run one activation: one attempt per item through the typed dispatch, then
/// either complete the batch or hand the failed head (and unprocessed tail)
/// back to the engine's ladder. The engine owns handout exclusivity,
/// admission, and the drain-tail protocol (`runner::poll_core`).
pub(super) fn run_activation(
    core: &crate::runner::poll_core::PollCore,
    act: crate::runner::poll_core::EngineActivation,
) {
    use crate::entity::ItemDisposition;
    use crate::runner::poll_core::{EngineActivation, ItemAttempt};

    let EngineActivation { token, payload } = act;
    let Some((slot, items)) = entity_work(payload) else {
        return;
    };
    let mut handout = Handout::new(core, token, items);
    while let Some(work) = handout.next_item() {
        // Returning with no connection is safe only because the guard
        // abandons on the way out.
        let Some(stream) = item_stream(&work.item) else {
            return;
        };
        let (now, attempt, is_final) = (work.now, work.attempt, work.is_final);
        match dispatch_locked(&slot, work) {
            ItemDisposition::Done { ops } => {
                handout.item_done();
                if complete_item(&slot, &ops, &stream) {
                    handout.hand_back();
                    return;
                }
            }
            ItemDisposition::RetryLater { item, error } => {
                handout.retry(
                    ItemAttempt {
                        item,
                        now,
                        attempt,
                        is_final,
                    },
                    error,
                );
                return;
            }
        }
    }
    handout.complete();
}

/// Whether a Procedure's Entities join the background flusher's drain pass.
/// One predicate, applied where an Entity is built AND where an invocation has
/// to decide whether its records will ever be drained for it: a Procedure with
/// nothing to persist has no entry, so anything timers give it to persist must
/// be drained on the spot.
fn flusher_registered<S: Procedure>(effectively_once: bool) -> bool {
    TypeId::of::<S::Change>() != TypeId::of::<NoChange>() || effectively_once
}

/// Join the background flusher's drain pass — unless this Procedure would
/// never give it anything. A `Change == NoChange` class registers nothing,
/// because registering one would spawn the drain thread for a program that
/// will never produce a record; its EntitySlot still carries an (always empty)
/// flush buffer so the dispatcher constructs a `StateHandle<'_, S>` uniformly.
/// Effectively-once operatives always register: their effect records (outputs
/// + consumed-offset, possibly with no change) drain through the same buffer.
fn register_flusher<S: Procedure>(
    parts: &flz_core::entity::SlotParts,
    flush_buffer: &Arc<Mutex<Box<dyn FlushableBuffer>>>,
    persistence_enabled: bool,
    effectively_once: bool,
) -> Option<crate::entity::flusher::FlusherEntryId> {
    flusher_registered::<S>(effectively_once).then(|| {
        EntityFlusher::instance().register(
            Arc::clone(&parts.entity_identity),
            Arc::clone(flush_buffer),
            persistence_enabled,
            Arc::clone(&parts.change_seq),
            crate::entity::header::header_template_for::<S>(),
            Arc::clone(&parts.push_deferred),
        )
    })
}

/// The dispatcher one Entity is served by: the producer- and consumer-edge
/// facts resolved out of the frozen topology once, so no invocation pays a
/// channel lookup.
fn build_dispatcher<S: Procedure>(
    args: &BuildEntityArgs<'_>,
    topology: &OperativeTopology,
    procedure_desc: &crate::operative::topology::ProcedureDescriptor,
    parts: &flz_core::entity::SlotParts,
) -> Arc<dyn ProcedureDispatcher> {
    let output_channel = procedure_desc
        .output_channel_id
        .and_then(|id| topology.channel(id));
    // Retry-then-park policy rides the procedure's input channel; an
    // unwired input keeps the defaults (park still works, channel unnamed).
    // A fan-in node has several input channels; the first wired one names
    // its DLQ entries and carries its retry ladder.
    let input_channel = procedure_desc
        .input_channel_ids
        .first()
        .and_then(|&id| topology.channel(id));
    Arc::new(ProcedureDispatcherImpl::<S>::new(
        args.procedure_id,
        EdgeFacts {
            output_channel_id: procedure_desc.output_channel_id,
            output_channel_kind: output_channel
                .map(|c| c.kind)
                .unwrap_or(ChannelKind::InterProcedure),
            output_channel_name: output_channel.map(|c| Arc::clone(&c.name)),
            output_channel_keyed: output_channel.map(|c| c.keyed).unwrap_or(false),
            input_channel_name: input_channel.map(|c| Arc::clone(&c.name)),
            retry: input_channel.map(|c| c.retry).unwrap_or_default(),
        },
        Arc::clone(&parts.entity_label),
        parts.entity_key.clone(),
        FlushWiring {
            entity_identity: Arc::clone(&parts.entity_identity),
            change_seq: Arc::clone(&parts.change_seq),
            header: crate::entity::header::header_template_for::<S>(),
            persistence_enabled: args.persistence_enabled,
            effectively_once: topology.effectively_once,
            checkpoint: topology.ack_granularity.checkpoint(),
            correction: topology.correction,
        },
    )) as Arc<dyn ProcedureDispatcher>
}

/// Build one `EntitySlot` for a Procedure. Captures S statically; the
/// channel builder collects this closure into `ProcedureEntry.build_entity`
/// during `StreamOperative::channels` and `build_topology` invokes it once per
/// entity at operative-instance startup.
pub fn build_entity_slot<S: Procedure>(args: BuildEntityArgs<'_>) -> Arc<EntitySlot> {
    // The slot's rounds are served by the typed layer's poll workers; make
    // sure they exist before the first input can be scheduled.
    crate::operative::poll_pool::ensure_poll_workers();
    // v1 cold-start path: `init_payload` is `None`. The controller change
    // wires the warm-restart path through `init_entity_with_payload`
    // and calls a richer constructor.
    let typed_state = EntityState::<S> {
        procedure: S::default(),
        state: S::init_state(args.entity_key, None),
    };

    let topology = Arc::clone(&args.topology);
    let procedure_desc = topology
        .procedure(args.procedure_id)
        .expect("build_entity_slot: procedure id missing in topology");

    let flush_buffer: Arc<Mutex<Box<dyn FlushableBuffer>>> = Arc::new(Mutex::new(Box::new(
        TypedBuffer::<S>::default(),
    )
        as Box<dyn FlushableBuffer>));

    let parts = flz_core::entity::slot_parts(
        args.instance_id.as_ref(),
        procedure_desc.name.as_ref(),
        args.entity_key,
    );
    let dispatcher = build_dispatcher::<S>(&args, &topology, procedure_desc, &parts);

    let flusher_entry_id = register_flusher::<S>(
        &parts,
        &flush_buffer,
        args.persistence_enabled,
        topology.effectively_once,
    );

    let mailbox_capacity = procedure_desc.mailbox_capacity;
    flz_core::entity::assemble_slot(
        args,
        parts,
        Box::new(typed_state),
        flush_buffer,
        dispatcher,
        flusher_entry_id,
        mailbox_capacity,
    )
}

#[cfg(test)]
#[path = "dispatcher_tests.rs"]
mod tests;
