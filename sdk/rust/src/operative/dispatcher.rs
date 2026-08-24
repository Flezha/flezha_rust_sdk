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
use crate::entity::flusher::{EntityFlusher, FlushableBuffer, TypedBuffer};
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

/// Typed dispatcher. Caches the producer-edge output channel id at
/// construction so emit pays zero name-lookup cost on the hot path.
pub struct ProcedureDispatcherImpl<S: Procedure> {
    procedure_id: ProcedureId,
    output_channel_id: Option<ChannelId>,
    output_channel_kind: ChannelKind,
    output_channel_name: Option<Arc<str>>,
    /// Input-channel name stamped on dead-letter entries; `None` for
    /// procedures without a wired input channel.
    input_channel_name: Option<Arc<str>>,
    /// Retry-then-park policy of the input channel (defaults when unwired).
    retry: RetryPolicy,
    /// `.keyed()` flag of the producer-edge channel, cached like the kind
    /// so the emit sink pays zero topology lookups on the hot path.
    output_channel_keyed: bool,
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
    #[allow(clippy::too_many_arguments)]
    fn new(
        procedure_id: ProcedureId,
        output_channel_id: Option<ChannelId>,
        output_channel_kind: ChannelKind,
        output_channel_name: Option<Arc<str>>,
        output_channel_keyed: bool,
        input_channel_name: Option<Arc<str>>,
        retry: RetryPolicy,
        entity_label: Arc<str>,
        entity_key: Option<Arc<str>>,
        flush: FlushWiring,
    ) -> Self {
        Self {
            procedure_id,
            output_channel_id,
            output_channel_kind,
            output_channel_name,
            output_channel_keyed,
            input_channel_name,
            retry,
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
                inner: changes,
                ..Default::default()
            }
        };
        crate::entity::flusher::flush_one(
            &mut buf,
            &self.flush.entity_identity,
            &self.flush.change_seq,
            &self.flush.header,
            self.flush.persistence_enabled,
        );
    }

    /// Run ONE ATTEMPT of one invocation — `run` on an input, or `on_timer`
    /// on a firing — through the emit sink, the timer session and the durable
    /// record it produces. The two differ only in the call into the author's
    /// code; everything a failure has to be contained by is shared, which is
    /// what makes a poison timer park exactly like a poison input.
    ///
    /// The retry ladder's counting, backoff and re-hand live in the engine
    /// (`runner::poll_core`): a retryable non-final failure returns
    /// `Some(error)` and leaves nothing shipped; a non-retryable or final
    /// failure parks here (the park rides this entity's buffer) and returns
    /// `None`, consuming the item.
    ///
    /// `now` is the invocation's `ctx.now()`: the declared Clock's value for an
    /// input, the scheduled timestamp for a firing.
    #[allow(clippy::too_many_arguments)]
    fn invoke_once(
        &self,
        what: Invocation<S>,
        now: u64,
        attempt: u32,
        is_final: bool,
        parked_payload_bytes: Option<Vec<u8>>,
        retained_input: Option<&Arc<S::Input>>,
        typed_state: &mut EntityState<S>,
        typed_buffer: &mut TypedBuffer<S>,
        live_timers: &mut PendingTimers,
        committed_ops: &mut Vec<TimerOp>,
        stream: &Arc<FlezhaStream>,
        instance: &Weak<StreamInstance>,
    ) -> Option<String> {
        let consumed_offset = what.consumed_offset();
        let replay = what.replay();
        // Share the change buffer between `StateHandle::apply` and the
        // flush-before-emit hook. Both run on this single dispatch thread
        // inside one invocation, so the `RefCell` borrows never overlap.
        // A previous failed attempt's changes carry over: its state
        // mutations stayed applied (at-least-once), so durable state must
        // not diverge from memory.
        let mut carried = std::mem::take(&mut typed_buffer.inner);
        carried.append(&mut typed_buffer.pending_attempt);
        let buffer_cell = RefCell::new(carried);

        // Flush-before-emit: drain this entity's pending changes to the
        // durable WAL path before the sink writes the matching output
        // frame, so change N reaches the controller's WAL before N's
        // output reaches Kafka. Gated on `persistence_enabled` (and the
        // `FLEZHA_FLUSH_BEFORE_EMIT` knob) so `NoChange` / non-persistent
        // operatives keep the zero-cost inline emit. Moot in tier mode:
        // changes and outputs ship in ONE effect record, so there is no
        // separate output frame to order against.
        let flush_before_emit: Option<FlushBeforeEmit> = if !self.flush.effectively_once
            && self.flush.persistence_enabled
            && flush_before_emit_enabled()
        {
            let identity = Arc::clone(&self.flush.entity_identity);
            let seq = Arc::clone(&self.flush.change_seq);
            let header = self.flush.header;
            let cell = &buffer_cell;
            Some(Box::new(move || {
                let mut buf = TypedBuffer::<S> {
                    inner: std::mem::take(&mut *cell.borrow_mut()),
                    ..Default::default()
                };
                crate::entity::flusher::flush_one(&mut buf, &identity, &seq, &header, true);
            }))
        } else {
            None
        };

        let invocation_seq = self.flush.change_seq.load(Ordering::Relaxed);

        // Tier mode: plugin emits are captured here (in emit order) instead
        // of leaving on the primary stream, then combined with the run's
        // changes + consumed-offset into one atomic effect record.
        let effect_outputs = self.effect_capture();

        let mut sink = EntityEmitSink {
            output_channel_id: self.output_channel_id,
            output_channel_name: self.output_channel_name.as_deref(),
            keyed: self.output_channel_keyed,
            entity_label: &self.entity_label,
            change_seq: &self.flush.change_seq,
            emit_ordinal: 0,
            instance: instance.upgrade(),
            stream: Arc::clone(stream),
            flush_before_emit,
            consumed_offset,
            effect_outputs: effect_outputs.as_ref(),
        };

        // One attempt (`dead-letter-queue` ladder, engine-driven): a failing
        // invocation — returned error or contained panic — is re-handed by
        // the engine up to the input channel's `retry_max`; a
        // `ProcedureError::NonRetryable` skips the remaining budget and
        // parks here, as does a failing final attempt.
        // Captured before the value moves into the run: a park still has to
        // record which timer fired.
        let firing = what.firing();
        // Only an attempt that succeeded may close the interval or leave
        // timers registered; a failed attempt's `ctx` and its timer session
        // are discarded with it.
        let mut checkpointed = false;
        let mut timer_ops: Vec<TimerOp> = Vec::new();

        // The removal is op 0 for a firing, so it rides the same durable
        // record as whatever `on_timer` goes on to apply and emit.
        let session = self.timers.session(live_timers, now, firing);

        let mut ctx =
            ProcedureCtx::new(&mut sink, now, invocation_seq, &self.flush.entity_identity)
                .with_entity_key(self.entity_key.as_deref())
                .during_replay(replay)
                .with_timers(&session);
        let handle = StateHandle::<S>::new(&mut typed_state.state, &buffer_cell);

        // Contain a procedure panic so it can't poison the held `state` lock.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match what {
            Invocation::Input { input, .. } => typed_state.procedure.run(input, handle, &mut ctx),
            Invocation::Timer { fired_at, kind } => typed_state
                .procedure
                .on_timer(fired_at, kind, handle, &mut ctx),
        }));

        let park_error: Option<String> = match outcome {
            Ok(Ok(())) => {
                checkpointed = ctx.checkpointed();
                timer_ops = session.into_inner().commit(live_timers);
                None
            }
            failed => {
                let (error, retryable) = match failed {
                    Ok(Err(err)) => {
                        let retryable = !err.is_non_retryable();
                        (err.to_string(), retryable)
                    }
                    // A panic is retryable by default — the author never got
                    // to classify it (design D5).
                    Err(panic) => (crate::procedure::error::format_panic(panic), true),
                    Ok(Ok(())) => unreachable!("handled above"),
                };
                // A failed attempt's captured tier outputs must not ship
                // (the invocation did not complete); changes it already
                // applied to the in-memory state stay buffered so durable
                // state cannot diverge from memory.
                if let Some(cell) = effect_outputs.as_ref() {
                    cell.borrow_mut().clear();
                }
                if retryable && !is_final {
                    drop(sink);
                    // Keep the attempt's changes out of the flusher's reach
                    // until the item resolves — the next attempt re-takes
                    // them.
                    typed_buffer.pending_attempt = buffer_cell.into_inner();
                    return Some(error);
                }
                // A firing that exhausted its budget still records the
                // removal: a poison timer must park once, not fire forever —
                // the same reason a park closes a checkpoint interval.
                if firing.is_some() {
                    timer_ops = self
                        .timers
                        .session(live_timers, now, firing)
                        .into_inner()
                        .commit(live_timers);
                }
                Some(error)
            }
        };

        drop(sink);
        let dlq = if let Some(error) = park_error {
            crate::entity::park::PARKED_TOTAL.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[flezha] procedure {} parking poison payload to DLQ after {} attempt(s): {}",
                S::name(),
                attempt,
                error
            );
            Some(crate::entity::flusher::DlqEntryBody {
                channel: self
                    .input_channel_name
                    .as_deref()
                    .unwrap_or("<unwired>")
                    .to_string(),
                error,
                attempts: attempt,
                payload: parked_payload_bytes.or_else(|| {
                    retained_input
                        .and_then(|arc| crate::entity::park::extract_payload_bytes(&**arc))
                }),
            })
        } else {
            None
        };

        committed_ops.extend(timer_ops.iter().copied());

        if let Some(outputs_cell) = effect_outputs {
            // Tier mode: this invocation's changes + outputs + consumed
            // offset become ONE effect entry, drained by the flusher as one
            // `entity_effect_record` frame (never coalesced). An
            // effect-free invocation contributes nothing durable — unless it
            // parked, in which case the entry MUST ship so the dead-letter
            // park and the consumed-offset advance stay atomic.
            let shipped = typed_buffer.staged.fold(
                self.flush.checkpoint,
                crate::entity::flusher::InvocationEffects {
                    changes: buffer_cell.into_inner(),
                    timers: timer_ops,
                    outputs: outputs_cell.into_inner(),
                    offset: consumed_offset,
                    event_time: now,
                    checkpointed,
                    parked: dlq.is_some(),
                },
                crate::util::wall_now_ms(),
            );
            if let Some(record) = shipped {
                typed_buffer
                    .effects
                    .push(crate::entity::flusher::EffectEntry::from_shipped(
                        record, dlq,
                    ));
            }
        } else {
            // Return any changes not yet drained by flush-before-emit (or all
            // of them when the hook is disabled) to the buffer the background
            // flusher drains. `change_seq` and the move-out drain guarantee
            // each change is shipped exactly once.
            typed_buffer.inner = buffer_cell.into_inner();
            // Outside the tier there is no effect record to ride: the pending
            // set's mutations drain as their own `format_id = 3` record.
            typed_buffer.timer_ops.extend(timer_ops);
            // A `NoChange` Procedure has no flusher entry — there was nothing
            // to drain until timers gave it something. Drain here rather than
            // register every Entity with the flusher, which would spawn its
            // thread for programs that will never produce a record.
            if !typed_buffer.timer_ops.is_empty()
                && !flusher_registered::<S>(self.flush.effectively_once)
            {
                crate::entity::flusher::flush_one(
                    typed_buffer,
                    &self.flush.entity_identity,
                    &self.flush.change_seq,
                    &self.flush.header,
                    self.flush.persistence_enabled,
                );
            }
            if let Some(entry) = dlq {
                // Tier-off park: ship the park frame and hold this entity
                // (the drain thread) until the controller acks the DLQ
                // append as durable — park-then-advance, never a drop.
                crate::entity::park::park_tier_off(
                    &self.flush.entity_identity,
                    &self.entity_label,
                    consumed_offset,
                    entry,
                );
            }
        }
        None
    }
}

/// What one invocation runs. The retry ladder, the emit sink, the effect
/// record and the DLQ park are the same machinery either way — only the call
/// into the author's code differs, which is why they share one code path.
enum Invocation<S: Procedure> {
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

impl<S: Procedure> Invocation<S> {
    fn consumed_offset(&self) -> Option<u64> {
        match self {
            Invocation::Input {
                consumed_offset, ..
            } => *consumed_offset,
            Invocation::Timer { .. } => None,
        }
    }

    fn replay(&self) -> bool {
        matches!(self, Invocation::Input { replay: true, .. })
    }

    /// The firing this invocation is, if it is one — what seeds the timer
    /// session and what a park still has to record.
    fn firing(&self) -> Option<(TimerKind, u64)> {
        match self {
            Invocation::Timer { fired_at, kind } => Some((*kind, *fired_at)),
            Invocation::Input { .. } => None,
        }
    }
}

impl<S: Procedure> ProcedureDispatcher for ProcedureDispatcherImpl<S> {
    fn dispatch_item(
        &self,
        work: crate::runner::poll_core::WorkItem,
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

        let crate::runner::poll_core::WorkItem {
            item,
            now,
            attempt,
            is_final,
        } = work;

        let mut committed: Vec<TimerOp> = Vec::new();
        match item {
            // Consumed by the batch assembler, which closes the interval
            // without an invocation — it never reaches dispatch.
            EntityItem::CloseInterval => ItemDisposition::Done { ops: committed },
            EntityItem::Timer(fire) => {
                // Assembly validated the firing, but the set is the
                // authority: re-check under the held locks — a restore
                // between handout and dispatch can rebuild it.
                if !timers.contains(fire.kind, fire.scheduled_at) {
                    return ItemDisposition::Done { ops: committed };
                }
                let retry_error = self.invoke_once(
                    Invocation::Timer {
                        fired_at: fire.scheduled_at,
                        kind: fire.kind,
                    },
                    fire.scheduled_at,
                    attempt,
                    is_final,
                    None,
                    None,
                    typed_state,
                    typed_buffer,
                    timers,
                    &mut committed,
                    &fire.stream,
                    instance,
                );
                match retry_error {
                    None => ItemDisposition::Done { ops: committed },
                    Some(error) => ItemDisposition::RetryLater {
                        item: EntityItem::Timer(fire),
                        error,
                    },
                }
            }
            EntityItem::Input(payload) => {
                let EntityPayload {
                    value,
                    stream,
                    consumed_offset,
                    replay,
                } = payload;

                // Downcast value to Arc<S::Input>. Consumers with different
                // Input types in the same StreamOperative downcast
                // independently because the channel wiring guarantees they
                // only ever receive their own type.
                let input_arc = match Arc::downcast::<S::Input>(value) {
                    Ok(arc) => arc,
                    Err(_) => {
                        eprintln!(
                            "[flezha] dispatcher: payload type mismatch for procedure {}",
                            S::name()
                        );
                        return ItemDisposition::Done { ops: committed };
                    }
                };

                // Engine-stamped event time, or the declared clock at entry.
                let now = now.unwrap_or_else(|| self.invocation_inputs(Some(&input_arc)).0);

                if is_final {
                    // The value moves into `run`; capture the park bytes it
                    // would need first.
                    let park_bytes = crate::entity::park::extract_payload_bytes(&*input_arc);
                    // Zero-copy when the Arc is uniquely held; falls back to
                    // a value clone when fan-out kept it shared.
                    let input: S::Input = Arc::unwrap_or_clone(input_arc);
                    let retry_error = self.invoke_once(
                        Invocation::Input {
                            input,
                            consumed_offset,
                            replay,
                        },
                        now,
                        attempt,
                        true,
                        park_bytes,
                        None,
                        typed_state,
                        typed_buffer,
                        timers,
                        &mut committed,
                        &stream,
                        instance,
                    );
                    debug_assert!(
                        retry_error.is_none(),
                        "a final attempt parks instead of retrying"
                    );
                    ItemDisposition::Done { ops: committed }
                } else {
                    // A non-final attempt runs against a copy so the
                    // original survives for the engine's re-hand.
                    let input: S::Input = (*input_arc).clone();
                    let retry_error = self.invoke_once(
                        Invocation::Input {
                            input,
                            consumed_offset,
                            replay,
                        },
                        now,
                        attempt,
                        false,
                        None,
                        Some(&input_arc),
                        typed_state,
                        typed_buffer,
                        timers,
                        &mut committed,
                        &stream,
                        instance,
                    );
                    match retry_error {
                        None => ItemDisposition::Done { ops: committed },
                        Some(error) => ItemDisposition::RetryLater {
                            item: EntityItem::Input(EntityPayload {
                                value: input_arc as Arc<dyn Any + Send + Sync>,
                                stream,
                                consumed_offset,
                                replay,
                            }),
                            error,
                        },
                    }
                }
            }
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
        let Some(record) = buffer.staged.close_on_deadline() else {
            return false;
        };
        buffer
            .effects
            .push(crate::entity::flusher::EffectEntry::from_shipped(
                record, None,
            ));
        true
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
        self.retry
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

        // Per-run change buffer: `StateHandle::apply` records Changes here; the
        // flush after the run ships them to the WAL when the class is durable.
        let buffer: RefCell<Vec<S::Change>> = RefCell::new(Vec::new());

        let (now, invocation_seq) = self.invocation_inputs(Some(&input));

        // Tier mode: keyed plugin emits are captured for the invocation's
        // atomic effect record instead of the workflow plugin wire.
        let effect_outputs = self.effect_capture();

        let mut collect_sink = WorkflowCollectAdapter {
            sink,
            output_channel_id: self.output_channel_id,
            output_channel_kind: self.output_channel_kind,
            output_channel_name: self.output_channel_name.as_deref(),
            topology,
            effect_outputs: effect_outputs.as_ref(),
        };
        let mut ctx = ProcedureCtx::new(
            &mut collect_sink,
            now,
            invocation_seq,
            &self.flush.entity_identity,
        )
        .with_entity_key(self.entity_key.as_deref());
        let handle = StateHandle::<S>::new(&mut typed_state.state, &buffer);

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            typed_state.procedure.run(input, handle, &mut ctx)
        }));

        // Ship the applied Changes to the WAL regardless of outcome, so the WAL
        // reflects the state applied before any failure (at-least-once; the saga
        // compensates a partial run).
        self.flush_collected(
            buffer,
            effect_outputs.map(RefCell::into_inner).unwrap_or_default(),
            now,
        );

        match outcome {
            Ok(Ok(())) => Ok(()),
            // The author's own classification rides through to the workflow
            // retry ladder; a panic stays retryable (the author never got to
            // classify it), as on the stream retry-then-park path.
            Ok(Err(err)) => Err(WorkflowError::new(
                format!("procedure {} failed: {}", S::name(), err),
                err.is_non_retryable(),
            )),
            Err(panic) => Err(WorkflowError::from(format!(
                "procedure {} panicked: {}",
                S::name(),
                crate::procedure::error::format_panic(panic)
            ))),
        }
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

        // Per-compensation change buffer; flushed to the WAL after the hook runs
        // when the class is durable, same as `run_collecting`.
        let buffer: RefCell<Vec<S::Change>> = RefCell::new(Vec::new());

        // Compensation has no current input: EventTimeFrom yields 0 here.
        let (now, invocation_seq) = self.invocation_inputs(None);
        // Compensation keeps the pre-tier emit contract (keyed emits are
        // rejected); under the tier its changes still flush atomically.
        let mut collect_sink = WorkflowCollectAdapter {
            sink,
            output_channel_id: self.output_channel_id,
            output_channel_kind: self.output_channel_kind,
            output_channel_name: self.output_channel_name.as_deref(),
            topology,
            effect_outputs: None,
        };
        let mut ctx = ProcedureCtx::new(
            &mut collect_sink,
            now,
            invocation_seq,
            &self.flush.entity_identity,
        )
        .with_entity_key(self.entity_key.as_deref());
        let handle = StateHandle::<S>::new(&mut typed_state.state, &buffer);

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            typed_state
                .procedure
                .compensate(typed_outputs, handle, &mut ctx)
        }));

        // Persist the compensation's state changes to the WAL (durable classes)
        // so a crash mid-compensation leaves the undo recorded.
        self.flush_collected(buffer, Vec::new(), now);

        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(WorkflowError::from(format!(
                "compensation for procedure {} failed: {}",
                S::name(),
                err
            ))),
            Err(panic) => Err(WorkflowError::from(format!(
                "compensation for procedure {} panicked: {}",
                S::name(),
                crate::procedure::error::format_panic(panic)
            ))),
        }
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
    ) -> Result<(Vec<u8>, u64, usize), String> {
        // Drain any buffered-but-unshipped changes FIRST, so the snapshot's
        // state and its seq are captured together (design D3). After the drain
        // the in-memory state equals the shipped WAL and `change_seq` is one
        // past the last shipped record — so stamping the snapshot with
        // `change_seq - 1` (the highest seq it already contains) lets the
        // controller `truncate_up_to(seq)` — which keeps records STRICTLY above
        // it — drop exactly the records the snapshot folds and no more.
        crate::entity::flusher::flush_one(
            &mut **flush_buffer,
            &self.flush.entity_identity,
            &self.flush.change_seq,
            &self.flush.header,
            self.flush.persistence_enabled,
        );

        let seq = self
            .flush
            .change_seq
            .load(Ordering::Acquire)
            .saturating_sub(1);

        // The snapshot body is `S::State` alone, so the timer ops the coming
        // truncation would delete are re-stated as a full set AFTER the seq is
        // read — landing at `snapshot_seq + 1`, which `truncate_up_to` keeps.
        // A crash between the two leaves an untruncated log that still holds
        // the originals, so neither order can lose a timer.
        if !timers.is_empty() {
            flush_buffer
                .as_any_mut()
                .downcast_mut::<TypedBuffer<S>>()
                .expect("snapshot_frame: flush buffer type mismatch for procedure")
                .timer_ops = timers.as_register_ops();
            crate::entity::flusher::flush_one(
                &mut **flush_buffer,
                &self.flush.entity_identity,
                &self.flush.change_seq,
                &self.flush.header,
                self.flush.persistence_enabled,
            );
        }

        let typed = state
            .downcast_ref::<EntityState<S>>()
            .ok_or_else(|| "snapshot_frame: state type mismatch for procedure".to_string())?;
        // Postcard — the codec `entity/restore.rs` decodes — NOT the JSON
        // migration serializer `build_state_fns` produces.
        let body = postcard::to_allocvec(&typed.state)
            .map_err(|e| format!("snapshot_frame: postcard state encode failed: {e}"))?;

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

/// Synchronous drain of the producing entity's pending changes onto
/// the durable WAL path, invoked before each output frame is written.
type FlushBeforeEmit<'a> = Box<dyn FnMut() + 'a>;

/// Per-invocation emit sink, constructed fresh inside each `dispatch`
/// call. Holds an upgraded `Arc<StreamInstance>` for the duration of the
/// run; if the instance has been dropped, every emit returns
/// `UnknownChannel`.
struct EntityEmitSink<'a> {
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
/// parks the calling thread on the slot's `BlockParker` and retries
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

/// Run one activation: one attempt per item through the typed dispatch, then
/// either complete the batch or hand the failed head (and unprocessed tail)
/// back to the engine's ladder. The engine owns handout exclusivity,
/// admission, and the drain-tail protocol (`runner::poll_core`).
pub(super) fn run_activation(
    core: &crate::runner::poll_core::PollCore,
    act: crate::runner::poll_core::EngineActivation,
) {
    use crate::entity::ItemDisposition;
    use crate::runner::poll_core::{EngineActivation, EnginePayload, WorkItem};

    let EngineActivation { token, payload } = act;
    let (slot, items) = match payload {
        EnginePayload::Work { slot, items } => (slot, items),
        // One entity partition of a workflow stage. It carries its own reply,
        // so it releases this worker the moment it answers — a retry backoff
        // is timed by the driver at its barrier, never slept here.
        EnginePayload::WorkflowStep(step) => {
            flz_core::workflow::step::run_workflow_step(step);
            return;
        }
        // Command/prepare payloads are host-only (C-ABI classes); a typed
        // worker never receives one because typed and host classes do not
        // share a process.
        _ => return,
    };
    let mut queue: std::collections::VecDeque<WorkItem> = items.into();
    while let Some(work) = queue.pop_front() {
        // The connection this item rides — inherited by any processing-time
        // timer it arms, so a firing's plugin emits leave the same way an
        // input's would.
        let stream = match &work.item {
            EntityItem::Input(payload) => Arc::clone(&payload.stream),
            EntityItem::Timer(fire) => Arc::clone(&fire.stream),
            // Never dispatched (see `dispatch_item`); without a connection of
            // its own it takes the live secondary.
            EntityItem::CloseInterval => match flz_core::state_sink::secondary_stream() {
                Some(stream) => stream,
                None => return,
            },
        };
        let (now, attempt, is_final) = (work.now, work.attempt, work.is_final);
        let disposition = {
            let mut state_guard = slot.state.lock_recover();
            let mut buffer_guard = slot.flush_buffer.lock_recover();
            let mut timer_guard = slot.timers.lock_recover();
            let instance_weak = Weak::clone(&slot.instance);
            slot.dispatcher.dispatch_item(
                work,
                &mut state_guard,
                &mut buffer_guard,
                &mut timer_guard,
                &instance_weak,
            )
        };
        match disposition {
            ItemDisposition::Done { ops } => {
                // Outside the locks: arming can fire an already-due timer
                // straight back onto this slot's mailbox, and the drain that
                // would run it needs them.
                crate::entity::timer::apply_ops(&slot, &ops, &stream);
                flz_core::timer::arm_interval_deadline(&slot);
                crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
            }
            ItemDisposition::RetryLater { item, error } => {
                let mut rest = Vec::with_capacity(queue.len() + 1);
                rest.push(WorkItem {
                    item,
                    now,
                    attempt,
                    is_final,
                });
                rest.extend(queue);
                core.retry_after_failure(token, rest, error);
                return;
            }
        }
    }
    core.complete(token);
}

/// Whether a Procedure's Entities join the background flusher's drain pass.
/// One predicate, applied where an Entity is built AND where an invocation has
/// to decide whether its records will ever be drained for it: a Procedure with
/// nothing to persist has no entry, so anything timers give it to persist must
/// be drained on the spot.
fn flusher_registered<S: Procedure>(effectively_once: bool) -> bool {
    TypeId::of::<S::Change>() != TypeId::of::<NoChange>() || effectively_once
}

/// Build one `EntitySlot` for a Procedure. Captures S statically; the
/// channel builder collects this closure into `ProcedureEntry.build_entity`
/// during `StreamOperative::channels` and `build_topology` invokes it once per
/// entity at operative-instance startup.
pub fn build_entity_slot<S: Procedure>(args: BuildEntityArgs<'_>) -> Arc<EntitySlot> {
    // The slot's rounds are served by the typed layer's poll workers; make
    // sure they exist before the first input can be scheduled.
    crate::operative::poll_pool::ensure_poll_workers();
    let procedure = S::default();
    // v1 cold-start path: `init_payload` is `None`. The controller change
    // wires the warm-restart path through `init_entity_with_payload`
    // and calls a richer constructor.
    let state = S::init_state(args.entity_key, None);
    let typed_state = EntityState::<S> { procedure, state };

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
    let dispatcher = Arc::new(ProcedureDispatcherImpl::<S>::new(
        args.procedure_id,
        procedure_desc.output_channel_id,
        output_channel
            .map(|c| c.kind)
            .unwrap_or(ChannelKind::InterProcedure),
        output_channel.map(|c| Arc::clone(&c.name)),
        output_channel.map(|c| c.keyed).unwrap_or(false),
        input_channel.map(|c| Arc::clone(&c.name)),
        input_channel.map(|c| c.retry).unwrap_or_default(),
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
    )) as Arc<dyn ProcedureDispatcher>;

    // Skip flusher registration entirely for `Change == NoChange` Procedures
    // per `design.md` Open Question 2 (TypeId comparison at registration
    // time). The EntitySlot still carries a (always-empty) flush
    // buffer so the dispatcher can construct a `StateHandle<'_, S>`
    // uniformly for all Procedures; only the per-process flusher registry
    // is conditional. Effectively-once operatives always register: their
    // effect records (outputs + consumed-offset, possibly with no change)
    // drain through the same buffer.
    let flusher_entry_id = if flusher_registered::<S>(topology.effectively_once) {
        Some(EntityFlusher::instance().register(
            Arc::clone(&parts.entity_identity),
            Arc::clone(&flush_buffer),
            args.persistence_enabled,
            Arc::clone(&parts.change_seq),
            crate::entity::header::header_template_for::<S>(),
        ))
    } else {
        None
    };

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
mod tests {
    use super::*;
    use crate::channel::ChannelBuilder;
    use crate::entity::restore::RestoreInitPayload;
    use crate::operative::StreamOperative;
    use crate::procedure::state_handle::StateHandle;
    use crate::procedure::{Procedure, ProcedureCtx, ProcedureError};
    use crate::workflow::WorkflowCollectSink;

    #[derive(Default)]
    struct KeyedProc;
    impl Procedure for KeyedProc {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = crate::procedure::NoChange;
        type Clock = crate::procedure::WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_keyed_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: String,
            _state: StateHandle<'_, Self>,
            _: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Ok(())
        }
    }

    struct KeyedOperative;
    impl StreamOperative for KeyedOperative {
        fn factory_id() -> &'static str {
            "workflow_keyed_operative"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<KeyedProc>()
                .named("out")
                .keyed()
                .sink("kafka");
        }
    }

    #[derive(Default)]
    struct NullCollect {
        plugin_bytes: usize,
    }
    impl WorkflowCollectSink for NullCollect {
        fn collect(&mut self, _value: Arc<dyn Any + Send + Sync>) {}
        fn emit_plugin_bytes(&mut self, _channel: &str, _bytes: Vec<u8>) {
            self.plugin_bytes += 1;
        }
    }

    /// The workflow plugin wire carries no metadata, so a keyed emit
    /// through the workflow adapter must fail typed, never silently drop
    /// the identity contract.
    #[test]
    fn workflow_adapter_rejects_keyed_emits() {
        let topology =
            crate::operative::builder::build_topology_for::<KeyedOperative>().expect("topology");
        let out_id = topology.lookup_channel_id("out");
        let mut collect = NullCollect::default();
        let mut adapter = WorkflowCollectAdapter {
            sink: &mut collect,
            output_channel_id: out_id,
            output_channel_kind: ChannelKind::PluginBound,
            output_channel_name: Some("out"),
            topology: &topology,
            effect_outputs: None,
        };
        assert!(adapter.is_keyed(), "descriptor keyed flag must surface");
        let mut ctx = ProcedureCtx::new(&mut adapter, 0, 0, b"workflow/keyed/");
        let err = ctx
            .emit_keyed::<String>("k", "v".into())
            .expect_err("keyed emit on the workflow wire must fail");
        assert!(err.to_string().contains("workflow"));
        assert_eq!(collect.plugin_bytes, 0, "nothing may reach the wire");
    }

    /// Under the effectively-once tier the workflow adapter accepts keyed
    /// emits and captures them for the atomic effect record — never the
    /// metadata-less workflow plugin wire.
    #[test]
    fn workflow_adapter_captures_keyed_emits_under_the_tier() {
        let topology =
            crate::operative::builder::build_topology_for::<KeyedOperative>().expect("topology");
        let out_id = topology.lookup_channel_id("out");
        let mut collect = NullCollect::default();
        let effect_outputs = RefCell::new(Vec::new());
        let mut adapter = WorkflowCollectAdapter {
            sink: &mut collect,
            output_channel_id: out_id,
            output_channel_kind: ChannelKind::PluginBound,
            output_channel_name: Some("out"),
            topology: &topology,
            effect_outputs: Some(&effect_outputs),
        };
        let mut ctx = ProcedureCtx::new(&mut adapter, 0, 0, b"workflow/keyed/");
        ctx.emit_keyed::<String>("BTC/1m", "v".into())
            .expect("keyed emit must succeed under the tier");

        assert_eq!(collect.plugin_bytes, 0, "the workflow wire carries nothing");
        let captured = effect_outputs.into_inner();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].channel, "out");
        assert_eq!(captured[0].key, "BTC/1m");
        assert_eq!(captured[0].payload, b"v".to_vec());
    }

    /// A workflow stage and a `compensate` hook both get a context with no
    /// timer session — the two places the design excludes. The refusal is
    /// structural (there is nothing to register into), and typed rather than
    /// a silent drop.
    #[test]
    fn a_context_without_a_timer_session_refuses_rather_than_dropping() {
        let topology =
            crate::operative::builder::build_topology_for::<KeyedOperative>().expect("topology");
        let mut collect = NullCollect::default();
        let mut adapter = WorkflowCollectAdapter {
            sink: &mut collect,
            output_channel_id: topology.lookup_channel_id("out"),
            output_channel_kind: ChannelKind::PluginBound,
            output_channel_name: Some("out"),
            topology: &topology,
            effect_outputs: None,
        };
        let ctx = ProcedureCtx::new(&mut adapter, 0, 0, b"workflow/keyed/");
        let err = ctx
            .timer_after(std::time::Duration::from_secs(1))
            .expect_err("a session-less context must refuse");
        assert!(err.to_string().contains("not available"), "{err}");
        assert!(ctx.timer_at_event_time(1).is_err());
        assert!(ctx.cancel_processing_time_timer(1).is_err());
        assert!(ctx.cancel_event_time_timer(1).is_err());
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct AddChange(i64);
    impl crate::procedure::change::Change for AddChange {}

    #[derive(Default)]
    struct CounterProc;
    impl Procedure for CounterProc {
        type Input = String;
        type Output = String;
        type State = i64;
        type Change = AddChange;
        type Clock = crate::procedure::WallClock;
        type Config = ();
        fn name() -> &'static str {
            "snapshot_counter_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
            0
        }
        fn apply_change(state: &mut i64, change: &AddChange) {
            *state += change.0;
        }
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Ok(())
        }
    }

    /// A snapshot produced by `snapshot_frame` here decodes, through the exact
    /// restore path, back to the state the Entity held (task 2.6). Stamps the
    /// highest seq the snapshot already contains (`change_seq - 1`), so the
    /// controller's `truncate_up_to` — which keeps records strictly above it —
    /// drops exactly what the snapshot folds.
    #[test]
    fn snapshot_frame_round_trips_through_restore() {
        use crate::entity::restore::init_entity_with_payload;

        let dispatcher = ProcedureDispatcherImpl::<CounterProc>::new(
            0,
            None,
            ChannelKind::InterProcedure,
            None,
            false,
            None,
            RetryPolicy::default(),
            Arc::from("inst/proc/"),
            None,
            FlushWiring {
                entity_identity: Arc::new(flz_core::flusher::encode_entity_identity(
                    "inst", "proc", "",
                )),
                change_seq: Arc::new(AtomicU64::new(3)),
                header: crate::entity::header::header_template_for::<CounterProc>(),
                persistence_enabled: false,
                effectively_once: false,
                checkpoint: None,
                correction: None,
            },
        );

        let mut state: Box<dyn Any + Send> = Box::new(EntityState::<CounterProc> {
            procedure: CounterProc,
            state: 115i64,
        });
        let mut buffer: Box<dyn FlushableBuffer> = Box::new(TypedBuffer::<CounterProc>::default());

        let (frame, seq, size) = dispatcher
            .snapshot_frame(&mut state, &mut buffer, &PendingTimers::default())
            .expect("snapshot_frame must succeed for a live-typed state");
        assert_eq!(
            seq, 2,
            "change_seq 3 - 1 = the highest seq the snapshot folds"
        );

        // Extract the postcard body (header-less, exactly what the controller
        // ships back as RestoreInitState.snapshot_bytes) from the wire frame.
        let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
        let body_len_off = 2 + pid_len + 8 + 16;
        let body_len =
            u32::from_be_bytes(frame[body_len_off..body_len_off + 4].try_into().unwrap()) as usize;
        let body = &frame[body_len_off + 4..body_len_off + 4 + body_len];
        assert_eq!(size, body.len(), "reported byte size is the body length");

        // The header the frame carries is what the controller stores and now
        // ships back beside the body.
        let header = &frame[2 + pid_len + 8..2 + pid_len + 8 + 16];
        let payload = RestoreInitPayload {
            snapshot_header: header,
            snapshot_bytes: body,
            change_log_bytes: &[],
        };
        let (restored, _timers, migrated) =
            init_entity_with_payload::<CounterProc>(None, Some(&payload))
                .expect("the snapshot body must decode through the restore path");
        assert!(!migrated, "same version restores without a migration");
        assert_eq!(
            restored, 115,
            "snapshot round-trips back to the original state"
        );
    }
}
