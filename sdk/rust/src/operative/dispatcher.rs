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
use std::io::Write;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use crate::bounded::{BoundedCollectSink, BoundedError};
use crate::channel::builder::BuildEntityArgs;
use crate::channel::{ChannelId, ChannelKind, OverflowPolicy, ProcedureId, RetryPolicy};
use crate::entity::correction::Lateness;
use crate::entity::flusher::{encode_entity_identity, EntityFlusher, FlushableBuffer, TypedBuffer};
use crate::entity::header::HeaderTemplate;
use crate::entity::{make_entity_payload, EntityPayload, EntitySlot, ProcedureDispatcher};
use crate::operative::instance::UnboundedInstance;
use crate::operative::topology::{BeyondWindowPolicy, OperativeTopology};
use crate::procedure::change::NoChange;
use crate::procedure::ctx::EmitSink;
use crate::procedure::state_handle::StateHandle;
use crate::procedure::{Procedure, ProcedureCtx, ProcedureError};
use crate::runner::mailbox::Mailbox;
use crate::util::LockRecover;

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
    /// Flush-before-emit wiring, mirrored from the owning `EntitySlot`
    /// so the emit sink can drain this entity's just-applied changes to
    /// the durable WAL path before its output frame is written.
    flush: FlushWiring,
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
    /// consumed-offset advances only at an explicit `ctx.checkpoint()`, and
    /// the changes an open interval applies are held in the flush buffer
    /// until it does.
    checkpoint_ack: bool,
    /// Late-input correction policy, `None` unless the class declared a
    /// `correction_window()`. Shares the owning slot's high-water atomic —
    /// the dispatcher is the only place the event-time extractor can run, so
    /// it is also the only place that can maintain it.
    correction: Option<crate::operative::topology::CorrectionPolicy>,
    event_time_high_water: Arc<AtomicI64>,
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
            flush,
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

    /// Decide whether an input should be dispatched at all, and keep the
    /// Entity's event-time high-water. Returns `false` when the caller must
    /// drop the input instead of running it.
    ///
    /// An input behind the high-water but inside the correction window is
    /// dropped *here* and asked for back: it is already durable in the input
    /// log, so the correction re-delivers it inside the replayed slice, in its
    /// rightful place. Running it now would fold it into the state at the
    /// wrong position — the very mistake the correction undoes.
    ///
    /// A `replayed` input is exempt from classification: the engine has just
    /// put it back into event-time order, so it is in order by construction.
    /// It still advances the high-water — the replay is what rebuilds it.
    ///
    /// A class with no `correction_window()` — the default — pays one
    /// `Option` check and nothing else.
    fn admit_and_track(&self, event_time: u64, replayed: bool) -> bool {
        let Some(policy) = self.flush.correction else {
            return true;
        };
        let advance = || {
            self.flush
                .event_time_high_water
                .fetch_max(event_time as i64, Ordering::AcqRel);
        };
        if replayed {
            advance();
            return true;
        }
        let high_water = self.flush.event_time_high_water.load(Ordering::Acquire);

        match crate::entity::correction::classify(event_time, high_water, policy.window_ms) {
            Lateness::OnTime => {
                advance();
                true
            }
            // Requested: drop it here, the slice brings it back. Not requested
            // (transport-less run, nobody to ask): fall back to the
            // no-correction behaviour and process it as an arrival.
            Lateness::Correctable => {
                !crate::entity::correction::request(&self.flush.entity_identity, event_time)
            }
            // The high-water deliberately does NOT advance for a beyond-window
            // arrival: a very old event must not drag the window backward and
            // make the inputs after it look late.
            Lateness::BeyondWindow => match policy.beyond_window {
                BeyondWindowPolicy::ProcessAsArrival => true,
                BeyondWindowPolicy::Drop => false,
            },
        }
    }

    /// Ship the Changes (and, under the effectively-once tier, the captured
    /// outputs as one atomic effect entry) a collecting-path proc applied — or
    /// drain and discard when the class opted out. The bounded analog of the
    /// unbounded `dispatch` flush hook; both feed `flusher::flush_one`.
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
            // Bounded inputs arrive via args/inbox, not the input-log —
            // no consumed-offset to carry.
            TypedBuffer::<S> {
                effects: vec![crate::entity::flusher::EffectEntry {
                    changes,
                    outputs,
                    consumed_offset: None,
                    dlq: None,
                    event_time,
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
}

impl<S: Procedure> ProcedureDispatcher for ProcedureDispatcherImpl<S> {
    fn dispatch(
        &self,
        payload: EntityPayload,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn FlushableBuffer>,
        instance: &Weak<UnboundedInstance>,
    ) {
        let EntityPayload {
            value,
            stream,
            consumed_offset,
            replay,
        } = payload;

        // Downcast value to Arc<S::Input>. Consumers with different
        // Input types in the same UnboundedOperative downcast independently because
        // the channel wiring guarantees they only ever receive their
        // own type.
        let input_arc = match Arc::downcast::<S::Input>(value) {
            Ok(arc) => arc,
            Err(_) => {
                eprintln!(
                    "[flezha] dispatcher: payload type mismatch for procedure {}",
                    S::name()
                );
                return;
            }
        };

        // Zero-copy when the Arc is uniquely held; falls back to a
        // value clone when fan-out kept it shared. The `Clone` bound on
        // `Procedure::Input` makes this explicit at the type level.
        let input: S::Input = Arc::unwrap_or_clone(input_arc);

        let typed_state = state
            .downcast_mut::<EntityState<S>>()
            .expect("dispatcher: state type mismatch for procedure");

        let typed_buffer = flush_buffer
            .as_any_mut()
            .downcast_mut::<TypedBuffer<S>>()
            .expect("dispatcher: flush buffer type mismatch for procedure");

        // Share the change buffer between `StateHandle::apply` and the
        // flush-before-emit hook. Both run on this single dispatch thread
        // inside one `Procedure::run`, so the `RefCell` borrows never overlap.
        let buffer_cell = RefCell::new(std::mem::take(&mut typed_buffer.inner));

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

        let (now, invocation_seq) = self.invocation_inputs(Some(&input));

        // Late-input correction: decide whether this input runs at all, and
        // keep the Entity's event-time high-water.
        if !self.admit_and_track(now, replay) {
            return;
        }

        // Tier mode: plugin emits are captured here (in emit order) instead
        // of leaving on the primary stream, then combined with the run's
        // changes + consumed-offset into one atomic effect record.
        let effect_outputs = self.effect_capture();

        let mut sink = EntityEmitSink {
            output_channel_id: self.output_channel_id,
            output_channel_kind: self.output_channel_kind,
            output_channel_name: self.output_channel_name.as_deref(),
            keyed: self.output_channel_keyed,
            entity_label: &self.entity_label,
            change_seq: &self.flush.change_seq,
            emit_ordinal: 0,
            instance: instance.upgrade(),
            stream: Arc::clone(&stream),
            flush_before_emit,
            consumed_offset,
            effect_outputs: effect_outputs.as_ref(),
        };

        // Retry-then-park loop (`dead-letter-queue`): a failing
        // invocation — returned error or contained panic — is re-run up to
        // the input channel's `retry_max` with exponential backoff + jitter;
        // a `ProcedureError::NonRetryable` skips the remaining budget. A
        // non-final attempt passes a clone of the input so the original
        // survives for the re-run; the final attempt moves it (after saving
        // the park bytes, needed only if it fails too).
        let policy = self.retry;
        let mut input_slot = Some(input);
        let mut parked_payload_bytes: Option<Vec<u8>> = None;
        let mut attempt: u32 = 0;
        // Only the attempt that succeeded may close the interval; a failed
        // attempt's `ctx` — and its checkpoint intent — is discarded with it.
        let mut checkpointed = false;
        let park_error: Option<String> = loop {
            attempt += 1;
            let is_final = attempt >= policy.max_attempts;
            let attempt_input = if is_final {
                let input = input_slot.take().expect("retry loop: input consumed twice");
                parked_payload_bytes = crate::entity::park::extract_payload_bytes(&input);
                input
            } else {
                input_slot
                    .as_ref()
                    .expect("retry loop: input consumed twice")
                    .clone()
            };

            let mut ctx =
                ProcedureCtx::new(&mut sink, now, invocation_seq, &self.flush.entity_identity)
                    .during_replay(replay);
            let handle = StateHandle::<S>::new(&mut typed_state.state, &buffer_cell);

            // Contain a procedure panic so it can't poison the held `state` lock.
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                typed_state.procedure.run(attempt_input, handle, &mut ctx)
            }));

            let (error, retryable) = match outcome {
                Ok(Ok(())) => {
                    checkpointed = ctx.checkpointed();
                    break None;
                }
                Ok(Err(err)) => {
                    let retryable = !err.is_non_retryable();
                    (err.to_string(), retryable)
                }
                // A panic is retryable by default — the author never got to
                // classify it (design D5).
                Err(panic) => (crate::procedure::error::format_panic(panic), true),
            };
            // A failed attempt's captured tier outputs must not ship (the
            // invocation did not complete); changes it already applied to
            // the in-memory state stay buffered so durable state cannot
            // diverge from memory.
            if let Some(cell) = effect_outputs.as_ref() {
                cell.borrow_mut().clear();
            }
            if !retryable || is_final {
                break Some(error);
            }
            let delay = crate::entity::retry::note_failed_attempt(
                &format!("procedure {}", S::name()),
                attempt,
                policy,
                &error,
            );
            crate::util::sleep_interruptible(delay, None);
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
                payload: input_slot
                    .as_ref()
                    .and_then(crate::entity::park::extract_payload_bytes)
                    .or(parked_payload_bytes),
            })
        } else {
            None
        };

        if let Some(outputs_cell) = effect_outputs {
            // Tier mode: this invocation's changes + outputs + consumed
            // offset become ONE effect entry, drained by the flusher as one
            // `entity_effect_record` frame (never coalesced). An
            // effect-free invocation contributes nothing durable — unless it
            // parked, in which case the entry MUST ship so the dead-letter
            // park and the consumed-offset advance stay atomic.
            let changes = buffer_cell.into_inner();
            let outputs = outputs_cell.into_inner();
            let (changes, consumed_offset, ship) = if self.flush.checkpoint_ack {
                typed_buffer.staged.extend(changes);
                // The interval closes on an explicit checkpoint — or on a
                // park, whose poison input is about to be acknowledged into
                // the DLQ and must never be replayed back onto the procedure.
                if checkpointed || dlq.is_some() {
                    // Even a checkpoint that accumulated nothing ships: the
                    // record exists to carry the offset.
                    let staged = std::mem::take(&mut typed_buffer.staged);
                    (staged, consumed_offset, true)
                } else {
                    // Mid-interval: emits still flush as they happen, but they
                    // acknowledge nothing — the state they were computed from
                    // is not durable yet, so a record carrying their offset
                    // would let a restore resume past inputs it never saw.
                    (Vec::new(), None, !outputs.is_empty())
                }
            } else {
                let ship = !(changes.is_empty() && outputs.is_empty()) || dlq.is_some();
                (changes, consumed_offset, ship)
            };
            if ship {
                typed_buffer
                    .effects
                    .push(crate::entity::flusher::EffectEntry {
                        changes,
                        outputs,
                        consumed_offset,
                        dlq,
                        event_time: now,
                    });
            }
        } else {
            // Return any changes not yet drained by flush-before-emit (or all
            // of them when the hook is disabled) to the buffer the background
            // flusher drains. `change_seq` and the move-out drain guarantee
            // each change is shipped exactly once.
            typed_buffer.inner = buffer_cell.into_inner();
            if let Some(entry) = dlq {
                // Tier-off park: ship the park frame and hold this entity
                // (the drain thread) until the controller acks the DLQ
                // append as durable — park-then-advance, never a drop.
                let body = crate::entity::park::ParkBody {
                    park_id: crate::entity::park::mint_park_id(&self.entity_label),
                    origin_offset: consumed_offset,
                    entry,
                };
                crate::entity::park::park_blocking(&self.flush.entity_identity, &body);
            }
        }
    }

    fn procedure_id(&self) -> ProcedureId {
        self.procedure_id
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
        sink: &mut dyn BoundedCollectSink,
    ) -> Result<(), BoundedError> {
        let input_arc = Arc::downcast::<S::Input>(input).map_err(|_| {
            BoundedError::from(format!(
                "bounded procedure {} received an input of the wrong type",
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
        // atomic effect record instead of the bounded plugin wire.
        let effect_outputs = self.effect_capture();

        let mut collect_sink = BoundedCollectAdapter {
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
        );
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
            // The author's own classification rides through to the bounded
            // retry ladder; a panic stays retryable (the author never got to
            // classify it), as on the unbounded retry-then-park path.
            Ok(Err(err)) => Err(BoundedError::new(
                format!("procedure {} failed: {}", S::name(), err),
                err.is_non_retryable(),
            )),
            Err(panic) => Err(BoundedError::from(format!(
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
        sink: &mut dyn BoundedCollectSink,
    ) -> Result<(), BoundedError> {
        // Recover the typed outputs the entity collected on its forward run.
        let mut typed_outputs: Vec<Arc<S::Output>> = Vec::with_capacity(outputs.len());
        for value in outputs {
            match Arc::downcast::<S::Output>(value) {
                Ok(out) => typed_outputs.push(out),
                Err(_) => {
                    return Err(BoundedError::from(format!(
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
        let mut collect_sink = BoundedCollectAdapter {
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
        );
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
            Ok(Err(err)) => Err(BoundedError::from(format!(
                "compensation for procedure {} failed: {}",
                S::name(),
                err
            ))),
            Err(panic) => Err(BoundedError::from(format!(
                "compensation for procedure {} panicked: {}",
                S::name(),
                crate::procedure::error::format_panic(panic)
            ))),
        }
    }

    fn restore_state(
        &self,
        state: &mut Box<dyn Any + Send>,
        snapshot_bytes: &[u8],
        change_log_bytes: &[u8],
        entity_key: Option<&str>,
    ) -> Result<u64, crate::entity::restore::RestoreError> {
        use crate::entity::restore::{
            init_entity_with_payload, iter_change_log_records, RestoreInitPayload,
        };

        let payload = RestoreInitPayload {
            snapshot_bytes,
            change_log_bytes,
        };

        // Rebuild the typed state from snapshot + change-log replay. This runs
        // the same path as cold-start `init_state` + `apply_change`, so a
        // counter that summed to N comes back at N rather than 0.
        let new_state = init_entity_with_payload::<S>(entity_key, Some(&payload))?;

        let typed = state
            .downcast_mut::<EntityState<S>>()
            .expect("restore_state: state type mismatch for procedure");
        typed.state = new_state;

        // Terminal seq = highest WAL seq applied, so the caller can re-arm the
        // entity's change_seq and post-restore appends continue contiguously
        // (no controller-side epoch realign / truncation).
        let mut terminal: u64 = 0;
        for record in iter_change_log_records(change_log_bytes) {
            terminal = terminal.max(record?.seq);
        }
        Ok(terminal)
    }

    fn snapshot_frame(
        &self,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn FlushableBuffer>,
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

        let typed = state
            .downcast_ref::<EntityState<S>>()
            .ok_or_else(|| "snapshot_frame: state type mismatch for procedure".to_string())?;
        // Postcard — the codec `entity/restore.rs` decodes — NOT the JSON
        // migration serializer `build_state_fns` produces.
        let body = postcard::to_allocvec(&typed.state)
            .map_err(|e| format!("snapshot_frame: postcard state encode failed: {e}"))?;

        let seq = self
            .flush
            .change_seq
            .load(Ordering::Acquire)
            .saturating_sub(1);
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
/// call. Holds an upgraded `Arc<UnboundedInstance>` for the duration of the
/// run; if the instance has been dropped, every emit returns
/// `UnknownChannel`.
struct EntityEmitSink<'a> {
    output_channel_id: Option<ChannelId>,
    output_channel_kind: ChannelKind,
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
    instance: Option<Arc<UnboundedInstance>>,
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

    /// Resolve the channel descriptor + UnboundedInstance for the cached
    /// output channel. Returns `UnknownChannel` if either has been
    /// dropped or is unset.
    fn resolve_channel(&self) -> Result<(Arc<UnboundedInstance>, ChannelId), ProcedureError> {
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

    /// Hybrid plugin-sink tail: serialize once inside `catch_unwind`
    /// and forward the bytes through the existing plugin-wire framing
    /// path. A serializer panic surfaces as
    /// `ProcedureError::PluginSerializerPanic` to the producing Procedure;
    /// the dispatcher thread is unaffected.
    fn emit_plugin_sink(
        &mut self,
        instance: &UnboundedInstance,
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

        // `sink.name` is informational on the SDK side (the wire frame
        // routes by `channel: String`); kept for parity with
        // `output(...)` semantics and useful for diagnostics.
        let _plugin_name = &sink.name;
        send_plugin_frame(&self.stream, &instance.instance_id, &bytes, &metadata);
        Ok(())
    }
}

impl<'a> EmitSink for EntityEmitSink<'a> {
    fn emit_inter_procedure(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Result<(), ProcedureError> {
        self.emit_inter_procedure_with_meta(value, None, None)
    }

    fn emit_inter_procedure_with_meta(
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
        route_to_consumers(
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

    fn emit_plugin(
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

        let serialize = descriptor.plugin_serializer.as_ref().ok_or_else(|| {
            ProcedureError::Custom(format!(
                "plugin channel `{}` is missing a serializer (use .output(name) at \
                 declaration time)",
                descriptor.name
            ))
        })?;

        let bytes = serialize(&*value);

        if let Some(cell) = self.effect_outputs {
            push_effect_output(cell, &descriptor.name, key, meta, bytes);
            return Ok(());
        }

        // Reserved key: any caller-supplied "channel" entry is dropped per
        // ProcedureCtx emit semantics, then the real channel name is stamped.
        let mut metadata = channel_metadata(meta, &descriptor.name);
        self.stamp_identity(&mut metadata, key);

        send_plugin_frame(&self.stream, &instance.instance_id, &bytes, &metadata);
        Ok(())
    }

    fn channel_name(&self) -> &str {
        self.output_channel_name.unwrap_or("<unset>")
    }

    fn is_plugin_bound(&self) -> bool {
        matches!(self.output_channel_kind, ChannelKind::PluginBound)
    }

    fn has_plugin_sink(&self) -> bool {
        // Resolving the descriptor each time keeps the trait surface
        // small. The cached `output_channel_kind` is used for the
        // pure-plugin fast path; hybrid detection is rare on the hot
        // path (a single `Option::is_some()` after a topology lookup).
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

/// Emit sink for the bounded sequential driver. Inter-procedure output is
/// **collected** for handoff to the next stage at the barrier; plugin-bound
/// output is serialized and routed to the wire via the `BoundedCollectSink`.
/// Constructed fresh per `run_collecting` call, so it never threads through
/// the unbounded push-dispatch mailbox path.
struct BoundedCollectAdapter<'a> {
    sink: &'a mut dyn BoundedCollectSink,
    output_channel_id: Option<ChannelId>,
    output_channel_kind: ChannelKind,
    output_channel_name: Option<&'a str>,
    topology: &'a OperativeTopology,
    /// `Some` under the effectively-once tier: plugin emits (keyed
    /// included) are captured into the invocation's atomic effect record
    /// instead of the metadata-less bounded plugin wire.
    effect_outputs: Option<&'a RefCell<Vec<crate::entity::flusher::OutputRecord>>>,
}

impl<'a> BoundedCollectAdapter<'a> {
    fn unknown_channel(&self) -> ProcedureError {
        ProcedureError::UnknownChannel {
            channel: self
                .output_channel_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| "<unknown>".to_string()),
        }
    }

    /// Serialize a plugin-bound value via the producer-edge channel's
    /// serializer (pure `output`) or hybrid plugin sink
    /// (`also_output_with`) and route the bytes: into the invocation's
    /// effect record under the tier, else to the bounded plugin wire.
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
            .plugin_serializer
            .as_ref()
            .or_else(|| descriptor.plugin_sink.as_ref().map(|s| &s.serializer))
            .ok_or_else(|| {
                ProcedureError::Custom(format!(
                    "bounded plugin channel `{}` is missing a serializer",
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

impl<'a> BoundedCollectAdapter<'a> {
    /// The bounded plugin wire (`emit_plugin_bytes`) carries no metadata,
    /// so `.keyed()` identity stamps cannot ride it. Reject rather than
    /// silently dropping the identity contract — except under the
    /// effectively-once tier, where emits ride the effect record (which
    /// carries the key durably).
    fn reject_key(&self, key: &Option<String>) -> Result<(), ProcedureError> {
        if self.effect_outputs.is_none() && key.is_some() {
            return Err(ProcedureError::Custom(format!(
                "keyed output identity is not supported on bounded operatives \
                 (channel `{}`): the bounded plugin wire carries no metadata",
                self.channel_name()
            )));
        }
        Ok(())
    }
}

impl<'a> EmitSink for BoundedCollectAdapter<'a> {
    fn emit_inter_procedure(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Result<(), ProcedureError> {
        self.emit_inter_procedure_with_meta(value, None, None)
    }

    fn emit_inter_procedure_with_meta(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), ProcedureError> {
        self.reject_key(&key)?;
        // Hybrid `also_output_with`: collect for the next stage AND route
        // a serialized copy to the plugin wire / effect record.
        if self.has_plugin_sink() {
            self.route_plugin(&value, key, meta)?;
        }
        self.sink.collect(value);
        Ok(())
    }

    fn emit_plugin(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), ProcedureError> {
        self.reject_key(&key)?;
        self.route_plugin(&value, key, meta)
    }

    fn channel_name(&self) -> &str {
        self.output_channel_name.unwrap_or("<bounded>")
    }

    fn is_plugin_bound(&self) -> bool {
        matches!(self.output_channel_kind, ChannelKind::PluginBound)
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
/// Bounded park per Block-policy retry; progress is re-checked (and the
/// inline drain re-attempted) on every wakeup.
const BLOCK_PARK_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

/// One throttled warning per blocked episode once it exceeds this — a full
/// mailbox holding a producer this long means a consumer procedure is wedged or
/// severely behind, and silence here is how the 2026-06-10 freeze hid.
const BLOCK_WARN_AFTER: std::time::Duration = std::time::Duration::from_secs(5);

pub(crate) fn enqueue(slot: &Arc<EntitySlot>, payload: EntityPayload, policy: OverflowPolicy) {
    // Open the slice here, on the frame-handling thread, rather than in the
    // drain: the controller's end-of-slice broadcast arrives on this same thread
    // after every slice frame, so opening here guarantees the close can never
    // overtake a member it was meant to include. Opening in the drain would race
    // — a slow drain could see the close first and then buffer members nobody
    // would ever flush.
    if payload.replay {
        slot.replay_open.store(true, Ordering::Release);
    }
    let mut next = payload;
    let mut blocked_since: Option<std::time::Instant> = None;
    let mut block_warned = false;
    loop {
        match slot.mailbox.try_push(next) {
            Ok(()) => {
                slot.mailbox
                    .counters()
                    .pushes_total
                    .fetch_add(1, Ordering::Relaxed);
                // Activity stamp for the dynamic-mode reaper (one atomic
                // store; inert for single/static Entities that are never
                // reaped).
                slot.last_activity_at
                    .store(crate::entity::dynamic::now_millis(), Ordering::Relaxed);
                if slot.mailbox.try_mark_inflight() {
                    let cloned = Arc::clone(slot);
                    slot.pool.submit(Box::new(move || drain_entity(&cloned)));
                }
                return;
            }
            Err(returned) => match policy {
                OverflowPolicy::Block => {
                    slot.mailbox
                        .counters()
                        .pushes_blocked_total
                        .fetch_add(1, Ordering::Relaxed);
                    next = returned;
                    if blocked_since.is_none() {
                        blocked_since = Some(std::time::Instant::now());
                        crate::entity::BLOCKED_PARK_EPISODES.fetch_add(1, Ordering::Relaxed);
                    }
                    // Drain the full slot inline rather than parking on it:
                    // the slot's drain may exist only as a queued pool task,
                    // and a parked pool worker can be the lone thread able to
                    // run it (a one-worker pool self-deadlocks; prod
                    // 2026-06-10). The drain_active gate in drain_entity
                    // keeps this race-safe and order-preserving.
                    drain_entity(slot);
                    if slot.mailbox.is_full() {
                        // A live drainer on another thread owns progress;
                        // bounded park so even a lost wake degrades to a
                        // retry loop instead of a permanent freeze.
                        slot.block_parker
                            .wait_one_timeout(&slot.mailbox, BLOCK_PARK_TIMEOUT);
                    }
                    if !block_warned
                        && blocked_since.is_some_and(|t| t.elapsed() >= BLOCK_WARN_AFTER)
                    {
                        block_warned = true;
                        tracing::warn!(
                            target: "flezha::mailbox",
                            procedure_id = slot.procedure_id,
                            entity_key = ?slot.entity_key,
                            "Block-policy producer stalled >{:?} on a full entity mailbox; consumer procedure wedged or far behind",
                            BLOCK_WARN_AFTER
                        );
                    }
                }
                OverflowPolicy::DropNewest => {
                    slot.mailbox
                        .counters()
                        .drops_newest_total
                        .fetch_add(1, Ordering::Relaxed);
                    drop(returned);
                    return;
                }
                OverflowPolicy::DropOldest => {
                    let _dropped = slot.mailbox.force_push(returned);
                    slot.mailbox
                        .counters()
                        .drops_oldest_total
                        .fetch_add(1, Ordering::Relaxed);
                    if slot.mailbox.try_mark_inflight() {
                        let cloned = Arc::clone(slot);
                        slot.pool.submit(Box::new(move || drain_entity(&cloned)));
                    }
                    return;
                }
            },
        }
    }
}

/// Fan a typed value out to every consumer Procedure's entity mailbox per
/// the channel's `OverflowPolicy`. Shared by the inter-procedure emit path
/// (`EntityEmitSink`) and the inbound wire-dispatch path
/// (`OperativeDispatchHandle`). Single-mode procedures route to entity 0;
/// static-mode procedures route by `entities_for_erased` key lookup;
/// dynamic-mode procedures derive a key via `entity_key` and lazily create the
/// Entity through the per-Procedure `DynamicEntityRegistry`.
/// A poison entry payload climbing its channel's retry→DLQ ladder off the
/// wire-reader thread (`sdk-graph-authoring` typed entries): the failing parse
/// is re-attempted per the channel's retry budget — the parser contract says
/// deterministic, so this is expected to fail identically — and then parked to
/// the dead-letter queue under the entry channel's identity, exactly like a
/// retry-exhausted invocation.
pub(crate) struct EntryParseLadder {
    pub instance: Arc<UnboundedInstance>,
    pub channel_id: u32,
    pub bytes: Vec<u8>,
    pub first_error: String,
    pub stream: Arc<flz_transport::transport::FlezhaStream>,
    pub consumed_offset: Option<u64>,
    pub replay: bool,
}

pub(crate) fn spawn_entry_parse_ladder(ladder: EntryParseLadder) {
    let spawned = std::thread::Builder::new()
        .name("flezha-entry-parse-park".to_string())
        .spawn(move || run_entry_parse_ladder(ladder));
    if let Err(e) = spawned {
        eprintln!("[flezha] failed to spawn entry-parse park thread: {e}");
    }
}

fn run_entry_parse_ladder(ladder: EntryParseLadder) {
    let channel = ladder
        .instance
        .topology
        .channel(ladder.channel_id)
        .expect("entry channel present in topology");
    let wrap = channel
        .wrap_input
        .as_ref()
        .expect("parsed entry channel carries a wrap_input");
    let label = format!("entry channel `{}`", channel.name);

    let mut attempt: u32 = 1; // the wire-path parse was attempt 1
    let mut error = ladder.first_error;
    while attempt < channel.retry.max_attempts {
        let delay =
            crate::entity::retry::note_failed_attempt(&label, attempt, channel.retry, &error);
        crate::util::sleep_interruptible(delay, None);
        attempt += 1;
        match wrap(ladder.bytes.clone()) {
            // A parser violating the determinism contract may succeed on a
            // re-attempt; deliver rather than drop.
            Ok(typed) => {
                route_to_consumers(
                    &ladder.instance,
                    channel,
                    &typed,
                    &ladder.stream,
                    ladder.consumed_offset,
                    ladder.replay,
                );
                return;
            }
            Err(e) => error = e,
        }
    }

    crate::entity::park::PARKED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    eprintln!(
        "[flezha] {} parking unparseable payload to DLQ after {} attempt(s): {}",
        label, attempt, error
    );
    let identity = crate::entity::flusher::encode_entity_identity(
        &ladder.instance.instance_id,
        &channel.name,
        "_entry",
    );
    let body = crate::entity::park::ParkBody {
        park_id: crate::entity::park::mint_park_id(&format!(
            "{}/{}/_entry",
            ladder.instance.instance_id, channel.name
        )),
        origin_offset: ladder.consumed_offset,
        entry: crate::entity::flusher::DlqEntryBody {
            channel: channel.name.to_string(),
            error,
            attempts: attempt,
            payload: Some(ladder.bytes),
        },
    };
    crate::entity::park::park_blocking(&identity, &body);
}

/// Route one channel value to every subscribed consumer, each through its own
/// edge's merge mapping. The one seam a `ConsumerEdge` mapping is applied on:
/// the wire-reader path (live delivery and the controller's re-forwarded replay
/// slice) and an inter-procedure emit both come through here, so the two can
/// never drift.
pub(crate) fn route_to_consumers(
    instance: &UnboundedInstance,
    channel: &crate::channel::ChannelDescriptor,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
    consumed_offset: Option<u64>,
    replay: bool,
) {
    use crate::operative::topology::EntityMode;
    let policy = channel.overflow_policy;
    for edge in &channel.consumers {
        let consumer_procedure_id = edge.procedure;
        let consumer = match instance.topology.procedure(consumer_procedure_id) {
            Some(s) => s,
            None => continue,
        };
        // A merged subscription lifts this channel's payload into the
        // consumer's `Input` sum type before anything downstream sees it, so
        // `entities_for` / `entity_key` and the mailbox all carry the mapped
        // value — on live delivery and on replay alike.
        let mapped = edge.map_value(value);
        let value = mapped.as_ref().unwrap_or(value);
        match consumer.mode {
            EntityMode::Dynamic => {
                let registry = match instance
                    .dynamic_registries
                    .get(consumer_procedure_id as usize)
                    .and_then(|r| r.as_ref())
                {
                    Some(r) => r,
                    None => continue,
                };
                let key = match consumer
                    .entity_key_erased
                    .as_ref()
                    .and_then(|f| f(value.as_ref()))
                {
                    Some(k) => k,
                    None => {
                        registry
                            .counters
                            .inputs_dropped_no_key_total
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                if let Ok(slot) = registry.get_or_create(&key) {
                    enqueue(
                        &slot,
                        make_entity_payload(value, stream, consumed_offset, replay),
                        policy,
                    );
                }
            }
            EntityMode::Single => {
                if let Some(slot) = instance
                    .entities
                    .get(consumer_procedure_id as usize)
                    .and_then(|p| p.first())
                {
                    enqueue(
                        slot,
                        make_entity_payload(value, stream, consumed_offset, replay),
                        policy,
                    );
                }
            }
            EntityMode::Static => {
                let entities = match instance.entities.get(consumer_procedure_id as usize) {
                    Some(p) => p,
                    None => continue,
                };
                let key_to_idx = instance
                    .entity_key_to_idx
                    .get(consumer_procedure_id as usize);
                for key in (consumer.entities_for_erased)(value.as_ref()) {
                    if let Some(&idx) = key_to_idx.and_then(|m| m.get(key)) {
                        if let Some(slot) = entities.get(idx) {
                            enqueue(
                                slot,
                                make_entity_payload(value, stream, consumed_offset, replay),
                                policy,
                            );
                        }
                    }
                }
            }
        }
    }
}

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

/// Tier-mode capture (shared by the unbounded emit sink and the bounded
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

fn send_plugin_frame(
    stream: &Arc<FlezhaStream>,
    instance_id: &str,
    bytes: &[u8],
    metadata: &HashMap<String, String>,
) {
    let frame = payloader::build_from_binary(instance_id, bytes, metadata.clone());
    if let Err(e) = (&**stream).write_all(&frame) {
        eprintln!(
            "[flezha] plugin emit: failed to write {} bytes for {}: {}",
            bytes.len(),
            instance_id,
            e
        );
    }
}

pub fn drain_entity(slot: &Arc<EntitySlot>) {
    // Single-ACTIVE-drainer gate. The queued pool task and a Block-policy
    // producer draining inline may both arrive here; the loser leaves and the
    // winner owns the pop loop, so per-slot dispatch order is preserved.
    if slot.drain_active.swap(true, Ordering::AcqRel) {
        return;
    }
    loop {
        while let Some(payload) = slot.mailbox.pop() {
            // Free one waiter (if any) before running the dispatcher;
            // the popped slot is now available for the unparked producer
            // to re-attempt its push.
            slot.block_parker.unpark_one();
            // Effectively-once crash-replay: the controller re-forwards the
            // journaled input tail; offsets this Entity already durably
            // consumed (restored effect records) are skipped, never re-run.
            if let Some(offset) = payload.consumed_offset {
                if (offset as i64) <= slot.consumed_floor.load(Ordering::Acquire) {
                    crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            // A replayed slice is held whole, ordered by event time, and only
            // then dispatched. Re-running it in arrival order would put the
            // late input back after the inputs that overtook it and recompute
            // the same wrong answer.
            if payload.replay {
                slot.replay_buffer.lock_recover().push(payload);
                slot.replay_depth.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            // A live input popped while a closed slice is still buffered: the
            // slice is older than it, so the slice runs first. Without this the
            // live input jumps the queue and lands ahead of the very inputs the
            // correction was re-ordering.
            maybe_dispatch_replay_slice(slot);
            dispatch_one(slot, payload);
        }

        // The mailbox is dry. If a slice is buffered and the controller has
        // closed it, this is the moment to order and run it — in the drain, not
        // the frame handler, so it happens under the single-drainer gate with no
        // dispatch racing it.
        maybe_dispatch_replay_slice(slot);
        // Drop the active gate BEFORE releasing inflight so a producer that
        // observes inflight=false can immediately become the next drainer.
        slot.drain_active.store(false, Ordering::Release);
        slot.mailbox.release_inflight();
        if slot.mailbox.is_empty() {
            return;
        }
        if !slot.mailbox.try_mark_inflight() {
            return;
        }
        if slot.drain_active.swap(true, Ordering::AcqRel) {
            // Another thread became the active drainer for the round we just
            // marked; it releases the inflight token when it finishes.
            return;
        }
    }
}

/// Close every Entity's replayed slice and wake their drains to run it.
///
/// A broadcast rather than a marker riding the slice's last input, because it
/// has to be: an input routes to exactly one Entity by key, so a marker on the
/// last one would reach one Entity and leave every other Entity that buffered
/// slice members holding them forever.
///
/// Arrives on the frame-handling thread after every frame of the slice, so a
/// Entity cannot still be receiving members when its slice is closed.
pub(crate) fn end_replay_slice(instance: &UnboundedInstance) {
    let statics = instance.entities.iter().flatten().cloned();
    let dynamics = instance.dynamic_registries.iter().flatten().flat_map(|r| {
        r.entities
            .iter()
            .map(|e| Arc::clone(e.value()))
            .collect::<Vec<_>>()
    });

    for slot in statics.chain(dynamics) {
        slot.replay_open.store(false, Ordering::Release);
        // The drain may already have run dry and exited with the slice still
        // buffered — it had no way to know more was coming. Wake it so it can
        // see the closed slice and flush it.
        if slot.mailbox.try_mark_inflight() {
            let cloned = Arc::clone(&slot);
            slot.pool.submit(Box::new(move || drain_entity(&cloned)));
        }
    }
}

/// Dispatch the buffered slice if there is one and the controller has closed it.
/// Two relaxed loads in the overwhelmingly common case of no replay in flight.
fn maybe_dispatch_replay_slice(slot: &Arc<EntitySlot>) {
    if slot.replay_depth.load(Ordering::Relaxed) != 0 && !slot.replay_open.load(Ordering::Acquire) {
        dispatch_replay_slice(slot);
    }
}

/// Run one payload through the Procedure, holding the state and flush-buffer
/// locks for the invocation.
fn dispatch_one(slot: &Arc<EntitySlot>, payload: EntityPayload) {
    let mut state_guard = slot.state.lock_recover();
    let mut buffer_guard = slot.flush_buffer.lock_recover();
    let instance_weak = Weak::clone(&slot.instance);
    slot.dispatcher
        .dispatch(payload, &mut state_guard, &mut buffer_guard, &instance_weak);
    crate::entity::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
}

/// The end-of-slice marker arrived: order the buffered slice by event time and
/// dispatch it.
///
/// The sort is **stable** and tie-broken by ascending input-log offset, so two
/// inputs bearing the same event time replay in log order. That is what makes a
/// correction reproducible: slice membership is an offset range and slice order
/// is a function of the inputs alone, so a re-run after a crash folds the same
/// inputs in the same order into the same state — and re-emits byte-identical
/// output identities.
fn dispatch_replay_slice(slot: &Arc<EntitySlot>) {
    let mut slice = std::mem::take(&mut *slot.replay_buffer.lock_recover());
    slot.replay_depth.store(0, Ordering::Relaxed);

    slice.sort_by_key(|p| {
        (
            slot.dispatcher.event_time_of(p).unwrap_or(0),
            p.consumed_offset.unwrap_or(0),
        )
    });

    // The high-water is NOT reset here. Every replay is preceded by a restore,
    // which already re-established it from the change log it restored — the
    // rewound baseline's for a correction, the recovered state's for a crash
    // resume. Zeroing it would be redundant in the first case and *lossy* in the
    // second: a resumed Entity whose unconsumed tail happens to carry a lower
    // max event time than its restored state would come back with a high-water
    // below the truth, and read a genuinely late input as on-time. The slice's
    // payloads raise it by `fetch_max` as they dispatch.
    for payload in slice {
        dispatch_one(slot, payload);
    }
}

/// Build one `EntitySlot` for a Procedure. Captures S statically; the
/// channel builder collects this closure into `ProcedureEntry.build_entity`
/// during `UnboundedOperative::channels` and `build_topology` invokes it once per
/// entity at operative-instance startup.
pub fn build_entity_slot<S: Procedure>(args: BuildEntityArgs<'_>) -> Arc<EntitySlot> {
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

    let change_seq = Arc::new(AtomicU64::new(0));
    let event_time_high_water = Arc::new(AtomicI64::new(-1));
    // Key the on-disk WAL identity by the operative INSTANCE id, not the factory
    // id: two operatives of the same factory must not share one Entity WAL.
    let entity_identity = Arc::new(encode_entity_identity(
        args.instance_id.as_ref(),
        procedure_desc.name.as_ref(),
        args.entity_key.unwrap_or(""),
    ));

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
    // `flz.entity` stamp: same triple the WAL identity encodes, readable.
    let entity_label: Arc<str> = Arc::from(format!(
        "{}/{}/{}",
        args.instance_id,
        procedure_desc.name,
        args.entity_key.unwrap_or("")
    ));
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
        entity_label,
        FlushWiring {
            entity_identity: Arc::clone(&entity_identity),
            change_seq: Arc::clone(&change_seq),
            header: HeaderTemplate::for_procedure::<S>(),
            persistence_enabled: args.persistence_enabled,
            effectively_once: topology.effectively_once,
            checkpoint_ack: topology.ack_granularity
                == crate::operative::topology::AckGranularity::Checkpoint,
            correction: topology.correction,
            event_time_high_water: Arc::clone(&event_time_high_water),
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
    let flusher_entry_id =
        if TypeId::of::<S::Change>() != TypeId::of::<NoChange>() || topology.effectively_once {
            let flusher = EntityFlusher::instance();
            Some(flusher.register(
                Arc::clone(&entity_identity),
                Arc::clone(&flush_buffer),
                args.persistence_enabled,
                Arc::clone(&change_seq),
                HeaderTemplate::for_procedure::<S>(),
            ))
        } else {
            None
        };

    Arc::new(EntitySlot {
        state: std::sync::Mutex::new(Box::new(typed_state)),
        mailbox: Arc::new(Mailbox::with_capacity(procedure_desc.mailbox_capacity)),
        dispatcher,
        procedure_id: args.procedure_id,
        entity_key: args.entity_key.map(Arc::from),
        pool: args.pool,
        instance: args.instance,
        topology,
        block_parker: crate::entity::BlockParker::new(),
        drain_active: std::sync::atomic::AtomicBool::new(false),
        flush_buffer,
        persistence_enabled: args.persistence_enabled,
        change_seq,
        consumed_floor: std::sync::atomic::AtomicI64::new(-1),
        event_time_high_water: Arc::clone(&event_time_high_water),
        replay_buffer: Mutex::new(Vec::new()),
        replay_open: std::sync::atomic::AtomicBool::new(false),
        replay_depth: AtomicU64::new(0),
        entity_identity,
        last_activity_at: std::sync::atomic::AtomicI64::new(crate::entity::dynamic::now_millis()),
        flusher_entry_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded::BoundedCollectSink;
    use crate::channel::ChannelBuilder;
    use crate::entity::restore::RestoreInitPayload;
    use crate::operative::UnboundedOperative;
    use crate::procedure::state_handle::StateHandle;
    use crate::procedure::{Procedure, ProcedureCtx, ProcedureError};

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
            "bounded_keyed_op"
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
    impl UnboundedOperative for KeyedOperative {
        fn factory_id() -> &'static str {
            "bounded_keyed_operative"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<KeyedProc>()
                .named("out")
                .keyed()
                .output("kafka");
        }
    }

    #[derive(Default)]
    struct NullCollect {
        plugin_bytes: usize,
    }
    impl BoundedCollectSink for NullCollect {
        fn collect(&mut self, _value: Arc<dyn Any + Send + Sync>) {}
        fn emit_plugin_bytes(&mut self, _channel: &str, _bytes: Vec<u8>) {
            self.plugin_bytes += 1;
        }
    }

    /// The bounded plugin wire carries no metadata, so a keyed emit
    /// through the bounded adapter must fail typed, never silently drop
    /// the identity contract.
    #[test]
    fn bounded_adapter_rejects_keyed_emits() {
        let topology =
            crate::operative::builder::build_topology_for::<KeyedOperative>().expect("topology");
        let out_id = topology.lookup_channel_id("out");
        let mut collect = NullCollect::default();
        let mut adapter = BoundedCollectAdapter {
            sink: &mut collect,
            output_channel_id: out_id,
            output_channel_kind: ChannelKind::PluginBound,
            output_channel_name: Some("out"),
            topology: &topology,
            effect_outputs: None,
        };
        assert!(adapter.is_keyed(), "descriptor keyed flag must surface");
        let mut ctx = ProcedureCtx::new(&mut adapter, 0, 0, b"bounded/keyed/");
        let err = ctx
            .emit_keyed::<String>("k", "v".into())
            .expect_err("keyed emit on the bounded wire must fail");
        assert!(err.to_string().contains("bounded"));
        assert_eq!(collect.plugin_bytes, 0, "nothing may reach the wire");
    }

    /// Under the effectively-once tier the bounded adapter accepts keyed
    /// emits and captures them for the atomic effect record — never the
    /// metadata-less bounded plugin wire.
    #[test]
    fn bounded_adapter_captures_keyed_emits_under_the_tier() {
        let topology =
            crate::operative::builder::build_topology_for::<KeyedOperative>().expect("topology");
        let out_id = topology.lookup_channel_id("out");
        let mut collect = NullCollect::default();
        let effect_outputs = RefCell::new(Vec::new());
        let mut adapter = BoundedCollectAdapter {
            sink: &mut collect,
            output_channel_id: out_id,
            output_channel_kind: ChannelKind::PluginBound,
            output_channel_name: Some("out"),
            topology: &topology,
            effect_outputs: Some(&effect_outputs),
        };
        let mut ctx = ProcedureCtx::new(&mut adapter, 0, 0, b"bounded/keyed/");
        ctx.emit_keyed::<String>("BTC/1m", "v".into())
            .expect("keyed emit must succeed under the tier");

        assert_eq!(collect.plugin_bytes, 0, "the bounded wire carries nothing");
        let captured = effect_outputs.into_inner();
        assert_eq!(captured.len(), 1);
        assert_eq!(captured[0].channel, "out");
        assert_eq!(captured[0].key, "BTC/1m");
        assert_eq!(captured[0].payload, b"v".to_vec());
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
            FlushWiring {
                entity_identity: Arc::new(encode_entity_identity("inst", "proc", "")),
                change_seq: Arc::new(AtomicU64::new(3)),
                header: HeaderTemplate::for_procedure::<CounterProc>(),
                persistence_enabled: false,
                effectively_once: false,
                checkpoint_ack: false,
                correction: None,
                event_time_high_water: Arc::new(AtomicI64::new(-1)),
            },
        );

        let mut state: Box<dyn Any + Send> = Box::new(EntityState::<CounterProc> {
            procedure: CounterProc,
            state: 115i64,
        });
        let mut buffer: Box<dyn FlushableBuffer> = Box::new(TypedBuffer::<CounterProc>::default());

        let (frame, seq, size) = dispatcher
            .snapshot_frame(&mut state, &mut buffer)
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

        let payload = RestoreInitPayload {
            snapshot_bytes: body,
            change_log_bytes: &[],
        };
        let restored = init_entity_with_payload::<CounterProc>(None, Some(&payload))
            .expect("the snapshot body must decode through the restore path");
        assert_eq!(
            restored, 115,
            "snapshot round-trips back to the original state"
        );
    }
}
