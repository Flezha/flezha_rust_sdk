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

use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};

use flz_transport::transport::FlezhaStream;

use crate::channel::{ChannelId, ChannelKind, ProcedureId, RetryPolicy};
use crate::entity::flusher::{FlushableBuffer, TypedBuffer};
use crate::entity::header::HeaderTemplate;
use crate::entity::timer::{PendingTimers, TimerOp, TimerPolicy};
use crate::entity::{EntityItem, EntityPayload, EntitySlot, ProcedureDispatcher};
use crate::operative::instance::StreamInstance;
use crate::operative::topology::OperativeTopology;
use crate::procedure::Procedure;
use crate::util::LockRecover;
use crate::workflow::{WorkflowCollectSink, WorkflowError};

pub(crate) mod emit_sink;
mod entity_slot;
mod invocation;

pub use entity_slot::build_entity_slot;
use entity_slot::Handout;
use invocation::{CollectShape, EntityScope, Invocation};

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

#[cfg(test)]
#[path = "dispatcher_tests.rs"]
mod tests;
