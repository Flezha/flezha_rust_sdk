//! Per-Tasklet typed dispatcher.
//!
//! `TaskletDispatcherImpl<S>` carries S statically so:
//! - mailbox payloads are downcast to `Arc<S::Input>` without allocation
//! - the cell's state is downcast to `(S, S::State)` and threaded
//!   through `S::run` with a typed signature
//! - the emit output already knows the cached output channel id
//!
//! The dispatcher trait object stores a single `Arc<dyn TaskletDispatcher>`
//! per `CellSlot`. The inflight CAS on the mailbox guarantees that
//! `dispatch(...)` is never called concurrently for the same slot, so
//! the per-cell `(S, S::State)` is safely accessible behind a
//! `Mutex` lock that is always uncontended in practice.

use std::any::Any;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use flz_core::header::HeaderTemplate;
use flz_core::poll_core::global;
use flz_core::poll_pool::Governor;
use flz_transport::transport::FlezhaStream;

use crate::cell::flusher::TypedBuffer;
use crate::cell::timer::{PendingTimers, TimerOp, TimerPolicy};
use crate::cell::{CellItem, CellPayload, CellSlot, TaskletDispatcher};
use crate::channel::{ChannelId, RetryPolicy, TaskletId};
use crate::relay::instance::RelayInstance;
use crate::tasklet::Tasklet;
use crate::util::LockRecover;

mod cell_slot;
pub(crate) mod emit_sink;
mod invocation;

pub use cell_slot::build_cell_slot;
use cell_slot::Handout;
use invocation::{CellScope, Invocation};

/// Per-cell holder for a typed Tasklet instance + its State. Boxed and
/// type-erased into `CellSlot.state`.
pub struct CellState<S: Tasklet> {
    pub tasklet: S,
    pub state: S::State,
}

/// Build the per-Tasklet serialize/deserialize state fn pointers used by
/// `serialize_relay_state` and `apply_init_values`. The closure captures
/// S statically; `serialize_state` downcasts the type-erased state to
/// `CellState<S>` and emits `serde_json::Value` via `S::State:
/// Serialize`. `deserialize_state` parses a `serde_json::Value` into
/// `S::State` and wraps it with a fresh `S::default()` to match what the
/// dispatcher expects when it locks the Mutex.
pub(crate) fn build_state_fns<S: Tasklet>() -> (
    crate::channel::builder::SerializeStateFn,
    crate::channel::builder::DeserializeStateFn,
) {
    let serialize: crate::channel::builder::SerializeStateFn = Arc::new(
        |state: &Box<dyn Any + Send>| -> Result<serde_json::Value, String> {
            let typed = state
                .downcast_ref::<CellState<S>>()
                .ok_or_else(|| "serialize_state: state type mismatch".to_string())?;
            serde_json::to_value(&typed.state).map_err(|e| format!("serialize_state: {}", e))
        },
    );
    let deserialize: crate::channel::builder::DeserializeStateFn = Arc::new(
        |value: &serde_json::Value| -> Result<Box<dyn Any + Send>, String> {
            let state: S::State = serde_json::from_value(value.clone())
                .map_err(|e| format!("deserialize_state: {}", e))?;
            let typed = CellState::<S> {
                tasklet: S::default(),
                state,
            };
            Ok(Box::new(typed) as Box<dyn Any + Send>)
        },
    );
    (serialize, deserialize)
}

/// The producer- and consumer-edge facts a Cell's dispatcher resolves once
/// at construction, so emit pays zero topology lookups on the hot path.
struct EdgeFacts {
    output_channel_id: Option<ChannelId>,
    output_channel_name: Option<Arc<str>>,
    /// `.keyed()` opt-in of the producer-edge channel.
    output_channel_keyed: bool,
    /// Retry-then-park policy of the input channel (defaults when unwired).
    retry: RetryPolicy,
}

/// Typed dispatcher. Caches its edge facts at construction so emit pays zero
/// name-lookup cost on the hot path.
pub struct TaskletDispatcherImpl<S: Tasklet> {
    tasklet_id: TaskletId,
    edges: EdgeFacts,
    /// Readable Cell identity (`instance/tasklet/cell_key`) stamped
    /// as `flz.cell` on keyed-channel emits.
    cell_label: Arc<str>,
    /// The key this Cell was routed by, `None` in single-cell mode.
    /// Served to every invocation's `Ctx`, where it is both
    /// `ctx.cell_key()` and the default key of a keyed-channel emit.
    cell_key: Option<Arc<str>>,
    /// Flush-before-emit wiring, mirrored from the owning `CellSlot`
    /// so the emit output can drain this cell's just-applied changes to
    /// the durable WAL path before its output frame is written.
    flush: FlushWiring,
    /// What this Tasklet declares about its timers, resolved once.
    timers: TimerPolicy,
    _phantom: std::marker::PhantomData<fn() -> S>,
}

/// Per-cell state the dispatcher needs to drive flush-before-emit.
/// Cloned cheaply (Arc handles + a Copy header) into the emit output.
#[derive(Clone)]
struct FlushWiring {
    cell_identity: Arc<Vec<u8>>,
    change_seq: Arc<AtomicU64>,
    header: HeaderTemplate,
    keeps_state: bool,
    /// One effect record per invocation (`RelayTopology::records_effects`):
    /// the effectively-once tier or a durable steps relay. The invocation's plugin
    /// emits are captured into the record on the reliable state output instead
    /// of the primary stream.
    records_effects: bool,
    /// Checkpoint ack-granularity (`AckGranularity::Checkpoint`): the
    /// consumed-offset advances only when the interval closes, and the
    /// changes it applies — with its outputs, under
    /// `CheckpointOutputs::AtCheckpoint` — are held in the flush buffer
    /// until then. `None` under the per-input default.
    checkpoint: Option<crate::relay::topology::CheckpointPolicy>,
    /// Late-input correction policy, `None` unless the class declared a
    /// `correction_window()`. Admission and the high-water live in the
    /// engine's assembly (`runner::poll_core`), which reads this through
    /// `TaskletDispatcher::correction_policy`.
    correction: Option<crate::relay::topology::CorrectionPolicy>,
    /// A steps relay Cell: its record ships as each item settles, named on the
    /// pass sheet, and its final failure fails the stage instead of parking.
    steps: bool,
}

impl<S: Tasklet> TaskletDispatcherImpl<S> {
    fn new(
        tasklet_id: TaskletId,
        edges: EdgeFacts,
        cell_label: Arc<str>,
        cell_key: Option<Arc<str>>,
        flush: FlushWiring,
    ) -> Self {
        Self {
            tasklet_id,
            edges,
            cell_label,
            cell_key,
            flush,
            timers: crate::cell::timer::timer_policy_for::<S>(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S: Tasklet> TaskletDispatcherImpl<S> {
    /// One due firing. Assembly validated it, but the pending set is the
    /// authority: re-check under the held locks, because a restore between
    /// handout and dispatch can rebuild it.
    fn dispatch_firing(
        &self,
        fire: flz_core::timer::TimerFire,
        attempt: u32,
        is_final: bool,
        scope: CellScope<'_, S>,
    ) -> crate::cell::ItemDisposition {
        use crate::cell::ItemDisposition;
        if !scope.timers.contains(fire.kind, fire.scheduled_at) {
            return ItemDisposition::Done { ops: Vec::new() };
        }
        match self.invoke_once(Invocation::firing(&fire, attempt, is_final), scope) {
            Ok(ops) => ItemDisposition::Done { ops },
            Err(error) => ItemDisposition::RetryLater {
                item: CellItem::Timer(fire),
                error,
            },
        }
    }

    /// One input. A final attempt parks instead of retrying, so it consumes
    /// the item either way; a re-handable one gives the untouched payload back
    /// to the engine's ladder.
    fn dispatch_input(
        &self,
        payload: CellPayload,
        now: Option<u64>,
        attempt: u32,
        is_final: bool,
        scope: CellScope<'_, S>,
    ) -> crate::cell::ItemDisposition {
        use crate::cell::ItemDisposition;
        let CellPayload {
            value,
            stream,
            consumed_offset,
            call,
            replay,
            forwarded,
            hold: _,
        } = payload;
        let Some(input) = Self::typed_input(value) else {
            return ItemDisposition::Done { ops: Vec::new() };
        };
        // Engine-stamped event time, or the declared clock at entry.
        let now = now.unwrap_or_else(|| self.invocation_inputs(Some(&input)).0);

        if is_final {
            let last = Invocation::last_input(input, consumed_offset, call, replay, now, attempt);
            let ops = self.invoke_once(last, scope);
            debug_assert!(ops.is_ok(), "a final attempt parks instead of retrying");
            return ItemDisposition::Done {
                ops: ops.unwrap_or_default(),
            };
        }
        let again = Invocation::retryable_input(
            &input,
            consumed_offset,
            call.clone(),
            replay,
            now,
            attempt,
        );
        match self.invoke_once(again, scope) {
            Ok(ops) => ItemDisposition::Done { ops },
            Err(error) => ItemDisposition::RetryLater {
                item: CellItem::Input(CellPayload {
                    value: input as Arc<dyn Any + Send + Sync>,
                    stream,
                    consumed_offset,
                    call,
                    replay,
                    forwarded,
                    hold: None,
                }),
                error,
            },
        }
    }

    /// The typed input inside a mailbox payload, `None` with the mismatch
    /// reported. Consumers with different Input types in the same
    /// Relay downcast independently, because the channel wiring
    /// guarantees each only ever receives its own type.
    fn typed_input(value: Arc<dyn Any + Send + Sync>) -> Option<Arc<S::Input>> {
        match Arc::downcast::<S::Input>(value) {
            Ok(arc) => Some(arc),
            Err(_) => {
                flz_transport::notice!(
                    Error,
                    "dispatcher: payload type mismatch for tasklet {}",
                    S::NAME
                );
                None
            }
        }
    }
}

impl<S: Tasklet> TaskletDispatcher for TaskletDispatcherImpl<S> {
    fn dispatch_item(
        &self,
        work: crate::runner::poll_core::ItemAttempt,
        slot: &Arc<CellSlot>,
        state: &mut Box<dyn Any + Send>,
        timers: &mut PendingTimers,
        instance: &Weak<RelayInstance>,
    ) -> crate::cell::ItemDisposition {
        use crate::cell::ItemDisposition;

        let typed_state = state
            .downcast_mut::<CellState<S>>()
            .expect("dispatcher: state type mismatch for tasklet");

        let crate::runner::poll_core::ItemAttempt {
            mut item,
            now,
            attempt,
            is_final,
        } = work;
        // A `CloseInterval` is consumed by the batch assembler, which closes
        // the interval without an invocation — it never reaches dispatch, and
        // it is the one item with no connection of its own.
        let stream = match &item {
            CellItem::Input(payload) => Arc::clone(&payload.stream),
            CellItem::Timer(fire) => Arc::clone(&fire.stream),
            CellItem::CloseInterval => return ItemDisposition::Done { ops: Vec::new() },
        };
        // The input's hold outlives the run: the settle keeps it with the
        // buffer, a retry hands it back with the item.
        let mut hold = match &mut item {
            CellItem::Input(payload) => payload.hold.take(),
            _ => None,
        };
        let scope = CellScope {
            slot,
            state: typed_state,
            timers,
            stream: &stream,
            instance,
            hold: &mut hold,
        };
        match item {
            CellItem::Timer(fire) => self.dispatch_firing(fire, attempt, is_final, scope),
            CellItem::Input(payload) => {
                match self.dispatch_input(payload, now, attempt, is_final, scope) {
                    ItemDisposition::RetryLater {
                        item: CellItem::Input(mut retried),
                        error,
                    } => {
                        retried.hold = hold;
                        ItemDisposition::RetryLater {
                            item: CellItem::Input(retried),
                            error,
                        }
                    }
                    // Only an input of the wrong type is done without a settle.
                    settled => {
                        invocation::keep_hold::<S>(slot, hold);
                        settled
                    }
                }
            }
            CellItem::CloseInterval => unreachable!(),
        }
    }

    fn close_interval(
        &self,
        flush_buffer: &mut Box<dyn flz_core::flusher::FlushableBuffer>,
    ) -> bool {
        // Only a checkpoint class has an interval to close.
        let Some(policy) = self.flush.checkpoint else {
            return false;
        };
        let Some(buffer) = flush_buffer
            .as_any_mut()
            .downcast_mut::<crate::cell::flusher::TypedBuffer<S>>()
        else {
            return false;
        };
        buffer.close_interval(policy)
    }

    fn write_timer_ops(&self, slot: &Arc<CellSlot>, ops: Vec<TimerOp>) {
        let mut buffer = slot.flush_buffer.lock_recover();
        if let Some(typed) = buffer.as_any_mut().downcast_mut::<TypedBuffer<S>>() {
            typed.timer_ops.extend(ops);
        }
        self.flush_now(&mut **buffer);
    }

    fn interval_deadline(
        &self,
        flush_buffer: &mut Box<dyn flz_core::flusher::FlushableBuffer>,
    ) -> Option<u64> {
        let policy = self.flush.checkpoint?;
        let buffer = flush_buffer
            .as_any_mut()
            .downcast_mut::<crate::cell::flusher::TypedBuffer<S>>()?;
        buffer.staged.deadline_ms(policy)
    }

    fn tasklet_id(&self) -> TaskletId {
        self.tasklet_id
    }

    fn tasklet_name(&self) -> &'static str {
        S::NAME
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.edges.retry
    }

    fn is_event_time(&self) -> bool {
        S::CLOCK == crate::tasklet::ClockKind::EventTime
    }

    fn correction_policy(&self) -> Option<crate::relay::topology::CorrectionPolicy> {
        self.flush.correction
    }

    fn cell_label(&self) -> &str {
        &self.cell_label
    }

    fn event_time_of(&self, payload: &CellPayload) -> Option<u64> {
        if S::CLOCK != crate::tasklet::ClockKind::EventTime {
            return None;
        }
        let input = payload.value.downcast_ref::<S::Input>()?;
        Some(S::event_time(input))
    }

    fn compensate(
        &self,
        slot: &Arc<CellSlot>,
        outputs: Vec<Arc<dyn Any + Send + Sync>>,
        offset: Option<u64>,
    ) {
        // The outputs this Cell recorded forward; empty after a crash-resume,
        // which is why a compensable tasklet keeps its undo handle in State.
        let typed: Vec<Arc<S::Output>> = outputs
            .into_iter()
            .filter_map(|value| Arc::downcast::<S::Output>(value).ok())
            .collect();
        let mut state_guard = slot.state.lock_recover();
        let mut timer_guard = slot.timers.lock_recover();
        let typed_state = state_guard
            .downcast_mut::<CellState<S>>()
            .expect("compensate: state type mismatch for tasklet");
        let stream = flz_core::steps::unwired_stream();
        let instance = Weak::clone(&slot.instance);
        let scope = CellScope {
            slot,
            state: typed_state,
            timers: &mut timer_guard,
            stream: &stream,
            instance: &instance,
            hold: &mut None,
        };
        // Compensation has no current input: an event-time clock yields 0 here. A
        // failing hook is the undo's to report, never a retry.
        let _ = self.invoke_once(
            Invocation::compensation(typed, offset, self.invocation_inputs(None).0),
            scope,
        );
    }

    fn restore(
        &self,
        slot: &Arc<CellSlot>,
        payload: &crate::cell::restore::RestoreInitPayload<'_>,
    ) -> Result<flz_core::restore::RestoreOutcome, flz_core::Error> {
        let mut state = slot.state.lock_recover();
        let mut timers = slot.timers.lock_recover();
        // The same path as a cold start's `init_state` + `apply_to`, so a
        // counter that summed to N comes back at N rather than 0; the pending
        // timers fold out of the same log.
        let (restored, restored_timers, outcome) =
            crate::cell::restore::init_cell_with_payload::<S>(
                slot.cell_key.as_deref(),
                Some(payload),
            )
            .map_err(flz_core::host::restore_failed)?;
        state
            .downcast_mut::<CellState<S>>()
            .expect("restore: state type mismatch for tasklet")
            .state = restored;
        *timers = restored_timers;
        Ok(outcome)
    }

    fn snapshot_frame(
        &self,
        slot: &Arc<CellSlot>,
    ) -> Result<(Vec<u8>, u64, usize), flz_core::Error> {
        // The state lock first, as a dispatch takes it, then the buffer.
        let state = slot.state.lock_recover();
        flz_core::host::snapshot_frame(slot, &self.flush.header, || {
            let typed = state
                .downcast_ref::<CellState<S>>()
                .ok_or_else(|| snapshot_failed("the state is not this tasklet's type".into()))?;
            // Postcard — the codec `cell/restore.rs` decodes — NOT the JSON
            // migration serializer `build_state_fns` produces.
            postcard::to_allocvec(&typed.state).map_err(|e| snapshot_failed(e.to_string()))
        })
    }
}

fn snapshot_failed(reason: String) -> flz_core::Error {
    flz_core::Error::State(flz_core::Code::SnapshotFailed.with([("reason", reason.into())]))
}

/// One item through the typed dispatch, holding the slot's state and timers;
/// the flush buffer is taken under them only briefly: to settle, per emit, on a retry.
fn dispatch_locked(
    slot: &Arc<CellSlot>,
    work: crate::runner::poll_core::ItemAttempt,
) -> crate::cell::ItemDisposition {
    let mut state_guard = slot.state.lock_recover();
    let mut timer_guard = slot.timers.lock_recover();
    let instance_weak = Weak::clone(&slot.instance);
    slot.dispatcher.dispatch_item(
        work,
        slot,
        &mut state_guard,
        &mut timer_guard,
        &instance_weak,
    )
}

/// What a completed item owes outside the locks: arming can fire an
/// already-due timer straight back onto this slot's mailbox, and the drain
/// that would run it needs them released. Answers whether the batch must stop
/// here — this item's emit could not be delivered, and the parked producer
/// this replaces stopped here too (its thread never returned into the batch),
/// so the untouched tail goes back to the head of the cell's line and runs
/// once the push lands. Without that the holdback would grow by a whole batch.
fn complete_item(slot: &Arc<CellSlot>, ops: &[TimerOp], stream: &Arc<FlezhaStream>) -> bool {
    crate::cell::timer::apply_ops(slot, ops, stream);
    flz_core::timer::arm_interval_deadline(slot);
    crate::cell::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
    slot.push_deferred.load(Ordering::Acquire)
}

/// The Cell work one activation carries, or `None` when there is none to run
/// here: a steps relay undo is served in process and answered at once, and the
/// other command and prepare payloads are host-only — a typed worker never
/// receives one, because typed and host classes do not share a process.
fn cell_work(
    payload: crate::runner::poll_core::EnginePayload,
) -> Option<(Arc<CellSlot>, Vec<crate::runner::poll_core::ItemAttempt>)> {
    use crate::runner::poll_core::EnginePayload;
    match payload {
        EnginePayload::Work { slot, items, .. } => Some((slot, items)),
        EnginePayload::Command {
            slot,
            kind: flz_core::schema::ActivationKind::CompensateRun { .. },
            values,
            offset,
            reply,
            ..
        } => {
            slot.dispatcher.compensate(&slot, values, offset);
            reply.answer(flz_core::schema::ActivationOutcome::Work(Vec::new()));
            None
        }
        _ => None,
    }
}

/// Start the process-wide pool (once), sized from the host's parallelism:
/// the host half of the poll/complete seam, one named thread per worker
/// running [`run_activation`]. Called before the first input can be scheduled.
pub(crate) fn ensure_poll_workers() -> &'static Governor {
    static POOL: std::sync::OnceLock<Arc<Governor>> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        let governor =
            Governor::from_env(std::thread::available_parallelism().map_or(2, |n| n.get()));
        let (pool, core, next_id) = (Arc::clone(&governor), global(), AtomicUsize::new(0));
        governor.start(core, move |grown| {
            let (pool, id) = (Arc::clone(&pool), next_id.fetch_add(1, Ordering::Relaxed));
            std::thread::Builder::new()
                .name(format!("flz-poll-{id}"))
                .spawn(move || pool.work(core, grown, |act| run_activation(core, act)))
                .map(|_| ())
        });
        governor
    })
}

/// Run one activation: one attempt per item through the typed dispatch, then
/// either complete the batch or hand the failed head (and unprocessed tail)
/// back to the engine's ladder. The engine owns handout exclusivity,
/// admission, and the drain-tail protocol (`runner::poll_core`).
fn run_activation(
    core: &crate::runner::poll_core::PollCore,
    act: crate::runner::poll_core::EngineActivation,
) {
    use crate::cell::ItemDisposition;
    use crate::runner::poll_core::{EngineActivation, ItemAttempt};

    let EngineActivation { token, payload } = act;
    let Some((slot, items)) = cell_work(payload) else {
        return;
    };
    let steps = slot.topology.steps.is_some();
    let mut handout = Handout::new(core, token, items);
    while let Some(work) = handout.next_item() {
        // Returning with no connection is safe only because the guard
        // abandons on the way out.
        let Some(stream) = flz_core::host::stream_of(&work.item) else {
            return;
        };
        let (now, attempt, is_final) = (work.now, work.attempt, work.is_final);
        match dispatch_locked(&slot, work) {
            ItemDisposition::Done { ops } => {
                handout.item_done();
                let hand_back = complete_item(&slot, &ops, &stream);
                if steps {
                    flz_core::steps::item_settled(&slot);
                }
                if hand_back {
                    handout.hand_back();
                    return;
                }
            }
            ItemDisposition::RetryLater { item, error } => {
                if steps {
                    flz_core::steps::attempt_failed(&slot);
                }
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
