//! One invocation of the author's code, from the call to the settle.
//!
//! The retry ladder, the emit sink, the effect record and the DLQ park are the
//! same machinery for an input, a timer firing and a workflow undo; only
//! [`Call`] differs, which is why all three reach `invoke_once`.

use std::any::Any;
use std::cell::RefCell;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use flz_transport::transport::FlezhaStream;

use super::cell_slot::flusher_registered;
use super::emit_sink::{flush_before_emit_enabled, CellEmitSink, FlushBeforeEmit};
use super::{CellState, TaskletDispatcherImpl};
use crate::cell::flusher::{FlushableBuffer, TypedBodies, TypedBuffer};
use crate::cell::timer::{PendingTimers, TimerKind, TimerOp};
use crate::cell::CellSlot;
use crate::operative::instance::OperativeInstance;
use crate::tasklet::state_handle::StateHandle;
use crate::tasklet::{Tasklet, TaskletCtx, TaskletError};
use crate::workflow::WorkflowError;

impl<S: Tasklet> TaskletDispatcherImpl<S> {
    /// Fresh per-invocation output capture — `Some` only under the
    /// effectively-once tier (plugin emits then form the atomic effect
    /// record instead of leaving on a wire path).
    fn effect_capture(&self) -> Option<RefCell<Vec<crate::cell::flusher::OutputRecord>>> {
        self.flush
            .effectively_once
            .then(|| RefCell::new(Vec::new()))
    }

    /// Per-invocation determinism inputs, computed once at entry: the
    /// declared Clock's value (served by `ctx.now()`) and the durable
    /// change seq (`stable_id` derivation). `input` is `None` only for
    /// `compensate`. Wall time is lazy — only WallClock pays the
    /// `SystemTime::now()`.
    pub(super) fn invocation_inputs(&self, input: Option<&S::Input>) -> (u64, u64) {
        let seq = self.flush.change_seq.load(Ordering::Relaxed);
        let now = <S::Clock as crate::tasklet::ClockFor<S::Input>>::evaluate(
            input,
            crate::tasklet::clock::wall_now_ms,
            || seq,
        );
        (now, seq)
    }

    /// Ship whatever the buffer holds now, on this Cell's identity, seq and
    /// header — the inline half of the drain the background flusher otherwise
    /// performs.
    pub(super) fn flush_now(&self, buffer: &mut dyn FlushableBuffer) {
        crate::cell::flusher::flush_one(
            buffer,
            &self.flush.cell_identity,
            &self.flush.change_seq,
            &self.flush.header,
            self.flush.persistence_enabled,
        );
    }

    /// The changes one invocation applies into, seeded with a previous failed
    /// attempt's: those state mutations stayed applied (at-least-once), so
    /// durable state must not diverge from memory. The `RefCell` built over them is
    /// shared with the flush-before-emit hook — both run on this one dispatch
    /// thread inside one invocation, so the `RefCell` borrows never overlap.
    fn carry_over(&self, buffer: &mut TypedBuffer<S>) -> Vec<S::Change> {
        let mut carried = std::mem::take(&mut buffer.changes);
        carried.append(&mut buffer.pending_attempt);
        carried
    }

    /// Drain this cell's pending changes to the durable WAL path before the
    /// sink writes the matching output frame, so change N reaches the
    /// controller's WAL before N's output reaches Kafka. `None` when there is
    /// nothing to order against: a `NoChange` / non-persistent operative keeps
    /// the zero-cost inline emit, and under the tier the changes and the
    /// outputs ship in ONE effect record.
    fn flush_before_emit<'a>(
        &self,
        changes: &'a RefCell<Vec<S::Change>>,
    ) -> Option<FlushBeforeEmit<'a>> {
        if self.flush.effectively_once
            || !self.flush.persistence_enabled
            || !flush_before_emit_enabled()
        {
            return None;
        }
        let identity = Arc::clone(&self.flush.cell_identity);
        let seq = Arc::clone(&self.flush.change_seq);
        let header = self.flush.header;
        Some(Box::new(move || {
            let mut buf = TypedBuffer::<S> {
                changes: std::mem::take(&mut *changes.borrow_mut()),
                ..Default::default()
            };
            crate::cell::flusher::flush_one(&mut buf, &identity, &seq, &header, true);
        }))
    }

    /// The emit sink one invocation writes through, on this Cell's producer
    /// edge and the connection its item rode in on.
    fn emit_sink<'a>(
        &'a self,
        scope: &CellScope<'a, S>,
        consumed_offset: Option<u64>,
        effect_outputs: Option<&'a RefCell<Vec<crate::cell::flusher::OutputRecord>>>,
        changes: &'a RefCell<Vec<S::Change>>,
    ) -> CellEmitSink<'a> {
        CellEmitSink {
            producer: scope.slot,
            output_channel_id: self.edges.output_channel_id,
            output_channel_name: self.edges.output_channel_name.as_deref(),
            keyed: self.edges.output_channel_keyed,
            cell_label: &self.cell_label,
            change_seq: &self.flush.change_seq,
            emit_ordinal: 0,
            instance: scope.instance.upgrade(),
            stream: Arc::clone(scope.stream),
            flush_before_emit: self.flush_before_emit(changes),
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
        scope: &mut CellScope<'_, S>,
        changes: &RefCell<Vec<S::Change>>,
        captured: Option<&RefCell<Vec<crate::cell::flusher::OutputRecord>>>,
    ) -> Result<(Vec<TimerOp>, bool), (String, bool)> {
        let invocation_seq = self.flush.change_seq.load(Ordering::Relaxed);
        let replay = call.replay();
        // The removal is op 0 for a firing, so it rides the same durable
        // record as whatever `on_timer` goes on to apply and emit.
        let firing = call.firing();
        let mut sink = self.emit_sink(scope, call.consumed_offset(), captured, changes);
        let state = &mut *scope.state;
        let live_timers = &mut *scope.timers;
        let session = self.timers.session(live_timers, now, firing);
        let ctx = TaskletCtx::new(&mut sink, now, invocation_seq, &self.flush.cell_identity)
            .with_cell_key(self.cell_key.as_deref())
            .during_replay(replay);
        // A rollback is no place to arm a deadline: `compensate` answers
        // `TimersUnavailable`, in either kind.
        let mut ctx = if matches!(call, Call::Compensate { .. }) {
            ctx
        } else {
            ctx.with_timers(&session)
        };
        let handle = StateHandle::<S>::new(&mut state.state, changes);

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match call {
            Call::Input { input, .. } => state.tasklet.run(input, handle, &mut ctx),
            Call::Timer { fired_at, kind } => {
                state.tasklet.on_timer(fired_at, kind, handle, &mut ctx)
            }
            Call::Compensate { outputs } => state.tasklet.compensate(outputs, handle, &mut ctx),
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
        scope: &mut CellScope<'_, S>,
        captured: Option<RefCell<Vec<crate::cell::flusher::OutputRecord>>>,
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
    ) -> crate::cell::flusher::DlqEntryBody {
        crate::cell::park::PARKED_TOTAL.fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "[flezha] tasklet {} parking poison payload to DLQ after {} attempt(s): {}",
            S::name(),
            attempt,
            error
        );
        crate::cell::flusher::DlqEntryBody {
            channel: self
                .edges
                .input_channel_name
                .as_deref()
                .unwrap_or("<unwired>")
                .to_string(),
            error,
            attempts: attempt,
            payload: parked_payload_bytes.or_else(|| {
                retained_input.and_then(|arc| crate::cell::park::extract_payload_bytes(&**arc))
            }),
        }
    }

    /// Settle one attempt into this Cell's buffer through the one settlement
    /// rule the erased host also calls, then act on what it hands back: a
    /// class with no flusher entry has nothing that would ever drain its timer
    /// ops, so it drains them here, and a tier-off park holds the Cell until
    /// the controller acks the dead-letter append.
    fn settle(
        &self,
        buffer: &mut TypedBuffer<S>,
        effects: crate::cell::flusher::InvocationEffects<S::Change>,
        dlq: Option<crate::cell::flusher::DlqEntryBody>,
    ) {
        let consumed_offset = effects.offset;
        let tier_off_dlq = crate::cell::flusher::settle_effects::<TypedBodies<S>>(
            buffer,
            crate::cell::flusher::SettlePolicy {
                effectively_once: self.flush.effectively_once,
                checkpoint: self.flush.checkpoint,
                buffered: true,
            },
            effects,
            dlq,
        );
        // A workflow Cell ships its changes as its item settles, ahead of
        // the stage's commit that follows.
        if self.flush.workflow
            || (!buffer.timer_ops.is_empty()
                && !flusher_registered::<S>(self.flush.effectively_once))
        {
            self.flush_now(buffer);
        }
        if let Some(entry) = tier_off_dlq {
            // Park-then-advance, never a drop: ship the park frame and hold
            // this cell (the drain thread) until the controller acks the
            // dead-letter append as durable.
            crate::cell::park::park_tier_off(
                &self.flush.cell_identity,
                &self.cell_label,
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
    /// failure parks here (the park rides this cell's buffer) and returns
    /// the timer ops the attempt committed, consuming the item.
    pub(super) fn invoke_once(
        &self,
        invocation: Invocation<'_, S>,
        mut scope: CellScope<'_, S>,
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
        let changes = RefCell::new(self.carry_over(scope.buffer));
        let captured = self.effect_capture();

        let outcome = self.call_contained(call, now, &mut scope, &changes, captured.as_ref());
        let mut settled =
            match self.resolve_attempt(outcome, firing, now, is_final, &mut scope, captured) {
                Ok(settled) => settled,
                // Keep the attempt's changes out of the flusher's reach until
                // the item resolves — the next attempt re-takes them.
                Err(error) => {
                    scope.buffer.pending_attempt = changes.into_inner();
                    return Err(error);
                }
            };

        // A workflow's final attempt is its kind's: the stage fails into
        // compensation, and no dead letter is minted.
        let dlq = match settled.park_error.take() {
            Some(error) if self.flush.workflow => {
                flz_core::workflow::note_failure(
                    scope.slot,
                    WorkflowError::from(format!("tasklet {} failed: {error}", S::name())),
                    None,
                    attempt,
                );
                None
            }
            Some(error) => {
                Some(self.park_entry(error, attempt, parked_payload_bytes, retained_input))
            }
            None => None,
        };
        let committed = settled.timers.clone();
        flz_core::timer::hold_firing_removal(scope.slot, &mut settled.timers, firing);
        let effects = settled.into_effects(changes.into_inner(), offset, now);
        self.settle(scope.buffer, effects, dlq);
        Ok(committed)
    }
}

/// The call into the author's code one invocation makes. The retry ladder, the
/// emit sink, the effect record and the DLQ park are the same machinery either
/// way — only this differs, which is why they share one code path.
pub(super) enum Call<S: Tasklet> {
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
    /// A workflow undo over the outputs the Cell recorded forward.
    Compensate {
        outputs: Vec<Arc<S::Output>>,
    },
}

impl<S: Tasklet> Call<S> {
    fn consumed_offset(&self) -> Option<u64> {
        match self {
            Call::Input {
                consumed_offset, ..
            } => *consumed_offset,
            Call::Timer { .. } | Call::Compensate { .. } => None,
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
            Call::Input { .. } | Call::Compensate { .. } => None,
        }
    }
}

/// One attempt of one invocation: the call, and the ladder facts that decide
/// whether a failure re-hands the item or parks it.
pub(super) struct Invocation<'a, S: Tasklet> {
    pub(super) call: Call<S>,
    /// The invocation's `ctx.now()`: the declared Clock's value for an input,
    /// the scheduled timestamp for a firing.
    pub(super) now: u64,
    pub(super) attempt: u32,
    pub(super) is_final: bool,
    pub(super) parked_payload_bytes: Option<Vec<u8>>,
    pub(super) retained_input: Option<&'a Arc<S::Input>>,
}

impl<'a, S: Tasklet> Invocation<'a, S> {
    /// A due firing. It consumes no input, so it acknowledges no offset and
    /// has no payload a park could record.
    pub(super) fn firing(
        fire: &'a flz_core::timer::TimerFire,
        attempt: u32,
        is_final: bool,
    ) -> Self {
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

    /// A workflow undo: one attempt, never retried, and nothing a park could
    /// record.
    pub(super) fn compensation(outputs: Vec<Arc<S::Output>>, now: u64) -> Self {
        Self {
            call: Call::Compensate { outputs },
            now,
            attempt: 1,
            is_final: true,
            parked_payload_bytes: None,
            retained_input: None,
        }
    }

    /// The last attempt on an input: the value moves into `run` — zero-copy
    /// when the Arc is uniquely held, a value clone when fan-out kept it
    /// shared — so its park bytes are captured first, because a park still has
    /// to record what poisoned the Cell.
    pub(super) fn last_input(
        input: Arc<S::Input>,
        consumed_offset: Option<u64>,
        replay: bool,
        now: u64,
        attempt: u32,
    ) -> Self {
        Self {
            parked_payload_bytes: crate::cell::park::extract_payload_bytes(&*input),
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
    pub(super) fn retryable_input(
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

/// The Cell's live state for one dispatch, behind the slot locks the engine
/// already holds.
pub(super) struct CellScope<'a, S: Tasklet> {
    pub(super) slot: &'a Arc<CellSlot>,
    pub(super) state: &'a mut CellState<S>,
    pub(super) buffer: &'a mut TypedBuffer<S>,
    pub(super) timers: &'a mut PendingTimers,
    /// The connection this invocation's emits ride — the one its item came in
    /// on, inherited by any processing-time timer it arms.
    pub(super) stream: &'a Arc<FlezhaStream>,
    pub(super) instance: &'a Weak<OperativeInstance>,
}

/// What one settling attempt left. `park_error` is set when the item parks —
/// so the park rides the same record as the consumed-offset advance it
/// acknowledges.
struct Settled {
    timers: Vec<TimerOp>,
    outputs: Vec<crate::cell::flusher::OutputRecord>,
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
    ) -> crate::cell::flusher::InvocationEffects<C> {
        crate::cell::flusher::InvocationEffects {
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

/// The error text of a failed attempt and whether the ladder may retry it. A
/// panic is retryable by default — the author never got to classify it.
fn classify_failure(
    failed: Result<Result<(), TaskletError>, Box<dyn Any + Send>>,
) -> (String, bool) {
    match failed {
        Ok(Err(err)) => {
            let retryable = !err.is_non_retryable();
            (err.to_string(), retryable)
        }
        Err(panic) => (crate::tasklet::error::format_panic(panic), true),
        Ok(Ok(())) => unreachable!("handled above"),
    }
}
