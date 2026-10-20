//! One invocation of the author's code, from the call to the settle.
//!
//! The retry ladder, the emit output, the effect record and the DLQ park are the
//! same machinery for an input, a timer firing and a steps relay undo; only
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
use crate::relay::instance::RelayInstance;
use crate::steps::StepsError;
use crate::tasklet::ctx::{PartRoute, PartSource};
use crate::tasklet::{Ctx, Tasklet, TaskletError};
use crate::util::LockRecover;

impl<S: Tasklet> TaskletDispatcherImpl<S> {
    /// Fresh per-invocation capture: `Some` where the Cell's record keeps the
    /// invocation's inter-tasklet values ([`CellSlot::keeps_edges`]); a Cell
    /// that records effects captures its plugin emits there too, for the
    /// atomic effect record, instead of leaving them on a wire path.
    fn effect_capture(slot: &CellSlot, offset: Option<u64>) -> Option<RefCell<Captured>> {
        slot.keeps_edges(offset)
            .then(|| RefCell::new(Captured::default()))
    }

    /// Per-invocation determinism inputs, computed once at entry: the
    /// declared clock's value (served by `ctx.now()`) and the durable
    /// change seq (`stable_id` derivation). `input` is `None` only for
    /// `compensate`. Wall time is lazy: only the wall clock pays the
    /// `SystemTime::now()`.
    pub(super) fn invocation_inputs(&self, input: Option<&S::Input>) -> (u64, u64) {
        let seq = self.flush.change_seq.load(Ordering::Relaxed);
        let now =
            crate::tasklet::clock::evaluate::<S>(input, crate::tasklet::clock::wall_now_ms, || seq);
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
            self.flush.keeps_state,
        );
    }

    /// Drain this cell's pending changes to the durable WAL path before the
    /// output writes the matching output frame, so change N reaches the
    /// controller's WAL before N's output reaches Kafka. `None` when there is
    /// nothing to order against: a `NoChange` / non-persistent relay keeps
    /// the zero-cost inline emit, a Cell that records effects ships the
    /// changes and the outputs in ONE effect record, and a Cell that consumes
    /// input offsets ships its changes in the record carrying the offset, as
    /// does one whose buffer still holds such a record when it emits. The
    /// `changes` `RefCell` is shared with the invocation's `Ctx`; both run on
    /// this one dispatch thread, so its borrows never overlap.
    fn flush_before_emit<'a>(
        &'a self,
        slot: &'a CellSlot,
        changes: &'a RefCell<Vec<S::Change>>,
        consumes_offsets: bool,
    ) -> Option<FlushBeforeEmit<'a>> {
        if self.flush.records_effects
            || consumes_offsets
            || !self.flush.keeps_state
            || !flush_before_emit_enabled()
        {
            return None;
        }
        Some(Box::new(move || {
            with_buffer::<S, _>(slot, |buffer| {
                if !buffer.effects.is_empty() {
                    return;
                }
                // A failed attempt's changes were applied first, so they ship first.
                buffer.changes.append(&mut buffer.pending_attempt);
                buffer.changes.append(&mut changes.borrow_mut());
                self.flush_now(buffer);
            })
        }))
    }

    /// The emit output one invocation writes through, on this Cell's producer
    /// edge and the connection its item rode in on.
    fn emit_sink<'a>(
        &'a self,
        scope: &CellScope<'a, S>,
        consumed_offset: Option<u64>,
        reply_to: Option<Arc<str>>,
        effect_capture: Option<&'a RefCell<Captured>>,
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
            flush_before_emit: self.flush_before_emit(
                scope.slot,
                changes,
                consumed_offset.is_some(),
            ),
            consumed_offset,
            reply_to,
            effect_capture: effect_capture.filter(|_| self.flush.records_effects),
            edge_capture: effect_capture,
        }
    }

    /// The call into the author's code, contained: `run` on an input or
    /// `on_timer` on a firing, inside the emit output and the timer session both
    /// share. `Ok((ops, checkpointed))` is a completed attempt and what its
    /// session committed; `Err` is the failure text and whether the ladder may
    /// retry it. The panic containment is what keeps a poison payload from
    /// poisoning the held `state` lock.
    fn call_contained(
        &self,
        call: Call<S>,
        now: u64,
        attempt: u32,
        scope: &mut CellScope<'_, S>,
        changes: &RefCell<Vec<S::Change>>,
        captured: Option<&RefCell<Captured>>,
    ) -> Result<(Vec<TimerOp>, bool), (String, bool)> {
        let invocation_seq = self.flush.change_seq.load(Ordering::Relaxed);
        let replay = call.replay();
        // The removal is op 0 for a firing, so it rides the same durable
        // record as whatever `on_timer` goes on to apply and emit.
        let firing = call.firing();
        // A steps pass's call is the pass's, whichever stage emits.
        let reply_to = if self.flush.steps {
            flz_core::steps::pass_call(scope.slot)
        } else {
            call.reply_to().cloned()
        };
        let mut sink = self.emit_sink(scope, call.consumed_offset(), reply_to, captured, changes);
        let part = PartRoute::Item(PartSource {
            slot: scope.slot,
            stream: scope.stream,
            firing,
            attempt,
        });
        let state = &mut *scope.state;
        let live_timers = &mut *scope.timers;
        let session = self.timers.session(live_timers, now, firing);
        let ctx = Ctx::<S>::new(
            &mut state.state,
            changes,
            &mut sink,
            now,
            invocation_seq,
            &self.flush.cell_identity,
        )
        .with_cell_key(self.cell_key.as_deref())
        .taking(call.consumed_offset())
        .during_replay(replay);
        // A rollback is no place to arm a deadline: `compensate` answers
        // `TimersUnavailable`, in either kind, and has no work item to preview.
        let mut ctx = match &call {
            Call::Compensate { offset, .. } => ctx.compensating(*offset),
            _ => ctx.with_timers(&session).with_part(part),
        };
        let tasklet = &mut state.tasklet;
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match call {
            Call::Input { input, .. } => tasklet.run(input, &mut ctx),
            Call::Timer { fired_at, kind } => tasklet.on_timer(fired_at, kind, &mut ctx),
            Call::Compensate { outputs, .. } => tasklet.compensate(outputs, &mut ctx),
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
        captured: Option<RefCell<Captured>>,
    ) -> Result<Settled, String> {
        let (error, retryable) = match outcome {
            Ok((timers, checkpointed)) => {
                let Captured { outputs, edges } =
                    captured.map(RefCell::into_inner).unwrap_or_default();
                return Ok(Settled {
                    timers,
                    outputs,
                    edges,
                    checkpointed,
                    park_error: None,
                });
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
            edges: Vec::new(),
            checkpointed: false,
            park_error: Some(error),
        })
    }

    /// The dead letter a final failure mints. A steps relay's final attempt is
    /// its kind's: the stage fails into compensation, and none is minted.
    fn dead_letter(
        &self,
        slot: &Arc<CellSlot>,
        error: String,
        attempt: u32,
        parked_payload_bytes: Option<Vec<u8>>,
        retained_input: Option<&Arc<S::Input>>,
    ) -> Option<crate::cell::flusher::DlqEntryBody> {
        if !self.flush.steps {
            let payload = parked_payload_bytes.or_else(|| {
                retained_input.and_then(|input| crate::cell::park::extract_payload_bytes(&**input))
            });
            return Some(flz_core::host::park_entry(slot, payload, error, attempt));
        }
        flz_core::steps::note_failure(
            slot,
            StepsError::from(format!("tasklet {} failed: {error}", S::NAME)),
            None,
            attempt,
        );
        None
    }

    /// Settle one attempt into this Cell's buffer, then hold the Cell through
    /// a tier-off park until the controller acks the dead-letter append. The
    /// buffer is held only to settle, never across the park's wait; the
    /// input's hold joins the buffer once the park is durable.
    fn settle(
        &self,
        scope: CellScope<'_, S>,
        effects: crate::cell::flusher::InvocationEffects<S::Change>,
        dlq: Option<crate::cell::flusher::DlqEntryBody>,
        reply_to: Option<&Arc<str>>,
    ) {
        let slot = scope.slot;
        let consumed_offset = effects.offset;
        let park = with_buffer::<S, _>(slot, |buffer| {
            // A Cell whose relay is gone parks nothing: its input is the new owner's.
            let park = self
                .settle_into_buffer(slot, buffer, effects, dlq)
                .and_then(|entry| Some((entry, slot.instance.upgrade()?)));
            if park.is_none() {
                flz_core::cell::keep_hold(slot, buffer, scope.hold.take());
            }
            park
        });
        let Some((entry, relay)) = park else {
            return;
        };
        // Park-then-advance, never a drop: ship the park frame and hold this
        // cell (the drain thread) until the controller acks the dead-letter
        // append as durable.
        crate::cell::park::park_tier_off(
            &self.flush.cell_identity,
            &self.cell_label,
            consumed_offset,
            reply_to,
            entry,
            &*relay,
        );
        keep_hold::<S>(slot, scope.hold.take());
    }

    /// The one settlement rule the erased host also calls, then what its
    /// result asks of this Cell: a steps relay Cell's record ships at once and
    /// is named on its pass sheet, and a class with no flusher entry has
    /// nothing that would ever drain what it buffered, so it drains it here.
    /// Answers the tier-off park still owed.
    fn settle_into_buffer(
        &self,
        slot: &Arc<CellSlot>,
        buffer: &mut TypedBuffer<S>,
        effects: crate::cell::flusher::InvocationEffects<S::Change>,
        dlq: Option<crate::cell::flusher::DlqEntryBody>,
    ) -> Option<crate::cell::flusher::DlqEntryBody> {
        let channels: Vec<String> = effects.outputs.iter().map(|o| o.channel.clone()).collect();
        let tier_off_dlq = crate::cell::flusher::settle_effects::<TypedBodies<S>>(
            buffer,
            crate::cell::flusher::SettlePolicy {
                records_effects: self.flush.records_effects,
                checkpoint: self.flush.checkpoint,
                buffered: true,
            },
            effects,
            dlq,
        );
        if self.flush.steps && self.flush.records_effects {
            flz_core::steps::ship_stage_record(slot, buffer, &self.flush.header, channels);
        } else if !buffer.is_empty() && !flusher_registered::<S>(self.flush.records_effects) {
            self.flush_now(buffer);
        }
        tier_off_dlq
    }

    /// Run ONE ATTEMPT of one invocation — `run` on an input, or `on_timer`
    /// on a firing — through the emit output, the timer session and the durable
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
        let reply_to = call.reply_to().cloned();
        let firing = call.firing();
        let changes = RefCell::new(Vec::new());
        let captured = Self::effect_capture(scope.slot, offset);

        let outcome =
            self.call_contained(call, now, attempt, &mut scope, &changes, captured.as_ref());
        let mut settled =
            match self.resolve_attempt(outcome, firing, now, is_final, &mut scope, captured) {
                Ok(settled) => settled,
                // Keep the attempt's changes out of the flusher's reach until
                // the item resolves — the next attempt's settle re-takes them.
                Err(error) => {
                    with_buffer::<S, _>(scope.slot, |buffer| {
                        buffer.pending_attempt.append(&mut changes.into_inner());
                    });
                    return Err(error);
                }
            };

        let dlq = settled.park_error.take().and_then(|error| {
            self.dead_letter(
                scope.slot,
                error,
                attempt,
                parked_payload_bytes,
                retained_input,
            )
        });
        let committed = settled.timers.clone();
        flz_core::timer::hold_firing_removal(scope.slot, &mut settled.timers, firing);
        let effects = settled.into_effects(changes.into_inner(), offset, now);
        self.settle(scope, effects, dlq, reply_to.as_ref());
        Ok(committed)
    }
}

/// The call into the author's code one invocation makes. The retry ladder, the
/// emit output, the effect record and the DLQ park are the same machinery either
/// way — only this differs, which is why they share one code path.
pub(super) enum Call<S: Tasklet> {
    Input {
        input: S::Input,
        /// Durable input-log offset this invocation acknowledges. A firing
        /// carries none — it consumes no input — which is why it lives on the
        /// variant rather than beside it.
        consumed_offset: Option<u64>,
        /// The outside call waiting on the input (`flz.reply_to`), which an
        /// output answers. A firing answers nobody.
        reply_to: Option<Arc<str>>,
        replay: bool,
    },
    Timer {
        fired_at: u64,
        kind: TimerKind,
    },
    /// A steps relay undo over the outputs the Cell recorded forward, in the
    /// iteration that took the input-log `offset`.
    Compensate {
        outputs: Vec<Arc<S::Output>>,
        offset: Option<u64>,
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

    fn reply_to(&self) -> Option<&Arc<str>> {
        match self {
            Call::Input { reply_to, .. } => reply_to.as_ref(),
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

    /// A steps relay undo: one attempt, never retried, and nothing a park could
    /// record.
    pub(super) fn compensation(
        outputs: Vec<Arc<S::Output>>,
        offset: Option<u64>,
        now: u64,
    ) -> Self {
        Self {
            call: Call::Compensate { outputs, offset },
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
        reply_to: Option<Arc<str>>,
        replay: bool,
        now: u64,
        attempt: u32,
    ) -> Self {
        Self {
            parked_payload_bytes: crate::cell::park::extract_payload_bytes(&*input),
            call: Call::Input {
                input: Arc::unwrap_or_clone(input),
                consumed_offset,
                reply_to,
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
        reply_to: Option<Arc<str>>,
        replay: bool,
        now: u64,
        attempt: u32,
    ) -> Self {
        Self {
            call: Call::Input {
                input: (**input).clone(),
                consumed_offset,
                reply_to,
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
/// already holds. The flush buffer is not among them: the flusher drains it on
/// its own thread, so an invocation takes it only briefly: to settle, per emit, on a retry.
pub(super) struct CellScope<'a, S: Tasklet> {
    pub(super) slot: &'a Arc<CellSlot>,
    pub(super) state: &'a mut CellState<S>,
    pub(super) timers: &'a mut PendingTimers,
    /// The connection this invocation's emits ride — the one its item came in
    /// on, inherited by any processing-time timer it arms.
    pub(super) stream: &'a Arc<FlezhaStream>,
    pub(super) instance: &'a Weak<RelayInstance>,
    /// The input's hold on its instance's processed prefix: the settle keeps
    /// it with the buffer, a retry leaves it for the re-handed item.
    pub(super) hold: &'a mut Option<Arc<flz_core::prefix::Hold>>,
}

/// This Cell's flush buffer as its tasklet's type.
pub(super) fn typed_buffer<S: Tasklet>(
    buffer: &mut Box<dyn crate::cell::flusher::FlushableBuffer>,
) -> &mut TypedBuffer<S> {
    buffer
        .as_any_mut()
        .downcast_mut::<TypedBuffer<S>>()
        .expect("flush buffer type mismatch for tasklet")
}

/// Run `f` on this Cell's flush buffer under its lock: the flusher drains the
/// same buffer on its own thread, so an invocation holds it only this long.
fn with_buffer<S: Tasklet, R>(slot: &CellSlot, f: impl FnOnce(&mut TypedBuffer<S>) -> R) -> R {
    f(typed_buffer::<S>(&mut slot.flush_buffer.lock_recover()))
}

/// Keep an input's hold with the buffer outside a settle, so it lasts until
/// the next drain writes what the Cell recorded before it.
pub(super) fn keep_hold<S: Tasklet>(slot: &CellSlot, hold: Option<Arc<flz_core::prefix::Hold>>) {
    if hold.is_some() {
        with_buffer::<S, _>(slot, |buffer| flz_core::cell::keep_hold(slot, buffer, hold));
    }
}

/// What a Cell that records effects captures from one attempt for its
/// effect record: the plugin outputs, and the inter-tasklet values a restore
/// may have to deliver again.
#[derive(Default)]
pub(super) struct Captured {
    pub(super) outputs: Vec<crate::cell::flusher::OutputRecord>,
    pub(super) edges: Vec<crate::cell::flusher::EdgeRecord>,
}

/// What one settling attempt left. `park_error` is set when the item parks —
/// so the park rides the same record as the consumed-offset advance it
/// acknowledges.
struct Settled {
    timers: Vec<TimerOp>,
    outputs: Vec<crate::cell::flusher::OutputRecord>,
    edges: Vec<crate::cell::flusher::EdgeRecord>,
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
            edges: self.edges,
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
