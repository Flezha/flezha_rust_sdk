//! Staged-commit and transactional-outbox tasklets: emission that must
//! not leak, and the undo that carries its own.

use std::sync::Arc;

use super::*;
use crate::bounded::*;
use crate::cell::restore::RestoreInitPayload;
use crate::channel::ChannelBuilder;
use crate::tasklet::change::NoChange;
use crate::tasklet::clock::WallClock;
use crate::tasklet::{StateHandle, Tasklet, TaskletCtx, TaskletError};

// ---- staged-commit / transactional-outbox tasks --------------------------

// Emits plugin-bound output and THEN fails: the tasklet never commits,
// so its staged output must never reach a plugin (nothing leaked).
string_task!(EmitThenFail, "bounded_emit_then_fail", |input, ctx| {
    ctx.emit(format!("leak:{input}"))?;
    Err(TaskletError::Custom("failed after emitting".into()))
});

/// One tasklet that emits then fails, straight to a plugin sink.
pub(crate) struct LeakBounded;
impl BoundedOperator for LeakBounded {
    fn factory_id() -> &'static str {
        "test-leak-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<EmitThenFail>()
            .sink_named("out");
    }
}

/// Compensable task on a hybrid channel: its forward output feeds the next
/// tasklet AND a plugin sink, so its `compensate` hook's `ctx.emit`
/// produces plugin-bound undo output (staged for the `compensated` entry).
#[derive(Default)]
pub(crate) struct UndoEmit;
impl Tasklet for UndoEmit {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "bounded_undo_emit"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("did:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        for out in &completed_output {
            ctx.emit(format!("undo:{out}"))?;
        }
        Ok(())
    }
}

/// UndoEmit(hybrid, compensable) → FailLast. The failure rolls UndoEmit
/// back; its undo output is staged and journaled in a `compensated` entry.
pub(crate) struct UndoSaga;
impl BoundedOperator for UndoSaga {
    fn factory_id() -> &'static str {
        "test-undo-saga"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<UndoEmit>()
            .sink_named_with("mid", |s: &String| s.clone().into_bytes())
            .into::<FailLast>()
            .sink_named("out");
    }
}

/// A → B → ship(fails). Durable-by-default; both A and B are compensable.
pub(crate) struct ResumeSaga;
impl BoundedOperator for ResumeSaga {
    fn factory_id() -> &'static str {
        "test-resume-saga"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ResumeUndoA>()
            .named("c1")
            .into::<ResumeUndoB>()
            .named("c2")
            .into::<SagaShipFails>()
            .sink_named("out");
    }
}
