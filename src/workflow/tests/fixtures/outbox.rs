//! Staged-commit and transactional-outbox procedures: emission that must
//! not leak, and the undo that carries its own.

use std::sync::Arc;

use super::*;
use crate::channel::ChannelBuilder;
use crate::entity::restore::RestoreInitPayload;
use crate::procedure::change::NoChange;
use crate::procedure::clock::WallClock;
use crate::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
use crate::workflow::*;

// ---- staged-commit / transactional-outbox procs --------------------------

/// Emits plugin-bound output and THEN fails: the procedure never commits,
/// so its staged output must never reach a plugin (nothing leaked).
#[derive(Default)]
pub(crate) struct EmitThenFail;
impl Procedure for EmitThenFail {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_emit_then_fail"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("leak:{input}"))?;
        Err(ProcedureError::Custom("failed after emitting".into()))
    }
}

/// One procedure that emits then fails, straight to a plugin sink.
pub(crate) struct LeakWorkflow;
impl WorkflowOperative for LeakWorkflow {
    fn factory_id() -> &'static str {
        "test-leak-workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<EmitThenFail>()
            .sink_named("out");
    }
}

/// Compensable proc on a hybrid channel: its forward output feeds the next
/// procedure AND a plugin sink, so its `compensate` hook's `ctx.emit`
/// produces plugin-bound undo output (staged for the `compensated` entry).
#[derive(Default)]
pub(crate) struct UndoEmit;
impl Procedure for UndoEmit {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_undo_emit"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("did:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        for out in &completed_output {
            ctx.emit(format!("undo:{out}"))?;
        }
        Ok(())
    }
}

/// UndoEmit(hybrid, compensable) → FailLast. The failure rolls UndoEmit
/// back; its undo output is staged and journaled in a `compensated` entry.
pub(crate) struct UndoSaga;
impl WorkflowOperative for UndoSaga {
    fn factory_id() -> &'static str {
        "test-undo-saga"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<UndoEmit>()
            .sink_named_with("mid", |s: &String| s.clone().into_bytes())
            .into::<FailLast>()
            .sink_named("out");
    }
}

/// A → B → ship(fails). Durable-by-default; both A and B are compensable.
pub(crate) struct ResumeSaga;
impl WorkflowOperative for ResumeSaga {
    fn factory_id() -> &'static str {
        "test-resume-saga"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ResumeUndoA>()
            .named("c1")
            .into::<ResumeUndoB>()
            .named("c2")
            .into::<SagaShipFails>()
            .sink_named("out");
    }
}
