//! Saga classes and the observation logs every compensation scenario
//! reads: what ran, what was undone, and what reached the WAL.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::*;
use crate::channel::ChannelBuilder;
use crate::entity::restore::RestoreInitPayload;
use crate::procedure::change::NoChange;
use crate::procedure::clock::WallClock;
use crate::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
use crate::workflow::*;

// ---- saga compensation operatives (Phase 2 §4) --------------------------

/// Process-global compensation observation log. The driver-observation test
/// owns it (like `EMIT_RUNS`); compensable procedures append a marker so
/// rollback order and per-entity firing are assertable without wire
/// plumbing (a compensable inter-procedure proc's `ctx.emit` is collected, not
/// wired).
pub(crate) static COMPENSATION_LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());

// Captures the `(payload_id, frame)` pairs a durable workflow ships to the
// reliable sink — `entity_state_change` from the flush plus the saga
// `compensation_intent` / `compensation_done` markers.
pub(crate) static WAL_FRAMES: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

pub(crate) fn count_wal_frames(payload_id: &str) -> usize {
    WAL_FRAMES
        .lock()
        .unwrap()
        .iter()
        .filter(|(pid, _)| pid == payload_id)
        .count()
}

pub(crate) fn comp_log(entry: String) {
    COMPENSATION_LOG.lock().unwrap().push(entry);
}

pub(crate) fn drain_comp_log() -> Vec<String> {
    std::mem::take(&mut *COMPENSATION_LOG.lock().unwrap())
}

/// Forced-stop handshake: `StopReserve` signals it has started, then parks
/// until the test trips cancellation, so the "stop after partial progress"
/// path is deterministic (cancel is observed before the next stage's
/// pre-check rather than racing the fast chain).
pub(crate) static STAGE0_RAN: AtomicBool = AtomicBool::new(false);
pub(crate) static STAGE0_RELEASE: AtomicBool = AtomicBool::new(false);

/// Compensable: reserves (emits `reserved:<arg>`); undo logs `undo-reserve`.
#[derive(Default)]
pub(crate) struct SagaReserve;
impl Procedure for SagaReserve {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "saga_reserve"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("reserved:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        for out in &completed_output {
            comp_log(format!("undo-reserve:{out}"));
        }
        Ok(())
    }
}

/// Compensable: charges (emits `charged:<input>`); undo logs `undo-charge`.
#[derive(Default)]
pub(crate) struct SagaCharge;
impl Procedure for SagaCharge {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "saga_charge"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("charged:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        for out in &completed_output {
            comp_log(format!("undo-charge:{out}"));
        }
        Ok(())
    }
}

/// Terminal step that always fails — triggers reverse-order rollback of the
/// completed `reserve` + `charge`. Itself uncompleted ⇒ never compensated.
#[derive(Default)]
pub(crate) struct SagaShipFails;
impl Procedure for SagaShipFails {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "saga_ship_fails"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: String,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Err(ProcedureError::Custom("ship failed".into()))
    }
}

/// Reserve → Charge → Ship(fails). Default all-or-nothing rollback.
/// Compensable leaf of the fan-out branch: archives (emits `archived:<input>`);
/// undo logs `undo-archive`.
#[derive(Default)]
pub(crate) struct SagaArchive;
impl Procedure for SagaArchive {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "saga_archive"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("archived:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        for out in &completed_output {
            comp_log(format!("undo-archive:{out}"));
        }
        Ok(())
    }
}

/// `workflow-branching` saga: reserve fans out to (charge → ship-fails) and
/// archive. The failure sits under one branch, so the abort must unwind BOTH
/// branches and only then their shared parent.
pub(crate) struct TreeSaga;
impl WorkflowOperative for TreeSaga {
    fn factory_id() -> &'static str {
        "test-tree-saga"
    }
    fn procedures(c: &ChannelBuilder) {
        let t1 = c.input::<String>("in").into::<SagaReserve>().named("t1");
        t1.into::<SagaCharge>()
            .named("t2")
            .into::<SagaShipFails>()
            .named("t_ship")
            .sink("sink");
        // The archive leaf releases an inter-proc batch nobody consumes: it
        // stays a leaf, and its retained output is what its undo reverses
        // (a plugin-bound leaf would stage instead of collect, leaving the
        // hook nothing to name).
        t1.into::<SagaArchive>().named("t_out");
    }
}

pub(crate) struct SagaWorkflow;
impl WorkflowOperative for SagaWorkflow {
    fn factory_id() -> &'static str {
        "test-saga-workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SagaReserve>()
            .named("c1")
            .into::<SagaCharge>()
            .named("c2")
            .into::<SagaShipFails>()
            .named("out")
            .sink("sink");
    }
}

/// Same chain, keep-progress mode declared — for a single no-loop pass it
/// unwinds identically (the mode only diverges with committed iterations,
/// covered by the planner unit tests). Proves `rollback_mode()` is plumbed
/// through the registration → driver path.
pub(crate) struct KeepProgressSaga;
impl WorkflowOperative for KeepProgressSaga {
    fn factory_id() -> &'static str {
        "test-keep-progress-saga"
    }
    fn rollback_mode() -> RollbackMode {
        RollbackMode::KeepProgress
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SagaReserve>()
            .named("c1")
            .into::<SagaCharge>()
            .named("c2")
            .into::<SagaShipFails>()
            .named("out")
            .sink("sink");
    }
}

/// Reserve → Charge → Echo(succeeds): a clean `ok` MUST NOT compensate.
pub(crate) struct CleanSaga;
impl WorkflowOperative for CleanSaga {
    fn factory_id() -> &'static str {
        "test-clean-saga"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SagaReserve>()
            .named("c1")
            .into::<SagaCharge>()
            .named("c2")
            .into::<EchoProc>()
            .named("out")
            .sink("sink");
    }
}

/// Static-entity compensable proc: stores its entity key in state, emits
/// `<key>:<item>`, and compensates ONCE per entity (logs all that entity's
/// outputs together) so per-entity firing is assertable.
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct FanKeyState {
    key: String,
}
#[derive(Default)]
pub(crate) struct SagaFan;
impl Procedure for SagaFan {
    type Input = String;
    type Output = String;
    type State = FanKeyState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "saga_fan"
    }
    fn entity_keys() -> Vec<&'static str> {
        vec!["even", "odd"]
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(input: &String) -> Vec<&'static str> {
        if input.len().is_multiple_of(2) {
            vec!["even"]
        } else {
            vec!["odd"]
        }
    }
    fn init_state(entity_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> FanKeyState {
        FanKeyState {
            key: entity_key.unwrap_or("").to_string(),
        }
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("{}:{input}", state.key))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let items: Vec<String> = completed_output.iter().map(|o| o.to_string()).collect();
        comp_log(format!("undo-fan:{}:[{}]", state.key, items.join(",")));
        Ok(())
    }
}

/// Terminal failing proc for the per-entity rollback chain.
#[derive(Default)]
pub(crate) struct FailLast;
impl Procedure for FailLast {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "saga_fail_last"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: String,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Err(ProcedureError::Custom("fan ship failed".into()))
    }
}

/// Split(hookless) → Fan(compensable, even/odd) → FailLast. The hookless
/// `SplitProc` is skipped during rollback; only `SagaFan` compensates, once
/// per entity.
pub(crate) struct FanSaga;
impl WorkflowOperative for FanSaga {
    fn factory_id() -> &'static str {
        "test-fan-saga"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitProc>()
            .named("mid")
            .into::<SagaFan>()
            .named("mid2")
            .into::<FailLast>()
            .named("out")
            .sink("sink");
    }
}

/// Compensable stage-0 proc with a handshake so a cooperative stop lands after
/// it completes but before stage 1 (deterministic forced-stop rollback).
#[derive(Default)]
pub(crate) struct StopReserve;
impl Procedure for StopReserve {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "saga_stop_reserve"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        STAGE0_RAN.store(true, Ordering::SeqCst);
        while !STAGE0_RELEASE.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        ctx.emit(format!("reserved:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        for out in &completed_output {
            comp_log(format!("undo-stop-reserve:{out}"));
        }
        Ok(())
    }
}

/// StopReserve → Echo. A cooperative stop after StopReserve completes
/// compensates only StopReserve; Echo never runs.
pub(crate) struct StopSaga;
impl WorkflowOperative for StopSaga {
    fn factory_id() -> &'static str {
        "test-stop-saga"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<StopReserve>()
            .named("mid")
            .into::<EchoProc>()
            .named("out")
            .sink("sink");
    }
}

// ---- saga crash-resume procs (§4.3) ---------------------------------------
// Compensable procs whose `compensate` logs UNCONDITIONALLY (not per output),
// so a crash-resume — which reconstructs completed stages with EMPTY outputs
// (a procedure's collected output was never durable) — is still observable.

#[derive(Default)]
pub(crate) struct ResumeUndoA;
impl Procedure for ResumeUndoA {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "resume_undo_a"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("a:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        _out: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        comp_log("undo-A".to_string());
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct ResumeUndoB;
impl Procedure for ResumeUndoB {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "resume_undo_b"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("b:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        _out: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        comp_log("undo-B".to_string());
        Ok(())
    }
}
