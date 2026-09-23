//! Saga classes and the observation logs every compensation scenario
//! reads: what ran, what was undone, and what reached the WAL.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::*;
use crate::cell::restore::RestoreInitPayload;
use crate::channel::ChannelBuilder;
use crate::tasklet::change::NoChange;
use crate::tasklet::clock::WallClock;
use crate::tasklet::{StateHandle, Tasklet, TaskletCtx, TaskletError};
use crate::workflow::*;

// ---- saga compensation operatives (Phase 2 §4) --------------------------

/// Process-global compensation observation log. The workflow scenario test
/// owns it (like `EMIT_RUNS`); compensable tasklets append a marker so
/// rollback order and per-cell firing are assertable without wire
/// plumbing (a compensable inter-tasklet task's `ctx.emit` is collected, not
/// wired).
pub(crate) static COMPENSATION_LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());

// Captures the `(payload_id, frame)` pairs a durable workflow ships to the
// reliable sink — `cell_state_change` from the flush plus the saga
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
impl Tasklet for SagaReserve {
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
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("reserved:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        for out in &completed_output {
            comp_log(format!("undo-reserve:{out}"));
        }
        Ok(())
    }
}

/// Compensable: charges (emits `charged:<input>`); undo logs `undo-charge`.
#[derive(Default)]
pub(crate) struct SagaCharge;
impl Tasklet for SagaCharge {
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
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("charged:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        for out in &completed_output {
            comp_log(format!("undo-charge:{out}"));
        }
        Ok(())
    }
}

// Terminal step that always fails — triggers reverse-order rollback of the
// completed `reserve` + `charge`. Itself uncompleted ⇒ never compensated.
string_task!(SagaShipFails, "saga_ship_fails", |_input, _ctx| Err(
    TaskletError::Custom("ship failed".into())
));

/// Reserve → Charge → Ship(fails). Default all-or-nothing rollback.
/// Compensable leaf of the fan-out branch: archives (emits `archived:<input>`);
/// undo logs `undo-archive`.
#[derive(Default)]
pub(crate) struct SagaArchive;
impl Tasklet for SagaArchive {
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
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("archived:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
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
    fn channels(c: &ChannelBuilder) {
        let t1 = c.input::<String>("in").into::<SagaReserve>().named("t1");
        t1.into::<SagaCharge>()
            .named("t2")
            .into::<SagaShipFails>()
            .sink_named("t_ship");
        // The archive leaf releases an inter-task batch nobody consumes: it
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
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SagaReserve>()
            .named("c1")
            .into::<SagaCharge>()
            .named("c2")
            .into::<SagaShipFails>()
            .sink_named("out");
    }
}

/// Same chain, keep-progress mode declared — for a single no-loop pass it
/// unwinds identically (the mode only diverges with committed iterations,
/// covered by the planner unit tests). Proves `rollback_mode()` is plumbed
/// through the registration → run path.
pub(crate) struct KeepProgressSaga;
impl WorkflowOperative for KeepProgressSaga {
    fn factory_id() -> &'static str {
        "test-keep-progress-saga"
    }
    fn rollback_mode() -> RollbackMode {
        RollbackMode::KeepProgress
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SagaReserve>()
            .named("c1")
            .into::<SagaCharge>()
            .named("c2")
            .into::<SagaShipFails>()
            .sink_named("out");
    }
}

/// Reserve → Charge → Echo(succeeds): a clean `ok` MUST NOT compensate.
pub(crate) struct CleanSaga;
impl WorkflowOperative for CleanSaga {
    fn factory_id() -> &'static str {
        "test-clean-saga"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SagaReserve>()
            .named("c1")
            .into::<SagaCharge>()
            .named("c2")
            .into::<EchoTask>()
            .sink_named("out");
    }
}

/// Static-cell compensable task: stores its cell key in state, emits
/// `<key>:<item>`, and compensates ONCE per cell (logs all that cell's
/// outputs together) so per-cell firing is assertable.
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct FanKeyState {
    key: String,
}
#[derive(Default)]
pub(crate) struct SagaFan;
impl Tasklet for SagaFan {
    type Input = String;
    type Output = String;
    type State = FanKeyState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "saga_fan"
    }
    fn cell_keys() -> Vec<&'static str> {
        vec!["even", "odd"]
    }
    const CELLS_FOR_OVERRIDDEN: bool = true;
    fn cells_for(input: &String) -> Vec<&'static str> {
        if input.len().is_multiple_of(2) {
            vec!["even"]
        } else {
            vec!["odd"]
        }
    }
    fn init_state(cell_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> FanKeyState {
        FanKeyState {
            key: cell_key.unwrap_or("").to_string(),
        }
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("{}:{input}", state.key))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let items: Vec<String> = completed_output.iter().map(|o| o.to_string()).collect();
        comp_log(format!("undo-fan:{}:[{}]", state.key, items.join(",")));
        Ok(())
    }
}

// Terminal failing task for the per-cell rollback chain.
string_task!(FailLast, "saga_fail_last", |_input, _ctx| Err(
    TaskletError::Custom("fan ship failed".into())
));

/// Split(hookless) → Fan(compensable, even/odd) → FailLast. The hookless
/// `SplitTask` is skipped during rollback; only `SagaFan` compensates, once
/// per cell.
pub(crate) struct FanSaga;
impl WorkflowOperative for FanSaga {
    fn factory_id() -> &'static str {
        "test-fan-saga"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitTask>()
            .named("mid")
            .into::<SagaFan>()
            .named("mid2")
            .into::<FailLast>()
            .sink_named("out");
    }
}

/// Compensable stage-0 task with a handshake so a cooperative stop lands after
/// it completes but before stage 1 (deterministic forced-stop rollback).
#[derive(Default)]
pub(crate) struct StopReserve;
impl Tasklet for StopReserve {
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
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
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
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
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
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<StopReserve>()
            .named("mid")
            .into::<EchoTask>()
            .sink_named("out");
    }
}

// ---- saga crash-resume tasks (§4.3) ---------------------------------------
// Compensable tasks whose `compensate` logs UNCONDITIONALLY (not per output),
// so a crash-resume — which reconstructs completed stages with EMPTY outputs
// (a tasklet's collected output was never durable) — is still observable.

#[derive(Default)]
pub(crate) struct ResumeUndoA;
impl Tasklet for ResumeUndoA {
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
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("a:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        _out: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        comp_log("undo-A".to_string());
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct ResumeUndoB;
impl Tasklet for ResumeUndoB {
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
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("b:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        _out: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        comp_log("undo-B".to_string());
        Ok(())
    }
}

// ---- a sibling that commits before its sibling fails -----------------------

/// Compensable leaf that finishes at once; its undo names what it emitted.
#[derive(Default)]
pub(crate) struct SiblingFast;
impl Tasklet for SiblingFast {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "sibling_fast"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("fast:{input}"))?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        for out in &completed_output {
            comp_log(format!("undo-{out}"));
        }
        Ok(())
    }
}

/// The instance [`SiblingSlow`] watches for its sibling's commit.
pub(crate) const SIBLING_SAGA_ID: &str = "sibling-commit";

// Fails only once the journal holds its sibling's commit, so the failure
// lands after that sibling's stage committed on its own.
string_task!(SiblingSlow, "sibling_slow", |_input, _ctx| {
    let fast = crate::cell::flusher::encode_cell_identity(SIBLING_SAGA_ID, "sibling_fast", "");
    let committed = || {
        super::super::fake_controller::TASK_COMMITS
            .lock()
            .unwrap()
            .iter()
            .any(|commit| commit.cells_ran.iter().any(|cell| cell.cell_id == fast))
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
    while !committed() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    Err(TaskletError::non_retryable(if committed() {
        "slow sibling failed after its sibling committed"
    } else {
        "slow sibling never saw its sibling commit"
    }))
});

/// A root into two leaves: `SiblingFast` commits while `SiblingSlow` still
/// runs, then `SiblingSlow` fails and the committed sibling is undone.
pub(crate) struct SiblingSaga;
impl WorkflowOperative for SiblingSaga {
    fn factory_id() -> &'static str {
        "test-sibling-saga"
    }
    fn channels(c: &ChannelBuilder) {
        let root = c.input::<String>("in").into::<EchoTask>().named("s_root");
        root.into::<SiblingFast>().named("s_fast");
        root.into::<SiblingSlow>().named("s_slow");
    }
}
