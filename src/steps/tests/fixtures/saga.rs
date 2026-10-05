//! Saga classes and the observation logs every compensation scenario
//! reads: what ran, what was undone, and what reached the WAL.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::*;
use crate::channel::ChannelBuilder;
use crate::steps::*;
use crate::tasklet::{Ctx, Tasklet, TaskletError};
use crate::{Relay, RelaySettings, Run};

// ---- saga compensation relays (Phase 2 §4) --------------------------

/// Process-global compensation observation log. The steps relay scenario test
/// owns it (like `EMIT_RUNS`); compensable tasklets append a marker so
/// rollback order and per-cell firing are assertable without wire
/// plumbing (a compensable inter-tasklet task's `ctx.emit` is collected, not
/// wired).
pub(crate) static COMPENSATION_LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());

// Captures the `(payload_id, frame)` pairs a durable steps relay ships to the
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
#[flezha_runner::tasklet(name = "saga_reserve")]
impl Tasklet for SagaReserve {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("reserved:{input}"))?;
        Ok(())
    }
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _ctx: &mut Ctx<'_, Self>,
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
#[flezha_runner::tasklet(name = "saga_charge")]
impl Tasklet for SagaCharge {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("charged:{input}"))?;
        Ok(())
    }
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _ctx: &mut Ctx<'_, Self>,
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
#[flezha_runner::tasklet(name = "saga_archive")]
impl Tasklet for SagaArchive {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("archived:{input}"))?;
        Ok(())
    }
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        for out in &completed_output {
            comp_log(format!("undo-archive:{out}"));
        }
        Ok(())
    }
}

/// `steps-branching` saga: reserve fans out to (charge → ship-fails) and
/// archive. The failure sits under one branch, so the abort must unwind BOTH
/// branches and only then their shared parent.
pub(crate) struct TreeSaga;
impl Relay for TreeSaga {
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

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

pub(crate) struct SagaSteps;
impl Relay for SagaSteps {
    fn factory_id() -> &'static str {
        "test-saga-steps"
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

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

/// Same chain, keep-progress mode declared — for a single no-loop pass it
/// unwinds identically (the mode only diverges with committed iterations,
/// covered by the planner unit tests). Proves `on_failure()` is plumbed
/// through the registration → run path.
pub(crate) struct KeepProgressSaga;
impl Relay for KeepProgressSaga {
    fn factory_id() -> &'static str {
        "test-keep-progress-saga"
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

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::KeepProgress,
            },
            ..RelaySettings::default()
        }
    }
}

/// Reserve → Charge → Echo(succeeds): a clean `ok` MUST NOT compensate.
pub(crate) struct CleanSaga;
impl Relay for CleanSaga {
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

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
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
#[flezha_runner::tasklet(name = "saga_fan", keys = ["even", "odd"])]
impl Tasklet for SagaFan {
    type Input = String;
    type Output = String;
    type State = FanKeyState;
    fn cells_for(input: &String) -> Vec<&'static str> {
        if input.len().is_multiple_of(2) {
            vec!["even"]
        } else {
            vec!["odd"]
        }
    }
    fn init_state(cell_key: Option<&str>) -> FanKeyState {
        FanKeyState {
            key: cell_key.unwrap_or("").to_string(),
        }
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("{}:{input}", ctx.state().key))?;
        Ok(())
    }
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        let items: Vec<String> = completed_output.iter().map(|o| o.to_string()).collect();
        comp_log(format!(
            "undo-fan:{}:[{}]",
            ctx.state().key,
            items.join(",")
        ));
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
impl Relay for FanSaga {
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

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

/// Compensable stage-0 task with a handshake so a cooperative stop lands after
/// it completes but before stage 1 (deterministic forced-stop rollback).
#[derive(Default)]
pub(crate) struct StopReserve;
#[flezha_runner::tasklet(name = "saga_stop_reserve")]
impl Tasklet for StopReserve {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        STAGE0_RAN.store(true, Ordering::SeqCst);
        while !STAGE0_RELEASE.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        ctx.emit(format!("reserved:{input}"))?;
        Ok(())
    }
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _ctx: &mut Ctx<'_, Self>,
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
impl Relay for StopSaga {
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

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

// ---- saga crash-resume tasks (§4.3) ---------------------------------------
// Compensable tasks whose `compensate` logs UNCONDITIONALLY (not per output),
// so a crash-resume — which reconstructs completed stages with EMPTY outputs
// (a tasklet's collected output was never durable) — is still observable.

#[derive(Default)]
pub(crate) struct ResumeUndoA;
#[flezha_runner::tasklet(name = "resume_undo_a")]
impl Tasklet for ResumeUndoA {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("a:{input}"))?;
        Ok(())
    }
    fn compensate(
        &mut self,
        _out: Vec<Arc<String>>,
        _ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        comp_log("undo-A".to_string());
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct ResumeUndoB;
#[flezha_runner::tasklet(name = "resume_undo_b")]
impl Tasklet for ResumeUndoB {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("b:{input}"))?;
        Ok(())
    }
    fn compensate(
        &mut self,
        _out: Vec<Arc<String>>,
        _ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        comp_log("undo-B".to_string());
        Ok(())
    }
}

// ---- a sibling that commits before its sibling fails -----------------------

/// Compensable leaf that finishes at once; its undo names what it emitted.
#[derive(Default)]
pub(crate) struct SiblingFast;
#[flezha_runner::tasklet(name = "sibling_fast")]
impl Tasklet for SiblingFast {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("fast:{input}"))?;
        Ok(())
    }
    fn compensate(
        &mut self,
        completed_output: Vec<Arc<String>>,
        _ctx: &mut Ctx<'_, Self>,
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
impl Relay for SiblingSaga {
    fn factory_id() -> &'static str {
        "test-sibling-saga"
    }
    fn channels(c: &ChannelBuilder) {
        let root = c.input::<String>("in").into::<EchoTask>().named("s_root");
        root.into::<SiblingFast>().named("s_fast");
        root.into::<SiblingSlow>().named("s_slow");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}
