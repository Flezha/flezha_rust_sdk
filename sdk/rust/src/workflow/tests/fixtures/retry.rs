//! The per-entity retry ladder's procedures, and the step probe that
//! records who ran a stage and what a backoff held.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::*;
use crate::channel::ChannelBuilder;
use crate::entity::restore::RestoreInitPayload;
use crate::procedure::change::NoChange;
use crate::procedure::clock::WallClock;
use crate::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
use crate::workflow::*;

// ---- per-entity retry ladder procs (`workflow-entity-retry`) -------------

/// Attempts the `odd` entity has made, and items the `even` sibling has
/// processed — a retry must re-run ONLY the failing entity.
pub(crate) static ODD_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
pub(crate) static EVEN_RUNS: AtomicUsize = AtomicUsize::new(0);
/// How many of the `odd` entity's attempts fail, and how they are
/// classified — set per scenario.
pub(crate) static ODD_FAILURES: AtomicUsize = AtomicUsize::new(0);
pub(crate) static ODD_NON_RETRYABLE: AtomicBool = AtomicBool::new(false);

/// Compensable pass-through in front of the flaky stage: its undo proves
/// whether an entity failure escalated to the procedure (compensation) or
/// was absorbed by the ladder.
#[derive(Default)]
pub(crate) struct RetryReserve;
impl Procedure for RetryReserve {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "retry_reserve"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(input)?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        _completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        comp_log("undo-retry-reserve".to_string());
        Ok(())
    }
}

/// Static-entity flaky stage: the `odd` entity fails its first
/// `ODD_FAILURES` attempts (retryably, or `NonRetryable` when the scenario
/// says so) and then succeeds; the `even` entity always succeeds and
/// counts the items it saw.
#[derive(Default)]
pub(crate) struct FlakyFan;
impl Procedure for FlakyFan {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "flaky_fan"
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
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        if input.len().is_multiple_of(2) {
            EVEN_RUNS.fetch_add(1, Ordering::SeqCst);
            ctx.emit(format!("even:{input}"))?;
            return Ok(());
        }
        let attempt = ODD_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt <= ODD_FAILURES.load(Ordering::SeqCst) {
            let message = format!("flaky attempt {attempt}");
            return Err(if ODD_NON_RETRYABLE.load(Ordering::SeqCst) {
                ProcedureError::non_retryable(message)
            } else {
                ProcedureError::Custom(message)
            });
        }
        ctx.emit(format!("odd:{input}"))?;
        Ok(())
    }
}

pub(crate) fn retry_chain(c: &ChannelBuilder) {
    c.input::<String>("in")
        .into::<SplitProc>()
        .named("mid")
        .codec()
        .into::<RetryReserve>()
        .named("mid2")
        .codec()
        .into::<FlakyFan>()
        .named("out")
        .sink("sink");
}

/// Split → Reserve(compensable) → FlakyFan(even/odd). Default 3-attempt
/// ladder, backoff shrunk so the scenarios do not sleep out a real one.
pub(crate) struct RetryWorkflow;
impl WorkflowOperative for RetryWorkflow {
    fn factory_id() -> &'static str {
        "test-retry-workflow"
    }
    fn retry_backoff() -> Duration {
        Duration::from_millis(10)
    }
    fn procedures(c: &ChannelBuilder) {
        retry_chain(c);
    }
}

/// The same chain with the ladder opted out: the first entity error fails
/// the procedure, exactly as before this capability existed.
pub(crate) struct RetryFailFastWorkflow;
impl WorkflowOperative for RetryFailFastWorkflow {
    fn factory_id() -> &'static str {
        "test-retry-failfast-workflow"
    }
    fn retry_attempts() -> u32 {
        0
    }
    fn procedures(c: &ChannelBuilder) {
        retry_chain(c);
    }
}

/// The same chain with a backoff long enough that a stop must interrupt the
/// wait rather than outlast it.
pub(crate) struct RetrySlowBackoffWorkflow;
impl WorkflowOperative for RetrySlowBackoffWorkflow {
    fn factory_id() -> &'static str {
        "test-retry-slow-workflow"
    }
    fn retry_backoff() -> Duration {
        Duration::from_secs(30)
    }
    fn procedures(c: &ChannelBuilder) {
        retry_chain(c);
    }
}

// ---- who runs a stage step, and what a backoff holds -------------------

/// Every attempt the probe stage ran, as `(entity key, thread name)`.
pub(crate) static STEP_ATTEMPTS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

/// One entity per SDK worker, twice over. More entities backing off at
/// once than the pool has threads is what makes a backoff that HELD a
/// worker observable: it would starve the pool, and the core answers
/// starvation with threads above the base set.
pub(crate) fn probe_keys() -> &'static [&'static str] {
    static KEYS: std::sync::OnceLock<Vec<&'static str>> = std::sync::OnceLock::new();
    KEYS.get_or_init(|| {
        (0..crate::operative::poll_pool::base_worker_count() * 2)
            .map(|i| &*Box::leak(format!("probe{i}").into_boxed_str()))
            .collect()
    })
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct ProbeState {
    key: String,
}

/// Every entity fails its first attempt and succeeds on the retry, so the
/// whole stage sits on a backoff at the same moment, and each attempt
/// records the thread that ran it.
#[derive(Default)]
pub(crate) struct StepProbe;
impl Procedure for StepProbe {
    type Input = String;
    type Output = String;
    type State = ProbeState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "step_probe"
    }
    fn entity_keys() -> Vec<&'static str> {
        probe_keys().to_vec()
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_input: &String) -> Vec<&'static str> {
        probe_keys().to_vec()
    }
    fn init_state(entity_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> ProbeState {
        ProbeState {
            key: entity_key.unwrap_or("").to_string(),
        }
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let thread = std::thread::current()
            .name()
            .unwrap_or("<unnamed>")
            .to_string();
        let mut attempts = STEP_ATTEMPTS.lock().unwrap();
        let prior = attempts.iter().filter(|(key, _)| *key == state.key).count();
        attempts.push((state.key.clone(), thread));
        drop(attempts);
        if prior == 0 {
            return Err(ProcedureError::Custom(format!(
                "probe {} attempt 1",
                state.key
            )));
        }
        ctx.emit(format!("{}:{input}", state.key))?;
        Ok(())
    }
}

/// One flaky stage whose entities outnumber the SDK's workers, on a
/// backoff long enough that a worker holding one would be observed.
pub(crate) struct StepProbeWorkflow;
impl WorkflowOperative for StepProbeWorkflow {
    fn factory_id() -> &'static str {
        "test-step-probe-workflow"
    }
    fn retry_backoff() -> Duration {
        Duration::from_millis(200)
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<StepProbe>()
            .named("probe_out")
            .sink("sink");
    }
}

/// Arm the flaky stage: `failures` attempts of the `odd` entity fail,
/// classified retryable or not, with both attempt counters reset.
pub(crate) fn arm_flaky(failures: usize, non_retryable: bool) {
    ODD_ATTEMPTS.store(0, Ordering::SeqCst);
    EVEN_RUNS.store(0, Ordering::SeqCst);
    ODD_FAILURES.store(failures, Ordering::SeqCst);
    ODD_NON_RETRYABLE.store(non_retryable, Ordering::SeqCst);
    let _ = drain_comp_log();
}
