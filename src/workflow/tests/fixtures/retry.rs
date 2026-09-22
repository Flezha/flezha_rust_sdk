//! The per-cell retry ladder's tasklets, and the step probe that
//! records who ran a stage and what a backoff held.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use super::*;
use crate::cell::restore::RestoreInitPayload;
use crate::channel::ChannelBuilder;
use crate::tasklet::change::NoChange;
use crate::tasklet::clock::WallClock;
use crate::tasklet::{StateHandle, Tasklet, TaskletCtx, TaskletError};
use crate::workflow::*;

// ---- per-cell retry ladder tasks (`workflow-cell-retry`) -------------

/// Attempts the `odd` cell has made, and items the `even` sibling has
/// processed — a retry must re-run ONLY the failing cell.
pub(crate) static ODD_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
pub(crate) static EVEN_RUNS: AtomicUsize = AtomicUsize::new(0);
/// How many of the `odd` cell's attempts fail, and how they are
/// classified — set per scenario.
pub(crate) static ODD_FAILURES: AtomicUsize = AtomicUsize::new(0);
pub(crate) static ODD_NON_RETRYABLE: AtomicBool = AtomicBool::new(false);

/// Compensable pass-through in front of the flaky stage: its undo proves
/// whether a cell failure escalated to the tasklet (compensation) or
/// was absorbed by the ladder.
#[derive(Default)]
pub(crate) struct RetryReserve;
impl Tasklet for RetryReserve {
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
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(input)?;
        Ok(())
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        _completed_output: Vec<Arc<String>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        comp_log("undo-retry-reserve".to_string());
        Ok(())
    }
}

/// Static-cell flaky stage: the `odd` cell fails its first
/// `ODD_FAILURES` attempts (retryably, or `NonRetryable` when the scenario
/// says so) and then succeeds; the `even` cell always succeeds and
/// counts the items it saw.
#[derive(Default)]
pub(crate) struct FlakyFan;
impl Tasklet for FlakyFan {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "flaky_fan"
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
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        if input.len().is_multiple_of(2) {
            EVEN_RUNS.fetch_add(1, Ordering::SeqCst);
            ctx.emit(format!("even:{input}"))?;
            return Ok(());
        }
        let attempt = ODD_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt <= ODD_FAILURES.load(Ordering::SeqCst) {
            let message = format!("flaky attempt {attempt}");
            return Err(if ODD_NON_RETRYABLE.load(Ordering::SeqCst) {
                TaskletError::non_retryable(message)
            } else {
                TaskletError::Custom(message)
            });
        }
        ctx.emit(format!("odd:{input}"))?;
        Ok(())
    }
}

/// Split → Reserve(compensable) → FlakyFan(even/odd), `FlakyFan`'s edge on a
/// ladder of `attempts` tries backing off from `backoff_ms`.
pub(crate) fn retry_chain(c: &ChannelBuilder, attempts: u32, backoff_ms: u64) {
    c.input::<String>("in")
        .into::<SplitTask>()
        .named("mid")
        .into::<RetryReserve>()
        .named("mid2")
        .retry_max(attempts)
        .retry_backoff_ms(backoff_ms)
        .into::<FlakyFan>()
        .sink_named("out");
}

/// The default 3-attempt ladder, backoff shrunk so the scenarios do not
/// sleep out a real one.
pub(crate) struct RetryWorkflow;
impl WorkflowOperative for RetryWorkflow {
    fn factory_id() -> &'static str {
        "test-retry-workflow"
    }
    fn channels(c: &ChannelBuilder) {
        retry_chain(c, 3, 10);
    }
}

/// The same chain with a one-attempt edge: the first cell error fails the
/// tasklet.
pub(crate) struct RetryFailFastWorkflow;
impl WorkflowOperative for RetryFailFastWorkflow {
    fn factory_id() -> &'static str {
        "test-retry-failfast-workflow"
    }
    fn channels(c: &ChannelBuilder) {
        retry_chain(c, 1, 10);
    }
}

/// The same chain with a backoff long enough that a stop must interrupt the
/// wait rather than outlast it.
pub(crate) struct RetrySlowBackoffWorkflow;
impl WorkflowOperative for RetrySlowBackoffWorkflow {
    fn factory_id() -> &'static str {
        "test-retry-slow-workflow"
    }
    fn channels(c: &ChannelBuilder) {
        retry_chain(c, 3, 30_000);
    }
}

// ---- who runs a stage step, and what a backoff holds -------------------

/// Every attempt the probe stage ran, as `(cell key, thread name)`.
pub(crate) static STEP_ATTEMPTS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

/// One cell per SDK worker, twice over. More cells backing off at
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

/// Every cell fails its first attempt and succeeds on the retry, so the
/// whole stage sits on a backoff at the same moment, and each attempt
/// records the thread that ran it.
#[derive(Default)]
pub(crate) struct StepProbe;
impl Tasklet for StepProbe {
    type Input = String;
    type Output = String;
    type State = ProbeState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "step_probe"
    }
    fn cell_keys() -> Vec<&'static str> {
        probe_keys().to_vec()
    }
    const CELLS_FOR_OVERRIDDEN: bool = true;
    fn cells_for(_input: &String) -> Vec<&'static str> {
        probe_keys().to_vec()
    }
    fn init_state(cell_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> ProbeState {
        ProbeState {
            key: cell_key.unwrap_or("").to_string(),
        }
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let thread = std::thread::current()
            .name()
            .unwrap_or("<unnamed>")
            .to_string();
        let mut attempts = STEP_ATTEMPTS.lock().unwrap();
        let prior = attempts.iter().filter(|(key, _)| *key == state.key).count();
        attempts.push((state.key.clone(), thread));
        drop(attempts);
        if prior == 0 {
            return Err(TaskletError::Custom(format!(
                "probe {} attempt 1",
                state.key
            )));
        }
        ctx.emit(format!("{}:{input}", state.key))?;
        Ok(())
    }
}

/// One flaky stage whose cells outnumber the SDK's workers, on a
/// backoff long enough that a worker holding one would be observed.
pub(crate) struct StepProbeWorkflow;
impl WorkflowOperative for StepProbeWorkflow {
    fn factory_id() -> &'static str {
        "test-step-probe-workflow"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .retry_backoff_ms(200)
            .into::<StepProbe>()
            .sink_named("probe_out");
    }
}

/// Arm the flaky stage: `failures` attempts of the `odd` cell fail,
/// classified retryable or not, with both attempt counters reset.
pub(crate) fn arm_flaky(failures: usize, non_retryable: bool) {
    ODD_ATTEMPTS.store(0, Ordering::SeqCst);
    EVEN_RUNS.store(0, Ordering::SeqCst);
    ODD_FAILURES.store(failures, Ordering::SeqCst);
    ODD_NON_RETRYABLE.store(non_retryable, Ordering::SeqCst);
    let _ = drain_comp_log();
}

// ---- the per-item ladder --------------------------------------------------

/// Every item the one-cell stage below attempted, in order.
pub(crate) static ITEM_ATTEMPTS: Mutex<Vec<String>> = Mutex::new(Vec::new());

// One Cell whose item `b` fails its first attempt: the retry re-hands `b`
// and what followed it, never the item before it.
string_task!(ItemFlaky, "item_flaky", |input, ctx| {
    let prior = {
        let mut attempts = ITEM_ATTEMPTS.lock().unwrap();
        let prior = attempts.iter().filter(|seen| **seen == input).count();
        attempts.push(input.clone());
        prior
    };
    if input == "b" && prior == 0 {
        return Err(TaskletError::Custom("item b attempt 1".into()));
    }
    ctx.emit(format!("item:{input}"))
});

/// Split → ItemFlaky: every piece of the message lands in the one Cell.
pub(crate) struct ItemRetryWorkflow;
impl WorkflowOperative for ItemRetryWorkflow {
    fn factory_id() -> &'static str {
        "test-item-retry-workflow"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitTask>()
            .named("pieces")
            .retry_backoff_ms(10)
            .into::<ItemFlaky>()
            .sink_named("item_out");
    }
}
