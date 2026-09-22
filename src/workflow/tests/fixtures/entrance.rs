//! Classes for the entrance: two entry channels each rooting a chain that
//! meets at one join, an entry fanning out to two roots through their own
//! transforms, a chained loop over two chains, and a once pass whose tasklet
//! reads the state its `init_values` set.

use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::cell::restore::RestoreInitPayload;
use crate::channel::ChannelBuilder;
use crate::tasklet::change::NoChange;
use crate::tasklet::clock::WallClock;
use crate::tasklet::{StateHandle, Tasklet, TaskletCtx, TaskletError};
use crate::workflow::*;

string_task!(ChainA, "entrance_a", |input, ctx| ctx
    .emit(format!("a:{input}")));
string_task!(ChainB, "entrance_b", |input, ctx| ctx
    .emit(format!("b:{input}")));
string_task!(ChainJoin, "entrance_join", |input, ctx| ctx
    .emit(format!("j:{input}")));

/// Entries `a` (A→J) and `b` (B→J): a message runs the chain of its channel
/// and the join waits for that chain's producer only.
pub(crate) struct TwoChainsWorkflow;
impl WorkflowOperative for TwoChainsWorkflow {
    const PERSISTENCE_ENABLED: bool = false;
    fn factory_id() -> &'static str {
        "test-two-chains"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(600))
    }
    fn channels(c: &ChannelBuilder) {
        let a = c.input::<String>("a").into::<ChainA>().named("a_out");
        let b = c.input::<String>("b").into::<ChainB>().named("b_out");
        a.into::<ChainJoin>().sink_named("out");
        b.into::<ChainJoin>();
    }
}

/// One entry feeding two roots, each through its own edge transform.
pub(crate) struct FanEntryWorkflow;
impl WorkflowOperative for FanEntryWorkflow {
    const PERSISTENCE_ENABLED: bool = false;
    fn factory_id() -> &'static str {
        "test-fan-entry"
    }
    fn channels(c: &ChannelBuilder) {
        let entry = c.input::<String>("in");
        entry
            .map(|s: String| -> String { format!("l:{s}") })
            .into::<ChainA>()
            .sink_named("out_a");
        entry
            .map(|s: String| -> String { format!("r:{s}") })
            .into::<ChainB>()
            .sink_named("out_b");
    }
}

/// Which root each pass of the chained loop entered through.
pub(crate) static CHAINED_ROOTS: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

string_task!(CountA, "entrance_count_a", |input, ctx| {
    CHAINED_ROOTS.lock().unwrap().push("a");
    ctx.emit(input)
});
string_task!(CountB, "entrance_count_b", |input, ctx| {
    CHAINED_ROOTS.lock().unwrap().push("b");
    ctx.emit(input)
});
// Counts its input down and releases nothing at zero, which ends the loop.
string_task!(Countdown, "entrance_countdown", |input, ctx| {
    match input.parse::<u32>().unwrap_or(0) {
        0 => Ok(()),
        n => ctx.emit((n - 1).to_string()),
    }
});

/// A chained loop with entries `a` (A→countdown) and `b` (B→countdown): the
/// carry re-enters the chain the first message picked.
pub(crate) struct ChainedChainsWorkflow;
impl WorkflowOperative for ChainedChainsWorkflow {
    const PERSISTENCE_ENABLED: bool = false;
    fn factory_id() -> &'static str {
        "test-chained-chains"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::Chained
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("a")
            .into::<CountA>()
            .named("count_a")
            .into::<Countdown>();
        c.input::<String>("b")
            .into::<CountB>()
            .named("count_b")
            .into::<Countdown>();
    }
}

/// A durable chained loop over one entry: only its first pass takes a message,
/// and the countdown's own channel is the carry the journal hands the next.
pub(crate) struct JournaledCountdownWorkflow;
impl WorkflowOperative for JournaledCountdownWorkflow {
    fn factory_id() -> &'static str {
        "test-journaled-countdown"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::Chained
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<Countdown>().named("carry");
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct Budget {
    pub(crate) value: u32,
}

/// Emits the state it holds beside its input, so a test reads what
/// `init_values` set before the message arrived.
#[derive(Default)]
pub(crate) struct BudgetTask;
impl Tasklet for BudgetTask {
    type Input = String;
    type Output = String;
    type State = Budget;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "entrance_budget"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> Budget {
        Budget::default()
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("{}:{input}", state.value))
    }
}

pub(crate) struct BudgetOnce;
impl WorkflowOperative for BudgetOnce {
    const PERSISTENCE_ENABLED: bool = false;
    fn factory_id() -> &'static str {
        "test-budget-once"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<BudgetTask>()
            .sink_named("out");
    }
}
