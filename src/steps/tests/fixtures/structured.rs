//! Structured classes: tasklets, chains, loops and the hybrid
//! channel wiring.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::*;
use crate::channel::ChannelBuilder;
use crate::steps::*;
use crate::tasklet::change::Change;
use crate::tasklet::{Ctx, Tasklet, TaskletError};
use crate::{Durable, Relay, RelaySettings, Run, SleepAfter};

// ---- structured tasklets + relays ----------------------------------

string_task!(EchoTask, "steps_echo_op", |input, ctx| ctx
    .emit(format!("echo:{input}")));

string_task!(Stage1, "steps_stage1", |input, ctx| ctx
    .emit(format!("{input}-1")));

string_task!(Stage2, "steps_stage2", |input, ctx| ctx
    .emit(format!("{input}-2")));

/// `steps-branching`: Stage1 fans out to Stage2 and EchoTask on one channel,
/// each branch ending at its own plugin sink. The edge is (channel,
/// consumer), so both branches read exactly what Stage1 released on `mid`.
pub(crate) fn tree_chain(c: &ChannelBuilder) {
    let mid = c.input::<String>("in").into::<Stage1>().named("mid");
    mid.into::<Stage2>().sink_named("out_b");
    mid.into::<EchoTask>().sink_named("out_c");
}

pub(crate) struct TreeSteps;
impl Relay for TreeSteps {
    fn factory_id() -> &'static str {
        "test-tree-steps"
    }
    fn channels(c: &ChannelBuilder) {
        tree_chain(c);
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

/// Two-stage linear chain: Stage1 → Stage2 → plugin.
/// The two-stage durable chain every chain/loop fixture below declares:
/// `in -> Stage1 -> mid (codec) -> Stage2 -> out -> sink`.
pub(crate) fn two_stage_chain(c: &ChannelBuilder) {
    c.input::<String>("in")
        .into::<Stage1>()
        .named("mid")
        .into::<Stage2>()
        .sink_named("out");
}

pub(crate) struct ChainSteps;
impl Relay for ChainSteps {
    fn factory_id() -> &'static str {
        "test-chain-steps"
    }
    fn channels(c: &ChannelBuilder) {
        two_stage_chain(c);
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: Durable::Local {
                sleep_after: SleepAfter::After(SHORT_SLEEP),
            },
            ..RelaySettings::default()
        }
    }
}

/// The same chain, driven as a chat-style inbox loop: one iteration per
/// post-start input, parking when the inbox goes quiet
/// (`steps-durable-loop` + `durable-suspension`).
pub(crate) struct ChatLoop;
impl Relay for ChatLoop {
    fn factory_id() -> &'static str {
        "test-chat-loop"
    }
    fn channels(c: &ChannelBuilder) {
        two_stage_chain(c);
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(30)),
            durable: Durable::Local {
                sleep_after: SleepAfter::After(SHORT_SLEEP),
            },
            ..RelaySettings::default()
        }
    }
}

string_task!(PartTurn, "steps_part_turn", |input, ctx| {
    ctx.part(format!("{input}-a"))?;
    ctx.part(format!("{input}-b"))?;
    ctx.emit(format!("done:{input}"))
});

/// A `per_input` loop whose one stage sends two parts before its answer.
pub(crate) struct PartLoop;
impl Relay for PartLoop {
    fn factory_id() -> &'static str {
        "test-part-loop"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<PartTurn>()
            .sink_named("answer");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(30)),
            durable: Durable::Local {
                sleep_after: SleepAfter::After(SHORT_SLEEP),
            },
            ..RelaySettings::default()
        }
    }
}

/// Compensable stage-0 task of the loop saga. Its undo logs the number of
/// retained outputs it was handed, which is how the test tells the two
/// rollback sources apart: the in-flight iteration carries its outputs in
/// memory (`1`), while an iteration rebuilt from the journal carries none
/// (`0`) — the documented at-least-once contract, where a durable undo
/// handle lives in Cell state.
#[derive(Default)]
pub(crate) struct LoopReserve;
#[flezha_runner::tasklet(name = "loop_reserve")]
impl Tasklet for LoopReserve {
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
        comp_log(format!("undo-loop:{}", completed_output.len()));
        Ok(())
    }
}

// Terminal task of the loop saga: fails the iteration whose turn says "boom".
string_task!(LoopMaybeFail, "loop_maybe_fail", |input, ctx| {
    if input.contains("boom") {
        return Err(TaskletError::Custom("turn failed".into()));
    }
    ctx.emit(format!("done:{input}"))
});

/// Inbox-driven saga loop, all-or-nothing: a failing turn unwinds every
/// committed turn too.
pub(crate) struct LoopSagaUndoAll;
impl Relay for LoopSagaUndoAll {
    fn factory_id() -> &'static str {
        "test-loop-saga-aon"
    }
    fn channels(c: &ChannelBuilder) {
        loop_saga_chain(c);
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(30)),
            durable: Durable::Local {
                sleep_after: SleepAfter::After(SHORT_SLEEP),
            },
            ..RelaySettings::default()
        }
    }
}

/// The same loop, keep-progress: a failing turn unwinds only itself.
pub(crate) struct LoopSagaKeepProgress;
impl Relay for LoopSagaKeepProgress {
    fn factory_id() -> &'static str {
        "test-loop-saga-keep"
    }
    fn channels(c: &ChannelBuilder) {
        loop_saga_chain(c);
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::KeepProgress,
            },
            max_duration: Some(Duration::from_secs(30)),
            durable: Durable::Local {
                sleep_after: SleepAfter::After(SHORT_SLEEP),
            },
            ..RelaySettings::default()
        }
    }
}

pub(crate) fn loop_saga_chain(c: &ChannelBuilder) {
    c.input::<String>("in")
        .into::<LoopReserve>()
        .named("mid")
        .into::<LoopMaybeFail>()
        .sink_named("out");
}

/// A durable handoff-chained loop whose terminal tasklet releases nothing:
/// no consumer is wired from it and it names no sink, so it owns no output
/// channel at all and iteration N+1 has no handoff to be re-seeded from.
pub(crate) struct ChainedLoopNoTerminalOutput;
impl Relay for ChainedLoopNoTerminalOutput {
    fn factory_id() -> &'static str {
        "test-chained-loop-no-terminal-output"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<Stage1>().into::<Stage2>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Chained,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

/// The second shape the same refusal covers: the terminal DOES own a channel,
/// but its only wiring is a plugin sink, so the channel derives plugin-bound
/// and carries no journal codec. The iteration commit would ship an empty
/// handoff and the resume would read the loop as converged.
pub(crate) struct ChainedLoopPluginTerminal;
impl Relay for ChainedLoopPluginTerminal {
    fn factory_id() -> &'static str {
        "test-chained-loop-plugin-terminal"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Stage1>()
            .named("mid")
            .into::<Stage2>()
            .sink_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Chained,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

// Splits the comma-separated args into individual items (one cell).
string_task!(SplitTask, "steps_split_op", |input, ctx| {
    for piece in input.split(',') {
        ctx.emit(piece.to_string())?;
    }
    Ok(())
});

#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct CountState {
    n: u64,
}
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Inc;
impl Change<CountState> for Inc {
    fn apply_to(&self, state: &mut CountState) {
        state.n += 1;
    }
}

/// Static-cell tasklet: routes items to `even`/`odd` cells by
/// length parity, counts per-cell (independent state), emits `<n>:<item>`.
#[derive(Default)]
pub(crate) struct FanTask;
#[flezha_runner::tasklet(name = "steps_fan_op", keys = ["even", "odd"])]
impl Tasklet for FanTask {
    type Input = String;
    type Output = String;
    type State = CountState;
    type Change = Inc;
    fn cells_for(input: &String) -> Vec<&'static str> {
        if input.len().is_multiple_of(2) {
            vec!["even"]
        } else {
            vec!["odd"]
        }
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Inc);
        let n = ctx.state().n;
        ctx.emit(format!("{n}:{input}"))?;
        Ok(())
    }
}

/// Split → fan-out across static cells → plugin.
pub(crate) struct FanSteps;
impl Relay for FanSteps {
    fn factory_id() -> &'static str {
        "test-fan-steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitTask>()
            .named("mid")
            .into::<FanTask>()
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

/// Opt-out twin of `FanSteps`: identical topology, persistence disabled —
/// its Changes drain-and-discard instead of reaching the WAL.
pub(crate) struct FanOptOutSteps;
impl Relay for FanOptOutSteps {
    fn factory_id() -> &'static str {
        "test-fan-optout-steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitTask>()
            .named("mid")
            .into::<FanTask>()
            .sink_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: Durable::Memory,
            ..RelaySettings::default()
        }
    }
}

string_task!(HybridStage1, "steps_hybrid_stage1", |input, ctx| ctx
    .emit(format!("h:{input}")));

string_task!(HybridStage2, "steps_hybrid_stage2", |input, ctx| ctx
    .emit(format!("final:{input}")));

/// Hybrid inter-tasklet channel (`sink_with`): Stage1's output
/// both feeds Stage2 AND is serialized to a plugin sink.
pub(crate) struct HybridSteps;
impl Relay for HybridSteps {
    fn factory_id() -> &'static str {
        "test-hybrid-steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<HybridStage1>()
            .sink_named_with("mid", |s: &String| format!("mid:{s}").into_bytes())
            .into::<HybridStage2>()
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
