//! Structured classes: tasklets, chains, loops and the hybrid
//! channel wiring.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::*;
use crate::bounded::*;
use crate::cell::restore::RestoreInitPayload;
use crate::channel::ChannelBuilder;
use crate::tasklet::change::{Change, NoChange};
use crate::tasklet::clock::WallClock;
use crate::tasklet::{StateHandle, Tasklet, TaskletCtx, TaskletError};

// ---- structured tasklets + operators ----------------------------------

string_task!(EchoTask, "bounded_echo_op", |input, ctx| ctx
    .emit(format!("echo:{input}")));

string_task!(Stage1, "bounded_stage1", |input, ctx| ctx
    .emit(format!("{input}-1")));

string_task!(Stage2, "bounded_stage2", |input, ctx| ctx
    .emit(format!("{input}-2")));

/// `bounded-branching`: Stage1 fans out to Stage2 and EchoTask on one channel,
/// each branch ending at its own plugin sink. The edge is (channel,
/// consumer), so both branches read exactly what Stage1 released on `mid`.
pub(crate) fn tree_chain(c: &ChannelBuilder) {
    let mid = c.input::<String>("in").into::<Stage1>().named("mid");
    mid.into::<Stage2>().sink_named("out_b");
    mid.into::<EchoTask>().sink_named("out_c");
}

pub(crate) struct TreeBounded;
impl BoundedOperator for TreeBounded {
    fn factory_id() -> &'static str {
        "test-tree-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        tree_chain(c);
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

pub(crate) struct ChainBounded;
impl BoundedOperator for ChainBounded {
    fn factory_id() -> &'static str {
        "test-chain-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        two_stage_chain(c);
    }
}

/// The same chain, driven as a chat-style inbox loop: one iteration per
/// post-start input, parking when the inbox goes quiet
/// (`bounded-durable-loop` + `bounded-durable-suspension`).
pub(crate) struct ChatLoop;
impl BoundedOperator for ChatLoop {
    fn factory_id() -> &'static str {
        "test-chat-loop"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }
    fn channels(c: &ChannelBuilder) {
        two_stage_chain(c);
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
impl Tasklet for LoopReserve {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "loop_reserve"
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
pub(crate) struct LoopSagaAllOrNothing;
impl BoundedOperator for LoopSagaAllOrNothing {
    fn factory_id() -> &'static str {
        "test-loop-saga-aon"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }
    fn channels(c: &ChannelBuilder) {
        loop_saga_chain(c);
    }
}

/// The same loop, keep-progress: a failing turn unwinds only itself.
pub(crate) struct LoopSagaKeepProgress;
impl BoundedOperator for LoopSagaKeepProgress {
    fn factory_id() -> &'static str {
        "test-loop-saga-keep"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn rollback_mode() -> RollbackMode {
        RollbackMode::KeepProgress
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(30))
    }
    fn channels(c: &ChannelBuilder) {
        loop_saga_chain(c);
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
impl BoundedOperator for ChainedLoopNoTerminalOutput {
    fn factory_id() -> &'static str {
        "test-chained-loop-no-terminal-output"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::Chained
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<Stage1>().into::<Stage2>();
    }
}

/// The second shape the same refusal covers: the terminal DOES own a channel,
/// but its only wiring is a plugin sink, so the channel derives plugin-bound
/// and carries no journal codec. The iteration commit would ship an empty
/// handoff and the resume would read the loop as converged.
pub(crate) struct ChainedLoopPluginTerminal;
impl BoundedOperator for ChainedLoopPluginTerminal {
    fn factory_id() -> &'static str {
        "test-chained-loop-plugin-terminal"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::Chained
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Stage1>()
            .named("mid")
            .into::<Stage2>()
            .sink_named("out");
    }
}

// Splits the comma-separated args into individual items (one cell).
string_task!(SplitTask, "bounded_split_op", |input, ctx| {
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
impl Change for Inc {}

/// Static-cell tasklet: routes items to `even`/`odd` cells by
/// length parity, counts per-cell (independent state), emits `<n>:<item>`.
#[derive(Default)]
pub(crate) struct FanTask;
impl Tasklet for FanTask {
    type Input = String;
    type Output = String;
    type State = CountState;
    type Change = Inc;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "bounded_fan_op"
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
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> CountState {
        CountState::default()
    }
    fn apply_change(state: &mut CountState, _: &Inc) {
        state.n += 1;
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        state.apply(Inc);
        let n = state.n;
        ctx.emit(format!("{n}:{input}"))?;
        Ok(())
    }
}

/// Split → fan-out across static cells → plugin.
pub(crate) struct FanBounded;
impl BoundedOperator for FanBounded {
    fn factory_id() -> &'static str {
        "test-fan-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitTask>()
            .named("mid")
            .into::<FanTask>()
            .sink_named("out");
    }
}

/// Opt-out twin of `FanBounded`: identical topology, persistence disabled —
/// its Changes drain-and-discard instead of reaching the WAL.
pub(crate) struct FanOptOutBounded;
impl BoundedOperator for FanOptOutBounded {
    fn factory_id() -> &'static str {
        "test-fan-optout-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitTask>()
            .named("mid")
            .into::<FanTask>()
            .sink_named("out");
    }
    const PERSISTENCE_ENABLED: bool = false;
}

string_task!(HybridStage1, "bounded_hybrid_stage1", |input, ctx| ctx
    .emit(format!("h:{input}")));

string_task!(HybridStage2, "bounded_hybrid_stage2", |input, ctx| ctx
    .emit(format!("final:{input}")));

/// Hybrid inter-tasklet channel (`sink_with`): Stage1's output
/// both feeds Stage2 AND is serialized to a plugin sink.
pub(crate) struct HybridBounded;
impl BoundedOperator for HybridBounded {
    fn factory_id() -> &'static str {
        "test-hybrid-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<HybridStage1>()
            .sink_named_with("mid", |s: &String| format!("mid:{s}").into_bytes())
            .into::<HybridStage2>()
            .sink_named("out");
    }
}
