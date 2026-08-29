//! Structured classes: procedures, chains, loops and the hybrid
//! channel wiring.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::*;
use crate::channel::ChannelBuilder;
use crate::entity::restore::RestoreInitPayload;
use crate::procedure::change::{Change, NoChange};
use crate::procedure::clock::WallClock;
use crate::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
use crate::workflow::*;

// ---- structured procedures + operatives ----------------------------------

#[derive(Default)]
pub(crate) struct EchoProc;
impl Procedure for EchoProc {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_echo_op"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("echo:{input}"))?;
        Ok(())
    }
}

/// Single-Procedure structured form, equivalent to `EmitWorkflow`-style sugar.
pub(crate) struct EchoStructured;
impl WorkflowOperative for EchoStructured {
    fn factory_id() -> &'static str {
        "test-echo-structured"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<EchoProc>()
            .named("out")
            .sink("sink");
    }
}

#[derive(Default)]
pub(crate) struct Stage1;
impl Procedure for Stage1 {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_stage1"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("{input}-1"))?;
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct Stage2;
impl Procedure for Stage2 {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_stage2"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("{input}-2"))?;
        Ok(())
    }
}

/// `workflow-branching`: Stage1 fans out to Stage2 and EchoProc on one channel,
/// each branch ending at its own plugin sink. The edge is (channel,
/// consumer), so both branches read exactly what Stage1 released on `mid`.
pub(crate) fn tree_chain(c: &ChannelBuilder) {
    let mid = c
        .input::<String>("in")
        .into::<Stage1>()
        .named("mid")
        .codec();
    mid.into::<Stage2>().named("out_b").sink("sink");
    mid.into::<EchoProc>().named("out_c").sink("sink");
}

pub(crate) struct TreeWorkflow;
impl WorkflowOperative for TreeWorkflow {
    fn factory_id() -> &'static str {
        "test-tree-workflow"
    }
    fn procedures(c: &ChannelBuilder) {
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
        .codec()
        .into::<Stage2>()
        .named("out")
        .sink("sink");
}

pub(crate) struct ChainWorkflow;
impl WorkflowOperative for ChainWorkflow {
    fn factory_id() -> &'static str {
        "test-chain-workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        two_stage_chain(c);
    }
}

/// The same chain, driven as a chat-style inbox loop: one iteration per
/// post-start input, parking when the inbox goes quiet
/// (`workflow-durable-loop` + `workflow-durable-suspension`).
/// `ChainWorkflow` with a short re-send backoff, so the barrier re-send
/// scenarios wait out their ladder inside the drain's deadline.
pub(crate) struct ResendChain;
impl WorkflowOperative for ResendChain {
    fn factory_id() -> &'static str {
        "test-resend-chain"
    }
    fn retry_backoff() -> Duration {
        Duration::from_millis(50)
    }
    fn procedures(c: &ChannelBuilder) {
        two_stage_chain(c);
    }
}

/// `ChatLoop` with a short re-send backoff.
pub(crate) struct ResendLoop;
impl WorkflowOperative for ResendLoop {
    fn factory_id() -> &'static str {
        "test-resend-loop"
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
    fn retry_backoff() -> Duration {
        Duration::from_millis(50)
    }
    fn procedures(c: &ChannelBuilder) {
        two_stage_chain(c);
    }
}

pub(crate) struct ChatLoop;
impl WorkflowOperative for ChatLoop {
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
    fn procedures(c: &ChannelBuilder) {
        two_stage_chain(c);
    }
}

/// Compensable stage-0 proc of the loop saga. Its undo logs the number of
/// retained outputs it was handed, which is how the test tells the two
/// rollback sources apart: the in-flight iteration carries its outputs in
/// memory (`1`), while an iteration rebuilt from the journal carries none
/// (`0`) — the documented at-least-once contract, where a durable undo
/// handle lives in Entity state.
#[derive(Default)]
pub(crate) struct LoopReserve;
impl Procedure for LoopReserve {
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
        comp_log(format!("undo-loop:{}", completed_output.len()));
        Ok(())
    }
}

/// Terminal proc of the loop saga: fails the iteration whose turn says "boom".
#[derive(Default)]
pub(crate) struct LoopMaybeFail;
impl Procedure for LoopMaybeFail {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "loop_maybe_fail"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        if input.contains("boom") {
            return Err(ProcedureError::Custom("turn failed".into()));
        }
        ctx.emit(format!("done:{input}"))?;
        Ok(())
    }
}

/// Inbox-driven saga loop, all-or-nothing: a failing turn unwinds every
/// committed turn too.
pub(crate) struct LoopSagaAllOrNothing;
impl WorkflowOperative for LoopSagaAllOrNothing {
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
    fn procedures(c: &ChannelBuilder) {
        loop_saga_chain(c);
    }
}

/// The same loop, keep-progress: a failing turn unwinds only itself.
pub(crate) struct LoopSagaKeepProgress;
impl WorkflowOperative for LoopSagaKeepProgress {
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
    fn procedures(c: &ChannelBuilder) {
        loop_saga_chain(c);
    }
}

pub(crate) fn loop_saga_chain(c: &ChannelBuilder) {
    c.input::<String>("in")
        .into::<LoopReserve>()
        .named("mid")
        .codec()
        .into::<LoopMaybeFail>()
        .named("out")
        .sink("sink");
}

/// A loop declared on a `run`-sugar class: no chain to iterate.
pub(crate) struct LoopWithoutChain;
impl WorkflowOperative for LoopWithoutChain {
    fn factory_id() -> &'static str {
        "test-loop-no-chain"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn run(_ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
        Ok(())
    }
}

/// A durable handoff-chained loop whose terminal procedure emits to a plugin
/// (no codec), so iteration N+1 could not be re-seeded after a crash.
pub(crate) struct ChainedLoopNoCodec;
impl WorkflowOperative for ChainedLoopNoCodec {
    fn factory_id() -> &'static str {
        "test-chained-loop-no-codec"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::Chained
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Stage1>()
            .named("mid")
            .codec()
            .into::<Stage2>()
            .named("out")
            .sink("sink");
    }
}

/// Splits the comma-separated args into individual items (one entity).
#[derive(Default)]
pub(crate) struct SplitProc;
impl Procedure for SplitProc {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_split_op"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        for piece in input.split(',') {
            ctx.emit(piece.to_string())?;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct CountState {
    n: u64,
}
#[derive(Serialize, Deserialize, Clone)]
pub(crate) struct Inc;
impl Change for Inc {}

/// Static-entity procedure: routes items to `even`/`odd` entities by
/// length parity, counts per-entity (independent state), emits `<n>:<item>`.
#[derive(Default)]
pub(crate) struct FanProc;
impl Procedure for FanProc {
    type Input = String;
    type Output = String;
    type State = CountState;
    type Change = Inc;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_fan_op"
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
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(Inc);
        let n = state.n;
        ctx.emit(format!("{n}:{input}"))?;
        Ok(())
    }
}

/// Split → fan-out across static entities → plugin.
pub(crate) struct FanWorkflow;
impl WorkflowOperative for FanWorkflow {
    fn factory_id() -> &'static str {
        "test-fan-workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitProc>()
            .named("mid")
            .codec()
            .into::<FanProc>()
            .named("out")
            .sink("sink");
    }
}

/// Opt-out twin of `FanWorkflow`: identical topology, persistence disabled —
/// its Changes drain-and-discard instead of reaching the WAL.
pub(crate) struct FanOptOutWorkflow;
impl WorkflowOperative for FanOptOutWorkflow {
    fn factory_id() -> &'static str {
        "test-fan-optout-workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitProc>()
            .named("mid")
            .into::<FanProc>()
            .named("out")
            .sink("sink");
    }
    const PERSISTENCE_ENABLED: bool = false;
}

#[derive(Default)]
pub(crate) struct HybridStage1;
impl Procedure for HybridStage1 {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_hybrid_stage1"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("h:{input}"))?;
        Ok(())
    }
}

#[derive(Default)]
pub(crate) struct HybridStage2;
impl Procedure for HybridStage2 {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_hybrid_stage2"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("final:{input}"))?;
        Ok(())
    }
}

/// Hybrid inter-procedure channel (`sink_with`): Stage1's output
/// both feeds Stage2 AND is serialized to a plugin sink.
pub(crate) struct HybridWorkflow;
impl WorkflowOperative for HybridWorkflow {
    fn factory_id() -> &'static str {
        "test-hybrid-workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<HybridStage1>()
            .named("mid")
            .codec()
            .sink_with("midsink", |s: &String| format!("mid:{s}").into_bytes())
            .into::<HybridStage2>()
            .named("out")
            .sink("out");
    }
}
