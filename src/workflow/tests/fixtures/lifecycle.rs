//! Lifecycle classes: a single pass, a failure, a panic, a completion
//! result, inbox loops a procedure ends, and a duration backstop.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::channel::ChannelBuilder;
use crate::entity::restore::RestoreInitPayload;
use crate::procedure::change::NoChange;
use crate::procedure::clock::WallClock;
use crate::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
use crate::workflow::*;

pub(crate) static EMIT_RUNS: AtomicUsize = AtomicUsize::new(0);
pub(crate) static FAIL_RUNS: AtomicUsize = AtomicUsize::new(0);

string_proc!(EmitProc, "lifecycle_emit", |input, ctx| {
    EMIT_RUNS.fetch_add(1, Ordering::SeqCst);
    ctx.emit(input)
});

string_proc!(FailProc, "workflow_fail", |_input, _ctx| {
    FAIL_RUNS.fetch_add(1, Ordering::SeqCst);
    Err(ProcedureError::non_retryable("boom"))
});

string_proc!(PanicProc, "workflow_panic", |_input, _ctx| {
    panic!("kaboom")
});

string_proc!(CompleteProc, "workflow_complete", |input, ctx| {
    ctx.complete(format!("result:{input}"))
});

// Echoes `got:<msg>` and ends its instance on `quit`.
string_proc!(GotProc, "workflow_got", |input, ctx| {
    if input == "quit" {
        ctx.complete(Vec::new())
    } else {
        ctx.emit(format!("got:{input}"))
    }
});

// Completes with `first` and hands its input on.
string_proc!(CompleteFirst, "workflow_complete_first", |input, ctx| {
    ctx.complete("first")?;
    ctx.emit(input)
});

string_proc!(CompleteSecond, "workflow_complete_second", |_input, ctx| {
    ctx.complete("second")
});

string_proc!(BoomProc, "workflow_boom", |_input, _ctx| {
    Err(ProcedureError::non_retryable("boom after complete"))
});

// Hands every turn on, and on `bye` also ends its instance with `farewell`.
string_proc!(ChatTurn, "workflow_chat_turn", |input, ctx| {
    if input == "bye" {
        ctx.complete("farewell")?;
    }
    ctx.emit(input)
});

/// Emits `input` and nothing else, once per run.
pub(crate) struct EmitWorkflow;
impl WorkflowOperative for EmitWorkflow {
    fn factory_id() -> &'static str {
        "test-emit-workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in").into::<EmitProc>().sink_named("out");
    }
}

/// A non-retryable failure skips the ladder: its procedure runs once.
pub(crate) struct FailWorkflow;
impl WorkflowOperative for FailWorkflow {
    fn factory_id() -> &'static str {
        "test-fail-workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in").into::<FailProc>();
    }
}

pub(crate) struct PanicWorkflow;
impl WorkflowOperative for PanicWorkflow {
    fn factory_id() -> &'static str {
        "test-panic-workflow"
    }
    fn retry_attempts() -> u32 {
        0
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in").into::<PanicProc>();
    }
}

/// A once pass that ends with the result its procedure asked for.
pub(crate) struct CompleteWorkflow;
impl WorkflowOperative for CompleteWorkflow {
    fn factory_id() -> &'static str {
        "test-complete-workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in").into::<CompleteProc>();
    }
}

/// One iteration per inbox item until a procedure completes it. It journals
/// nothing, so an idle inbox waits for a stop instead of parking.
pub(crate) struct LoopWorkflow;
impl WorkflowOperative for LoopWorkflow {
    const PERSISTENCE_ENABLED: bool = false;
    fn factory_id() -> &'static str {
        "test-loop-workflow"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in").into::<GotProc>().sink_named("out");
    }
}

/// `LoopWorkflow` that never receives input: the duration backstop ends it.
pub(crate) struct TimedWorkflow;
impl WorkflowOperative for TimedWorkflow {
    const PERSISTENCE_ENABLED: bool = false;
    fn factory_id() -> &'static str {
        "test-timed-workflow"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_millis(200))
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in").into::<GotProc>().sink_named("out");
    }
}

/// Two procedures complete in one pass: the later one in the chain's commit
/// order decides the result.
pub(crate) struct TwoCompletions;
impl WorkflowOperative for TwoCompletions {
    fn factory_id() -> &'static str {
        "test-two-completions"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CompleteFirst>()
            .into::<CompleteSecond>();
    }
}

/// A completion followed by a failing stage: the failure wins.
pub(crate) struct CompleteThenFail;
impl WorkflowOperative for CompleteThenFail {
    fn factory_id() -> &'static str {
        "test-complete-then-fail"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CompleteFirst>()
            .into::<BoomProc>();
    }
}

/// Every entity of a static procedure completes with its own key.
#[derive(Default)]
pub(crate) struct KeyedComplete;
impl Procedure for KeyedComplete {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "workflow_keyed_complete"
    }
    fn entity_keys() -> Vec<&'static str> {
        vec!["a", "b"]
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_input: &String) -> Vec<&'static str> {
        vec!["a", "b"]
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let key = ctx.entity_key().unwrap_or("-").to_string();
        ctx.complete(key)
    }
}

pub(crate) struct KeyedCompletions;
impl WorkflowOperative for KeyedCompletions {
    fn factory_id() -> &'static str {
        "test-keyed-completions"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in").into::<KeyedComplete>();
    }
}

/// A durable chat loop its first procedure ends: `in -> ChatTurn -> mid ->
/// Stage2 -> out`.
pub(crate) struct CompletingChat;
impl WorkflowOperative for CompletingChat {
    fn factory_id() -> &'static str {
        "test-completing-chat"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatTurn>()
            .named("mid")
            .into::<Stage2>()
            .sink_named("out");
    }
}

/// External-stop-only: declares it does not self-terminate and has no
/// duration backstop, which registration refuses.
pub(crate) struct ExternalStopOnlyWorkflow;
impl WorkflowOperative for ExternalStopOnlyWorkflow {
    fn factory_id() -> &'static str {
        "test-external-stop-only"
    }
    fn self_terminating() -> bool {
        false
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in").into::<EmitProc>();
    }
}

/// Declares no procedure at all.
pub(crate) struct NoProceduresWorkflow;
impl WorkflowOperative for NoProceduresWorkflow {
    fn factory_id() -> &'static str {
        "test-no-procedures"
    }
}
