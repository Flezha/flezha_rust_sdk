//! Lifecycle classes: a single pass, a failure, a panic, a completion
//! result, inbox loops a tasklet ends, and a duration backstop.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::bounded::*;
use crate::cell::restore::RestoreInitPayload;
use crate::channel::ChannelBuilder;
use crate::tasklet::change::NoChange;
use crate::tasklet::clock::WallClock;
use crate::tasklet::{StateHandle, Tasklet, TaskletCtx, TaskletError};

pub(crate) static EMIT_RUNS: AtomicUsize = AtomicUsize::new(0);
pub(crate) static FAIL_RUNS: AtomicUsize = AtomicUsize::new(0);

string_task!(EmitTask, "lifecycle_emit", |input, ctx| {
    EMIT_RUNS.fetch_add(1, Ordering::SeqCst);
    ctx.emit(input)
});

string_task!(FailTask, "bounded_fail", |_input, _ctx| {
    FAIL_RUNS.fetch_add(1, Ordering::SeqCst);
    Err(TaskletError::non_retryable("boom"))
});

string_task!(PanicTask, "bounded_panic", |_input, _ctx| {
    panic!("kaboom")
});

string_task!(CompleteTask, "bounded_complete", |input, ctx| {
    ctx.complete(format!("result:{input}"))
});

// Echoes `got:<msg>` and ends its instance on `quit`.
string_task!(GotTask, "bounded_got", |input, ctx| {
    if input == "quit" {
        ctx.complete(Vec::new())
    } else {
        ctx.emit(format!("got:{input}"))
    }
});

// Completes with `first` and hands its input on.
string_task!(CompleteFirst, "bounded_complete_first", |input, ctx| {
    ctx.complete("first")?;
    ctx.emit(input)
});

string_task!(CompleteSecond, "bounded_complete_second", |_input, ctx| {
    ctx.complete("second")
});

string_task!(BoomTask, "bounded_boom", |_input, _ctx| {
    Err(TaskletError::non_retryable("boom after complete"))
});

// Hands every turn on, and on `bye` also ends its instance with `farewell`.
string_task!(ChatTurn, "bounded_chat_turn", |input, ctx| {
    if input == "bye" {
        ctx.complete("farewell")?;
    }
    ctx.emit(input)
});

/// Emits `input` and nothing else, once per run.
pub(crate) struct EmitBounded;
impl BoundedOperator for EmitBounded {
    fn factory_id() -> &'static str {
        "test-emit-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<EmitTask>().sink_named("out");
    }
}

/// A non-retryable failure skips the ladder: its tasklet runs once.
pub(crate) struct FailBounded;
impl BoundedOperator for FailBounded {
    fn factory_id() -> &'static str {
        "test-fail-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<FailTask>();
    }
}

pub(crate) struct PanicBounded;
impl BoundedOperator for PanicBounded {
    fn factory_id() -> &'static str {
        "test-panic-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").retry_max(1).into::<PanicTask>();
    }
}

/// A once pass that ends with the result its tasklet asked for.
pub(crate) struct CompleteBounded;
impl BoundedOperator for CompleteBounded {
    fn factory_id() -> &'static str {
        "test-complete-bounded"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<CompleteTask>();
    }
}

/// One iteration per inbox item until a tasklet completes it. It journals
/// nothing, so an idle inbox waits for a stop instead of parking.
pub(crate) struct LoopBounded;
impl BoundedOperator for LoopBounded {
    const PERSISTENCE_ENABLED: bool = false;
    fn factory_id() -> &'static str {
        "test-loop-bounded"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<GotTask>().sink_named("out");
    }
}

/// `LoopBounded` that never receives input: the duration backstop ends it.
pub(crate) struct TimedBounded;
impl BoundedOperator for TimedBounded {
    const PERSISTENCE_ENABLED: bool = false;
    fn factory_id() -> &'static str {
        "test-timed-bounded"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_millis(200))
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<GotTask>().sink_named("out");
    }
}

/// Two tasklets complete in one pass: the later one in the chain's commit
/// order decides the result.
pub(crate) struct TwoCompletions;
impl BoundedOperator for TwoCompletions {
    fn factory_id() -> &'static str {
        "test-two-completions"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CompleteFirst>()
            .into::<CompleteSecond>();
    }
}

/// A completion followed by a failing stage: the failure wins.
pub(crate) struct CompleteThenFail;
impl BoundedOperator for CompleteThenFail {
    fn factory_id() -> &'static str {
        "test-complete-then-fail"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CompleteFirst>()
            .into::<BoomTask>();
    }
}

/// Every cell of a static tasklet completes with its own key.
#[derive(Default)]
pub(crate) struct KeyedComplete;
impl Tasklet for KeyedComplete {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "bounded_keyed_complete"
    }
    fn cell_keys() -> Vec<&'static str> {
        vec!["a", "b"]
    }
    const CELLS_FOR_OVERRIDDEN: bool = true;
    fn cells_for(_input: &String) -> Vec<&'static str> {
        vec!["a", "b"]
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let key = ctx.cell_key().unwrap_or("-").to_string();
        ctx.complete(key)
    }
}

pub(crate) struct KeyedCompletions;
impl BoundedOperator for KeyedCompletions {
    fn factory_id() -> &'static str {
        "test-keyed-completions"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<KeyedComplete>();
    }
}

/// A durable chat loop its first tasklet ends: `in -> ChatTurn -> mid ->
/// Stage2 -> out`.
pub(crate) struct CompletingChat;
impl BoundedOperator for CompletingChat {
    fn factory_id() -> &'static str {
        "test-completing-chat"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatTurn>()
            .named("mid")
            .into::<Stage2>()
            .sink_named("out");
    }
}

/// External-stop-only: declares it does not self-terminate and has no
/// duration backstop, which registration refuses.
pub(crate) struct ExternalStopOnlyBounded;
impl BoundedOperator for ExternalStopOnlyBounded {
    fn factory_id() -> &'static str {
        "test-external-stop-only"
    }
    fn self_terminating() -> bool {
        false
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<EmitTask>();
    }
}

/// Declares no tasklet at all.
pub(crate) struct NoTaskletsBounded;
impl BoundedOperator for NoTaskletsBounded {
    fn factory_id() -> &'static str {
        "test-no-tasklets"
    }
    fn channels(_c: &ChannelBuilder) {}
}
