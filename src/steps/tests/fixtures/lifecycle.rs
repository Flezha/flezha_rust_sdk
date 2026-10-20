//! Lifecycle classes: a single pass, a failure, a panic, a completion
//! result, inbox loops a tasklet ends, and a duration backstop.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::channel::ChannelBuilder;
use crate::steps::*;
use crate::tasklet::{Ctx, Tasklet, TaskletError};
use crate::{Durable, Relay, RelaySettings, Run, SleepAfter};

pub(crate) static EMIT_RUNS: AtomicUsize = AtomicUsize::new(0);
pub(crate) static FAIL_RUNS: AtomicUsize = AtomicUsize::new(0);

string_task!(EmitTask, "lifecycle_emit", |input, ctx| {
    EMIT_RUNS.fetch_add(1, Ordering::SeqCst);
    ctx.emit(input)
});

string_task!(FailTask, "steps_fail", |_input, _ctx| {
    FAIL_RUNS.fetch_add(1, Ordering::SeqCst);
    Err(TaskletError::non_retryable("boom"))
});

string_task!(PanicTask, "steps_panic", |_input, _ctx| {
    panic!("kaboom")
});

string_task!(CompleteTask, "steps_complete", |input, ctx| {
    ctx.complete(format!("result:{input}"))
});

// Echoes `got:<msg>` and ends its instance on `quit`.
string_task!(GotTask, "steps_got", |input, ctx| {
    if input == "quit" {
        ctx.complete(Vec::new())
    } else {
        ctx.emit(format!("got:{input}"))
    }
});

// Completes with `first` and hands its input on.
string_task!(CompleteFirst, "steps_complete_first", |input, ctx| {
    ctx.complete("first")?;
    ctx.emit(input)
});

string_task!(CompleteSecond, "steps_complete_second", |_input, ctx| {
    ctx.complete("second")
});

string_task!(BoomTask, "steps_boom", |_input, _ctx| {
    Err(TaskletError::non_retryable("boom after complete"))
});

// Hands every turn on, and on `bye` also ends its instance with `farewell`.
string_task!(ChatTurn, "steps_chat_turn", |input, ctx| {
    if input == "bye" {
        ctx.complete("farewell")?;
    }
    ctx.emit(input)
});

/// Emits `input` and nothing else, once per run.
pub(crate) struct EmitSteps;
impl Relay for EmitSteps {
    fn factory_id() -> &'static str {
        "test-emit-steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<EmitTask>()
            .output_named("out");
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

/// A non-retryable failure skips the ladder: its tasklet runs once.
pub(crate) struct FailSteps;
impl Relay for FailSteps {
    fn factory_id() -> &'static str {
        "test-fail-steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<FailTask>();
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

pub(crate) struct PanicSteps;
impl Relay for PanicSteps {
    fn factory_id() -> &'static str {
        "test-panic-steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").retry_max(1).into::<PanicTask>();
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

/// A once pass that ends with the result its tasklet asked for.
pub(crate) struct CompleteSteps;
impl Relay for CompleteSteps {
    fn factory_id() -> &'static str {
        "test-complete-steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<CompleteTask>();
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

/// One iteration per inbox item until a tasklet completes it. It journals
/// nothing, so an idle inbox waits for a stop instead of parking.
pub(crate) struct LoopSteps;
impl Relay for LoopSteps {
    fn factory_id() -> &'static str {
        "test-loop-steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<GotTask>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            durable: Durable::Memory,
            ..RelaySettings::default()
        }
    }
}

/// `LoopSteps` that never receives input: the duration backstop ends it.
pub(crate) struct TimedSteps;
impl Relay for TimedSteps {
    fn factory_id() -> &'static str {
        "test-timed-steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<GotTask>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_millis(200)),
            durable: Durable::Memory,
            ..RelaySettings::default()
        }
    }
}

/// Two tasklets complete in one pass: the later one in the chain's commit
/// order decides the result.
pub(crate) struct TwoCompletions;
impl Relay for TwoCompletions {
    fn factory_id() -> &'static str {
        "test-two-completions"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CompleteFirst>()
            .into::<CompleteSecond>();
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

/// A completion followed by a failing stage: the failure wins.
pub(crate) struct CompleteThenFail;
impl Relay for CompleteThenFail {
    fn factory_id() -> &'static str {
        "test-complete-then-fail"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CompleteFirst>()
            .into::<BoomTask>();
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

/// Every cell of a static tasklet completes with its own key.
#[derive(Default)]
pub(crate) struct KeyedComplete;
#[flezha_runner::tasklet(name = "steps_keyed_complete", cell_keys = ["a", "b"])]
impl Tasklet for KeyedComplete {
    type Input = String;
    type Output = String;
    fn cells_for(_input: &String) -> Vec<&'static str> {
        vec!["a", "b"]
    }
    fn run(&mut self, _input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let key = ctx.cell_key().unwrap_or("-").to_string();
        ctx.complete(key)
    }
}

pub(crate) struct KeyedCompletions;
impl Relay for KeyedCompletions {
    fn factory_id() -> &'static str {
        "test-keyed-completions"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<KeyedComplete>();
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

/// A durable chat loop its first tasklet ends: `in -> ChatTurn -> mid ->
/// Stage2 -> out`.
pub(crate) struct CompletingChat;
impl Relay for CompletingChat {
    fn factory_id() -> &'static str {
        "test-completing-chat"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatTurn>()
            .named("mid")
            .into::<Stage2>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            durable: Durable::Local {
                sleep_after: SleepAfter::After(SHORT_SLEEP),
            },
            ..RelaySettings::default()
        }
    }
}

/// A loop over every message with no wall-clock stop: only an external stop
/// ends it.
pub(crate) struct NeverEndingSteps;
impl Relay for NeverEndingSteps {
    fn factory_id() -> &'static str {
        "test-never-ending"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<EmitTask>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::steps(Each::PerInput),
            ..RelaySettings::default()
        }
    }
}

/// Declares no tasklet at all.
pub(crate) struct NoTaskletsSteps;
impl Relay for NoTaskletsSteps {
    fn factory_id() -> &'static str {
        "test-no-tasklets"
    }
    fn channels(_c: &ChannelBuilder) {}

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
