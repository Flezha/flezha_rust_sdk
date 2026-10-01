// -- an operator inside an operator ---------------------------------------------
//
// One stepped session per key inside a flow gateway: `key:text` inputs, each
// key's session counting its turns in its own state and answering
// `key#turns:text` on the gateway's `replies` sink. The session sleeps after
// `FLEZHA_FIXTURE_SLEEP_AFTER_MS` and wakes on its key's next message with its
// count intact, while the gateway and every other key go on. Its second stage
// fails on the text `fail` (the turn is undone) and holds `hold` until the
// release env (a crash lands between the two stages); the gateway's reply holds
// `stall` the same way (a crash lands after the session answered).
//
// `loop_gateway` embeds the chained `countdown`: `label:n` counts down one per
// pass, and only the converged `label:0` reaches the gateway's `replies`.

use crate::steps::{block_until_released, stateless_step};
use crate::*;

#[derive(Default)]
struct SessionTurnTasklet;
#[flezha_runner::tasklet(name = "session_turn")]
impl Tasklet for SessionTurnTasklet {
    type Input = String;
    type Output = String;
    type State = crate::state::CounterState;
    type Change = crate::state::CounterChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let (key, text) = input.split_once(':').unwrap_or((input.as_str(), ""));
        ctx.change(crate::state::CounterChange { delta: 1 });
        ctx.emit(format!("{key}#{}:{text}", ctx.state().value))
    }
    fn compensate(
        &mut self,
        _completed_output: Vec<std::sync::Arc<String>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        ctx.change(crate::state::CounterChange { delta: -1 });
        Ok(())
    }
}

stateless_step!(SessionCheck, "session_check", |input, ctx| {
    match input.split_once(':').map_or("", |(_, text)| text) {
        "fail" => Err(TaskletError::Custom("session_check refuses fail".into())),
        "hold" => {
            block_until_released();
            ctx.emit(input)
        }
        _ => ctx.emit(input),
    }
});

pub(crate) struct SessionSteps;
impl Operator for SessionSteps {
    fn factory_id() -> &'static str {
        "session"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("turns")
            .into::<SessionTurnTasklet>()
            .into::<SessionCheck>();
    }
    fn settings() -> OperatorSettings {
        OperatorSettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..OperatorSettings::default()
        }
    }
}

#[derive(Default)]
struct GatewayReplyTasklet;
#[flezha_runner::tasklet(name = "gateway_reply")]
impl Tasklet for GatewayReplyTasklet {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if input
            .split_once(':')
            .is_some_and(|(_, text)| text == "stall")
        {
            block_until_released();
        }
        ctx.emit(input)
    }
}

pub(crate) struct GatewayOperator;
impl Operator for GatewayOperator {
    fn factory_id() -> &'static str {
        "session_gateway"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("requests")
            .key_by(|input: &String| crate::cell::key_prefix(input))
            .into_operator::<SessionSteps, String>()
            .into::<GatewayReplyTasklet>()
            .sink_named("replies");
    }
    // It journals its input (a kept instance that may sleep), so an answer a
    // session passed it is replayed after a crash.
    fn settings() -> OperatorSettings {
        OperatorSettings {
            durable: Durable::Local {
                sleep_after: SleepAfter::After(Duration::from_secs(3600)),
            },
            ..OperatorSettings::default()
        }
    }
}

stateless_step!(CountdownStep, "countdown_step", |input, ctx| {
    let (label, n) = input.split_once(':').unwrap_or((input.as_str(), "0"));
    match n.parse::<u32>().unwrap_or(0) {
        0 => Ok(()),
        n => ctx.emit(format!("{label}:{}", n - 1)),
    }
});

pub(crate) struct CountdownSteps;
impl Operator for CountdownSteps {
    fn factory_id() -> &'static str {
        "countdown"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CountdownStep>()
            .named("carry");
    }
    fn settings() -> OperatorSettings {
        OperatorSettings {
            run: Run::steps(Each::Chained),
            ..OperatorSettings::default()
        }
    }
}

pub(crate) struct LoopGatewayOperator;
impl Operator for LoopGatewayOperator {
    fn factory_id() -> &'static str {
        "loop_gateway"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("requests")
            .into_operator::<CountdownSteps, String>()
            .into::<GatewayReplyTasklet>()
            .sink_named("replies");
    }
}
