//! Call fixtures (`call_response`): operators an outside caller calls and
//! whose graph answers it through `response()` — from the first tasklet, from
//! a downstream one, from whichever of two filtered paths the input took, and
//! from a durable steps turn.

use crate::steps::stateless_step;
use crate::*;

stateless_step!(CallEcho, "call_echo", |input, ctx| ctx.emit(input));

pub(crate) struct CallEchoOperator;
impl Operator for CallEchoOperator {
    fn factory_id() -> &'static str {
        "call_echo"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<CallEcho>().response();
    }
}

stateless_step!(CallParse, "call_parse", |input, ctx| ctx.emit(input));
stateless_step!(CallAnswer, "call_answer", |input, ctx| ctx
    .emit(format!("re:{input}")));

pub(crate) struct CallDownstreamOperator;
impl Operator for CallDownstreamOperator {
    fn factory_id() -> &'static str {
        "call_downstream"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CallParse>()
            .into::<CallAnswer>()
            .response();
    }
}

// An input starting `no` takes the refused path, every other the accepted one.
stateless_step!(CallAdmit, "call_admit", |input, ctx| ctx.emit(input));
stateless_step!(CallAccept, "call_accept", |input, ctx| ctx
    .emit(format!("accepted:{input}")));
stateless_step!(CallRefuse, "call_refuse", |input, ctx| ctx
    .emit(format!("refused:{input}")));

pub(crate) struct CallSplitOperator;
impl Operator for CallSplitOperator {
    fn factory_id() -> &'static str {
        "call_split"
    }
    fn channels(c: &ChannelBuilder) {
        let admitted = c.input::<String>("in").into::<CallAdmit>();
        admitted
            .filter(|v: &String| !v.starts_with("no"))
            .into::<CallAccept>()
            .response();
        admitted
            .filter(|v: &String| v.starts_with("no"))
            .into::<CallRefuse>()
            .response();
    }
}

stateless_step!(CallTurn, "call_turn", |input, ctx| ctx.emit(input));

pub(crate) struct CallTurnSteps;
impl Operator for CallTurnSteps {
    fn factory_id() -> &'static str {
        "call_turn"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<CallTurn>().response();
    }

    fn settings() -> OperatorSettings {
        OperatorSettings {
            run: Run::Steps { each: Each::PerInput, on_failure: OnFailure::UndoAll },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..OperatorSettings::default()
        }
    }
}
