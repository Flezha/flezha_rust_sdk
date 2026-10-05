//! Call fixtures (`call_response`): relays an outside caller calls and
//! whose sink output answers it — from the first tasklet, from a downstream
//! one, from whichever of two filtered paths the input took, and from a
//! durable steps turn.

use crate::steps::stateless_step;
use crate::*;

stateless_step!(CallEcho, "call_echo", |input, ctx| ctx.emit(input));

pub(crate) struct CallEchoRelay;
impl Relay for CallEchoRelay {
    fn factory_id() -> &'static str {
        "call_echo"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<CallEcho>().sink();
    }
}

stateless_step!(CallParse, "call_parse", |input, ctx| ctx.emit(input));
stateless_step!(CallAnswer, "call_answer", |input, ctx| ctx
    .emit(format!("re:{input}")));

pub(crate) struct CallDownstreamRelay;
impl Relay for CallDownstreamRelay {
    fn factory_id() -> &'static str {
        "call_downstream"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CallParse>()
            .into::<CallAnswer>()
            .sink();
    }
}

// An input starting `no` takes the refused path, every other the accepted one.
stateless_step!(CallAdmit, "call_admit", |input, ctx| ctx.emit(input));
stateless_step!(CallAccept, "call_accept", |input, ctx| ctx
    .emit(format!("accepted:{input}")));
stateless_step!(CallRefuse, "call_refuse", |input, ctx| ctx
    .emit(format!("refused:{input}")));

pub(crate) struct CallSplitRelay;
impl Relay for CallSplitRelay {
    fn factory_id() -> &'static str {
        "call_split"
    }
    fn channels(c: &ChannelBuilder) {
        let admitted = c.input::<String>("in").into::<CallAdmit>();
        admitted
            .filter(|v: &String| !v.starts_with("no"))
            .into::<CallAccept>()
            .sink();
        admitted
            .filter(|v: &String| v.starts_with("no"))
            .into::<CallRefuse>()
            .sink();
    }
}

stateless_step!(CallTurn, "call_turn", |input, ctx| ctx.emit(input));

pub(crate) struct CallTurnSteps;
impl Relay for CallTurnSteps {
    fn factory_id() -> &'static str {
        "call_turn"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<CallTurn>().sink();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}
