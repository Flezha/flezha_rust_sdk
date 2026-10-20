//! Relay call fixtures (`relay_call`): relays that call another
//! relay from their graph and read the outcome as a `Response`.
//!
//! - `call_pricing`: a flow callee answering `price:<input>` on its output
//!   `quotes`; an input starting `silent` is never answered.
//! - `call_orders`: a flow caller calling the instance `pricing` by name,
//!   answered from `quotes`.
//! - `call_agent`: a durable `:per_input` `keep_progress` session answering
//!   `answer:<turn>`; `boom` fails the turn, `slow` answers after 2 s.
//! - `call_gateway`: an effectively-once flow caller calling `call_agent`
//!   by the session key of each `<session>:<text>` turn, the Response channel
//!   wired back into its calling tasklet; a `resume:<session>:<text>` turn
//!   reaches its session only if it runs (`create: false`).
//! - `call_tool_session`: a durable `:per_input` session calling `pricing`
//!   from a turn and reading the outcome as its next turn.
//! - `call_orders_steps`: `call_orders` as a durable `:per_input` session, so
//!   `order_ask`'s channel ends only in the call.
//!
//! Every outcome is written to an output as one line,
//! `<request>=ok:<answer>@<target>/<created>` or
//! `<request>=error:<code>@<target>/<created>`.

use flezha_runner::{CallTarget, Response};

use crate::*;

/// The deadline every fixture call carries.
const DEADLINE_MS: u64 = 30_000;

/// One outcome as the line every SDK's fixture writes.
fn render(response: &Response<String, String>) -> String {
    let outcome = match &response.result {
        Ok(answer) => format!("ok:{answer}"),
        Err(error) => format!("error:{}", error.code),
    };
    format!(
        "{}={outcome}@{}/{}",
        response.request, response.target, response.created
    )
}

/// The prefix a caller's tasklet puts on an outcome line, which no request
/// carries, so one output channel can feed both the call and the output.
const OUTCOME: &str = "response ";

/// The prefix of a turn that must not start its session.
const RESUME: &str = "resume:";

fn is_request(line: &str) -> bool {
    !line.starts_with(OUTCOME)
}

fn is_turn(line: &str) -> bool {
    is_request(line) && !line.starts_with(RESUME)
}

fn is_resume(line: &str) -> bool {
    line.starts_with(RESUME)
}

fn is_outcome(line: &str) -> bool {
    line.starts_with(OUTCOME)
}

// -- call_pricing ---------------------------------------------------------------

#[derive(Default)]
struct PricingQuote;
#[flezha_runner::tasklet(name = "pricing_quote")]
impl Tasklet for PricingQuote {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if input.starts_with("silent") {
            return Ok(());
        }
        ctx.emit(format!("price:{input}"))
    }
}

pub(crate) struct CallPricingRelay;
impl Relay for CallPricingRelay {
    fn factory_id() -> &'static str {
        "call_pricing"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<PricingQuote>()
            .output_named("quotes");
    }
}

// -- call_orders ----------------------------------------------------------------

#[derive(Default)]
struct OrderAsk;
#[flezha_runner::tasklet(name = "order_ask")]
impl Tasklet for OrderAsk {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input)
    }
}

#[derive(Default)]
struct OrderPriced;
#[flezha_runner::tasklet(name = "order_priced")]
impl Tasklet for OrderPriced {
    type Input = Response<String, String>;
    type Output = String;
    fn run(
        &mut self,
        input: Response<String, String>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        ctx.emit(render(&input))
    }
}

pub(crate) struct CallOrdersRelay;
impl Relay for CallOrdersRelay {
    fn factory_id() -> &'static str {
        "call_orders"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<OrderAsk>()
            .named("order")
            .call::<String>(
                CallTarget::id("pricing").answer_channel("quotes"),
                DEADLINE_MS,
            )
            .into::<OrderPriced>()
            .output_named("priced");
    }
}

// -- call_agent -----------------------------------------------------------------

#[derive(Default)]
struct AgentTurn;
#[flezha_runner::tasklet(name = "agent_turn")]
impl Tasklet for AgentTurn {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let text = input
            .split_once(':')
            .map_or(input.as_str(), |(_, text)| text);
        match text {
            "boom" => return Err(TaskletError::non_retryable("the turn failed")),
            "slow" => thread::sleep(Duration::from_secs(2)),
            _ => {}
        }
        ctx.emit(format!("answer:{input}"))
    }
}

pub(crate) struct CallAgentSteps;
impl Relay for CallAgentSteps {
    fn factory_id() -> &'static str {
        "call_agent"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("turn").into::<AgentTurn>().output();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::KeepProgress,
            },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// -- call_gateway ---------------------------------------------------------------

/// What reaches a calling tasklet (the gateway's account, a tool session's
/// turn): a turn from outside, or the outcome of a call it made.
#[derive(Clone)]
enum CallerIn {
    Turn(String),
    Outcome(Response<String, String>),
}

#[derive(Default)]
struct CallerTurn;
#[flezha_runner::tasklet(name = "gateway_account")]
impl Tasklet for CallerTurn {
    type Input = CallerIn;
    type Output = String;
    fn run(&mut self, input: CallerIn, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        match input {
            CallerIn::Turn(turn) => ctx.emit(turn),
            CallerIn::Outcome(response) => ctx.emit(format!("{OUTCOME}{}", render(&response))),
        }
    }
}

#[derive(Default)]
struct GatewayResume;
#[flezha_runner::tasklet(name = "gateway_resume")]
impl Tasklet for GatewayResume {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input[RESUME.len()..].to_string())
    }
}

#[derive(Default)]
struct OutcomeLog;
#[flezha_runner::tasklet(name = "gateway_log")]
impl Tasklet for OutcomeLog {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input[OUTCOME.len()..].to_string())
    }
}

/// The session a turn or an outcome line belongs to: what precedes its `:`.
fn session_of(line: &str) -> String {
    let line = line.strip_prefix(OUTCOME).unwrap_or(line);
    line.split(':').next().unwrap_or_default().to_string()
}

pub(crate) struct CallGatewayRelay;
impl Relay for CallGatewayRelay {
    fn factory_id() -> &'static str {
        "call_gateway"
    }
    fn channels(c: &ChannelBuilder) {
        let account = c
            .input::<String>("in")
            .map(CallerIn::Turn)
            .into::<CallerTurn>()
            .named("gateway");
        account
            .filter(|line: &String| is_turn(line))
            .key_by(|line: &String| session_of(line))
            .call::<String>(CallTarget::factory("call_agent"), DEADLINE_MS)
            .map(CallerIn::Outcome)
            .into::<CallerTurn>();
        account
            .filter(|line: &String| is_resume(line))
            .into::<GatewayResume>()
            .named("resume")
            .key_by(|line: &String| session_of(line))
            .call::<String>(CallTarget::factory("call_agent").create(false), DEADLINE_MS)
            .map(CallerIn::Outcome)
            .into::<CallerTurn>();
        account
            .filter(|line: &String| is_outcome(line))
            .key_by(|line: &String| session_of(line))
            .into::<OutcomeLog>()
            .keyed()
            .output_named("gateway_out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Flow {
                exactly: Exactly::effectively_once(),
            },
            ..RelaySettings::default()
        }
    }
}

// -- call_tool_session ----------------------------------------------------------

pub(crate) struct CallToolSessionSteps;
impl Relay for CallToolSessionSteps {
    fn factory_id() -> &'static str {
        "call_tool_session"
    }
    fn channels(c: &ChannelBuilder) {
        let turn = c
            .input::<String>("turn")
            .map(CallerIn::Turn)
            .into_named::<CallerTurn>("tool_turn")
            .named("tool");
        turn.filter(|line: &String| is_request(line))
            .call::<String>(
                CallTarget::id("pricing").answer_channel("quotes"),
                DEADLINE_MS,
            )
            .map(CallerIn::Outcome)
            .into_named::<CallerTurn>("tool_turn");
        turn.filter(|line: &String| is_outcome(line))
            .into_named::<OutcomeLog>("tool_log")
            .output_named("tool_out");
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

// -- call_orders_steps ----------------------------------------------------------

pub(crate) struct CallOrdersSteps;
impl Relay for CallOrdersSteps {
    fn factory_id() -> &'static str {
        "call_orders_steps"
    }
    fn channels(c: &ChannelBuilder) {
        CallOrdersRelay::channels(c);
    }
    fn settings() -> RelaySettings {
        CallToolSessionSteps::settings()
    }
}

// -- the reference rows' call steps ---------------------------------------------

/// `<x>.call(...)` and its kin from `test/fixtures/reference_topologies.txt`:
/// a 500 ms call whose Response channel feeds the role that produces `x`,
/// unless the step leaves it unread. `false` when `link` is no call step.
pub(crate) fn call_step(s: Stream<'_, String>, role: &str, link: &str) -> bool {
    let (verb, arg) = link
        .strip_suffix(')')
        .and_then(|l| l.split_once('('))
        .unwrap_or((link, ""));
    let (target, deadline) = match verb {
        "call" | "call_unread" => (CallTarget::id(arg), 500),
        "call_factory" => (CallTarget::factory(arg), 500),
        "call_factory_nocreate" => (CallTarget::factory(arg).create(false), 500),
        "call_deadline" => (CallTarget::id("pricing"), arg.parse().expect("a deadline")),
        _ => return false,
    };
    let response = s.call::<String>(target, deadline);
    if verb != "call_unread" {
        crate::conformance::wire(response.map(|r: Response<String, String>| render(&r)), role);
    }
    true
}
