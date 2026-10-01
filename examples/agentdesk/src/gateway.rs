use flezha_runner::{CallTarget, ChannelBuilder, Exactly, Operator, OperatorSettings, Run};

use crate::admit::Admit;
use crate::budget::{Budget, Event, Verdict};
use crate::reply::Answer;
use crate::request::Request;
use crate::watch::Watch;

const DEADLINE_MS: u64 = 60_000;

/// The HTTP side of the desk: a flow operator that carries the HTTP
/// plugin, answers every request, and calls the session a message is for.
pub struct GatewayOperator;

impl Operator for GatewayOperator {
    fn factory_id() -> &'static str {
        "gateway"
    }

    fn channels(c: &ChannelBuilder) {
        let http = c.input::<Request>("http");
        http.filter(|request| !request.sends())
            .into::<Watch>()
            .response(); // a stream, or 404
        let verdicts = http
            .filter(Request::sends)
            .map(|request| Event::Sent(request.message()))
            .key_by(|event| event.user().to_string()) // one cell per user
            .into::<Budget>();
        verdicts.into::<Admit>().response(); // 202, or 429 over the budget
        verdicts
            .filter(Verdict::admitted)
            .map(|verdict| verdict.message)
            .key_by(|message| message.user.clone()) // the user's session
            .call::<Answer>(CallTarget::factory("agent").channel("message"), DEADLINE_MS)
            .map(Event::used)
            .key_by(|event| event.user().to_string())
            .into::<Budget>(); // the tokens the answer used
    }

    fn settings() -> OperatorSettings {
        OperatorSettings {
            run: Run::Flow {
                exactly: Exactly::effectively_once(),
            },
            ..OperatorSettings::default()
        }
    }
}
