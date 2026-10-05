use flezha_runner::{CallTarget, ChannelBuilder, Exactly, Relay, RelaySettings, Run};

use crate::admit::Admit;
use crate::budget::{Budget, Event, Verdict};
use crate::reply::Answer;
use crate::request::Request;
use crate::watch::Watch;

const DEADLINE_MS: u64 = 60_000;

/// The HTTP side of the desk: a flow relay that carries the HTTP
/// plugin, answers every request, and calls the session a message is for.
pub struct GatewayRelay;

impl Relay for GatewayRelay {
    fn factory_id() -> &'static str {
        "gateway"
    }

    fn channels(c: &ChannelBuilder) {
        // every request arrives on "http"; kept as a handle to split it
        let http = c.input::<Request>("http");
        // route 1: GET .../events gets the session's stream
        http.filter(|request| !request.sends())
            .into::<Watch>()
            .keyed()
            .output(); // a stream, or 404

        // route 2: POST .../messages is checked against the user's budget
        let verdicts = http
            .filter(Request::sends)
            .map(|request| Event::Sent(request.message()))
            .key_by(|event| event.user().to_string()) // one cell per user
            .into::<Budget>();

        // a verdict: the message plus its user's tokens today; used twice
        // use 1: answer the POST
        verdicts.into::<Admit>().keyed().output(); // 202, or 429 over the budget

        // use 2: pass an admitted message on to the user's session
        verdicts
            .filter(Verdict::admitted)
            .map(|verdict| verdict.message)
            .key_by(|message| message.user.clone()) // the user's session
            // call the "agent" relay; its answer comes back here
            .call::<Answer>(CallTarget::factory("agent").channel("message"), DEADLINE_MS)
            .map(Event::used) // the session's answer, as tokens used
            .key_by(|event| event.user().to_string())
            .into::<Budget>(); // back into the same budget
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            // each request and answer on disk before it runs
            run: Run::Flow {
                exactly: Exactly::effectively_once(),
            },
            ..RelaySettings::default()
        }
    }
}
