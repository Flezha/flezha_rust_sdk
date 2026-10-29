use std::time::Duration;

use flezha_runner::{
    ChannelBuilder, Durable, Each, KeepFinished, OnFailure, Relay, RelaySettings, Run, SleepAfter,
};
use serde::{Deserialize, Serialize};

use crate::reply::Reply;

/// A message for a user's session, as the gateway sends it: the session is named after the user.
#[derive(Clone, Serialize, Deserialize)]
pub struct Message {
    pub user: String,
    pub text: String,
}

/// One conversation: a steps relay that runs one turn per message it is
/// sent, for as long as the session lasts. Every session is an instance of its
/// own, with its own state.
pub struct AgentRelay;

impl Relay for AgentRelay {
    fn factory_id() -> &'static str {
        "agent"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("turn") // entry 1: `flz relay call`
            .into::<Reply>()
            .output_named("answer"); // to whoever called, and whoever listens
        c.input::<Message>("message") // entry 2: the gateway, into the same Reply
            .map(|message| message.text)
            .into::<Reply>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,                // one turn per message
                on_failure: OnFailure::KeepProgress, // a failed turn fails alone
            },
            durable: Durable::Local {
                sleep_after: SleepAfter::After(Duration::from_secs(10)), // an idle session sleeps after 10 s
                keep_finished: KeepFinished::Nothing,
            },
            ..RelaySettings::default()
        }
    }
}
