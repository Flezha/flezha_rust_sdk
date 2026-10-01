use std::time::Duration;

use flezha_runner::{
    ChannelBuilder, Durable, Each, OnFailure, Operator, OperatorSettings, Run, SleepAfter,
};
use serde::{Deserialize, Serialize};

use crate::deliver::Deliver;
use crate::reply::Reply;

/// A message for a user's session, as the gateway sends it: the session is named after the user.
#[derive(Clone, Serialize, Deserialize)]
pub struct Message {
    pub user: String,
    pub text: String,
}

/// One conversation: a steps operator that runs one turn per message it is
/// sent, for as long as the session lasts. Every session is an instance of its
/// own, with its own state.
pub struct AgentOperator;

impl Operator for AgentOperator {
    fn factory_id() -> &'static str {
        "agent"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("turn")
            .into::<Reply>()
            .sink_named("answer") // to whoever listens
            .into::<Deliver>()
            .response(); // to whoever called
        c.input::<Message>("message") // from the gateway
            .map(|message| message.text)
            .into::<Reply>();
    }

    fn settings() -> OperatorSettings {
        OperatorSettings {
            run: Run::Steps {
                each: Each::PerInput,                // one turn per message
                on_failure: OnFailure::KeepProgress, // a failed turn fails alone
            },
            durable: Durable::Local {
                sleep_after: SleepAfter::After(Duration::from_secs(10)), // an idle session sleeps after 10 s
            },
            ..OperatorSettings::default()
        }
    }
}
