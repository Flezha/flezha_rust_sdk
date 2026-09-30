use flezha_runner::{BoundedOperator, ChannelBuilder, LoopMode, RollbackMode};
use serde::{Deserialize, Serialize};

use crate::deliver::Deliver;
use crate::reply::Reply;

/// A message for a user's session, as the gateway sends it: the session is named after the user.
#[derive(Clone, Serialize, Deserialize)]
pub struct Message {
    pub user: String,
    pub text: String,
}

/// One conversation: a bounded operator that runs one turn per message it is
/// sent, for as long as the session lasts. Every session is an instance of its
/// own, with its own state.
pub struct AgentOperator;

impl BoundedOperator for AgentOperator {
    fn factory_id() -> &'static str {
        "agent"
    }

    fn loop_mode() -> LoopMode {
        LoopMode::PerInput // one turn per message
    }

    fn rollback_mode() -> RollbackMode {
        RollbackMode::KeepProgress // a failed turn fails alone
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
}
