use serde::{Deserialize, Serialize};

/// The gateway's answer to a request: a status and a body, or the session to stream.
#[derive(Serialize, Deserialize)]
#[serde(untagged)] // the plugin reads the fields, with no name for the variant around them
pub enum Http {
    Plain { status: u16, body: String },
    Stream { stream: String },
}
