//! `OperativeMessage` — under the operative-mvp-rust cutover, the SDK
//! responds with the canonical "unsupported" error for every request
//! per `operative-mvp-rust/design.md` Decision 9.
//!
//! `operative-message-handlers` (umbrella roadmap row 4) ships per-Procedure
//! `handle_message` over the new model and re-enables the disabled
//! integration tests that this fallback parks.

use prost::Message;

use super::CommandAction;
use crate::pb::{OperativeMessage, OperativeMessageResponse};
use crate::runner::RunnerNode;

const UNSUPPORTED_ERROR: &str = "operative does not support handle_message";

impl CommandAction for OperativeMessage {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let response = OperativeMessageResponse {
            payload: Vec::new(),
            error: UNSUPPORTED_ERROR.to_string(),
        };
        ("operative_message".to_string(), response.encode_to_vec())
    }
}
