//! `ProcessorMessage` — under the relay-mvp-rust cutover, the SDK
//! responds with the canonical "unsupported" error for every request
//! per `relay-mvp-rust/design.md` Decision 9.
//!
//! `relay-message-handlers` (umbrella roadmap row 4) ships per-Shard
//! `handle_message` over the new model and re-enables the disabled
//! integration tests that this fallback parks.

use super::CommandAction;
use crate::compute_engine::ComputeEngineNode;
use crate::pb::{ProcessorMessage, ProcessorMessageResponse};
use prost::Message;

const UNSUPPORTED_ERROR: &str = "processor does not support handle_message";

impl CommandAction for ProcessorMessage {
    fn handle(
        &self,
        _compute_engine: &mut ComputeEngineNode,
        _message: &String,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let response = ProcessorMessageResponse {
            payload: Vec::new(),
            error: UNSUPPORTED_ERROR.to_string(),
        };
        ("processor_message".to_string(), response.encode_to_vec())
    }
}
