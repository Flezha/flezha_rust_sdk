use prost::Message;

use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::pb::{Ping, PingResponse};

/// Cheap end-to-end control-channel health probe.
///
/// The Controller issues `Ping` immediately before each `AddRelay` during
/// the Overwatch's cold-restart startup sequence (see
/// `executor-controller-connection-healing`). A successful round-trip
/// proves the channel is bound and the SDK's reply path is intact, so a
/// subsequent `AddRelay` is safe to issue. The handler echoes the request
/// `nonce` so the caller can correlate replies if multiple pings are in
/// flight on the same channel.
impl CommandAction for Ping {
    fn handle(
        &self,
        _executor: &mut ExecutorNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let response = PingResponse {
            nonce: self.nonce.clone(),
        };

        ("ping".to_string(), response.encode_to_vec())
    }
}
