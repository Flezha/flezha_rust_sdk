//! Inbound credit-grant flow control commands (controller → SDK).
//!
//! `CreditWindowInit` arms the process-global sender window
//! (`crate::segment::credit`) and acks by returning the encoded
//! `CreditWindowAck` as this command's `CommandResponse` payload — the
//! controller marks the session credit-speaking on that ack.
//! `CreditGrant` replenishes the window; both run on the control
//! coordinator and never block, so grants keep flowing while data-path
//! threads are parked on exhausted credit.

use prost::Message;

use super::CommandAction;
use crate::pb::{CreditGrant, CreditWindowAck, CreditWindowInit};
use crate::runner::RunnerNode;
use crate::segment::credit;

impl CommandAction for CreditWindowInit {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        credit::global().arm(self.stream_id.clone(), self.window_bytes);

        let ack = CreditWindowAck {
            stream_id: self.stream_id.clone(),
        };

        ("credit_window_init".to_string(), ack.encode_to_vec())
    }
}

impl CommandAction for CreditGrant {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        credit::global().grant(&self.stream_id, self.bytes);

        ("credit_grant".to_string(), Vec::new())
    }
}
