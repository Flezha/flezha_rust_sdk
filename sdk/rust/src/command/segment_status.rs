//! Inbound `SegmentMailboxStatus`: legacy controller → SDK back-pressure
//! signal, kept only for compatibility with pre-credit controllers.
//!
//! A credit-speaking controller never sends this (back-pressure rides the
//! credit-grant protocol — see `crate::segment::credit`); a pre-credit
//! controller's `"full"`/`"open"` flags are tolerated and dropped, since
//! this SDK's send path is already bounded by its local mailbox policies.
//!
//! No `CommandResponse` payload — this is a one-way notification.

use super::CommandAction;
use crate::pb::SegmentMailboxStatus;
use crate::runner::RunnerNode;

impl CommandAction for SegmentMailboxStatus {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        tracing::debug!(
            target: "flezha::backpressure",
            "legacy segment mailbox status from a pre-credit controller: {}",
            self.status,
        );

        ("segment_mailbox_status".to_string(), Vec::new())
    }
}
