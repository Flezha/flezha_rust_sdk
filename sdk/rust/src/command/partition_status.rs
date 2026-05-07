//! Inbound `PartitionMailboxStatus`: agent → SDK back-pressure signal.
//!
//! `status` is `"full"` (high watermark crossed) or `"open"` (drain
//! below low watermark). The handler updates the global mailbox-status
//! observer; the producing Shard's `OverflowPolicy` machinery reads
//! the observer to decide whether to park or unpark `ctx.emit::<C>(value)`
//! callers.
//!
//! No `CommandResponse` payload — this is a one-way notification. The
//! agent issues it via the async `send_control_async` path, so it
//! never blocks waiting for a reply.

use super::CommandAction;
use crate::compute_engine::ComputeEngineNode;
use crate::pb::PartitionMailboxStatus;

impl CommandAction for PartitionMailboxStatus {
    fn handle(
        &self,
        _compute_engine: &mut ComputeEngineNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        match self.status.as_str() {
            "full" | "open" => {
                tracing::debug!(
                    target: "flezha::backpressure",
                    "partition mailbox status: {}",
                    self.status,
                );
            }
            other => {
                errors.push(format!(
                    "PartitionMailboxStatus: unknown status \"{}\" (expected \"full\" or \"open\")",
                    other
                ));
            }
        }

        ("partition_mailbox_status".to_string(), Vec::new())
    }
}
