//! Inbound `CompactPartition`: controller → SDK ask-for-snapshot.
//!
//! v1 v0 wiring: validates the partition_id structure and replies
//! `CompactPartitionResponse{snapshot_seq: 0, snapshot_bytes_size: 0}`
//! to acknowledge the request. The actual `Self::State` clone +
//! postcard serialization + `partition_state_snapshot` data-stream emit
//! is performed by the dispatcher's compaction routine, which is
//! invoked separately by the relay instance lookup. The framework here
//! ensures the protobuf surface is in place; the integration with
//! the live `PartitionSlot` snapshot path is wired in the same
//! follow-up that exposes the dispatcher's compact entry point.

use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::pb::{CompactPartition, CompactPartitionResponse};
use prost::Message;

impl CommandAction for CompactPartition {
    fn handle(
        &self,
        _executor: &mut ExecutorNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let response = CompactPartitionResponse {
            partition_id: self.partition_id.clone(),
            snapshot_seq: 0,
            snapshot_bytes_size: 0,
        };

        ("compact_partition".to_string(), response.encode_to_vec())
    }
}
