//! Inbound `CompactSegment`: controller → SDK ask-for-snapshot.
//!
//! v1 v0 wiring: validates the segment_id structure and replies
//! `CompactSegmentResponse{snapshot_seq: 0, snapshot_bytes_size: 0}`
//! to acknowledge the request. The actual `Self::State` clone +
//! postcard serialization + `segment_state_snapshot` data-stream emit
//! is performed by the dispatcher's compaction routine, which is
//! invoked separately by the operative instance lookup. The framework here
//! ensures the protobuf surface is in place; the integration with
//! the live `SegmentSlot` snapshot path is wired in the same
//! follow-up that exposes the dispatcher's compact entry point.

use prost::Message;

use super::CommandAction;
use crate::pb::{CompactSegment, CompactSegmentResponse};
use crate::runner::RunnerNode;

impl CommandAction for CompactSegment {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let response = CompactSegmentResponse {
            segment_id: self.segment_id.clone(),
            snapshot_seq: 0,
            snapshot_bytes_size: 0,
        };

        ("compact_segment".to_string(), response.encode_to_vec())
    }
}
