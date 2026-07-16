//! Inbound `ParkPayloadAck` (controller → SDK): the dead-letter entry
//! identified by `park_id` is durable in the controller's DLQ store. Wakes
//! the dispatch thread blocked in `segment::park::park_blocking` so the
//! parked segment drops its poison payload and advances. Runs on the
//! control coordinator and never blocks.

use super::CommandAction;
use crate::pb::ParkPayloadAck;
use crate::runner::RunnerNode;
use crate::segment::park;

impl CommandAction for ParkPayloadAck {
    fn handle(
        &self,
        _runner: &mut RunnerNode,
        _message: &str,
        _errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        park::complete_ack(&self.park_id);
        ("park_payload_ack".to_string(), Vec::new())
    }
}
