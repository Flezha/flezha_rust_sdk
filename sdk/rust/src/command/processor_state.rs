//! `ProcessorState` — return the Relay instance's per-Shard / per-Partition
//! state JSON blob per `relay-mvp-rust/design.md` Decision 10.
//!
//! Section 4 fully implements `serialize_relay_state` (walks each Shard's
//! partitions via the dispatcher's State erasure layer). Section 3 ships
//! the wiring so the wire command continues to round-trip; the body
//! produces an empty JSON object until section 4 lands.

use std::sync::Arc;

use serde_json::Value;

use super::CommandAction;
use crate::compute_engine::ComputeEngineNode;
use crate::pb::ProcessorState;
use crate::relay::instance::RelayInstance;

impl CommandAction for ProcessorState {
    fn handle(
        &self,
        compute_engine: &mut ComputeEngineNode,
        _message: &String,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        match compute_engine.get_relay(&self.processor_id) {
            Some(instance) => {
                let payload = serialize_relay_state(&instance);
                ("processor_state".to_string(), payload)
            }
            None => {
                errors.push(format!(
                    "Relay instance with id {} not found",
                    self.processor_id
                ));
                ("processor_state".to_string(), Vec::new())
            }
        }
    }
}

/// Build the canonical JSON shape from `design.md` Decision 10:
/// - Single-mode shards: `{"<shard_name>": <state>}`
/// - Static-mode shards: `{"<shard_name>": {"<shard_key>": <state>, ...}}`
///
/// Section 4 wires this up via `erased_serde` and the dispatcher's
/// State-erasure layer. Section 3 returns an empty object placeholder so
/// the wire command continues to respond.
pub fn serialize_relay_state(_instance: &Arc<RelayInstance>) -> Vec<u8> {
    let v = Value::Object(serde_json::Map::new());
    serde_json::to_vec(&v).unwrap_or_default()
}

/// Apply an `AddProcessor.init_values` JSON blob to a fresh Relay
/// instance. Section 4 dispatches per-Shard `init_state` overrides;
/// section 3 accepts any parseable JSON and ignores it (the legacy
/// init-state path was per-processor; the relay-mvp-rust replacement
/// is per-Shard and ships in section 4 / `relay-message-handlers`).
pub fn apply_init_values(
    _instance: &Arc<RelayInstance>,
    init_values: &str,
) -> Result<(), String> {
    if init_values.is_empty() {
        return Ok(());
    }
    serde_json::from_str::<Value>(init_values)
        .map(|_| ())
        .map_err(|e| format!("Failed to parse init_values as JSON: {}", e))
}
