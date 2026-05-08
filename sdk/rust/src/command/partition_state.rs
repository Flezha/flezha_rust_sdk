//! `PartitionState` — return the Relay instance's per-Shard / per-Partition
//! state JSON blob per `relay-mvp-rust/design.md` Decision 10.
//!
//! `serialize_relay_state` walks each Shard's partitions via the per-Shard
//! `serialize_state` fn pointer cached on `ShardDescriptor` (built at
//! registration time, captures S statically). `apply_init_values` walks
//! the controller-supplied JSON object and seeds each Partition's state
//! through the symmetric `deserialize_state` fn pointer.

use std::sync::Arc;

use serde_json::{Map, Value};

use super::CommandAction;
use crate::executor::ExecutorNode;
use crate::pb::PartitionState;
use crate::relay::instance::RelayInstance;

impl CommandAction for PartitionState {
    fn handle(
        &self,
        executor: &mut ExecutorNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        match executor.get_relay(&self.relay_id) {
            Some(instance) => {
                let payload = serialize_relay_state(&instance);
                ("partition_state".to_string(), payload)
            }
            None => {
                errors.push(format!(
                    "Relay instance with id {} not found",
                    self.relay_id
                ));
                ("partition_state".to_string(), Vec::new())
            }
        }
    }
}

/// Build the canonical JSON shape from `design.md` Decision 10:
/// - Single-mode shards: `{"<shard_name>": <state>}`
/// - Static-mode shards: `{"<shard_name>": {"<shard_key>": <state>, ...}}`
///
/// State serialization goes through the per-Shard `serialize_state` fn
/// pointer cached on `ShardDescriptor`. Errors from individual partitions
/// are logged on stderr and the offending Shard's entry is omitted —
/// the controller treats a missing shard key the same as no state.
pub fn serialize_relay_state(instance: &Arc<RelayInstance>) -> Vec<u8> {
    let mut top: Map<String, Value> = Map::new();

    for (shard_id, shard_partitions) in instance.partitions.iter().enumerate() {
        let shard_desc = match instance.topology.shards.get(shard_id) {
            Some(d) => d,
            None => continue,
        };
        let shard_name = shard_desc.name.to_string();

        if shard_desc.partition_keys.is_empty() {
            // Single-mode: one Partition under the Shard's name.
            let slot = match shard_partitions.first() {
                Some(s) => s,
                None => continue,
            };
            let state_guard = match slot.state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            match (shard_desc.serialize_state)(&state_guard) {
                Ok(v) => {
                    top.insert(shard_name, v);
                }
                Err(e) => {
                    eprintln!(
                        "[flezha] serialize_relay_state: shard `{}` single-mode failed: {}",
                        shard_desc.name, e
                    );
                }
            }
        } else {
            // Static-mode: nested `{shard_key: state, ...}`.
            let mut inner: Map<String, Value> = Map::new();
            for (idx, key) in shard_desc.partition_keys.iter().enumerate() {
                let slot = match shard_partitions.get(idx) {
                    Some(s) => s,
                    None => continue,
                };
                let state_guard = match slot.state.lock() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                match (shard_desc.serialize_state)(&state_guard) {
                    Ok(v) => {
                        inner.insert((*key).to_string(), v);
                    }
                    Err(e) => {
                        eprintln!(
                            "[flezha] serialize_relay_state: shard `{}` key `{}` failed: {}",
                            shard_desc.name, key, e
                        );
                    }
                }
            }
            top.insert(shard_name, Value::Object(inner));
        }
    }

    serde_json::to_vec(&Value::Object(top)).unwrap_or_default()
}

/// Apply an `AddRelay.init_values` JSON blob to a fresh Relay instance.
/// Walks the top-level keys, matches them against registered Shard names,
/// and dispatches each value to the matching Shard's `deserialize_state`
/// fn pointer to overwrite the freshly-`init_state`d Partition state.
///
/// Empty `init_values` is a no-op (the cold-start path uses `init_state`).
/// Unknown top-level keys and unknown inner shard_key entries are logged
/// and skipped per the spec.
pub fn apply_init_values(instance: &Arc<RelayInstance>, init_values: &str) -> Result<(), String> {
    if init_values.is_empty() {
        return Ok(());
    }
    let parsed: Value = serde_json::from_str(init_values)
        .map_err(|e| format!("Failed to parse init_values as JSON: {}", e))?;
    let top = match parsed {
        Value::Object(map) => map,
        other => {
            return Err(format!(
                "init_values must be a JSON object, got {}",
                value_kind(&other)
            ));
        }
    };

    for (shard_name, shard_value) in top {
        let shard_id = match instance.topology.shard_name_to_id.get(shard_name.as_str()) {
            Some(&id) => id,
            None => {
                eprintln!(
                    "[flezha] apply_init_values: ignoring unknown shard `{}`",
                    shard_name
                );
                continue;
            }
        };
        let shard_desc = match instance.topology.shard(shard_id) {
            Some(d) => d,
            None => continue,
        };
        let shard_partitions = match instance.partitions.get(shard_id as usize) {
            Some(p) => p,
            None => continue,
        };

        if shard_desc.partition_keys.is_empty() {
            // Single-mode: the value is the State JSON itself.
            let slot = match shard_partitions.first() {
                Some(s) => s,
                None => continue,
            };
            apply_one(shard_desc, slot, &shard_value, &shard_name, None);
        } else {
            // Static-mode: the value is `{shard_key: state, ...}`.
            let inner = match shard_value {
                Value::Object(m) => m,
                other => {
                    eprintln!(
                        "[flezha] apply_init_values: shard `{}` expected object of \
                         shard_keys, got {} — skipping",
                        shard_name,
                        value_kind(&other)
                    );
                    continue;
                }
            };
            let key_to_idx = match instance.partition_key_to_idx.get(shard_id as usize) {
                Some(m) => m,
                None => continue,
            };
            for (key, value) in inner {
                let idx = match key_to_idx.get(key.as_str()) {
                    Some(&i) => i,
                    None => {
                        eprintln!(
                            "[flezha] apply_init_values: ignoring unknown shard_key \
                             `{}` for shard `{}`",
                            key, shard_name
                        );
                        continue;
                    }
                };
                let slot = match shard_partitions.get(idx) {
                    Some(s) => s,
                    None => continue,
                };
                apply_one(shard_desc, slot, &value, &shard_name, Some(&key));
            }
        }
    }

    Ok(())
}

fn apply_one(
    shard_desc: &crate::relay::topology::ShardDescriptor,
    slot: &Arc<crate::partition::PartitionSlot>,
    value: &Value,
    shard_name: &str,
    shard_key: Option<&str>,
) {
    match (shard_desc.deserialize_state)(value) {
        Ok(new_state) => {
            let mut guard = match slot.state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            *guard = new_state;
        }
        Err(e) => match shard_key {
            Some(k) => eprintln!(
                "[flezha] apply_init_values: shard `{}` key `{}` deserialize failed: {}",
                shard_name, k, e
            ),
            None => eprintln!(
                "[flezha] apply_init_values: shard `{}` deserialize failed: {}",
                shard_name, e
            ),
        },
    }
}

fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}
