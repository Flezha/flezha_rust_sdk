//! `SegmentState` — return the UnboundedOperative instance's per-Operation / per-Segment
//! state JSON blob per `operative-mvp-rust/design.md` Decision 10.
//!
//! `serialize_operative_state` walks each Operation's segments via the per-Operation
//! `serialize_state` fn pointer cached on `OperationDescriptor` (built at
//! registration time, captures S statically). `apply_init_values` walks
//! the controller-supplied JSON object and seeds each Segment's state
//! through the symmetric `deserialize_state` fn pointer.

use std::sync::Arc;

use serde_json::{Map, Value};

use super::CommandAction;
use crate::operative::instance::UnboundedInstance;
use crate::pb::SegmentState;
use crate::runner::RunnerNode;
use crate::util::LockRecover;

impl CommandAction for SegmentState {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        match runner.get_unbounded(&self.operative_id) {
            Some(instance) => {
                let payload = serialize_operative_state(&instance);
                ("segment_state".to_string(), payload)
            }
            None => {
                errors.push(format!(
                    "UnboundedOperative instance with id {} not found",
                    self.operative_id
                ));
                ("segment_state".to_string(), Vec::new())
            }
        }
    }
}

/// Build the canonical JSON shape from `design.md` Decision 10:
/// - Single-mode operations: `{"<operation_name>": <state>}`
/// - Static-mode operations: `{"<operation_name>": {"<operation_key>": <state>, ...}}`
///
/// State serialization goes through the per-Operation `serialize_state` fn
/// pointer cached on `OperationDescriptor`. Errors from individual segments
/// are logged on stderr and the offending Operation's entry is omitted —
/// the controller treats a missing operation key the same as no state.
pub fn serialize_operative_state(instance: &Arc<UnboundedInstance>) -> Vec<u8> {
    let mut top: Map<String, Value> = Map::new();

    for (operation_id, operation_segments) in instance.segments.iter().enumerate() {
        let operation_desc = match instance.topology.operations.get(operation_id) {
            Some(d) => d,
            None => continue,
        };
        let operation_name = operation_desc.name.to_string();

        if operation_desc.segment_keys.is_empty() {
            // Single-mode: one Segment under the Operation's name.
            let slot = match operation_segments.first() {
                Some(s) => s,
                None => continue,
            };
            let state_guard = slot.state.lock_recover();
            match (operation_desc.serialize_state)(&state_guard) {
                Ok(v) => {
                    top.insert(operation_name, v);
                }
                Err(e) => {
                    eprintln!(
                        "[flezha] serialize_operative_state: operation `{}` single-mode failed: {}",
                        operation_desc.name, e
                    );
                }
            }
        } else {
            // Static-mode: nested `{operation_key: state, ...}`.
            let mut inner: Map<String, Value> = Map::new();
            for (idx, key) in operation_desc.segment_keys.iter().enumerate() {
                let slot = match operation_segments.get(idx) {
                    Some(s) => s,
                    None => continue,
                };
                let state_guard = match slot.state.lock() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                match (operation_desc.serialize_state)(&state_guard) {
                    Ok(v) => {
                        inner.insert((*key).to_string(), v);
                    }
                    Err(e) => {
                        eprintln!(
                            "[flezha] serialize_operative_state: operation `{}` key `{}` failed: {}",
                            operation_desc.name, key, e
                        );
                    }
                }
            }
            top.insert(operation_name, Value::Object(inner));
        }
    }

    serde_json::to_vec(&Value::Object(top)).unwrap_or_default()
}

/// Apply an `AddOperative.init_values` JSON blob to a fresh UnboundedOperative instance.
/// Walks the top-level keys, matches them against registered Operation names,
/// and dispatches each value to the matching Operation's `deserialize_state`
/// fn pointer to overwrite the freshly-`init_state`d Segment state.
///
/// Empty `init_values` is a no-op (the cold-start path uses `init_state`).
/// Unknown top-level keys and unknown inner operation_key entries are logged
/// and skipped per the spec.
pub fn apply_init_values(
    instance: &Arc<UnboundedInstance>,
    init_values: &str,
) -> Result<(), String> {
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

    for (operation_name, operation_value) in top {
        let operation_id = match instance
            .topology
            .operation_name_to_id
            .get(operation_name.as_str())
        {
            Some(&id) => id,
            None => {
                eprintln!(
                    "[flezha] apply_init_values: ignoring unknown operation `{}`",
                    operation_name
                );
                continue;
            }
        };
        let operation_desc = match instance.topology.operation(operation_id) {
            Some(d) => d,
            None => continue,
        };
        let operation_segments = match instance.segments.get(operation_id as usize) {
            Some(p) => p,
            None => continue,
        };

        if operation_desc.segment_keys.is_empty() {
            // Single-mode: the value is the State JSON itself.
            let slot = match operation_segments.first() {
                Some(s) => s,
                None => continue,
            };
            apply_one(
                operation_desc,
                slot,
                &operation_value,
                &operation_name,
                None,
            );
        } else {
            // Static-mode: the value is `{operation_key: state, ...}`.
            let inner = match operation_value {
                Value::Object(m) => m,
                other => {
                    eprintln!(
                        "[flezha] apply_init_values: operation `{}` expected object of \
                         operation_keys, got {} — skipping",
                        operation_name,
                        value_kind(&other)
                    );
                    continue;
                }
            };
            let key_to_idx = match instance.segment_key_to_idx.get(operation_id as usize) {
                Some(m) => m,
                None => continue,
            };
            for (key, value) in inner {
                let idx = match key_to_idx.get(key.as_str()) {
                    Some(&i) => i,
                    None => {
                        eprintln!(
                            "[flezha] apply_init_values: ignoring unknown operation_key \
                             `{}` for operation `{}`",
                            key, operation_name
                        );
                        continue;
                    }
                };
                let slot = match operation_segments.get(idx) {
                    Some(s) => s,
                    None => continue,
                };
                apply_one(operation_desc, slot, &value, &operation_name, Some(&key));
            }
        }
    }

    Ok(())
}

fn apply_one(
    operation_desc: &crate::operative::topology::OperationDescriptor,
    slot: &Arc<crate::segment::SegmentSlot>,
    value: &Value,
    operation_name: &str,
    operation_key: Option<&str>,
) {
    match (operation_desc.deserialize_state)(value) {
        Ok(new_state) => {
            *slot.state.lock_recover() = new_state;
        }
        Err(e) => match operation_key {
            Some(k) => eprintln!(
                "[flezha] apply_init_values: operation `{}` key `{}` deserialize failed: {}",
                operation_name, k, e
            ),
            None => eprintln!(
                "[flezha] apply_init_values: operation `{}` deserialize failed: {}",
                operation_name, e
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
