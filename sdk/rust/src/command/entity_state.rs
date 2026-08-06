//! `EntityState` — return the UnboundedOperative instance's per-Procedure / per-Entity
//! state JSON blob per `operative-mvp-rust/design.md` Decision 10.
//!
//! `serialize_operative_state` walks each Procedure's entities via the per-Procedure
//! `serialize_state` fn pointer cached on `ProcedureDescriptor` (built at
//! registration time, captures S statically). `apply_init_values` walks
//! the controller-supplied JSON object and seeds each Entity's state
//! through the symmetric `deserialize_state` fn pointer.

use std::sync::Arc;

use serde_json::{Map, Value};

use super::CommandAction;
use crate::operative::instance::UnboundedInstance;
use crate::pb::EntityState;
use crate::runner::RunnerNode;
use crate::util::LockRecover;

impl CommandAction for EntityState {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        match runner.get_unbounded(&self.operative_id) {
            Some(instance) => {
                let payload = serialize_operative_state(&instance);
                ("entity_state".to_string(), payload)
            }
            None => {
                errors.push(format!(
                    "UnboundedOperative instance with id {} not found",
                    self.operative_id
                ));
                ("entity_state".to_string(), Vec::new())
            }
        }
    }
}

/// Build the canonical JSON shape from `design.md` Decision 10:
/// - Single-mode procedures: `{"<procedure_name>": <state>}`
/// - Static-mode procedures: `{"<procedure_name>": {"<entity_key>": <state>, ...}}`
///
/// State serialization goes through the per-Procedure `serialize_state` fn
/// pointer cached on `ProcedureDescriptor`. Errors from individual entities
/// are logged on stderr and the offending Procedure's entry is omitted —
/// the controller treats a missing procedure key the same as no state.
pub fn serialize_operative_state(instance: &Arc<UnboundedInstance>) -> Vec<u8> {
    let mut top: Map<String, Value> = Map::new();

    for (procedure_id, procedure_entities) in instance.entities.iter().enumerate() {
        let procedure_desc = match instance.topology.procedures.get(procedure_id) {
            Some(d) => d,
            None => continue,
        };
        let procedure_name = procedure_desc.name.to_string();

        if procedure_desc.entity_keys.is_empty() {
            // Single-mode: one Entity under the Procedure's name.
            let slot = match procedure_entities.first() {
                Some(s) => s,
                None => continue,
            };
            let state_guard = slot.state.lock_recover();
            match (procedure_desc.serialize_state)(&state_guard) {
                Ok(v) => {
                    top.insert(procedure_name, v);
                }
                Err(e) => {
                    eprintln!(
                        "[flezha] serialize_operative_state: procedure `{}` single-mode failed: {}",
                        procedure_desc.name, e
                    );
                }
            }
        } else {
            // Static-mode: nested `{entity_key: state, ...}`.
            let mut inner: Map<String, Value> = Map::new();
            for (idx, key) in procedure_desc.entity_keys.iter().enumerate() {
                let slot = match procedure_entities.get(idx) {
                    Some(s) => s,
                    None => continue,
                };
                let state_guard = match slot.state.lock() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                match (procedure_desc.serialize_state)(&state_guard) {
                    Ok(v) => {
                        inner.insert((*key).to_string(), v);
                    }
                    Err(e) => {
                        eprintln!(
                            "[flezha] serialize_operative_state: procedure `{}` key `{}` failed: {}",
                            procedure_desc.name, key, e
                        );
                    }
                }
            }
            top.insert(procedure_name, Value::Object(inner));
        }
    }

    serde_json::to_vec(&Value::Object(top)).unwrap_or_default()
}

/// Apply an `AddOperative.init_values` JSON blob to a fresh UnboundedOperative instance.
/// Walks the top-level keys, matches them against registered Procedure names,
/// and dispatches each value to the matching Procedure's `deserialize_state`
/// fn pointer to overwrite the freshly-`init_state`d Entity state.
///
/// Empty `init_values` is a no-op (the cold-start path uses `init_state`).
/// Unknown top-level keys and unknown inner entity_key entries are logged
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

    for (procedure_name, procedure_value) in top {
        let procedure_id = match instance
            .topology
            .procedure_name_to_id
            .get(procedure_name.as_str())
        {
            Some(&id) => id,
            None => {
                eprintln!(
                    "[flezha] apply_init_values: ignoring unknown procedure `{}`",
                    procedure_name
                );
                continue;
            }
        };
        let procedure_desc = match instance.topology.procedure(procedure_id) {
            Some(d) => d,
            None => continue,
        };
        let procedure_entities = match instance.entities.get(procedure_id as usize) {
            Some(p) => p,
            None => continue,
        };

        if procedure_desc.entity_keys.is_empty() {
            // Single-mode: the value is the State JSON itself.
            let slot = match procedure_entities.first() {
                Some(s) => s,
                None => continue,
            };
            apply_one(
                procedure_desc,
                slot,
                &procedure_value,
                &procedure_name,
                None,
            );
        } else {
            // Static-mode: the value is `{entity_key: state, ...}`.
            let inner = match procedure_value {
                Value::Object(m) => m,
                other => {
                    eprintln!(
                        "[flezha] apply_init_values: procedure `{}` expected object of \
                         entity_keys, got {} — skipping",
                        procedure_name,
                        value_kind(&other)
                    );
                    continue;
                }
            };
            let key_to_idx = match instance.entity_key_to_idx.get(procedure_id as usize) {
                Some(m) => m,
                None => continue,
            };
            for (key, value) in inner {
                let idx = match key_to_idx.get(key.as_str()) {
                    Some(&i) => i,
                    None => {
                        eprintln!(
                            "[flezha] apply_init_values: ignoring unknown entity_key \
                             `{}` for procedure `{}`",
                            key, procedure_name
                        );
                        continue;
                    }
                };
                let slot = match procedure_entities.get(idx) {
                    Some(s) => s,
                    None => continue,
                };
                apply_one(procedure_desc, slot, &value, &procedure_name, Some(&key));
            }
        }
    }

    Ok(())
}

fn apply_one(
    procedure_desc: &crate::operative::topology::ProcedureDescriptor,
    slot: &Arc<crate::entity::EntitySlot>,
    value: &Value,
    procedure_name: &str,
    entity_key: Option<&str>,
) {
    match (procedure_desc.deserialize_state)(value) {
        Ok(new_state) => {
            *slot.state.lock_recover() = new_state;
        }
        Err(e) => match entity_key {
            Some(k) => eprintln!(
                "[flezha] apply_init_values: procedure `{}` key `{}` deserialize failed: {}",
                procedure_name, k, e
            ),
            None => eprintln!(
                "[flezha] apply_init_values: procedure `{}` deserialize failed: {}",
                procedure_name, e
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
