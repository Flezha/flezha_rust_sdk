//! `EntityState` — return the UnboundedOperative instance's per-Procedure / per-Entity
//! state JSON blob per `operative-mvp-rust/design.md` Decision 10.
//!
//! `serialize_selected_state` walks each Procedure's entities via the per-Procedure
//! `serialize_state` fn pointer cached on `ProcedureDescriptor` (built at
//! registration time, captures S statically). `apply_init_values` walks
//! the controller-supplied JSON object and seeds each Entity's state
//! through the symmetric `deserialize_state` fn pointer.
//!
//! The command's optional `procedure` / `entity_key` selector narrows the walk
//! so a live read costs what it asked for; the selector-less call is the
//! migration/handover shape and covers every Procedure.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use serde_json::{Map, Value};

use super::CommandAction;
use crate::entity::dynamic::DynamicEntityRegistry;
use crate::entity::EntitySlot;
use crate::operative::instance::UnboundedInstance;
use crate::operative::topology::ProcedureDescriptor;
use crate::pb::EntityState;
use crate::runner::RunnerNode;
use crate::util::LockRecover;

/// Reply-frame metadata key carrying the `change_seq` a single selected
/// Entity was serialized at. Absent when the selector covers more than one.
pub const CHANGE_SEQ_META_KEY: &str = "flz.change_seq";

/// Canonical refusal tokens. Each error string starts with one so the
/// controller can map the refusal without parsing prose.
pub const ENTITY_KEY_WITHOUT_PROCEDURE: &str = "entity_key_without_procedure";
pub const PROCEDURE_NOT_FOUND: &str = "procedure_not_found";
pub const ENTITY_NOT_FOUND: &str = "entity_not_found";
pub const ENTITY_EVICTED: &str = "entity_evicted";

/// Which Entities a read covers. Both fields absent is the whole-Operative
/// (migration/handover) walk.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StateSelector<'a> {
    pub procedure: Option<&'a str>,
    pub entity_key: Option<&'a str>,
}

impl CommandAction for EntityState {
    fn handle(
        &self,
        runner: &mut RunnerNode,
        message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>) {
        let (payload_id, payload, _metadata) = self.reply(runner, message, errors);
        (payload_id, payload)
    }

    fn reply(
        &self,
        runner: &mut RunnerNode,
        _message: &str,
        errors: &mut Vec<String>,
    ) -> (String, Vec<u8>, HashMap<String, String>) {
        let payload_id = "entity_state".to_string();

        let instance = match runner.get_unbounded(&self.operative_id) {
            Some(i) => i,
            None => {
                errors.push(format!(
                    "UnboundedOperative instance with id {} not found",
                    self.operative_id
                ));
                return (payload_id, Vec::new(), HashMap::new());
            }
        };

        match serialize_selected_state(&instance, self.selector()) {
            Ok((payload, Some(seq))) => (
                payload_id,
                payload,
                HashMap::from([(CHANGE_SEQ_META_KEY.to_string(), seq.to_string())]),
            ),
            Ok((payload, None)) => (payload_id, payload, HashMap::new()),
            Err(refusal) => {
                errors.push(refusal);
                (payload_id, Vec::new(), HashMap::new())
            }
        }
    }
}

impl EntityState {
    /// The command's selector. Empty strings are absent — proto3 leaves an
    /// unset string empty, which is what keeps the selector-less encoding
    /// byte-identical to the pre-selector one.
    fn selector(&self) -> StateSelector<'_> {
        StateSelector {
            procedure: non_empty(&self.procedure),
            entity_key: non_empty(&self.entity_key),
        }
    }
}

fn non_empty(s: &str) -> Option<&str> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Whole-Operative walk — the migration/handover shape. Refusals are
/// impossible without a selector, so the JSON is returned directly.
pub fn serialize_operative_state(instance: &Arc<UnboundedInstance>) -> Vec<u8> {
    serialize_selected_state(instance, StateSelector::default())
        .map(|(payload, _seq)| payload)
        .unwrap_or_default()
}

/// Build the canonical JSON shape from `design.md` Decision 10, narrowed by
/// `selector`:
/// - Single-mode procedures: `{"<procedure_name>": <state>}`
/// - Static- and dynamic-mode procedures: `{"<procedure_name>": {"<entity_key>": <state>, ...}}`
///
/// State serialization goes through the per-Procedure `serialize_state` fn
/// pointer cached on `ProcedureDescriptor`, under the Entity's state lock and
/// without draining pending changes — the value therefore includes changes not
/// yet flushed to the WAL, and the returned seq says which version it is.
///
/// The seq is `Some` only when the selector named exactly one Entity. Errors
/// from individual entities are logged on stderr and that entity is omitted —
/// the controller treats a missing procedure key the same as no state.
pub fn serialize_selected_state(
    instance: &Arc<UnboundedInstance>,
    selector: StateSelector<'_>,
) -> Result<(Vec<u8>, Option<u64>), String> {
    if selector.entity_key.is_some() && selector.procedure.is_none() {
        return Err(format!(
            "{}: an Entity key is unique only within its Procedure — name the \
             Procedure as well",
            ENTITY_KEY_WITHOUT_PROCEDURE
        ));
    }

    let mut top: Map<String, Value> = Map::new();
    let mut seqs: Vec<u64> = Vec::new();
    let mut matched_procedure = false;

    for (procedure_id, procedure_desc) in instance.topology.procedures.iter().enumerate() {
        if selector
            .procedure
            .is_some_and(|wanted| wanted != &*procedure_desc.name)
        {
            continue;
        }
        matched_procedure = true;
        let procedure_name = procedure_desc.name.to_string();

        // Keyed Procedures — static (declared keys) and dynamic (the live
        // registry IS the keyset) — differ only in where the keyset comes from,
        // so both take the same lookup-one-or-walk-all shape below. Single-mode
        // has no keyset and sits under the Procedure's own name.
        let keyed = match Keyset::of(instance, procedure_id) {
            Some(keyed) => keyed,
            None => {
                if let Some(key) = selector.entity_key {
                    return Err(missing_entity_refusal(false, &procedure_name, key));
                }
                let Some(slot) = instance
                    .entities
                    .get(procedure_id)
                    .and_then(|entities| entities.first())
                else {
                    continue;
                };
                collect(procedure_desc, slot, &procedure_name, &mut top, &mut seqs);
                continue;
            }
        };

        let mut inner: Map<String, Value> = Map::new();
        match selector.entity_key {
            // A narrowed read must cost one lookup, not one walk — that is what
            // the selector is for.
            Some(wanted) => {
                let Some(slot) = keyed.get(wanted) else {
                    return Err(missing_entity_refusal(
                        keyed.was_evicted(wanted),
                        &procedure_name,
                        wanted,
                    ));
                };
                collect(procedure_desc, &slot, wanted, &mut inner, &mut seqs);
            }
            None => {
                for (key, slot) in keyed.pairs() {
                    collect(procedure_desc, &slot, &key, &mut inner, &mut seqs);
                }
            }
        }
        top.insert(procedure_name, Value::Object(inner));
    }

    if let Some(procedure) = selector.procedure {
        if !matched_procedure {
            return Err(format!(
                "{}: operative `{}` has no Procedure named `{}`",
                PROCEDURE_NOT_FOUND, instance.instance_id, procedure
            ));
        }
    }

    let payload = serde_json::to_vec(&Value::Object(top)).unwrap_or_default();
    let seq = match seqs.as_slice() {
        [only] => Some(*only),
        _ => None,
    };
    Ok((payload, seq))
}

/// A keyed Procedure's live Entities: the declared keys for static mode, the
/// registry for dynamic mode (which is where an evicted Entity disappears
/// from). Both answer a single-key lookup and a walk-all, so a caller expresses
/// one algorithm over either keyset — and a narrowed read never materializes
/// the keys it did not ask for.
enum Keyset<'a> {
    Static {
        keys: &'a [&'static str],
        slots: &'a [Arc<EntitySlot>],
    },
    Dynamic(&'a DynamicEntityRegistry),
}

impl<'a> Keyset<'a> {
    /// `None` for single mode, which has no keyset.
    fn of(instance: &'a Arc<UnboundedInstance>, procedure_id: usize) -> Option<Self> {
        if let Some(Some(registry)) = instance.dynamic_registries.get(procedure_id) {
            return Some(Keyset::Dynamic(registry));
        }
        let procedure_desc = instance.topology.procedures.get(procedure_id)?;
        if procedure_desc.entity_keys.is_empty() {
            return None;
        }
        Some(Keyset::Static {
            keys: &procedure_desc.entity_keys,
            slots: instance.entities.get(procedure_id)?,
        })
    }

    fn get(&self, key: &str) -> Option<Arc<EntitySlot>> {
        match self {
            Keyset::Static { keys, slots } => keys
                .iter()
                .position(|declared| *declared == key)
                .and_then(|idx| slots.get(idx))
                .map(Arc::clone),
            Keyset::Dynamic(registry) => registry.entities.get(key).map(|e| Arc::clone(e.value())),
        }
    }

    fn pairs(&self) -> Vec<(String, Arc<EntitySlot>)> {
        match self {
            Keyset::Static { keys, slots } => keys
                .iter()
                .zip(slots.iter())
                .map(|(key, slot)| ((*key).to_string(), Arc::clone(slot)))
                .collect(),
            Keyset::Dynamic(registry) => registry
                .entities
                .iter()
                .map(|e| (e.key().clone(), Arc::clone(e.value())))
                .collect(),
        }
    }

    /// Only dynamic mode evicts, so a static key that is absent never existed.
    fn was_evicted(&self, key: &str) -> bool {
        match self {
            Keyset::Static { .. } => false,
            Keyset::Dynamic(registry) => registry.recently_evicted(key),
        }
    }
}

/// Serialize one Entity into `into` under `key`, recording its seq. A failing
/// Entity is logged and omitted — the controller treats a missing key the same
/// as no state.
fn collect(
    procedure_desc: &ProcedureDescriptor,
    slot: &Arc<EntitySlot>,
    key: &str,
    into: &mut Map<String, Value>,
    seqs: &mut Vec<u64>,
) {
    match serialize_slot(procedure_desc, slot) {
        Ok((value, seq)) => {
            into.insert(key.to_string(), value);
            seqs.push(seq);
        }
        Err(e) => eprintln!(
            "[flezha] serialize_operative_state: procedure `{}` key `{}` failed: {}",
            procedure_desc.name, key, e
        ),
    }
}

/// Serialize one Entity's state and read the seq it was serialized at, both
/// under the state lock. Pending changes are deliberately NOT drained: a read
/// returns the freshest value and reports the version, where compaction must
/// drain because its seq becomes a truncation point.
fn serialize_slot(
    procedure_desc: &ProcedureDescriptor,
    slot: &Arc<EntitySlot>,
) -> Result<(Value, u64), String> {
    let state_guard = slot.state.lock_recover();
    let value = (procedure_desc.serialize_state)(&state_guard)?;
    let seq = slot.change_seq.load(Ordering::Relaxed);
    Ok((value, seq))
}

/// Eviction is destructive (the recreate cold-starts), so an evicted key has
/// no durable value either — the two refusals differ in diagnosis, not in
/// what could have been served.
fn missing_entity_refusal(evicted: bool, procedure_name: &str, key: &str) -> String {
    if evicted {
        format!(
            "{}: Entity `{}` of procedure `{}` was idle-evicted; eviction is \
             destructive, so there is no durable value to serve",
            ENTITY_EVICTED, key, procedure_name
        )
    } else {
        format!(
            "{}: procedure `{}` holds no Entity keyed `{}`",
            ENTITY_NOT_FOUND, procedure_name, key
        )
    }
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
