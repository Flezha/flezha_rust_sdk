//! Entity modes: one entity for a procedure that declares none, the static
//! set created at startup, and a dynamic declaration classified as such.

use std::sync::Arc;

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{Change, NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::StreamOperative;

mod common;
use common::*;

#[test]
fn single_mode_entity_creation() {
    let topology = build_topology_for::<SingleOperative>().expect("build topology");
    assert_eq!(topology.procedures.len(), 1);
    assert_eq!(topology.channels.len(), 1);
    assert!(topology.procedures[0].entity_keys.is_empty());
    assert_eq!(topology.procedures[0].input_channel_ids, vec![0]);
    assert_eq!(
        topology.channels[0].consumer_ids().collect::<Vec<_>>(),
        vec![0]
    );
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
struct AppendChange(String);
impl Change for AppendChange {}

#[derive(Default)]
struct StaticProcedure;
impl Procedure for StaticProcedure {
    type Input = String;
    type Output = String;
    type State = Vec<String>;
    type Change = AppendChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_procedure"
    }
    fn init_state(entity_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> Vec<String> {
        vec![entity_key.unwrap_or("none").to_string()]
    }
    fn apply_change(state: &mut Vec<String>, change: &AppendChange) {
        state.push(change.0.clone());
    }
    fn entity_keys() -> Vec<&'static str> {
        vec!["a", "b", "c"]
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(input: &String) -> Vec<&'static str> {
        if input.starts_with('a') {
            vec!["a"]
        } else if input.starts_with('b') {
            vec!["b"]
        } else {
            vec!["c"]
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(AppendChange(input));
        Ok(())
    }
}

struct StaticOperative;
impl StreamOperative for StaticOperative {
    fn factory_id() -> &'static str {
        "static_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp").into::<StaticProcedure>();
    }
}

#[test]
fn static_mode_entity_creation() {
    let topology = build_topology_for::<StaticOperative>().expect("build topology");
    assert_eq!(topology.procedures.len(), 1);
    assert_eq!(topology.procedures[0].entity_keys, vec!["a", "b", "c"]);
    assert!(topology.procedures[0].entities_for_overridden);
}

/// Dynamic-mode Procedure: declares no keying of its own — the `.key_by` on
/// the wiring is what classifies it as `EntityMode::Dynamic`.
#[derive(Default)]
struct DynamicOnlyProcedure;
impl Procedure for DynamicOnlyProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "dynamic_only_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

struct DynamicOnlyOperative;
impl StreamOperative for DynamicOnlyOperative {
    fn factory_id() -> &'static str {
        "dynamic_only_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp")
            .key_by(|s: &String| s.clone())
            .into::<DynamicOnlyProcedure>();
    }
}

#[test]
fn dynamic_mode_is_accepted_and_classified() {
    use flezha_runner::operative::topology::EntityMode;
    let topology = build_topology_for::<DynamicOnlyOperative>().expect("dynamic mode accepted");
    assert_eq!(topology.procedures.len(), 1);
    assert_eq!(topology.procedures[0].mode, EntityMode::Dynamic);
    assert!(topology.procedures[0].entity_keys.is_empty());
    assert!(topology.channels[0].consumers[0].key.is_some());
}

/// Static-mode procedure whose `entities_for` override is missing the
/// matching `ENTITIES_FOR_OVERRIDDEN = true` const.
#[derive(Default)]
struct UnannotatedStaticProcedure;
impl Procedure for UnannotatedStaticProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "unannotated_static_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn entity_keys() -> Vec<&'static str> {
        vec!["a"]
    }
    fn entities_for(_: &String) -> Vec<&'static str> {
        vec!["a"]
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

struct UnannotatedOperative;
impl StreamOperative for UnannotatedOperative {
    fn factory_id() -> &'static str {
        "unannotated_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp")
            .into::<UnannotatedStaticProcedure>();
    }
}

#[test]
fn unannotated_static_mode_rejection() {
    let err = build_topology_for::<UnannotatedOperative>()
        .err()
        .expect("expected entities_for-missing rejection");
    let msg = format!("{:?}", err.errors);
    assert!(
        msg.contains("EntitiesForMissing"),
        "expected EntitiesForMissing error, got {msg}"
    );
}

// Procedure whose `mailbox_capacity()` returns 0; `build_topology` must
// reject the operative with the offending Procedure's name.
test_procedure!(ZeroCapacityProcedure, "zero_capacity_procedure", String => String, (), cap 0, |_input, _state, _ctx| {
    Ok(())
});

struct ZeroCapacityOperative;
impl StreamOperative for ZeroCapacityOperative {
    fn factory_id() -> &'static str {
        "zero_capacity_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp").into::<ZeroCapacityProcedure>();
    }
}

#[test]
fn capacity_zero_is_rejected_at_operative_startup() {
    let err = build_topology_for::<ZeroCapacityOperative>()
        .err()
        .expect("expected mailbox-capacity-zero rejection");
    let msg = format!("{:?}", err.errors);
    assert!(
        msg.contains("MailboxCapacityZero"),
        "expected MailboxCapacityZero error, got {msg}"
    );
    assert!(
        msg.contains("zero_capacity_procedure"),
        "expected offending Procedure name in error, got {msg}"
    );
}

// Procedure with explicit override of 4096 — the resolved capacity must
// equal that value when threaded through `build_topology`.
test_procedure!(ExplicitCapProcedure, "explicit_cap_procedure", String => String, (), cap 4096, |_input, _state, _ctx| {
    Ok(())
});

struct ExplicitCapOperative;
impl StreamOperative for ExplicitCapOperative {
    fn factory_id() -> &'static str {
        "explicit_cap_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp").into::<ExplicitCapProcedure>();
    }
}

#[test]
fn explicit_capacity_threads_to_topology_descriptor() {
    let topology = build_topology_for::<ExplicitCapOperative>().expect("topology");
    assert_eq!(topology.procedures.len(), 1);
    assert_eq!(topology.procedures[0].mailbox_capacity, 4096);
    assert!(topology.procedures[0].mailbox_capacity_overridden);
}

/// Default Procedure inherits the 1024 default.
#[test]
fn default_capacity_is_1024_in_topology() {
    let topology = build_topology_for::<SingleOperative>().expect("topology");
    assert_eq!(topology.procedures[0].mailbox_capacity, 1024);
    assert!(!topology.procedures[0].mailbox_capacity_overridden);
}

#[test]
fn operative_instance_creates_entities() {
    let topology = build_topology_for::<StaticOperative>().expect("topology");
    let instance =
        flezha_runner::operative::StreamInstance::new(topology, Arc::from("instance-id"));
    assert_eq!(instance.entities.len(), 1);
    assert_eq!(instance.entities[0].len(), 3);
    assert_eq!(instance.entity_key_to_idx[0].get("a"), Some(&0));
    assert_eq!(instance.entity_key_to_idx[0].get("b"), Some(&1));
    assert_eq!(instance.entity_key_to_idx[0].get("c"), Some(&2));
}
