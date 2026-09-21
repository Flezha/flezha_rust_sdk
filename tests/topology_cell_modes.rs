//! Cell modes: one cell for a tasklet that declares none, the static
//! set created at startup, and a dynamic declaration classified as such.

use flezha_runner::state::{Change, NoChange, RestoreInitPayload, StateHandle, WallClock};

mod common;
use common::prelude::*;

#[test]
fn single_mode_cell_creation() {
    let topology = build_topology_for::<SingleOperative>().expect("build topology");
    assert_eq!(topology.tasklets.len(), 1);
    assert_eq!(topology.channels.len(), 1);
    assert!(topology.tasklets[0].cell_keys.is_empty());
    assert_eq!(topology.tasklets[0].input_channel_ids, vec![0]);
    assert_eq!(
        topology.channels[0].consumer_ids().collect::<Vec<_>>(),
        vec![0]
    );
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
struct AppendChange(String);
impl Change for AppendChange {}

#[derive(Default)]
struct StaticTasklet;
impl Tasklet for StaticTasklet {
    type Input = String;
    type Output = String;
    type State = Vec<String>;
    type Change = AppendChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_tasklet"
    }
    fn init_state(cell_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> Vec<String> {
        vec![cell_key.unwrap_or("none").to_string()]
    }
    fn apply_change(state: &mut Vec<String>, change: &AppendChange) {
        state.push(change.0.clone());
    }
    fn cell_keys() -> Vec<&'static str> {
        vec!["a", "b", "c"]
    }
    const CELLS_FOR_OVERRIDDEN: bool = true;
    fn cells_for(input: &String) -> Vec<&'static str> {
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
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
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
        c.input::<String>("inp").into::<StaticTasklet>();
    }
}

#[test]
fn static_mode_cell_creation() {
    let topology = build_topology_for::<StaticOperative>().expect("build topology");
    assert_eq!(topology.tasklets.len(), 1);
    assert_eq!(topology.tasklets[0].cell_keys, vec!["a", "b", "c"]);
    assert!(topology.tasklets[0].cells_for_overridden);
}

/// Dynamic-mode Tasklet: declares no keying of its own — the `.key_by` on
/// the wiring is what classifies it as `CellMode::Dynamic`.
#[derive(Default)]
struct DynamicOnlyTasklet;
impl Tasklet for DynamicOnlyTasklet {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "dynamic_only_tasklet"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
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
            .into::<DynamicOnlyTasklet>();
    }
}

#[test]
fn dynamic_mode_is_accepted_and_classified() {
    use flezha_runner::operative::topology::CellMode;
    let topology = build_topology_for::<DynamicOnlyOperative>().expect("dynamic mode accepted");
    assert_eq!(topology.tasklets.len(), 1);
    assert_eq!(topology.tasklets[0].mode, CellMode::Dynamic);
    assert!(topology.tasklets[0].cell_keys.is_empty());
    assert!(topology.channels[0].consumers[0].key.is_some());
}

/// Static-mode tasklet whose `cells_for` override is missing the
/// matching `CELLS_FOR_OVERRIDDEN = true` const.
#[derive(Default)]
struct UnannotatedStaticTasklet;
impl Tasklet for UnannotatedStaticTasklet {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "unannotated_static_tasklet"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn cell_keys() -> Vec<&'static str> {
        vec!["a"]
    }
    fn cells_for(_: &String) -> Vec<&'static str> {
        vec!["a"]
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}

struct UnannotatedOperative;
impl StreamOperative for UnannotatedOperative {
    fn factory_id() -> &'static str {
        "unannotated_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp").into::<UnannotatedStaticTasklet>();
    }
}

#[test]
fn unannotated_static_mode_rejection() {
    let err = build_topology_for::<UnannotatedOperative>()
        .err()
        .expect("expected cells_for-missing rejection");
    let msg = format!("{:?}", err.errors);
    assert!(
        msg.contains("CellsForMissing"),
        "expected CellsForMissing error, got {msg}"
    );
}

// Tasklet whose `mailbox_capacity()` returns 0; `build_topology` must
// reject the operative with the offending Tasklet's name.
test_tasklet!(ZeroCapacityTasklet, "zero_capacity_tasklet", String => String, (), cap 0, |_input, _state, _ctx| {
    Ok(())
});

struct ZeroCapacityOperative;
impl StreamOperative for ZeroCapacityOperative {
    fn factory_id() -> &'static str {
        "zero_capacity_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp").into::<ZeroCapacityTasklet>();
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
        msg.contains("zero_capacity_tasklet"),
        "expected offending Tasklet name in error, got {msg}"
    );
}

// Tasklet with explicit override of 4096 — the resolved capacity must
// equal that value when threaded through `build_topology`.
test_tasklet!(ExplicitCapTasklet, "explicit_cap_tasklet", String => String, (), cap 4096, |_input, _state, _ctx| {
    Ok(())
});

struct ExplicitCapOperative;
impl StreamOperative for ExplicitCapOperative {
    fn factory_id() -> &'static str {
        "explicit_cap_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp").into::<ExplicitCapTasklet>();
    }
}

#[test]
fn explicit_capacity_threads_to_topology_descriptor() {
    let topology = build_topology_for::<ExplicitCapOperative>().expect("topology");
    assert_eq!(topology.tasklets.len(), 1);
    assert_eq!(topology.tasklets[0].mailbox_capacity, 4096);
    assert!(topology.tasklets[0].mailbox_capacity_overridden);
}

/// Default Tasklet inherits the 1024 default.
#[test]
fn default_capacity_is_1024_in_topology() {
    let topology = build_topology_for::<SingleOperative>().expect("topology");
    assert_eq!(topology.tasklets[0].mailbox_capacity, 1024);
    assert!(!topology.tasklets[0].mailbox_capacity_overridden);
}

#[test]
fn operative_instance_creates_cells() {
    let topology = build_topology_for::<StaticOperative>().expect("topology");
    let instance =
        flezha_runner::operative::StreamInstance::new(topology, Arc::from("instance-id"));
    assert_eq!(instance.cells.len(), 1);
    assert_eq!(instance.cells[0].len(), 3);
    assert_eq!(instance.cell_key_to_idx[0].get("a"), Some(&0));
    assert_eq!(instance.cell_key_to_idx[0].get("b"), Some(&1));
    assert_eq!(instance.cell_key_to_idx[0].get("c"), Some(&2));
}
