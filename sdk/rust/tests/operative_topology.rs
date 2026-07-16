//! Section 2 unit tests: topology construction + UnboundedInstance segment
//! creation. Covers the umbrella scenarios:
//! - "Single segment for a Operation with no segmenting declared"
//! - "Static segments created at operative startup"
//! - "Dynamic-mode declaration is accepted and classified Dynamic"
//! - "Entry channel of arbitrary type fails at startup" (compile-time
//!   covered by tests/ui/entry_channel_disallowed_type.rs)

use std::sync::Arc;

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operation::{Operation, OperationCtx, OperationError};
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::state::{Change, NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::UnboundedOperative;

mod common;

test_operation!(EchoOperation, "echo_operation", String => String, (), |_input, _state, _ctx| {
    Ok(())
});

struct SingleOperative;
impl UnboundedOperative for SingleOperative {
    fn factory_id() -> &'static str {
        "single_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<EchoOperation>();
    }
}

#[test]
fn single_mode_segment_creation() {
    let topology = build_topology_for::<SingleOperative>().expect("build topology");
    assert_eq!(topology.operations.len(), 1);
    assert_eq!(topology.channels.len(), 1);
    assert!(topology.operations[0].segment_keys.is_empty());
    assert_eq!(topology.operations[0].input_channel_id, Some(0));
    assert_eq!(topology.channels[0].consumers, vec![0]);
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
struct AppendChange(String);
impl Change for AppendChange {}

#[derive(Default)]
struct StaticOperation;
impl Operation for StaticOperation {
    type Input = String;
    type Output = String;
    type State = Vec<String>;
    type Change = AppendChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_operation"
    }
    fn init_state(operation_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> Vec<String> {
        vec![operation_key.unwrap_or("none").to_string()]
    }
    fn apply_change(state: &mut Vec<String>, change: &AppendChange) {
        state.push(change.0.clone());
    }
    fn operation_keys() -> Vec<&'static str> {
        vec!["a", "b", "c"]
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(input: &String) -> Vec<&'static str> {
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
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        state.apply(AppendChange(input));
        Ok(())
    }
}

struct StaticOperative;
impl UnboundedOperative for StaticOperative {
    fn factory_id() -> &'static str {
        "static_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<StaticOperation>();
    }
}

#[test]
fn static_mode_segment_creation() {
    let topology = build_topology_for::<StaticOperative>().expect("build topology");
    assert_eq!(topology.operations.len(), 1);
    assert_eq!(topology.operations[0].segment_keys, vec!["a", "b", "c"]);
    assert!(topology.operations[0].segment_for_overridden);
}

/// Dynamic-mode Operation: overrides `operation_key` with an empty `operation_keys`.
/// `operative-dynamic-segmenting` accepts this and classifies it as
/// `SegmentingMode::Dynamic`.
#[derive(Default)]
struct DynamicOnlyOperation;
impl Operation for DynamicOnlyOperation {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "dynamic_only_operation"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    const OPERATION_KEY_OVERRIDDEN: bool = true;
    fn operation_key(_input: &String) -> Option<String> {
        Some("k".to_string())
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        Ok(())
    }
}

struct DynamicOnlyOperative;
impl UnboundedOperative for DynamicOnlyOperative {
    fn factory_id() -> &'static str {
        "dynamic_only_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<DynamicOnlyOperation>();
    }
}

#[test]
fn dynamic_mode_is_accepted_and_classified() {
    use flezha_runner::operative::topology::SegmentingMode;
    let topology = build_topology_for::<DynamicOnlyOperative>().expect("dynamic mode accepted");
    assert_eq!(topology.operations.len(), 1);
    assert_eq!(topology.operations[0].mode, SegmentingMode::Dynamic);
    assert!(topology.operations[0].segment_keys.is_empty());
    assert!(topology.operations[0].operation_key_erased.is_some());
}

/// Static-mode operation whose `segment_for` override is missing the
/// matching `SEGMENT_FOR_OVERRIDDEN = true` const.
#[derive(Default)]
struct UnannotatedStaticOperation;
impl Operation for UnannotatedStaticOperation {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "unannotated_static_operation"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn operation_keys() -> Vec<&'static str> {
        vec!["a"]
    }
    fn segment_for(_: &String) -> Vec<&'static str> {
        vec!["a"]
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        Ok(())
    }
}

struct UnannotatedOperative;
impl UnboundedOperative for UnannotatedOperative {
    fn factory_id() -> &'static str {
        "unannotated_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp")
            .into::<UnannotatedStaticOperation>();
    }
}

#[test]
fn unannotated_static_mode_rejection() {
    let err = build_topology_for::<UnannotatedOperative>()
        .err()
        .expect("expected segment_for-missing rejection");
    let msg = format!("{:?}", err.errors);
    assert!(
        msg.contains("SegmentForMissing"),
        "expected SegmentForMissing error, got {msg}"
    );
}

// Operation whose `mailbox_capacity()` returns 0; `build_topology` must
// reject the operative with the offending Operation's name.
test_operation!(ZeroCapacityOperation, "zero_capacity_operation", String => String, (), cap 0, |_input, _state, _ctx| {
    Ok(())
});

struct ZeroCapacityOperative;
impl UnboundedOperative for ZeroCapacityOperative {
    fn factory_id() -> &'static str {
        "zero_capacity_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<ZeroCapacityOperation>();
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
        msg.contains("zero_capacity_operation"),
        "expected offending Operation name in error, got {msg}"
    );
}

// Operation with explicit override of 4096 — the resolved capacity must
// equal that value when threaded through `build_topology`.
test_operation!(ExplicitCapOperation, "explicit_cap_operation", String => String, (), cap 4096, |_input, _state, _ctx| {
    Ok(())
});

struct ExplicitCapOperative;
impl UnboundedOperative for ExplicitCapOperative {
    fn factory_id() -> &'static str {
        "explicit_cap_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<ExplicitCapOperation>();
    }
}

#[test]
fn explicit_capacity_threads_to_topology_descriptor() {
    let topology = build_topology_for::<ExplicitCapOperative>().expect("topology");
    assert_eq!(topology.operations.len(), 1);
    assert_eq!(topology.operations[0].mailbox_capacity, 4096);
    assert!(topology.operations[0].mailbox_capacity_overridden);
}

/// Default Operation inherits the 1024 default.
#[test]
fn default_capacity_is_1024_in_topology() {
    let topology = build_topology_for::<SingleOperative>().expect("topology");
    assert_eq!(topology.operations[0].mailbox_capacity, 1024);
    assert!(!topology.operations[0].mailbox_capacity_overridden);
}

#[test]
fn operative_instance_creates_segments() {
    use flezha_runner::__dispatch_internals as bi;
    let topology = build_topology_for::<StaticOperative>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(2));
    let instance =
        flezha_runner::operative::UnboundedInstance::new(topology, Arc::from("instance-id"), pool);
    assert_eq!(instance.segments.len(), 1);
    assert_eq!(instance.segments[0].len(), 3);
    assert_eq!(instance.segment_key_to_idx[0].get("a"), Some(&0));
    assert_eq!(instance.segment_key_to_idx[0].get("b"), Some(&1));
    assert_eq!(instance.segment_key_to_idx[0].get("c"), Some(&2));
}

// ----- Keyed channel declaration + build-time clock validation -----

test_operation!(KeyedProducerOperation, "keyed_producer_operation", String => String, (), |input, _state, ctx| {
    ctx.emit_keyed::<String>(input.clone(), input)?;
    Ok(())
});

test_operation!(KeyedConsumerOperation, "keyed_consumer_operation", String => (), (), |_input, _state, _ctx| {
    Ok(())
});

/// `.keyed()` with a pure plugin wire is accepted and recorded.
struct KeyedPluginOperative;
impl UnboundedOperative for KeyedPluginOperative {
    fn factory_id() -> &'static str {
        "keyed_plugin_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<KeyedProducerOperation>();
        c.channel::<String>("out")
            .from::<KeyedProducerOperation>()
            .keyed()
            .to_plugin("kafka");
    }
}

#[test]
fn keyed_plugin_channel_is_accepted_and_marked() {
    let topology = build_topology_for::<KeyedPluginOperative>().expect("topology");
    let out = topology.lookup_channel_id("out").unwrap();
    assert!(topology.channels[out as usize].keyed);
    assert!(!topology.channels[0].keyed, "entry channel is not keyed");
}

/// `.keyed()` on a hybrid (`also_to_plugin_with`) channel is accepted.
struct KeyedHybridOperative;
impl UnboundedOperative for KeyedHybridOperative {
    fn factory_id() -> &'static str {
        "keyed_hybrid_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<KeyedProducerOperation>();
        c.channel::<String>("out")
            .from::<KeyedProducerOperation>()
            .also_to_plugin_with("kafka", |v: &String| v.clone().into_bytes())
            .keyed()
            .into::<KeyedConsumerOperation>();
    }
}

#[test]
fn keyed_hybrid_channel_is_accepted_and_marked() {
    let topology = build_topology_for::<KeyedHybridOperative>().expect("topology");
    let out = topology.lookup_channel_id("out").unwrap();
    assert!(topology.channels[out as usize].keyed);
    assert!(topology.channels[out as usize].plugin_sink.is_some());
}

/// `.keyed()` on a channel with no plugin wire fails the build with a
/// typed error naming the channel.
struct KeyedInterOperationOperative;
impl UnboundedOperative for KeyedInterOperationOperative {
    fn factory_id() -> &'static str {
        "keyed_inter_operation_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<KeyedProducerOperation>();
        c.channel::<String>("hop")
            .from::<KeyedProducerOperation>()
            .keyed()
            .into::<KeyedConsumerOperation>();
    }
}

#[test]
fn keyed_channel_without_plugin_wire_is_rejected() {
    let err = match build_topology_for::<KeyedInterOperationOperative>() {
        Ok(_) => panic!("keyed inter-operation channel must fail the build"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::BuildTopologyError::KeyedChannelWithoutPluginWire {
            channel: "hop"
        }
    )));
}

/// LogicalClock with `Change = NoChange` cannot be served (the durable
/// change seq never advances) and fails the build naming the clock.
#[derive(Default)]
struct FrozenLogicalOperation;
impl Operation for FrozenLogicalOperation {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::state::LogicalClock;
    type Config = ();
    fn name() -> &'static str {
        "frozen_logical_operation"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: String,
        _state: StateHandle<'_, Self>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        Ok(())
    }
}

struct FrozenLogicalOperative;
impl UnboundedOperative for FrozenLogicalOperative {
    fn factory_id() -> &'static str {
        "frozen_logical_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("inp").into::<FrozenLogicalOperation>();
    }
}

#[test]
fn logical_clock_without_persistent_changes_is_rejected_at_build() {
    let err = match build_topology_for::<FrozenLogicalOperative>() {
        Ok(_) => panic!("LogicalClock + NoChange must fail the build"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::BuildTopologyError::UnservableClock {
            operation: "frozen_logical_operation",
            clock_type: "LogicalClock",
        }
    )));
}
