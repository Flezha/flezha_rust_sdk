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
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp").into::<EchoOperation>();
    }
}

#[test]
fn single_mode_segment_creation() {
    let topology = build_topology_for::<SingleOperative>().expect("build topology");
    assert_eq!(topology.operations.len(), 1);
    assert_eq!(topology.channels.len(), 1);
    assert!(topology.operations[0].segment_keys.is_empty());
    assert_eq!(topology.operations[0].input_channel_ids, vec![0]);
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
    fn channels(c: &ChannelBuilder) {
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
    fn channels(c: &ChannelBuilder) {
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
    fn channels(c: &ChannelBuilder) {
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
    fn channels(c: &ChannelBuilder) {
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
    fn channels(c: &ChannelBuilder) {
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
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp")
            .into::<KeyedProducerOperation>()
            .named("out")
            .keyed()
            .output("kafka");
    }
}

#[test]
fn keyed_plugin_channel_is_accepted_and_marked() {
    let topology = build_topology_for::<KeyedPluginOperative>().expect("topology");
    let out = topology.lookup_channel_id("out").unwrap();
    assert!(topology.channels[out as usize].keyed);
    assert!(!topology.channels[0].keyed, "entry channel is not keyed");
}

/// `.keyed()` on a hybrid (`also_output_with`) channel is accepted.
struct KeyedHybridOperative;
impl UnboundedOperative for KeyedHybridOperative {
    fn factory_id() -> &'static str {
        "keyed_hybrid_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp")
            .into::<KeyedProducerOperation>()
            .named("out")
            .also_output_with("kafka", |v: &String| v.clone().into_bytes())
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
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp")
            .into::<KeyedProducerOperation>()
            .named("hop")
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
        flezha_runner::channel::builder::TopologyIssue::KeyedChannelWithoutPluginWire {
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
    fn channels(c: &ChannelBuilder) {
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
        flezha_runner::channel::builder::TopologyIssue::UnservableClock {
            operation: "frozen_logical_operation",
            clock_type: "LogicalClock",
        }
    )));
}

// ---- sdk-graph-authoring pins -------------------------------------------

test_operation!(GraphStage1, "graph_stage1", String => String, (), |_input, _state, _ctx| {
    Ok(())
});
test_operation!(GraphStage2, "graph_stage2", String => String, (), |_input, _state, _ctx| {
    Ok(())
});
test_operation!(GraphMerge, "graph_merge", String => String, (), |_input, _state, _ctx| {
    Ok(())
});

/// Flatten a frozen topology to a comparable shape: channels as
/// `(name, kind, producer, consumers)` plus the operation identity list.
fn topo_shape(
    t: &flezha_runner::operative::OperativeTopology,
) -> (
    Vec<(
        String,
        flezha_runner::channel::ChannelKind,
        Option<u32>,
        Vec<u32>,
    )>,
    Vec<String>,
) {
    (
        t.channels
            .iter()
            .map(|c| (c.name.to_string(), c.kind, c.producer, c.consumers.clone()))
            .collect(),
        t.operations.iter().map(|o| o.name.to_string()).collect(),
    )
}

/// `branch(...)` is pure sugar: the frozen topology is identical to the
/// handle-reuse spelling (same channels, consumers, derived names).
#[test]
fn branch_builds_the_identical_topology() {
    struct ByHandles;
    impl UnboundedOperative for ByHandles {
        fn factory_id() -> &'static str {
            "graph_by_handles"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoOperation>();
            validated.into::<GraphStage1>();
            validated.into::<GraphStage2>();
        }
    }
    struct ByBranch;
    impl UnboundedOperative for ByBranch {
        fn factory_id() -> &'static str {
            "graph_by_branch"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<EchoOperation>().branch([
                &|b| {
                    b.into::<GraphStage1>();
                },
                &|b| {
                    b.into::<GraphStage2>();
                },
            ]);
        }
    }
    let handles = build_topology_for::<ByHandles>().expect("handle-reuse builds");
    let branch = build_topology_for::<ByBranch>().expect("branch builds");
    assert_eq!(topo_shape(&handles), topo_shape(&branch));
}

#[test]
fn branch_with_fewer_than_two_arms_is_refused() {
    struct OneArm;
    impl UnboundedOperative for OneArm {
        fn factory_id() -> &'static str {
            "graph_one_arm"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EchoOperation>()
                .branch([&|b| {
                    b.into::<GraphStage1>();
                }]);
        }
    }
    let err = match build_topology_for::<OneArm>() {
        Ok(_) => panic!("one arm must fail"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::BranchTooFewArms { arms: 1 }
    )));
}

/// Node identity: an explicit node name creates a distinct node whose
/// identity replaces `name()` everywhere — descriptor name and the derived
/// channel name included — so one class serves several graph positions.
#[test]
fn named_nodes_reuse_one_class_at_two_positions() {
    struct TwoFilters;
    impl UnboundedOperative for TwoFilters {
        fn factory_id() -> &'static str {
            "graph_two_filters"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoOperation>();
            validated
                .into_named::<GraphStage1>("fraud_filter")
                .into::<GraphMerge>();
            validated
                .into_named::<GraphStage1>("spam_filter")
                .into::<GraphMerge>();
        }
    }
    let t = build_topology_for::<TwoFilters>().expect("named nodes build");
    let names: Vec<&str> = t.operations.iter().map(|o| o.name.as_ref()).collect();
    assert_eq!(
        names,
        vec![
            "echo_operation",
            "fraud_filter",
            "graph_merge",
            "spam_filter"
        ]
    );
    // Derived channel names are the node identities.
    assert!(t.lookup_channel_id("fraud_filter").is_some());
    assert!(t.lookup_channel_id("spam_filter").is_some());
    // The merge node accumulates both named nodes' channels: fan-in.
    let merge_id = *t.operation_name_to_id.get("graph_merge").unwrap();
    assert_eq!(t.operations[merge_id as usize].input_channel_ids.len(), 2);
}

/// Two unnamed mentions of one class resolve to the same node — which is
/// exactly how fan-in is spelled.
#[test]
fn unnamed_mentions_unify_into_a_fan_in() {
    struct FanInGraph;
    impl UnboundedOperative for FanInGraph {
        fn factory_id() -> &'static str {
            "graph_fan_in"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoOperation>();
            validated.into::<GraphStage1>().into::<GraphMerge>();
            validated.into::<GraphStage2>().into::<GraphMerge>();
        }
    }
    let t = build_topology_for::<FanInGraph>().expect("fan-in builds");
    assert_eq!(t.operations.len(), 4, "one Merge node, not two");
    let merge_id = *t.operation_name_to_id.get("graph_merge").unwrap();
    assert_eq!(
        t.operations[merge_id as usize].input_channel_ids.len(),
        2,
        "the join consumes both producers' channels"
    );
}

#[test]
fn node_identity_collision_is_refused() {
    struct Colliding;
    impl UnboundedOperative for Colliding {
        fn factory_id() -> &'static str {
            "graph_identity_collision"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoOperation>();
            // A node name equal to another operation's `name()`.
            validated.into_named::<GraphStage2>("graph_stage1");
            validated.into::<GraphStage1>();
        }
    }
    let err = match build_topology_for::<Colliding>() {
        Ok(_) => panic!("identity collision must fail"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::NodeIdentityCollision {
            name: "graph_stage1"
        }
    )));
}

/// The channel out of a node is named by the node's identity unless
/// `.named(...)` overrides it; all names share one namespace with a
/// duplicate check.
#[test]
fn channel_names_derive_from_the_producer_identity() {
    struct Derived;
    impl UnboundedOperative for Derived {
        fn factory_id() -> &'static str {
            "graph_derived_names"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EchoOperation>()
                .into::<GraphStage1>()
                .named("orders.validated")
                .into::<GraphStage2>();
        }
    }
    let t = build_topology_for::<Derived>().expect("derived names build");
    assert!(
        t.lookup_channel_id("echo_operation").is_some(),
        "no override ⇒ the producer's identity"
    );
    assert!(
        t.lookup_channel_id("orders.validated").is_some(),
        ".named(...) overrides the derived name"
    );

    struct NameClash;
    impl UnboundedOperative for NameClash {
        fn factory_id() -> &'static str {
            "graph_name_clash"
        }
        fn channels(c: &ChannelBuilder) {
            // The entry claims the name the derived channel would take.
            c.input::<String>("echo_operation")
                .into::<EchoOperation>()
                .into::<GraphStage1>();
        }
    }
    let err = match build_topology_for::<NameClash>() {
        Ok(_) => panic!("name collision must fail"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::DuplicateChannel("echo_operation")
    )));
}

#[test]
fn a_cycle_is_refused() {
    struct Cyclic;
    impl UnboundedOperative for Cyclic {
        fn factory_id() -> &'static str {
            "graph_cyclic"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<GraphStage1>()
                .into::<GraphStage2>()
                .into::<GraphStage1>();
        }
    }
    let err = match build_topology_for::<Cyclic>() {
        Ok(_) => panic!("a cycle must fail"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::Cycle { .. }
    )));
}

#[test]
fn multiple_entry_channels_build() {
    // A single operative may declare several entry channels — one per external
    // source (e.g. a market pipeline reading a trade feed and an order-book
    // feed on distinct channels). The controller routes each frame to its
    // entry by `metadata["channel"]`. Both entries and their subgraphs resolve.
    struct TwoEntries;
    impl UnboundedOperative for TwoEntries {
        fn factory_id() -> &'static str {
            "graph_two_entries"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in_a").into::<GraphStage1>();
            c.input::<String>("in_b").into::<GraphStage2>();
        }
    }
    let topology = build_topology_for::<TwoEntries>()
        .expect("multiple entry channels must build a valid topology");
    let entries: Vec<&str> = topology
        .channels
        .iter()
        .filter(|c| c.kind == flezha_runner::channel::ChannelKind::Entry)
        .map(|c| &*c.name)
        .collect();
    assert_eq!(entries.len(), 2, "both entry channels are retained");
    assert!(entries.contains(&"in_a") && entries.contains(&"in_b"));
}

// ---- typed entry inputs ---------------------------------------------------

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
struct OrderEvent {
    id: u64,
    sku: String,
}

test_operation!(OrderConsumer, "order_consumer", OrderEvent => String, (), |_input, _state, _ctx| {
    Ok(())
});

/// `input::<T>` for a non-passthrough type decodes JSON at the entry edge;
/// the descriptor is flagged as a real parser so a failing decode rides the
/// retry→DLQ ladder.
#[test]
fn typed_entry_decodes_json_at_the_edge() {
    struct TypedEntry;
    impl UnboundedOperative for TypedEntry {
        fn factory_id() -> &'static str {
            "graph_typed_entry"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<OrderEvent>("orders").into::<OrderConsumer>();
        }
    }
    let t = build_topology_for::<TypedEntry>().expect("typed entry builds");
    let entry = &t.channels[0];
    assert!(entry.entry_parsed, "a typed entry is a real parser");
    let wrap = entry.wrap_input.as_ref().expect("entry wrapper present");

    let ok = wrap(br#"{"id":7,"sku":"X1"}"#.to_vec()).expect("valid JSON decodes");
    let decoded = ok.downcast_ref::<OrderEvent>().expect("typed payload");
    assert_eq!(
        decoded,
        &OrderEvent {
            id: 7,
            sku: "X1".into()
        }
    );
    assert!(
        wrap(b"not json".to_vec()).is_err(),
        "a failing parse errs (and rides the channel's retry→DLQ ladder)"
    );
}

/// `input_with` replaces the default JSON codec with the supplied parser.
#[test]
fn a_custom_parser_replaces_the_default_json() {
    struct CustomEntry;
    impl UnboundedOperative for CustomEntry {
        fn factory_id() -> &'static str {
            "graph_custom_entry"
        }
        fn channels(c: &ChannelBuilder) {
            c.input_with::<OrderEvent, _>("orders", |bytes| {
                let s = String::from_utf8(bytes).map_err(|e| e.to_string())?;
                let (id, sku) = s.split_once(':').ok_or("expected id:sku")?;
                Ok(OrderEvent {
                    id: id.parse().map_err(|_| "bad id".to_string())?,
                    sku: sku.to_string(),
                })
            })
            .into::<OrderConsumer>();
        }
    }
    let t = build_topology_for::<CustomEntry>().expect("custom parser builds");
    let entry = &t.channels[0];
    assert!(entry.entry_parsed);
    let wrap = entry.wrap_input.as_ref().unwrap();
    // The custom format decodes; JSON (the default it replaced) does not.
    let ok = wrap(b"9:ABC".to_vec()).expect("custom format decodes");
    assert_eq!(ok.downcast_ref::<OrderEvent>().unwrap().sku, "ABC");
    assert!(wrap(br#"{"id":9,"sku":"ABC"}"#.to_vec()).is_err());
}

/// `Vec<u8>`/`String` entries keep the pre-typed-entry contract byte-identical:
/// passthrough / UTF-8 decode, flagged as non-parsers (invalid input keeps
/// the drop-and-log path, not the DLQ ladder).
#[test]
fn bytes_and_string_entries_stay_passthrough() {
    let t = build_topology_for::<SingleOperative>().expect("string entry builds");
    let entry = &t.channels[0];
    assert!(!entry.entry_parsed, "String is not a parser");
    let wrap = entry.wrap_input.as_ref().unwrap();
    let ok = wrap(b"plain".to_vec()).expect("utf-8 decodes");
    assert_eq!(ok.downcast_ref::<String>().unwrap(), "plain");
    assert!(wrap(vec![0xff, 0xfe]).is_err(), "invalid UTF-8 still errs");

    test_operation!(BytesConsumer, "bytes_consumer", Vec<u8> => String, (), |_input, _state, _ctx| {
        Ok(())
    });
    struct BytesEntry;
    impl UnboundedOperative for BytesEntry {
        fn factory_id() -> &'static str {
            "graph_bytes_entry"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<Vec<u8>>("raw").into::<BytesConsumer>();
        }
    }
    let t = build_topology_for::<BytesEntry>().expect("bytes entry builds");
    let entry = &t.channels[0];
    assert!(!entry.entry_parsed);
    let wrap = entry.wrap_input.as_ref().unwrap();
    let ok = wrap(vec![1, 2, 3]).expect("bytes pass through");
    assert_eq!(ok.downcast_ref::<Vec<u8>>().unwrap(), &vec![1, 2, 3]);
}
