//! Graph authoring: the shapes the wiring accepts, the refusals it answers
//! with, and how a typed entry channel decodes at the edge.

mod common;
use common::prelude::*;

// ---- sdk-graph-authoring pins -------------------------------------------

test_procedure!(GraphStage1, "graph_stage1", String => String, (), |_input, _state, _ctx| {
    Ok(())
});
test_procedure!(GraphStage2, "graph_stage2", String => String, (), |_input, _state, _ctx| {
    Ok(())
});
test_procedure!(GraphMerge, "graph_merge", String => String, (), |_input, _state, _ctx| {
    Ok(())
});

/// One channel flattened to `(name, kind, producer, consumers)`.
type ChannelShape = (
    String,
    flezha_runner::channel::ChannelKind,
    Option<u32>,
    Vec<u32>,
);

/// Flatten a frozen topology to a comparable shape: channels as
/// `(name, kind, producer, consumers)` plus the procedure identity list.
fn topo_shape(t: &flezha_runner::operative::OperativeTopology) -> (Vec<ChannelShape>, Vec<String>) {
    (
        t.channels
            .iter()
            .map(|c| {
                (
                    c.name.to_string(),
                    c.kind,
                    c.producer,
                    c.consumer_ids().collect::<Vec<_>>(),
                )
            })
            .collect(),
        t.procedures.iter().map(|o| o.name.to_string()).collect(),
    )
}

/// `branch(...)` is pure sugar: the frozen topology is identical to the
/// handle-reuse spelling (same channels, consumers, derived names).
#[test]
fn branch_builds_the_identical_topology() {
    struct ByHandles;
    impl StreamOperative for ByHandles {
        fn factory_id() -> &'static str {
            "graph_by_handles"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoProcedure>();
            validated.into::<GraphStage1>();
            validated.into::<GraphStage2>();
        }
    }
    struct ByBranch;
    impl StreamOperative for ByBranch {
        fn factory_id() -> &'static str {
            "graph_by_branch"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<EchoProcedure>().branch([
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
    impl StreamOperative for OneArm {
        fn factory_id() -> &'static str {
            "graph_one_arm"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EchoProcedure>()
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
    impl StreamOperative for TwoFilters {
        fn factory_id() -> &'static str {
            "graph_two_filters"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoProcedure>();
            validated
                .into_named::<GraphStage1>("fraud_filter")
                .into::<GraphMerge>();
            validated
                .into_named::<GraphStage1>("spam_filter")
                .into::<GraphMerge>();
        }
    }
    let t = build_topology_for::<TwoFilters>().expect("named nodes build");
    let names: Vec<&str> = t.procedures.iter().map(|o| o.name.as_ref()).collect();
    assert_eq!(
        names,
        vec![
            "echo_procedure",
            "fraud_filter",
            "graph_merge",
            "spam_filter"
        ]
    );
    // Derived channel names are the node identities.
    assert!(t.lookup_channel_id("fraud_filter").is_some());
    assert!(t.lookup_channel_id("spam_filter").is_some());
    // The merge node accumulates both named nodes' channels: fan-in.
    let merge_id = *t.procedure_name_to_id.get("graph_merge").unwrap();
    assert_eq!(t.procedures[merge_id as usize].input_channel_ids.len(), 2);
}

/// Two unnamed mentions of one class resolve to the same node — which is
/// exactly how fan-in is spelled.
#[test]
fn unnamed_mentions_unify_into_a_fan_in() {
    struct FanInGraph;
    impl StreamOperative for FanInGraph {
        fn factory_id() -> &'static str {
            "graph_fan_in"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoProcedure>();
            validated.into::<GraphStage1>().into::<GraphMerge>();
            validated.into::<GraphStage2>().into::<GraphMerge>();
        }
    }
    let t = build_topology_for::<FanInGraph>().expect("fan-in builds");
    assert_eq!(t.procedures.len(), 4, "one Merge node, not two");
    let merge_id = *t.procedure_name_to_id.get("graph_merge").unwrap();
    assert_eq!(
        t.procedures[merge_id as usize].input_channel_ids.len(),
        2,
        "the join consumes both producers' channels"
    );
}

#[test]
fn node_identity_collision_is_refused() {
    struct Colliding;
    impl StreamOperative for Colliding {
        fn factory_id() -> &'static str {
            "graph_identity_collision"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoProcedure>();
            // A node name equal to another procedure's `name()`.
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
        flezha_runner::channel::builder::TopologyIssue::NodeIdentityCollision { name }
            if name == "graph_stage1"
    )));
}

/// The channel out of a node is named by the node's identity unless
/// `.named(...)` overrides it; all names share one namespace with a
/// duplicate check.
#[test]
fn channel_names_derive_from_the_producer_identity() {
    struct Derived;
    impl StreamOperative for Derived {
        fn factory_id() -> &'static str {
            "graph_derived_names"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EchoProcedure>()
                .into::<GraphStage1>()
                .named("orders.validated")
                .into::<GraphStage2>();
        }
    }
    let t = build_topology_for::<Derived>().expect("derived names build");
    assert!(
        t.lookup_channel_id("echo_procedure").is_some(),
        "no override ⇒ the producer's identity"
    );
    assert!(
        t.lookup_channel_id("orders.validated").is_some(),
        ".named(...) overrides the derived name"
    );

    struct NameClash;
    impl StreamOperative for NameClash {
        fn factory_id() -> &'static str {
            "graph_name_clash"
        }
        fn channels(c: &ChannelBuilder) {
            // The entry claims the name the derived channel would take.
            c.input::<String>("echo_procedure")
                .into::<EchoProcedure>()
                .into::<GraphStage1>();
        }
    }
    let err = match build_topology_for::<NameClash>() {
        Ok(_) => panic!("name collision must fail"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::DuplicateChannel(name)
            if name == "echo_procedure"
    )));
}

#[test]
fn a_cycle_is_refused() {
    struct Cyclic;
    impl StreamOperative for Cyclic {
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
    impl StreamOperative for TwoEntries {
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

test_procedure!(OrderConsumer, "order_consumer", OrderEvent => String, (), |_input, _state, _ctx| {
    Ok(())
});

/// `input::<T>` for a non-passthrough type decodes JSON at the entry edge;
/// the descriptor is flagged as a real parser so a failing decode rides the
/// retry→DLQ ladder.
#[test]
fn typed_entry_decodes_json_at_the_edge() {
    struct TypedEntry;
    impl StreamOperative for TypedEntry {
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
    impl StreamOperative for CustomEntry {
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

    test_procedure!(BytesConsumer, "bytes_consumer", Vec<u8> => String, (), |_input, _state, _ctx| {
        Ok(())
    });
    struct BytesEntry;
    impl StreamOperative for BytesEntry {
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
