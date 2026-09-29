//! Registration, spec resolution and authoring refusals: everything a
//! bounded class is checked for before an instance ever runs.

use std::sync::Arc;

use super::fixtures::*;
use crate::bounded::*;
use crate::channel::{ChannelBuilder, ChannelKind};
use crate::operator::registry::OperatorRegistry;
use crate::tasklet::{Ctx, Tasklet, TaskletError};
use crate::ErrorModel;

#[test]
fn bounded_error_conversions() {
    // String conversions stay retryable-by-default: the ladder is opt-out.
    let from_str: BoundedError = "x".into();
    assert_eq!(from_str.to_string(), "x");
    assert!(!from_str.non_retryable);
    let from_string: BoundedError = String::from("y").into();
    assert_eq!(from_string.message, "y");
    assert!(!from_string.non_retryable);

    let permanent = BoundedError::non_retryable("z");
    assert_eq!(permanent.to_string(), "z");
    assert!(permanent.non_retryable);
}

#[test]
fn register_bounded_makes_factory_resolvable() {
    struct RegBounded;
    impl BoundedOperator for RegBounded {
        fn factory_id() -> &'static str {
            "test-getclass-bounded"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<EchoTask>();
        }
    }
    let get_class = crate::operator::registry::get_class;
    assert!(get_class("test-getclass-bounded").is_none());
    crate::register_class::<crate::BoundedKind, RegBounded>().unwrap();
    let class = get_class("test-getclass-bounded").expect("registered");
    assert!(class.bounded.is_some(), "the class carries its kind");
    assert!(get_class("test-nonexistent-bounded").is_none());
}

/// One registry holds both kinds: a bounded instance is an operator
/// instance that carries a run.
#[test]
fn the_one_registry_holds_a_bounded_instance() {
    let reg = OperatorRegistry::new();
    let class = spec::build_spec::<EmitBounded>().expect("the fixture registers");
    reg.register(crate::operator::instance::OperatorInstance::new(
        class,
        Arc::from("t1"),
    ));

    assert!(reg.contains("t1"));
    let instance = reg.get("t1").expect("registered");
    assert_eq!(&*instance.instance_id, "t1");
    assert!(
        instance.bounded.is_some(),
        "a bounded instance carries a run"
    );
    assert!(reg.remove("t1").is_some());
    assert!(!reg.contains("t1"));
}

#[test]
fn a_bounded_without_tasklets_is_refused() {
    let err = spec::build_spec::<NoTaskletsBounded>().expect_err("a bounded always runs tasklets");
    assert!(
        matches!(err, spec::BuildBoundedError::NoTasklets { .. }),
        "expected NoTasklets, got {err:?}"
    );
    assert_eq!(err.code(), "topology.no_tasklets");
    assert_eq!(err.to_error().details["factory_id"], "test-no-tasklets");
}

#[test]
fn structured_spec_resolves_linear_chain() {
    let class = spec::build_spec::<ChainBounded>().expect("structured spec builds");
    let spec = class.bounded.as_ref().expect("a bounded class");
    let topology = &class;
    // Chain is ordered entry-consumer → terminal: Stage1 then Stage2.
    let names: Vec<&str> = spec
        .dag
        .order
        .iter()
        .map(|&id| topology.tasklet(id).unwrap().name.as_ref())
        .collect();
    assert_eq!(names, vec!["bounded_stage1", "bounded_stage2"]);
}

#[test]
fn external_stop_only_authoring_is_rejected() {
    let err = spec::build_spec::<ExternalStopOnlyBounded>()
        .expect_err("external-stop-only must be refused at registration");
    assert!(
        matches!(err, spec::BuildBoundedError::ExternalStopOnly { .. }),
        "expected ExternalStopOnly, got {err:?}"
    );
    assert_eq!(err.code(), "topology.external_stop_only");
    assert!(err.to_string().contains("max_duration"), "{err}");
}

#[test]
fn a_durable_handoff_is_journalable_with_nothing_declared() {
    struct NoCodec;
    impl BoundedOperator for NoCodec {
        fn factory_id() -> &'static str {
            "test-no-codec-bounded"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .into::<Stage2>();
        }
    }
    let class = spec::build_spec::<NoCodec>().expect("the runtime derives the handoff codec");
    let mid = class
        .channels
        .iter()
        .find(|c| &*c.name == "mid")
        .expect("the declared handoff channel exists");
    assert_eq!(mid.kind, ChannelKind::InterTasklet);

    // Derived, not declared: the channel round-trips Stage1's Output.
    let codec = mid
        .codec
        .as_ref()
        .expect("every produced channel has a codec");
    let value: Arc<dyn std::any::Any + Send + Sync> = Arc::new("handoff".to_string());
    let bytes = (codec.encode)(&*value).expect("the derived codec encodes");
    let back = (codec.decode)(bytes).expect("the derived codec decodes");
    assert_eq!(
        back.downcast_ref::<String>()
            .expect("the same type came back"),
        "handoff"
    );
}

/// `bounded-durable-loop` registration validation: a durable handoff-chained
/// loop needs a terminal tasklet whose output channel the journal carries —
/// otherwise a crash mid-loop would re-seed iteration N+1 with an empty batch
/// and silently read as "converged". Two shapes fail it: no output channel,
/// and a plugin-bound one.
#[test]
fn loop_authoring_is_validated() {
    for shape in ["no channel at all", "a plugin-bound channel"] {
        let err = match shape {
            "no channel at all" => spec::build_spec::<ChainedLoopNoTerminalOutput>(),
            _ => spec::build_spec::<ChainedLoopPluginTerminal>(),
        }
        .expect_err("a chained loop's terminal tasklet must release a handoff");
        match err {
            spec::BuildBoundedError::LoopTerminalWithoutHandoff { tasklet, .. } => {
                assert_eq!(
                    tasklet, "bounded_stage2",
                    "the error names the terminal tasklet ({shape})"
                );
            }
            other => panic!("expected LoopTerminalWithoutHandoff for {shape}, got {other:?}"),
        }
    }

    // An inbox-driven loop over the same topology is fine: it re-seeds each
    // iteration from its inbox, never from the terminal handoff.
    let class = spec::build_spec::<ChatLoop>().expect("PerInput loop registers");
    let spec = class.bounded.as_ref().expect("a bounded class");
    assert_eq!(spec.loop_mode, LoopMode::PerInput);
    assert_eq!(spec.dag.len(), 2);
}

/// `bounded-cell-retry` policy surface: a stage's ladder is the edge's, as a
/// stream's is. The default edge is 3 attempts from 50 ms, `retry_max(1)` is
/// fail-fast, and `retry_max(0)` is the shared topology refusal.
#[test]
fn the_retry_policy_is_the_edges() {
    let edge = |class: &crate::operator::topology::OperatorTopology, name: &str| {
        let id = class.lookup_channel_id(name).expect("channel declared");
        class.channel(id).expect("channel built").retry
    };
    let class = spec::build_spec::<ChainBounded>().expect("defaults register");
    let default = edge(&class, "in");
    assert_eq!((default.max_attempts, default.backoff_base_ms), (3, 50));

    let fail_fast = spec::build_spec::<RetryFailFastBounded>().expect("retry_max(1) registers");
    assert_eq!(edge(&fail_fast, "mid2").max_attempts, 1);

    struct NoAttempt;
    impl BoundedOperator for NoAttempt {
        fn factory_id() -> &'static str {
            "test-retry-no-attempt"
        }
        fn channels(c: &ChannelBuilder) {
            retry_chain(c, 0, 10);
        }
    }
    let refused = spec::build_spec::<NoAttempt>().expect_err("an edge needs one attempt");
    assert_eq!(refused.code(), "topology.retry_max_zero", "{refused}");
}

/// A sleeping instance is evicted and restored from its durable state, so a
/// class that keeps none is refused by name, whichever kind it is; a bounded operator
/// sleeps by default exactly when it persists.
#[test]
fn sleeping_without_persistence_is_refused_by_name_for_either_kind() {
    struct SleepyBounded;
    impl BoundedOperator for SleepyBounded {
        const PERSISTENCE_ENABLED: bool = false;
        const SLEEP_WHEN_IDLE: bool = true;
        fn factory_id() -> &'static str {
            "test-sleep-no-persist-wf"
        }
        fn channels(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
    }
    struct SleepyUnbounded;
    impl crate::operator::UnboundedOperator for SleepyUnbounded {
        const PERSISTENCE_ENABLED: bool = false;
        const SLEEP_WHEN_IDLE: bool = true;
        fn factory_id() -> &'static str {
            "test-sleep-no-persist-stream"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<EchoTask>();
        }
    }
    struct OptedOut;
    impl BoundedOperator for OptedOut {
        const PERSISTENCE_ENABLED: bool = false;
        fn factory_id() -> &'static str {
            "test-opted-out-wf"
        }
        fn channels(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
    }

    let bounded = spec::build_spec::<SleepyBounded>().expect_err("refused");
    assert_eq!(
        bounded.code(),
        "topology.sleep_without_persistence",
        "{bounded}"
    );
    let stream =
        crate::operator::builder::build_topology_for::<SleepyUnbounded>().expect_err("refused");
    assert_eq!(
        crate::ErrorModel::code(&stream),
        "topology.sleep_without_persistence",
        "{stream}"
    );
    let opted_out = spec::build_spec::<OptedOut>().expect("an opted-out bounded registers");
    assert!(!opted_out.sleep_when_idle);
    let durable = spec::build_spec::<ChainBounded>().expect("a durable bounded registers");
    assert!(durable.sleep_when_idle);
}

/// An unbounded operator's tier, checkpoint and correction settings are refused on a
/// bounded operator by the names the core gives them, never carried.
#[test]
fn a_stream_setting_on_a_bounded_is_refused_by_name() {
    struct Tier;
    impl BoundedOperator for Tier {
        fn factory_id() -> &'static str {
            "test-refused-tier"
        }
        fn channels(c: &ChannelBuilder) {
            c.effectively_once();
            two_stage_chain(c);
        }
    }
    struct Checkpoint;
    impl BoundedOperator for Checkpoint {
        fn factory_id() -> &'static str {
            "test-refused-checkpoint"
        }
        fn channels(c: &ChannelBuilder) {
            c.checkpoint_every(500);
            two_stage_chain(c);
        }
    }
    struct Correction;
    impl BoundedOperator for Correction {
        fn factory_id() -> &'static str {
            "test-refused-correction"
        }
        fn channels(c: &ChannelBuilder) {
            c.correction_window(60_000);
            two_stage_chain(c);
        }
    }
    assert!(matches!(
        spec::build_spec::<Tier>(),
        Err(spec::BuildBoundedError::EffectivelyOnceOnBounded { .. })
    ));
    assert!(matches!(
        spec::build_spec::<Checkpoint>(),
        Err(spec::BuildBoundedError::CheckpointOnBounded { .. })
    ));
    let correction = spec::build_spec::<Correction>().expect_err("a correction window is refused");
    assert!(matches!(
        correction,
        spec::BuildBoundedError::CorrectionWindowOnBounded { .. }
    ));
    assert_eq!(
        correction.code(),
        "topology.correction_window_on_bounded",
        "{correction}"
    );
}

/// The per-class synchronous-durability declaration reaches the spec the controller's `AddOperator` response is
/// built from. Off by default — a class that says nothing never makes a
/// commit barrier wait on a remote store.
#[test]
fn sync_durability_is_declared_per_class_and_off_by_default() {
    let default = spec::build_spec::<ChainBounded>().expect("defaults register");
    assert!(
        !default
            .bounded
            .as_ref()
            .expect("a bounded class")
            .sync_durability
    );

    struct SyncDurable;
    impl BoundedOperator for SyncDurable {
        const SYNC_DURABILITY: bool = true;
        fn factory_id() -> &'static str {
            "test-sync-durable"
        }
        fn channels(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
    }

    let class = spec::build_spec::<SyncDurable>().expect("a declaring class registers");
    let spec = class.bounded.as_ref().expect("a bounded class");
    assert!(spec.sync_durability);
}

/// `bounded-branching` registration validation. A DAG is accepted (with its
/// nodes in Kahn topological order, every producer before its consumers —
/// the invariant that makes completion order topological and its reverse a
/// valid compensation order); a cycle is refused, naming a node on it.
#[test]
fn dag_topology_is_validated() {
    // A→(B,C): one channel, two consumers. The edge is (channel, consumer),
    // so both branches read what Stage1 released on `mid`.
    let class = spec::build_spec::<TreeBounded>().expect("a fan-out tree registers");
    let spec = class.bounded.as_ref().expect("a bounded class");
    let topology = &class;
    let names: Vec<&str> = spec
        .dag
        .order
        .iter()
        .map(|&id| topology.tasklet(id).unwrap().name.as_ref())
        .collect();
    assert_eq!(
        names,
        vec!["bounded_stage1", "bounded_stage2", "bounded_echo_op"],
        "Kahn order: the root, then its children (= the former BFS numbering)"
    );
    assert_eq!(spec.dag.parents, vec![vec![], vec![0], vec![0]]);
    assert_eq!(spec.dag.children, vec![vec![1, 2], vec![], vec![]]);
    assert_eq!(spec.dag.leaves().collect::<Vec<_>>(), vec![1, 2]);
    assert!(
        spec.dag.entries[0].1.single_leaf(&spec.dag).is_none(),
        "two leaves ⇒ no single terminal tasklet"
    );
    for (node, parents) in spec.dag.parents.iter().enumerate() {
        for p in parents {
            assert!(
                *p < node,
                "a producer's index always precedes its consumers'"
            );
        }
    }

    // A linear chain is the single-branch case, unchanged.
    let linear = spec::build_spec::<ChainBounded>().expect("linear still registers");
    let linear = linear.bounded.as_ref().expect("a bounded class");
    assert_eq!(linear.dag.children, vec![vec![1], vec![]]);
    assert_eq!(linear.dag.entries[0].1.single_leaf(&linear.dag), Some(1));

    // A join — two streams feeding one node — registers, and Kahn numbers
    // BOTH producers before it (BFS would have enqueued the join on its
    // first producer).
    match spec::build_spec::<JoinBounded>() {
        Ok(class) => {
            let topology = &class;
            let spec = class.bounded.as_ref().expect("a bounded class");
            let names: Vec<&str> = spec
                .dag
                .order
                .iter()
                .map(|&id| topology.tasklet(id).unwrap().name.as_ref())
                .collect();
            assert_eq!(
                names,
                vec![
                    "bounded_stage1",
                    "bounded_stage2",
                    "bounded_echo_op",
                    "bounded_join_op"
                ]
            );
            assert_eq!(spec.dag.parents[3], vec![1, 2], "the join has two parents");
            assert_eq!(
                spec.dag.leaves().collect::<Vec<_>>(),
                vec![3],
                "the join is the single leaf"
            );
        }
        Err(e) => panic!("a join must register (fan-in accumulates): {e}"),
    }

    // A cycle is refused with an error identifying it (surfaced through the
    // shared topology validation).
    struct Cyclic;
    impl BoundedOperator for Cyclic {
        fn factory_id() -> &'static str {
            "test-cyclic-bounded"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .into::<Stage2>()
                .named("back")
                .into::<Stage1>();
        }
    }
    let refused = spec::build_spec::<Cyclic>().expect_err("a cycle is refused");
    assert!(
        matches!(refused, spec::BuildBoundedError::Topology { .. }),
        "expected the shared topology's refusal, got {refused:?}"
    );
    assert_eq!(refused.code(), "topology.cycle");

    // `multi-input-tasklets`: every message carries the channel it arrived
    // on, so an entry channel is a merge source on a bounded operator as on a
    // stream, and each entry channel roots its own chain over the merged
    // tasklet.
    struct EntryMergeBounded;
    impl BoundedOperator for EntryMergeBounded {
        fn factory_id() -> &'static str {
            "test-entry-merge-bounded"
        }
        fn channels(c: &ChannelBuilder) {
            c.merge::<MergedJoinInput>()
                .from(c.input::<String>("in"), MergedJoinInput::B)
                .from(c.input::<String>("also"), MergedJoinInput::C)
                .into::<MergeJoinTask>();
        }
    }
    let entry_merge = spec::build_spec::<EntryMergeBounded>()
        .expect("a bounded merge over two entry channels registers");
    let entry_merge = entry_merge.bounded.as_ref().expect("a bounded class");
    assert_eq!(
        entry_merge.dag.len(),
        1,
        "the merged tasklet is the one node"
    );
    assert_eq!(
        entry_merge
            .dag
            .entries
            .iter()
            .map(|(channel_id, chain)| (*channel_id, chain.roots.clone()))
            .collect::<Vec<_>>(),
        vec![(0, vec![0]), (1, vec![0])],
        "each entry channel roots its own chain at the merged tasklet"
    );

    // The inter-task form of the same merge is the supported one and still
    // registers, with the merged node as a two-parent inner node.
    let merged = spec::build_spec::<MergeBounded>().expect("an inter-task merge registers");
    let merged = merged.bounded.as_ref().expect("a bounded class");
    assert_eq!(
        merged.dag.parents[3],
        vec![1, 2],
        "the merged join has both producers as parents"
    );

    // A handoff-chained loop over a branching tree has no single terminal
    // batch to seed iteration N+1 from.
    struct BranchingChained;
    impl BoundedOperator for BranchingChained {
        fn factory_id() -> &'static str {
            "test-branching-chained-bounded"
        }
        fn loop_mode() -> LoopMode {
            LoopMode::Chained
        }
        fn channels(c: &ChannelBuilder) {
            tree_chain(c);
        }
    }
    match spec::build_spec::<BranchingChained>()
        .expect_err("a chained loop needs one terminal tasklet")
    {
        spec::BuildBoundedError::BranchingChainedLoop {
            channel, leaves, ..
        } => {
            assert_eq!((channel.as_str(), leaves), ("in", 2));
        }
        other => panic!("expected BranchingChainedLoop, got {other:?}"),
    }
}

#[test]
fn a_keyed_edge_makes_a_bounded_tasklet_dynamic() {
    #[derive(Default)]
    struct DynTask;
    #[flezha_runner::tasklet(name = "bounded_dyn_op")]
    impl Tasklet for DynTask {
        type Input = String;
        type Output = String;
        fn run(&mut self, _input: String, _ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
            Ok(())
        }
    }
    struct DynBounded;
    impl BoundedOperator for DynBounded {
        fn factory_id() -> &'static str {
            "test-dyn-bounded"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EchoTask>()
                .key_by(|s: &String| s.clone())
                .into::<DynTask>();
        }
    }
    let topology = spec::build_spec::<DynBounded>().expect("a keyed edge registers");
    let keyed = topology.tasklet_name_to_id["bounded_dyn_op"];
    assert_eq!(
        topology.tasklets[keyed as usize].mode,
        flz_core::operator::topology::CellMode::Dynamic
    );
    let instance =
        flz_core::operator::instance::OperatorInstance::new(topology, "dyn-bounded-1".into());
    assert!(
        instance.cells[keyed as usize].is_empty(),
        "no Cell before a pass"
    );
    assert!(instance.dynamic_registries[keyed as usize].is_some());
}
