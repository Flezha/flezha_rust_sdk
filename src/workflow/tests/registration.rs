//! Registration, spec resolution and authoring refusals: everything a
//! workflow class is checked for before an instance ever runs.

use std::sync::Arc;

use super::fixtures::*;
use crate::cell::restore::RestoreInitPayload;
use crate::channel::{ChannelBuilder, ChannelKind};
use crate::operative::registry::OperativeRegistry;
use crate::tasklet::change::NoChange;
use crate::tasklet::clock::WallClock;
use crate::tasklet::{StateHandle, Tasklet, TaskletCtx, TaskletError};
use crate::workflow::*;

#[test]
fn workflow_error_conversions() {
    // String conversions stay retryable-by-default: the ladder is opt-out.
    let from_str: WorkflowError = "x".into();
    assert_eq!(from_str.to_string(), "x");
    assert!(!from_str.non_retryable);
    let from_string: WorkflowError = String::from("y").into();
    assert_eq!(from_string.message, "y");
    assert!(!from_string.non_retryable);

    let permanent = WorkflowError::non_retryable("z");
    assert_eq!(permanent.to_string(), "z");
    assert!(permanent.non_retryable);
}

#[test]
fn register_workflow_makes_factory_resolvable() {
    struct RegWorkflow;
    impl WorkflowOperative for RegWorkflow {
        fn factory_id() -> &'static str {
            "test-getclass-workflow"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<EchoTask>();
        }
    }
    let get_class = crate::operative::registry::get_class;
    assert!(get_class("test-getclass-workflow").is_none());
    crate::register_class::<crate::WorkflowKind, RegWorkflow>().unwrap();
    let class = get_class("test-getclass-workflow").expect("registered");
    assert!(class.workflow.is_some(), "the class carries its kind");
    assert!(get_class("test-nonexistent-workflow").is_none());
}

/// One registry holds both kinds: a workflow instance is an operative
/// instance that carries a run.
#[test]
fn the_one_registry_holds_a_workflow_instance() {
    let reg = OperativeRegistry::new();
    let class = spec::build_spec::<EmitWorkflow>().expect("the fixture registers");
    reg.register(crate::operative::instance::OperativeInstance::new(
        class,
        Arc::from("t1"),
    ));

    assert!(reg.contains("t1"));
    let instance = reg.get("t1").expect("registered");
    assert_eq!(&*instance.instance_id, "t1");
    assert!(
        instance.workflow.is_some(),
        "a workflow instance carries a run"
    );
    assert!(reg.remove("t1").is_some());
    assert!(!reg.contains("t1"));
}

#[test]
fn a_workflow_without_tasklets_is_refused() {
    let err =
        spec::build_spec::<NoTaskletsWorkflow>().expect_err("a workflow always runs tasklets");
    assert!(
        matches!(err, spec::BuildWorkflowError::NoTasklets { .. }),
        "expected NoTasklets, got {err:?}"
    );
    assert!(err.to_string().contains("test-no-tasklets"), "{err}");
}

#[test]
fn structured_spec_resolves_linear_chain() {
    let class = spec::build_spec::<ChainWorkflow>().expect("structured spec builds");
    let spec = class.workflow.as_ref().expect("a workflow class");
    let topology = &class;
    // Chain is ordered entry-consumer → terminal: Stage1 then Stage2.
    let names: Vec<&str> = spec
        .dag
        .order
        .iter()
        .map(|&id| topology.tasklet(id).unwrap().name.as_ref())
        .collect();
    assert_eq!(names, vec!["workflow_stage1", "workflow_stage2"]);
}

#[test]
fn external_stop_only_authoring_is_rejected() {
    let err = spec::build_spec::<ExternalStopOnlyWorkflow>()
        .expect_err("external-stop-only must be refused at registration");
    assert!(
        matches!(err, spec::BuildWorkflowError::ExternalStopOnly { .. }),
        "expected ExternalStopOnly, got {err:?}"
    );
    // Naming the missing terminal condition is part of the contract.
    assert!(err.to_string().contains("terminal condition"));
}

#[test]
fn a_durable_handoff_is_journalable_with_nothing_declared() {
    struct NoCodec;
    impl WorkflowOperative for NoCodec {
        fn factory_id() -> &'static str {
            "test-no-codec-workflow"
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

/// `workflow-durable-loop` registration validation: a durable handoff-chained
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
            spec::BuildWorkflowError::LoopTerminalWithoutHandoff { tasklet, .. } => {
                assert_eq!(
                    tasklet, "workflow_stage2",
                    "the error names the terminal tasklet ({shape})"
                );
            }
            other => panic!("expected LoopTerminalWithoutHandoff for {shape}, got {other:?}"),
        }
    }

    // An inbox-driven loop over the same topology is fine: it re-seeds each
    // iteration from its inbox, never from the terminal handoff.
    let class = spec::build_spec::<ChatLoop>().expect("PerInput loop registers");
    let spec = class.workflow.as_ref().expect("a workflow class");
    assert_eq!(spec.loop_mode, LoopMode::PerInput);
    assert_eq!(spec.dag.len(), 2);
}

/// `workflow-cell-retry` policy surface: a stage's ladder is the edge's, as a
/// stream's is. The default edge is 3 attempts from 50 ms, `retry_max(1)` is
/// fail-fast, and `retry_max(0)` is the shared topology refusal.
#[test]
fn the_retry_policy_is_the_edges() {
    let edge = |class: &crate::operative::topology::OperativeTopology, name: &str| {
        let id = class.lookup_channel_id(name).expect("channel declared");
        class.channel(id).expect("channel built").retry
    };
    let class = spec::build_spec::<ChainWorkflow>().expect("defaults register");
    let default = edge(&class, "in");
    assert_eq!((default.max_attempts, default.backoff_base_ms), (3, 50));

    let fail_fast = spec::build_spec::<RetryFailFastWorkflow>().expect("retry_max(1) registers");
    assert_eq!(edge(&fail_fast, "mid2").max_attempts, 1);

    struct NoAttempt;
    impl WorkflowOperative for NoAttempt {
        fn factory_id() -> &'static str {
            "test-retry-no-attempt"
        }
        fn channels(c: &ChannelBuilder) {
            retry_chain(c, 0, 10);
        }
    }
    let refused = spec::build_spec::<NoAttempt>().expect_err("an edge needs one attempt");
    assert!(refused.to_string().contains("RetryMaxZero"), "{refused}");
}

/// The stream's tier, checkpoint and correction settings are refused on a
/// workflow by the names the core gives them, never carried.
#[test]
fn a_stream_setting_on_a_workflow_is_refused_by_name() {
    struct Tier;
    impl WorkflowOperative for Tier {
        fn factory_id() -> &'static str {
            "test-refused-tier"
        }
        fn channels(c: &ChannelBuilder) {
            c.effectively_once();
            two_stage_chain(c);
        }
    }
    struct Checkpoint;
    impl WorkflowOperative for Checkpoint {
        fn factory_id() -> &'static str {
            "test-refused-checkpoint"
        }
        fn channels(c: &ChannelBuilder) {
            c.checkpoint_every(500);
            two_stage_chain(c);
        }
    }
    struct Correction;
    impl WorkflowOperative for Correction {
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
        Err(spec::BuildWorkflowError::EffectivelyOnceOnWorkflow { .. })
    ));
    assert!(matches!(
        spec::build_spec::<Checkpoint>(),
        Err(spec::BuildWorkflowError::CheckpointOnWorkflow { .. })
    ));
    let correction = spec::build_spec::<Correction>().expect_err("a correction window is refused");
    assert!(matches!(
        correction,
        spec::BuildWorkflowError::CorrectionWindowOnWorkflow { .. }
    ));
    assert!(
        correction
            .to_string()
            .starts_with("CorrectionWindowOnWorkflow"),
        "the author reads the name: {correction}"
    );
}

/// The per-class synchronous-durability declaration reaches the spec the controller's `AddOperative` response is
/// built from. Off by default — a class that says nothing never makes a
/// commit barrier wait on a remote store.
#[test]
fn sync_durability_is_declared_per_class_and_off_by_default() {
    let default = spec::build_spec::<ChainWorkflow>().expect("defaults register");
    assert!(
        !default
            .workflow
            .as_ref()
            .expect("a workflow class")
            .sync_durability
    );

    struct SyncDurable;
    impl WorkflowOperative for SyncDurable {
        const SYNC_DURABILITY: bool = true;
        fn factory_id() -> &'static str {
            "test-sync-durable"
        }
        fn channels(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
    }

    let class = spec::build_spec::<SyncDurable>().expect("a declaring class registers");
    let spec = class.workflow.as_ref().expect("a workflow class");
    assert!(spec.sync_durability);
}

/// `workflow-branching` registration validation. A DAG is accepted (with its
/// nodes in Kahn topological order, every producer before its consumers —
/// the invariant that makes completion order topological and its reverse a
/// valid compensation order); a cycle is refused, naming a node on it.
#[test]
fn dag_topology_is_validated() {
    // A→(B,C): one channel, two consumers. The edge is (channel, consumer),
    // so both branches read what Stage1 released on `mid`.
    let class = spec::build_spec::<TreeWorkflow>().expect("a fan-out tree registers");
    let spec = class.workflow.as_ref().expect("a workflow class");
    let topology = &class;
    let names: Vec<&str> = spec
        .dag
        .order
        .iter()
        .map(|&id| topology.tasklet(id).unwrap().name.as_ref())
        .collect();
    assert_eq!(
        names,
        vec!["workflow_stage1", "workflow_stage2", "workflow_echo_op"],
        "Kahn order: the root, then its children (= the former BFS numbering)"
    );
    assert_eq!(spec.dag.parents, vec![vec![], vec![0], vec![0]]);
    assert_eq!(spec.dag.children, vec![vec![1, 2], vec![], vec![]]);
    assert_eq!(spec.dag.leaves().collect::<Vec<_>>(), vec![1, 2]);
    assert!(
        spec.dag.chains[0].single_leaf(&spec.dag).is_none(),
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
    let linear = spec::build_spec::<ChainWorkflow>().expect("linear still registers");
    let linear = linear.workflow.as_ref().expect("a workflow class");
    assert_eq!(linear.dag.children, vec![vec![1], vec![]]);
    assert_eq!(linear.dag.chains[0].single_leaf(&linear.dag), Some(1));

    // A join — two streams feeding one node — registers, and Kahn numbers
    // BOTH producers before it (BFS would have enqueued the join on its
    // first producer).
    match spec::build_spec::<JoinWorkflow>() {
        Ok(class) => {
            let topology = &class;
            let spec = class.workflow.as_ref().expect("a workflow class");
            let names: Vec<&str> = spec
                .dag
                .order
                .iter()
                .map(|&id| topology.tasklet(id).unwrap().name.as_ref())
                .collect();
            assert_eq!(
                names,
                vec![
                    "workflow_stage1",
                    "workflow_stage2",
                    "workflow_echo_op",
                    "workflow_join_op"
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
    impl WorkflowOperative for Cyclic {
        fn factory_id() -> &'static str {
            "test-cyclic-workflow"
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
    match spec::build_spec::<Cyclic>().expect_err("a cycle is refused") {
        spec::BuildWorkflowError::Topology { detail, .. } => {
            assert!(
                detail.contains("Cycle") || detail.contains("cycle"),
                "the refusal identifies the cycle: {detail}"
            );
        }
        other => panic!("expected the shared cycle refusal, got {other:?}"),
    }

    // `multi-input-tasklets`: every message carries the channel it arrived
    // on, so an entry channel is a merge source on a workflow as on a
    // stream, and each entry channel roots its own chain over the merged
    // tasklet.
    struct EntryMergeWorkflow;
    impl WorkflowOperative for EntryMergeWorkflow {
        fn factory_id() -> &'static str {
            "test-entry-merge-workflow"
        }
        fn channels(c: &ChannelBuilder) {
            c.merge::<MergedJoinInput>()
                .from(c.input::<String>("in"), MergedJoinInput::B)
                .from(c.input::<String>("also"), MergedJoinInput::C)
                .into::<MergeJoinTask>();
        }
    }
    let entry_merge = spec::build_spec::<EntryMergeWorkflow>()
        .expect("a workflow merge over two entry channels registers");
    let entry_merge = entry_merge.workflow.as_ref().expect("a workflow class");
    assert_eq!(
        entry_merge.dag.len(),
        1,
        "the merged tasklet is the one node"
    );
    assert_eq!(
        entry_merge
            .dag
            .chains
            .iter()
            .map(|chain| (chain.channel_id, chain.roots.clone()))
            .collect::<Vec<_>>(),
        vec![(0, vec![0]), (1, vec![0])],
        "each entry channel roots its own chain at the merged tasklet"
    );

    // The inter-task form of the same merge is the supported one and still
    // registers, with the merged node as a two-parent inner node.
    let merged = spec::build_spec::<MergeWorkflow>().expect("an inter-task merge registers");
    let merged = merged.workflow.as_ref().expect("a workflow class");
    assert_eq!(
        merged.dag.parents[3],
        vec![1, 2],
        "the merged join has both producers as parents"
    );

    // A handoff-chained loop over a branching tree has no single terminal
    // batch to seed iteration N+1 from.
    struct BranchingChained;
    impl WorkflowOperative for BranchingChained {
        fn factory_id() -> &'static str {
            "test-branching-chained-workflow"
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
        spec::BuildWorkflowError::BranchingChainedLoop { leaves, .. } => {
            assert_eq!(leaves, 2);
        }
        other => panic!("expected BranchingChainedLoop, got {other:?}"),
    }
}

#[test]
fn dynamic_cell_tasklet_is_rejected() {
    #[derive(Default)]
    struct DynTask;
    impl Tasklet for DynTask {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_dyn_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _input: String,
            _state: StateHandle<'_, Self>,
            _ctx: &mut TaskletCtx<'_>,
        ) -> Result<(), TaskletError> {
            Ok(())
        }
    }
    struct DynWorkflow;
    impl WorkflowOperative for DynWorkflow {
        fn factory_id() -> &'static str {
            "test-dyn-workflow"
        }
        fn channels(c: &ChannelBuilder) {
            // The key rides an inter-tasklet edge: a keyed ENTRY edge is
            // refused earlier, by the workflow seed's own rule.
            c.input::<String>("in")
                .into::<EchoTask>()
                .key_by(|s: &String| s.clone())
                .into::<DynTask>();
        }
    }
    let err = spec::build_spec::<DynWorkflow>().expect_err("dynamic cells must be refused");
    assert!(
        matches!(err, spec::BuildWorkflowError::DynamicCells { .. }),
        "expected DynamicCells, got {err:?}"
    );
}
