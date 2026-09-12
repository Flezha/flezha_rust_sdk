//! Registration, spec resolution and authoring refusals: everything a
//! workflow class is checked for before a driver ever runs.

use std::sync::Arc;
use std::time::Duration;

use super::fixtures::*;
use crate::channel::{ChannelBuilder, ChannelKind};
use crate::entity::restore::RestoreInitPayload;
use crate::operative::registry::{Operative, OperativeRegistry};
use crate::procedure::change::NoChange;
use crate::procedure::clock::WallClock;
use crate::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
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
        fn run(_c: &mut WorkflowCtx, _a: &str) -> Result<(), WorkflowError> {
            Ok(())
        }
    }
    assert!(registry::get_class("test-getclass-workflow").is_none());
    crate::register_workflow_class::<RegWorkflow>().unwrap();
    assert!(registry::get_class("test-getclass-workflow").is_some());
    assert!(registry::get_class("test-nonexistent-workflow").is_none());
}

#[test]
fn registry_holds_workflow_as_sealed_variant() {
    let reg = OperativeRegistry::new();
    let (instance, _inbox_rx) = crate::workflow::instance::WorkflowInstance::new(
        Arc::from("t1"),
        Arc::from("t1-factory"),
        Vec::new(),
        0,
        0,
        None,
    );
    reg.register_workflow(instance);

    assert!(reg.contains("t1"));
    // A workflow is not a operative: the operative-only accessor returns None.
    assert!(reg.get_stream("t1").is_none());
    match reg.get_operative("t1").as_deref() {
        Some(Operative::Workflow(inst)) => assert_eq!(&*inst.instance_id, "t1"),
        _ => panic!("expected a WorkflowOperative variant"),
    }

    assert!(matches!(
        reg.remove("t1").as_deref(),
        Some(Operative::Workflow(_))
    ));
    assert!(!reg.contains("t1"));
}

#[test]
fn run_sugar_spec_has_no_topology() {
    let spec = spec::build_spec::<EmitWorkflow>().expect("run-sugar spec builds");
    assert!(
        spec.topology.is_none(),
        "bare run() ⇒ run-sugar (no topology)"
    );
    assert!(spec.dag.order.is_empty());
}

#[test]
fn structured_spec_resolves_linear_chain() {
    let spec = spec::build_spec::<ChainWorkflow>().expect("structured spec builds");
    let topology = spec.topology.as_ref().expect("structured ⇒ topology");
    // Chain is ordered entry-consumer → terminal: Stage1 then Stage2.
    let names: Vec<&str> = spec
        .dag
        .order
        .iter()
        .map(|&id| topology.procedure(id).unwrap().name.as_ref())
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
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .into::<Stage2>();
        }
    }
    let spec = spec::build_spec::<NoCodec>().expect("the runtime derives the handoff codec");
    let mid = spec
        .topology
        .as_ref()
        .expect("a structured class has a topology")
        .channels
        .iter()
        .find(|c| &*c.name == "mid")
        .expect("the declared handoff channel exists");
    assert_eq!(mid.kind, ChannelKind::InterProcedure);

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

/// `workflow-durable-loop` registration validation: a loop needs a chain to
/// iterate, and a durable handoff-chained loop needs a terminal procedure whose
/// output channel the journal carries — otherwise a crash mid-loop would
/// re-seed iteration N+1 with an empty batch and silently read as "converged".
/// Two shapes fail it: no output channel, and a plugin-bound one.
#[test]
fn loop_authoring_is_validated() {
    match spec::build_spec::<LoopWithoutChain>().expect_err("run-sugar cannot loop a chain") {
        spec::BuildWorkflowError::LoopWithoutChain { .. } => {}
        other => panic!("expected LoopWithoutChain, got {other:?}"),
    }

    for shape in ["no channel at all", "a plugin-bound channel"] {
        let err = match shape {
            "no channel at all" => spec::build_spec::<ChainedLoopNoTerminalOutput>(),
            _ => spec::build_spec::<ChainedLoopPluginTerminal>(),
        }
        .expect_err("a chained loop's terminal procedure must release a handoff");
        match err {
            spec::BuildWorkflowError::LoopTerminalWithoutHandoff { procedure, .. } => {
                assert_eq!(
                    procedure, "workflow_stage2",
                    "the error names the terminal procedure ({shape})"
                );
            }
            other => panic!("expected LoopTerminalWithoutHandoff for {shape}, got {other:?}"),
        }
    }

    // An inbox-driven loop over the same topology is fine: it re-seeds each
    // iteration from its inbox, never from the terminal handoff.
    let spec = spec::build_spec::<ChatLoop>().expect("PerInput loop registers");
    assert_eq!(spec.loop_mode, LoopMode::PerInput);
    assert_eq!(spec.dag.len(), 2);
}

/// `workflow-entity-retry` policy surface: the defaults land on the shared
/// `RetryPolicy`, `retry_attempts() = 0` collapses to the one-attempt
/// fail-fast, and a ladder without a backoff is refused at registration.
#[test]
fn retry_policy_is_resolved_and_validated() {
    let spec = spec::build_spec::<ChainWorkflow>().expect("defaults register");
    assert_eq!(spec.retry.max_attempts, 3, "platform default is 3 attempts");
    assert_eq!(spec.retry.backoff_base_ms, 500);

    let fail_fast = spec::build_spec::<RetryFailFastWorkflow>().expect("attempts = 0 registers");
    assert_eq!(
        fail_fast.retry.max_attempts, 1,
        "attempts = 0 ⇒ the single fail-fast attempt"
    );

    struct HotLoop;
    impl WorkflowOperative for HotLoop {
        fn factory_id() -> &'static str {
            "test-retry-hot-loop"
        }
        fn retry_backoff() -> Duration {
            Duration::ZERO
        }
        fn procedures(c: &ChannelBuilder) {
            retry_chain(c);
        }
    }
    match spec::build_spec::<HotLoop>().expect_err("a retry ladder needs a backoff") {
        spec::BuildWorkflowError::ZeroRetryBackoff { attempts, .. } => assert_eq!(attempts, 3),
        other => panic!("expected ZeroRetryBackoff, got {other:?}"),
    }
}

/// The per-class synchronous-durability declaration reaches the spec the controller's `AddOperative` response is
/// built from. Off by default — a class that says nothing never makes a
/// commit barrier wait on a remote store.
#[test]
fn sync_durability_is_declared_per_class_and_off_by_default() {
    let default = spec::build_spec::<ChainWorkflow>().expect("defaults register");
    assert!(!default.sync_durability);

    struct SyncDurable;
    impl WorkflowOperative for SyncDurable {
        const SYNC_DURABILITY: bool = true;
        fn factory_id() -> &'static str {
            "test-sync-durable"
        }
        fn procedures(c: &ChannelBuilder) {
            retry_chain(c);
        }
    }

    let spec = spec::build_spec::<SyncDurable>().expect("a declaring class registers");
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
    let spec = spec::build_spec::<TreeWorkflow>().expect("a fan-out tree registers");
    let topology = spec.topology.as_ref().expect("structured ⇒ topology");
    let names: Vec<&str> = spec
        .dag
        .order
        .iter()
        .map(|&id| topology.procedure(id).unwrap().name.as_ref())
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
        spec.dag.single_leaf().is_none(),
        "two leaves ⇒ no single terminal procedure"
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
    assert_eq!(linear.dag.children, vec![vec![1], vec![]]);
    assert_eq!(linear.dag.single_leaf(), Some(1));

    // A join — two streams feeding one node — registers, and Kahn numbers
    // BOTH producers before it (BFS would have enqueued the join on its
    // first producer).
    match spec::build_spec::<JoinWorkflow>() {
        Ok(spec) => {
            let topology = spec.topology.as_ref().expect("structured ⇒ topology");
            let names: Vec<&str> = spec
                .dag
                .order
                .iter()
                .map(|&id| topology.procedure(id).unwrap().name.as_ref())
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
        fn procedures(c: &ChannelBuilder) {
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

    // `multi-input-procedures`: a workflow seed is a bare payload naming no
    // channel, so an entry channel cannot be one merge source among
    // several — the mapping could not be selected and every entry channel
    // past the first would be unreadable. Refused by name, at definition
    // time, instead of accepted and failing every invocation on a type
    // mismatch.
    struct EntryMergeWorkflow;
    impl WorkflowOperative for EntryMergeWorkflow {
        fn factory_id() -> &'static str {
            "test-entry-merge-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            c.merge::<MergedJoinInput>()
                .from(c.input::<String>("in"), MergedJoinInput::B)
                .from(c.input::<String>("also"), MergedJoinInput::C)
                .into::<MergeJoinProc>();
        }
    }
    match spec::build_spec::<EntryMergeWorkflow>()
        .expect_err("a workflow merge over an entry channel is refused")
    {
        spec::BuildWorkflowError::MergeFromEntryChannel {
            channel, procedure, ..
        } => {
            assert_eq!(channel, "in");
            assert_eq!(procedure, "workflow_merge_join_op");
        }
        other => panic!("expected MergeFromEntryChannel, got {other:?}"),
    }

    // The inter-proc form of the same merge is the supported one and still
    // registers, with the merged node as a two-parent inner node.
    let merged = spec::build_spec::<MergeWorkflow>().expect("an inter-proc merge registers");
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
        fn procedures(c: &ChannelBuilder) {
            tree_chain(c);
        }
    }
    match spec::build_spec::<BranchingChained>()
        .expect_err("a chained loop needs one terminal procedure")
    {
        spec::BuildWorkflowError::BranchingChainedLoop { leaves, .. } => {
            assert_eq!(leaves, 2);
        }
        other => panic!("expected BranchingChainedLoop, got {other:?}"),
    }
}

#[test]
fn dynamic_entity_procedure_is_rejected() {
    #[derive(Default)]
    struct DynProc;
    impl Procedure for DynProc {
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
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Ok(())
        }
    }
    struct DynWorkflow;
    impl WorkflowOperative for DynWorkflow {
        fn factory_id() -> &'static str {
            "test-dyn-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            // The key rides an inter-procedure edge: a keyed ENTRY edge is
            // refused earlier, by the workflow seed's own rule.
            c.input::<String>("in")
                .into::<EchoProc>()
                .key_by(|s: &String| s.clone())
                .into::<DynProc>();
        }
    }
    let err = spec::build_spec::<DynWorkflow>().expect_err("dynamic entities must be refused");
    assert!(
        matches!(err, spec::BuildWorkflowError::DynamicEntities { .. }),
        "expected DynamicEntities, got {err:?}"
    );
}
