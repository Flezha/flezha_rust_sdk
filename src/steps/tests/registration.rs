//! Registration, spec resolution and authoring refusals: everything a
//! steps class is checked for before an instance ever runs.

use std::sync::Arc;

use super::fixtures::*;
use crate::channel::{ChannelBuilder, ChannelKind};
use crate::relay::registry::RelayRegistry;
use crate::steps::*;
use crate::tasklet::{Ctx, Tasklet, TaskletError};
use crate::{Durable, ErrorModel, KeepFinished, Relay, RelaySettings, Run, SleepAfter};

#[test]
fn steps_error_conversions() {
    // String conversions stay retryable-by-default: the ladder is opt-out.
    let from_str: StepsError = "x".into();
    assert_eq!(from_str.to_string(), "x");
    assert!(!from_str.non_retryable);
    let from_string: StepsError = String::from("y").into();
    assert_eq!(from_string.message, "y");
    assert!(!from_string.non_retryable);

    let permanent = StepsError::non_retryable("z");
    assert_eq!(permanent.to_string(), "z");
    assert!(permanent.non_retryable);
}

#[test]
fn register_steps_makes_factory_resolvable() {
    struct RegSteps;
    impl Relay for RegSteps {
        fn factory_id() -> &'static str {
            "test-getclass-steps"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<EchoTask>();
        }

        fn settings() -> RelaySettings {
            RelaySettings {
                run: Run::Steps {
                    each: Each::Once,
                    on_failure: OnFailure::UndoAll,
                },
                ..RelaySettings::default()
            }
        }
    }
    let get_class = crate::relay::registry::get_class;
    assert!(get_class("test-getclass-steps").is_none());
    crate::register_class::<RegSteps>().unwrap();
    let class = get_class("test-getclass-steps").expect("registered");
    assert!(class.steps.is_some(), "the class carries its kind");
    assert!(get_class("test-nonexistent-steps").is_none());
}

/// One registry holds both kinds: a steps instance is a relay
/// instance that carries a run.
#[test]
fn the_one_registry_holds_a_steps_instance() {
    let reg = RelayRegistry::new();
    let class = spec::build_spec::<EmitSteps>().expect("the fixture registers");
    reg.register(crate::relay::instance::RelayInstance::new(
        class,
        Arc::from("t1"),
    ));

    assert!(reg.contains("t1"));
    let instance = reg.get("t1").expect("registered");
    assert_eq!(&*instance.instance_id, "t1");
    assert!(instance.steps.is_some(), "a steps instance carries a run");
    assert!(reg.remove("t1").is_some());
    assert!(!reg.contains("t1"));
}

#[test]
fn a_steps_without_tasklets_is_refused() {
    let err = spec::build_spec::<NoTaskletsSteps>().expect_err("a steps always runs tasklets");
    assert_eq!(err.code(), "topology.no_tasklets");
}

#[test]
fn structured_spec_resolves_linear_chain() {
    let class = spec::build_spec::<ChainSteps>().expect("structured spec builds");
    let spec = class.steps.as_ref().expect("a steps class");
    let topology = &class;
    // Chain is ordered entry-consumer → terminal: Stage1 then Stage2.
    let names: Vec<&str> = spec
        .dag
        .order
        .iter()
        .map(|&id| topology.tasklet(id).unwrap().name.as_ref())
        .collect();
    assert_eq!(names, vec!["steps_stage1", "steps_stage2"]);
}

/// A steps class that has no way to end is ordinary: it runs until it is
/// stopped from outside.
#[test]
fn a_steps_class_that_never_ends_is_accepted() {
    let class = spec::build_spec::<NeverEndingSteps>().expect("an endless loop registers");
    let spec = class.steps.as_ref().expect("a steps class");
    assert_eq!(spec.each, Each::PerInput);
    assert_eq!(class.max_duration, None);
}

#[test]
fn a_durable_handoff_is_journalable_with_nothing_declared() {
    struct NoCodec;
    impl Relay for NoCodec {
        fn factory_id() -> &'static str {
            "test-no-codec-steps"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .into::<Stage2>();
        }

        fn settings() -> RelaySettings {
            RelaySettings {
                run: Run::Steps {
                    each: Each::Once,
                    on_failure: OnFailure::UndoAll,
                },
                ..RelaySettings::default()
            }
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

/// `steps-durable-loop` registration validation: a durable handoff-chained
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
        let refusal = err.to_error();
        assert_eq!(
            refusal.code,
            "topology.chained_loop_terminal_without_handoff"
        );
        assert_eq!(
            refusal.details["tasklet"], "steps_stage2",
            "the error names the terminal tasklet ({shape})"
        );
    }

    // An inbox-driven loop over the same topology is fine: it re-seeds each
    // iteration from its inbox, never from the terminal handoff.
    let class = spec::build_spec::<ChatLoop>().expect("PerInput loop registers");
    let spec = class.steps.as_ref().expect("a steps class");
    assert_eq!(spec.each, Each::PerInput);
    assert_eq!(spec.dag.len(), 2);
}

/// `steps-cell-retry` policy surface: a stage's ladder is the edge's, as a
/// stream's is. The default edge is 3 attempts from 50 ms, `retry_max(1)` is
/// fail-fast, and `retry_max(0)` is the shared topology refusal.
#[test]
fn the_retry_policy_is_the_edges() {
    let edge = |class: &crate::relay::topology::RelayTopology, name: &str| {
        let id = class.lookup_channel_id(name).expect("channel declared");
        class.channel(id).expect("channel built").retry
    };
    let class = spec::build_spec::<ChainSteps>().expect("defaults register");
    let default = edge(&class, "in");
    assert_eq!((default.max_attempts, default.backoff_base_ms), (3, 50));

    let fail_fast = spec::build_spec::<RetryFailFastSteps>().expect("retry_max(1) registers");
    assert_eq!(edge(&fail_fast, "mid2").max_attempts, 1);

    struct NoAttempt;
    impl Relay for NoAttempt {
        fn factory_id() -> &'static str {
            "test-retry-no-attempt"
        }
        fn channels(c: &ChannelBuilder) {
            retry_chain(c, 0, 10);
        }

        fn settings() -> RelaySettings {
            RelaySettings {
                run: Run::Steps {
                    each: Each::Once,
                    on_failure: OnFailure::UndoAll,
                },
                ..RelaySettings::default()
            }
        }
    }
    let refused = spec::build_spec::<NoAttempt>().expect_err("an edge needs one attempt");
    assert_eq!(refused.code(), "topology.retry_max_zero", "{refused}");
}

/// A sleeping instance is evicted and restored from its durable state, so a
/// class that keeps none is refused by name, whichever kind it is; a steps relay
/// sleeps by default exactly when it persists.
/// The sleep interval is the class's own, whichever way it runs: by default
/// six hours for a steps class and never for a flow class, off with
/// `SleepAfter::Never`, and nothing to declare for a class kept in memory.
#[test]
fn the_sleep_interval_is_a_durable_setting_of_either_run() {
    struct Quick;
    impl Relay for Quick {
        fn factory_id() -> &'static str {
            "test-sleep-quick"
        }
        fn channels(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
        fn settings() -> RelaySettings {
            RelaySettings {
                run: Run::steps(Each::Once),
                durable: Durable::Local {
                    sleep_after: SleepAfter::After(std::time::Duration::from_secs(2)),
                    keep_finished: KeepFinished::Nothing,
                },
                ..RelaySettings::default()
            }
        }
    }
    struct InMemory;
    impl Relay for InMemory {
        fn factory_id() -> &'static str {
            "test-sleep-in-memory"
        }
        fn channels(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
        fn settings() -> RelaySettings {
            RelaySettings {
                run: Run::steps(Each::Once),
                durable: Durable::Memory,
                ..RelaySettings::default()
            }
        }
    }

    let quick = spec::build_spec::<Quick>().expect("registers");
    assert_eq!(quick.sleep_after(), Some(std::time::Duration::from_secs(2)));
    let in_memory = spec::build_spec::<InMemory>().expect("registers");
    assert_eq!(in_memory.sleep_after(), None);
    let default = spec::build_spec::<NeverEndingSteps>().expect("registers");
    assert_eq!(
        default.sleep_after(),
        Some(crate::relay::DEFAULT_SLEEP_AFTER)
    );
    struct FlowDefault;
    impl Relay for FlowDefault {
        fn factory_id() -> &'static str {
            "test-sleep-flow-default"
        }
        fn channels(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
    }
    let flow = crate::relay::builder::build_topology_for::<FlowDefault>().expect("registers");
    assert_eq!(
        flow.sleep_after(),
        None,
        "a flow class that says nothing never sleeps"
    );
}

/// `durable: remote` reaches the class the controller's `AddRelay`
/// response is built from; a class that says nothing keeps its work locally,
/// so no commit barrier waits on a remote store.
#[test]
fn remote_durability_is_declared_per_class_and_off_by_default() {
    let default = spec::build_spec::<ChainSteps>().expect("defaults register");
    assert!(!default.durable.is_remote());

    struct Remote;
    impl Relay for Remote {
        fn factory_id() -> &'static str {
            "test-sync-durable"
        }
        fn channels(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
        fn settings() -> RelaySettings {
            RelaySettings {
                run: Run::steps(Each::Once),
                durable: Durable::remote(),
                ..RelaySettings::default()
            }
        }
    }

    let class = spec::build_spec::<Remote>().expect("a declaring class registers");
    assert!(class.durable.is_remote());
}

/// A flow class keeps its work remote as a steps class does.
#[test]
fn a_flow_class_may_declare_remote() {
    struct RemoteFlow;
    impl Relay for RemoteFlow {
        fn factory_id() -> &'static str {
            "test-remote-flow"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<EchoTask>();
        }
        fn settings() -> RelaySettings {
            RelaySettings {
                durable: Durable::remote(),
                ..RelaySettings::default()
            }
        }
    }
    let topology = crate::relay::builder::build_topology_for::<RemoteFlow>()
        .expect("a remote flow class registers");
    assert!(topology.durable.is_remote());
}

/// `steps-branching` registration validation. A DAG is accepted (with its
/// nodes in Kahn topological order, every producer before its consumers —
/// the invariant that makes completion order topological and its reverse a
/// valid compensation order); a cycle is refused, naming a node on it.
#[test]
fn dag_topology_is_validated() {
    // A→(B,C): one channel, two consumers. The edge is (channel, consumer),
    // so both branches read what Stage1 released on `mid`.
    let class = spec::build_spec::<TreeSteps>().expect("a fan-out tree registers");
    let spec = class.steps.as_ref().expect("a steps class");
    let topology = &class;
    let names: Vec<&str> = spec
        .dag
        .order
        .iter()
        .map(|&id| topology.tasklet(id).unwrap().name.as_ref())
        .collect();
    assert_eq!(
        names,
        vec!["steps_stage1", "steps_stage2", "steps_echo_op"],
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
    let linear = spec::build_spec::<ChainSteps>().expect("linear still registers");
    let linear = linear.steps.as_ref().expect("a steps class");
    assert_eq!(linear.dag.children, vec![vec![1], vec![]]);
    assert_eq!(linear.dag.entries[0].1.single_leaf(&linear.dag), Some(1));

    // A join — two streams feeding one node — registers, and Kahn numbers
    // BOTH producers before it (BFS would have enqueued the join on its
    // first producer).
    match spec::build_spec::<JoinSteps>() {
        Ok(class) => {
            let topology = &class;
            let spec = class.steps.as_ref().expect("a steps class");
            let names: Vec<&str> = spec
                .dag
                .order
                .iter()
                .map(|&id| topology.tasklet(id).unwrap().name.as_ref())
                .collect();
            assert_eq!(
                names,
                vec![
                    "steps_stage1",
                    "steps_stage2",
                    "steps_echo_op",
                    "steps_join_op"
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
    impl Relay for Cyclic {
        fn factory_id() -> &'static str {
            "test-cyclic-steps"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .into::<Stage2>()
                .named("back")
                .into::<Stage1>();
        }

        fn settings() -> RelaySettings {
            RelaySettings {
                run: Run::Steps {
                    each: Each::Once,
                    on_failure: OnFailure::UndoAll,
                },
                ..RelaySettings::default()
            }
        }
    }
    let refused = spec::build_spec::<Cyclic>().expect_err("a cycle is refused");
    assert_eq!(refused.code(), "topology.cycle");

    // `multi-input-tasklets`: every message carries the channel it arrived
    // on, so an entry channel is a merge source on a steps relay as on a
    // stream, and each entry channel roots its own chain over the merged
    // tasklet.
    struct EntryMergeSteps;
    impl Relay for EntryMergeSteps {
        fn factory_id() -> &'static str {
            "test-entry-merge-steps"
        }
        fn channels(c: &ChannelBuilder) {
            c.merge::<MergedJoinInput>()
                .from(c.input::<String>("in"), MergedJoinInput::B)
                .from(c.input::<String>("also"), MergedJoinInput::C)
                .into::<MergeJoinTask>();
        }

        fn settings() -> RelaySettings {
            RelaySettings {
                run: Run::Steps {
                    each: Each::Once,
                    on_failure: OnFailure::UndoAll,
                },
                ..RelaySettings::default()
            }
        }
    }
    let entry_merge = spec::build_spec::<EntryMergeSteps>()
        .expect("a steps merge over two entry channels registers");
    let entry_merge = entry_merge.steps.as_ref().expect("a steps class");
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
    let merged = spec::build_spec::<MergeSteps>().expect("an inter-task merge registers");
    let merged = merged.steps.as_ref().expect("a steps class");
    assert_eq!(
        merged.dag.parents[3],
        vec![1, 2],
        "the merged join has both producers as parents"
    );

    // A handoff-chained loop over a branching tree has no single terminal
    // batch to seed iteration N+1 from.
    struct BranchingChained;
    impl Relay for BranchingChained {
        fn factory_id() -> &'static str {
            "test-branching-chained-steps"
        }
        fn channels(c: &ChannelBuilder) {
            tree_chain(c);
        }

        fn settings() -> RelaySettings {
            RelaySettings {
                run: Run::Steps {
                    each: Each::Chained,
                    on_failure: OnFailure::UndoAll,
                },
                ..RelaySettings::default()
            }
        }
    }
    let refusal = spec::build_spec::<BranchingChained>()
        .expect_err("a chained loop needs one terminal tasklet")
        .to_error();
    assert_eq!(refusal.code, "topology.branching_chained_loop");
    assert_eq!(
        (&refusal.details["channel"], &refusal.details["leaves"]),
        (&"in".into(), &2.into())
    );
}

#[test]
fn a_keyed_edge_makes_a_steps_tasklet_dynamic() {
    #[derive(Default)]
    struct DynTask;
    #[flezha_runner::tasklet(name = "steps_dyn_op")]
    impl Tasklet for DynTask {
        type Input = String;
        type Output = String;
        fn run(&mut self, _input: String, _ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
            Ok(())
        }
    }
    struct DynSteps;
    impl Relay for DynSteps {
        fn factory_id() -> &'static str {
            "test-dyn-steps"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EchoTask>()
                .key_by(|s: &String| s.clone())
                .into::<DynTask>();
        }

        fn settings() -> RelaySettings {
            RelaySettings {
                run: Run::Steps {
                    each: Each::Once,
                    on_failure: OnFailure::UndoAll,
                },
                ..RelaySettings::default()
            }
        }
    }
    let topology = spec::build_spec::<DynSteps>().expect("a keyed edge registers");
    let keyed = topology.tasklet_name_to_id["steps_dyn_op"];
    assert_eq!(
        topology.tasklets[keyed as usize].mode,
        flz_core::relay::topology::CellMode::Dynamic
    );
    let instance = flz_core::relay::instance::RelayInstance::new(topology, "dyn-steps-1".into());
    assert!(
        instance.cells[keyed as usize].is_empty(),
        "no Cell before a pass"
    );
    assert!(instance.dynamic_registries[keyed as usize].is_some());
}
