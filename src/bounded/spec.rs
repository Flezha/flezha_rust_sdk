//! The typed half of bounded-spec construction: `build_spec::<T>()` runs the
//! author's `channels` against a `ChannelBuilder` and validates the chain.
//! The spec/DAG data structures live in `flz_core::bounded::spec`.

pub use flz_core::bounded::spec::*;
use flz_core::bounded::LoopMode;
use flz_core::operator::topology::OperatorTopology;

use crate::bounded::BoundedOperator;
use crate::channel::{ChannelBuilder, TaskletId};
use crate::operator::builder::build_topology_with_persistence;

/// Build and validate the spec for a bounded class: its `channels()`
/// declaration, validated as a static DAG.
pub fn build_spec<T: BoundedOperator>(
) -> Result<std::sync::Arc<OperatorTopology>, BuildBoundedError> {
    let factory_id = T::factory_id();

    // Self-termination guard: an operator whose only exit is an external stop,
    // with no duration backstop, can never terminate cooperatively.
    if !T::self_terminating() && T::max_duration().is_none() {
        return Err(BuildBoundedError::ExternalStopOnly { factory_id });
    }

    let cb = ChannelBuilder::new();
    T::channels(&cb);

    if !cb.has_tasklets() {
        return Err(BuildBoundedError::NoTasklets { factory_id });
    }
    refuse_unbounded_settings(factory_id, &cb)?;

    let topology =
        build_topology_with_persistence(factory_id, cb, T::PERSISTENCE_ENABLED, T::SLEEP_WHEN_IDLE)
            .map_err(|e| BuildBoundedError::Topology {
                factory_id,
                error: flz_core::host::render_issues(&e.errors),
            })?;
    let mut topology =
        std::sync::Arc::into_inner(topology).expect("a topology just built is shared with nobody");
    let dag = build_dag(factory_id, &topology)?;

    refuse_chained_loop_shape::<T>(factory_id, &topology, &dag)?;
    refuse_call_outside_per_input::<T>(factory_id, &topology)?;
    if T::PERSISTENCE_ENABLED {
        refuse_loop_terminal_without_handoff::<T>(factory_id, &topology, &dag)?;
    }

    topology.bounded = Some(BoundedSpec {
        dag,
        max_duration: T::max_duration(),
        rollback_mode: T::rollback_mode(),
        loop_mode: T::loop_mode(),
        sync_durability: T::SYNC_DURABILITY,
    });
    Ok(std::sync::Arc::new(topology))
}

/// Only a `:per_input` loop reads the later message a call's `Response` is.
fn refuse_call_outside_per_input<T: BoundedOperator>(
    factory_id: &'static str,
    topology: &OperatorTopology,
) -> Result<(), BuildBoundedError> {
    match topology.channels.iter().find(|c| c.call.is_some()) {
        Some(channel) if T::loop_mode() != LoopMode::PerInput => {
            Err(BuildBoundedError::CallOutsidePerInputLoop {
                factory_id,
                channel: channel.name.to_string(),
            })
        }
        _ => Ok(()),
    }
}

/// A handoff-chained loop re-seeds iteration N+1 from iteration N's terminal
/// batch, so "the terminal tasklet" must be unambiguous in every chain the
/// loop can enter: a chain with several leaves has no single terminal batch
/// to carry forward.
fn refuse_chained_loop_shape<T: BoundedOperator>(
    factory_id: &'static str,
    topology: &OperatorTopology,
    dag: &TaskDag,
) -> Result<(), BuildBoundedError> {
    if T::loop_mode() != LoopMode::Chained {
        return Ok(());
    }
    match dag.branching_chain() {
        Some((channel_id, branching)) => Err(BuildBoundedError::BranchingChainedLoop {
            factory_id,
            channel: topology
                .channel(channel_id)
                .map(|c| c.name.to_string())
                .unwrap_or_default(),
            leaves: branching.leaves(dag).count(),
        }),
        None => Ok(()),
    }
}

/// A durable chained loop seeds iteration N+1 from iteration N's terminal
/// handoff, so the terminal tasklet has to release one the journal carries.
/// Two shapes do not: a tasklet nothing is wired from and that names no sink
/// owns no output channel at all, and one whose only output is plugin-bound
/// gets no journal codec, by the rule `channel_descriptor` applies on both the
/// typed and the byte-passing path. Either way a crash mid-loop would resume
/// with an empty seed, which the loop reads as "converged" and stops.
/// Unpublished library: fail registration instead of shimming.
fn refuse_loop_terminal_without_handoff<T: BoundedOperator>(
    factory_id: &'static str,
    topology: &OperatorTopology,
    dag: &TaskDag,
) -> Result<(), BuildBoundedError> {
    if T::loop_mode() != LoopMode::Chained {
        return Ok(());
    }
    match dag.terminal_without_handoff(topology) {
        Some(terminal) => Err(BuildBoundedError::LoopTerminalWithoutHandoff {
            factory_id,
            tasklet: terminal.name.to_string(),
        }),
        None => Ok(()),
    }
}

/// A bounded operator commits at its stage boundary, so the unbounded operator's tier, checkpoint
/// and correction settings have nothing to rest on; the builder lets an author
/// write them, so they are refused here by the names the core gives them.
fn refuse_unbounded_settings(
    factory_id: &'static str,
    cb: &ChannelBuilder,
) -> Result<(), BuildBoundedError> {
    let settings = cb.class_settings();
    if settings.effectively_once {
        return Err(BuildBoundedError::EffectivelyOnceOnBounded { factory_id });
    }
    if settings.declares_checkpoint() {
        return Err(BuildBoundedError::CheckpointOnBounded { factory_id });
    }
    if settings.correction.is_some() {
        return Err(BuildBoundedError::CorrectionWindowOnBounded { factory_id });
    }
    Ok(())
}

/// Resolve the execution DAG of a structured bounded operator's tasklets and refuse
/// anything outside the shared graph-shape rule (`sdk-graph-authoring`): an
/// entry-rooted connected acyclic graph (several entry channels, fan-out and
/// fan-in all legal) with at most one output channel per tasklet.
///
/// Nodes come out in **Kahn topological order** from the roots, so every
/// producer's index precedes its consumers' — for a join, *all* of them. That
/// is the invariant the completion and compensation orders rest on (BFS, the
/// former walk, enqueued a join on its first producer and broke it).
fn build_dag(
    factory_id: &'static str,
    topology: &OperatorTopology,
) -> Result<TaskDag, BuildBoundedError> {
    refuse_multiple_outputs(factory_id, topology)?;
    let entries = topology.entry_consumers();
    if entries.is_empty() {
        return Err(BuildBoundedError::NoEntryChannel { factory_id });
    }
    let roots: Vec<TaskletId> = entries
        .iter()
        .flat_map(|(_, c)| c.iter().copied())
        .collect();

    // Reachability is named first, so a genuinely disconnected tasklet is
    // not reported as a cycle symptom; a leftover Kahn node is on a cycle.
    // Kahn pop-in-FIFO from the roots yields exactly the former BFS numbering
    // for a tree, so journaled `task_idx` values keep their meaning.
    let n = topology.tasklets.len();
    let edges: Vec<_> = topology
        .channels
        .iter()
        .filter_map(|c| c.producer.map(|p| (p, c)))
        .flat_map(|(p, c)| c.consumer_ids().map(move |id| (p, id)))
        .collect();
    let shape = graph_order(n, &edges, &roots, GraphSeed::EveryIndegreeZero);
    if let Some(node) = shape.unreachable.filter(|_| shape.on_cycle.is_none()) {
        return Err(BuildBoundedError::Disconnected {
            factory_id,
            tasklet: topology.tasklets[node as usize].name.to_string(),
        });
    }
    if let Some(node) = shape.on_cycle {
        return Err(BuildBoundedError::Cycle {
            factory_id,
            tasklet: topology.tasklets[node as usize].name.to_string(),
        });
    }

    Ok(TaskDag::from_walk(shape, entries))
}

/// A tasklet producing two channels is unroutable (the topology keeps one
/// `output_channel_id`, so the second would be silently orphaned).
/// Structurally impossible on the stream-handle builder; kept as
/// defense-in-depth.
fn refuse_multiple_outputs(
    factory_id: &'static str,
    topology: &OperatorTopology,
) -> Result<(), BuildBoundedError> {
    for (idx, task) in topology.tasklets.iter().enumerate() {
        let task_id = idx as TaskletId;
        let outgoing = topology
            .channels
            .iter()
            .filter(|c| c.producer == Some(task_id))
            .count();
        if outgoing > 1 {
            return Err(BuildBoundedError::MultipleOutputChannels {
                factory_id,
                tasklet: task.name.to_string(),
            });
        }
    }
    Ok(())
}
