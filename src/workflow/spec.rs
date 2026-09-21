//! The typed half of workflow-spec construction: `build_spec::<T>()` runs the
//! author's `tasklets` against a `ChannelBuilder` and validates the chain.
//! The spec/DAG data structures live in `flz_core::workflow::spec`.

use flz_core::operative::topology::{CellMode, OperativeTopology};
pub use flz_core::workflow::spec::*;
use flz_core::workflow::LoopMode;

use crate::channel::{ChannelBuilder, ChannelKind, RetryPolicy, TaskletId};
use crate::operative::builder::build_topology;
use crate::workflow::WorkflowOperative;

/// Build and validate the spec for a workflow class: its `tasklets()`
/// declaration, validated as a static DAG.
pub fn build_spec<T: WorkflowOperative>() -> Result<WorkflowSpec, BuildWorkflowError> {
    let factory_id = T::factory_id();

    // Self-termination guard: an operative whose only exit is an external stop,
    // with no duration backstop, can never terminate cooperatively.
    if !T::self_terminating() && T::max_duration().is_none() {
        return Err(BuildWorkflowError::ExternalStopOnly { factory_id });
    }

    let retry = retry_policy::<T>(factory_id)?;

    let cb = ChannelBuilder::new();
    T::tasklets(&cb);

    if !cb.has_tasklets() {
        return Err(BuildWorkflowError::NoTasklets { factory_id });
    }

    let topology = build_topology(factory_id, cb).map_err(|e| BuildWorkflowError::Topology {
        factory_id,
        detail: format!("{e}"),
    })?;
    let dag = build_dag(factory_id, &topology)?;

    refuse_chained_loop_shape::<T>(factory_id, &dag)?;
    if T::PERSISTENCE_ENABLED {
        refuse_loop_terminal_without_handoff::<T>(factory_id, &topology, &dag)?;
    }

    Ok(WorkflowSpec {
        topology,
        dag,
        max_duration: T::max_duration(),
        rollback_mode: T::rollback_mode(),
        loop_mode: T::loop_mode(),
        wake_after: T::wake_after(),
        retry,
        barrier_retry_attempts: T::barrier_retry_attempts(),
        persistence_enabled: T::PERSISTENCE_ENABLED,
        sync_durability: T::SYNC_DURABILITY,
    })
}

/// A handoff-chained loop re-seeds iteration N+1 from iteration N's terminal
/// batch, so "the terminal tasklet" must be unambiguous — a graph with
/// several leaves has no single terminal batch to carry forward.
fn refuse_chained_loop_shape<T: WorkflowOperative>(
    factory_id: &'static str,
    dag: &TaskDag,
) -> Result<(), BuildWorkflowError> {
    if T::loop_mode() == LoopMode::Chained && dag.single_leaf().is_none() {
        return Err(BuildWorkflowError::BranchingChainedLoop {
            factory_id,
            leaves: dag.leaves().count(),
        });
    }
    Ok(())
}

/// A durable chained loop seeds iteration N+1 from iteration N's terminal
/// handoff, so the terminal tasklet has to release one the journal carries.
/// Two shapes do not: a tasklet nothing is wired from and that names no sink
/// owns no output channel at all, and one whose only output is plugin-bound
/// gets no journal codec, by the rule `channel_descriptor` applies on both the
/// typed and the byte-passing path. Either way a crash mid-loop would resume
/// with an empty seed, which the loop reads as "converged" and stops.
/// Unpublished library: fail registration instead of shimming.
fn refuse_loop_terminal_without_handoff<T: WorkflowOperative>(
    factory_id: &'static str,
    topology: &OperativeTopology,
    dag: &TaskDag,
) -> Result<(), BuildWorkflowError> {
    if T::loop_mode() != LoopMode::Chained {
        return Ok(());
    }
    let terminal = dag
        .single_leaf()
        .and_then(|leaf| topology.tasklet(dag.order[leaf]))
        .expect("a chained loop has a single leaf (checked above)");
    let carries_handoff = terminal
        .output_channel_id
        .and_then(|cid| topology.channel(cid))
        .is_some_and(|ch| ch.kind == ChannelKind::InterTasklet);
    if !carries_handoff {
        return Err(BuildWorkflowError::LoopTerminalWithoutHandoff {
            factory_id,
            tasklet: terminal.name.to_string(),
        });
    }
    Ok(())
}

/// Resolve the class's per-cell retry ladder onto the shared
/// [`RetryPolicy`], refusing a ladder that would spin without a backoff. A
/// declared 0 collapses to the one-attempt fail-fast the driver runs.
fn retry_policy<T: WorkflowOperative>(
    factory_id: &'static str,
) -> Result<RetryPolicy, BuildWorkflowError> {
    let attempts = T::retry_attempts();
    let backoff_base_ms = T::retry_backoff().as_millis().min(u64::MAX as u128) as u64;
    if attempts > 1 && backoff_base_ms == 0 {
        return Err(BuildWorkflowError::ZeroRetryBackoff {
            factory_id,
            attempts,
        });
    }
    Ok(RetryPolicy {
        max_attempts: attempts.max(1),
        backoff_base_ms,
    })
}

/// Resolve the execution DAG of a structured workflow's tasklets and refuse
/// anything outside the shared graph-shape rule (`sdk-graph-authoring`): one
/// entry-rooted connected acyclic graph — fan-out and fan-in both legal — with
/// at most one output channel per tasklet, no dynamic cell mode.
///
/// Nodes come out in **Kahn topological order** from the root, so every
/// producer's index precedes its consumers' — for a join, *all* of them. That
/// is the invariant the completion and compensation orders rest on (BFS, the
/// former walk, enqueued a join on its first producer and broke it).
fn build_dag(
    factory_id: &'static str,
    topology: &OperativeTopology,
) -> Result<TaskDag, BuildWorkflowError> {
    refuse_dynamic_cells(factory_id, topology)?;
    refuse_multiple_outputs(factory_id, topology)?;
    refuse_entry_edges(factory_id, topology)?;
    let root = resolve_root(factory_id, topology)?;

    // Reachability is named first, so a genuinely disconnected tasklet is
    // not reported as a cycle symptom; a leftover Kahn node is on a cycle.
    // Kahn pop-in-FIFO from the root yields exactly the former BFS numbering
    // for a tree, so journaled `task_idx` values keep their meaning.
    let n = topology.tasklets.len();
    let edges: Vec<_> = topology
        .channels
        .iter()
        .filter_map(|c| c.producer.map(|p| (p, c)))
        .flat_map(|(p, c)| c.consumer_ids().map(move |id| (p, id)))
        .collect();
    let shape = graph_order(n, &edges, &[root], GraphSeed::EveryIndegreeZero);
    if let Some(node) = shape.unreachable.filter(|_| shape.on_cycle.is_none()) {
        return Err(BuildWorkflowError::Disconnected {
            factory_id,
            tasklet: topology.tasklets[node as usize].name.to_string(),
        });
    }
    if let Some(node) = shape.on_cycle {
        return Err(BuildWorkflowError::Cycle {
            factory_id,
            tasklet: topology.tasklets[node as usize].name.to_string(),
        });
    }

    Ok(dag_of(shape, &edges))
}

/// The DAG the driver runs: the walk's order, with the adjacency re-indexed
/// positionally so stage `i`'s parents and children are stage indices too.
fn dag_of(shape: GraphOrder, edges: &[(TaskletId, TaskletId)]) -> TaskDag {
    let n = shape.pos.len();
    let mut dag = TaskDag {
        order: shape.order,
        parents: vec![Vec::new(); n],
        children: vec![Vec::new(); n],
    };
    for &(producer, consumer) in edges {
        dag.children[shape.pos[producer as usize]].push(shape.pos[consumer as usize]);
        dag.parents[shape.pos[consumer as usize]].push(shape.pos[producer as usize]);
    }
    for children in &mut dag.children {
        children.sort_unstable();
    }
    for parents in &mut dag.parents {
        parents.sort_unstable();
    }
    dag
}

/// Workflow has no `dynamic_cells?` capability.
fn refuse_dynamic_cells(
    factory_id: &'static str,
    topology: &OperativeTopology,
) -> Result<(), BuildWorkflowError> {
    for task in &topology.tasklets {
        if task.mode == CellMode::Dynamic {
            return Err(BuildWorkflowError::DynamicCells {
                factory_id,
                tasklet: task.name.to_string(),
            });
        }
    }
    Ok(())
}

/// A tasklet producing two channels is unroutable (the topology keeps one
/// `output_channel_id`, so the second would be silently orphaned).
/// Structurally impossible on the stream-handle builder; kept as
/// defense-in-depth.
fn refuse_multiple_outputs(
    factory_id: &'static str,
    topology: &OperativeTopology,
) -> Result<(), BuildWorkflowError> {
    for (idx, task) in topology.tasklets.iter().enumerate() {
        let task_id = idx as TaskletId;
        let outgoing = topology
            .channels
            .iter()
            .filter(|c| c.producer == Some(task_id))
            .count();
        if outgoing > 1 {
            return Err(BuildWorkflowError::MultipleOutputChannels {
                factory_id,
                tasklet: task.name.to_string(),
            });
        }
    }
    Ok(())
}

/// The entry channel seeds the root tasklet from the spawn args, and the
/// seed is a bare payload that bypasses the routing seam entirely — so no edge
/// closure declared there could ever be selected, whether it is a merge
/// mapping (every entry channel past the first would be unreadable) or a plain
/// transform (it would silently never run). Refuse the shape rather than
/// accept one that cannot run.
fn refuse_entry_edges(
    factory_id: &'static str,
    topology: &OperativeTopology,
) -> Result<(), BuildWorkflowError> {
    for channel in topology
        .channels
        .iter()
        .filter(|c| c.kind == ChannelKind::Entry)
    {
        if let Some(edge) = channel
            .consumers
            .iter()
            .find(|e| e.transform.is_some() || e.key.is_some())
        {
            return Err(BuildWorkflowError::MergeFromEntryChannel {
                factory_id,
                channel: channel.name.to_string(),
                tasklet: topology.tasklets[edge.tasklet as usize].name.to_string(),
            });
        }
    }
    Ok(())
}

/// The one tasklet the entry channel seeds — a bounded topology has a single
/// root.
fn resolve_root(
    factory_id: &'static str,
    topology: &OperativeTopology,
) -> Result<TaskletId, BuildWorkflowError> {
    let entry = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::Entry)
        .ok_or(BuildWorkflowError::NoEntryChannel { factory_id })?;
    if entry.consumers.len() != 1 {
        return Err(BuildWorkflowError::EntryRoot {
            factory_id,
            channel: entry.name.to_string(),
            consumers: entry.consumers.len(),
        });
    }
    Ok(entry.consumers[0].tasklet)
}
