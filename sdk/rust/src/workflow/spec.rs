//! The typed half of workflow-spec construction: `build_spec::<T>()` runs the
//! author's `procedures` against a `ChannelBuilder` and validates the chain.
//! The spec/DAG data structures live in `flz_core::workflow::spec`.

use std::sync::Arc;
use std::time::Duration;

use flz_core::operative::topology::{EntityMode, OperativeTopology};
pub use flz_core::workflow::spec::*;
use flz_core::workflow::{LoopMode, WorkflowRunFn};

use crate::channel::{ChannelBuilder, ChannelKind, ProcedureId, RetryPolicy};
use crate::operative::builder::build_topology;
use crate::workflow::WorkflowOperative;

/// Build and validate the spec for a workflow class. Empty `procedures()`
/// selects the run-sugar path; a non-empty declaration is validated as a
/// static DAG.
pub fn build_spec<T: WorkflowOperative>() -> Result<WorkflowSpec, BuildWorkflowError> {
    let factory_id = T::factory_id();

    // Self-termination guard: an operative whose only exit is an external stop,
    // with no duration backstop, can never terminate cooperatively.
    if !T::self_terminating() && T::max_duration().is_none() {
        return Err(BuildWorkflowError::ExternalStopOnly { factory_id });
    }

    let retry = retry_policy::<T>(factory_id)?;

    let cb = ChannelBuilder::new();
    T::procedures(&cb);

    // No declared procedures ⇒ run-sugar. A loop needs a chain to iterate.
    if !cb.has_procedures() {
        if T::loop_mode() != LoopMode::Once {
            return Err(BuildWorkflowError::LoopWithoutChain { factory_id });
        }
        return Ok(spec_of::<T>(
            None,
            ProcDag::default(),
            LoopMode::Once,
            None,
            retry,
        ));
    }

    let topology = build_topology(factory_id, cb).map_err(|e| BuildWorkflowError::Topology {
        factory_id,
        detail: format!("{e}"),
    })?;
    let dag = build_dag(factory_id, &topology)?;

    refuse_chained_loop_shape::<T>(factory_id, &dag)?;
    if T::PERSISTENCE_ENABLED {
        refuse_missing_codecs::<T>(factory_id, &topology, &dag)?;
    }

    Ok(spec_of::<T>(
        Some(topology),
        dag,
        T::loop_mode(),
        T::wake_after(),
        retry,
    ))
}

/// The class facts every workflow spec carries whichever shape it took; the
/// run-sugar arm and the structured arm differ only in the four the caller
/// hands in.
fn spec_of<T: WorkflowOperative>(
    topology: Option<Arc<OperativeTopology>>,
    dag: ProcDag,
    loop_mode: LoopMode,
    wake_after: Option<Duration>,
    retry: RetryPolicy,
) -> WorkflowSpec {
    WorkflowSpec {
        sdk_lang: flz_core::header::SDK_LANG_RUST,
        host: false,
        run: T::run as WorkflowRunFn,
        topology,
        dag,
        max_duration: T::max_duration(),
        rollback_mode: T::rollback_mode(),
        loop_mode,
        wake_after,
        retry,
        barrier_retry_attempts: T::barrier_retry_attempts(),
        persistence_enabled: T::PERSISTENCE_ENABLED,
        sync_durability: T::SYNC_DURABILITY,
    }
}

/// A handoff-chained loop re-seeds iteration N+1 from iteration N's terminal
/// batch, so "the terminal procedure" must be unambiguous — a graph with
/// several leaves has no single terminal batch to carry forward.
fn refuse_chained_loop_shape<T: WorkflowOperative>(
    factory_id: &'static str,
    dag: &ProcDag,
) -> Result<(), BuildWorkflowError> {
    if T::loop_mode() == LoopMode::Chained && dag.single_leaf().is_none() {
        return Err(BuildWorkflowError::BranchingChainedLoop {
            factory_id,
            leaves: dag.leaves().count(),
        });
    }
    Ok(())
}

/// A durable structured class journals the inter-proc handoff at every proc
/// boundary (`workflow-proc-journal`), which requires every inter-procedure
/// channel to carry a byte codec — and, for a chained loop, the terminal batch
/// too, since a crash mid-loop would otherwise silently resume with an empty
/// seed and read as "converged". Unpublished library: fail registration
/// instead of shimming.
fn refuse_missing_codecs<T: WorkflowOperative>(
    factory_id: &'static str,
    topology: &OperativeTopology,
    dag: &ProcDag,
) -> Result<(), BuildWorkflowError> {
    if let Some(ch) = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::InterProcedure && c.codec.is_none())
    {
        return Err(BuildWorkflowError::MissingInterProcCodec {
            factory_id,
            channel: ch.name.to_string(),
        });
    }
    if T::loop_mode() != LoopMode::Chained {
        return Ok(());
    }
    let terminal = dag
        .single_leaf()
        .and_then(|leaf| topology.procedure(dag.order[leaf]))
        .expect("a chained loop has a single leaf (checked above)");
    let codec_ok = terminal
        .output_channel_id
        .and_then(|cid| topology.channel(cid))
        .is_some_and(|ch| ch.codec.is_some());
    if !codec_ok {
        return Err(BuildWorkflowError::LoopTerminalWithoutCodec {
            factory_id,
            procedure: terminal.name.to_string(),
        });
    }
    Ok(())
}

/// Resolve the class's per-entity retry ladder onto the shared
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

/// Resolve the execution DAG of a structured workflow's procedures and refuse
/// anything outside the shared graph-shape rule (`sdk-graph-authoring`): one
/// entry-rooted connected acyclic graph — fan-out and fan-in both legal — with
/// at most one output channel per procedure, no dynamic entity mode.
///
/// Nodes come out in **Kahn topological order** from the root, so every
/// producer's index precedes its consumers' — for a join, *all* of them. That
/// is the invariant the completion and compensation orders rest on (BFS, the
/// former walk, enqueued a join on its first producer and broke it).
fn build_dag(
    factory_id: &'static str,
    topology: &OperativeTopology,
) -> Result<ProcDag, BuildWorkflowError> {
    refuse_dynamic_entities(factory_id, topology)?;
    refuse_multiple_outputs(factory_id, topology)?;
    refuse_entry_edges(factory_id, topology)?;
    let root = resolve_root(factory_id, topology)?;

    // Reachability is named first, so a genuinely disconnected procedure is
    // not reported as a cycle symptom; a leftover Kahn node is on a cycle.
    // Kahn pop-in-FIFO from the root yields exactly the former BFS numbering
    // for a tree, so journaled `proc_idx` values keep their meaning.
    let n = topology.procedures.len();
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
            procedure: topology.procedures[node as usize].name.to_string(),
        });
    }
    if let Some(node) = shape.on_cycle {
        return Err(BuildWorkflowError::Cycle {
            factory_id,
            procedure: topology.procedures[node as usize].name.to_string(),
        });
    }

    Ok(dag_of(shape, &edges))
}

/// The DAG the driver runs: the walk's order, with the adjacency re-indexed
/// positionally so stage `i`'s parents and children are stage indices too.
fn dag_of(shape: GraphOrder, edges: &[(ProcedureId, ProcedureId)]) -> ProcDag {
    let n = shape.pos.len();
    let mut dag = ProcDag {
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

/// Workflow has no `dynamic_entities?` capability.
fn refuse_dynamic_entities(
    factory_id: &'static str,
    topology: &OperativeTopology,
) -> Result<(), BuildWorkflowError> {
    for proc in &topology.procedures {
        if proc.mode == EntityMode::Dynamic {
            return Err(BuildWorkflowError::DynamicEntities {
                factory_id,
                procedure: proc.name.to_string(),
            });
        }
    }
    Ok(())
}

/// A procedure producing two channels is unroutable (the topology keeps one
/// `output_channel_id`, so the second would be silently orphaned).
/// Structurally impossible on the stream-handle builder; kept as
/// defense-in-depth.
fn refuse_multiple_outputs(
    factory_id: &'static str,
    topology: &OperativeTopology,
) -> Result<(), BuildWorkflowError> {
    for (idx, proc) in topology.procedures.iter().enumerate() {
        let proc_id = idx as ProcedureId;
        let outgoing = topology
            .channels
            .iter()
            .filter(|c| c.producer == Some(proc_id))
            .count();
        if outgoing > 1 {
            return Err(BuildWorkflowError::MultipleOutputChannels {
                factory_id,
                procedure: proc.name.to_string(),
            });
        }
    }
    Ok(())
}

/// The entry channel seeds the root procedure from the spawn args, and the
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
                procedure: topology.procedures[edge.procedure as usize]
                    .name
                    .to_string(),
            });
        }
    }
    Ok(())
}

/// The one procedure the entry channel seeds — a bounded topology has a single
/// root.
fn resolve_root(
    factory_id: &'static str,
    topology: &OperativeTopology,
) -> Result<ProcedureId, BuildWorkflowError> {
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
    Ok(entry.consumers[0].procedure)
}
