//! The typed half of steps-spec construction: `build_spec::<T>()` runs the
//! author's `channels` against a `ChannelBuilder` and validates the chain.
//! The spec/DAG data structures live in `flz_core::steps::spec`.

use flz_core::relay::settings::Run;
use flz_core::relay::topology::RelayTopology;
pub use flz_core::steps::spec::*;

use crate::channel::{ChannelBuilder, TaskletId};
use crate::relay::builder::build_topology_with_settings;
use crate::relay::Relay;

/// Build and validate the spec for a class that runs in steps: its
/// `channels()` declaration, validated as a static DAG. The call-end rule is
/// the core's, judged with the rest of the record.
pub fn build_spec<T: Relay>() -> Result<std::sync::Arc<RelayTopology>, BuildStepsError> {
    let factory_id = T::factory_id();
    let settings = T::settings();
    let Run::Steps { each, on_failure } = settings.run else {
        unreachable!("build_spec is called for a class that runs in steps");
    };

    let cb = ChannelBuilder::new();
    T::channels(&cb);

    if !cb.has_tasklets() {
        return Err(BuildStepsError::NoTasklets { factory_id });
    }

    let topology = build_topology_with_settings(factory_id, cb, &settings).map_err(|e| {
        BuildStepsError::Topology {
            factory_id,
            error: flz_core::host::render_issues(&e.errors),
        }
    })?;
    let mut topology =
        std::sync::Arc::into_inner(topology).expect("a topology just built is shared with nobody");
    let dag = build_dag(factory_id, &topology)?;
    topology.steps = Some(StepsSpec {
        dag,
        on_failure,
        each,
    });
    Ok(std::sync::Arc::new(topology))
}

/// Resolve the execution DAG of a structured steps relay's tasklets, whose
/// shape `check` judged by the shared graph-shape rule (`sdk-graph-authoring`):
/// an entry-rooted connected acyclic graph (several entry channels, fan-out and
/// fan-in all legal) with at most one output channel per tasklet.
///
/// Nodes come out in **Kahn topological order** from the roots, so every
/// producer's index precedes its consumers' — for a join, *all* of them. That
/// is the invariant the completion and compensation orders rest on (BFS, the
/// former walk, enqueued a join on its first producer and broke it).
fn build_dag(
    factory_id: &'static str,
    topology: &RelayTopology,
) -> Result<TaskDag, BuildStepsError> {
    let entries = topology.entry_consumers();
    if entries.is_empty() {
        return Err(BuildStepsError::NoEntryChannel { factory_id });
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
        return Err(BuildStepsError::Disconnected {
            factory_id,
            tasklet: topology.tasklets[node as usize].name.to_string(),
        });
    }
    if let Some(node) = shape.on_cycle {
        return Err(BuildStepsError::Cycle {
            factory_id,
            tasklet: topology.tasklets[node as usize].name.to_string(),
        });
    }

    Ok(TaskDag::from_walk(shape, entries))
}
