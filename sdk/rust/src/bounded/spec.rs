//! Bounded class spec: the frozen authoring result produced at registration.
//!
//! A bounded operative is authored either as bare [`run`](super::BoundedOperative::run)
//! sugar (no declared procedures) or as a structured **DAG** of
//! Procedures/Channels/Entities via
//! [`procedures`](super::BoundedOperative::procedures). `build_spec` runs the
//! declaration once, validates it (single-rooted acyclic graph — fan-out and
//! fan-in both legal, no dynamic entities, self-terminating), and freezes the
//! result for every spawn of the class.

use std::sync::Arc;
use std::time::Duration;

use super::{BoundedOperative, BoundedRunFn, LoopMode, RollbackMode};
use crate::channel::builder::ChannelBuilder;
use crate::channel::{ChannelKind, ProcedureId, RetryPolicy};
use crate::operative::builder::{build_topology, graph_order};
use crate::operative::topology::{EntityMode, OperativeTopology};

/// The static procedure DAG of a structured bounded class (`bounded-branching`).
///
/// `order` holds every procedure in **Kahn topological order from the root**,
/// so every producer of a node — a fan-in has several — sits at a lower index
/// than the node itself. A node's index is also its `stage_idx` and its wire
/// `proc_idx`. That numbering is load-bearing: completion order is topological,
/// and reversing it (what `plan_compensation` already does) is
/// reverse-topological compensation — a join is undone before any of its
/// producers. A linear chain is the single-branch case.
#[derive(Debug, Default)]
pub(crate) struct ProcDag {
    pub order: Vec<ProcedureId>,
    /// Indexes of each node's producers (empty for the root), positionally.
    pub parents: Vec<Vec<usize>>,
    /// Indexes of each node's children — the consumers of its output channel.
    pub children: Vec<Vec<usize>>,
}

impl ProcDag {
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// The nodes with no children — the operative is done when all of them
    /// have committed.
    pub fn leaves(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).filter(|&i| self.children[i].is_empty())
    }

    /// The single leaf, when the graph has exactly one — the terminal procedure
    /// whose batch a handoff-chained loop re-seeds from, and whose output
    /// channel an iteration commit journals.
    pub fn single_leaf(&self) -> Option<usize> {
        let mut leaves = self.leaves();
        let first = leaves.next()?;
        leaves.next().is_none().then_some(first)
    }
}

/// Frozen per-class authoring result stored in the registry. `topology` is
/// `None` for the run-sugar form; `Some` (with the resolved `tree`) for the
/// structured form.
pub(crate) struct BoundedSpec {
    /// Run-sugar body. Invoked only when `topology` is `None`.
    pub run: BoundedRunFn,
    /// Structured topology, or `None` for run-sugar.
    pub topology: Option<Arc<OperativeTopology>>,
    /// Procedures as a static DAG rooted at the entry channel's consumer.
    /// Empty for run-sugar.
    pub dag: ProcDag,
    /// Optional wall-clock backstop applied by the driver.
    pub max_duration: Option<Duration>,
    /// Saga rollback scope applied on abnormal termination.
    pub rollback_mode: RollbackMode,
    /// Loop shape of the structured chain (`LoopMode::Once` for run-sugar).
    pub loop_mode: LoopMode,
    /// Durable wake deadline armed when a `PerInput` loop parks.
    pub wake_after: Option<Duration>,
    /// Per-entity retry ladder applied inside a structured stage
    /// (`bounded-entity-retry`), on the same policy type the unbounded
    /// retry-then-park loop uses.
    pub retry: RetryPolicy,
    /// Class-level persistence toggle; when true, a structured bounded's
    /// per-Entity Changes are flushed to the WAL as they apply.
    pub persistence_enabled: bool,
}

// Manual: `OperativeTopology` holds closures and is not `Debug`. A summary is
// enough for test assertions (`expect`/`expect_err`) and diagnostics.
impl std::fmt::Debug for BoundedSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedSpec")
            .field("structured", &self.topology.is_some())
            .field("dag_len", &self.dag.len())
            .field("max_duration", &self.max_duration)
            .field("rollback_mode", &self.rollback_mode)
            .field("loop_mode", &self.loop_mode)
            .field("retry", &self.retry)
            .field("persistence_enabled", &self.persistence_enabled)
            .finish()
    }
}

/// Registration-time rejections for a bounded operative authoring.
#[derive(Debug, thiserror::Error)]
pub enum BuildBoundedError {
    #[error("bounded operative `{factory_id}` has an invalid topology: {detail}")]
    Topology {
        factory_id: &'static str,
        detail: String,
    },

    #[error(
        "bounded operative `{factory_id}` declares procedures but no entry channel; a structured \
         bounded must declare one input channel (c.input::<T>(...)) to seed its first procedure"
    )]
    NoEntryChannel { factory_id: &'static str },

    #[error(
        "bounded operative `{factory_id}` declares procedure `{procedure}` on a cycle; a \
         bounded topology is a DAG — a procedure's output may never reach its own input"
    )]
    Cycle {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "bounded operative `{factory_id}` declares procedure `{procedure}` as the producer of \
         more than one channel; a procedure emits on exactly one output channel (its `Output` \
         type), so a second one could never be routed. Fan out by declaring several consumers on \
         the one channel"
    )]
    MultipleOutputChannels {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "bounded operative `{factory_id}` declares entry channel `{channel}` feeding {consumers} \
         procedures; a bounded topology has a single root — the entry channel must feed exactly \
         one procedure, which then fans out"
    )]
    EntryRoot {
        factory_id: &'static str,
        channel: String,
        consumers: usize,
    },

    #[error(
        "bounded operative `{factory_id}` merges entry channel `{channel}` into procedure \
         `{procedure}`; a bounded operative is seeded from its spawn args (and, for a `PerInput` \
         loop, its inbox items) through ONE entry channel, and neither carries the channel a \
         payload arrived on — so an entry channel cannot be one merge source among several. \
         Merge the output streams of two or more of this operative's procedures instead"
    )]
    MergeFromEntryChannel {
        factory_id: &'static str,
        channel: String,
        procedure: String,
    },

    #[error(
        "bounded operative `{factory_id}` declares a handoff-chained loop over a branching \
         topology ({leaves} terminal procedures); iteration N+1 is seeded from iteration N's \
         terminal batch, which a graph with several leaves does not define. Use \
         `LoopMode::PerInput`, or keep the chained loop converging on one terminal"
    )]
    BranchingChainedLoop {
        factory_id: &'static str,
        leaves: usize,
    },

    #[error(
        "bounded operative `{factory_id}` declares a dynamic-entity procedure `{procedure}`; \
         bounded operatives do not support dynamic entity mode"
    )]
    DynamicEntities {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "bounded operative `{factory_id}` is not terminable: its only possible exit is an external \
         stop. Add a terminal condition — let run()/the chain complete, give the loop a terminal \
         predicate, or declare max_duration()."
    )]
    ExternalStopOnly { factory_id: &'static str },

    #[error(
        "bounded operative `{factory_id}` declares procedure `{procedure}` that is not reachable \
         from the entry channel; every procedure must hang off the one graph rooted there"
    )]
    Disconnected {
        factory_id: &'static str,
        procedure: String,
    },

    #[error(
        "bounded operative `{factory_id}` declares a retry ladder ({attempts} attempts) with a \
         zero backoff base; retries would re-run the failing entity in a hot loop. Give \
         retry_backoff() a non-zero base, or set retry_attempts() to 0 for fail-fast"
    )]
    ZeroRetryBackoff {
        factory_id: &'static str,
        attempts: u32,
    },

    #[error(
        "bounded operative `{factory_id}` declares a chain loop but no procedures; a loop iterates \
         a structured chain — declare procedures() (c.input::<T>(...) + procedures), or drop \
         loop_mode() and let the run() body drive its own loop_until"
    )]
    LoopWithoutChain { factory_id: &'static str },

    #[error(
        "bounded operative `{factory_id}` is durable but its inter-procedure channel `{channel}` \
         has no byte codec; declare `.codec()` (standard serde payloads) or `.codec_with(...)` \
         so the proc-journal can persist and re-seed the stage handoff, or opt the class out with \
         `PERSISTENCE_ENABLED = false`"
    )]
    MissingInterProcCodec {
        factory_id: &'static str,
        channel: String,
    },

    #[error(
        "bounded operative `{factory_id}` is a durable handoff-chained loop, but its terminal \
         procedure `{procedure}` releases no codec'd output channel; iteration N+1 is seeded from \
         iteration N's terminal batch, which the iteration commit must be able to journal. Give \
         the terminal procedure an output channel with `.codec()`, or switch to \
         `LoopMode::PerInput`"
    )]
    LoopTerminalWithoutCodec {
        factory_id: &'static str,
        procedure: String,
    },
}

/// Build and validate the spec for a bounded class. Empty `procedures()`
/// selects the run-sugar path; a non-empty declaration is validated as a
/// static DAG.
pub(crate) fn build_spec<T: BoundedOperative>() -> Result<BoundedSpec, BuildBoundedError> {
    let factory_id = T::factory_id();

    // Self-termination guard: an operative whose only exit is an external stop,
    // with no duration backstop, can never terminate cooperatively.
    if !T::self_terminating() && T::max_duration().is_none() {
        return Err(BuildBoundedError::ExternalStopOnly { factory_id });
    }

    let retry = retry_policy::<T>(factory_id)?;

    let cb = ChannelBuilder::new();
    T::procedures(&cb);

    // No declared procedures ⇒ run-sugar. A loop needs a chain to iterate.
    if !cb.has_procedures() {
        if T::loop_mode() != LoopMode::Once {
            return Err(BuildBoundedError::LoopWithoutChain { factory_id });
        }

        return Ok(BoundedSpec {
            run: T::run as BoundedRunFn,
            topology: None,
            dag: ProcDag::default(),
            max_duration: T::max_duration(),
            rollback_mode: T::rollback_mode(),
            loop_mode: LoopMode::Once,
            wake_after: None,
            retry,
            persistence_enabled: T::PERSISTENCE_ENABLED,
        });
    }

    let topology = build_topology(factory_id, cb).map_err(|e| BuildBoundedError::Topology {
        factory_id,
        detail: format!("{e}"),
    })?;
    let dag = build_dag(factory_id, &topology)?;

    // A handoff-chained loop re-seeds iteration N+1 from iteration N's terminal
    // batch, so "the terminal procedure" must be unambiguous — a graph with
    // several leaves has no single terminal batch to carry forward.
    if T::loop_mode() == LoopMode::Chained && dag.single_leaf().is_none() {
        return Err(BuildBoundedError::BranchingChainedLoop {
            factory_id,
            leaves: dag.leaves().count(),
        });
    }

    // Durable structured classes journal the inter-proc handoff at every proc
    // boundary (`bounded-proc-journal`), which requires every inter-procedure
    // channel to carry a byte codec. Unpublished library: fail registration
    // instead of shimming.
    if T::PERSISTENCE_ENABLED {
        if let Some(ch) = topology
            .channels
            .iter()
            .find(|c| c.kind == ChannelKind::InterProcedure && c.codec.is_none())
        {
            return Err(BuildBoundedError::MissingInterProcCodec {
                factory_id,
                channel: ch.name.to_string(),
            });
        }

        // The chained loop's terminal batch is journaled, so it must be
        // serializable — otherwise a crash mid-loop would silently resume with
        // an empty seed and read as "converged".
        if T::loop_mode() == LoopMode::Chained {
            let terminal = dag
                .single_leaf()
                .and_then(|leaf| topology.procedure(dag.order[leaf]))
                .expect("a chained loop has a single leaf (checked above)");
            let codec_ok = terminal
                .output_channel_id
                .and_then(|cid| topology.channel(cid))
                .is_some_and(|ch| ch.codec.is_some());
            if !codec_ok {
                return Err(BuildBoundedError::LoopTerminalWithoutCodec {
                    factory_id,
                    procedure: terminal.name.to_string(),
                });
            }
        }
    }

    Ok(BoundedSpec {
        run: T::run as BoundedRunFn,
        topology: Some(topology),
        dag,
        max_duration: T::max_duration(),
        rollback_mode: T::rollback_mode(),
        loop_mode: T::loop_mode(),
        wake_after: T::wake_after(),
        retry,
        persistence_enabled: T::PERSISTENCE_ENABLED,
    })
}

/// Resolve the class's per-entity retry ladder onto the shared
/// [`RetryPolicy`], rejecting a ladder that would spin without a backoff. A
/// declared 0 collapses to the one-attempt fail-fast the driver runs.
fn retry_policy<T: BoundedOperative>(
    factory_id: &'static str,
) -> Result<RetryPolicy, BuildBoundedError> {
    let attempts = T::retry_attempts();
    let backoff_base_ms = T::retry_backoff().as_millis().min(u64::MAX as u128) as u64;
    if attempts > 1 && backoff_base_ms == 0 {
        return Err(BuildBoundedError::ZeroRetryBackoff {
            factory_id,
            attempts,
        });
    }
    Ok(RetryPolicy {
        max_attempts: attempts.max(1),
        backoff_base_ms,
    })
}

/// Resolve the execution DAG of a structured bounded's procedures and reject
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
) -> Result<ProcDag, BuildBoundedError> {
    // Bounded has no `dynamic_entities?` capability.
    for proc in &topology.procedures {
        if proc.mode == EntityMode::Dynamic {
            return Err(BuildBoundedError::DynamicEntities {
                factory_id,
                procedure: proc.name.to_string(),
            });
        }
    }
    // A procedure producing two channels is unroutable (the topology keeps
    // one `output_channel_id`, so the second would be silently orphaned).
    // Structurally impossible on the stream-handle builder; kept as
    // defense-in-depth.
    for (idx, proc) in topology.procedures.iter().enumerate() {
        let proc_id = idx as ProcedureId;
        let outgoing = topology
            .channels
            .iter()
            .filter(|c| c.producer == Some(proc_id))
            .count();
        if outgoing > 1 {
            return Err(BuildBoundedError::MultipleOutputChannels {
                factory_id,
                procedure: proc.name.to_string(),
            });
        }
    }

    // The entry channel seeds the root procedure from the spawn args. The seed
    // is a bare payload — it names no channel — so a merge mapping on an entry
    // edge could never be selected, and every entry channel past the first
    // would be unreadable. Refuse the shape rather than accept one that cannot
    // run (`multi-input-procedures`).
    for channel in topology
        .channels
        .iter()
        .filter(|c| c.kind == ChannelKind::Entry)
    {
        if let Some(edge) = channel.consumers.iter().find(|e| e.map.is_some()) {
            return Err(BuildBoundedError::MergeFromEntryChannel {
                factory_id,
                channel: channel.name.to_string(),
                procedure: topology.procedures[edge.procedure as usize]
                    .name
                    .to_string(),
            });
        }
    }
    let entry = topology
        .channels
        .iter()
        .find(|c| c.kind == ChannelKind::Entry)
        .ok_or(BuildBoundedError::NoEntryChannel { factory_id })?;
    if entry.consumers.len() != 1 {
        return Err(BuildBoundedError::EntryRoot {
            factory_id,
            channel: entry.name.to_string(),
            consumers: entry.consumers.len(),
        });
    }
    let root = entry.consumers[0].procedure;

    // The shared graph-shape walk (`operative::builder::graph_order`) — one
    // mechanism for both SDK-level validation and the bounded execution order.
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
    let shape = graph_order(n, &edges, &[root]);
    if let Some(node) = shape.unreachable.filter(|_| shape.on_cycle.is_none()) {
        return Err(BuildBoundedError::Disconnected {
            factory_id,
            procedure: topology.procedures[node as usize].name.to_string(),
        });
    }
    if let Some(node) = shape.on_cycle {
        return Err(BuildBoundedError::Cycle {
            factory_id,
            procedure: topology.procedures[node as usize].name.to_string(),
        });
    }

    let mut dag = ProcDag {
        order: shape.order,
        parents: vec![Vec::new(); n],
        children: vec![Vec::new(); n],
    };
    for &(producer, consumer) in &edges {
        dag.children[shape.pos[producer as usize]].push(shape.pos[consumer as usize]);
        dag.parents[shape.pos[consumer as usize]].push(shape.pos[producer as usize]);
    }
    for children in &mut dag.children {
        children.sort_unstable();
    }
    for parents in &mut dag.parents {
        parents.sort_unstable();
    }

    Ok(dag)
}
