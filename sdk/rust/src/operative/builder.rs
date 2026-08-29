//! `build_topology` consumes a `ChannelBuilder` (populated by
//! `StreamOperative::channels`) and produces a frozen `Arc<OperativeTopology>`.
//!
//! Build-time validation:
//! - refuse duplicate channel names (derived, `.named(...)`-overridden, and
//!   entry names share one per-operative namespace)
//! - enforce the shared graph-shape rule (`sdk-graph-authoring`): one entry
//!   channel, every node reachable from it, no cycles — fan-out and fan-in
//!   are both legal
//! - refuse an `entities_for` default impl on a procedure with non-empty
//!   `entity_keys()`, told apart by the `ENTITIES_FOR_OVERRIDDEN` const
//!   sentinel on the `Procedure` trait

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use flz_core::workflow::spec::{graph_order, GraphSeed};

use crate::channel::builder::{
    BuilderState, ChannelBuilder, ChannelEntry, PendingEdge, ProcedureEntry, TopologyIssue,
};
use crate::channel::{ChannelDescriptor, ChannelKind, OverflowPolicy, PluginSink};
use crate::operative::topology::{EntityMode, OperativeTopology, ProcedureDescriptor};

/// Built-in default per-Entity mailbox capacity used when a Procedure does
/// not override `mailbox_capacity()` and `FLEZHA_MAILBOX_CAPACITY` is
/// either unset or unparseable.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Hard upper bound on per-Entity mailbox capacity. ~1M items × ~40 B
/// per slot ≈ 40 MB per Entity is past sanity for any expected
/// workload; values above this fail registration with a named error.
pub const MAX_MAILBOX_CAPACITY: usize = 1_048_576;

/// Environment variable consulted when a Procedure does not set
/// `MAILBOX_CAPACITY_OVERRIDDEN = true`. Parsed as `usize`; empty,
/// missing, or unparseable values fall through to
/// [`DEFAULT_MAILBOX_CAPACITY`].
pub const ENV_MAILBOX_CAPACITY: &str = "FLEZHA_MAILBOX_CAPACITY";

#[derive(Debug, thiserror::Error)]
#[error("operative topology validation failed: {errors:?}")]
pub struct TopologyBuildError {
    pub errors: Vec<TopologyIssue>,
}

pub fn build_topology(
    factory_id: &'static str,
    builder: ChannelBuilder,
) -> Result<Arc<OperativeTopology>, TopologyBuildError> {
    build_topology_with_persistence(factory_id, builder, true)
}

/// Like [`build_topology`] but accepts the `StreamOperative::PERSISTENCE_ENABLED`
/// const captured at registration. `build_topology_for::<R>()` is the
/// canonical caller; tests that exercise topology without a StreamOperative
/// generic call [`build_topology`] which defaults the const to `true`.
pub fn build_topology_with_persistence(
    factory_id: &'static str,
    builder: ChannelBuilder,
    persistence_enabled_const: bool,
) -> Result<Arc<OperativeTopology>, TopologyBuildError> {
    let BuilderState {
        channels,
        procedures,
        procedure_name_to_id,
        pending,
        mut errors,
        effectively_once,
        correction,
        ack_granularity,
    } = builder.into_state();

    refuse_dangling_chains(&channels, &pending, &mut errors);
    let name_to_id = resolve_channel_names(&channels, &mut errors);
    refuse_graph_shape(&channels, &procedures, &mut errors);
    refuse_class_facts(effectively_once, correction, ack_granularity, &mut errors);
    let keyed_consumers = derive_keyed_consumers(&channels);
    refuse_edge_keying(&channels, &procedures, &keyed_consumers, &mut errors);
    refuse_channels(&channels, effectively_once, &mut errors);
    let capacities = refuse_procedures(&procedures, correction.is_some(), &mut errors);

    if !errors.is_empty() {
        return Err(TopologyBuildError { errors });
    }

    Ok(Arc::new(OperativeTopology {
        sdk_lang: flz_core::header::SDK_LANG_RUST,
        channels: channels.into_iter().map(build_channel_desc).collect(),
        name_to_id,
        procedures: build_procedure_descs(procedures, capacities, &keyed_consumers),
        procedure_name_to_id,
        factory_id,
        persistence_enabled_const,
        effectively_once,
        correction,
        ack_granularity,
    }))
}

/// A transform chain is stored on the consumer edge it terminates in, so one
/// that no `.into(...)` ever closed would silently do nothing. Only a chain's
/// tip can dangle — every earlier link was superseded by the next.
fn refuse_dangling_chains(
    channels: &[ChannelEntry],
    pending: &[PendingEdge],
    errors: &mut Vec<TopologyIssue>,
) {
    for chain in pending {
        if !chain.superseded && !chain.consumed {
            errors.push(TopologyIssue::DanglingTransformChain {
                channel: channels[chain.channel as usize].name,
            });
        }
    }
}

/// One per-operative channel namespace: derived names (producer node
/// identity), `.named(...)` overrides, and entry names all collide here.
fn resolve_channel_names(
    channels: &[ChannelEntry],
    errors: &mut Vec<TopologyIssue>,
) -> HashMap<&'static str, u32> {
    let mut name_to_id: HashMap<&'static str, u32> = HashMap::new();
    for (id, entry) in channels.iter().enumerate() {
        if name_to_id.insert(entry.name, id as u32).is_some() {
            errors.push(TopologyIssue::DuplicateChannel(entry.name));
        }
    }
    name_to_id
}

/// Shared graph-shape rule (`sdk-graph-authoring`): one OR MORE entry
/// channels feed the graph, every node reachable from some entry, no cycles.
/// Multiple entries let one operative ingest several external sources (e.g. a
/// market pipeline consuming a trade feed and an order-book feed on distinct
/// channels); the controller routes each frame to its entry by
/// `metadata["channel"]`, falling back to the first entry when unset. Fan-out
/// and fan-in are both legal; at-most-one output channel per node holds by
/// construction of the stream-handle builder.
fn refuse_graph_shape(
    channels: &[ChannelEntry],
    procedures: &[ProcedureEntry],
    errors: &mut Vec<TopologyIssue>,
) {
    if procedures.is_empty() {
        return;
    }
    let edges: Vec<_> = channels
        .iter()
        .filter_map(|c| c.producer.map(|p| (p, c)))
        .flat_map(|(p, c)| c.consumers.iter().map(move |e| (p, e.procedure)))
        .collect();
    let roots: Vec<_> = channels
        .iter()
        .filter(|c| c.kind == ChannelKind::Entry)
        .flat_map(|c| c.consumers.iter().map(|e| e.procedure))
        .collect();
    let shape = graph_order(
        procedures.len(),
        &edges,
        &roots,
        GraphSeed::EveryIndegreeZero,
    );
    if let Some(node) = shape.on_cycle {
        errors.push(TopologyIssue::Cycle {
            procedure: procedures[node as usize].name,
        });
    } else if let Some(node) = shape.unreachable {
        // Defensive: the stream-handle builder only creates nodes by wiring
        // from the entry, so an unreachable acyclic node cannot normally
        // arise. A cycle already explains unreachability.
        errors.push(TopologyIssue::UnreachableProcedure {
            procedure: procedures[node as usize].name,
        });
    }
}

/// The class-level declarations that only mean something on the
/// effectively-once tier. Correction replays the durable input log against a
/// rewound baseline and the checkpoint is where the tier's consumed-offset
/// advances; off the tier neither exists, so the declaration is meaningless,
/// not merely degraded. (Correction's other hard prerequisite — every plugin
/// channel keyed, so a bumped epoch can overwrite by natural key — is
/// enforced for the whole tier by `UnkeyedChannelUnderEffectivelyOnce`.)
fn refuse_class_facts(
    effectively_once: bool,
    correction: Option<crate::operative::topology::CorrectionPolicy>,
    ack_granularity: crate::operative::topology::AckGranularity,
    errors: &mut Vec<TopologyIssue>,
) {
    if correction.is_some() && !effectively_once {
        errors.push(TopologyIssue::CorrectionWithoutTier);
    }
    if correction.is_some_and(|c| c.window_ms == 0) {
        errors.push(TopologyIssue::CorrectionWindowZero);
    }
    if ack_granularity.is_checkpoint() && !effectively_once {
        errors.push(TopologyIssue::CheckpointAckWithoutTier);
    }
    if ack_granularity
        .checkpoint()
        .is_some_and(|p| p.every == Some(0))
    {
        errors.push(TopologyIssue::CheckpointEveryZero);
    }
}

/// Entity mode is read off the WIRING: a procedure reached by an edge carrying
/// a key function is dynamic. Derived before the descriptors are built because
/// that pass consumes the consumer edges.
fn derive_keyed_consumers(channels: &[ChannelEntry]) -> HashSet<crate::channel::ProcedureId> {
    channels
        .iter()
        .flat_map(|entry| entry.consumers.iter())
        .filter(|edge| edge.key.is_some())
        .map(|edge| edge.procedure)
        .collect()
}

/// What an edge may declare about its consumer's Entity mode.
fn refuse_edge_keying(
    channels: &[ChannelEntry],
    procedures: &[ProcedureEntry],
    keyed_consumers: &HashSet<crate::channel::ProcedureId>,
    errors: &mut Vec<TopologyIssue>,
) {
    for entry in channels {
        for edge in &entry.consumers {
            let procedure = &procedures[edge.procedure as usize];
            // Static keying routes by `entities_for` and may name several
            // Entities; edge keying names exactly one. Declaring both is a
            // contradiction, not a precedence question.
            if edge.key.is_some() && !procedure.entity_keys.is_empty() {
                errors.push(TopologyIssue::KeyByOnUnkeyableTarget {
                    channel: entry.name,
                    procedure: procedure.name,
                });
            }
            // The key function is total, so a dynamic-mode procedure has no
            // keyless-drop path — every edge into one must carry a key.
            if edge.key.is_none() && keyed_consumers.contains(&edge.procedure) {
                errors.push(TopologyIssue::UnkeyedEdgeIntoDynamicProcedure {
                    channel: entry.name,
                    procedure: procedure.name,
                });
            }
        }
    }
}

/// What a channel's declarations may claim about its plugin wire.
fn refuse_channels(
    channels: &[ChannelEntry],
    effectively_once: bool,
    errors: &mut Vec<TopologyIssue>,
) {
    for entry in channels {
        refuse_channel(entry, effectively_once, errors);
    }
}

fn refuse_channel(entry: &ChannelEntry, effectively_once: bool, errors: &mut Vec<TopologyIssue>) {
    // Identity stamps only exist on plugin frames; a keyed channel must have
    // a plugin wire.
    if entry.keyed && entry.plugin_sink.is_none() {
        errors.push(TopologyIssue::KeyedChannelWithoutPluginWire {
            channel: entry.name,
        });
    }
    // The effectively-once tier deduplicates by output identity, which only
    // keyed channels carry — an unkeyed plugin wire would silently break the
    // guarantee.
    if effectively_once && !entry.keyed && entry.plugin_sink.is_some() {
        errors.push(TopologyIssue::UnkeyedChannelUnderEffectivelyOnce {
            channel: entry.name,
        });
    }
    // Promising effectively-once and asking to shed is not a trade-off, it is
    // a contradiction: a dropped input's offset is advanced past by a later
    // item's watermark, so the loss is permanent under a contract that says
    // nothing is lost. A correction window needs the tier
    // (`CorrectionWithoutTier`), so this one arm covers it too.
    if effectively_once {
        let policy = match entry.overflow_policy {
            OverflowPolicy::DropNothing => None,
            OverflowPolicy::DropNewest => Some("DropNewest"),
            OverflowPolicy::DropOldest => Some("DropOldest"),
        };
        if let Some(policy) = policy {
            errors.push(TopologyIssue::DroppingPolicyUnderEffectivelyOnce {
                channel: entry.name,
                policy,
            });
        }
    }
}

/// Terminality is derived from the wiring, not from which method the author
/// called: a sink with nothing consuming behind it IS the plugin-bound
/// terminal. Deriving it here is what keeps `.sink(...)` one method and makes
/// a sink declared alongside consumers hybrid by construction rather than by
/// ordering.
fn build_channel_desc(entry: ChannelEntry) -> ChannelDescriptor {
    let kind = if entry.plugin_sink.is_some() && entry.consumers.is_empty() {
        ChannelKind::PluginBound
    } else {
        entry.kind
    };
    ChannelDescriptor {
        name: Arc::from(entry.name),
        kind,
        producer: entry.producer,
        consumers: entry.consumers,
        wrap_input: entry.wrap_input,
        entry_parsed: entry.entry_parsed,
        plugin_sink: entry.plugin_sink.map(|sink| PluginSink {
            name: Arc::from(sink.name),
            serializer: sink.serializer,
        }),
        keyed: entry.keyed,
        overflow_policy: entry.overflow_policy,
        retry: entry.retry,
        codec: entry.codec,
        entry_observation: flz_core::channel::entry_observation_for(kind, entry.name),
    }
}

/// Every per-procedure refusal, and the mailbox capacity each procedure
/// resolved to. Resolving before any descriptor exists is what makes the build
/// below infallible: a refused capacity leaves the vector short of the
/// procedure list, and a short vector is unreachable past the error return.
fn refuse_procedures(
    procedures: &[ProcedureEntry],
    corrected: bool,
    errors: &mut Vec<TopologyIssue>,
) -> Vec<usize> {
    let env_capacity = std::env::var(ENV_MAILBOX_CAPACITY).ok();
    let mut capacities = Vec::with_capacity(procedures.len());
    for entry in procedures {
        refuse_procedure(entry, corrected, errors);
        match resolve_mailbox_capacity(
            entry.name,
            entry.mailbox_capacity,
            entry.mailbox_capacity_overridden,
            env_capacity.as_deref(),
        ) {
            Ok(capacity) => capacities.push(capacity),
            Err(issue) => errors.push(issue),
        }
    }
    capacities
}

/// What one procedure's declaration may claim about its Entities and its clock.
fn refuse_procedure(entry: &ProcedureEntry, corrected: bool, errors: &mut Vec<TopologyIssue>) {
    if !entry.entity_keys.is_empty() && !entry.entities_for_overridden {
        errors.push(TopologyIssue::EntitiesForMissing { name: entry.name });
    }
    // LogicalClock serves the durable change seq, which never advances for a
    // NoChange Procedure — fail fast instead of serving a clock frozen at 0.
    if entry.clock_type_name == "LogicalClock" && entry.change_is_nochange {
        errors.push(TopologyIssue::UnservableClock {
            procedure: entry.name,
            clock_type: entry.clock_type_name,
        });
    }
    // Correction orders a replayed slice by event time. A procedure whose
    // clock is the wall or the change seq has no event time, so the sort key
    // does not exist and lateness is undefined.
    if corrected && !entry.clock_is_event_time {
        errors.push(TopologyIssue::CorrectionWithoutEventTime {
            procedure: entry.name,
            clock_type: entry.clock_type_name,
        });
    }
}

fn build_procedure_descs(
    procedures: Vec<ProcedureEntry>,
    capacities: Vec<usize>,
    keyed_consumers: &HashSet<crate::channel::ProcedureId>,
) -> Vec<ProcedureDescriptor> {
    procedures
        .into_iter()
        .zip(capacities)
        .enumerate()
        .map(|(id, (entry, capacity))| build_procedure_desc(id, entry, capacity, keyed_consumers))
        .collect()
}

fn build_procedure_desc(
    id: usize,
    entry: ProcedureEntry,
    mailbox_capacity: usize,
    keyed_consumers: &HashSet<crate::channel::ProcedureId>,
) -> ProcedureDescriptor {
    let mode = match (
        entry.entity_keys.is_empty(),
        keyed_consumers.contains(&(id as crate::channel::ProcedureId)),
    ) {
        (false, _) => EntityMode::Static,
        (true, true) => EntityMode::Dynamic,
        (true, false) => EntityMode::Single,
    };
    ProcedureDescriptor {
        host_facts: None,
        name: Arc::from(entry.name),
        entity_keys: entry.entity_keys,
        entities_for_overridden: entry.entities_for_overridden,
        compensate_overridden: entry.compensate_overridden,
        mode,
        idle_ttl: entry.idle_ttl,
        max_entities: entry.max_entities,
        input_channel_ids: entry.input_channel_ids,
        output_channel_id: entry.output_channel_id,
        input_type_id: entry.input_type_id,
        output_type_id: entry.output_type_id,
        mailbox_capacity,
        mailbox_capacity_overridden: entry.mailbox_capacity_overridden,
        compact_after_bytes: entry.compact_after_bytes,
        build_entity: entry.build_entity,
        entities_for_erased: entry.entities_for_erased,
        serialize_state: entry.serialize_state,
        deserialize_state: entry.deserialize_state,
    }
}

/// Convenience constructor used by `define_stream::<R>()`. Calls
/// `R::channels(...)` once and returns the resulting topology, capturing
/// `R::PERSISTENCE_ENABLED` so `StreamInstance::new` can resolve the
/// final per-instance flag.
pub fn build_topology_for<R: crate::operative::StreamOperative>(
) -> Result<Arc<OperativeTopology>, TopologyBuildError> {
    let cb = ChannelBuilder::new();
    R::channels(&cb);
    build_topology_with_persistence(R::factory_id(), cb, R::PERSISTENCE_ENABLED)
}

/// Resolve a Procedure's mailbox capacity: an explicit override on the
/// Procedure wins over `FLEZHA_MAILBOX_CAPACITY`, which wins
/// over the built-in default. Out-of-range values fail registration with
/// the offending Procedure's name; an unparseable env var falls through to
/// the default rather than failing (runners tolerate typos in env
/// values during development).
pub(crate) fn resolve_mailbox_capacity(
    procedure_name: &'static str,
    declared: usize,
    overridden: bool,
    env_value: Option<&str>,
) -> Result<usize, TopologyIssue> {
    let resolved = if overridden {
        declared
    } else {
        env_value
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAILBOX_CAPACITY)
    };
    refuse_capacity(procedure_name, resolved)
}

/// Refuse a capacity outside the declared range, naming the Procedure.
fn refuse_capacity(procedure_name: &'static str, value: usize) -> Result<usize, TopologyIssue> {
    if value == 0 {
        return Err(TopologyIssue::MailboxCapacityZero {
            procedure: procedure_name,
        });
    }
    if value > MAX_MAILBOX_CAPACITY {
        return Err(TopologyIssue::MailboxCapacityTooLarge {
            procedure: procedure_name,
            value,
        });
    }
    Ok(value)
}

#[cfg(test)]
#[path = "builder_tests.rs"]
mod tests;
