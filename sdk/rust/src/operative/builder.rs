//! `build_topology` consumes a `ChannelBuilder` (populated by
//! `StreamOperative::channels`) and produces a frozen `Arc<OperativeTopology>`.
//!
//! The builder judges nothing. It writes what the author declared into the
//! shared record ([`flz_core::host::HostClassDecl`]), submits it to
//! [`flz_core::host::check`] — the one place the topology rules live for every
//! SDK — and materialises the typed topology from the declaration the core
//! derived: its channel names, its channel kinds and its consumer edges, with
//! the closures this builder held aside for each chain attached to the edge
//! that chain produced.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use flz_core::host::{
    check, DerivedChannel, HostAckGranularity, HostBeyondWindow, HostChannelDecl, HostChannelKind,
    HostCheckpointOutputs, HostClassDecl, HostClock, HostEntityModeDecl, HostLink, HostOverflow,
    HostProcedureDecl,
};

use crate::channel::builder::{
    BuilderState, ChannelBuilder, ChannelEntry, ProcedureEntry, TopologyIssue,
};
use crate::channel::{
    ChannelDescriptor, ChannelKind, ConsumerEdge, OverflowPolicy, PluginSink, ProcedureId,
};
use crate::operative::topology::{
    AckGranularity, BeyondWindowPolicy, CheckpointOutputs, CheckpointPolicy, EntityMode,
    OperativeTopology, ProcedureDescriptor,
};

/// Built-in default per-Entity mailbox capacity used when a Procedure does
/// not override `mailbox_capacity()` and `FLEZHA_MAILBOX_CAPACITY` is
/// either unset or unparseable.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Environment variable consulted when a Procedure does not set
/// `MAILBOX_CAPACITY_OVERRIDDEN = true`. Parsed as `usize`; empty,
/// missing, or unparseable values fall through to
/// [`DEFAULT_MAILBOX_CAPACITY`].
pub const ENV_MAILBOX_CAPACITY: &str = "FLEZHA_MAILBOX_CAPACITY";

#[derive(Debug, thiserror::Error)]
#[error("{}", flz_core::host::render_issues(errors))]
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
    let mut state = builder.into_state();
    // A chain nothing closed is recorded as written; the core answers it.
    state.record_open_chains();
    let keyed_consumers = derive_keyed_consumers(&state.channels);
    let capacities = resolve_capacities(&state.procedures);
    let derived = check(declaration(
        factory_id,
        persistence_enabled_const,
        &state,
        &capacities,
        &keyed_consumers,
    ))
    .map_err(|errors| TopologyBuildError { errors })?;

    let BuilderState {
        channels,
        procedures,
        procedure_name_to_id,
        effectively_once,
        correction,
        checkpoint_every,
        checkpoint_outputs,
        checkpoint_max_delay_ms,
        ..
    } = state;

    let mut descriptors = Vec::with_capacity(channels.len());
    let mut name_to_id: HashMap<&'static str, u32> = HashMap::new();
    for (id, (entry, derived_channel)) in channels.into_iter().zip(derived.channels).enumerate() {
        name_to_id.insert(entry.name, id as u32);
        descriptors.push(channel_descriptor(entry, derived_channel));
    }

    Ok(Arc::new(OperativeTopology {
        sdk_lang: flz_core::header::SDK_LANG_RUST,
        channels: descriptors,
        name_to_id,
        procedures: build_procedure_descs(procedures, capacities, &keyed_consumers),
        procedure_name_to_id,
        factory_id,
        persistence_enabled_const,
        effectively_once,
        correction,
        ack_granularity: match derived.ack_granularity {
            HostAckGranularity::PerInput => AckGranularity::PerInput,
            HostAckGranularity::Checkpoint => AckGranularity::Checkpoint(CheckpointPolicy {
                every: checkpoint_every,
                outputs: checkpoint_outputs,
                max_delay_ms: checkpoint_max_delay_ms,
            }),
        },
    }))
}

/// The record the core judges: every fact a rule reads, and nothing derived.
/// The closures stay behind on the builder, keyed by the chain they close.
///
/// A workflow class declares `None` here: its own facts — the loop shape, the
/// retry ladder, the proc-journal codecs — are `build_spec`'s to refuse, and
/// this record carries the channel topology they are declared over.
fn declaration(
    factory_id: &'static str,
    persistent: bool,
    state: &BuilderState,
    capacities: &[usize],
    keyed_consumers: &HashSet<ProcedureId>,
) -> HostClassDecl {
    HostClassDecl {
        factory_id: factory_id.to_string(),
        persistent,
        sdk_lang: flz_core::header::SDK_LANG_RUST,
        sdk_version: env!("CARGO_PKG_VERSION").to_string(),
        change_format_id: flz_core::header::FORMAT_ID_POSTCARD,
        effectively_once: state.effectively_once,
        checkpoint_ack: state.checkpoint_ack,
        checkpoint_every: state.checkpoint_every,
        checkpoint_outputs: match state.checkpoint_outputs {
            CheckpointOutputs::AsProduced => HostCheckpointOutputs::AsProduced,
            CheckpointOutputs::AtCheckpoint => HostCheckpointOutputs::AtCheckpoint,
        },
        checkpoint_max_delay_ms: state.checkpoint_max_delay_ms,
        correction_window_ms: state.correction.map(|c| c.window_ms),
        beyond_window_policy: match state.correction.map(|c| c.beyond_window) {
            Some(BeyondWindowPolicy::Drop) => HostBeyondWindow::Drop,
            _ => HostBeyondWindow::ProcessAsArrival,
        },
        compact_after_bytes: None,
        sync_durability: false,
        channels: state.channels.iter().map(channel_decl).collect(),
        procedures: state
            .procedures
            .iter()
            .enumerate()
            .map(|(id, entry)| procedure_decl(id, entry, capacities[id], keyed_consumers))
            .collect(),
        workflow: None,
    }
}

fn channel_decl(entry: &ChannelEntry) -> HostChannelDecl {
    HostChannelDecl {
        name: entry.name.to_string(),
        kind: match entry.kind {
            ChannelKind::Entry => HostChannelKind::Entry,
            ChannelKind::InterProcedure => HostChannelKind::InterProc,
            ChannelKind::PluginBound => HostChannelKind::Plugin,
        },
        producer: entry.producer,
        chains: entry.chains.clone(),
        retry_max: entry.retry.max_attempts,
        retry_backoff_ms: entry.retry.backoff_base_ms,
        overflow: match entry.overflow_policy {
            OverflowPolicy::DropNothing => HostOverflow::DropNothing,
            OverflowPolicy::DropNewest => HostOverflow::DropNewest,
            OverflowPolicy::DropOldest => HostOverflow::DropOldest,
        },
        keyed: entry.keyed,
    }
}

fn procedure_decl(
    id: usize,
    entry: &ProcedureEntry,
    mailbox_capacity: usize,
    keyed_consumers: &HashSet<ProcedureId>,
) -> HostProcedureDecl {
    HostProcedureDecl {
        name: entry.name.to_string(),
        entity_mode: match entity_mode(id, entry, keyed_consumers) {
            EntityMode::Single => HostEntityModeDecl::Single,
            EntityMode::Static => HostEntityModeDecl::Static,
            EntityMode::Dynamic => HostEntityModeDecl::Dynamic,
        },
        entity_keys: entry.entity_keys.iter().map(|k| k.to_string()).collect(),
        idle_ttl_ms: entry.idle_ttl.map(|ttl| ttl.as_millis() as u64),
        max_entities: Some(entry.max_entities),
        entities_for_overridden: entry.entities_for_overridden,
        compensate_overridden: entry.compensate_overridden,
        clock: clock_of(entry),
        mailbox_capacity,
        change_is_nochange: entry.change_is_nochange,
        // No rule reads either: a typed class stamps `S::SCHEMA_VERSION` on the
        // records it writes, which is not a topology fact.
        schema_version: 1,
        min_readable_schema_version: None,
    }
}

/// The record names the clock the runtime serves, not the Rust type behind it.
fn clock_of(entry: &ProcedureEntry) -> HostClock {
    if entry.clock_is_event_time {
        HostClock::EventTime
    } else if entry.clock_type_name == "LogicalClock" {
        HostClock::Logical
    } else {
        HostClock::Wall
    }
}

/// Entity mode is read off the WIRING: a procedure that names its own keys is
/// static, one reached by a chain that keys its values is dynamic, and a
/// procedure nothing keys serves one Entity.
fn entity_mode(
    id: usize,
    entry: &ProcedureEntry,
    keyed_consumers: &HashSet<ProcedureId>,
) -> EntityMode {
    if !entry.entity_keys.is_empty() {
        EntityMode::Static
    } else if keyed_consumers.contains(&(id as ProcedureId)) {
        EntityMode::Dynamic
    } else {
        EntityMode::Single
    }
}

/// The procedures some chain names an Entity for, read off the record's links.
fn derive_keyed_consumers(channels: &[ChannelEntry]) -> HashSet<ProcedureId> {
    let mut keyed = HashSet::new();
    for entry in channels {
        for links in &entry.chains {
            let mut key = false;
            for link in links {
                match link {
                    HostLink::Key => key = true,
                    HostLink::Into { procedure } | HostLink::Merge { procedure, .. } if key => {
                        keyed.insert(*procedure);
                    }
                    _ => {}
                }
            }
        }
    }
    keyed
}

/// One channel as the runtime holds it: the ends and the kind the core
/// derived, and the closures this builder held aside for the chain each edge
/// was declared by.
fn channel_descriptor(entry: ChannelEntry, derived: DerivedChannel) -> ChannelDescriptor {
    let kind = match derived.kind {
        HostChannelKind::Entry => ChannelKind::Entry,
        HostChannelKind::InterProc => ChannelKind::InterProcedure,
        HostChannelKind::Plugin => ChannelKind::PluginBound,
    };
    let consumers: Vec<ConsumerEdge> = derived
        .consumers
        .iter()
        .map(|edge| {
            let (transform, key) = entry.chain_edges[edge.chain].clone();
            ConsumerEdge {
                procedure: edge.procedure,
                transform,
                key,
            }
        })
        .collect();
    ChannelDescriptor {
        name: Arc::from(entry.name),
        kind,
        producer: entry.producer,
        consumers,
        wrap_input: entry.wrap_input,
        entry_parsed: entry.entry_parsed,
        plugin_sink: entry.plugin_sink.map(|sink| PluginSink {
            name: Arc::from(sink.name),
            serializer: sink.serializer,
        }),
        keyed: entry.keyed,
        overflow_policy: entry.overflow_policy,
        retry: entry.retry,
        // One rule, both paths: a channel carries a journal codec exactly when
        // the journal reads it back, which is when the derived kind is
        // inter-procedure. `flz_core::host::build::channel_descriptor` says the
        // same thing for a host that ships bytes. A plugin-bound terminal is
        // not a handoff: nothing re-seeds from it, and journaling every plugin
        // payload at every proc barrier would be WAL volume nobody reads.
        codec: matches!(kind, ChannelKind::InterProcedure)
            .then_some(entry.codec)
            .flatten(),
    }
}

fn build_procedure_descs(
    procedures: Vec<ProcedureEntry>,
    capacities: Vec<usize>,
    keyed_consumers: &HashSet<ProcedureId>,
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
    keyed_consumers: &HashSet<ProcedureId>,
) -> ProcedureDescriptor {
    let mode = entity_mode(id, &entry, keyed_consumers);
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

/// Every procedure's resolved mailbox capacity, in declaration order. The
/// resolved value is what the record carries; the range it must fall in is the
/// core's to refuse (`MailboxCapacityZero` / `MailboxCapacityTooLarge`).
fn resolve_capacities(procedures: &[ProcedureEntry]) -> Vec<usize> {
    let env_capacity = flz_core::util::env_raw(ENV_MAILBOX_CAPACITY);
    procedures
        .iter()
        .map(|entry| {
            resolve_mailbox_capacity(
                entry.mailbox_capacity,
                entry.mailbox_capacity_overridden,
                env_capacity.as_deref(),
            )
        })
        .collect()
}

/// Resolve a Procedure's mailbox capacity: an explicit override on the
/// Procedure wins over `FLEZHA_MAILBOX_CAPACITY`, which wins over the built-in
/// default. An unparseable env var falls through to the default rather than
/// failing (runners tolerate typos in env values during development).
pub(crate) fn resolve_mailbox_capacity(
    declared: usize,
    overridden: bool,
    env_value: Option<&str>,
) -> usize {
    if overridden {
        return declared;
    }
    env_value
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAILBOX_CAPACITY)
}

#[cfg(test)]
#[path = "builder_tests.rs"]
mod tests;
