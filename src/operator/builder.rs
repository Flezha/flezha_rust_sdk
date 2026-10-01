//! `build_topology_with_settings` consumes a `ChannelBuilder` (populated by
//! `Operator::channels`) and the class's settings tree, and produces a frozen
//! `Arc<OperatorTopology>`.
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
    check, DerivedChannel, HostCellModeDecl, HostChannelDecl, HostChannelKind, HostClassDecl,
    HostLink, HostOverflow, HostTaskletDecl,
};
use flz_core::operator::settings::{durable_json, run_json, Exactly, OperatorSettings, Run};

use crate::channel::builder::{
    BuilderState, ChannelBuilder, ChannelEntry, TaskletEntry, TopologyIssue,
};
use crate::channel::{
    ChannelDescriptor, ChannelKind, ConsumerEdge, OverflowPolicy, PluginSink, TaskletId,
};
use crate::operator::topology::{
    AckGranularity, CellMode, CorrectionPolicy, OperatorTopology, TaskletDescriptor,
};

/// Built-in default per-Cell mailbox capacity used when a Tasklet does
/// not override `mailbox_capacity()` and `FLEZHA_MAILBOX_CAPACITY` is
/// either unset or unparseable.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

/// Environment variable consulted when a Tasklet does not set
/// `MAILBOX_CAPACITY_OVERRIDDEN = true`. Parsed as `usize`; empty,
/// missing, or unparseable values fall through to
/// [`DEFAULT_MAILBOX_CAPACITY`].
pub const ENV_MAILBOX_CAPACITY: &str = "FLEZHA_MAILBOX_CAPACITY";

#[derive(Debug, thiserror::Error)]
#[error("{}", flz_core::host::render_issues(errors))]
pub struct TopologyBuildError {
    pub errors: Vec<TopologyIssue>,
}

/// A flow class with no settings: the topology a test that exercises a
/// topology without an operator type builds.
pub fn build_topology(
    factory_id: &'static str,
    builder: ChannelBuilder,
) -> Result<Arc<OperatorTopology>, TopologyBuildError> {
    build_topology_with_settings(factory_id, builder, &OperatorSettings::default())
}

/// The topology `builder` declares under the class's settings tree. A steps
/// class gets its DAG from `build_spec`; this is the part both runs share.
pub fn build_topology_with_settings(
    factory_id: &'static str,
    builder: ChannelBuilder,
    settings: &OperatorSettings,
) -> Result<Arc<OperatorTopology>, TopologyBuildError> {
    let mut state = builder.into_state();
    // A chain nothing closed is recorded as written; the core answers it.
    state.record_open_chains();
    let keyed_consumers = derive_keyed_consumers(&state.channels);
    let capacities = resolve_capacities(&state.tasklets);
    let derived = check(declaration(
        factory_id,
        settings,
        &state,
        &capacities,
        &keyed_consumers,
    ))
    .map_err(|errors| TopologyBuildError { errors })?;

    let BuilderState {
        channels,
        tasklets,
        tasklet_name_to_id,
        ..
    } = state;

    let mut descriptors = Vec::with_capacity(channels.len());
    let mut name_to_id: HashMap<&'static str, u32> = HashMap::new();
    for (id, (entry, derived_channel)) in channels.into_iter().zip(derived.channels).enumerate() {
        name_to_id.insert(entry.name, id as u32);
        descriptors.push(channel_descriptor(entry, derived_channel));
    }

    let (effectively_once, ack_granularity, correction) = match settings.run {
        Run::Flow {
            exactly:
                Exactly::EffectivelyOnce {
                    ack,
                    correction_ms,
                    beyond_window,
                },
        } => (
            true,
            ack,
            correction_ms.map(|window_ms| CorrectionPolicy {
                window_ms,
                beyond_window,
            }),
        ),
        _ => (false, AckGranularity::PerInput, None),
    };
    Ok(Arc::new(OperatorTopology {
        sdk_lang: flz_core::header::SDK_LANG_RUST,
        channels: descriptors,
        name_to_id,
        tasklets: build_tasklet_descs(tasklets, capacities, &keyed_consumers),
        tasklet_name_to_id,
        factory_id,
        description: settings.description,
        durable: settings.durable,
        max_duration: settings.max_duration,
        effectively_once,
        correction,
        ack_granularity,
        steps: None,
    }))
}

/// The record the core judges: every fact a rule reads, and nothing derived.
/// The closures stay behind on the builder, keyed by the chain they close.
fn declaration(
    factory_id: &'static str,
    settings: &OperatorSettings,
    state: &BuilderState,
    capacities: &[usize],
    keyed_consumers: &HashSet<TaskletId>,
) -> HostClassDecl {
    HostClassDecl {
        factory_id: factory_id.to_string(),
        description: settings.description.to_string(),
        max_duration_ms: settings.max_duration.map(|limit| limit.as_millis() as u64),
        run: Some(run_json(&settings.run)),
        durable: Some(durable_json(&settings.durable)),
        sdk_lang: flz_core::header::SDK_LANG_RUST,
        sdk_version: env!("CARGO_PKG_VERSION").to_string(),
        change_format_id: flz_core::header::FORMAT_ID_POSTCARD,
        compact_after_bytes: None,
        channels: state.channels.iter().map(channel_decl).collect(),
        tasklets: state
            .tasklets
            .iter()
            .enumerate()
            .map(|(id, entry)| tasklet_decl(id, entry, capacities[id], keyed_consumers))
            .collect(),
    }
}

fn channel_decl(entry: &ChannelEntry) -> HostChannelDecl {
    HostChannelDecl {
        name: entry.name.to_string(),
        kind: match entry.kind {
            ChannelKind::Entry => HostChannelKind::Entry,
            ChannelKind::InterTasklet => HostChannelKind::InterTask,
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

fn tasklet_decl(
    id: usize,
    entry: &TaskletEntry,
    mailbox_capacity: usize,
    keyed_consumers: &HashSet<TaskletId>,
) -> HostTaskletDecl {
    HostTaskletDecl {
        name: entry.name.to_string(),
        cell_mode: match cell_mode(id, entry, keyed_consumers) {
            CellMode::Single => HostCellModeDecl::Single,
            CellMode::Static => HostCellModeDecl::Static,
            CellMode::Dynamic => HostCellModeDecl::Dynamic,
        },
        cell_keys: entry.cell_keys.iter().map(|k| k.to_string()).collect(),
        idle_ttl_ms: entry.idle_ttl.map(|ttl| ttl.as_millis() as u64),
        max_cells: Some(entry.max_cells),
        cells_for_overridden: entry.cells_for_overridden,
        compensate_overridden: entry.compensate_overridden,
        clock: entry.clock,
        mailbox_capacity,
        change_is_nochange: entry.change_is_nochange,
        // No rule reads either: a typed class stamps `S::SCHEMA_VERSION` on the
        // records it writes, which is not a topology fact.
        schema_version: 1,
        min_readable_schema_version: None,
    }
}

/// Cell mode is read off the WIRING: a tasklet that names its own keys is
/// static, one reached by a chain that keys its values is dynamic, and a
/// tasklet nothing keys serves one Cell.
fn cell_mode(id: usize, entry: &TaskletEntry, keyed_consumers: &HashSet<TaskletId>) -> CellMode {
    if !entry.cell_keys.is_empty() {
        CellMode::Static
    } else if keyed_consumers.contains(&(id as TaskletId)) {
        CellMode::Dynamic
    } else {
        CellMode::Single
    }
}

/// The tasklets some chain names a Cell for, read off the record's links.
fn derive_keyed_consumers(channels: &[ChannelEntry]) -> HashSet<TaskletId> {
    let mut keyed = HashSet::new();
    for entry in channels {
        for links in &entry.chains {
            let mut key = false;
            for link in links {
                match link {
                    HostLink::Key => key = true,
                    HostLink::Into { tasklet } | HostLink::Merge { tasklet, .. } if key => {
                        keyed.insert(*tasklet);
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
        HostChannelKind::InterTask => ChannelKind::InterTasklet,
        HostChannelKind::Plugin => ChannelKind::PluginBound,
    };
    let consumers: Vec<ConsumerEdge> = derived
        .consumers
        .iter()
        .map(|edge| {
            let (transform, key) = entry.chain_edges[edge.chain].clone();
            ConsumerEdge {
                tasklet: edge.tasklet,
                transform,
                key,
            }
        })
        .collect();
    let call = derived.call.as_ref().map(|call| {
        let (transform, key) = entry.chain_edges[call.chain].clone();
        flz_core::channel::CallEnd {
            transform,
            key,
            ..flz_core::host::call_end(call)
        }
    });
    ChannelDescriptor {
        name: Arc::from(entry.name),
        kind,
        producer: entry.producer,
        consumers,
        wrap_input: entry.wrap_input,
        entry_parsed: entry.entry_parsed,
        plugin_sink: entry.plugin_sink.map(|sink| PluginSink {
            serializer: sink.serializer,
        }),
        response: derived.response,
        sink: derived.sink.is_some(),
        call,
        keyed: entry.keyed,
        overflow_policy: entry.overflow_policy,
        retry: entry.retry,
        // One rule, both paths: a channel carries a journal codec exactly when
        // the journal reads it back, which is when the derived kind is
        // inter-tasklet. `flz_core::host::build::channel_descriptor` says the
        // same thing for a host that ships bytes. A plugin-bound terminal is
        // not a handoff: nothing re-seeds from it, and journaling every plugin
        // payload at every task barrier would be WAL volume nobody reads.
        codec: matches!(kind, ChannelKind::InterTasklet)
            .then_some(entry.codec)
            .flatten(),
    }
}

fn build_tasklet_descs(
    tasklets: Vec<TaskletEntry>,
    capacities: Vec<usize>,
    keyed_consumers: &HashSet<TaskletId>,
) -> Vec<TaskletDescriptor> {
    tasklets
        .into_iter()
        .zip(capacities)
        .enumerate()
        .map(|(id, (entry, capacity))| build_tasklet_desc(id, entry, capacity, keyed_consumers))
        .collect()
}

fn build_tasklet_desc(
    id: usize,
    entry: TaskletEntry,
    mailbox_capacity: usize,
    keyed_consumers: &HashSet<TaskletId>,
) -> TaskletDescriptor {
    let mode = cell_mode(id, &entry, keyed_consumers);
    TaskletDescriptor {
        host_facts: None,
        name: Arc::from(entry.name),
        cell_keys: entry.cell_keys,
        cells_for_overridden: entry.cells_for_overridden,
        compensate_overridden: entry.compensate_overridden,
        mode,
        idle_ttl: entry.idle_ttl,
        max_cells: entry.max_cells,
        input_channel_ids: entry.input_channel_ids,
        output_channel_id: entry.output_channel_id,
        input_type_id: entry.input_type_id,
        output_type_id: entry.output_type_id,
        mailbox_capacity,
        mailbox_capacity_overridden: entry.mailbox_capacity_overridden,
        compact_after_bytes: entry.compact_after_bytes,
        build_cell: entry.build_cell,
        cells_for_erased: entry.cells_for_erased,
        serialize_state: entry.serialize_state,
        deserialize_state: entry.deserialize_state,
    }
}

/// The topology `register_factory(R)` builds for a flow class: `R::channels`
/// once, under `R::settings()`.
pub fn build_topology_for<R: crate::operator::Operator>(
) -> Result<Arc<OperatorTopology>, TopologyBuildError> {
    let cb = ChannelBuilder::new();
    R::channels(&cb);
    build_topology_with_settings(R::factory_id(), cb, &R::settings())
}

/// Every tasklet's resolved mailbox capacity, in declaration order. The
/// resolved value is what the record carries; the range it must fall in is the
/// core's to refuse (`MailboxCapacityZero` / `MailboxCapacityTooLarge`).
fn resolve_capacities(tasklets: &[TaskletEntry]) -> Vec<usize> {
    let env_capacity = flz_core::util::env_raw(ENV_MAILBOX_CAPACITY);
    tasklets
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

/// Resolve a Tasklet's mailbox capacity: an explicit override on the
/// Tasklet wins over `FLEZHA_MAILBOX_CAPACITY`, which wins over the built-in
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
