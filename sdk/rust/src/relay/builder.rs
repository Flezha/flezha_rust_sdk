//! `build_topology` consumes a `ChannelBuilder` (populated by
//! `Relay::channels`) and produces a frozen `Arc<RelayTopology>`.
//!
//! Build-time validation:
//! - reject duplicate channel names
//! - reject channels declared but never wired with `.from`
//! - reject multi-producer (also blocked by the type system)
//! - reject `partition_for` default impl on shards with non-empty
//!   `shard_keys()` per `design.md` Open Question 1 (resolved via the
//!   `PARTITION_FOR_OVERRIDDEN` const sentinel on the `Shard` trait)

use std::collections::HashMap;
use std::sync::Arc;

use crate::channel::builder::{BuildTopologyError, ChannelBuilder};
use crate::channel::{ChannelDescriptor, ChannelKind};
use crate::relay::topology::{RelayTopology, ShardDescriptor};

#[derive(Debug, thiserror::Error)]
#[error("relay topology validation failed: {errors:?}")]
pub struct TopologyBuildError {
    pub errors: Vec<BuildTopologyError>,
}

pub fn build_topology(
    factory_id: &'static str,
    builder: ChannelBuilder,
) -> Result<Arc<RelayTopology>, TopologyBuildError> {
    let ChannelBuilder {
        channels,
        name_to_id,
        shards,
        shard_name_to_id,
        mut errors,
    } = builder;

    // Build channel descriptors and validate.
    let mut channel_descs: Vec<ChannelDescriptor> = Vec::with_capacity(channels.len());
    for entry in channels {
        if entry.producer.is_none() && entry.kind != ChannelKind::Entry {
            errors.push(BuildTopologyError::UnwiredChannel { name: entry.name });
        }
        channel_descs.push(ChannelDescriptor {
            name: Arc::from(entry.name),
            kind: entry.kind,
            producer: entry.producer,
            consumers: entry.consumers,
            plugin_name: entry.plugin_name.map(Arc::from),
            wrap_input: entry.wrap_input,
            plugin_serializer: entry.plugin_serializer,
        });
    }

    // Build shard descriptors and validate partition declarations.
    let mut shard_descs: Vec<ShardDescriptor> = Vec::with_capacity(shards.len());
    for entry in shards {
        if !entry.partition_keys.is_empty() && !entry.partition_for_overridden {
            errors.push(BuildTopologyError::PartitionForMissing { name: entry.name });
        }
        if entry.shard_key_overridden && entry.partition_keys.is_empty() {
            errors.push(BuildTopologyError::DynamicModeOnly { name: entry.name });
        }
        shard_descs.push(ShardDescriptor {
            name: Arc::from(entry.name),
            partition_keys: entry.partition_keys,
            partition_for_overridden: entry.partition_for_overridden,
            shard_key_overridden: entry.shard_key_overridden,
            input_channel_id: entry.input_channel_id,
            output_channel_id: entry.output_channel_id,
            input_type_id: entry.input_type_id,
            output_type_id: entry.output_type_id,
            build_partition: entry.build_partition,
            partition_for_erased: entry.partition_for_erased,
        });
    }

    if !errors.is_empty() {
        return Err(TopologyBuildError { errors });
    }

    let mut name_to_id_static: HashMap<&'static str, u32> = HashMap::new();
    for (k, v) in name_to_id {
        name_to_id_static.insert(k, v);
    }
    let mut shard_name_to_id_static: HashMap<&'static str, u32> = HashMap::new();
    for (k, v) in shard_name_to_id {
        shard_name_to_id_static.insert(k, v);
    }

    Ok(Arc::new(RelayTopology {
        channels: channel_descs,
        name_to_id: name_to_id_static,
        shards: shard_descs,
        shard_name_to_id: shard_name_to_id_static,
        factory_id,
    }))
}

/// Convenience constructor used by `register_relay::<R>()`. Calls
/// `R::channels(...)` once and returns the resulting topology.
pub fn build_topology_for<R: crate::relay::Relay>() -> Result<Arc<RelayTopology>, TopologyBuildError>
{
    let mut cb = ChannelBuilder::new();
    R::channels(&mut cb);
    build_topology(R::factory_id(), cb)
}
