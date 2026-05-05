//! `RelayTopology`: shared, immutable topology produced by
//! `register_relay::<R>()`. Section 2 implements `build_topology`.

use std::collections::HashMap;
use std::sync::Arc;

use crate::channel::builder::{PartitionBuilder, PartitionForFn};
use crate::channel::{ChannelDescriptor, ChannelId, ShardId};

/// One Shard's slot in the topology. Pre-resolved channel ids so the
/// hot path needs zero name lookups.
pub struct ShardDescriptor {
    pub name: Arc<str>,
    pub partition_keys: Vec<&'static str>,
    pub partition_for_overridden: bool,
    pub shard_key_overridden: bool,
    pub input_channel_id: Option<ChannelId>,
    pub output_channel_id: Option<ChannelId>,
    pub input_type_id: std::any::TypeId,
    pub output_type_id: std::any::TypeId,
    /// Final per-Partition mailbox capacity for every Partition this
    /// Shard owns, after applying the override / env-var / default
    /// precedence in `relay::builder::resolve_mailbox_capacity`.
    pub mailbox_capacity: usize,
    /// `Shard::MAILBOX_CAPACITY_OVERRIDDEN` captured at registration time.
    /// Carried for diagnostics so a future tooling change can show which
    /// Shards opted into an explicit capacity.
    pub mailbox_capacity_overridden: bool,
    pub build_partition: PartitionBuilder,
    pub partition_for_erased: PartitionForFn,
}

pub struct RelayTopology {
    pub channels: Vec<ChannelDescriptor>,
    pub name_to_id: HashMap<&'static str, ChannelId>,
    pub shards: Vec<ShardDescriptor>,
    pub shard_name_to_id: HashMap<&'static str, ShardId>,
    pub factory_id: &'static str,
}

impl RelayTopology {
    pub fn lookup_channel_id(&self, name: &str) -> Option<ChannelId> {
        self.name_to_id.get(name).copied()
    }

    pub fn channel(&self, id: ChannelId) -> Option<&ChannelDescriptor> {
        self.channels.get(id as usize)
    }

    pub fn shard(&self, id: ShardId) -> Option<&ShardDescriptor> {
        self.shards.get(id as usize)
    }
}
