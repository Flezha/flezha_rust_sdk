//! `RelayInstance`: per-`AddProcessor` runtime state.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::channel::builder::BuildPartitionArgs;
use crate::compute_engine::work_stealing_pool::WorkStealingPool;
use crate::partition::PartitionSlot;
use crate::relay::topology::RelayTopology;

pub struct RelayInstance {
    pub topology: Arc<RelayTopology>,
    pub instance_id: Arc<str>,
    /// Indexed by `ShardId`. Each entry is a Vec of partitions for that
    /// Shard. Single-mode shards have one entry; static-mode shards have
    /// `shard_keys().len()` entries.
    pub partitions: Vec<Vec<Arc<PartitionSlot>>>,
    /// Indexed by `ShardId`. Maps `&'static str` partition keys to the
    /// partition index. Empty for single-mode shards.
    pub partition_key_to_idx: Vec<HashMap<&'static str, usize>>,
}

impl RelayInstance {
    /// Construct a fresh `RelayInstance`. `Arc::new_cyclic` provides the
    /// `Weak<RelayInstance>` self-reference each `PartitionSlot` needs
    /// for emit dispatch — no unsafe, no two-stage publication.
    pub fn new(
        topology: Arc<RelayTopology>,
        instance_id: Arc<str>,
        pool: Arc<WorkStealingPool>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak: &Weak<RelayInstance>| {
            let shard_count = topology.shards.len();
            let mut partitions: Vec<Vec<Arc<PartitionSlot>>> = Vec::with_capacity(shard_count);
            let mut partition_key_to_idx: Vec<HashMap<&'static str, usize>> =
                Vec::with_capacity(shard_count);

            for (shard_id, shard_desc) in topology.shards.iter().enumerate() {
                let shard_id_u32 = shard_id as u32;
                if shard_desc.partition_keys.is_empty() {
                    let slot = (shard_desc.build_partition)(BuildPartitionArgs {
                        topology: Arc::clone(&topology),
                        instance: Weak::clone(weak),
                        shard_id: shard_id_u32,
                        shard_key: None,
                        pool: Arc::clone(&pool),
                    });
                    partitions.push(vec![slot]);
                    partition_key_to_idx.push(HashMap::new());
                } else {
                    let mut slots = Vec::with_capacity(shard_desc.partition_keys.len());
                    let mut key_map = HashMap::with_capacity(shard_desc.partition_keys.len());
                    for (idx, key) in shard_desc.partition_keys.iter().enumerate() {
                        let slot = (shard_desc.build_partition)(BuildPartitionArgs {
                            topology: Arc::clone(&topology),
                            instance: Weak::clone(weak),
                            shard_id: shard_id_u32,
                            shard_key: Some(key),
                            pool: Arc::clone(&pool),
                        });
                        slots.push(slot);
                        key_map.insert(*key, idx);
                    }
                    partitions.push(slots);
                    partition_key_to_idx.push(key_map);
                }
            }

            RelayInstance {
                topology,
                instance_id,
                partitions,
                partition_key_to_idx,
            }
        })
    }
}
