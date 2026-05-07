//! `RelayInstance`: per-`AddProcessor` runtime state.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::channel::builder::BuildPartitionArgs;
use crate::compute_engine::work_stealing_pool::WorkStealingPool;
use crate::partition::PartitionSlot;
use crate::relay::persistence::resolve_persistence_enabled;
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
    /// Frozen-at-instance-start persistence flag. Resolved from the
    /// Relay class const + operator override; propagated to every
    /// `PartitionSlot`. Subsequent operator config changes do not
    /// affect this instance — only `stop_relay` + `start_relay` cycles
    /// pick up new values.
    persistence_enabled: bool,
}

impl RelayInstance {
    /// Construct a fresh `RelayInstance` with no operator persistence
    /// override. Equivalent to
    /// `RelayInstance::with_persistence_override(.., None)`.
    pub fn new(
        topology: Arc<RelayTopology>,
        instance_id: Arc<str>,
        pool: Arc<WorkStealingPool>,
    ) -> Arc<Self> {
        Self::with_persistence_override(topology, instance_id, pool, None)
    }

    /// Construct a fresh `RelayInstance` with an explicit operator
    /// override for `relay_persistence.<RelayClass>`. The operator
    /// override (when `Some`) wins over the class const.
    pub fn with_persistence_override(
        topology: Arc<RelayTopology>,
        instance_id: Arc<str>,
        pool: Arc<WorkStealingPool>,
        operator_override: Option<bool>,
    ) -> Arc<Self> {
        let persistence_enabled = resolve_persistence_enabled(
            topology.persistence_enabled_const,
            operator_override,
        );
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
                        partition_index: 0,
                        persistence_enabled,
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
                            partition_index: idx,
                            persistence_enabled,
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
                persistence_enabled,
            }
        })
    }

    /// Read the resolved persistence flag for this instance. Frozen at
    /// construction; only changes on the next stop+start cycle.
    pub fn persistence_enabled(&self) -> bool {
        self.persistence_enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::ChannelBuilder;
    use crate::partition::restore::RestoreInitPayload;
    use crate::relay::builder::build_topology_with_persistence;
    use crate::relay::Relay;
    use crate::shard::change::NoChange;
    use crate::shard::clock::WallClock;
    use crate::shard::ctx::ShardCtx;
    use crate::shard::error::ShardError;
    use crate::shard::state_handle::StateHandle;
    use crate::shard::Shard;

    #[derive(Default)]
    struct DummyShard;
    impl Shard for DummyShard {
        type Input = String;
        type Output = ();
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "dummy_shard_persistence_test"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut ShardCtx<'_>,
        ) -> Result<(), ShardError> {
            Ok(())
        }
    }

    struct DummyRelay;
    impl Relay for DummyRelay {
        fn factory_id() -> &'static str {
            "dummy_relay_persistence_test"
        }
        fn channels(c: &mut ChannelBuilder) {
            c.input::<String>("input").into::<DummyShard>();
        }
    }

    #[test]
    fn persistence_flag_frozen_at_construction_with_const_default() {
        let mut cb = ChannelBuilder::new();
        DummyRelay::channels(&mut cb);
        let topology = build_topology_with_persistence(
            DummyRelay::factory_id(),
            cb,
            DummyRelay::PERSISTENCE_ENABLED,
        )
        .expect("topology");
        let pool = Arc::new(WorkStealingPool::with_size(1));
        let instance = RelayInstance::new(topology, Arc::from("instance"), pool);
        assert!(instance.persistence_enabled());
    }

    #[test]
    fn operator_override_wins_over_const() {
        let mut cb = ChannelBuilder::new();
        DummyRelay::channels(&mut cb);
        let topology = build_topology_with_persistence(
            DummyRelay::factory_id(),
            cb,
            DummyRelay::PERSISTENCE_ENABLED,
        )
        .expect("topology");
        let pool = Arc::new(WorkStealingPool::with_size(1));
        let instance = RelayInstance::with_persistence_override(
            topology,
            Arc::from("instance"),
            pool,
            Some(false),
        );
        assert!(!instance.persistence_enabled());
        // Every PartitionSlot inherits the resolved value.
        for shard_partitions in &instance.partitions {
            for slot in shard_partitions {
                assert!(!slot.persistence_enabled);
            }
        }
    }
}
