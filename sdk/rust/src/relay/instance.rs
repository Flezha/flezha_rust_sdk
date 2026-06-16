//! `RelayInstance`: per-`AddRelay` runtime state.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;

use crate::channel::builder::BuildPartitionArgs;
use crate::executor::work_stealing_pool::WorkStealingPool;
use crate::partition::dynamic::{spawn_reaper, DynamicPartitionRegistry};
use crate::partition::PartitionSlot;
use crate::relay::persistence::resolve_persistence_enabled;
use crate::relay::topology::{PartitioningMode, RelayTopology};

pub struct RelayInstance {
    pub topology: Arc<RelayTopology>,
    pub instance_id: Arc<str>,
    /// Indexed by `ShardId`. Each entry is a Vec of partitions for that
    /// Shard. Single-mode shards have one entry; static-mode shards have
    /// `shard_keys().len()` entries; dynamic-mode shards have none (their
    /// Partitions live in `dynamic_registries`).
    pub partitions: Vec<Vec<Arc<PartitionSlot>>>,
    /// Indexed by `ShardId`. Maps `&'static str` partition keys to the
    /// partition index. Empty for single- and dynamic-mode shards.
    pub partition_key_to_idx: Vec<HashMap<&'static str, usize>>,
    /// Indexed by `ShardId`. `Some` only for dynamic-mode shards; holds
    /// the lazily-populated partition map + reaper-facing counters.
    pub dynamic_registries: Vec<Option<Arc<DynamicPartitionRegistry>>>,
    /// Stop flag + join handles for this instance's reaper threads (one per
    /// dynamic Shard with a TTL). Driven by [`Self::shutdown_reapers`] / `Drop`.
    reaper_stop: Arc<AtomicBool>,
    reaper_handles: Mutex<Vec<JoinHandle<()>>>,
    /// Frozen-at-instance-start persistence flag. Resolved from the
    /// Relay class const + executor override; propagated to every
    /// `PartitionSlot`. Subsequent executor config changes do not
    /// affect this instance — only `stop_relay` + `start_relay` cycles
    /// pick up new values.
    persistence_enabled: bool,
}

impl RelayInstance {
    /// Construct a fresh `RelayInstance` with no executor persistence
    /// override. Equivalent to
    /// `RelayInstance::with_persistence_override(.., None)`.
    pub fn new(
        topology: Arc<RelayTopology>,
        instance_id: Arc<str>,
        pool: Arc<WorkStealingPool>,
    ) -> Arc<Self> {
        Self::with_persistence_override(topology, instance_id, pool, None)
    }

    /// Construct a fresh `RelayInstance` with an explicit executor
    /// override for `relay_persistence.<RelayClass>`. The executor
    /// override (when `Some`) wins over the class const.
    pub fn with_persistence_override(
        topology: Arc<RelayTopology>,
        instance_id: Arc<str>,
        pool: Arc<WorkStealingPool>,
        operator_override: Option<bool>,
    ) -> Arc<Self> {
        let persistence_enabled =
            resolve_persistence_enabled(topology.persistence_enabled_const, operator_override);
        Arc::new_cyclic(|weak: &Weak<RelayInstance>| {
            let shard_count = topology.shards.len();
            let mut partitions: Vec<Vec<Arc<PartitionSlot>>> = Vec::with_capacity(shard_count);
            let mut partition_key_to_idx: Vec<HashMap<&'static str, usize>> =
                Vec::with_capacity(shard_count);
            let mut dynamic_registries: Vec<Option<Arc<DynamicPartitionRegistry>>> =
                Vec::with_capacity(shard_count);

            for (shard_id, shard_desc) in topology.shards.iter().enumerate() {
                let shard_id_u32 = shard_id as u32;
                if shard_desc.mode == PartitioningMode::Dynamic {
                    // Dynamic shards own zero Partitions at startup; the
                    // registry allocates them lazily on first key.
                    partitions.push(Vec::new());
                    partition_key_to_idx.push(HashMap::new());
                    dynamic_registries.push(Some(Arc::new(DynamicPartitionRegistry::new(
                        shard_id_u32,
                        shard_desc.max_partitions,
                        Arc::clone(&topology),
                        Weak::clone(weak),
                        Arc::clone(&instance_id),
                        Arc::clone(&pool),
                        persistence_enabled,
                    ))));
                } else if shard_desc.partition_keys.is_empty() {
                    let slot = (shard_desc.build_partition)(BuildPartitionArgs {
                        topology: Arc::clone(&topology),
                        instance: Weak::clone(weak),
                        instance_id: Arc::clone(&instance_id),
                        shard_id: shard_id_u32,
                        shard_key: None,
                        partition_index: 0,
                        persistence_enabled,
                        pool: Arc::clone(&pool),
                    });
                    partitions.push(vec![slot]);
                    partition_key_to_idx.push(HashMap::new());
                    dynamic_registries.push(None);
                } else {
                    let mut slots = Vec::with_capacity(shard_desc.partition_keys.len());
                    let mut key_map = HashMap::with_capacity(shard_desc.partition_keys.len());
                    for (idx, key) in shard_desc.partition_keys.iter().enumerate() {
                        let slot = (shard_desc.build_partition)(BuildPartitionArgs {
                            topology: Arc::clone(&topology),
                            instance: Weak::clone(weak),
                            instance_id: Arc::clone(&instance_id),
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
                    dynamic_registries.push(None);
                }
            }

            // One reaper per dynamic Shard that opts into a TTL. Default
            // `idle_ttl == None` spawns none — dynamic Partitions then live
            // as long as the Relay, like static ones.
            let reaper_stop = Arc::new(AtomicBool::new(false));
            let mut reaper_handles: Vec<JoinHandle<()>> = Vec::new();
            for (shard_id, registry) in dynamic_registries.iter().enumerate() {
                if let (Some(registry), Some(idle_ttl)) =
                    (registry, topology.shards[shard_id].idle_ttl)
                {
                    reaper_handles.push(spawn_reaper(
                        idle_ttl,
                        Arc::clone(registry),
                        Arc::clone(&reaper_stop),
                    ));
                }
            }

            RelayInstance {
                topology,
                instance_id,
                partitions,
                partition_key_to_idx,
                dynamic_registries,
                reaper_stop,
                reaper_handles: Mutex::new(reaper_handles),
                persistence_enabled,
            }
        })
    }

    /// Read the resolved persistence flag for this instance. Frozen at
    /// construction; only changes on the next stop+start cycle.
    pub fn persistence_enabled(&self) -> bool {
        self.persistence_enabled
    }

    /// Stop and join this instance's reaper threads. Called by
    /// `ExecutorNode::stop_relay`; idempotent and a no-op for non-dynamic
    /// instances (no reapers).
    pub fn shutdown_reapers(&self) {
        self.reaper_stop.store(true, std::sync::atomic::Ordering::Release);
        let handles = std::mem::take(&mut *self.reaper_handles.lock().expect("reaper handles poisoned"));
        for handle in handles {
            let _ = handle.join();
        }
    }
}

impl Drop for RelayInstance {
    fn drop(&mut self) {
        // Safety net for an instance dropped without `stop_relay`: signal the
        // reapers (don't join — Drop must not block on a sweep interval) so
        // each thread and its `Arc<DynamicPartitionRegistry>` wind down.
        self.reaper_stop.store(true, std::sync::atomic::Ordering::Release);
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
