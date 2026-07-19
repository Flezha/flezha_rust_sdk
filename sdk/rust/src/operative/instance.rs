//! `UnboundedInstance`: per-`AddOperative` runtime state.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;

use crate::channel::builder::BuildSegmentArgs;
use crate::operative::persistence::resolve_persistence_enabled;
use crate::operative::topology::{OperativeTopology, SegmentingMode};
use crate::runner::work_stealing_pool::WorkStealingPool;
use crate::segment::dynamic::{spawn_reaper, DynamicSegmentRegistry};
use crate::segment::SegmentSlot;

pub struct UnboundedInstance {
    pub topology: Arc<OperativeTopology>,
    pub instance_id: Arc<str>,
    /// Indexed by `OperationId`. Each entry is a Vec of segments for that
    /// Operation. Single-mode operations have one entry; static-mode operations have
    /// `operation_keys().len()` entries; dynamic-mode operations have none (their
    /// Segments live in `dynamic_registries`).
    pub segments: Vec<Vec<Arc<SegmentSlot>>>,
    /// Indexed by `OperationId`. Maps `&'static str` segment keys to the
    /// segment index. Empty for single- and dynamic-mode operations.
    pub segment_key_to_idx: Vec<HashMap<&'static str, usize>>,
    /// Indexed by `OperationId`. `Some` only for dynamic-mode operations; holds
    /// the lazily-populated segment map + reaper-facing counters.
    pub dynamic_registries: Vec<Option<Arc<DynamicSegmentRegistry>>>,
    /// Stop flag + join handles for this instance's reaper threads (one per
    /// dynamic Operation with a TTL). Driven by [`Self::shutdown_reapers`] / `Drop`.
    reaper_stop: Arc<AtomicBool>,
    reaper_handles: Mutex<Vec<JoinHandle<()>>>,
    /// Frozen-at-instance-start persistence flag. Resolved from the
    /// UnboundedOperative class const + runner override; propagated to every
    /// `SegmentSlot`. Subsequent runner config changes do not
    /// affect this instance — only `stop_operative` + `start_operative` cycles
    /// pick up new values.
    persistence_enabled: bool,
}

impl UnboundedInstance {
    /// Construct a fresh `UnboundedInstance` with no runner persistence
    /// override. Equivalent to
    /// `UnboundedInstance::with_persistence_override(.., None)`.
    pub fn new(
        topology: Arc<OperativeTopology>,
        instance_id: Arc<str>,
        pool: Arc<WorkStealingPool>,
    ) -> Arc<Self> {
        Self::with_persistence_override(topology, instance_id, pool, None)
    }

    /// Construct a fresh `UnboundedInstance` with an explicit runner
    /// override for `operative_persistence.<OperativeClass>`. The runner
    /// override (when `Some`) wins over the class const.
    pub fn with_persistence_override(
        topology: Arc<OperativeTopology>,
        instance_id: Arc<str>,
        pool: Arc<WorkStealingPool>,
        operator_override: Option<bool>,
    ) -> Arc<Self> {
        let persistence_enabled =
            resolve_persistence_enabled(topology.persistence_enabled_const, operator_override);
        Arc::new_cyclic(|weak: &Weak<UnboundedInstance>| {
            let operation_count = topology.operations.len();
            let mut segments: Vec<Vec<Arc<SegmentSlot>>> = Vec::with_capacity(operation_count);
            let mut segment_key_to_idx: Vec<HashMap<&'static str, usize>> =
                Vec::with_capacity(operation_count);
            let mut dynamic_registries: Vec<Option<Arc<DynamicSegmentRegistry>>> =
                Vec::with_capacity(operation_count);

            for (operation_id, operation_desc) in topology.operations.iter().enumerate() {
                let operation_id_u32 = operation_id as u32;
                if operation_desc.mode == SegmentingMode::Dynamic {
                    // Dynamic operations own zero Segments at startup; the
                    // registry allocates them lazily on first key.
                    segments.push(Vec::new());
                    segment_key_to_idx.push(HashMap::new());
                    dynamic_registries.push(Some(Arc::new(DynamicSegmentRegistry::new(
                        operation_id_u32,
                        operation_desc.max_segments,
                        Arc::clone(&topology),
                        Weak::clone(weak),
                        Arc::clone(&instance_id),
                        Arc::clone(&pool),
                        persistence_enabled,
                    ))));
                } else if operation_desc.segment_keys.is_empty() {
                    let slot = (operation_desc.build_segment)(BuildSegmentArgs {
                        topology: Arc::clone(&topology),
                        instance: Weak::clone(weak),
                        instance_id: Arc::clone(&instance_id),
                        operation_id: operation_id_u32,
                        operation_key: None,
                        segment_index: 0,
                        persistence_enabled,
                        pool: Arc::clone(&pool),
                    });
                    segments.push(vec![slot]);
                    segment_key_to_idx.push(HashMap::new());
                    dynamic_registries.push(None);
                } else {
                    let mut slots = Vec::with_capacity(operation_desc.segment_keys.len());
                    let mut key_map = HashMap::with_capacity(operation_desc.segment_keys.len());
                    for (idx, key) in operation_desc.segment_keys.iter().enumerate() {
                        let slot = (operation_desc.build_segment)(BuildSegmentArgs {
                            topology: Arc::clone(&topology),
                            instance: Weak::clone(weak),
                            instance_id: Arc::clone(&instance_id),
                            operation_id: operation_id_u32,
                            operation_key: Some(key),
                            segment_index: idx,
                            persistence_enabled,
                            pool: Arc::clone(&pool),
                        });
                        slots.push(slot);
                        key_map.insert(*key, idx);
                    }
                    segments.push(slots);
                    segment_key_to_idx.push(key_map);
                    dynamic_registries.push(None);
                }
            }

            // One reaper per dynamic Operation that opts into a TTL. Default
            // `idle_ttl == None` spawns none — dynamic Segments then live
            // as long as the UnboundedOperative, like static ones.
            let reaper_stop = Arc::new(AtomicBool::new(false));
            let mut reaper_handles: Vec<JoinHandle<()>> = Vec::new();
            for (operation_id, registry) in dynamic_registries.iter().enumerate() {
                if let (Some(registry), Some(idle_ttl)) =
                    (registry, topology.operations[operation_id].idle_ttl)
                {
                    reaper_handles.push(spawn_reaper(
                        idle_ttl,
                        Arc::clone(registry),
                        Arc::clone(&reaper_stop),
                    ));
                }
            }

            UnboundedInstance {
                topology,
                instance_id,
                segments,
                segment_key_to_idx,
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
    /// `RunnerNode::stop_operative`; idempotent and a no-op for non-dynamic
    /// instances (no reapers).
    pub fn shutdown_reapers(&self) {
        self.reaper_stop
            .store(true, std::sync::atomic::Ordering::Release);
        let handles =
            std::mem::take(&mut *self.reaper_handles.lock().expect("reaper handles poisoned"));
        for handle in handles {
            let _ = handle.join();
        }
    }
}

impl Drop for UnboundedInstance {
    fn drop(&mut self) {
        // Safety net for an instance dropped without `stop_operative`: signal the
        // reapers (don't join — Drop must not block on a sweep interval) so
        // each thread and its `Arc<DynamicSegmentRegistry>` wind down.
        self.reaper_stop
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::ChannelBuilder;
    use crate::operation::change::NoChange;
    use crate::operation::clock::WallClock;
    use crate::operation::ctx::OperationCtx;
    use crate::operation::error::OperationError;
    use crate::operation::state_handle::StateHandle;
    use crate::operation::Operation;
    use crate::operative::builder::build_topology_with_persistence;
    use crate::operative::UnboundedOperative;
    use crate::segment::restore::RestoreInitPayload;

    #[derive(Default)]
    struct DummyOperation;
    impl Operation for DummyOperation {
        type Input = String;
        type Output = ();
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "dummy_operation_persistence_test"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            Ok(())
        }
    }

    struct DummyOperative;
    impl UnboundedOperative for DummyOperative {
        fn factory_id() -> &'static str {
            "dummy_operative_persistence_test"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("input").into::<DummyOperation>();
        }
    }

    #[test]
    fn persistence_flag_frozen_at_construction_with_const_default() {
        let cb = ChannelBuilder::new();
        DummyOperative::channels(&cb);
        let topology = build_topology_with_persistence(
            DummyOperative::factory_id(),
            cb,
            DummyOperative::PERSISTENCE_ENABLED,
        )
        .expect("topology");
        let pool = Arc::new(WorkStealingPool::with_size(1));
        let instance = UnboundedInstance::new(topology, Arc::from("instance"), pool);
        assert!(instance.persistence_enabled());
    }

    #[test]
    fn operator_override_wins_over_const() {
        let cb = ChannelBuilder::new();
        DummyOperative::channels(&cb);
        let topology = build_topology_with_persistence(
            DummyOperative::factory_id(),
            cb,
            DummyOperative::PERSISTENCE_ENABLED,
        )
        .expect("topology");
        let pool = Arc::new(WorkStealingPool::with_size(1));
        let instance = UnboundedInstance::with_persistence_override(
            topology,
            Arc::from("instance"),
            pool,
            Some(false),
        );
        assert!(!instance.persistence_enabled());
        // Every SegmentSlot inherits the resolved value.
        for operation_segments in &instance.segments {
            for slot in operation_segments {
                assert!(!slot.persistence_enabled);
            }
        }
    }
}
