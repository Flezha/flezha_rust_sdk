//! `UnboundedInstance`: per-`AddOperative` runtime state.

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;

use crate::channel::builder::BuildEntityArgs;
use crate::entity::dynamic::{spawn_reaper, DynamicEntityRegistry};
use crate::entity::timer::{spawn_wheel, TimerWheel};
use crate::entity::EntitySlot;
use crate::operative::persistence::resolve_persistence_enabled;
use crate::operative::topology::{EntityMode, OperativeTopology};
use crate::runner::work_stealing_pool::WorkStealingPool;

pub struct UnboundedInstance {
    pub topology: Arc<OperativeTopology>,
    pub instance_id: Arc<str>,
    /// Indexed by `ProcedureId`. Each entry is a Vec of entities for that
    /// Procedure. Single-mode procedures have one entry; static-mode procedures have
    /// `entity_keys().len()` entries; dynamic-mode procedures have none (their
    /// Entities live in `dynamic_registries`).
    pub entities: Vec<Vec<Arc<EntitySlot>>>,
    /// Indexed by `ProcedureId`. Maps `&'static str` entity keys to the
    /// entity index. Empty for single- and dynamic-mode procedures.
    pub entity_key_to_idx: Vec<HashMap<&'static str, usize>>,
    /// Indexed by `ProcedureId`. `Some` only for dynamic-mode procedures; holds
    /// the lazily-populated entity map + reaper-facing counters.
    pub dynamic_registries: Vec<Option<Arc<DynamicEntityRegistry>>>,
    /// Stop flag + join handles for this instance's background threads: one
    /// reaper per dynamic Procedure with a TTL, plus the timer wheel once
    /// something arms a processing-time timer. Driven by
    /// [`Self::shutdown_background_threads`] / `Drop`.
    background_stop: Arc<AtomicBool>,
    background_handles: Mutex<Vec<JoinHandle<()>>>,
    /// Processing-time timer wheel, created on the first registration. An
    /// Operative that never uses one never pays for a thread.
    timer_wheel: OnceLock<Arc<TimerWheel>>,
    /// Frozen-at-instance-start persistence flag. Resolved from the
    /// UnboundedOperative class const + runner override; propagated to every
    /// `EntitySlot`. Subsequent runner config changes do not
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
            let procedure_count = topology.procedures.len();
            let mut entities: Vec<Vec<Arc<EntitySlot>>> = Vec::with_capacity(procedure_count);
            let mut entity_key_to_idx: Vec<HashMap<&'static str, usize>> =
                Vec::with_capacity(procedure_count);
            let mut dynamic_registries: Vec<Option<Arc<DynamicEntityRegistry>>> =
                Vec::with_capacity(procedure_count);

            for (procedure_id, procedure_desc) in topology.procedures.iter().enumerate() {
                let procedure_id_u32 = procedure_id as u32;
                if procedure_desc.mode == EntityMode::Dynamic {
                    // Dynamic procedures own zero Entities at startup; the
                    // registry allocates them lazily on first key.
                    entities.push(Vec::new());
                    entity_key_to_idx.push(HashMap::new());
                    dynamic_registries.push(Some(Arc::new(DynamicEntityRegistry::new(
                        procedure_id_u32,
                        procedure_desc.max_entities,
                        Arc::clone(&topology),
                        Weak::clone(weak),
                        Arc::clone(&instance_id),
                        Arc::clone(&pool),
                        persistence_enabled,
                    ))));
                } else if procedure_desc.entity_keys.is_empty() {
                    let slot = (procedure_desc.build_entity)(BuildEntityArgs {
                        topology: Arc::clone(&topology),
                        instance: Weak::clone(weak),
                        instance_id: Arc::clone(&instance_id),
                        procedure_id: procedure_id_u32,
                        entity_key: None,
                        entity_index: 0,
                        persistence_enabled,
                        pool: Arc::clone(&pool),
                    });
                    entities.push(vec![slot]);
                    entity_key_to_idx.push(HashMap::new());
                    dynamic_registries.push(None);
                } else {
                    let mut slots = Vec::with_capacity(procedure_desc.entity_keys.len());
                    let mut key_map = HashMap::with_capacity(procedure_desc.entity_keys.len());
                    for (idx, key) in procedure_desc.entity_keys.iter().enumerate() {
                        let slot = (procedure_desc.build_entity)(BuildEntityArgs {
                            topology: Arc::clone(&topology),
                            instance: Weak::clone(weak),
                            instance_id: Arc::clone(&instance_id),
                            procedure_id: procedure_id_u32,
                            entity_key: Some(key),
                            entity_index: idx,
                            persistence_enabled,
                            pool: Arc::clone(&pool),
                        });
                        slots.push(slot);
                        key_map.insert(*key, idx);
                    }
                    entities.push(slots);
                    entity_key_to_idx.push(key_map);
                    dynamic_registries.push(None);
                }
            }

            // One reaper per dynamic Procedure that opts into a TTL. Default
            // `idle_ttl == None` spawns none — dynamic Entities then live
            // as long as the UnboundedOperative, like static ones.
            let background_stop = Arc::new(AtomicBool::new(false));
            let mut background_handles: Vec<JoinHandle<()>> = Vec::new();
            for (procedure_id, registry) in dynamic_registries.iter().enumerate() {
                if let (Some(registry), Some(idle_ttl)) =
                    (registry, topology.procedures[procedure_id].idle_ttl)
                {
                    background_handles.push(spawn_reaper(
                        idle_ttl,
                        Arc::clone(registry),
                        Arc::clone(&background_stop),
                    ));
                }
            }

            UnboundedInstance {
                topology,
                instance_id,
                entities,
                entity_key_to_idx,
                dynamic_registries,
                background_stop,
                background_handles: Mutex::new(background_handles),
                timer_wheel: OnceLock::new(),
                persistence_enabled,
            }
        })
    }

    /// Read the resolved persistence flag for this instance. Frozen at
    /// construction; only changes on the next stop+start cycle.
    pub fn persistence_enabled(&self) -> bool {
        self.persistence_enabled
    }

    /// This instance's processing-time timer wheel, spawning its thread on the
    /// first call. One wheel per Operative: it keys entries by
    /// `(procedure, entity_key, scheduled_at)`, so one thread serves every
    /// Entity the instance owns.
    pub fn timer_wheel(&self) -> Arc<TimerWheel> {
        Arc::clone(self.timer_wheel.get_or_init(|| {
            let wheel = TimerWheel::new();
            self.background_handles
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(spawn_wheel(Arc::clone(&wheel)));
            wheel
        }))
    }

    /// The wheel if one exists. For callers that only ever REMOVE entries —
    /// creating a wheel (and its thread) to drop timers that cannot exist
    /// would be backwards.
    pub fn timer_wheel_if_started(&self) -> Option<&Arc<TimerWheel>> {
        self.timer_wheel.get()
    }

    /// Stop and join this instance's background threads (reapers + the timer
    /// wheel). Called by `RunnerNode::stop_operative`; idempotent and a no-op
    /// for an instance that started none.
    pub fn shutdown_background_threads(&self) {
        self.signal_background_stop();
        let handles = std::mem::take(
            &mut *self
                .background_handles
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        );
        for handle in handles {
            let _ = handle.join();
        }
    }

    /// Ask every background thread to wind down. The wheel parks on a condvar
    /// rather than a sleep, so it needs the signal as well as the flag.
    fn signal_background_stop(&self) {
        self.background_stop
            .store(true, std::sync::atomic::Ordering::Release);
        if let Some(wheel) = self.timer_wheel.get() {
            wheel.stop();
        }
    }
}

impl Drop for UnboundedInstance {
    fn drop(&mut self) {
        // Safety net for an instance dropped without `stop_operative`: signal the
        // background threads (don't join — Drop must not block on a sweep
        // interval) so each thread and the state it holds wind down.
        self.signal_background_stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel::ChannelBuilder;
    use crate::entity::restore::RestoreInitPayload;
    use crate::operative::builder::build_topology_with_persistence;
    use crate::operative::UnboundedOperative;
    use crate::procedure::change::NoChange;
    use crate::procedure::clock::WallClock;
    use crate::procedure::ctx::ProcedureCtx;
    use crate::procedure::error::ProcedureError;
    use crate::procedure::state_handle::StateHandle;
    use crate::procedure::Procedure;

    #[derive(Default)]
    struct DummyProcedure;
    impl Procedure for DummyProcedure {
        type Input = String;
        type Output = ();
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "dummy_procedure_persistence_test"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Ok(())
        }
    }

    struct DummyOperative;
    impl UnboundedOperative for DummyOperative {
        fn factory_id() -> &'static str {
            "dummy_operative_persistence_test"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("input").into::<DummyProcedure>();
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
        // Every EntitySlot inherits the resolved value.
        for procedure_entities in &instance.entities {
            for slot in procedure_entities {
                assert!(!slot.persistence_enabled);
            }
        }
    }
}
