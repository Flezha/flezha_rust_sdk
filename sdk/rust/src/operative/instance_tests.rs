//! Relocated kernel tests: `UnboundedInstance` construction with typed fixtures.

#[cfg(test)]
mod tests {
    use flz_core::operative::instance::*;

    use crate::channel::ChannelBuilder;
    use crate::operative::builder::build_topology_with_persistence;
    use crate::operative::UnboundedOperative;
    use crate::procedure::change::NoChange;
    use crate::procedure::clock::WallClock;
    use crate::procedure::ctx::ProcedureCtx;
    use crate::procedure::error::ProcedureError;
    use crate::procedure::state_handle::StateHandle;
    use crate::procedure::Procedure;
    use flz_core::restore::RestoreInitPayload;
    use flz_core::work_stealing_pool::WorkStealingPool;
    use std::sync::Arc;

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
