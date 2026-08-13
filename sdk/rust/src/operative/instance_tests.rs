//! Relocated kernel tests: `StreamInstance` construction with typed fixtures.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flz_core::operative::instance::*;
    use flz_core::restore::RestoreInitPayload;

    use crate::channel::ChannelBuilder;
    use crate::operative::builder::build_topology_with_persistence;
    use crate::operative::StreamOperative;
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
    impl StreamOperative for DummyOperative {
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
        let instance = StreamInstance::new(topology, Arc::from("instance"));
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
        let instance =
            StreamInstance::with_persistence_override(topology, Arc::from("instance"), Some(false));
        assert!(!instance.persistence_enabled());
        // Every EntitySlot inherits the resolved value.
        for procedure_entities in &instance.entities {
            for slot in procedure_entities {
                assert!(!slot.persistence_enabled);
            }
        }
    }
}
