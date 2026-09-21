//! Relocated kernel tests: `StreamInstance` construction with typed fixtures.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flz_core::operative::instance::*;
    use flz_core::restore::RestoreInitPayload;

    use crate::channel::ChannelBuilder;
    use crate::operative::builder::build_topology_with_persistence;
    use crate::operative::StreamOperative;
    use crate::tasklet::change::NoChange;
    use crate::tasklet::clock::WallClock;
    use crate::tasklet::ctx::TaskletCtx;
    use crate::tasklet::error::TaskletError;
    use crate::tasklet::state_handle::StateHandle;
    use crate::tasklet::Tasklet;

    #[derive(Default)]
    struct DummyTasklet;
    impl Tasklet for DummyTasklet {
        type Input = String;
        type Output = ();
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "dummy_tasklet_persistence_test"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: String,
            _: StateHandle<'_, Self>,
            _: &mut TaskletCtx<'_>,
        ) -> Result<(), TaskletError> {
            Ok(())
        }
    }

    struct DummyOperative;
    impl StreamOperative for DummyOperative {
        fn factory_id() -> &'static str {
            "dummy_operative_persistence_test"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("input").into::<DummyTasklet>();
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
        let instance = StreamInstance::with_options(
            topology,
            Arc::from("instance"),
            Some(false),
            flz_core::operative::instance::KeyGroupRange::UNPARTITIONED,
        );
        assert!(!instance.persistence_enabled());
        // Every CellSlot inherits the resolved value.
        for tasklet_cells in &instance.cells {
            for slot in tasklet_cells {
                assert!(!slot.persistence_enabled);
            }
        }
    }
}
