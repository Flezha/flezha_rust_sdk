//! Relocated kernel tests: `RelayInstance` construction with typed fixtures.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flz_core::relay::instance::*;

    use crate::channel::ChannelBuilder;
    use crate::relay::builder::build_topology_with_settings;
    use crate::relay::Relay;
    use crate::tasklet::error::TaskletError;
    use crate::tasklet::{Ctx, Tasklet};

    #[derive(Default)]
    struct DummyTasklet;
    #[flezha_runner::tasklet(name = "dummy_tasklet_persistence_test")]
    impl Tasklet for DummyTasklet {
        type Input = String;
        fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
            Ok(())
        }
    }

    struct DummyRelay;
    impl Relay for DummyRelay {
        fn factory_id() -> &'static str {
            "dummy_relay_persistence_test"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("input").into::<DummyTasklet>();
        }
    }

    #[test]
    fn persistence_flag_frozen_at_construction_with_const_default() {
        let cb = ChannelBuilder::new();
        DummyRelay::channels(&cb);
        let topology =
            build_topology_with_settings(DummyRelay::factory_id(), cb, &DummyRelay::settings())
                .expect("topology");
        let instance = RelayInstance::new(topology, Arc::from("instance"));
        assert!(instance.keeps_state());
    }

    #[test]
    fn user_override_wins_over_const() {
        let cb = ChannelBuilder::new();
        DummyRelay::channels(&cb);
        let topology =
            build_topology_with_settings(DummyRelay::factory_id(), cb, &DummyRelay::settings())
                .expect("topology");
        let instance = RelayInstance::with_options(
            topology,
            Arc::from("instance"),
            Some(false),
            flz_core::relay::instance::KeyGroupRange::UNPARTITIONED,
        );
        assert!(!instance.keeps_state());
        // Every CellSlot inherits the resolved value.
        for tasklet_cells in &instance.cells {
            for slot in tasklet_cells {
                assert!(!slot.keeps_state);
            }
        }
    }
}
