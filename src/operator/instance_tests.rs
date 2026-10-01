//! Relocated kernel tests: `OperatorInstance` construction with typed fixtures.

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flz_core::operator::instance::*;

    use crate::channel::ChannelBuilder;
    use crate::operator::builder::build_topology_with_settings;
    use crate::operator::Operator;
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

    struct DummyOperator;
    impl Operator for DummyOperator {
        fn factory_id() -> &'static str {
            "dummy_operator_persistence_test"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("input").into::<DummyTasklet>();
        }
}

    #[test]
    fn persistence_flag_frozen_at_construction_with_const_default() {
        let cb = ChannelBuilder::new();
        DummyOperator::channels(&cb);
        let topology = build_topology_with_settings(
            DummyOperator::factory_id(),
            cb,
            &DummyOperator::settings(),
        )
        .expect("topology");
        let instance = OperatorInstance::new(topology, Arc::from("instance"));
        assert!(instance.keeps_state());
    }

    #[test]
    fn user_override_wins_over_const() {
        let cb = ChannelBuilder::new();
        DummyOperator::channels(&cb);
        let topology = build_topology_with_settings(
            DummyOperator::factory_id(),
            cb,
            &DummyOperator::settings(),
        )
        .expect("topology");
        let instance = OperatorInstance::with_options(
            topology,
            Arc::from("instance"),
            Some(false),
            flz_core::operator::instance::KeyGroupRange::UNPARTITIONED,
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
