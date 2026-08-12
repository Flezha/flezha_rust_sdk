//! Relocated kernel tests: `flz_core::fast` seams driven with typed fixtures.

#[cfg(test)]
mod remove_factory_tests {
    //! The live-instance refusal at the coordinator seam
    //! (`RunnerNode::remove_factory`): a factory with live instances refuses
    //! and stays registered; once the last instance stops, the removal
    //! unregisters the class.
    use flz_core::fast::*;
    use flz_core::restore::RestoreInitPayload;

    use crate::procedure::state_handle::StateHandle;
    use crate::procedure::{NoChange, Procedure, ProcedureCtx, ProcedureError, WallClock};
    use crate::{ChannelBuilder, UnboundedOperative};

    #[derive(Default)]
    struct SeamProc;
    impl Procedure for SeamProc {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "remove_factory_seam_op"
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

    struct SeamOperative;
    impl UnboundedOperative for SeamOperative {
        fn factory_id() -> &'static str {
            "remove_factory_seam_probe"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<SeamProc>();
        }
    }

    #[test]
    fn removal_refuses_while_instances_live_and_unregisters_after() {
        let runner = RunnerNode::new("seam_ns".to_string());
        crate::register_unbounded_class::<SeamOperative>().expect("valid topology");

        runner
            .start_operative(
                "remove_factory_seam_probe".to_string(),
                "seam-inst-1".to_string(),
            )
            .expect("instance spawns");

        assert_eq!(
            runner.remove_factory("remove_factory_seam_probe", false),
            Err(1),
            "a live instance must refuse the removal with its count"
        );
        assert!(
            flz_core::operative::registry::get_class("remove_factory_seam_probe").is_some(),
            "a refused removal must leave the type registered"
        );

        assert!(runner.stop_operative("seam-inst-1"));
        assert_eq!(
            runner.remove_factory("remove_factory_seam_probe", false),
            Ok(())
        );
        assert!(
            flz_core::operative::registry::get_class("remove_factory_seam_probe").is_none(),
            "the removal must unregister the class"
        );
        // Idempotent on an unknown id (registry contract).
        assert_eq!(
            runner.remove_factory("remove_factory_seam_probe", false),
            Ok(())
        );
    }
}

#[cfg(test)]
mod reconnect_tests {
    //! Exercises the reconnect-preservation contract: `runner_id` and the
    //! in-memory `operative_registry` survive `handle_reconnect`'s status flap
    //! (the May 25 silent-freeze regression cleared them). The reconnect
    //! re-presents the runner's stable id via the connect handshake — there
    //! is no separate reconnect frame — so the controller resolves it as a
    //! binding swap; that controller-side path is covered by the Elixir suite
    //! and the cross-language matrix.
    use std::sync::Arc;

    use flz_core::fast::*;
    use flz_core::runner::RunnerStatus;

    #[test]
    fn runner_id_and_registry_are_preserved_across_status_flap() {
        let mut runner = RunnerNode::new("ns".to_string());
        runner.assign_runner_id("e-abc".to_string());
        // RunnerNode has no public `operative_registry.len()` so we drive
        // the state-preservation assertion through the public surface
        // that handle_reconnect uses: the runner_id and the
        // shared_io_streams handle pointer should be stable across the
        // status flap handle_reconnect performs.
        let original_id = runner.get_runner_id();
        let streams_before = Arc::as_ptr(&runner.get_io_streams());

        // What handle_reconnect actually does to the runner state:
        runner.set_status(RunnerStatus::Stopped);
        runner.set_status(RunnerStatus::Running);

        assert_eq!(runner.get_runner_id(), original_id);
        let streams_after = Arc::as_ptr(&runner.get_io_streams());
        assert_eq!(
            streams_before, streams_after,
            "shared_io_streams handle must survive handle_reconnect"
        );
    }
}
