//! Section 2.6: assert that the trait's default `compact_after_bytes`
//! and `SCHEMA_VERSION` values match the umbrella's per-Procedure contract.

use crate::entity::restore::RestoreInitPayload;
use crate::procedure::change::NoChange;
use crate::procedure::clock::WallClock;
use crate::procedure::ctx::ProcedureCtx;
use crate::procedure::error::ProcedureError;
use crate::procedure::state_handle::StateHandle;
use crate::procedure::Procedure;

#[derive(Default)]
struct DefaultsProcedure;

impl Procedure for DefaultsProcedure {
    type Input = ();
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "defaults_procedure"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

    fn run(
        &mut self,
        _: (),
        _state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

#[test]
fn compact_after_bytes_default_is_8_mib() {
    assert_eq!(DefaultsProcedure::compact_after_bytes(), 8 * 1024 * 1024);
}

#[test]
fn compact_after_bytes_override_wins_over_default() {
    #[derive(Default)]
    struct OverridingProcedure;

    impl Procedure for OverridingProcedure {
        type Input = ();
        type Output = ();
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();

        fn name() -> &'static str {
            "overriding_procedure"
        }

        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

        fn run(
            &mut self,
            _: (),
            _state: StateHandle<'_, Self>,
            _: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Ok(())
        }

        fn compact_after_bytes() -> usize {
            16 * 1024 * 1024
        }
    }

    assert_eq!(OverridingProcedure::compact_after_bytes(), 16 * 1024 * 1024);
}

#[test]
fn schema_version_default_is_one() {
    assert_eq!(DefaultsProcedure::SCHEMA_VERSION, 1);
}
