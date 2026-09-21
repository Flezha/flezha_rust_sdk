//! Section 2.6: assert that the trait's default `compact_after_bytes`
//! and `SCHEMA_VERSION` values match the umbrella's per-Tasklet contract.

use crate::cell::restore::RestoreInitPayload;
use crate::tasklet::change::NoChange;
use crate::tasklet::clock::WallClock;
use crate::tasklet::ctx::TaskletCtx;
use crate::tasklet::error::TaskletError;
use crate::tasklet::state_handle::StateHandle;
use crate::tasklet::Tasklet;

#[derive(Default)]
struct DefaultsTasklet;

impl Tasklet for DefaultsTasklet {
    type Input = ();
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "defaults_tasklet"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

    fn run(
        &mut self,
        _: (),
        _state: StateHandle<'_, Self>,
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}

#[test]
fn compact_after_bytes_default_is_8_mib() {
    assert_eq!(DefaultsTasklet::compact_after_bytes(), 8 * 1024 * 1024);
}

#[test]
fn compact_after_bytes_override_wins_over_default() {
    #[derive(Default)]
    struct OverridingTasklet;

    impl Tasklet for OverridingTasklet {
        type Input = ();
        type Output = ();
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();

        fn name() -> &'static str {
            "overriding_tasklet"
        }

        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

        fn run(
            &mut self,
            _: (),
            _state: StateHandle<'_, Self>,
            _: &mut TaskletCtx<'_>,
        ) -> Result<(), TaskletError> {
            Ok(())
        }

        fn compact_after_bytes() -> usize {
            16 * 1024 * 1024
        }
    }

    assert_eq!(OverridingTasklet::compact_after_bytes(), 16 * 1024 * 1024);
}

#[test]
fn schema_version_default_is_one() {
    assert_eq!(DefaultsTasklet::SCHEMA_VERSION, 1);
}
