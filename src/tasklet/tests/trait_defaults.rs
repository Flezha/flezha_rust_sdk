//! Section 2.6: assert that the trait's default `compact_after_bytes`
//! and `SCHEMA_VERSION` values match the umbrella's per-Tasklet contract.

use crate::tasklet::{Ctx, Tasklet, TaskletError};

#[derive(Default)]
struct DefaultsTasklet;

#[flezha_runner::tasklet(name = "defaults_tasklet")]
impl Tasklet for DefaultsTasklet {
    type Input = ();

    fn run(&mut self, _: (), _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
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

    #[flezha_runner::tasklet(name = "overriding_tasklet", compact_after_bytes = 16 * 1024 * 1024)]
    impl Tasklet for OverridingTasklet {
        type Input = ();

        fn run(&mut self, _: (), _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
            Ok(())
        }
    }

    assert_eq!(OverridingTasklet::compact_after_bytes(), 16 * 1024 * 1024);
}

#[test]
fn schema_version_default_is_one() {
    assert_eq!(DefaultsTasklet::SCHEMA_VERSION, 1);
}
