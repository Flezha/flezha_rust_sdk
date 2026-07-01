//! Section 2.6: assert that the trait's default `compact_after_bytes`
//! and `SCHEMA_VERSION` values match the umbrella's per-Operation contract.

use crate::operation::change::NoChange;
use crate::operation::clock::WallClock;
use crate::operation::ctx::OperationCtx;
use crate::operation::error::OperationError;
use crate::operation::state_handle::StateHandle;
use crate::operation::Operation;
use crate::segment::restore::RestoreInitPayload;

#[derive(Default)]
struct DefaultsOperation;

impl Operation for DefaultsOperation {
    type Input = ();
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "defaults_operation"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

    fn run(
        &mut self,
        _: (),
        _state: StateHandle<'_, Self>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        Ok(())
    }
}

#[test]
fn compact_after_bytes_default_is_64_mib() {
    assert_eq!(DefaultsOperation::compact_after_bytes(), 64 * 1024 * 1024);
}

#[test]
fn schema_version_default_is_one() {
    assert_eq!(DefaultsOperation::SCHEMA_VERSION, 1);
}
