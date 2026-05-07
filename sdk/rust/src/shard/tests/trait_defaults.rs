//! Section 2.6: assert that the trait's default `compact_after_bytes`
//! and `SCHEMA_VERSION` values match the umbrella's per-Shard contract.

use crate::partition::restore::RestoreInitPayload;
use crate::shard::change::NoChange;
use crate::shard::clock::WallClock;
use crate::shard::ctx::ShardCtx;
use crate::shard::error::ShardError;
use crate::shard::state_handle::StateHandle;
use crate::shard::Shard;

#[derive(Default)]
struct DefaultsShard;

impl Shard for DefaultsShard {
    type Input = ();
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "defaults_shard"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

    fn run(
        &mut self,
        _: (),
        _state: StateHandle<'_, Self>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        Ok(())
    }
}

#[test]
fn compact_after_bytes_default_is_64_mib() {
    assert_eq!(DefaultsShard::compact_after_bytes(), 64 * 1024 * 1024);
}

#[test]
fn schema_version_default_is_one() {
    assert_eq!(DefaultsShard::SCHEMA_VERSION, 1);
}
