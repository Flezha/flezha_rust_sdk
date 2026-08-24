//! The authoring view of a schema migration: an author bumps
//! `SCHEMA_VERSION`, declares how far back they read, and proves the
//! migration against bytes the previous version wrote — without a controller.

use flezha_runner::entity::header::header_template_for;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{Change, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::testing::ProcedureTestHarness;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
struct BasketV1 {
    items: u32,
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
struct Basket {
    items: u32,
    currency: String,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct AddItems(u32);
impl Change for AddItems {}

/// The deployed v1, kept only to write the fixture bytes this test restores
/// from — in a real repo those bytes are already on disk.
#[derive(Default)]
struct BasketV1Procedure;
impl Procedure for BasketV1Procedure {
    type Input = String;
    type Output = String;
    type State = BasketV1;
    type Change = AddItems;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "basket"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> BasketV1 {
        BasketV1::default()
    }
    fn apply_change(state: &mut BasketV1, change: &AddItems) {
        state.items += change.0;
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

/// v2: one field added, the version bumped, and the floor lowered to 1 with a
/// migration to back the claim.
#[derive(Default)]
struct BasketProcedure;
impl Procedure for BasketProcedure {
    type Input = String;
    type Output = String;
    type State = Basket;
    type Change = AddItems;
    type Clock = WallClock;
    type Config = ();
    const SCHEMA_VERSION: u32 = 2;
    const MIN_READABLE_SCHEMA_VERSION: u32 = 1;
    fn name() -> &'static str {
        "basket"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> Basket {
        Basket::default()
    }
    fn apply_change(state: &mut Basket, change: &AddItems) {
        state.items += change.0;
    }
    fn migrate_state(from_version: u32, bytes: &[u8]) -> Result<Basket, String> {
        match from_version {
            1 => {
                let old: BasketV1 = postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
                Ok(Basket {
                    items: old.items,
                    currency: "EUR".to_string(),
                })
            }
            v => Err(format!("no migration from {v}")),
        }
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

/// fails when: an author cannot exercise their own migration without standing
/// up a controller — which is how a migration ships untested.
#[test]
fn a_migration_is_provable_at_the_authoring_seam() {
    let body = postcard::to_allocvec(&BasketV1 { items: 3 }).expect("encode v1 state");
    let header = header_template_for::<BasketV1Procedure>().stamp(&body);

    let harness = ProcedureTestHarness::<BasketProcedure>::with_payload(
        None,
        RestoreInitPayload {
            snapshot_header: &header,
            snapshot_bytes: &body,
            change_log_bytes: &[],
        },
    )
    .expect("v1 state restores through the declared migration");

    assert_eq!(
        harness.state(),
        &Basket {
            items: 3,
            currency: "EUR".to_string()
        }
    );
}
