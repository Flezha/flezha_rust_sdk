//! The authoring view of a schema migration: an author bumps
//! `SCHEMA_VERSION`, declares how far back they read, and proves the
//! migration against bytes the previous version wrote — without a controller.

use flezha_runner::cell::header::header_template_for;
use flezha_runner::state::{Change, RestoreInitPayload};
use flezha_runner::tasklet::{Tasklet, TaskletError};
use flezha_runner::testing::TaskletTestHarness;
use flezha_runner::Ctx;
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
impl Change<BasketV1> for AddItems {
    fn apply_to(&self, state: &mut BasketV1) {
        state.items += self.0;
    }
}

impl Change<Basket> for AddItems {
    fn apply_to(&self, state: &mut Basket) {
        state.items += self.0;
    }
}

/// The deployed v1, kept only to write the fixture bytes this test restores
/// from — in a real repo those bytes are already on disk.
#[derive(Default)]
struct BasketV1Tasklet;
#[flezha_runner::tasklet(name = "basket")]
impl Tasklet for BasketV1Tasklet {
    type Input = String;
    type Output = String;
    type State = BasketV1;
    type Change = AddItems;
    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

/// v2: one field added, the version bumped, and the floor lowered to 1 with a
/// migration to back the claim.
#[derive(Default)]
struct BasketTasklet;
#[flezha_runner::tasklet(name = "basket")]
impl Tasklet for BasketTasklet {
    type Input = String;
    type Output = String;
    type State = Basket;
    type Change = AddItems;
    const SCHEMA_VERSION: u32 = 2;
    const MIN_READABLE_SCHEMA_VERSION: u32 = 1;
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
    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

/// fails when: an author cannot exercise their own migration without standing
/// up a controller — which is how a migration ships untested.
#[test]
fn a_migration_is_provable_at_the_authoring_seam() {
    let body = postcard::to_allocvec(&BasketV1 { items: 3 }).expect("encode v1 state");
    let header = header_template_for::<BasketV1Tasklet>().stamp(&body);

    let harness = TaskletTestHarness::<BasketTasklet>::with_payload(
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
