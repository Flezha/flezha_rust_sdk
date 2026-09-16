//! The typed halves of the Entity layer; the kernel — `EntitySlot`, the
//! mailbox/drain gates, the dispatcher trait — lives in `flz_core::entity`
//! and is re-exported here. Slot construction is the typed dispatcher's
//! (`operative::dispatcher::build_entity_slot`).

pub mod flusher;
pub mod header;
pub mod restore;
pub mod timer;

pub use flz_core::entity::*;
pub use flz_core::{correction, credit, dynamic, park, retry, state_sink};
