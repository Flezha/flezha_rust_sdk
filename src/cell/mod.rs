//! The typed halves of the Cell layer; the kernel — `CellSlot`, the
//! mailbox/drain gates, the dispatcher trait — lives in `flz_core::cell`
//! and is re-exported here. Slot construction is the typed dispatcher's
//! (`operative::dispatcher::build_cell_slot`).

pub mod flusher;
pub mod header;
pub mod restore;
pub mod timer;

pub use flz_core::cell::*;
pub use flz_core::{correction, credit, dynamic, park, retry, state_sink};
