//! The typed halves of the Cell layer; the kernel — `CellSlot`, the
//! mailbox/drain gates, the dispatcher trait — lives in `flz_core::cell`
//! and is re-exported here. Slot construction is the typed dispatcher's
//! (`relay::dispatcher::build_cell_slot`).

// The typed flush path and header binding are the runtime's own; the
// crate's tests and benches reach them under `bench-internals`.
#[cfg(feature = "bench-internals")]
pub mod flusher;
#[cfg(not(feature = "bench-internals"))]
pub(crate) mod flusher;
#[cfg(feature = "bench-internals")]
pub mod header;
#[cfg(not(feature = "bench-internals"))]
pub(crate) mod header;
pub mod restore;
pub mod timer;

pub use flz_core::cell::*;
pub use flz_core::{correction, credit, dynamic, park, retry, state_sink};
