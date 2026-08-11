//! Re-export shim: the runner kernel lives in `flz_core`; the typed layer's
//! poll workers live in `operative::poll_pool`.

pub(crate) use flz_core::runner::*;
pub(crate) use flz_core::{poll_core, work_stealing_pool};

#[cfg(test)]
mod fast_tests;
#[cfg(test)]
pub(crate) mod poll_core_tests;
