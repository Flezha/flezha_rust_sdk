//! Re-export shim: the runner kernel lives in `flz_core`; the typed layer's
//! poll workers live in `relay::dispatcher`.

pub(crate) use flz_core::poll_core;
#[cfg(test)]
pub(crate) use flz_core::runner::*;

#[cfg(test)]
mod fast_tests;
