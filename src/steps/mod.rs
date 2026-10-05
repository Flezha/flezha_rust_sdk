//! The steps run's authoring surface. The run, saga machinery and spec kernel
//! live in `flz_core::steps` and are re-exported here; a class runs in steps
//! by declaring `Run::Steps` in its settings (`crate::relay::Relay`).

pub use flz_core::steps::*;
pub mod spec;

#[cfg(test)]
mod tests;
