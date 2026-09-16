//! Test harness for `Procedure` implementations.
//!
//! Gated by `cfg(any(test, feature = "testing"))` so downstream crates
//! must explicitly opt in via `--features testing`.

pub mod harness;

pub use harness::{ProcedureTestHarness, ReplayMismatch};

#[cfg(test)]
mod tests;
