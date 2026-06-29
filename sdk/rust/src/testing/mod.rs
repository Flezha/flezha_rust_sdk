//! Test harness for `Operation` implementations per
//! `operative-state-persistence-rust/design.md` Decision 6.
//!
//! Gated by `cfg(any(test, feature = "testing"))` so downstream crates
//! must explicitly opt in via `--features testing`.

pub mod harness;

pub use harness::{ReplayMismatch, OperationTestHarness};

#[cfg(test)]
mod tests;
