//! Test harness for `Tasklet` implementations.
//!
//! Gated by `cfg(any(test, feature = "testing"))` so downstream crates
//! must explicitly opt in via `--features testing`.

pub mod harness;

pub use harness::{ReplayMismatch, TaskletTestHarness};

#[cfg(test)]
mod tests;
