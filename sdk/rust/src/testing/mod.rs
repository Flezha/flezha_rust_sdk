//! Test harness for `Shard` implementations per
//! `relay-state-persistence-rust/design.md` Decision 6.
//!
//! Gated by `cfg(any(test, feature = "testing"))` so downstream crates
//! must explicitly opt in via `--features testing`.

pub mod harness;

pub use harness::{ReplayMismatch, ShardTestHarness};

#[cfg(test)]
mod tests;
