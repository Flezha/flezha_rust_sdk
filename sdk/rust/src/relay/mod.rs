//! Relay trait + topology/instance/dispatcher/registry modules per
//! `relay-mvp-rust/design.md` Decisions 1, 2, 3, 4, 6.

pub mod builder;
pub mod dispatcher;
pub mod instance;
pub mod registry;
pub mod topology;

pub use instance::RelayInstance;
pub use topology::RelayTopology;

use crate::channel::ChannelBuilder;

/// One Relay class. Implementors declare a stable `factory_id` and the
/// per-class topology of channels + shards via `channels(&mut c)`.
///
/// The runtime calls `R::channels(...)` once at `register_relay::<R>()`
/// time and freezes the resulting topology in an `Arc<RelayTopology>`
/// shared by every `RelayInstance` of this class.
pub trait Relay: Send + Sync + 'static {
    /// Stable name. Used as the agent-facing factory identifier — the
    /// agent's `flz processor start <factory_id>` looks this up.
    fn factory_id() -> &'static str;

    /// One-shot topology declaration.
    fn channels(c: &mut ChannelBuilder);
}
