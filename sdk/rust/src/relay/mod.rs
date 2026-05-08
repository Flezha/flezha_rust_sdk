//! Relay trait + topology/instance/dispatcher/registry modules per
//! `relay-mvp-rust/design.md` Decisions 1, 2, 3, 4, 6.

pub mod builder;
pub mod dispatcher;
pub mod instance;
pub mod persistence;
pub mod registry;
pub mod topology;

pub use instance::RelayInstance;
pub use topology::RelayTopology;

use crate::channel::ChannelBuilder;

/// One Relay class. Implementors declare a stable `factory_id` and the
/// per-class topology of channels + shards via `channels(&mut c)`.
///
/// State persistence (added by `relay-state-persistence-rust`):
///
/// - [`Self::PERSISTENCE_ENABLED`] — frozen-at-instance-start toggle.
///   Default `true`. The executor can override via the relay-start
///   metadata key `relay_persistence.<RelayClass>`. When `false`, the
///   per-Process flusher drains-and-discards Changes for this Relay's
///   Partitions (no wire I/O, no controller contact).
///
/// The runtime calls `R::channels(...)` once at `register_relay::<R>()`
/// time and freezes the resulting topology in an `Arc<RelayTopology>`
/// shared by every `RelayInstance` of this class.
pub trait Relay: Send + Sync + 'static {
    /// Stable name. Used as the controller-facing factory identifier — the
    /// controller's `flz relay start <factory_id>` looks this up.
    fn factory_id() -> &'static str;

    /// One-shot topology declaration.
    fn channels(c: &mut ChannelBuilder);

    /// Class-level persistence toggle. The runtime resolves the final
    /// value at instance start by checking the relay-start metadata
    /// (executor override) first; if no override is present, this const
    /// is used. Frozen on the `RelayInstance` at construction; runtime
    /// changes only take effect on the next `stop_relay` + `start_relay`
    /// cycle.
    const PERSISTENCE_ENABLED: bool = true;
}
