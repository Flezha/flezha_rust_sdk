//! UnboundedOperative trait + topology/instance/dispatcher/registry modules per
//! `operative-mvp-rust/design.md` Decisions 1, 2, 3, 4, 6.

pub mod builder;
pub mod dispatcher;
pub mod instance;
pub mod persistence;
pub mod registry;
pub mod topology;

pub use instance::UnboundedInstance;
pub use topology::OperativeTopology;

use crate::channel::ChannelBuilder;

/// One UnboundedOperative class. Implementors declare a stable `factory_id` and the
/// per-class topology of channels + operations via `channels(&mut c)`.
///
/// State persistence (added by `operative-state-persistence-rust`):
///
/// - [`Self::PERSISTENCE_ENABLED`] — frozen-at-instance-start toggle.
///   Default `true`. The runner can override via the operative-start
///   metadata key `operative_persistence.<OperativeClass>`. When `false`, the
///   per-Process flusher drains-and-discards Changes for this UnboundedOperative's
///   Segments (no wire I/O, no controller contact).
///
/// The runtime calls `R::channels(...)` once at `register_unbounded::<R>()`
/// time and freezes the resulting topology in an `Arc<OperativeTopology>`
/// shared by every `UnboundedInstance` of this class.
pub trait UnboundedOperative: Send + Sync + 'static {
    /// Stable name. Used as the controller-facing factory identifier — the
    /// controller's `flz operative start <factory_id>` looks this up.
    fn factory_id() -> &'static str;

    /// One-shot topology declaration.
    fn channels(c: &mut ChannelBuilder);

    /// Class-level persistence toggle. The runtime resolves the final
    /// value at instance start by checking the operative-start metadata
    /// (runner override) first; if no override is present, this const
    /// is used. Frozen on the `UnboundedInstance` at construction; runtime
    /// changes only take effect on the next `stop_operative` + `start_operative`
    /// cycle.
    const PERSISTENCE_ENABLED: bool = true;
}
