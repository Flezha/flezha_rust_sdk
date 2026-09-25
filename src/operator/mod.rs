//! UnboundedOperator trait + topology/instance/dispatcher/registry modules per
//! `operator-mvp-rust/design.md` Decisions 1, 2, 3, 4, 6.

pub mod builder;
pub mod dispatcher;
pub(crate) mod poll_pool;
pub use flz_core::operator::{instance, registry, topology};
pub use instance::{KeyGroupRange, OperatorInstance};
pub use topology::OperatorTopology;

use crate::channel::ChannelBuilder;

/// One UnboundedOperator class. Implementors declare a stable `factory_id` and the
/// per-class topology of channels + tasklets via `channels(&c)`.
///
/// State persistence (added by `operator-state-persistence`):
///
/// - [`Self::PERSISTENCE_ENABLED`] — frozen-at-instance-start toggle.
///   Default `true`. When `false`, the per-Process flusher drains-and-discards
///   Changes for this UnboundedOperator's Cells (no wire I/O, no controller
///   contact).
/// - [`Self::SLEEP_WHEN_IDLE`]: sleep after `FLEZHA_PARK_IDLE_MS` idle.
///   Default `false`.
///
/// The runtime calls `R::channels(...)` once at `register_factory(R)`
/// time and freezes the resulting topology in an `Arc<OperatorTopology>`
/// shared by every `OperatorInstance` of this class.
pub trait UnboundedOperator: Send + Sync + 'static {
    /// Stable name. Used as the controller-facing factory identifier — the
    /// controller's `flz operator start <factory_id>` looks this up.
    fn factory_id() -> &'static str;

    /// One-shot topology declaration on the producer-first graph builder
    /// (`sdk-graph-authoring`): `c.input::<T>("name").into::<A>()...`.
    /// Stream handles borrow `c`, so they cannot outlive this call — the
    /// topology freezes when it returns.
    fn channels(c: &ChannelBuilder);

    /// Class-level persistence toggle, announced to the controller as the
    /// class's `persistent` declaration. When `false` the per-Process flusher
    /// drains and discards this class's Changes and the controller restores
    /// nothing on a rebind. Frozen on the `OperatorInstance` at construction;
    /// a change takes effect on the next `stop_operator` + `start_operator`
    /// cycle.
    const PERSISTENCE_ENABLED: bool = true;

    /// Sleep when idle: an instance with nothing to do for
    /// `FLEZHA_PARK_IDLE_MS` parks, is evicted from the runner and wakes on
    /// its next input with its state restored. Off for an unbounded operator by default;
    /// declaring it with [`Self::PERSISTENCE_ENABLED`] off is refused as
    /// `SleepWithoutPersistence`.
    const SLEEP_WHEN_IDLE: bool = false;
}

/// Sealing for [`Operator`]: only the SDK's two blanket impls satisfy it, so
/// no downstream type can invent a third operator kind and reach
/// `register_factory` with it.
mod sealed {
    pub trait Sealed<K> {}
}

/// The kind marker of an [`UnboundedOperator`]. Never constructed; it exists so
/// `register_factory` can dispatch on which authoring trait a value implements.
pub struct UnboundedKind;

/// The kind marker of a [`BoundedOperator`](crate::BoundedOperator).
pub struct BoundedKind;

/// The shared base of both operator kinds: what a runner needs to declare one,
/// whichever kind it is. `K` is the marker of the kind the implementing type
/// declares, which is what lets one
/// [`register_factory`](crate::FlezhaRunner::register_factory) take either.
///
/// The SDK implements this for every [`UnboundedOperator`] (as
/// `Operator<UnboundedKind>`) and every
/// [`BoundedOperator`](crate::BoundedOperator) (as
/// `Operator<BoundedKind>`), so an author implements only the kind trait. A
/// type implementing BOTH kind traits implements this twice, and `K` is then
/// unresolvable at the call site — a compile error, which is Rust's spelling of
/// the contract's `AmbiguousOperatorKind`.
///
/// The one member both kinds share is [`factory_id`](Self::factory_id); the
/// kind-divergent surface stays on the per-kind traits. Adding a further kind
/// means a third marker and a third blanket impl: neither authoring trait moves
/// and the runner gains no method.
pub trait Operator<K>: sealed::Sealed<K> {
    /// What building this class produces before it is installed: its frozen
    /// topology, which for a bounded operator carries the spec.
    type Class;

    /// The controller-facing factory id, the one member both kinds share.
    fn factory_id() -> &'static str;

    /// Build and validate the class WITHOUT touching any registry, so an
    /// invalid declaration leaves the runner exactly as it was.
    fn build_class() -> Result<Self::Class, crate::FlezhaError>;

    /// Put the built class in the process-global class registry.
    fn install_class(class: Self::Class);
}

impl<R: UnboundedOperator> sealed::Sealed<UnboundedKind> for R {}

impl<R: UnboundedOperator> Operator<UnboundedKind> for R {
    type Class = std::sync::Arc<OperatorTopology>;

    fn factory_id() -> &'static str {
        <R as UnboundedOperator>::factory_id()
    }

    fn build_class() -> Result<Self::Class, crate::FlezhaError> {
        Ok(builder::build_topology_for::<R>()?)
    }

    fn install_class(class: Self::Class) {
        registry::register_class(<R as UnboundedOperator>::factory_id(), class);
    }
}

impl<T: crate::BoundedOperator> sealed::Sealed<BoundedKind> for T {}

impl<T: crate::BoundedOperator> Operator<BoundedKind> for T {
    type Class = std::sync::Arc<OperatorTopology>;

    fn factory_id() -> &'static str {
        <T as crate::BoundedOperator>::factory_id()
    }

    fn build_class() -> Result<Self::Class, crate::FlezhaError> {
        Ok(crate::bounded::spec::build_spec::<T>()?)
    }

    fn install_class(class: Self::Class) {
        registry::register_class(<T as crate::BoundedOperator>::factory_id(), class);
    }
}

#[cfg(test)]
mod instance_tests;
