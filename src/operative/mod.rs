//! StreamOperative trait + topology/instance/dispatcher/registry modules per
//! `operative-mvp-rust/design.md` Decisions 1, 2, 3, 4, 6.

pub mod builder;
pub mod dispatcher;
pub(crate) mod poll_pool;
pub use flz_core::operative::{instance, registry, topology};
pub use instance::{KeyGroupRange, OperativeInstance};
pub use topology::OperativeTopology;

use crate::channel::ChannelBuilder;

/// One StreamOperative class. Implementors declare a stable `factory_id` and the
/// per-class topology of channels + tasklets via `channels(&c)`.
///
/// State persistence (added by `operative-state-persistence`):
///
/// - [`Self::PERSISTENCE_ENABLED`] — frozen-at-instance-start toggle.
///   Default `true`. The runner can override via the operative-start
///   metadata key `operative_persistence.<OperativeClass>`. When `false`, the
///   per-Process flusher drains-and-discards Changes for this StreamOperative's
///   Cells (no wire I/O, no controller contact).
///
/// The runtime calls `R::channels(...)` once at `register_factory(R)`
/// time and freezes the resulting topology in an `Arc<OperativeTopology>`
/// shared by every `OperativeInstance` of this class.
pub trait StreamOperative: Send + Sync + 'static {
    /// Stable name. Used as the controller-facing factory identifier — the
    /// controller's `flz operative start <factory_id>` looks this up.
    fn factory_id() -> &'static str;

    /// One-shot topology declaration on the producer-first graph builder
    /// (`sdk-graph-authoring`): `c.input::<T>("name").into::<A>()...`.
    /// Stream handles borrow `c`, so they cannot outlive this call — the
    /// topology freezes when it returns.
    fn channels(c: &ChannelBuilder);

    /// Class-level persistence toggle. The runtime resolves the final
    /// value at instance start by checking the operative-start metadata
    /// (runner override) first; if no override is present, this const
    /// is used. Frozen on the `OperativeInstance` at construction; runtime
    /// changes only take effect on the next `stop_operative` + `start_operative`
    /// cycle.
    const PERSISTENCE_ENABLED: bool = true;
}

/// Sealing for [`Operative`]: only the SDK's two blanket impls satisfy it, so
/// no downstream type can invent a third operative kind and reach
/// `register_factory` with it.
mod sealed {
    pub trait Sealed<K> {}
}

/// The kind marker of a [`StreamOperative`]. Never constructed; it exists so
/// `register_factory` can dispatch on which authoring trait a value implements.
pub struct StreamKind;

/// The kind marker of a [`WorkflowOperative`](crate::WorkflowOperative).
pub struct WorkflowKind;

/// The shared base of both operative kinds: what a runner needs to declare one,
/// whichever kind it is. `K` is the marker of the kind the implementing type
/// declares, which is what lets one
/// [`register_factory`](crate::FlezhaRunner::register_factory) take either.
///
/// The SDK implements this for every [`StreamOperative`] (as
/// `Operative<StreamKind>`) and every
/// [`WorkflowOperative`](crate::WorkflowOperative) (as
/// `Operative<WorkflowKind>`), so an author implements only the kind trait. A
/// type implementing BOTH kind traits implements this twice, and `K` is then
/// unresolvable at the call site — a compile error, which is Rust's spelling of
/// the contract's `AmbiguousOperativeKind`.
///
/// The one member both kinds share is [`factory_id`](Self::factory_id); the
/// kind-divergent surface stays on the per-kind traits. Adding a further kind
/// means a third marker and a third blanket impl: neither authoring trait moves
/// and the runner gains no method.
pub trait Operative<K>: sealed::Sealed<K> {
    /// What building this class produces before it is installed: its frozen
    /// topology, which for a workflow carries the spec.
    type Class;

    /// The controller-facing factory id, the one member both kinds share.
    fn factory_id() -> &'static str;

    /// Build and validate the class WITHOUT touching any registry, so an
    /// invalid declaration leaves the runner exactly as it was.
    fn build_class() -> Result<Self::Class, crate::FlezhaError>;

    /// Put the built class in the process-global class registry.
    fn install_class(class: Self::Class);
}

impl<R: StreamOperative> sealed::Sealed<StreamKind> for R {}

impl<R: StreamOperative> Operative<StreamKind> for R {
    type Class = std::sync::Arc<OperativeTopology>;

    fn factory_id() -> &'static str {
        <R as StreamOperative>::factory_id()
    }

    fn build_class() -> Result<Self::Class, crate::FlezhaError> {
        Ok(builder::build_topology_for::<R>()?)
    }

    fn install_class(class: Self::Class) {
        registry::register_class(<R as StreamOperative>::factory_id(), class);
    }
}

impl<T: crate::WorkflowOperative> sealed::Sealed<WorkflowKind> for T {}

impl<T: crate::WorkflowOperative> Operative<WorkflowKind> for T {
    type Class = std::sync::Arc<OperativeTopology>;

    fn factory_id() -> &'static str {
        <T as crate::WorkflowOperative>::factory_id()
    }

    fn build_class() -> Result<Self::Class, crate::FlezhaError> {
        Ok(crate::workflow::spec::build_spec::<T>()?)
    }

    fn install_class(class: Self::Class) {
        registry::register_class(<T as crate::WorkflowOperative>::factory_id(), class);
    }
}

#[cfg(test)]
mod instance_tests;
