//! The `Relay` trait, its settings tree, and the topology, instance,
//! dispatcher and registry modules.

pub mod builder;
pub(crate) mod dispatcher;
pub use flz_core::relay::settings::{
    Durable, Each, Exactly, OnFailure, RelaySettings, Run, SleepAfter, DEFAULT_SLEEP_AFTER,
};
pub use flz_core::relay::{instance, registry, settings, topology};
pub use instance::{KeyGroupRange, RelayInstance};
pub use topology::RelayTopology;

use crate::channel::ChannelBuilder;

/// One relay class: a stable `factory_id`, the graph of channels and
/// tasklets declared in [`channels`](Self::channels), and the settings tree in
/// [`settings`](Self::settings) that says how it runs and where its work is
/// kept. Every "one of" of the tree is an enum, so a setting written under
/// the wrong parent does not compile.
///
/// The runtime calls `R::channels(...)` and `R::settings()` once at
/// `register_factory(R)` time and freezes the result in an
/// `Arc<RelayTopology>` shared by every `RelayInstance` of this class.
/// A worked example of a class that runs in steps:
/// `sdk/rust/examples/spawn_steps_worker.rs`.
pub trait Relay: Send + Sync + 'static {
    /// Stable name. Used as the controller-facing factory identifier — the
    /// controller's `flz relay start <factory_id>` looks this up.
    fn factory_id() -> &'static str;

    /// One-shot topology declaration on the producer-first graph builder
    /// (`sdk-graph-authoring`): `c.input::<T>("name").into::<A>()...`.
    /// Stream handles borrow `c`, so they cannot outlive this call — the
    /// topology freezes when it returns.
    ///
    /// A class that runs in steps runs this graph in topological order: a
    /// tasklet becomes runnable only after **every** producer feeding it has
    /// committed, and a fan-in node is dispatched once, with the accumulated
    /// batch of all its producers. Cycles are refused at registration.
    fn channels(c: &ChannelBuilder);

    /// The settings tree. The default is a flow relay kept on the
    /// controller's disk that sleeps after six hours idle.
    fn settings() -> RelaySettings {
        RelaySettings::default()
    }
}

/// Build and validate `R` WITHOUT touching any registry, so an invalid
/// declaration leaves the runner exactly as it was.
pub(crate) fn build_class<R: Relay>() -> Result<std::sync::Arc<RelayTopology>, crate::FlezhaError> {
    Ok(match R::settings().run {
        Run::Steps { .. } => crate::steps::spec::build_spec::<R>()?,
        Run::Flow { .. } => builder::build_topology_for::<R>()?,
    })
}

#[cfg(test)]
mod instance_tests;
