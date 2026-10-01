//! The `Operator` trait, its settings tree, and the topology, instance,
//! dispatcher and registry modules.

pub mod builder;
pub mod dispatcher;
pub(crate) mod poll_pool;
pub use flz_core::operator::settings::{
    Durable, Each, Exactly, OnFailure, OperatorSettings, Run, SleepAfter, DEFAULT_SLEEP_AFTER,
};
pub use flz_core::operator::{instance, registry, settings, topology};
pub use instance::{KeyGroupRange, OperatorInstance};
pub use topology::OperatorTopology;

use crate::channel::ChannelBuilder;

/// One operator class: a stable `factory_id`, the graph of channels and
/// tasklets declared in [`channels`](Self::channels), and the settings tree in
/// [`settings`](Self::settings) that says how it runs and where its work is
/// kept. Every "one of" of the tree is an enum, so a setting written under
/// the wrong parent does not compile.
///
/// The runtime calls `R::channels(...)` and `R::settings()` once at
/// `register_factory(R)` time and freezes the result in an
/// `Arc<OperatorTopology>` shared by every `OperatorInstance` of this class.
/// A worked example of a class that runs in steps:
/// `sdk/rust/examples/spawn_steps_worker.rs`.
pub trait Operator: Send + Sync + 'static {
    /// Stable name. Used as the controller-facing factory identifier — the
    /// controller's `flz operator start <factory_id>` looks this up.
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

    /// The settings tree. The default is a flow operator kept on the
    /// controller's disk that sleeps after six hours idle.
    fn settings() -> OperatorSettings {
        OperatorSettings::default()
    }
}

/// Build and validate `R` WITHOUT touching any registry, so an invalid
/// declaration leaves the runner exactly as it was.
pub(crate) fn build_class<R: Operator>(
) -> Result<std::sync::Arc<OperatorTopology>, crate::FlezhaError> {
    Ok(match R::settings().run {
        Run::Steps { .. } => crate::steps::spec::build_spec::<R>()?,
        Run::Flow { .. } => builder::build_topology_for::<R>()?,
    })
}

#[cfg(test)]
mod instance_tests;
