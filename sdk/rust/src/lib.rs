//! Rust SDK for the Flezha runner platform.
//!
//! Stream readers push directly into per-Entity bounded mailboxes; the typed
//! layer's poll workers drain them through `flz_core`'s activation engine
//! (`poll() → dispatch → complete`). The per-Entity inflight CAS is the
//! crate's one invariant: no two `Procedure::run` calls ever overlap on the
//! same Entity, so a class that appends to its state observes its inputs in
//! arrival order.
//!
//! Worked examples live in `sdk/rust/examples/` — start with
//! `append_operative.rs` (sticky ordering with persistent state). Method names
//! and lifecycle verbs follow `docs/sdk/naming_conventions.md`, mirrored 1:1
//! with the Java SDK; every environment knob the runner reads is listed in
//! `docs/configuration.md`.

pub mod channel;
pub mod workflow;
pub use flz_core::command;
pub mod diag;
pub mod entity;
pub mod entry_idle;
pub mod local;
pub mod operative;
pub mod procedure;
mod runner;
mod runner_handle;
mod util;

pub(crate) use flz_core::shutdown;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

/// One-stop import path for the `operative-state-persistence` trait
/// surface. Bring everything into scope with
/// `use flezha_runner::state::*;` to write Procedures that declare
/// `Change` types and route mutations through `state.apply(...)`.
pub mod state {
    pub use flz_core::header::RestoreError;
    pub use flz_core::restore::RestoreInitPayload;
    /// The codec the runtime encodes persisted `State` and `Change` bodies
    /// with, re-exported so a `migrate_state` / `migrate_change` can decode an
    /// earlier version's bytes at exactly the version the runtime wrote them —
    /// a separately-pinned postcard in the author's own crate would not be the
    /// same guarantee.
    pub use postcard;

    pub use crate::entity::timer::TimerKind;
    pub use crate::procedure::change::{Change, CoalesceKey, NoChange};
    pub use crate::procedure::clock::{
        Clock, ClockFor, EventTimeFrom, LogicalClock, TimeExtractor, WallClock,
    };
    pub use crate::procedure::state_handle::StateHandle;
}

pub use channel::builder::TopologyIssue;
pub use channel::{
    Channel, ChannelBuilder, EntryStream, Merge, MergeSource, OverflowPolicy, RetryPolicy, Stream,
};
pub use entity::timer::TimerKind;
pub use entity::{RestoreError, RestoreInitPayload};
pub use entry_idle::IdleObserver;
/// Authoring sugar. `#[flezha::procedure]`, `#[derive(flezha::State)]`, and
/// `flezha::operative!` expand to the trait surface above; see
/// `docs/sdk/authoring_contract.md`.
pub use flezha_macros::{operative, procedure, State};
pub use flz_transport::{service, transport};
/// Re-exported so macro-generated code can `submit!` into the auto-operative
/// collection without the user adding an `inventory` dependency.
pub use inventory;
pub use local::LocalOperative;
pub use operative::builder::TopologyBuildError;
pub use operative::StreamOperative;
pub use procedure::{
    Change, Clock, ClockFor, CoalesceKey, DerivedState, EventTimeFrom, LogicalClock, NoChange,
    Procedure, ProcedureCtx, ProcedureError, SimpleProcedure, StableId, StateHandle, TimeExtractor,
    WallClock,
};
pub use runner_handle::{Flezha, FlezhaError, FlezhaRunner};
pub use workflow::spec::BuildWorkflowError;
pub use workflow::{LoopMode, RollbackMode, WorkflowCtx, WorkflowError, WorkflowOperative};

/// An operative using the single-procedure auto-wire path (`#[flezha::procedure(sink = …)]`) or
/// `flezha::operative!` registers without an explicit `.define_stream::<R>()` call.
/// Generated code `submit!`s one of these; every [`FlezhaRunner::start`]
/// harvests the collection into the runner's declared set.
pub struct AutoOperative {
    /// The class's controller-facing factory id (for duplicate detection).
    pub factory_id: fn() -> &'static str,
    /// Registers the class topology (`__register_stream::<R>()`).
    pub register: fn(),
}

impl AutoOperative {
    pub const fn new(factory_id: fn() -> &'static str, register: fn()) -> Self {
        Self {
            factory_id,
            register,
        }
    }
}

inventory::collect!(AutoOperative);

/// Harvest every auto-registered operative (single-procedure auto-wire +
/// `flezha::operative!`) into the class registries WITHOUT a runner —
/// idempotent (the registries overwrite on insert). A started runner does
/// not need this: [`FlezhaRunner::start`] runs the same harvest and merges
/// the ids into its own (empty-at-creation) declared set.
pub fn register_inventory_operatives() {
    for auto in inventory::iter::<AutoOperative> {
        (auto.register)();
    }
}

pub use flz_core::pb;

/// Register a StreamOperative class topology in the global class registry.
/// The internal body behind [`FlezhaRunner::define_stream`]; kept separate
/// so macro-generated auto-wire code can reuse it via [`__register_stream`].
pub(crate) fn register_stream_class<R: StreamOperative>() -> Result<(), TopologyBuildError> {
    let topology = operative::builder::build_topology_for::<R>()?;
    operative::registry::register_class(R::factory_id(), topology);
    Ok(())
}

/// Register a workflow class spec in the global class registry — the same
/// body `FlezhaRunner::define_workflow` runs, kept callable without a runner
/// for the workflow driver's unit scenarios.
#[cfg(test)]
pub(crate) fn register_workflow_class<T: WorkflowOperative>() -> Result<(), BuildWorkflowError> {
    let spec = workflow::spec::build_spec::<T>()?;
    workflow::registry::register_class(T::factory_id(), spec);
    Ok(())
}

/// Macro plumbing for `#[flezha::procedure(...)]` / `flezha::operative!`
/// auto-registration. Not part of the public API — declare classes with
/// [`FlezhaRunner::define_stream`]. Panics on an invalid topology: the
/// topology is machine-generated, so a failure here is an SDK bug, not an
/// authoring mistake the user could handle.
#[doc(hidden)]
pub fn __register_stream<R: StreamOperative>() {
    register_stream_class::<R>().expect("flezha: auto-wired operative has an invalid topology");
}

// ---------------------------------------------------------------------------
// Internal dispatch surface for benches/tests. NOT a stable API. Only
// available under the `bench-internals` feature, which is enabled by
// this crate's `[[bench]]` targets via `required-features`. Downstream
// consumers never enable this feature, so the surface below is not part
// of the crate's public API for version-compat purposes.
// ---------------------------------------------------------------------------
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod __dispatch_internals {
    pub use flz_core::mailbox::Mailbox;
    pub use flz_core::runner::OperativeDispatchHandle;
}
