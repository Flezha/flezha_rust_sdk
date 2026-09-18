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

pub mod admin;
pub mod channel;
pub mod workflow;
pub use flz_core::command;
pub mod diag;
pub mod entity;
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

/// Administering the namespace a runner's connection proved.
pub use admin::{Admin, OperativeSpec, StartOutcome, StartResult};
pub use channel::builder::TopologyIssue;
pub use channel::{
    Channel, ChannelBuilder, EntryStream, Merge, MergeSource, OverflowPolicy, RetryPolicy, Stream,
};
pub use entity::timer::TimerKind;
pub use entity::{RestoreError, RestoreInitPayload};
/// Authoring sugar. `#[flezha::procedure]`, the short-form
/// `#[flezha::stream_operative]` / `#[flezha::workflow_operative]`,
/// `#[derive(flezha::State)]` and `flezha::operative!` expand to the trait
/// surface above; see `docs/sdk/authoring_contract.md`.
pub use flezha_macros::{operative, procedure, stream_operative, workflow_operative, State};
pub use flz_core::pb;
pub use flz_transport::{service, transport};
pub use local::LocalOperative;
pub use operative::builder::TopologyBuildError;
pub use operative::{Operative, StreamKind, StreamOperative, WorkflowKind};
pub use procedure::{
    Change, Clock, ClockFor, CoalesceKey, DerivedState, EventTimeFrom, LogicalClock, NoChange,
    Procedure, ProcedureCtx, ProcedureError, SimpleProcedure, StableId, StateHandle, TimeExtractor,
    WallClock,
};
pub use runner_handle::{Flezha, FlezhaError, FlezhaRunner};
pub use workflow::spec::BuildWorkflowError;
pub use workflow::{LoopMode, RollbackMode, WorkflowError, WorkflowOperative};

/// Build a class of kind `K` and install it in that kind's process-global
/// registry, with no runner involved — the body
/// [`FlezhaRunner::register_factory`] runs once it has cleared the runner's own
/// declared set. Kept callable on its own for the local run and for the driver
/// scenarios that drive a class without a runner at all.
pub(crate) fn register_class<K, O: Operative<K>>() -> Result<(), FlezhaError> {
    O::install_class(O::build_class()?);
    Ok(())
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
