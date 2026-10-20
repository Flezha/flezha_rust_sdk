//! Rust SDK for the Flezha runner platform.
//!
//! Stream readers push directly into per-Cell bounded mailboxes; the typed
//! layer's poll workers drain them through `flz_core`'s activation engine
//! (`poll() → dispatch → complete`). The per-Cell inflight CAS is the
//! crate's one invariant: no two `Tasklet::run` calls ever overlap on the
//! same Cell, so a class that appends to its state observes its inputs in
//! arrival order.
//!
//! Worked examples live in `sdk/rust/examples/` — start with
//! `append_relay.rs` (sticky ordering with persistent state). Method names
//! and lifecycle verbs follow `docs/internal/sdk/naming_conventions.md`, mirrored 1:1
//! with the Java SDK; every environment knob the runner reads is listed in
//! `docs/public/configuration.md`.

pub mod cell;
pub mod channel;
pub mod control;
pub mod diag;
pub mod local;
pub mod relay;
mod runner;
mod runner_handle;
pub mod steps;
pub mod tasklet;
mod util;

pub(crate) use flz_core::shutdown;

// The attribute macros expand to `::flezha_runner::...` paths; this lets the
// crate's own tests use them.
#[cfg(test)]
extern crate self as flezha_runner;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

/// One-stop import path for the state persistence surface: bring it into
/// scope with `use flezha_runner::state::*;` to write tasklets that declare
/// `Change` types and record them through `ctx.change(...)`.
pub mod state {
    pub use flz_core::header::RestoreError;
    pub use flz_core::restore::RestoreInitPayload;
    /// The codec the runtime encodes persisted `State` and `Change` bodies
    /// with, re-exported so a `migrate_state` / `migrate_change` can decode an
    /// earlier version's bytes at exactly the version the runtime wrote them —
    /// a separately-pinned postcard in the author's own crate would not be the
    /// same guarantee.
    pub use postcard;

    pub use crate::cell::timer::TimerKind;
    pub use crate::tasklet::change::{Change, CoalesceKey, NoChange};
    pub use crate::tasklet::clock::ClockKind;
}

pub use cell::timer::TimerKind;
pub use cell::{RestoreError, RestoreInitPayload};
pub use channel::builder::TopologyIssue;
pub use channel::{
    CallTarget, ChannelBuilder, EntryStream, Merge, MergeSource, OverflowPolicy, Response,
    RetryPolicy, Stream,
};
/// Administering the namespace a runner's connection proved.
pub use control::{Control, ControlError, PluginSpec, RelaySpec, StartOutcome, StartResult};
/// Authoring sugar. `#[flezha::tasklet]` on an `impl Tasklet` block, the
/// short-form `#[flezha::relay]` and `flezha::pipeline!` expand to the
/// trait surface above; see `docs/internal/sdk/authoring_contract.md`.
pub use flezha_macros::{pipeline, relay, tasklet};
pub use local::LocalRelay;
pub use relay::builder::TopologyBuildError;
pub use relay::topology::{
    AckGranularity, BeyondWindowPolicy, CheckpointOutputs, CheckpointPolicy,
};
pub use relay::{Durable, Each, Exactly, OnFailure, Relay, RelaySettings, Run, SleepAfter};
pub use runner_handle::{ErrorModel, Flezha, FlezhaError, FlezhaRunner, SendOpts};
pub use steps::StepsError;
pub use tasklet::{Change, ClockKind, CoalesceKey, Ctx, NoChange, StableId, Tasklet, TaskletError};

/// Build a class and install it in the process-global registry, with no
/// runner involved — the body [`FlezhaRunner::register_factory`] runs once it
/// has cleared the runner's own declared set. Kept callable on its own for the
/// local run and for the steps scenarios that drive a class without a runner.
pub(crate) fn register_class<O: Relay>() -> Result<(), FlezhaError> {
    let class = relay::build_class::<O>()?;
    relay::registry::register_class(O::factory_id(), class);
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
    pub use flz_core::runner::RelayDispatchHandle;
}
