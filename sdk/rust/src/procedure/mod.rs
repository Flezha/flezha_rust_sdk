//! Procedure trait + ProcedureCtx + ProcedureError per `operative-architecture/design.md`
//! Decision 1, `operative-mvp-rust/design.md` Decision 1, and
//! `operative-state-persistence/design.md` Decisions 1, 4, 5, 8, 9.

pub mod change;
pub mod clock;
pub mod ctx;
pub mod error;
pub mod spawn;
pub mod state_handle;

use std::sync::Arc;

pub use change::{Change, CoalesceKey, DerivedState, NoChange};
pub use clock::{Clock, ClockFor, EventTimeFrom, LogicalClock, TimeExtractor, WallClock};
#[allow(unused_imports)]
pub(crate) use ctx::EmitSink;
pub use ctx::{ProcedureCtx, StableId};
pub use error::ProcedureError;
use serde::de::DeserializeOwned;
use serde::Serialize;
pub use spawn::{IfRunning, SpawnError, SpawnHandle, SpawnOutcome};
pub use state_handle::StateHandle;

use crate::entity::restore::RestoreInitPayload;

/// Default panic message stamped on `Procedure::apply_change` for Procedures
/// that declare a non-`NoChange` Change without overriding
/// `apply_change`. Public-but-hidden so the runtime's EntityFlusher
/// can grep for the same string in tests.
#[doc(hidden)]
pub const DEFAULT_PROCEDURE_APPLY_CHANGE_PANIC_MESSAGE: &str =
    "Procedure declared a non-NoChange `type Change` but did not override `apply_change`. \
     Implement `fn apply_change(state: &mut Self::State, change: &Self::Change)` to fold \
     the Change into the State deterministically.";

/// Default per-Entity WAL size compaction threshold (8 MiB), in lockstep with
/// the controller's `:default_compact_after_bytes`. The single source both the
/// `Procedure::compact_after_bytes` default and the `AddOperative` metadata
/// "omit when equal to the default" check read.
pub const DEFAULT_COMPACT_AFTER_BYTES: usize = 8 * 1024 * 1024;

/// One Procedure in an UnboundedOperative topology. Stateless dispatcher; per-entity
/// State is held by the runtime and threaded through `run` via a
/// [`StateHandle`].
///
/// # Determinism contract
///
/// Replay is Flezha's recovery primitive: crash-resume, retry-replay, and
/// the effectively-once / rewind capabilities all re-run Procedures over
/// restored state and rely on the results coming out the same. [`Self::run`],
/// [`Self::apply_change`], and [`Self::compensate`] MUST therefore be
/// deterministic: given the same Entity state and the same input sequence
/// they produce identical state changes and identical emitted outputs.
/// Ambient non-determinism inside these functions — `SystemTime::now()`,
/// random numbers / `Uuid::new_v4()`, reads of external mutable state — is
/// a contract violation. Obtain time via [`ProcedureCtx::now`] (served from
/// the declared [`Self::Clock`]) and identifiers via
/// [`ProcedureCtx::stable_id`]. `ProcedureTestHarness::replay_emits_equal`
/// verifies the output side of the contract; `replay_equals` the state side.
///
/// State persistence (added by `operative-state-persistence`):
///
/// - [`Self::Change`] — associated type bounded on
///   `Serialize + DeserializeOwned + Send + Sync + 'static`. Default
///   [`NoChange`] opts out of persistence machinery.
/// - [`Self::Clock`] — sealed trait. Default [`WallClock`].
/// - [`Self::Config`] — typed startup config bound to `DeserializeOwned`.
///   Default `()` invokes `Self::default()`.
/// - [`Self::SCHEMA_VERSION`] — bump on non-additive Change-schema edits.
/// - [`Self::apply_change`] — fold a Change into the State; MUST be
///   deterministic; verified by `ProcedureTestHarness::replay_equals`.
/// - [`Self::compact_after_bytes`] — per-Entity WAL compaction
///   threshold reported to the controller at operative-start time. Default 64 MiB.
/// - [`Self::from_config`] — constructor from typed Config. Default
///   `Self::default()` for `Config = ()`.
pub trait Procedure: Send + Sync + 'static + Default {
    /// Inter-procedure transport is `Arc<Self::Input>`-zero-copy; the
    /// runtime hands the consumer the unique Arc when possible and
    /// clones the value when fan-out keeps the Arc shared. Requires
    /// `Clone` so the runtime can fall back to a value-clone in the
    /// shared-Arc case (`Arc::unwrap_or_clone`).
    type Input: Send + Sync + 'static + Clone;
    type Output: Send + Sync + 'static;
    /// Per-Entity state. Bounded on `Serialize + DeserializeOwned` so
    /// the runtime can serialize state for `EntityState` /
    /// `MigrationSerialization` and seed it from `AddOperative.init_values`
    /// without going through the user's `init_state`.
    type State: Send + Sync + 'static + Serialize + DeserializeOwned;

    /// Persistent mutation record. Use [`NoChange`] (the uninhabited
    /// default) to opt out of persistence machinery — the runtime
    /// detects `NoChange` via TypeId and skips flush-buffer allocation.
    /// Stable Rust does not yet stabilize `associated_type_defaults`, so
    /// every impl declares this; persistence-free Procedures write
    /// `type Change = NoChange;` explicitly.
    type Change: Change;

    /// Time source. Sealed trait; evaluated by the runtime once per `run`
    /// invocation and served via [`ProcedureCtx::now`]. The
    /// `ClockFor<Self::Input>` bound makes an unservable configuration
    /// (e.g. `EventTimeFrom<F>` whose `F` lacks a `TimeExtractor` for this
    /// Input) a compile error. Persistence-free Procedures write
    /// `type Clock = WallClock;`.
    type Clock: ClockFor<Self::Input>;

    /// Startup-time configuration deserialized from operative-start metadata
    /// (postcard). Use `()` for the no-config default; impls write
    /// `type Config = ();` explicitly.
    type Config: DeserializeOwned + Send + Sync + 'static;

    /// Per-Procedure schema version stamped into persisted Change headers.
    /// Bump on non-additive Change-schema edits.
    const SCHEMA_VERSION: u32 = 1;

    /// Stable identifier within the UnboundedOperative class. Must be unique per UnboundedOperative.
    fn name() -> &'static str;

    /// Per-entity state factory. Called once per Entity at UnboundedOperative
    /// startup. `entity_key` is `None` in single-entity mode and
    /// `Some(k)` for static mode where `k` is one of the values returned
    /// by `entity_keys()`.
    ///
    /// `init_payload` is `None` for cold starts. For warm restarts the
    /// runtime hands the user a borrowed view of the snapshot +
    /// change-log bytes; the user MAY inspect them for instrumentation
    /// but does not have to decode them — the runtime applies the
    /// payload automatically AFTER `init_state` returns.
    fn init_state(
        entity_key: Option<&str>,
        init_payload: Option<RestoreInitPayload<'_>>,
    ) -> Self::State;

    /// Fold a Change into the per-Entity state. Default panics for
    /// non-`NoChange` Change types; Procedures declaring a Change MUST
    /// override. Determinism is convention-enforced; verified by
    /// `ProcedureTestHarness::replay_equals`.
    fn apply_change(_state: &mut Self::State, _change: &Self::Change) {
        if std::any::TypeId::of::<Self::Change>() == std::any::TypeId::of::<NoChange>() {
            // No-op: NoChange is uninhabited so this branch never runs
            // in practice; included for completeness.
            return;
        }
        panic!("{}", DEFAULT_PROCEDURE_APPLY_CHANGE_PANIC_MESSAGE);
    }

    /// Per-Entity WAL size threshold reported to the controller. The
    /// runtime never compacts on the SDK side — compaction is controller-
    /// driven per `operative-state-persistence`. Default 8 MiB,
    /// in lockstep with the controller's `:default_compact_after_bytes`.
    fn compact_after_bytes() -> usize {
        DEFAULT_COMPACT_AFTER_BYTES
    }

    /// Constructor from typed Config. Default invokes `Self::default()`,
    /// which requires `Config = ()` (`Self: Default` is already part of
    /// the trait bound).
    ///
    /// This is the sanctioned path for a **parameterized procedure** — one
    /// class serving several configurations, each instance constructed from
    /// the typed config carried in the operative-start metadata:
    ///
    /// ```
    /// use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
    /// use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
    ///
    /// #[derive(serde::Deserialize)]
    /// struct SmaConfig {
    ///     window: usize,
    /// }
    ///
    /// #[derive(Default)]
    /// struct Sma {
    ///     window: usize,
    /// }
    ///
    /// impl Procedure for Sma {
    ///     type Input = f64;
    ///     type Output = f64;
    ///     type State = ();
    ///     type Change = NoChange;
    ///     type Clock = WallClock;
    ///     type Config = SmaConfig;
    ///     fn name() -> &'static str { "sma" }
    ///     fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    ///     fn from_config(cfg: SmaConfig) -> Self {
    ///         Sma { window: cfg.window }
    ///     }
    ///     fn run(
    ///         &mut self,
    ///         _input: f64,
    ///         _state: StateHandle<'_, Self>,
    ///         _ctx: &mut ProcedureCtx<'_>,
    ///     ) -> Result<(), ProcedureError> {
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn from_config(_cfg: Self::Config) -> Self {
        // The default impl is monomorphized per Procedure; Procedures with a
        // non-`()` Config must override.
        Self::default()
    }

    /// Static-entity declaration. Empty (the default) means
    /// single-entity mode; non-empty enables static entity.
    fn entity_keys() -> Vec<&'static str> {
        Vec::new()
    }

    /// Static-mode entity routing. For each input, returns the entity
    /// keys that should receive it. Empty vec drops the input. Single-mode
    /// procedures inherit the default `vec![]`; the runtime skips this call
    /// for them.
    ///
    /// Static-mode procedures (`entity_keys()` non-empty) MUST override this
    /// AND set `ENTITIES_FOR_OVERRIDDEN = true`; the topology builder
    /// rejects the operative otherwise.
    fn entities_for(_input: &Self::Input) -> Vec<&'static str> {
        Vec::new()
    }

    /// Override-detection sentinel. Default `false`; static-mode Procedures
    /// MUST set this to `true` alongside their `entities_for` override.
    /// `build_topology` checks the const+`entity_keys()` consistency at
    /// startup and rejects mismatched declarations.
    const ENTITIES_FOR_OVERRIDDEN: bool = false;

    /// Dynamic-mode entity routing. Called per input; the returned key
    /// selects (lazily creating) the Entity, `None` drops the input. A
    /// dynamic-mode Procedure overrides this AND sets `ENTITY_KEY_OVERRIDDEN =
    /// true` with empty `entity_keys()`; declaring both modes is a
    /// registration error.
    fn entity_key(_input: &Self::Input) -> Option<String> {
        None
    }

    /// Override-detection sentinel for [`Self::entity_key`] (dynamic mode).
    const ENTITY_KEY_OVERRIDDEN: bool = false;

    /// Dynamic-mode idle eviction window. `None` (default) = no eviction:
    /// Entities live as long as the UnboundedOperative, like static ones. `Some(ttl)`
    /// opts a per-Procedure reaper in; eviction is destructive (in-memory slot +
    /// on-disk WAL/snapshot dropped) and a later input cold-starts fresh —
    /// no restore-from-disk. Ignored for single/static Procedures.
    fn entity_idle_ttl() -> Option<std::time::Duration> {
        None
    }

    /// Dynamic-mode soft cap on live Entities per Procedure. At the cap,
    /// new-key inputs are dropped (`inputs_dropped_capped_total`); existing
    /// keys are unaffected. Bounds memory against a runaway `entity_key`, not
    /// admission — no error path back to the controller.
    fn max_entities() -> usize {
        100_000
    }

    /// Override-detection sentinel for [`Self::max_entities`].
    const MAX_ENTITIES_OVERRIDDEN: bool = false;

    /// Per-Entity mailbox capacity. The consumer Entity allocates
    /// one bounded ring buffer of this size at operative startup. Static-mode
    /// Procedures allocate one mailbox per declared entity key, all sharing
    /// the same capacity.
    ///
    /// Override precedence at registration time:
    ///   1. This method's value, if `MAILBOX_CAPACITY_OVERRIDDEN = true`.
    ///   2. `FLEZHA_MAILBOX_CAPACITY` env var (parsed as `usize`).
    ///   3. The built-in default (`DEFAULT_MAILBOX_CAPACITY`).
    ///
    /// Implementors overriding the default MUST also set
    /// `MAILBOX_CAPACITY_OVERRIDDEN = true` so the runtime can tell an
    /// explicit-but-equal-to-default override from the inherited default.
    /// Capacity must be in `1..=1_048_576`; out-of-range values fail
    /// `define_unbounded::<R>()` with the offending Procedure name.
    fn mailbox_capacity() -> usize {
        Self::DEFAULT_MAILBOX_CAPACITY
    }

    /// Default mailbox capacity used by [`Self::mailbox_capacity`].
    const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

    /// Override-detection sentinel for `mailbox_capacity`. Default `false`;
    /// implementors that override `mailbox_capacity()` MUST set this to
    /// `true` so the runtime applies the Procedure value over any
    /// `FLEZHA_MAILBOX_CAPACITY` env override.
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = false;

    /// Compute. Receives the typed input, a [`StateHandle`] to read state
    /// via `Deref` and mutate via `state.apply(change)`, and a context for
    /// emitting outputs. Producing zero outputs == no `ctx.emit(...)` call.
    /// Returning Err logs against `(operative, procedure, entity)` and discards
    /// any partial emits.
    fn run(
        &mut self,
        input: Self::Input,
        state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError>;

    /// Optional saga **compensation** hook (bounded operatives only). An
    /// procedure whose `run` commits a reversible side effect declares this to
    /// undo it. The bounded driver fires it ONLY during abnormal-termination
    /// rollback — never on a clean `ok`, never for an unbounded operative — once
    /// per entity that ran, in reverse completion order, handing back that
    /// entity's own collected outputs (zero-copy `Arc`s) and its
    /// [`StateHandle`], so it can reverse precisely. Compensation MUST be
    /// idempotent by author contract: a crash mid-rollback may re-invoke it.
    ///
    /// The default is a no-op; a procedure that overrides this MUST also set
    /// [`COMPENSATE_OVERRIDDEN`](Self::COMPENSATE_OVERRIDDEN) `= true` so the
    /// bounded driver registers it as compensable (hookless procedures are
    /// skipped during rollback).
    fn compensate(
        &mut self,
        _completed_output: Vec<Arc<Self::Output>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }

    /// Override-detection sentinel for [`Self::compensate`]. Default `false`; an
    /// procedure that overrides `compensate` MUST set this to `true` so the
    /// bounded driver knows the procedure participates in saga rollback.
    const COMPENSATE_OVERRIDDEN: bool = false;
}

#[cfg(test)]
mod tests;
