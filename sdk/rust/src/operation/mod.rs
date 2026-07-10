//! Operation trait + OperationCtx + OperationError per `operative-architecture/design.md`
//! Decision 1, `operative-mvp-rust/design.md` Decision 1, and
//! `operative-state-persistence-rust/design.md` Decisions 1, 4, 5, 8, 9.

pub mod change;
pub mod clock;
pub mod ctx;
pub mod error;
pub mod state_handle;

use std::sync::Arc;

pub use change::{Change, CoalesceKey, DerivedState, NoChange};
pub use clock::{Clock, ClockFor, EventTimeFrom, LogicalClock, TimeExtractor, WallClock};
#[allow(unused_imports)]
pub(crate) use ctx::EmitSink;
pub use ctx::{OperationCtx, StableId};
pub use error::OperationError;
use serde::de::DeserializeOwned;
use serde::Serialize;
pub use state_handle::StateHandle;

use crate::segment::restore::RestoreInitPayload;

/// Default panic message stamped on `Operation::apply_change` for Operations
/// that declare a non-`NoChange` Change without overriding
/// `apply_change`. Public-but-hidden so the runtime's SegmentFlusher
/// can grep for the same string in tests.
#[doc(hidden)]
pub const DEFAULT_OPERATION_APPLY_CHANGE_PANIC_MESSAGE: &str =
    "Operation declared a non-NoChange `type Change` but did not override `apply_change`. \
     Implement `fn apply_change(state: &mut Self::State, change: &Self::Change)` to fold \
     the Change into the State deterministically.";

/// One Operation in an UnboundedOperative topology. Stateless dispatcher; per-segment
/// State is held by the runtime and threaded through `run` via a
/// [`StateHandle`].
///
/// # Determinism contract
///
/// Replay is Flezha's recovery primitive: crash-resume, retry-replay, and
/// the effectively-once / rewind capabilities all re-run Operations over
/// restored state and rely on the results coming out the same. [`Self::run`],
/// [`Self::apply_change`], and [`Self::compensate`] MUST therefore be
/// deterministic: given the same Segment state and the same input sequence
/// they produce identical state changes and identical emitted outputs.
/// Ambient non-determinism inside these functions — `SystemTime::now()`,
/// random numbers / `Uuid::new_v4()`, reads of external mutable state — is
/// a contract violation. Obtain time via [`OperationCtx::now`] (served from
/// the declared [`Self::Clock`]) and identifiers via
/// [`OperationCtx::stable_id`]. `OperationTestHarness::replay_emits_equal`
/// verifies the output side of the contract; `replay_equals` the state side.
///
/// State persistence (added by `operative-state-persistence-rust`):
///
/// - [`Self::Change`] — associated type bounded on
///   `Serialize + DeserializeOwned + Send + Sync + 'static`. Default
///   [`NoChange`] opts out of persistence machinery.
/// - [`Self::Clock`] — sealed trait. Default [`WallClock`].
/// - [`Self::Config`] — typed startup config bound to `DeserializeOwned`.
///   Default `()` invokes `Self::default()`.
/// - [`Self::SCHEMA_VERSION`] — bump on non-additive Change-schema edits.
/// - [`Self::apply_change`] — fold a Change into the State; MUST be
///   deterministic; verified by `OperationTestHarness::replay_equals`.
/// - [`Self::compact_after_bytes`] — per-Segment WAL compaction
///   threshold reported to the controller at operative-start time. Default 64 MiB.
/// - [`Self::from_config`] — constructor from typed Config. Default
///   `Self::default()` for `Config = ()`.
pub trait Operation: Send + Sync + 'static + Default {
    /// Inter-operation transport is `Arc<Self::Input>`-zero-copy; the
    /// runtime hands the consumer the unique Arc when possible and
    /// clones the value when fan-out keeps the Arc shared. Requires
    /// `Clone` so the runtime can fall back to a value-clone in the
    /// shared-Arc case (`Arc::unwrap_or_clone`).
    type Input: Send + Sync + 'static + Clone;
    type Output: Send + Sync + 'static;
    /// Per-Segment state. Bounded on `Serialize + DeserializeOwned` so
    /// the runtime can serialize state for `SegmentState` /
    /// `MigrationSerialization` and seed it from `AddOperative.init_values`
    /// without going through the user's `init_state`.
    type State: Send + Sync + 'static + Serialize + DeserializeOwned;

    /// Persistent mutation record. Use [`NoChange`] (the uninhabited
    /// default) to opt out of persistence machinery — the runtime
    /// detects `NoChange` via TypeId and skips flush-buffer allocation.
    /// Stable Rust does not yet stabilize `associated_type_defaults`, so
    /// every impl declares this; persistence-free Operations write
    /// `type Change = NoChange;` explicitly.
    type Change: Change;

    /// Time source. Sealed trait; evaluated by the runtime once per `run`
    /// invocation and served via [`OperationCtx::now`]. The
    /// `ClockFor<Self::Input>` bound makes an unservable configuration
    /// (e.g. `EventTimeFrom<F>` whose `F` lacks a `TimeExtractor` for this
    /// Input) a compile error. Persistence-free Operations write
    /// `type Clock = WallClock;`.
    type Clock: ClockFor<Self::Input>;

    /// Startup-time configuration deserialized from operative-start metadata
    /// (postcard). Use `()` for the no-config default; impls write
    /// `type Config = ();` explicitly.
    type Config: DeserializeOwned + Send + Sync + 'static;

    /// Per-Operation schema version stamped into persisted Change headers.
    /// Bump on non-additive Change-schema edits.
    const SCHEMA_VERSION: u32 = 1;

    /// Stable identifier within the UnboundedOperative class. Must be unique per UnboundedOperative.
    fn name() -> &'static str;

    /// Per-segment state factory. Called once per Segment at UnboundedOperative
    /// startup. `operation_key` is `None` in single-segment mode and
    /// `Some(k)` for static mode where `k` is one of the values returned
    /// by `operation_keys()`.
    ///
    /// `init_payload` is `None` for cold starts. For warm restarts the
    /// runtime hands the user a borrowed view of the snapshot +
    /// change-log bytes; the user MAY inspect them for instrumentation
    /// but does not have to decode them — the runtime applies the
    /// payload automatically AFTER `init_state` returns.
    fn init_state(
        operation_key: Option<&str>,
        init_payload: Option<RestoreInitPayload<'_>>,
    ) -> Self::State;

    /// Fold a Change into the per-Segment state. Default panics for
    /// non-`NoChange` Change types; Operations declaring a Change MUST
    /// override. Determinism is convention-enforced; verified by
    /// `OperationTestHarness::replay_equals`.
    fn apply_change(_state: &mut Self::State, _change: &Self::Change) {
        if std::any::TypeId::of::<Self::Change>() == std::any::TypeId::of::<NoChange>() {
            // No-op: NoChange is uninhabited so this branch never runs
            // in practice; included for completeness.
            return;
        }
        panic!("{}", DEFAULT_OPERATION_APPLY_CHANGE_PANIC_MESSAGE);
    }

    /// Per-Segment WAL size threshold reported to the controller. The
    /// runtime never compacts on the SDK side — compaction is controller-
    /// driven per `operative-state-persistence-controller`. Default 8 MiB,
    /// in lockstep with the controller's `:default_compact_after_bytes`.
    fn compact_after_bytes() -> usize {
        8 * 1024 * 1024
    }

    /// Constructor from typed Config. Default invokes `Self::default()`,
    /// which requires `Config = ()` (`Self: Default` is already part of
    /// the trait bound).
    fn from_config(_cfg: Self::Config) -> Self {
        // The default impl is monomorphized per Operation; Operations with a
        // non-`()` Config must override.
        Self::default()
    }

    /// Static-segment declaration. Empty (the default) means
    /// single-segment mode; non-empty enables static segmenting.
    fn operation_keys() -> Vec<&'static str> {
        Vec::new()
    }

    /// Static-mode segment routing. For each input, returns the segment
    /// keys that should receive it. Empty vec drops the input. Single-mode
    /// operations inherit the default `vec![]`; the runtime skips this call
    /// for them.
    ///
    /// Static-mode operations (`operation_keys()` non-empty) MUST override this
    /// AND set `SEGMENT_FOR_OVERRIDDEN = true`; the topology builder
    /// rejects the operative otherwise.
    fn segment_for(_input: &Self::Input) -> Vec<&'static str> {
        Vec::new()
    }

    /// Override-detection sentinel. Default `false`; static-mode Operations
    /// MUST set this to `true` alongside their `segment_for` override.
    /// `build_topology` checks the const+`operation_keys()` consistency at
    /// startup and rejects mismatched declarations.
    const SEGMENT_FOR_OVERRIDDEN: bool = false;

    /// Dynamic-mode segment routing. Called per input; the returned key
    /// selects (lazily creating) the Segment, `None` drops the input. A
    /// dynamic-mode Operation overrides this AND sets `OPERATION_KEY_OVERRIDDEN =
    /// true` with empty `operation_keys()`; declaring both modes is a
    /// registration error.
    fn operation_key(_input: &Self::Input) -> Option<String> {
        None
    }

    /// Override-detection sentinel for [`Self::operation_key`] (dynamic mode).
    const OPERATION_KEY_OVERRIDDEN: bool = false;

    /// Dynamic-mode idle eviction window. `None` (default) = no eviction:
    /// Segments live as long as the UnboundedOperative, like static ones. `Some(ttl)`
    /// opts a per-Operation reaper in; eviction is destructive (in-memory slot +
    /// on-disk WAL/snapshot dropped) and a later input cold-starts fresh —
    /// no restore-from-disk. Ignored for single/static Operations.
    fn segment_idle_ttl() -> Option<std::time::Duration> {
        None
    }

    /// Dynamic-mode soft cap on live Segments per Operation. At the cap,
    /// new-key inputs are dropped (`inputs_dropped_capped_total`); existing
    /// keys are unaffected. Bounds memory against a runaway `operation_key`, not
    /// admission — no error path back to the controller.
    fn max_segments() -> usize {
        100_000
    }

    /// Override-detection sentinel for [`Self::max_segments`].
    const MAX_SEGMENTS_OVERRIDDEN: bool = false;

    /// Per-Segment mailbox capacity. The consumer Segment allocates
    /// one bounded ring buffer of this size at operative startup. Static-mode
    /// Operations allocate one mailbox per declared segment key, all sharing
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
    /// `register_unbounded::<R>()` with the offending Operation name.
    fn mailbox_capacity() -> usize {
        Self::DEFAULT_MAILBOX_CAPACITY
    }

    /// Default mailbox capacity used by [`Self::mailbox_capacity`].
    const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

    /// Override-detection sentinel for `mailbox_capacity`. Default `false`;
    /// implementors that override `mailbox_capacity()` MUST set this to
    /// `true` so the runtime applies the Operation value over any
    /// `FLEZHA_MAILBOX_CAPACITY` env override.
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = false;

    /// Compute. Receives the typed input, a [`StateHandle`] to read state
    /// via `Deref` and mutate via `state.apply(change)`, and a context for
    /// emitting outputs. Producing zero outputs == no `ctx.emit(...)` call.
    /// Returning Err logs against `(operative, operation, segment)` and discards
    /// any partial emits.
    fn run(
        &mut self,
        input: Self::Input,
        state: StateHandle<'_, Self>,
        ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError>;

    /// Optional saga **compensation** hook (bounded operatives only). An
    /// operation whose `run` commits a reversible side effect declares this to
    /// undo it. The bounded driver fires it ONLY during abnormal-termination
    /// rollback — never on a clean `ok`, never for an unbounded operative — once
    /// per segment that ran, in reverse completion order, handing back that
    /// segment's own collected outputs (zero-copy `Arc`s) and its
    /// [`StateHandle`], so it can reverse precisely. Compensation MUST be
    /// idempotent by author contract: a crash mid-rollback may re-invoke it.
    ///
    /// The default is a no-op; an operation that overrides this MUST also set
    /// [`COMPENSATE_OVERRIDDEN`](Self::COMPENSATE_OVERRIDDEN) `= true` so the
    /// bounded driver registers it as compensable (hookless operations are
    /// skipped during rollback).
    fn compensate(
        &mut self,
        _completed_output: Vec<Arc<Self::Output>>,
        _state: StateHandle<'_, Self>,
        _ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        Ok(())
    }

    /// Override-detection sentinel for [`Self::compensate`]. Default `false`; an
    /// operation that overrides `compensate` MUST set this to `true` so the
    /// bounded driver knows the operation participates in saga rollback.
    const COMPENSATE_OVERRIDDEN: bool = false;
}

#[cfg(test)]
mod tests;
