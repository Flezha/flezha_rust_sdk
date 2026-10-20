//! The `Tasklet` trait, its context and its errors.

pub mod change;
pub mod clock;
pub mod ctx;
pub mod error;

use std::sync::Arc;

pub use change::{Change, CoalesceKey, JournalValue, NoChange};
pub use clock::ClockKind;
#[allow(unused_imports)]
pub(crate) use ctx::EmitSink;
pub use ctx::{Ctx, StableId};
pub use error::TaskletError;
pub use flz_core::spawn::{
    IfRunning, SpawnError, SpawnHandle, SpawnOptions, SpawnOutcome, SpawnResult,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Default panic message stamped on `Tasklet::on_timer` for Tasklets that
/// register a timer without handling it. Registration is a runtime call, so
/// the omission cannot be caught at build time.
#[doc(hidden)]
pub const DEFAULT_TASKLET_ON_TIMER_PANIC_MESSAGE: &str =
    "Tasklet registered a timer but did not write `on_timer`. Write \
     `fn on_timer(&mut self, firing: TimerFiring, ctx: &mut Ctx<'_, Self>)` \
     to handle the firing.";

pub use flz_core::relay::topology::DEFAULT_COMPACT_AFTER_BYTES;
/// Used by [`Tasklet::max_timers_per_cell`].
pub use flz_core::timer::DEFAULT_MAX_TIMERS_PER_CELL;

/// One tasklet: the author's own type, whose `run` the runtime calls for each
/// input a Cell receives. The Cell's state is held by the runtime and reached
/// through the [`Ctx`] every call takes.
///
/// The everyday form writes `#[flezha::tasklet(name = "...")]` on the
/// `impl Tasklet for MyType` block. The attribute generates no type: it only
/// adds the items the block leaves out ([`Self::NAME`], [`Self::CLOCK`], the
/// settings given as keys, the `*_OVERRIDDEN` flags from the hooks the block
/// writes, a default [`Self::init_state`], and `State = ()`,
/// `Change = NoChange`, `Output = ()`). A block without the attribute writes
/// every one of those items itself.
///
/// # Determinism contract
///
/// Replay is Flezha's recovery primitive: crash-resume, retry-replay, and
/// the effectively-once / rewind capabilities all re-run tasklets over
/// restored state and rely on the results coming out the same. [`Self::run`],
/// [`Change::apply_to`] and [`Self::compensate`] MUST therefore be
/// deterministic: given the same Cell state and the same input sequence
/// they produce identical state changes and identical emitted outputs.
/// Ambient non-determinism inside these functions (`SystemTime::now()`,
/// random numbers, `Uuid::new_v4()`, reads of external mutable state) is
/// a contract violation. Obtain time via [`Ctx::now`] and identifiers via
/// [`Ctx::stable_id`]. A call to anything outside the instance (a model, a
/// tool, a service) in `run` or `compensate` goes through [`Ctx::effect`],
/// which keeps its result so a re-run reads the same value; `apply_to` makes
/// no outside call at all. `TaskletTestHarness::replay_emits_equal` verifies
/// the output side of the contract, serving each kept result on its replay
/// pass; `replay_equals` the state side.
pub trait Tasklet: Send + Sync + 'static + Default {
    /// Inter-tasklet transport is `Arc<Self::Input>`-zero-copy; the
    /// runtime hands the consumer the unique Arc when possible and
    /// clones the value when fan-out keeps the Arc shared. Requires
    /// `Clone` so the runtime can fall back to a value-clone in the
    /// shared-Arc case (`Arc::unwrap_or_clone`).
    type Input: Send + Sync + 'static + Clone;
    /// Per-cell output value, handed to every consumer of this node's
    /// channel. Bound on [`JournalValue`] — a steps journals the handoff
    /// between two tasklets, so the runtime derives the channel's byte
    /// codec from this type and no declaration exists to forget.
    ///
    /// `Input` carries no such bound: an entry value is parsed by the
    /// author's own parser, and a consumer's input is some producer's output
    /// anyway, so the requirement reaches it where it matters. `State` keeps
    /// its raw serde bounds, which say the same thing for a different reader.
    type Output: Send + Sync + 'static + JournalValue;
    /// Per-Cell state. Steps relay on `Serialize + DeserializeOwned` so
    /// the runtime can serialize state for `CellState` /
    /// `MigrationSerialization` and seed it from `AddRelay.init_values`
    /// without going through the user's `init_state`.
    type State: Send + Sync + 'static + Serialize + DeserializeOwned;

    /// The change this tasklet records against its state, applied with
    /// [`Ctx::change`] and folded back on restore through
    /// [`Change::apply_to`]. [`NoChange`] keeps no state: the runtime
    /// detects it via `TypeId` and allocates no flush buffer.
    type Change: Change<Self::State>;

    /// The tasklet's name within its relay, unique there. Saved state is
    /// keyed by it, never by the Rust type, so renaming the type keeps the
    /// state.
    const NAME: &'static str;

    /// Where `ctx.now()` comes from: `EventTime` when the tasklet writes
    /// [`Self::event_time`], `Logical` for the durable change seq, `Wall`
    /// otherwise. `#[flezha::tasklet]` sets it from what the block writes.
    const CLOCK: ClockKind;

    /// Per-Tasklet schema version stamped into every persisted record this
    /// Tasklet writes — change records and state snapshots alike.
    ///
    /// Bump it on any non-additive edit to [`Self::State`] **or**
    /// [`Self::Change`]. Both are encoded positionally by a codec that is not
    /// self-describing, so reordering fields, removing one, or changing a
    /// field's type is not a compatibility question the codec can answer: v1
    /// bytes read as v2 either fail mid-buffer or decode into the wrong
    /// fields. The version is what turns that into a decision instead of a
    /// silent corruption.
    ///
    /// A bump alone refuses the old records. Declare
    /// [`Self::MIN_READABLE_SCHEMA_VERSION`] and the migration entry points to
    /// read them.
    const SCHEMA_VERSION: u32 = 1;

    /// The oldest `SCHEMA_VERSION` whose persisted records this Tasklet can
    /// still read. Defaults to [`Self::SCHEMA_VERSION`] — "I read only what I
    /// wrote".
    ///
    /// Lowering it is a promise that [`Self::migrate_state`] and
    /// [`Self::migrate_change`] handle every version in
    /// `MIN_READABLE_SCHEMA_VERSION .. SCHEMA_VERSION`. A record below the
    /// floor, and any record stamped ABOVE `SCHEMA_VERSION` (older code
    /// meeting newer state), is refused rather than guessed at.
    const MIN_READABLE_SCHEMA_VERSION: u32 = Self::SCHEMA_VERSION;

    /// Rebuild [`Self::State`] from a snapshot written at an older
    /// `SCHEMA_VERSION`. Invoked only for records in
    /// `MIN_READABLE_SCHEMA_VERSION .. SCHEMA_VERSION`; `bytes` is the
    /// snapshot body exactly as that version wrote it.
    ///
    /// Keep the retired type in your own source and decode it here — the
    /// runtime knows only the types this binary still declares:
    ///
    /// ```ignore
    /// fn migrate_state(from_version: u32, bytes: &[u8]) -> Result<Cart, String> {
    ///     match from_version {
    ///         1 => {
    ///             #[derive(serde::Deserialize)]
    ///             struct CartV1 { items: u32 }
    ///             let old: CartV1 = postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
    ///             Ok(Cart { items: old.items, currency: "EUR".into() })
    ///         }
    ///         v => Err(format!("no migration from {v}")),
    ///     }
    /// }
    /// ```
    ///
    /// Must be deterministic: crash-resume, retry-replay and rewind all
    /// re-enter the restore fold, and the same bytes must produce the same
    /// state every time, the contract [`Change::apply_to`] already carries.
    fn migrate_state(from_version: u32, _bytes: &[u8]) -> Result<Self::State, String> {
        Err(format!(
            "tasklet `{}` declares no state migration from schema_version {from_version}",
            Self::NAME
        ))
    }

    /// Rebuild [`Self::Change`] from a change record written at an older
    /// `SCHEMA_VERSION`. Same contract as [`Self::migrate_state`]; a WAL
    /// legitimately holds records from more than one version, so this is
    /// invoked per record rather than once per restore.
    fn migrate_change(from_version: u32, _bytes: &[u8]) -> Result<Self::Change, String> {
        Err(format!(
            "tasklet `{}` declares no change migration from schema_version {from_version}",
            Self::NAME
        ))
    }

    /// Per-cell state factory, called once for every new Cell: `cell_key` is
    /// `None` in single-cell mode and the Cell's key otherwise. On a warm
    /// restart the runtime applies the persisted snapshot and change log after
    /// this returns. `#[flezha::tasklet]` writes it as `Default::default()`
    /// when the block does not.
    fn init_state(cell_key: Option<&str>) -> Self::State;

    /// The event time of `input`, served as `ctx.now()` and the clock
    /// event-time timers fire against. Writing it is what makes
    /// `#[flezha::tasklet]` declare [`ClockKind::EventTime`]. It MUST depend
    /// only on the input. The default panics naming the tasklet, since only an
    /// event-time clock calls it.
    fn event_time(_input: &Self::Input) -> u64 {
        panic!(
            "tasklet `{}` declares an event-time clock but does not write `event_time`",
            Self::NAME
        );
    }

    /// Per-Cell WAL size threshold reported to the controller. The
    /// runtime never compacts on the SDK side — compaction is controller-
    /// driven per `relay-state-persistence`. Default 8 MiB,
    /// in lockstep with the controller's `:default_compact_after_bytes`.
    fn compact_after_bytes() -> usize {
        DEFAULT_COMPACT_AFTER_BYTES
    }

    /// Static-cell declaration. Empty (the default) means
    /// single-cell mode; non-empty enables static cell.
    fn cell_keys() -> Vec<&'static str> {
        Vec::new()
    }

    /// Static-mode cell routing. For each input, returns the cell
    /// keys that should receive it. Empty vec drops the input. Single-mode
    /// tasklets inherit the default `vec![]`; the runtime skips this call
    /// for them.
    ///
    /// Static-mode tasklets (`cell_keys()` non-empty) MUST write this;
    /// `#[flezha::tasklet]` then sets `CELLS_FOR_OVERRIDDEN`, and the topology
    /// builder refuses the relay otherwise.
    fn cells_for(_input: &Self::Input) -> Vec<&'static str> {
        Vec::new()
    }

    /// Whether the tasklet writes [`Self::cells_for`]. `#[flezha::tasklet]`
    /// derives it; `build_topology` checks it against `cell_keys()` at startup
    /// and refuses mismatched declarations.
    const CELLS_FOR_OVERRIDDEN: bool = false;

    /// Dynamic-mode idle eviction window. `None` (default) = no eviction:
    /// Cells live as long as the Relay, like static ones. `Some(ttl)`
    /// opts a per-Tasklet reaper in; eviction is destructive (in-memory slot +
    /// on-disk WAL/snapshot dropped) and a later input cold-starts fresh —
    /// no restore-from-disk. Ignored for single/static Tasklets.
    fn idle_ttl() -> Option<std::time::Duration> {
        None
    }

    /// Dynamic-mode soft cap on live Cells per Tasklet. At the cap,
    /// new-key inputs are dropped (`inputs_dropped_capped_total`); existing
    /// keys are unaffected. Bounds memory against a runaway `.key_by(...)`,
    /// not admission — no error path back to the controller.
    fn max_cells() -> usize {
        100_000
    }

    /// Whether the tasklet sets [`Self::max_cells`].
    const MAX_CELLS_OVERRIDDEN: bool = false;

    /// Per-Cell mailbox capacity. The consumer Cell allocates
    /// one bounded ring buffer of this size at relay startup. Static-mode
    /// Tasklets allocate one mailbox per declared cell key, all sharing
    /// the same capacity.
    ///
    /// Override precedence at registration time:
    ///   1. This method's value, if `MAILBOX_CAPACITY_OVERRIDDEN = true`.
    ///   2. `FLEZHA_MAILBOX_CAPACITY` env var (parsed as `usize`).
    ///   3. The built-in default (`DEFAULT_MAILBOX_CAPACITY`).
    ///
    /// `#[flezha::tasklet]` sets the flag from the `mailbox_capacity` key or a written
    /// `mailbox_capacity`, so the runtime can tell an explicit value equal to
    /// the default from the inherited default. Capacity must be in `1..=1_048_576`; out-of-range values fail
    /// `register_factory(R)` with the offending Tasklet name.
    fn mailbox_capacity() -> usize {
        Self::DEFAULT_MAILBOX_CAPACITY
    }

    /// Default mailbox capacity used by [`Self::mailbox_capacity`].
    const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

    /// Whether the tasklet sets [`Self::mailbox_capacity`], so the runtime
    /// applies it over any `FLEZHA_MAILBOX_CAPACITY` env override.
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = false;

    /// Compute. Receives the typed input and the context, which reads the
    /// state with `ctx.state()`, changes it with `ctx.change(..)` and emits
    /// with `ctx.emit(..)`. Producing zero outputs is calling no emit.
    /// Returning Err logs against `(relay, tasklet, cell)` and discards
    /// any partial emits.
    fn run(&mut self, input: Self::Input, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError>;

    /// Handle a timer this tasklet registered via
    /// [`Ctx::timer_at_event_time`] / [`Ctx::timer_after`]. Receives the same
    /// [`Ctx`] as [`Self::run`], so it may change state, emit, and register
    /// further timers.
    ///
    /// `firing.scheduled_at` is the timestamp the timer was **scheduled at**,
    /// never the wall clock at which it actually fired: a processing-time timer
    /// set for T that fires at T+δ still reports T. Passing T+δ would make a
    /// replay observe a different value and break the determinism contract in
    /// exactly the way `SystemTime::now()` inside `run` does. For the same
    /// reason [`Ctx::now`] equals `firing.scheduled_at` inside this call, so a chained
    /// `ctx.timer_after(..)` schedules off the timer that woke it.
    ///
    /// A failure here climbs the input channel's retry ladder and parks to the
    /// dead-letter queue exactly as a failing `run` does. The fired timer's
    /// removal is durable either way, so a poison timer parks once instead of
    /// firing forever.
    ///
    /// The default panics naming the tasklet: registering a timer without
    /// handling it is an authoring error, and registration is a runtime call,
    /// so no build-time check can catch it.
    fn on_timer(
        &mut self,
        _firing: crate::cell::timer::TimerFiring,
        _ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        panic!(
            "{} (tasklet `{}`)",
            DEFAULT_TASKLET_ON_TIMER_PANIC_MESSAGE,
            Self::NAME
        );
    }

    /// Per-Cell bound on pending timers. Registering past it returns
    /// [`TaskletError::TimerCapExceeded`] naming this Tasklet and the
    /// bound, rather than allocating. Mirrors the soft cap
    /// [`Self::max_cells`] puts on dynamic Cells.
    fn max_timers_per_cell() -> usize {
        DEFAULT_MAX_TIMERS_PER_CELL
    }

    /// Optional saga **compensation** hook (steps relays only). A
    /// tasklet whose `run` commits a reversible side effect writes this to
    /// undo it. The steps relay fires it ONLY during abnormal-termination
    /// rollback (never on a clean `ok`, never for a flow relay), once
    /// per cell that ran, in reverse completion order, handing back that
    /// cell's own collected outputs (zero-copy `Arc`s) and its [`Ctx`], so it
    /// can reverse precisely. Compensation MUST be idempotent by author
    /// contract: a crash mid-rollback may re-invoke it.
    ///
    /// The default is a no-op. Writing it in a `#[flezha::tasklet]` block sets
    /// [`COMPENSATE_OVERRIDDEN`](Self::COMPENSATE_OVERRIDDEN), which is what
    /// makes the steps relay register the tasklet as compensable.
    fn compensate(
        &mut self,
        _completed_output: Vec<Arc<Self::Output>>,
        _ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }

    /// Whether the tasklet writes [`Self::compensate`]; hookless tasklets are
    /// skipped during rollback.
    const COMPENSATE_OVERRIDDEN: bool = false;
}

#[cfg(test)]
mod tests;
