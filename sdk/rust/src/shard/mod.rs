//! Shard trait + ShardCtx + ShardError per `relay-architecture/design.md`
//! Decision 1, `relay-mvp-rust/design.md` Decision 1, and
//! `relay-state-persistence-rust/design.md` Decisions 1, 4, 5, 8, 9.

pub mod change;
pub mod clock;
pub mod ctx;
pub mod error;
pub mod state_handle;

pub use change::{Change, CoalesceKey, NoChange};
pub use clock::{Clock, EventTimeFrom, LogicalClock, WallClock};
#[allow(unused_imports)]
pub(crate) use ctx::EmitSink;
pub use ctx::ShardCtx;
pub use error::ShardError;
use serde::de::DeserializeOwned;
use serde::Serialize;
pub use state_handle::StateHandle;

use crate::partition::restore::RestoreInitPayload;

/// Default panic message stamped on `Shard::apply_change` for Shards
/// that declare a non-`NoChange` Change without overriding
/// `apply_change`. Public-but-hidden so the runtime's PartitionFlusher
/// can grep for the same string in tests.
#[doc(hidden)]
pub const DEFAULT_SHARD_APPLY_CHANGE_PANIC_MESSAGE: &str =
    "Shard declared a non-NoChange `type Change` but did not override `apply_change`. \
     Implement `fn apply_change(state: &mut Self::State, change: &Self::Change)` to fold \
     the Change into the State deterministically.";

/// One Shard in a Relay topology. Stateless dispatcher; per-partition
/// State is held by the runtime and threaded through `run` via a
/// [`StateHandle`].
///
/// State persistence (added by `relay-state-persistence-rust`):
///
/// - [`Self::Change`] — associated type bounded on
///   `Serialize + DeserializeOwned + Send + Sync + 'static`. Default
///   [`NoChange`] opts out of persistence machinery.
/// - [`Self::Clock`] — sealed trait. Default [`WallClock`].
/// - [`Self::Config`] — typed startup config bound to `DeserializeOwned`.
///   Default `()` invokes `Self::default()`.
/// - [`Self::SCHEMA_VERSION`] — bump on non-additive Change-schema edits.
/// - [`Self::apply_change`] — fold a Change into the State; MUST be
///   deterministic; verified by `ShardTestHarness::replay_equals`.
/// - [`Self::compact_after_bytes`] — per-Partition WAL compaction
///   threshold reported to the controller at relay-start time. Default 64 MiB.
/// - [`Self::from_config`] — constructor from typed Config. Default
///   `Self::default()` for `Config = ()`.
pub trait Shard: Send + Sync + 'static + Default {
    /// Inter-shard transport is `Arc<Self::Input>`-zero-copy; the
    /// runtime hands the consumer the unique Arc when possible and
    /// clones the value when fan-out keeps the Arc shared. Requires
    /// `Clone` so the runtime can fall back to a value-clone in the
    /// shared-Arc case (`Arc::unwrap_or_clone`).
    type Input: Send + Sync + 'static + Clone;
    type Output: Send + Sync + 'static;
    /// Per-Partition state. Bounded on `Serialize + DeserializeOwned` so
    /// the runtime can serialize state for `PartitionState` /
    /// `MigrationSerialization` and seed it from `AddRelay.init_values`
    /// without going through the user's `init_state`.
    type State: Send + Sync + 'static + Serialize + DeserializeOwned;

    /// Persistent mutation record. Use [`NoChange`] (the uninhabited
    /// default) to opt out of persistence machinery — the runtime
    /// detects `NoChange` via TypeId and skips flush-buffer allocation.
    /// Stable Rust does not yet stabilize `associated_type_defaults`, so
    /// every impl declares this; persistence-free Shards write
    /// `type Change = NoChange;` explicitly.
    type Change: Change;

    /// Time source. Sealed trait; v1 fully wires only [`WallClock`].
    /// Persistence-free Shards write `type Clock = WallClock;`.
    type Clock: Clock;

    /// Startup-time configuration deserialized from relay-start metadata
    /// (postcard). Use `()` for the no-config default; impls write
    /// `type Config = ();` explicitly.
    type Config: DeserializeOwned + Send + Sync + 'static;

    /// Per-Shard schema version stamped into persisted Change headers.
    /// Bump on non-additive Change-schema edits.
    const SCHEMA_VERSION: u32 = 1;

    /// Stable identifier within the Relay class. Must be unique per Relay.
    fn name() -> &'static str;

    /// Per-partition state factory. Called once per Partition at Relay
    /// startup. `shard_key` is `None` in single-partition mode and
    /// `Some(k)` for static mode where `k` is one of the values returned
    /// by `shard_keys()`.
    ///
    /// `init_payload` is `None` for cold starts. For warm restarts the
    /// runtime hands the user a borrowed view of the snapshot +
    /// change-log bytes; the user MAY inspect them for instrumentation
    /// but does not have to decode them — the runtime applies the
    /// payload automatically AFTER `init_state` returns.
    fn init_state(
        shard_key: Option<&str>,
        init_payload: Option<RestoreInitPayload<'_>>,
    ) -> Self::State;

    /// Fold a Change into the per-Partition state. Default panics for
    /// non-`NoChange` Change types; Shards declaring a Change MUST
    /// override. Determinism is convention-enforced; verified by
    /// `ShardTestHarness::replay_equals`.
    fn apply_change(_state: &mut Self::State, _change: &Self::Change) {
        if std::any::TypeId::of::<Self::Change>() == std::any::TypeId::of::<NoChange>() {
            // No-op: NoChange is uninhabited so this branch never runs
            // in practice; included for completeness.
            return;
        }
        panic!("{}", DEFAULT_SHARD_APPLY_CHANGE_PANIC_MESSAGE);
    }

    /// Per-Partition WAL size threshold reported to the controller. The
    /// runtime never compacts on the SDK side — compaction is controller-
    /// driven per `relay-state-persistence-controller`. Default 64 MiB.
    fn compact_after_bytes() -> usize {
        64 * 1024 * 1024
    }

    /// Constructor from typed Config. Default invokes `Self::default()`,
    /// which requires `Config = ()` (`Self: Default` is already part of
    /// the trait bound).
    fn from_config(_cfg: Self::Config) -> Self {
        // The default impl is monomorphized per Shard; Shards with a
        // non-`()` Config must override.
        Self::default()
    }

    /// Static-partition declaration. Empty (the default) means
    /// single-partition mode; non-empty enables static partitioning.
    fn shard_keys() -> Vec<&'static str> {
        Vec::new()
    }

    /// Static-mode partition routing. For each input, returns the partition
    /// keys that should receive it. Empty vec drops the input. Single-mode
    /// shards inherit the default `vec![]`; the runtime skips this call
    /// for them.
    ///
    /// Static-mode shards (`shard_keys()` non-empty) MUST override this
    /// AND set `PARTITION_FOR_OVERRIDDEN = true`; the topology builder
    /// rejects the relay otherwise.
    fn partition_for(_input: &Self::Input) -> Vec<&'static str> {
        Vec::new()
    }

    /// Override-detection sentinel. Default `false`; static-mode Shards
    /// MUST set this to `true` alongside their `partition_for` override.
    /// `build_topology` checks the const+`shard_keys()` consistency at
    /// startup and rejects mismatched declarations.
    const PARTITION_FOR_OVERRIDDEN: bool = false;

    /// Reserved for `relay-dynamic-partitioning`. v1 SHALL NOT call this
    /// on the hot path; the runtime rejects a Relay whose Shard
    /// overrides this without also setting `shard_keys()`.
    fn shard_key(_input: &Self::Input) -> Option<String> {
        None
    }

    /// Override-detection sentinel for `shard_key`. Default `false`;
    /// implementors that declare dynamic-mode partitioning MUST set
    /// this to `true` AND populate `shard_keys()`. The topology builder
    /// rejects `SHARD_KEY_OVERRIDDEN = true` with empty `shard_keys()`
    /// per umbrella Decision 7.
    const SHARD_KEY_OVERRIDDEN: bool = false;

    /// Per-Partition mailbox capacity. The consumer Partition allocates
    /// one bounded ring buffer of this size at relay startup. Static-mode
    /// Shards allocate one mailbox per declared partition key, all sharing
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
    /// `register_relay::<R>()` with the offending Shard name.
    fn mailbox_capacity() -> usize {
        Self::DEFAULT_MAILBOX_CAPACITY
    }

    /// Default mailbox capacity used by [`Self::mailbox_capacity`].
    const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

    /// Override-detection sentinel for `mailbox_capacity`. Default `false`;
    /// implementors that override `mailbox_capacity()` MUST set this to
    /// `true` so the runtime applies the Shard value over any
    /// `FLEZHA_MAILBOX_CAPACITY` env override.
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = false;

    /// Compute. Receives the typed input, a [`StateHandle`] to read state
    /// via `Deref` and mutate via `state.apply(change)`, and a context for
    /// emitting outputs. Producing zero outputs == no `ctx.emit(...)` call.
    /// Returning Err logs against `(relay, shard, partition)` and discards
    /// any partial emits.
    fn run(
        &mut self,
        input: Self::Input,
        state: StateHandle<'_, Self>,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError>;
}

#[cfg(test)]
mod tests;
