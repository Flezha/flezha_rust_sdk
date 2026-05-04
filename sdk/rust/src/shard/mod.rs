//! Shard trait + ShardCtx + ShardError per `relay-architecture/design.md`
//! Decision 1 and `relay-mvp-rust/design.md` Decision 1.

pub mod ctx;
pub mod error;

pub use ctx::ShardCtx;
pub use error::ShardError;

pub(crate) use ctx::EmitSink;

/// One Shard in a Relay topology. Stateless dispatcher; per-partition State
/// is held by the runtime and threaded through `run`.
///
/// Decision 1 rationale: associated types (vs generics on the trait) keep
/// `impl Shard for OhlcShard {}` clean; sync `run` matches today's mailbox
/// semantics. Async would only matter once bounded mailboxes land
/// (`relay-bounded-mailbox`).
pub trait Shard: Send + Sync + 'static + Default {
    /// Inter-shard transport is `Arc<Self::Input>`-zero-copy; the
    /// runtime hands the consumer the unique Arc when possible and
    /// clones the value when fan-out keeps the Arc shared. Requires
    /// `Clone` so the runtime can fall back to a value-clone in the
    /// shared-Arc case (`Arc::unwrap_or_clone`).
    type Input: Send + Sync + 'static + Clone;
    type Output: Send + Sync + 'static;
    type State: Send + Sync + 'static;

    /// Stable identifier within the Relay class. Must be unique per Relay.
    fn name() -> &'static str;

    /// Per-partition state factory. Called once per Partition at Relay
    /// startup. `shard_key` is `None` in single-partition mode and
    /// `Some(k)` for static mode where `k` is one of the values returned
    /// by `shard_keys()`.
    fn init_state(shard_key: Option<&str>) -> Self::State;

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

    /// Compute. Receives the typed input, mutable state for the
    /// partition, and a context for emitting outputs. Producing zero
    /// outputs == no `ctx.emit(...)` call. Returning Err logs against
    /// `(relay, shard, partition)` and discards any partial emits.
    fn run(
        &mut self,
        input: Self::Input,
        state: &mut Self::State,
        ctx: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError>;
}
