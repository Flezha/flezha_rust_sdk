//! Bounded-operator authoring surface. The run, saga machinery and spec
//! kernel live in `flz_core::bounded` and are re-exported here; this module
//! keeps the author-facing `BoundedOperator` trait (and the run's scenario
//! suite).

pub use flz_core::bounded::*;
pub mod spec;

use std::time::Duration;

use crate::channel::ChannelBuilder;

/// One bounded operator class: a static DAG of Tasklets/Channels/Cells
/// declared in [`channels`](Self::channels), on the same graph builder a
/// unbounded operator uses. A tasklet ends its instance with
/// [`TaskletCtx::complete`](crate::TaskletCtx::complete). Capabilities
/// attach to the kind, not here; choosing the bounded kind is sufficient.
/// A worked example: `sdk/rust/examples/spawn_bounded_worker.rs`.
pub trait BoundedOperator: Send + Sync + 'static {
    /// Stable, controller-facing factory identifier.
    fn factory_id() -> &'static str;

    /// The DAG, declared as an unbounded operator declares its graph. The bounded operator runs it
    /// in topological order: a tasklet becomes runnable only after
    /// **every** producer feeding it has committed — fan-out branches
    /// interleave independently, and a fan-in node is dispatched once, with
    /// the accumulated batch of all its producers. Cycles are refused at
    /// registration, and so is a class that declares no tasklet. A tasklet's
    /// retry ladder is the edge's (`retry_max` / `retry_backoff_ms`), as on a
    /// stream; the unbounded-only settings (`effectively_once`, the checkpoint
    /// flags, `correction_window`) are refused at registration.
    fn channels(c: &ChannelBuilder);

    /// Optional wall-clock backstop. `None` (the default) means no duration
    /// bound; when `Some`, the SDK cooperatively cancels in-flight work once
    /// it elapses.
    fn max_duration() -> Option<Duration> {
        None
    }

    /// Self-termination guarantee, validated at registration. Default `true`:
    /// the operator ends on its own (a chain that finishes, a tasklet's
    /// `complete`, or a loop that runs out of work). An operator whose **only**
    /// possible exit is an external stop sets this `false`; combined with no
    /// [`max_duration`](Self::max_duration) it is **refused** at registration
    /// with a message directing the author to add a terminal condition.
    fn self_terminating() -> bool {
        true
    }

    /// Loop shape of a structured chain (`bounded-durable-loop`). The default
    /// [`LoopMode::Once`] is the single pass. A looping class commits every
    /// iteration durably — journal + boundary snapshot + outbox flush — so a
    /// crash resumes at the iteration after the last committed one, and
    /// [`RollbackMode::KeepProgress`] keeps every committed iteration.
    fn loop_mode() -> LoopMode {
        LoopMode::Once
    }

    /// Saga rollback scope, applied when the operator terminates abnormally
    /// after partial progress. Default [`RollbackMode::AllOrNothing`] undoes
    /// every completed tasklet in reverse, back to the start;
    /// [`RollbackMode::KeepProgress`] keeps each finished loop iteration and
    /// undoes only the in-flight one. The mode is irrelevant for a single-pass
    /// chain (where both behave all-or-nothing) and for an operator with no
    /// compensation hooks.
    fn rollback_mode() -> RollbackMode {
        RollbackMode::AllOrNothing
    }

    /// Class-level persistence toggle (durable by default, mirroring
    /// [`UnboundedOperator::PERSISTENCE_ENABLED`](crate::operator::UnboundedOperator::PERSISTENCE_ENABLED)).
    /// When `true`, a structured bounded operator's per-Cell Changes are
    /// written to the WAL as they apply, so a crash resumes from persisted
    /// state. When `false`, the per-Cell flush drains and discards Changes
    /// with no serialization or wire I/O — the operator is in-memory only
    /// (a crash re-runs from the top). The class announces it to the
    /// controller, which resolves it with a per-class user override.
    const PERSISTENCE_ENABLED: bool = true;

    /// Sleep when idle: an instance waiting at a committed boundary for
    /// `FLEZHA_PARK_IDLE_MS` parks, is evicted from the runner and wakes on
    /// its next message or at the earliest processing-time timer its
    /// tasklets registered. On whenever persistence is;
    /// declaring it with [`Self::PERSISTENCE_ENABLED`] off is refused as
    /// `SleepWithoutPersistence`.
    const SLEEP_WHEN_IDLE: bool = Self::PERSISTENCE_ENABLED;

    /// Synchronous remote durability (`remote-durability-tier`): the
    /// controller completes the remote ship of each commit barrier — the
    /// task-journal suffix and the inbox the barrier consumed — before it
    /// acknowledges the barrier. The acknowledgement a tasklet's outputs wait
    /// for therefore implies the commit survives losing the controller's disk
    /// (RPO 0), at the cost of one remote round trip per barrier.
    ///
    /// Inert unless the controller has a durability backend configured, and
    /// available to bounded classes only: an unbounded class acknowledges at
    /// input rates, where a remote round trip per acknowledgement is not a
    /// guarantee anyone can keep.
    const SYNC_DURABILITY: bool = false;
}

#[cfg(test)]
mod tests;
