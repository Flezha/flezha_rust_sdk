//! Workflow-operative authoring surface. The driver, saga machinery,
//! registry and spec kernel live in `flz_core::workflow` and are re-exported
//! here; this module keeps the author-facing `WorkflowOperative` trait (and
//! the driver scenario suite).

pub use flz_core::workflow::*;
pub mod spec;

use std::time::Duration;

pub use flz_core::workflow::{compensation, driver, instance, registry};

use crate::channel::ChannelBuilder;

/// One workflow operative class: a static DAG of Tasklets/Channels/Cells
/// declared in [`tasklets`](Self::tasklets), on the same graph builder a
/// stream operative uses. A tasklet ends its instance with
/// [`TaskletCtx::complete`](crate::TaskletCtx::complete). Capabilities
/// attach to the kind, not here; choosing the workflow kind is sufficient.
/// A worked example: `sdk/rust/examples/spawn_workflow_worker.rs`.
pub trait WorkflowOperative: Send + Sync + 'static {
    /// Stable, controller-facing factory identifier.
    fn factory_id() -> &'static str;

    /// The DAG. The workflow driver runs it in topological order: a tasklet
    /// becomes runnable only after **every** producer feeding it has committed
    /// — fan-out branches interleave independently, and a fan-in node is
    /// dispatched once, with the accumulated batch of all its producers.
    /// Cycles are refused at registration, and so is a class that declares no
    /// tasklet.
    fn tasklets(_c: &ChannelBuilder) {}

    /// Optional wall-clock backstop. `None` (the default) means no duration
    /// bound; when `Some`, the SDK cooperatively cancels in-flight work once
    /// it elapses.
    fn max_duration() -> Option<Duration> {
        None
    }

    /// Self-termination guarantee, validated at registration. Default `true`:
    /// the operative ends on its own (a chain that finishes, a tasklet's
    /// `complete`, or a loop that runs out of work). An operative whose **only**
    /// possible exit is an external stop sets this `false`; combined with no
    /// [`max_duration`](Self::max_duration) it is **refused** at registration
    /// with a message directing the author to add a terminal condition.
    fn self_terminating() -> bool {
        true
    }

    /// Loop shape of a structured chain (`workflow-durable-loop`). The default
    /// [`LoopMode::Once`] is the single pass. A looping class commits every
    /// iteration durably — journal + boundary snapshot + outbox flush — so a
    /// crash resumes at the iteration after the last committed one, and
    /// [`RollbackMode::KeepProgress`] keeps every committed iteration.
    fn loop_mode() -> LoopMode {
        LoopMode::Once
    }

    /// Durable wake deadline for a [`LoopMode::PerInput`] loop: how long after
    /// parking the instance is rehydrated even with no input
    /// (`workflow-durable-suspension`). `None` (the default) parks until the next
    /// input arrives. The timer is journaled with the park record, so it
    /// survives a controller redeploy; it wakes the loop with an **empty** entry
    /// payload, which a class declaring one must accept as its tick.
    fn wake_after() -> Option<Duration> {
        None
    }

    /// Total attempts a **structured** tasklet's cell gets before its
    /// error fails the tasklet (`workflow-cell-retry`): the first run plus
    /// the retries. Default 3; `0` or `1` restores fail-fast. Only the failed
    /// cell re-runs — siblings that completed stay buffered at the barrier —
    /// and the retry always happens **before** the tasklet commits, so a
    /// committed tasklet is never retried.
    ///
    /// A retried cell re-runs its whole batch for that tasklet, under the
    /// same at-least-once idempotency contract crash-resume imposes: a failed
    /// attempt's state changes are already applied, so `Tasklet::run` must
    /// tolerate re-running over them. Classify a permanent failure with
    /// [`TaskletError::non_retryable`](crate::tasklet::TaskletError::non_retryable)
    /// to skip the ladder.
    fn retry_attempts() -> u32 {
        3
    }

    /// Backoff base of the retry ladder: attempt `n` waits `base × 2^(n-1)`
    /// with jitter, capped at [`RETRY_BACKOFF_MAX_MS`](crate::channel::RETRY_BACKOFF_MAX_MS).
    /// Default 500 ms. The wait observes cancellation, so an external stop or
    /// `max_duration` interrupts it promptly.
    fn retry_backoff() -> Duration {
        Duration::from_millis(500)
    }

    /// Sends of one commit barrier (`TaskCommit` / `IterCommit`, first send
    /// included) before an ack that never arrives fails the run. Default 3;
    /// `0` or `1` is the single wait. A re-send carries the same
    /// `reference_id`, and the controller journals a barrier once by its
    /// identity, so a transient control-channel stall delays the chain
    /// instead of ending it. The re-sends wait out
    /// [`retry_backoff`](Self::retry_backoff) between attempts.
    fn barrier_retry_attempts() -> u32 {
        3
    }

    /// Saga rollback scope, applied when the operative terminates abnormally
    /// after partial progress. Default [`RollbackMode::AllOrNothing`] undoes
    /// every completed tasklet in reverse, back to the start;
    /// [`RollbackMode::KeepProgress`] keeps each finished loop iteration and
    /// undoes only the in-flight one. The mode is irrelevant for a single-pass
    /// chain (where both behave all-or-nothing) and for an operative with no
    /// compensation hooks.
    fn rollback_mode() -> RollbackMode {
        RollbackMode::AllOrNothing
    }

    /// Class-level persistence toggle (durable by default, mirroring
    /// [`StreamOperative::PERSISTENCE_ENABLED`](crate::operative::StreamOperative::PERSISTENCE_ENABLED)).
    /// When `true`, a structured workflow operative's per-Cell Changes are
    /// written to the WAL as they apply, so a crash resumes from persisted
    /// state. When `false`, the per-Cell flush drains and discards Changes
    /// with no serialization or wire I/O — the operative is in-memory only
    /// (a crash re-runs from the top). The controller resolves this
    /// durable-by-default with a per-class operator opt-out.
    const PERSISTENCE_ENABLED: bool = true;

    /// Synchronous remote durability (`remote-durability-tier`): the
    /// controller completes the remote ship of each commit barrier — the
    /// task-journal suffix and the inbox the barrier consumed — before it
    /// acknowledges the barrier. The acknowledgement this driver blocks on
    /// therefore implies the commit survives losing the controller's disk
    /// (RPO 0), at the cost of one remote round trip per barrier.
    ///
    /// Inert unless the controller has a durability backend configured, and
    /// available to workflow classes only: a stream class acknowledges at
    /// input rates, where a remote round trip per acknowledgement is not a
    /// guarantee anyone can keep.
    const SYNC_DURABILITY: bool = false;
}

#[cfg(test)]
mod tests;
