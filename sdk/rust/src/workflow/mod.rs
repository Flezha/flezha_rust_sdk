//! Workflow-operative authoring surface. The driver, saga machinery,
//! registry and spec kernel live in `flz_core::workflow` and are re-exported
//! here; this module keeps the author-facing `WorkflowOperative` trait (and
//! the driver scenario suite).

pub use flz_core::workflow::*;
pub mod spec;

use std::time::Duration;

pub use flz_core::workflow::{compensation, driver, instance, registry};

use crate::channel::ChannelBuilder;

/// One workflow operative class. The bare [`run`](Self::run) form is the
/// trivial single-Procedure, single-Entity case; override
/// [`procedures`](Self::procedures) to grow into an explicit DAG of
/// Procedures/Channels/Entities without rewriting unrelated code. Capabilities
/// attach to the kind, not here — choosing the workflow kind is sufficient.
/// A worked example of both forms: `sdk/rust/examples/spawn_workflow_worker.rs`.
pub trait WorkflowOperative: Send + Sync + 'static {
    /// Stable, controller-facing factory identifier.
    fn factory_id() -> &'static str;

    /// Run-sugar body: the single-Procedure, single-Entity case. Invoked once
    /// per started instance with `args` from `init_values`. Emit output via
    /// `ctx`, optionally call [`WorkflowCtx::complete`] with a result, and
    /// return `Ok(())`; returning `Err` is a terminal failure — the retry
    /// ladder ([`retry_attempts`](Self::retry_attempts)) scopes to a structured
    /// procedure's entities, and a `run` body has none.
    ///
    /// The default errors — a class must implement either `run` or
    /// `procedures`. A structured operative (non-empty `procedures`) never has
    /// `run` invoked.
    fn run(_ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
        Err(WorkflowError::from(
            "WorkflowOperative: neither run() nor procedures() was implemented",
        ))
    }

    /// Structured form: declare a static **DAG** of Procedures/Channels/
    /// Entities on the same graph builder a stream operative uses. The
    /// default is empty, selecting the [`run`](Self::run)-sugar path. The
    /// workflow driver runs the declaration in topological order: a procedure
    /// becomes runnable only after **every** producer feeding it has committed
    /// — fan-out branches interleave independently, and a fan-in node is
    /// dispatched once, with the accumulated batch of all its producers.
    /// Cycles are rejected at registration.
    fn procedures(_c: &ChannelBuilder) {}

    /// Optional wall-clock backstop. `None` (the default) means no duration
    /// bound; when `Some`, the SDK cooperatively cancels in-flight work once
    /// it elapses.
    fn max_duration() -> Option<Duration> {
        None
    }

    /// Self-termination guarantee, validated at registration. Default `true`:
    /// the operative ends on its own (a `run`/chain that returns, or a loop
    /// with a terminal predicate / exhausted input). An operative whose **only**
    /// possible exit is an external stop sets this `false`; combined with no
    /// [`max_duration`](Self::max_duration) it is **rejected** at registration
    /// with a message directing the author to add a terminal condition.
    fn self_terminating() -> bool {
        true
    }

    /// Loop shape of a structured chain (`workflow-durable-loop`). The default
    /// [`LoopMode::Once`] is the single pass. A looping class commits every
    /// iteration durably — journal + boundary snapshot + outbox flush — so a
    /// crash resumes at the iteration after the last committed one, and
    /// [`RollbackMode::KeepProgress`] keeps every committed iteration. Declaring
    /// a loop on a `run`-sugar class (no `procedures`) is rejected at
    /// registration: there is no chain to iterate.
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

    /// Total attempts a **structured** procedure's entity gets before its
    /// error fails the procedure (`workflow-entity-retry`): the first run plus
    /// the retries. Default 3; `0` or `1` restores fail-fast. Only the failed
    /// entity re-runs — siblings that completed stay buffered at the barrier —
    /// and the retry always happens **before** the procedure commits, so a
    /// committed procedure is never retried.
    ///
    /// A retried entity re-runs its whole batch for that procedure, under the
    /// same at-least-once idempotency contract crash-resume imposes: a failed
    /// attempt's state changes are already applied, so `Procedure::run` must
    /// tolerate re-running over them. Classify a permanent failure with
    /// [`ProcedureError::non_retryable`](crate::procedure::ProcedureError::non_retryable)
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

    /// Sends of one commit barrier (`ProcCommit` / `IterCommit`, first send
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
    /// every completed procedure in reverse, back to the start;
    /// [`RollbackMode::KeepProgress`] keeps each finished loop iteration and
    /// undoes only the in-flight one. The mode is irrelevant for a single-pass
    /// chain (where both behave all-or-nothing) and for an operative with no
    /// compensation hooks.
    fn rollback_mode() -> RollbackMode {
        RollbackMode::AllOrNothing
    }

    /// Class-level persistence toggle (durable by default, mirroring
    /// [`StreamOperative::PERSISTENCE_ENABLED`](crate::operative::StreamOperative::PERSISTENCE_ENABLED)).
    /// When `true`, a structured workflow operative's per-Entity Changes are
    /// written to the WAL as they apply, so a crash resumes from persisted
    /// state. When `false`, the per-Entity flush drains and discards Changes
    /// with no serialization or wire I/O — the operative is in-memory only
    /// (a crash re-runs from the top). The controller resolves this
    /// durable-by-default with a per-class operator opt-out.
    const PERSISTENCE_ENABLED: bool = true;

    /// Synchronous remote durability (`remote-durability-tier`): the
    /// controller completes the remote ship of each commit barrier — the
    /// proc-journal suffix and the inbox the barrier consumed — before it
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
