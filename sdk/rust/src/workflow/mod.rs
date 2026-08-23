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
mod tests {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use crossbeam::channel::Receiver;
    use flz_core::fast::RunnerNode;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::command::CommandAction;
    use crate::entity::restore::RestoreInitPayload;
    use crate::operative::registry::{Operative, OperativeRegistry};
    use crate::pb::{AddOperative, WorkflowStop};
    use crate::procedure::change::{Change, NoChange};
    use crate::procedure::clock::WallClock;
    use crate::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
    use crate::runner::{install_reconnect_trigger, RunnerMessage};

    static EMIT_RUNS: AtomicUsize = AtomicUsize::new(0);
    static FAIL_RUNS: AtomicUsize = AtomicUsize::new(0);

    // ---- run-sugar operatives ------------------------------------------------

    /// Run-sugar that echoes its args as plugin output (counts runs).
    struct EmitWorkflow;
    impl WorkflowOperative for EmitWorkflow {
        fn factory_id() -> &'static str {
            "test-emit-workflow"
        }
        fn run(ctx: &mut WorkflowCtx, args: &str) -> Result<(), WorkflowError> {
            EMIT_RUNS.fetch_add(1, Ordering::SeqCst);
            ctx.emit(args.as_bytes().to_vec())?;
            Ok(())
        }
    }

    /// Run-sugar that always fails (counting its runs: a `run` body has no
    /// entities to scope a retry to, so it must run exactly once).
    struct FailWorkflow;
    impl WorkflowOperative for FailWorkflow {
        fn factory_id() -> &'static str {
            "test-fail-workflow"
        }
        fn run(_ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
            FAIL_RUNS.fetch_add(1, Ordering::SeqCst);
            Err(WorkflowError::from("boom"))
        }
    }

    struct PanicWorkflow;
    impl WorkflowOperative for PanicWorkflow {
        fn factory_id() -> &'static str {
            "test-panic-workflow"
        }
        fn run(_ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
            panic!("kaboom")
        }
    }

    /// Run-sugar that returns a terminal result via `ctx.complete`.
    struct CompleteWorkflow;
    impl WorkflowOperative for CompleteWorkflow {
        fn factory_id() -> &'static str {
            "test-complete-workflow"
        }
        fn run(ctx: &mut WorkflowCtx, args: &str) -> Result<(), WorkflowError> {
            ctx.complete(format!("result:{args}").into_bytes());
            Ok(())
        }
    }

    /// Interactive run-sugar: loops on the inbox, echoes `got:<msg>`, exits the
    /// loop on a `quit` message (terminal predicate) or on cancellation.
    struct LoopWorkflow;
    impl WorkflowOperative for LoopWorkflow {
        fn factory_id() -> &'static str {
            "test-loop-workflow"
        }
        fn run(ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
            let done = Cell::new(false);
            ctx.loop_until(
                || done.get(),
                |ctx| {
                    match ctx.recv() {
                        Some(msg) => {
                            let s = String::from_utf8_lossy(&msg).to_string();
                            if s == "quit" {
                                done.set(true);
                            } else {
                                ctx.emit(format!("got:{s}").into_bytes())?;
                            }
                        }
                        None => done.set(true), // cancellation tripped
                    }
                    Ok(())
                },
            )
        }
    }

    /// Interactive run-sugar bounded by `max_duration`: loops on the inbox
    /// forever; the only exit is the duration backstop (or external stop).
    struct TimedWorkflow;
    impl WorkflowOperative for TimedWorkflow {
        fn factory_id() -> &'static str {
            "test-timed-workflow"
        }
        fn max_duration() -> Option<Duration> {
            Some(Duration::from_millis(200))
        }
        fn run(ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
            ctx.loop_until(
                || false,
                |ctx| {
                    let _ = ctx.recv();
                    Ok(())
                },
            )
        }
    }

    /// External-stop-only: declares it does not self-terminate and has no
    /// duration backstop — rejected at registration.
    struct ExternalStopOnlyWorkflow;
    impl WorkflowOperative for ExternalStopOnlyWorkflow {
        fn factory_id() -> &'static str {
            "test-external-stop-only"
        }
        fn self_terminating() -> bool {
            false
        }
        fn run(_ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
            Ok(())
        }
    }

    // ---- structured procedures + operatives ----------------------------------

    #[derive(Default)]
    struct EchoProc;
    impl Procedure for EchoProc {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_echo_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("echo:{input}"))?;
            Ok(())
        }
    }

    /// Single-Procedure structured form, equivalent to `EmitWorkflow`-style sugar.
    struct EchoStructured;
    impl WorkflowOperative for EchoStructured {
        fn factory_id() -> &'static str {
            "test-echo-structured"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EchoProc>()
                .named("out")
                .sink("sink");
        }
    }

    #[derive(Default)]
    struct Stage1;
    impl Procedure for Stage1 {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_stage1"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("{input}-1"))?;
            Ok(())
        }
    }

    #[derive(Default)]
    struct Stage2;
    impl Procedure for Stage2 {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_stage2"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("{input}-2"))?;
            Ok(())
        }
    }

    /// `workflow-branching`: Stage1 fans out to Stage2 and EchoProc on one channel,
    /// each branch ending at its own plugin sink. The edge is (channel,
    /// consumer), so both branches read exactly what Stage1 released on `mid`.
    fn tree_chain(c: &ChannelBuilder) {
        let mid = c
            .input::<String>("in")
            .into::<Stage1>()
            .named("mid")
            .codec();
        mid.into::<Stage2>().named("out_b").sink("sink");
        mid.into::<EchoProc>().named("out_c").sink("sink");
    }

    struct TreeWorkflow;
    impl WorkflowOperative for TreeWorkflow {
        fn factory_id() -> &'static str {
            "test-tree-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            tree_chain(c);
        }
    }

    /// Two-stage linear chain: Stage1 → Stage2 → plugin.
    /// The two-stage durable chain every chain/loop fixture below declares:
    /// `in -> Stage1 -> mid (codec) -> Stage2 -> out -> sink`.
    fn two_stage_chain(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Stage1>()
            .named("mid")
            .codec()
            .into::<Stage2>()
            .named("out")
            .sink("sink");
    }

    struct ChainWorkflow;
    impl WorkflowOperative for ChainWorkflow {
        fn factory_id() -> &'static str {
            "test-chain-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
    }

    /// The same chain, driven as a chat-style inbox loop: one iteration per
    /// post-start input, parking when the inbox goes quiet
    /// (`workflow-durable-loop` + `workflow-durable-suspension`).
    /// `ChainWorkflow` with a short re-send backoff, so the barrier re-send
    /// scenarios wait out their ladder inside the drain's deadline.
    struct ResendChain;
    impl WorkflowOperative for ResendChain {
        fn factory_id() -> &'static str {
            "test-resend-chain"
        }
        fn retry_backoff() -> Duration {
            Duration::from_millis(50)
        }
        fn procedures(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
    }

    /// `ChatLoop` with a short re-send backoff.
    struct ResendLoop;
    impl WorkflowOperative for ResendLoop {
        fn factory_id() -> &'static str {
            "test-resend-loop"
        }
        fn loop_mode() -> LoopMode {
            LoopMode::PerInput
        }
        fn self_terminating() -> bool {
            false
        }
        fn max_duration() -> Option<Duration> {
            Some(Duration::from_secs(30))
        }
        fn retry_backoff() -> Duration {
            Duration::from_millis(50)
        }
        fn procedures(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
    }

    struct ChatLoop;
    impl WorkflowOperative for ChatLoop {
        fn factory_id() -> &'static str {
            "test-chat-loop"
        }
        fn loop_mode() -> LoopMode {
            LoopMode::PerInput
        }
        fn self_terminating() -> bool {
            false
        }
        fn max_duration() -> Option<Duration> {
            Some(Duration::from_secs(30))
        }
        fn procedures(c: &ChannelBuilder) {
            two_stage_chain(c);
        }
    }

    /// Compensable stage-0 proc of the loop saga. Its undo logs the number of
    /// retained outputs it was handed, which is how the test tells the two
    /// rollback sources apart: the in-flight iteration carries its outputs in
    /// memory (`1`), while an iteration rebuilt from the journal carries none
    /// (`0`) — the documented at-least-once contract, where a durable undo
    /// handle lives in Entity state.
    #[derive(Default)]
    struct LoopReserve;
    impl Procedure for LoopReserve {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "loop_reserve"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("reserved:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            comp_log(format!("undo-loop:{}", completed_output.len()));
            Ok(())
        }
    }

    /// Terminal proc of the loop saga: fails the iteration whose turn says "boom".
    #[derive(Default)]
    struct LoopMaybeFail;
    impl Procedure for LoopMaybeFail {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "loop_maybe_fail"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            if input.contains("boom") {
                return Err(ProcedureError::Custom("turn failed".into()));
            }
            ctx.emit(format!("done:{input}"))?;
            Ok(())
        }
    }

    /// Inbox-driven saga loop, all-or-nothing: a failing turn unwinds every
    /// committed turn too.
    struct LoopSagaAllOrNothing;
    impl WorkflowOperative for LoopSagaAllOrNothing {
        fn factory_id() -> &'static str {
            "test-loop-saga-aon"
        }
        fn loop_mode() -> LoopMode {
            LoopMode::PerInput
        }
        fn self_terminating() -> bool {
            false
        }
        fn max_duration() -> Option<Duration> {
            Some(Duration::from_secs(30))
        }
        fn procedures(c: &ChannelBuilder) {
            loop_saga_chain(c);
        }
    }

    /// The same loop, keep-progress: a failing turn unwinds only itself.
    struct LoopSagaKeepProgress;
    impl WorkflowOperative for LoopSagaKeepProgress {
        fn factory_id() -> &'static str {
            "test-loop-saga-keep"
        }
        fn loop_mode() -> LoopMode {
            LoopMode::PerInput
        }
        fn rollback_mode() -> RollbackMode {
            RollbackMode::KeepProgress
        }
        fn self_terminating() -> bool {
            false
        }
        fn max_duration() -> Option<Duration> {
            Some(Duration::from_secs(30))
        }
        fn procedures(c: &ChannelBuilder) {
            loop_saga_chain(c);
        }
    }

    fn loop_saga_chain(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<LoopReserve>()
            .named("mid")
            .codec()
            .into::<LoopMaybeFail>()
            .named("out")
            .sink("sink");
    }

    /// A loop declared on a `run`-sugar class: no chain to iterate.
    struct LoopWithoutChain;
    impl WorkflowOperative for LoopWithoutChain {
        fn factory_id() -> &'static str {
            "test-loop-no-chain"
        }
        fn loop_mode() -> LoopMode {
            LoopMode::PerInput
        }
        fn run(_ctx: &mut WorkflowCtx, _args: &str) -> Result<(), WorkflowError> {
            Ok(())
        }
    }

    /// A durable handoff-chained loop whose terminal procedure emits to a plugin
    /// (no codec), so iteration N+1 could not be re-seeded after a crash.
    struct ChainedLoopNoCodec;
    impl WorkflowOperative for ChainedLoopNoCodec {
        fn factory_id() -> &'static str {
            "test-chained-loop-no-codec"
        }
        fn loop_mode() -> LoopMode {
            LoopMode::Chained
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .codec()
                .into::<Stage2>()
                .named("out")
                .sink("sink");
        }
    }

    /// Splits the comma-separated args into individual items (one entity).
    #[derive(Default)]
    struct SplitProc;
    impl Procedure for SplitProc {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_split_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            for piece in input.split(',') {
                ctx.emit(piece.to_string())?;
            }
            Ok(())
        }
    }

    #[derive(Serialize, Deserialize, Clone, Default)]
    struct CountState {
        n: u64,
    }
    #[derive(Serialize, Deserialize, Clone)]
    struct Inc;
    impl Change for Inc {}

    /// Static-entity procedure: routes items to `even`/`odd` entities by
    /// length parity, counts per-entity (independent state), emits `<n>:<item>`.
    #[derive(Default)]
    struct FanProc;
    impl Procedure for FanProc {
        type Input = String;
        type Output = String;
        type State = CountState;
        type Change = Inc;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_fan_op"
        }
        fn entity_keys() -> Vec<&'static str> {
            vec!["even", "odd"]
        }
        const ENTITIES_FOR_OVERRIDDEN: bool = true;
        fn entities_for(input: &String) -> Vec<&'static str> {
            if input.len().is_multiple_of(2) {
                vec!["even"]
            } else {
                vec!["odd"]
            }
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> CountState {
            CountState::default()
        }
        fn apply_change(state: &mut CountState, _: &Inc) {
            state.n += 1;
        }
        fn run(
            &mut self,
            input: String,
            mut state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            state.apply(Inc);
            let n = state.n;
            ctx.emit(format!("{n}:{input}"))?;
            Ok(())
        }
    }

    /// Split → fan-out across static entities → plugin.
    struct FanWorkflow;
    impl WorkflowOperative for FanWorkflow {
        fn factory_id() -> &'static str {
            "test-fan-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SplitProc>()
                .named("mid")
                .codec()
                .into::<FanProc>()
                .named("out")
                .sink("sink");
        }
    }

    /// Opt-out twin of `FanWorkflow`: identical topology, persistence disabled —
    /// its Changes drain-and-discard instead of reaching the WAL.
    struct FanOptOutWorkflow;
    impl WorkflowOperative for FanOptOutWorkflow {
        fn factory_id() -> &'static str {
            "test-fan-optout-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SplitProc>()
                .named("mid")
                .into::<FanProc>()
                .named("out")
                .sink("sink");
        }
        const PERSISTENCE_ENABLED: bool = false;
    }

    #[derive(Default)]
    struct HybridStage1;
    impl Procedure for HybridStage1 {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_hybrid_stage1"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("h:{input}"))?;
            Ok(())
        }
    }

    #[derive(Default)]
    struct HybridStage2;
    impl Procedure for HybridStage2 {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_hybrid_stage2"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("final:{input}"))?;
            Ok(())
        }
    }

    /// Hybrid inter-procedure channel (`sink_with`): Stage1's output
    /// both feeds Stage2 AND is serialized to a plugin sink.
    struct HybridWorkflow;
    impl WorkflowOperative for HybridWorkflow {
        fn factory_id() -> &'static str {
            "test-hybrid-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<HybridStage1>()
                .named("mid")
                .codec()
                .sink_with("midsink", |s: &String| format!("mid:{s}").into_bytes())
                .into::<HybridStage2>()
                .named("out")
                .sink("out");
        }
    }

    // ---- saga compensation operatives (Phase 2 §4) --------------------------

    /// Process-global compensation observation log. The driver-observation test
    /// owns it (like `EMIT_RUNS`); compensable procedures append a marker so
    /// rollback order and per-entity firing are assertable without wire
    /// plumbing (a compensable inter-procedure proc's `ctx.emit` is collected, not
    /// wired).
    static COMPENSATION_LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());

    // Captures the `(payload_id, frame)` pairs a durable workflow ships to the
    // reliable sink — `entity_state_change` from the flush plus the saga
    // `compensation_intent` / `compensation_done` markers.
    static WAL_FRAMES: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

    fn count_wal_frames(payload_id: &str) -> usize {
        WAL_FRAMES
            .lock()
            .unwrap()
            .iter()
            .filter(|(pid, _)| pid == payload_id)
            .count()
    }

    fn comp_log(entry: String) {
        COMPENSATION_LOG.lock().unwrap().push(entry);
    }

    fn drain_comp_log() -> Vec<String> {
        std::mem::take(&mut *COMPENSATION_LOG.lock().unwrap())
    }

    /// Forced-stop handshake: `StopReserve` signals it has started, then parks
    /// until the test trips cancellation, so the "stop after partial progress"
    /// path is deterministic (cancel is observed before the next stage's
    /// pre-check rather than racing the fast chain).
    static STAGE0_RAN: AtomicBool = AtomicBool::new(false);
    static STAGE0_RELEASE: AtomicBool = AtomicBool::new(false);

    /// Compensable: reserves (emits `reserved:<arg>`); undo logs `undo-reserve`.
    #[derive(Default)]
    struct SagaReserve;
    impl Procedure for SagaReserve {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "saga_reserve"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("reserved:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            for out in &completed_output {
                comp_log(format!("undo-reserve:{out}"));
            }
            Ok(())
        }
    }

    /// Compensable: charges (emits `charged:<input>`); undo logs `undo-charge`.
    #[derive(Default)]
    struct SagaCharge;
    impl Procedure for SagaCharge {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "saga_charge"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("charged:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            for out in &completed_output {
                comp_log(format!("undo-charge:{out}"));
            }
            Ok(())
        }
    }

    /// Terminal step that always fails — triggers reverse-order rollback of the
    /// completed `reserve` + `charge`. Itself uncompleted ⇒ never compensated.
    #[derive(Default)]
    struct SagaShipFails;
    impl Procedure for SagaShipFails {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "saga_ship_fails"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _input: String,
            _state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Err(ProcedureError::Custom("ship failed".into()))
        }
    }

    /// Reserve → Charge → Ship(fails). Default all-or-nothing rollback.
    /// Compensable leaf of the fan-out branch: archives (emits `archived:<input>`);
    /// undo logs `undo-archive`.
    #[derive(Default)]
    struct SagaArchive;
    impl Procedure for SagaArchive {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "saga_archive"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("archived:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            for out in &completed_output {
                comp_log(format!("undo-archive:{out}"));
            }
            Ok(())
        }
    }

    /// `workflow-branching` saga: reserve fans out to (charge → ship-fails) and
    /// archive. The failure sits under one branch, so the abort must unwind BOTH
    /// branches and only then their shared parent.
    struct TreeSaga;
    impl WorkflowOperative for TreeSaga {
        fn factory_id() -> &'static str {
            "test-tree-saga"
        }
        fn procedures(c: &ChannelBuilder) {
            let t1 = c
                .input::<String>("in")
                .into::<SagaReserve>()
                .named("t1")
                .codec();
            t1.into::<SagaCharge>()
                .named("t2")
                .codec()
                .into::<SagaShipFails>()
                .named("t_ship")
                .sink("sink");
            // The archive leaf releases an inter-proc batch nobody consumes: it
            // stays a leaf, and its retained output is what its undo reverses
            // (a plugin-bound leaf would stage instead of collect, leaving the
            // hook nothing to name).
            t1.into::<SagaArchive>().named("t_out").codec();
        }
    }

    struct SagaWorkflow;
    impl WorkflowOperative for SagaWorkflow {
        fn factory_id() -> &'static str {
            "test-saga-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SagaReserve>()
                .named("c1")
                .codec()
                .into::<SagaCharge>()
                .named("c2")
                .codec()
                .into::<SagaShipFails>()
                .named("out")
                .sink("sink");
        }
    }

    /// Same chain, keep-progress mode declared — for a single no-loop pass it
    /// unwinds identically (the mode only diverges with committed iterations,
    /// covered by the planner unit tests). Proves `rollback_mode()` is plumbed
    /// through the registration → driver path.
    struct KeepProgressSaga;
    impl WorkflowOperative for KeepProgressSaga {
        fn factory_id() -> &'static str {
            "test-keep-progress-saga"
        }
        fn rollback_mode() -> RollbackMode {
            RollbackMode::KeepProgress
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SagaReserve>()
                .named("c1")
                .codec()
                .into::<SagaCharge>()
                .named("c2")
                .codec()
                .into::<SagaShipFails>()
                .named("out")
                .sink("sink");
        }
    }

    /// Reserve → Charge → Echo(succeeds): a clean `ok` MUST NOT compensate.
    struct CleanSaga;
    impl WorkflowOperative for CleanSaga {
        fn factory_id() -> &'static str {
            "test-clean-saga"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SagaReserve>()
                .named("c1")
                .codec()
                .into::<SagaCharge>()
                .named("c2")
                .codec()
                .into::<EchoProc>()
                .named("out")
                .sink("sink");
        }
    }

    /// Static-entity compensable proc: stores its entity key in state, emits
    /// `<key>:<item>`, and compensates ONCE per entity (logs all that entity's
    /// outputs together) so per-entity firing is assertable.
    #[derive(Serialize, Deserialize, Clone, Default)]
    struct FanKeyState {
        key: String,
    }
    #[derive(Default)]
    struct SagaFan;
    impl Procedure for SagaFan {
        type Input = String;
        type Output = String;
        type State = FanKeyState;
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "saga_fan"
        }
        fn entity_keys() -> Vec<&'static str> {
            vec!["even", "odd"]
        }
        const ENTITIES_FOR_OVERRIDDEN: bool = true;
        fn entities_for(input: &String) -> Vec<&'static str> {
            if input.len().is_multiple_of(2) {
                vec!["even"]
            } else {
                vec!["odd"]
            }
        }
        fn init_state(entity_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> FanKeyState {
            FanKeyState {
                key: entity_key.unwrap_or("").to_string(),
            }
        }
        fn run(
            &mut self,
            input: String,
            state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("{}:{input}", state.key))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            let items: Vec<String> = completed_output.iter().map(|o| o.to_string()).collect();
            comp_log(format!("undo-fan:{}:[{}]", state.key, items.join(",")));
            Ok(())
        }
    }

    /// Terminal failing proc for the per-entity rollback chain.
    #[derive(Default)]
    struct FailLast;
    impl Procedure for FailLast {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "saga_fail_last"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _input: String,
            _state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Err(ProcedureError::Custom("fan ship failed".into()))
        }
    }

    /// Split(hookless) → Fan(compensable, even/odd) → FailLast. The hookless
    /// `SplitProc` is skipped during rollback; only `SagaFan` compensates, once
    /// per entity.
    struct FanSaga;
    impl WorkflowOperative for FanSaga {
        fn factory_id() -> &'static str {
            "test-fan-saga"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SplitProc>()
                .named("mid")
                .codec()
                .into::<SagaFan>()
                .named("mid2")
                .codec()
                .into::<FailLast>()
                .named("out")
                .sink("sink");
        }
    }

    /// Compensable stage-0 proc with a handshake so a cooperative stop lands after
    /// it completes but before stage 1 (deterministic forced-stop rollback).
    #[derive(Default)]
    struct StopReserve;
    impl Procedure for StopReserve {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "saga_stop_reserve"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            STAGE0_RAN.store(true, Ordering::SeqCst);
            while !STAGE0_RELEASE.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
            ctx.emit(format!("reserved:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            for out in &completed_output {
                comp_log(format!("undo-stop-reserve:{out}"));
            }
            Ok(())
        }
    }

    /// StopReserve → Echo. A cooperative stop after StopReserve completes
    /// compensates only StopReserve; Echo never runs.
    struct StopSaga;
    impl WorkflowOperative for StopSaga {
        fn factory_id() -> &'static str {
            "test-stop-saga"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<StopReserve>()
                .named("mid")
                .codec()
                .into::<EchoProc>()
                .named("out")
                .sink("sink");
        }
    }

    // ---- saga crash-resume procs (§4.3) ---------------------------------------
    // Compensable procs whose `compensate` logs UNCONDITIONALLY (not per output),
    // so a crash-resume — which reconstructs completed stages with EMPTY outputs
    // (a procedure's collected output was never durable) — is still observable.

    #[derive(Default)]
    struct ResumeUndoA;
    impl Procedure for ResumeUndoA {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "resume_undo_a"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("a:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            _out: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            comp_log("undo-A".to_string());
            Ok(())
        }
    }

    #[derive(Default)]
    struct ResumeUndoB;
    impl Procedure for ResumeUndoB {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "resume_undo_b"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("b:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            _out: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            comp_log("undo-B".to_string());
            Ok(())
        }
    }

    // ---- staged-commit / transactional-outbox procs --------------------------

    /// Emits plugin-bound output and THEN fails: the procedure never commits,
    /// so its staged output must never reach a plugin (nothing leaked).
    #[derive(Default)]
    struct EmitThenFail;
    impl Procedure for EmitThenFail {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_emit_then_fail"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("leak:{input}"))?;
            Err(ProcedureError::Custom("failed after emitting".into()))
        }
    }

    /// One procedure that emits then fails, straight to a plugin sink.
    struct LeakWorkflow;
    impl WorkflowOperative for LeakWorkflow {
        fn factory_id() -> &'static str {
            "test-leak-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EmitThenFail>()
                .named("out")
                .sink("sink");
        }
    }

    /// Compensable proc on a hybrid channel: its forward output feeds the next
    /// procedure AND a plugin sink, so its `compensate` hook's `ctx.emit`
    /// produces plugin-bound undo output (staged for the `compensated` entry).
    #[derive(Default)]
    struct UndoEmit;
    impl Procedure for UndoEmit {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_undo_emit"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(format!("did:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            for out in &completed_output {
                ctx.emit(format!("undo:{out}"))?;
            }
            Ok(())
        }
    }

    /// UndoEmit(hybrid, compensable) → FailLast. The failure rolls UndoEmit
    /// back; its undo output is staged and journaled in a `compensated` entry.
    struct UndoSaga;
    impl WorkflowOperative for UndoSaga {
        fn factory_id() -> &'static str {
            "test-undo-saga"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<UndoEmit>()
                .named("mid")
                .codec()
                .sink_with("undosink", |s: &String| s.clone().into_bytes())
                .into::<FailLast>()
                .named("out")
                .sink("sink");
        }
    }

    /// A → B → ship(fails). Durable-by-default; both A and B are compensable.
    struct ResumeSaga;
    impl WorkflowOperative for ResumeSaga {
        fn factory_id() -> &'static str {
            "test-resume-saga"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<ResumeUndoA>()
                .named("c1")
                .codec()
                .into::<ResumeUndoB>()
                .named("c2")
                .codec()
                .into::<SagaShipFails>()
                .named("out")
                .sink("sink");
        }
    }

    // ---- per-entity retry ladder procs (`workflow-entity-retry`) -------------

    /// Attempts the `odd` entity has made, and items the `even` sibling has
    /// processed — a retry must re-run ONLY the failing entity.
    static ODD_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    static EVEN_RUNS: AtomicUsize = AtomicUsize::new(0);
    /// How many of the `odd` entity's attempts fail, and how they are
    /// classified — set per scenario.
    static ODD_FAILURES: AtomicUsize = AtomicUsize::new(0);
    static ODD_NON_RETRYABLE: AtomicBool = AtomicBool::new(false);

    /// Compensable pass-through in front of the flaky stage: its undo proves
    /// whether an entity failure escalated to the procedure (compensation) or
    /// was absorbed by the ladder.
    #[derive(Default)]
    struct RetryReserve;
    impl Procedure for RetryReserve {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "retry_reserve"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            ctx.emit(input)?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            _completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            comp_log("undo-retry-reserve".to_string());
            Ok(())
        }
    }

    /// Static-entity flaky stage: the `odd` entity fails its first
    /// `ODD_FAILURES` attempts (retryably, or `NonRetryable` when the scenario
    /// says so) and then succeeds; the `even` entity always succeeds and
    /// counts the items it saw.
    #[derive(Default)]
    struct FlakyFan;
    impl Procedure for FlakyFan {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "flaky_fan"
        }
        fn entity_keys() -> Vec<&'static str> {
            vec!["even", "odd"]
        }
        const ENTITIES_FOR_OVERRIDDEN: bool = true;
        fn entities_for(input: &String) -> Vec<&'static str> {
            if input.len().is_multiple_of(2) {
                vec!["even"]
            } else {
                vec!["odd"]
            }
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            if input.len().is_multiple_of(2) {
                EVEN_RUNS.fetch_add(1, Ordering::SeqCst);
                ctx.emit(format!("even:{input}"))?;
                return Ok(());
            }
            let attempt = ODD_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= ODD_FAILURES.load(Ordering::SeqCst) {
                let message = format!("flaky attempt {attempt}");
                return Err(if ODD_NON_RETRYABLE.load(Ordering::SeqCst) {
                    ProcedureError::non_retryable(message)
                } else {
                    ProcedureError::Custom(message)
                });
            }
            ctx.emit(format!("odd:{input}"))?;
            Ok(())
        }
    }

    fn retry_chain(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitProc>()
            .named("mid")
            .codec()
            .into::<RetryReserve>()
            .named("mid2")
            .codec()
            .into::<FlakyFan>()
            .named("out")
            .sink("sink");
    }

    /// Split → Reserve(compensable) → FlakyFan(even/odd). Default 3-attempt
    /// ladder, backoff shrunk so the scenarios do not sleep out a real one.
    struct RetryWorkflow;
    impl WorkflowOperative for RetryWorkflow {
        fn factory_id() -> &'static str {
            "test-retry-workflow"
        }
        fn retry_backoff() -> Duration {
            Duration::from_millis(10)
        }
        fn procedures(c: &ChannelBuilder) {
            retry_chain(c);
        }
    }

    /// The same chain with the ladder opted out: the first entity error fails
    /// the procedure, exactly as before this capability existed.
    struct RetryFailFastWorkflow;
    impl WorkflowOperative for RetryFailFastWorkflow {
        fn factory_id() -> &'static str {
            "test-retry-failfast-workflow"
        }
        fn retry_attempts() -> u32 {
            0
        }
        fn procedures(c: &ChannelBuilder) {
            retry_chain(c);
        }
    }

    /// The same chain with a backoff long enough that a stop must interrupt the
    /// wait rather than outlast it.
    struct RetrySlowBackoffWorkflow;
    impl WorkflowOperative for RetrySlowBackoffWorkflow {
        fn factory_id() -> &'static str {
            "test-retry-slow-workflow"
        }
        fn retry_backoff() -> Duration {
            Duration::from_secs(30)
        }
        fn procedures(c: &ChannelBuilder) {
            retry_chain(c);
        }
    }

    // ---- who runs a stage step, and what a backoff holds -------------------

    /// Every attempt the probe stage ran, as `(entity key, thread name)`.
    static STEP_ATTEMPTS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

    /// One entity per SDK worker, twice over. More entities backing off at
    /// once than the pool has threads is what makes a backoff that HELD a
    /// worker observable: it would starve the pool, and the core answers
    /// starvation with threads above the base set.
    fn probe_keys() -> &'static [&'static str] {
        static KEYS: std::sync::OnceLock<Vec<&'static str>> = std::sync::OnceLock::new();
        KEYS.get_or_init(|| {
            (0..crate::operative::poll_pool::base_worker_count() * 2)
                .map(|i| &*Box::leak(format!("probe{i}").into_boxed_str()))
                .collect()
        })
    }

    #[derive(Serialize, Deserialize, Clone, Default)]
    struct ProbeState {
        key: String,
    }

    /// Every entity fails its first attempt and succeeds on the retry, so the
    /// whole stage sits on a backoff at the same moment, and each attempt
    /// records the thread that ran it.
    #[derive(Default)]
    struct StepProbe;
    impl Procedure for StepProbe {
        type Input = String;
        type Output = String;
        type State = ProbeState;
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "step_probe"
        }
        fn entity_keys() -> Vec<&'static str> {
            probe_keys().to_vec()
        }
        const ENTITIES_FOR_OVERRIDDEN: bool = true;
        fn entities_for(_input: &String) -> Vec<&'static str> {
            probe_keys().to_vec()
        }
        fn init_state(entity_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> ProbeState {
            ProbeState {
                key: entity_key.unwrap_or("").to_string(),
            }
        }
        fn run(
            &mut self,
            input: String,
            state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            let thread = std::thread::current()
                .name()
                .unwrap_or("<unnamed>")
                .to_string();
            let mut attempts = STEP_ATTEMPTS.lock().unwrap();
            let prior = attempts.iter().filter(|(key, _)| *key == state.key).count();
            attempts.push((state.key.clone(), thread));
            drop(attempts);
            if prior == 0 {
                return Err(ProcedureError::Custom(format!(
                    "probe {} attempt 1",
                    state.key
                )));
            }
            ctx.emit(format!("{}:{input}", state.key))?;
            Ok(())
        }
    }

    /// One flaky stage whose entities outnumber the SDK's workers, on a
    /// backoff long enough that a worker holding one would be observed.
    struct StepProbeWorkflow;
    impl WorkflowOperative for StepProbeWorkflow {
        fn factory_id() -> &'static str {
            "test-step-probe-workflow"
        }
        fn retry_backoff() -> Duration {
            Duration::from_millis(200)
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<StepProbe>()
                .named("probe_out")
                .sink("sink");
        }
    }

    /// Arm the flaky stage: `failures` attempts of the `odd` entity fail,
    /// classified retryable or not, with both attempt counters reset.
    fn arm_flaky(failures: usize, non_retryable: bool) {
        ODD_ATTEMPTS.store(0, Ordering::SeqCst);
        EVEN_RUNS.store(0, Ordering::SeqCst);
        ODD_FAILURES.store(failures, Ordering::SeqCst);
        ODD_NON_RETRYABLE.store(non_retryable, Ordering::SeqCst);
        let _ = drain_comp_log();
    }

    // ---- fan-in / diamond procs (`workflow-branching` fan-in accumulates) ------

    /// Observation log for the diamond scenarios: forward runs append here so
    /// join dispatch order (after BOTH producers) is assertable.
    static DIAMOND_LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());
    fn diamond_log(entry: String) {
        DIAMOND_LOG.lock().unwrap().push(entry);
    }
    fn drain_diamond_log() -> Vec<String> {
        std::mem::take(&mut *DIAMOND_LOG.lock().unwrap())
    }

    macro_rules! diamond_op {
        ($ty:ident, $name:literal, $tag:literal) => {
            #[derive(Default)]
            struct $ty;
            impl Procedure for $ty {
                type Input = String;
                type Output = String;
                type State = ();
                type Change = NoChange;
                type Clock = WallClock;
                type Config = ();
                fn name() -> &'static str {
                    $name
                }
                fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
                fn run(
                    &mut self,
                    input: String,
                    _state: StateHandle<'_, Self>,
                    ctx: &mut ProcedureCtx<'_>,
                ) -> Result<(), ProcedureError> {
                    diamond_log(format!("{}:{input}", $tag));
                    ctx.emit(format!("{}:{input}", $tag))?;
                    Ok(())
                }
                const COMPENSATE_OVERRIDDEN: bool = true;
                fn compensate(
                    &mut self,
                    completed_output: Vec<Arc<String>>,
                    _state: StateHandle<'_, Self>,
                    _ctx: &mut ProcedureCtx<'_>,
                ) -> Result<(), ProcedureError> {
                    for out in &completed_output {
                        comp_log(format!("undo-{}:{out}", $tag));
                    }
                    Ok(())
                }
            }
        };
    }

    diamond_op!(DiamondA, "diamond_a", "a");
    diamond_op!(DiamondB, "diamond_b", "b");
    diamond_op!(DiamondC, "diamond_c", "c");
    diamond_op!(DiamondJoin, "workflow_join_op", "j");

    /// The diamond A→(B,C)→D: both branch outputs accumulate into the one
    /// join node (two unnamed mentions unify), which closes plugin-bound.
    struct DiamondWorkflow;
    impl WorkflowOperative for DiamondWorkflow {
        fn factory_id() -> &'static str {
            "test-diamond-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            let a = c
                .input::<String>("in")
                .into::<DiamondA>()
                .named("d_fan")
                .codec();
            a.into::<DiamondB>()
                .named("d_b")
                .codec()
                .into::<DiamondJoin>();
            a.into::<DiamondC>()
                .named("d_c")
                .codec()
                .into::<DiamondJoin>()
                .named("d_out")
                .sink("sink");
        }
    }

    /// Kahn-shape twin used by the registration test (no driver run).
    struct JoinWorkflow;
    impl WorkflowOperative for JoinWorkflow {
        fn factory_id() -> &'static str {
            "test-join-workflow-shape"
        }
        fn procedures(c: &ChannelBuilder) {
            let mid = c
                .input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .codec();
            mid.into::<Stage2>()
                .named("join_b")
                .codec()
                .into::<DiamondJoin>();
            mid.into::<EchoProc>()
                .named("join_c")
                .codec()
                .into::<DiamondJoin>();
        }
    }

    /// Diamond whose join feeds a failing tail: A→(B,C)→D→ship(fails). The
    /// abort must undo the join BEFORE either producer, and the shared root
    /// last (reverse-topological compensation over a DAG).
    struct DiamondSaga;
    impl WorkflowOperative for DiamondSaga {
        fn factory_id() -> &'static str {
            "test-diamond-saga"
        }
        fn procedures(c: &ChannelBuilder) {
            let a = c
                .input::<String>("in")
                .into::<DiamondA>()
                .named("d_fan")
                .codec();
            let join = a
                .into::<DiamondB>()
                .named("d_b")
                .codec()
                .into::<DiamondJoin>()
                .named("d_join")
                .codec();
            a.into::<DiamondC>()
                .named("d_c")
                .codec()
                .into::<DiamondJoin>();
            join.into::<SagaShipFails>().named("d_out").sink("sink");
        }
    }

    /// The merged input of the fan-in below (`multi-input-procedures`): the two
    /// producers carry the same payload type, so only the mapping tells the
    /// join which branch a contribution came from.
    #[derive(Clone, Debug)]
    enum MergedJoinInput {
        B(String),
        C(String),
    }

    /// The diamond's join re-authored as a merge: same two producers, but the
    /// node consumes one sum type and learns its source from the mapping.
    #[derive(Default)]
    struct MergeJoinProc;
    impl Procedure for MergeJoinProc {
        type Input = MergedJoinInput;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "workflow_merge_join_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: MergedJoinInput,
            _state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            let label = match input {
                MergedJoinInput::B(s) => format!("mb:{s}"),
                MergedJoinInput::C(s) => format!("mc:{s}"),
            };
            diamond_log(label.clone());
            ctx.emit(label)?;
            Ok(())
        }
    }

    /// A→(B,C)→merge(D): the fan-in wired as a merge, so the driver must apply
    /// each edge's mapping both where it accumulates a producer's released
    /// batch and where a resume re-seeds from the journaled handoff.
    struct MergeWorkflow;
    impl WorkflowOperative for MergeWorkflow {
        fn factory_id() -> &'static str {
            "test-merge-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            let a = c
                .input::<String>("in")
                .into::<DiamondA>()
                .named("m_fan")
                .codec();
            let b = a.into::<DiamondB>().named("m_b").codec();
            let cc = a.into::<DiamondC>().named("m_c").codec();
            c.merge::<MergedJoinInput>()
                .from(b, MergedJoinInput::B)
                .from(cc, MergedJoinInput::C)
                .into::<MergeJoinProc>()
                .named("m_out")
                .sink("sink");
        }
    }

    /// The same chain whose merge mapping panics. A mapping is the one author
    /// closure on a workflow chain that runs on the driver thread rather than
    /// inside an entity invocation, so without containment its unwind would
    /// take the driver thread with it and the instance would never report.
    struct PanickingMergeWorkflow;
    impl WorkflowOperative for PanickingMergeWorkflow {
        fn factory_id() -> &'static str {
            "test-panic-merge-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            let a = c
                .input::<String>("in")
                .into::<DiamondA>()
                .named("pm_fan")
                .codec();
            let b = a.into::<DiamondB>().named("pm_b").codec();
            let cc = a.into::<DiamondC>().named("pm_c").codec();
            c.merge::<MergedJoinInput>()
                .from(b, |_: String| panic!("merge mapping blew up"))
                .from(cc, MergedJoinInput::C)
                .into::<MergeJoinProc>()
                .named("pm_out")
                .sink("sink");
        }
    }

    /// A plain edge `map` on an inter-procedure edge — the same driver-thread
    /// seam a merge mapping runs on, reached without a merge.
    struct PanickingTransformWorkflow;
    impl WorkflowOperative for PanickingTransformWorkflow {
        fn factory_id() -> &'static str {
            "test-panic-transform-workflow"
        }
        fn procedures(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<DiamondA>()
                .named("pt_fan")
                .codec()
                .map(|_: String| -> String { panic!("edge map blew up") })
                .into::<DiamondB>()
                .named("pt_out")
                .sink("sink");
        }
    }

    // ---- non-driver unit tests ----------------------------------------------

    #[test]
    fn workflow_error_conversions() {
        // String conversions stay retryable-by-default: the ladder is opt-out.
        let from_str: WorkflowError = "x".into();
        assert_eq!(from_str.to_string(), "x");
        assert!(!from_str.non_retryable);
        let from_string: WorkflowError = String::from("y").into();
        assert_eq!(from_string.message, "y");
        assert!(!from_string.non_retryable);

        let permanent = WorkflowError::non_retryable("z");
        assert_eq!(permanent.to_string(), "z");
        assert!(permanent.non_retryable);
    }

    #[test]
    fn register_workflow_makes_factory_resolvable() {
        struct RegWorkflow;
        impl WorkflowOperative for RegWorkflow {
            fn factory_id() -> &'static str {
                "test-getclass-workflow"
            }
            fn run(_c: &mut WorkflowCtx, _a: &str) -> Result<(), WorkflowError> {
                Ok(())
            }
        }
        assert!(registry::get_class("test-getclass-workflow").is_none());
        crate::register_workflow_class::<RegWorkflow>().unwrap();
        assert!(registry::get_class("test-getclass-workflow").is_some());
        assert!(registry::get_class("test-nonexistent-workflow").is_none());
    }

    #[test]
    fn registry_holds_workflow_as_sealed_variant() {
        let reg = OperativeRegistry::new();
        let (instance, _inbox_rx) = crate::workflow::instance::WorkflowInstance::new(
            Arc::from("t1"),
            Arc::from("t1-factory"),
            Vec::new(),
            0,
            0,
            None,
        );
        reg.register_workflow(instance);

        assert!(reg.contains("t1"));
        // A workflow is not a operative: the operative-only accessor returns None.
        assert!(reg.get_stream("t1").is_none());
        match reg.get_operative("t1").as_deref() {
            Some(Operative::Workflow(inst)) => assert_eq!(&*inst.instance_id, "t1"),
            _ => panic!("expected a WorkflowOperative variant"),
        }

        assert!(matches!(
            reg.remove("t1").as_deref(),
            Some(Operative::Workflow(_))
        ));
        assert!(!reg.contains("t1"));
    }

    #[test]
    fn run_sugar_spec_has_no_topology() {
        let spec = spec::build_spec::<EmitWorkflow>().expect("run-sugar spec builds");
        assert!(
            spec.topology.is_none(),
            "bare run() ⇒ run-sugar (no topology)"
        );
        assert!(spec.dag.order.is_empty());
    }

    #[test]
    fn structured_spec_resolves_linear_chain() {
        let spec = spec::build_spec::<ChainWorkflow>().expect("structured spec builds");
        let topology = spec.topology.as_ref().expect("structured ⇒ topology");
        // Chain is ordered entry-consumer → terminal: Stage1 then Stage2.
        let names: Vec<&str> = spec
            .dag
            .order
            .iter()
            .map(|&id| topology.procedure(id).unwrap().name.as_ref())
            .collect();
        assert_eq!(names, vec!["workflow_stage1", "workflow_stage2"]);
    }

    #[test]
    fn external_stop_only_authoring_is_rejected() {
        let err = spec::build_spec::<ExternalStopOnlyWorkflow>()
            .expect_err("external-stop-only must be rejected at registration");
        assert!(
            matches!(err, spec::BuildWorkflowError::ExternalStopOnly { .. }),
            "expected ExternalStopOnly, got {err:?}"
        );
        // Naming the missing terminal condition is part of the contract.
        assert!(err.to_string().contains("terminal condition"));
    }

    #[test]
    fn durable_structured_without_inter_proc_codec_is_rejected() {
        struct NoCodec;
        impl WorkflowOperative for NoCodec {
            fn factory_id() -> &'static str {
                "test-no-codec-workflow"
            }
            fn procedures(c: &ChannelBuilder) {
                c.input::<String>("in")
                    .into::<Stage1>()
                    .named("mid")
                    .into::<Stage2>();
            }
        }
        let err = spec::build_spec::<NoCodec>().expect_err("durable class needs the codec");
        match err {
            spec::BuildWorkflowError::MissingInterProcCodec { channel, .. } => {
                assert_eq!(channel, "mid", "the error names the codec-less channel");
            }
            other => panic!("expected MissingInterProcCodec, got {other:?}"),
        }

        // The opted-out shape of the same topology registers fine.
        struct NoCodecOptOut;
        impl WorkflowOperative for NoCodecOptOut {
            fn factory_id() -> &'static str {
                "test-no-codec-optout-workflow"
            }
            const PERSISTENCE_ENABLED: bool = false;
            fn procedures(c: &ChannelBuilder) {
                c.input::<String>("in")
                    .into::<Stage1>()
                    .named("mid")
                    .into::<Stage2>();
            }
        }
        spec::build_spec::<NoCodecOptOut>().expect("opted-out classes may omit the codec");
    }

    /// `workflow-durable-loop` registration validation: a loop needs a chain to
    /// iterate, and a durable handoff-chained loop needs a serializable terminal
    /// batch — otherwise a crash mid-loop would re-seed iteration N+1 with an
    /// empty batch and silently read as "converged".
    #[test]
    fn loop_authoring_is_validated() {
        match spec::build_spec::<LoopWithoutChain>().expect_err("run-sugar cannot loop a chain") {
            spec::BuildWorkflowError::LoopWithoutChain { .. } => {}
            other => panic!("expected LoopWithoutChain, got {other:?}"),
        }

        match spec::build_spec::<ChainedLoopNoCodec>()
            .expect_err("a chained loop must be able to journal its terminal batch")
        {
            spec::BuildWorkflowError::LoopTerminalWithoutCodec { procedure, .. } => {
                assert_eq!(
                    procedure, "workflow_stage2",
                    "the error names the terminal procedure"
                );
            }
            other => panic!("expected LoopTerminalWithoutCodec, got {other:?}"),
        }

        // An inbox-driven loop over the same topology is fine: it re-seeds each
        // iteration from its inbox, never from the terminal handoff.
        let spec = spec::build_spec::<ChatLoop>().expect("PerInput loop registers");
        assert_eq!(spec.loop_mode, LoopMode::PerInput);
        assert_eq!(spec.dag.len(), 2);
    }

    /// `workflow-entity-retry` policy surface: the defaults land on the shared
    /// `RetryPolicy`, `retry_attempts() = 0` collapses to the one-attempt
    /// fail-fast, and a ladder without a backoff is rejected at registration.
    #[test]
    fn retry_policy_is_resolved_and_validated() {
        let spec = spec::build_spec::<ChainWorkflow>().expect("defaults register");
        assert_eq!(spec.retry.max_attempts, 3, "platform default is 3 attempts");
        assert_eq!(spec.retry.backoff_base_ms, 500);

        let fail_fast =
            spec::build_spec::<RetryFailFastWorkflow>().expect("attempts = 0 registers");
        assert_eq!(
            fail_fast.retry.max_attempts, 1,
            "attempts = 0 ⇒ the single fail-fast attempt"
        );

        struct HotLoop;
        impl WorkflowOperative for HotLoop {
            fn factory_id() -> &'static str {
                "test-retry-hot-loop"
            }
            fn retry_backoff() -> Duration {
                Duration::ZERO
            }
            fn procedures(c: &ChannelBuilder) {
                retry_chain(c);
            }
        }
        match spec::build_spec::<HotLoop>().expect_err("a retry ladder needs a backoff") {
            spec::BuildWorkflowError::ZeroRetryBackoff { attempts, .. } => assert_eq!(attempts, 3),
            other => panic!("expected ZeroRetryBackoff, got {other:?}"),
        }
    }

    /// `remote-durability-tier` D5: the per-class synchronous-durability
    /// declaration reaches the spec the controller's `AddOperative` response is
    /// built from. Off by default — a class that says nothing never makes a
    /// commit barrier wait on a remote store.
    #[test]
    fn sync_durability_is_declared_per_class_and_off_by_default() {
        let default = spec::build_spec::<ChainWorkflow>().expect("defaults register");
        assert!(!default.sync_durability);

        struct SyncDurable;
        impl WorkflowOperative for SyncDurable {
            const SYNC_DURABILITY: bool = true;
            fn factory_id() -> &'static str {
                "test-sync-durable"
            }
            fn procedures(c: &ChannelBuilder) {
                retry_chain(c);
            }
        }

        let spec = spec::build_spec::<SyncDurable>().expect("a declaring class registers");
        assert!(spec.sync_durability);
    }

    /// `workflow-branching` registration validation. A DAG is accepted (with its
    /// nodes in Kahn topological order, every producer before its consumers —
    /// the invariant that makes completion order topological and its reverse a
    /// valid compensation order); a cycle is refused, naming a node on it.
    #[test]
    fn dag_topology_is_validated() {
        // A→(B,C): one channel, two consumers. The edge is (channel, consumer),
        // so both branches read what Stage1 released on `mid`.
        let spec = spec::build_spec::<TreeWorkflow>().expect("a fan-out tree registers");
        let topology = spec.topology.as_ref().expect("structured ⇒ topology");
        let names: Vec<&str> = spec
            .dag
            .order
            .iter()
            .map(|&id| topology.procedure(id).unwrap().name.as_ref())
            .collect();
        assert_eq!(
            names,
            vec!["workflow_stage1", "workflow_stage2", "workflow_echo_op"],
            "Kahn order: the root, then its children (= the former BFS numbering)"
        );
        assert_eq!(spec.dag.parents, vec![vec![], vec![0], vec![0]]);
        assert_eq!(spec.dag.children, vec![vec![1, 2], vec![], vec![]]);
        assert_eq!(spec.dag.leaves().collect::<Vec<_>>(), vec![1, 2]);
        assert!(
            spec.dag.single_leaf().is_none(),
            "two leaves ⇒ no single terminal procedure"
        );
        for (node, parents) in spec.dag.parents.iter().enumerate() {
            for p in parents {
                assert!(
                    *p < node,
                    "a producer's index always precedes its consumers'"
                );
            }
        }

        // A linear chain is the single-branch case, unchanged.
        let linear = spec::build_spec::<ChainWorkflow>().expect("linear still registers");
        assert_eq!(linear.dag.children, vec![vec![1], vec![]]);
        assert_eq!(linear.dag.single_leaf(), Some(1));

        // A join — two streams feeding one node — registers, and Kahn numbers
        // BOTH producers before it (BFS would have enqueued the join on its
        // first producer).
        match spec::build_spec::<JoinWorkflow>() {
            Ok(spec) => {
                let topology = spec.topology.as_ref().expect("structured ⇒ topology");
                let names: Vec<&str> = spec
                    .dag
                    .order
                    .iter()
                    .map(|&id| topology.procedure(id).unwrap().name.as_ref())
                    .collect();
                assert_eq!(
                    names,
                    vec![
                        "workflow_stage1",
                        "workflow_stage2",
                        "workflow_echo_op",
                        "workflow_join_op"
                    ]
                );
                assert_eq!(spec.dag.parents[3], vec![1, 2], "the join has two parents");
                assert_eq!(
                    spec.dag.leaves().collect::<Vec<_>>(),
                    vec![3],
                    "the join is the single leaf"
                );
            }
            Err(e) => panic!("a join must register (fan-in accumulates): {e}"),
        }

        // A cycle is refused with an error identifying it (surfaced through the
        // shared topology validation).
        struct Cyclic;
        impl WorkflowOperative for Cyclic {
            fn factory_id() -> &'static str {
                "test-cyclic-workflow"
            }
            fn procedures(c: &ChannelBuilder) {
                c.input::<String>("in")
                    .into::<Stage1>()
                    .named("mid")
                    .codec()
                    .into::<Stage2>()
                    .named("back")
                    .codec()
                    .into::<Stage1>();
            }
        }
        match spec::build_spec::<Cyclic>().expect_err("a cycle is refused") {
            spec::BuildWorkflowError::Topology { detail, .. } => {
                assert!(
                    detail.contains("Cycle") || detail.contains("cycle"),
                    "the refusal identifies the cycle: {detail}"
                );
            }
            other => panic!("expected the shared cycle refusal, got {other:?}"),
        }

        // `multi-input-procedures`: a workflow seed is a bare payload naming no
        // channel, so an entry channel cannot be one merge source among
        // several — the mapping could not be selected and every entry channel
        // past the first would be unreadable. Refused by name, at definition
        // time, instead of accepted and failing every invocation on a type
        // mismatch.
        struct EntryMergeWorkflow;
        impl WorkflowOperative for EntryMergeWorkflow {
            fn factory_id() -> &'static str {
                "test-entry-merge-workflow"
            }
            fn procedures(c: &ChannelBuilder) {
                c.merge::<MergedJoinInput>()
                    .from(c.input::<String>("in"), MergedJoinInput::B)
                    .from(c.input::<String>("also"), MergedJoinInput::C)
                    .into::<MergeJoinProc>();
            }
        }
        match spec::build_spec::<EntryMergeWorkflow>()
            .expect_err("a workflow merge over an entry channel is refused")
        {
            spec::BuildWorkflowError::MergeFromEntryChannel {
                channel, procedure, ..
            } => {
                assert_eq!(channel, "in");
                assert_eq!(procedure, "workflow_merge_join_op");
            }
            other => panic!("expected MergeFromEntryChannel, got {other:?}"),
        }

        // The inter-proc form of the same merge is the supported one and still
        // registers, with the merged node as a two-parent inner node.
        let merged = spec::build_spec::<MergeWorkflow>().expect("an inter-proc merge registers");
        assert_eq!(
            merged.dag.parents[3],
            vec![1, 2],
            "the merged join has both producers as parents"
        );

        // A handoff-chained loop over a branching tree has no single terminal
        // batch to seed iteration N+1 from.
        struct BranchingChained;
        impl WorkflowOperative for BranchingChained {
            fn factory_id() -> &'static str {
                "test-branching-chained-workflow"
            }
            fn loop_mode() -> LoopMode {
                LoopMode::Chained
            }
            fn procedures(c: &ChannelBuilder) {
                tree_chain(c);
            }
        }
        match spec::build_spec::<BranchingChained>()
            .expect_err("a chained loop needs one terminal procedure")
        {
            spec::BuildWorkflowError::BranchingChainedLoop { leaves, .. } => {
                assert_eq!(leaves, 2);
            }
            other => panic!("expected BranchingChainedLoop, got {other:?}"),
        }
    }

    #[test]
    fn dynamic_entity_procedure_is_rejected() {
        #[derive(Default)]
        struct DynProc;
        impl Procedure for DynProc {
            type Input = String;
            type Output = String;
            type State = ();
            type Change = NoChange;
            type Clock = WallClock;
            type Config = ();
            fn name() -> &'static str {
                "workflow_dyn_op"
            }
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
            fn run(
                &mut self,
                _input: String,
                _state: StateHandle<'_, Self>,
                _ctx: &mut ProcedureCtx<'_>,
            ) -> Result<(), ProcedureError> {
                Ok(())
            }
        }
        struct DynWorkflow;
        impl WorkflowOperative for DynWorkflow {
            fn factory_id() -> &'static str {
                "test-dyn-workflow"
            }
            fn procedures(c: &ChannelBuilder) {
                // The key rides an inter-procedure edge: a keyed ENTRY edge is
                // refused earlier, by the workflow seed's own rule.
                c.input::<String>("in")
                    .into::<EchoProc>()
                    .key_by(|s: &String| s.clone())
                    .into::<DynProc>();
            }
        }
        let err = spec::build_spec::<DynWorkflow>().expect_err("dynamic entities must be rejected");
        assert!(
            matches!(err, spec::BuildWorkflowError::DynamicEntities { .. }),
            "expected DynamicEntities, got {err:?}"
        );
    }

    // ---- single driver-observation test --------------------------------------
    //
    // Installs the process-global coordinator trigger so it can observe the
    // EmitWorkflowOutput / EmitWorkflowCompleted messages the driver routes
    // through it; the shared runner-state lock keeps that install exclusive
    // against the other tests that install one. Scenarios run strictly
    // sequentially (each drained to its completion before the next) so a
    // single shared channel stays unambiguous.
    #[test]
    fn workflow_driver_scenarios() {
        let _runner_guard = crate::runner::env_test_guard();
        let (tx, rx) = crossbeam::channel::unbounded::<RunnerMessage>();
        install_reconnect_trigger(tx);

        // Durable workflow operatives in this test ship WAL frames through the
        // process-global sink; hold the shared sink lock for the whole test so
        // it never races the entity-state sink tests, and install a capturing
        // sink to observe the frames.
        let _sink_guard = crate::entity::state_sink::SINK_TEST_LOCK.lock().unwrap();
        let wal_sink: crate::entity::flusher::SendFrameFn = Arc::new(
            |payload_id: &str, frame: &[u8]| -> Result<(), crate::entity::flusher::SendError> {
                WAL_FRAMES
                    .lock()
                    .unwrap()
                    .push((payload_id.to_string(), frame.to_vec()));
                Ok(())
            },
        );
        crate::entity::state_sink::install_global_sink(wal_sink);

        crate::register_workflow_class::<EmitWorkflow>().unwrap();
        crate::register_workflow_class::<FailWorkflow>().unwrap();
        crate::register_workflow_class::<PanicWorkflow>().unwrap();
        crate::register_workflow_class::<CompleteWorkflow>().unwrap();
        crate::register_workflow_class::<LoopWorkflow>().unwrap();
        crate::register_workflow_class::<TimedWorkflow>().unwrap();
        crate::register_workflow_class::<EchoStructured>().unwrap();
        crate::register_workflow_class::<ChainWorkflow>().unwrap();
        crate::register_workflow_class::<TreeWorkflow>().unwrap();
        crate::register_workflow_class::<TreeSaga>().unwrap();
        crate::register_workflow_class::<FanWorkflow>().unwrap();
        crate::register_workflow_class::<FanOptOutWorkflow>().unwrap();
        crate::register_workflow_class::<HybridWorkflow>().unwrap();
        crate::register_workflow_class::<SagaWorkflow>().unwrap();
        crate::register_workflow_class::<KeepProgressSaga>().unwrap();
        crate::register_workflow_class::<CleanSaga>().unwrap();
        crate::register_workflow_class::<FanSaga>().unwrap();
        crate::register_workflow_class::<StopSaga>().unwrap();
        crate::register_workflow_class::<ResumeSaga>().unwrap();
        crate::register_workflow_class::<LeakWorkflow>().unwrap();
        crate::register_workflow_class::<UndoSaga>().unwrap();
        crate::register_workflow_class::<ChatLoop>().unwrap();
        crate::register_workflow_class::<ResendChain>().unwrap();
        crate::register_workflow_class::<ResendLoop>().unwrap();
        crate::register_workflow_class::<LoopSagaAllOrNothing>().unwrap();
        crate::register_workflow_class::<LoopSagaKeepProgress>().unwrap();
        crate::register_workflow_class::<RetryWorkflow>().unwrap();
        crate::register_workflow_class::<RetryFailFastWorkflow>().unwrap();
        crate::register_workflow_class::<RetrySlowBackoffWorkflow>().unwrap();
        crate::register_workflow_class::<StepProbeWorkflow>().unwrap();
        crate::register_workflow_class::<DiamondWorkflow>().unwrap();
        crate::register_workflow_class::<DiamondSaga>().unwrap();
        crate::register_workflow_class::<MergeWorkflow>().unwrap();
        crate::register_workflow_class::<PanickingMergeWorkflow>().unwrap();
        crate::register_workflow_class::<PanickingTransformWorkflow>().unwrap();

        let mut node = RunnerNode::new("test-ns".to_string());

        // --- run-sugar: run once, args echoed, status "ok" ---
        EMIT_RUNS.store(0, Ordering::SeqCst);
        assert!(node.start_workflow(
            "test-emit-workflow".into(),
            "workflow-ok".into(),
            "hello".into()
        ));
        let (outs, done) = drain(&rx, "workflow-ok");
        assert_eq!(
            EMIT_RUNS.load(Ordering::SeqCst),
            1,
            "run executes exactly once"
        );
        assert_eq!(outs, vec![b"hello".to_vec()], "args echoed as output");
        assert_eq!((done.0.as_str(), done.1.as_str()), ("ok", ""));

        // Instance stays registered; a duplicate start is idempotent (no re-run).
        assert!(node.has_operative("workflow-ok"));
        assert!(
            node.get_stream("workflow-ok").is_none(),
            "a workflow is not a operative"
        );
        assert!(node.start_workflow(
            "test-emit-workflow".into(),
            "workflow-ok".into(),
            "hello".into()
        ));
        assert_eq!(
            EMIT_RUNS.load(Ordering::SeqCst),
            1,
            "duplicate start must not re-run"
        );

        // --- run-sugar: terminal error, with no retry ladder to climb ---
        FAIL_RUNS.store(0, Ordering::SeqCst);
        assert!(node.start_workflow(
            "test-fail-workflow".into(),
            "workflow-err".into(),
            "".into()
        ));
        let (_o, done) = drain(&rx, "workflow-err");
        assert_eq!(done.0, "error");
        assert!(done.1.contains("boom"));
        assert_eq!(
            FAIL_RUNS.load(Ordering::SeqCst),
            1,
            "a run-sugar body has no entities to retry: Err is terminal on the first failure"
        );

        // --- run-sugar: panic caught and reported as "error" ---
        assert!(node.start_workflow(
            "test-panic-workflow".into(),
            "workflow-panic".into(),
            "".into()
        ));
        let (_o, done) = drain(&rx, "workflow-panic");
        assert_eq!(done.0, "error");
        assert!(done.1.contains("kaboom"));

        // --- run-sugar: ctx.complete carries the result payload ---
        assert!(node.start_workflow(
            "test-complete-workflow".into(),
            "workflow-c".into(),
            "z".into()
        ));
        let (_o, done) = drain(&rx, "workflow-c");
        assert_eq!(done.0, "ok");
        assert_eq!(
            done.2,
            b"result:z".to_vec(),
            "complete() result reaches WorkflowCompleted"
        );

        // --- inbox + loop: post-start input is processed, terminal predicate exits ok ---
        assert!(node.start_workflow("test-loop-workflow".into(), "loop-1".into(), "".into()));
        assert!(
            node.get_workflow("loop-1")
                .is_some_and(|i| i.push_inbox(b"alpha".to_vec(), 0)),
            "inbox delivers to running instance"
        );
        assert!(node
            .get_workflow("loop-1")
            .is_some_and(|i| i.push_inbox(b"beta".to_vec(), 0)));
        assert!(node
            .get_workflow("loop-1")
            .is_some_and(|i| i.push_inbox(b"quit".to_vec(), 0)));
        let (outs, done) = drain(&rx, "loop-1");
        assert_eq!(outs, vec![b"got:alpha".to_vec(), b"got:beta".to_vec()]);
        assert_eq!(done.0, "ok", "terminal predicate ⇒ ok");

        // --- external stop (WorkflowStop handler) → cancelled ---
        assert!(node.start_workflow("test-loop-workflow".into(), "loop-2".into(), "".into()));
        assert!(node
            .get_workflow("loop-2")
            .is_some_and(|i| i.push_inbox(b"one".to_vec(), 0)));
        // Drain the echo first so the instance is definitely live before stop.
        wait_for_output(&rx, "loop-2", b"got:one");
        let stop = WorkflowStop {
            operative_id: "loop-2".into(),
        };
        let mut errs = Vec::new();
        stop.handle(&mut node, "", &mut errs);
        assert!(errs.is_empty());
        let (_o, done) = drain(&rx, "loop-2");
        assert_eq!(done.0, "cancelled", "WorkflowStop ⇒ cancelled");

        // --- hard removal (RemOperative → stop_operative) cancels a workflow ---
        assert!(node.start_workflow("test-loop-workflow".into(), "loop-3".into(), "".into()));
        assert!(node
            .get_workflow("loop-3")
            .is_some_and(|i| i.push_inbox(b"x".to_vec(), 0)));
        wait_for_output(&rx, "loop-3", b"got:x");
        assert!(
            node.stop_operative("loop-3"),
            "stop_operative removes the workflow"
        );
        assert!(!node.has_operative("loop-3"));
        let (_o, done) = drain(&rx, "loop-3");
        assert_eq!(
            done.0, "cancelled",
            "hard stop ⇒ driver cancels and tears down"
        );

        // --- max_duration backstop → cancelled ---
        assert!(node.start_workflow("test-timed-workflow".into(), "timed-1".into(), "".into()));
        let (_o, done) = drain(&rx, "timed-1");
        assert_eq!(done.0, "cancelled", "max_duration elapse ⇒ cancelled");

        // --- structured single-proc equals run-sugar (§2.2 equivalence) ---
        assert!(node.start_workflow("test-echo-structured".into(), "echo-s".into(), "foo".into()));
        let (struct_outs, done) = drain(&rx, "echo-s");
        assert_eq!(done.0, "ok");
        assert_eq!(struct_outs, vec![b"echo:foo".to_vec()]);
        // The run-sugar equivalent (EmitWorkflow-style) would emit the same bytes
        // for the same transform — assert the structured single-proc form matches
        // a hand-written one-procedure echo.
        assert_eq!(struct_outs, vec![format!("echo:{}", "foo").into_bytes()]);

        // --- structured multi-proc chain runs in order with a barrier ---
        assert!(node.start_workflow("test-chain-workflow".into(), "chain-1".into(), "x".into()));
        let (chain_outs, done) = drain(&rx, "chain-1");
        assert_eq!(done.0, "ok");
        assert_eq!(
            chain_outs,
            vec![b"x-1-2".to_vec()],
            "Stage1 then Stage2, in order"
        );

        // --- structured parallel static entities + barrier + per-entity state ---
        assert!(node.start_workflow(
            "test-fan-workflow".into(),
            "fan-1".into(),
            "a,bb,ccc,dddd".into(),
        ));
        let (mut fan_outs, done) = drain(&rx, "fan-1");
        assert_eq!(done.0, "ok");
        fan_outs.sort();
        // odd entity: "a"(1) then "ccc"(2); even entity: "bb"(1) then "dddd"(2).
        // Independent per-entity counters; the barrier collected all four.
        let mut expected: Vec<Vec<u8>> = vec![
            b"1:a".to_vec(),
            b"2:ccc".to_vec(),
            b"1:bb".to_vec(),
            b"2:dddd".to_vec(),
        ];
        expected.sort();
        assert_eq!(fan_outs, expected);

        // --- hybrid channel: inter-proc output also routed to a plugin sink ---
        assert!(node.start_workflow("test-hybrid-workflow".into(), "hybrid-1".into(), "z".into()));
        let (mut hybrid_outs, done) = drain(&rx, "hybrid-1");
        assert_eq!(done.0, "ok");
        hybrid_outs.sort();
        // "mid:h:z" from the hybrid sink on Stage1's output; "final:h:z" from
        // Stage2's terminal plugin — Stage1's value reached both Stage2 and the sink.
        let mut hybrid_expected = vec![b"final:h:z".to_vec(), b"mid:h:z".to_vec()];
        hybrid_expected.sort();
        assert_eq!(hybrid_outs, hybrid_expected);

        // --- §7b.1 saga: a failed step rolls back completed steps in reverse ---
        let _ = drain_comp_log();
        assert!(node.start_workflow("test-saga-workflow".into(), "saga-1".into(), "X".into()));
        let (_o, done) = drain(&rx, "saga-1");
        assert_eq!(done.0, "error", "a failed terminal step ⇒ status error");
        assert!(done.1.contains("ship failed"));
        // reserve → charge completed; ship failed (not compensated). Undo runs
        // reverse: charge then reserve, each receiving its own output.
        assert_eq!(
            drain_comp_log(),
            vec![
                "undo-charge:charged:reserved:X".to_string(),
                "undo-reserve:reserved:X".to_string(),
            ],
        );

        // --- §7b.2 all-or-nothing: keep-progress unwinds an identical single
        // pass (the mode only diverges with committed iterations) — proves
        // rollback_mode() reaches the driver ---
        let _ = drain_comp_log();
        assert!(node.start_workflow(
            "test-keep-progress-saga".into(),
            "saga-kp".into(),
            "Y".into()
        ));
        let (_o, done) = drain(&rx, "saga-kp");
        assert_eq!(done.0, "error");
        assert_eq!(
            drain_comp_log(),
            vec![
                "undo-charge:charged:reserved:Y".to_string(),
                "undo-reserve:reserved:Y".to_string(),
            ],
        );

        // --- §7b.1 clean completion does NOT compensate ---
        let _ = drain_comp_log();
        assert!(node.start_workflow("test-clean-saga".into(), "saga-ok".into(), "Z".into()));
        let (clean_outs, done) = drain(&rx, "saga-ok");
        assert_eq!(done.0, "ok");
        assert_eq!(clean_outs, vec![b"echo:charged:reserved:Z".to_vec()]);
        assert!(
            drain_comp_log().is_empty(),
            "a clean ok must not compensate"
        );

        // --- §7b.1 per-entity compensation: once per entity that ran;
        // hookless SplitProc is skipped ---
        let _ = drain_comp_log();
        assert!(node.start_workflow("test-fan-saga".into(), "fan-saga".into(), "a,bb,ccc".into()));
        let (_o, done) = drain(&rx, "fan-saga");
        assert_eq!(done.0, "error");
        let mut fan_comp = drain_comp_log();
        fan_comp.sort();
        // odd entity ran "a","ccc"; even ran "bb"; the hook fires once per
        // entity with that entity's own outputs. SplitProc (no hook) is skipped.
        assert_eq!(
            fan_comp,
            vec![
                "undo-fan:even:[even:bb]".to_string(),
                "undo-fan:odd:[odd:a,odd:ccc]".to_string(),
            ],
        );

        // --- §7b.1 forced-stop after partial progress compensates ---
        let _ = drain_comp_log();
        STAGE0_RAN.store(false, Ordering::SeqCst);
        STAGE0_RELEASE.store(false, Ordering::SeqCst);
        assert!(node.start_workflow("test-stop-saga".into(), "stop-saga".into(), "W".into()));
        while !STAGE0_RAN.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(5));
        }
        node.get_workflow("stop-saga")
            .expect("live instance accepts cancel")
            .request_cancel();
        STAGE0_RELEASE.store(true, Ordering::SeqCst);
        let (_o, done) = drain(&rx, "stop-saga");
        assert_eq!(done.0, "cancelled", "cooperative stop ⇒ cancelled");
        // StopReserve completed before the stop landed; Echo never ran.
        assert_eq!(
            drain_comp_log(),
            vec!["undo-stop-reserve:reserved:W".to_string()],
        );

        // --- AddOperative --kind workflow routes to the workflow path ---
        let add = AddOperative {
            operative_id: "workflow-via-add".into(),
            factory_id: "test-emit-workflow".into(),
            init_values: "argz".into(),
            kind: "workflow".into(),
            resume_entity_count: 0,
            proc_journal: None,
            ..Default::default()
        };
        let mut errors = Vec::new();
        add.handle(&mut node, "", &mut errors);
        assert!(
            errors.is_empty(),
            "AddOperative --kind workflow should not error: {errors:?}"
        );
        let (outs, done) = drain(&rx, "workflow-via-add");
        assert_eq!(outs, vec![b"argz".to_vec()]);
        assert_eq!(done.0, "ok");

        // --- AddOperative --kind workflow for an unregistered factory reports it ---
        let add_bad = AddOperative {
            operative_id: "workflow-bad".into(),
            factory_id: "test-unregistered".into(),
            init_values: "".into(),
            kind: "workflow".into(),
            resume_entity_count: 0,
            proc_journal: None,
            ..Default::default()
        };
        let mut errors_bad = Vec::new();
        add_bad.handle(&mut node, "", &mut errors_bad);
        assert_eq!(errors_bad.len(), 1);
        assert!(errors_bad[0].contains("WorkflowOperative class"));

        // --- AddOperative with an unknown kind literal is rejected ---
        let add_unknown = AddOperative {
            operative_id: "kind-bad".into(),
            factory_id: "test-emit-workflow".into(),
            init_values: "".into(),
            kind: "continuous".into(),
            resume_entity_count: 0,
            proc_journal: None,
            ..Default::default()
        };
        let mut errors_unknown = Vec::new();
        add_unknown.handle(&mut node, "", &mut errors_unknown);
        assert_eq!(errors_unknown.len(), 1);
        assert!(errors_unknown[0].contains("unknown compute-unit kind 'continuous'"));
        assert!(node.get_workflow("kind-bad").is_none());

        // --- §5b.1/§7c.1 durable workflow actually writes WAL; opt-out writes nothing ---
        // FanWorkflow is durable-by-default; its FanProc applies a Change per item,
        // which the driver flushes as an entity_state_change frame to the sink.
        WAL_FRAMES.lock().unwrap().clear();
        assert!(node.start_workflow("test-fan-workflow".into(), "fan-wal".into(), "a,bb".into()));
        let (_o, done) = drain(&rx, "fan-wal");
        assert_eq!(done.0, "ok");
        assert!(
            !WAL_FRAMES.lock().unwrap().is_empty(),
            "a durable workflow operative ships entity_state_change frames to the WAL"
        );

        // The opt-out twin does identical work with PERSISTENCE_ENABLED=false:
        // Changes drain-and-discard, nothing reaches the WAL.
        WAL_FRAMES.lock().unwrap().clear();
        assert!(node.start_workflow(
            "test-fan-optout-workflow".into(),
            "fan-optout".into(),
            "a,bb".into(),
        ));
        let (_o, done) = drain(&rx, "fan-optout");
        assert_eq!(done.0, "ok");
        assert!(
            WAL_FRAMES.lock().unwrap().is_empty(),
            "an opt-out workflow operative writes nothing to the WAL"
        );

        // --- §5b.2 crash-resume gate: a durable structured workflow holds its
        // first Procedure until the Controller's Entity restores land, and its
        // Entities are reachable by the restore handler via wire identity ---
        WAL_FRAMES.lock().unwrap().clear();
        // Spawn as a "respawn" expecting one Entity restore: the driver builds
        // and registers its Entities, then parks on the resume gate — nothing
        // runs yet because no restore has landed.
        assert!(node.start_workflow_resumable(
            "test-fan-workflow".into(),
            "resume-inst".into(),
            "a,bb".into(),
            1,
            0,
            None,
        ));
        // The workflow Entities are now reachable by wire identity — the restore
        // handler could not see them before this change (the stream lookup
        // only scans StreamInstance entities).
        let even_id = crate::entity::flusher::encode_entity_identity(
            "resume-inst",
            "workflow_fan_op",
            "even",
        );
        let (resume_instance, _slot) = node
            .find_workflow_entity(&even_id)
            .expect("a durable workflow Entity is reachable by the restore handler");
        // Release one restore, exactly as the RestoreInitState handler does after
        // applying a persisted Entity: the parked driver wakes and runs to
        // completion (resume-not-cold-start is proven end-to-end in the Elixir
        // integration test, which does the real WAL round-trip).
        resume_instance.note_restored();
        let (_o, done) = drain(&rx, "resume-inst");
        assert_eq!(
            done.0, "ok",
            "the driver resumes once the expected Entity restore lands"
        );

        // --- workflow-proc-journal §D3: a durable structured chain commits every
        // proc boundary — one ProcCommit per stage, gated on the (fake
        // controller's) durable ack, carrying the codec-serialized handoff ---
        let _ = drain_proc_commits();
        assert!(node.start_workflow(
            "test-chain-workflow".into(),
            "chain-commit".into(),
            "x".into()
        ));
        let (outs, done) = drain(&rx, "chain-commit");
        assert_eq!(done.0, "ok");
        assert_eq!(outs, vec![b"x-1-2".to_vec()]);
        let commits = drain_proc_commits();
        assert_eq!(
            commits.iter().map(|c| c.proc_idx).collect::<Vec<_>>(),
            vec![0, 1],
            "one gated ProcCommit per completed stage, in chain order"
        );
        assert!(commits.iter().all(|c| c.operative_id == "chain-commit"));
        // Stage 0 hands off on the codec'd `mid` channel (id 1: in=0, mid=1);
        // the terminal stage's output is plugin-bound (no codec) ⇒ empty.
        assert_eq!(commits[0].handoff.len(), 1);
        assert_eq!(commits[0].handoff[0].channel_id, 1);
        assert_eq!(
            postcard::from_bytes::<String>(&commits[0].handoff[0].payload).unwrap(),
            "x-1"
        );
        assert!(commits[1].handoff.is_empty());
        assert_eq!(
            commits[0].entities_ran.len(),
            1,
            "one entity ran in stage 0"
        );

        // workflow-transactional-outbox: the terminal stage's plugin output did
        // NOT go to the wire — it was staged and rode INSIDE that stage's
        // commit, keyed by the producing entity and its per-proc sequence.
        assert!(
            wire_emits_for("chain-commit").is_empty(),
            "a durable structured operative stages: nothing reaches the wire mid-proc"
        );
        assert!(
            commits[0].outbox.is_empty(),
            "stage 0's output is inter-proc (codec handoff), not plugin-bound"
        );
        assert_eq!(commits[1].outbox.len(), 1, "stage 1's plugin output staged");
        assert_eq!(commits[1].outbox[0].payload, b"x-1-2".to_vec());
        assert_eq!(commits[1].outbox[0].channel, "out");
        assert_eq!(commits[1].outbox[0].seq_in_proc, 0);
        assert!(!commits[1].outbox[0].entity_id.is_empty());
        assert!(commits.iter().all(|c| !c.compensating));

        // The run-sugar path is untouched: no procedures, no barrier to stage
        // against, so its emit goes straight to the wire (at-least-once).
        assert_eq!(
            wire_emits_for("workflow-ok"),
            vec![b"hello".to_vec()],
            "run-sugar keeps the direct wire path"
        );

        // Opt-out twin: no journal, no handshake — zero commits, and (having
        // nothing that could ever flush a staged outbox) it still emits direct.
        assert!(node.start_workflow(
            "test-fan-optout-workflow".into(),
            "optout-no-commit".into(),
            "a".into(),
        ));
        let (outs, done) = drain(&rx, "optout-no-commit");
        assert_eq!(done.0, "ok");
        assert!(
            drain_proc_commits().is_empty(),
            "an opted-out class skips the proc-commit handshake entirely"
        );
        assert_eq!(
            wire_emits_for("optout-no-commit"),
            outs,
            "an opted-out class emits straight to the wire, as before"
        );
        assert_eq!(outs, vec![b"1:a".to_vec()]);

        // --- workflow-transactional-outbox: a failed procedure leaks nothing ---
        assert!(node.start_workflow("test-leak-workflow".into(), "leak-1".into(), "x".into()));
        let (outs, done) = drain(&rx, "leak-1");
        assert_eq!(done.0, "error");
        assert!(
            outs.is_empty() && wire_emits_for("leak-1").is_empty(),
            "the failing proc emitted before it failed; that output must never be delivered"
        );
        assert!(
            drain_proc_commits().is_empty(),
            "the proc never committed, so its staged output was never journaled"
        );

        // --- workflow-transactional-outbox §D5: a compensate hook's undo output
        // is staged too, and rides a `compensating` commit for the procedure it
        // reverses (the forward output already flushed under its own key) ---
        assert!(node.start_workflow("test-undo-saga".into(), "undo-1".into(), "u".into()));
        let (outs, done) = drain(&rx, "undo-1");
        assert_eq!(done.0, "error");
        assert!(
            wire_emits_for("undo-1").is_empty(),
            "undo output stages like forward output — never straight to the wire"
        );
        let commits = drain_proc_commits();
        let forward: Vec<_> = commits.iter().filter(|c| !c.compensating).collect();
        let undo: Vec<_> = commits.iter().filter(|c| c.compensating).collect();
        assert_eq!(forward.len(), 1, "only UndoEmit completed forward");
        assert_eq!(
            forward[0].outbox[0].payload,
            b"did:u".to_vec(),
            "the hybrid channel staged the forward copy"
        );
        assert_eq!(undo.len(), 1, "one compensated entity ⇒ one undo record");
        assert_eq!(undo[0].proc_idx, 0, "the compensated procedure's index");
        assert_eq!(undo[0].outbox.len(), 1);
        assert_eq!(undo[0].outbox[0].payload, b"undo:did:u".to_vec());
        assert_eq!(undo[0].outbox[0].channel, "mid");
        assert!(
            undo[0].handoff.is_empty(),
            "an undo record carries outbox only"
        );
        // Both the forward copy and the undo reached the plugins (the fake
        // controller flushes each commit's outbox on ack), in that order.
        assert_eq!(outs, vec![b"did:u".to_vec(), b"undo:did:u".to_vec()]);

        // --- workflow-proc-journal §D5: resume-skip — a shipped journal skips the
        // committed stages and seeds the next one from the journaled handoff ---
        let (outs, done) = resume_chain_with_journal(
            &node,
            &rx,
            "chain-resume",
            instance::JournalResume {
                committed_proc_idxs: vec![0],
                // The seed is the COMMITTING procedure's own handoff — keyed by
                // its proc_idx, so a fan-out's children each take their own
                // parent's edge (`workflow-branching`).
                proc_handoffs: vec![instance::EdgeHandoff {
                    proc_idx: 0,
                    handoff: vec![(1, postcard::to_allocvec(&"seed".to_string()).unwrap())],
                }],
                ..Default::default()
            },
        );
        assert_eq!(done.0, "ok");
        assert_eq!(
            outs,
            vec![b"seed-2".to_vec()],
            "stage 0 is skipped (no `-1` suffix, args unused); stage 1 runs \
             seeded from the journaled handoff"
        );
        assert_eq!(
            drain_proc_commits()
                .iter()
                .map(|c| c.proc_idx)
                .collect::<Vec<_>>(),
            vec![1],
            "only the resumed (uncommitted) stage re-commits"
        );

        // A journal recording EVERY stage committed resumes straight to a
        // clean completion — nothing re-runs, nothing re-commits.
        let (outs, done) = resume_chain_with_journal(
            &node,
            &rx,
            "chain-done",
            instance::JournalResume {
                committed_proc_idxs: vec![0, 1],
                ..Default::default()
            },
        );
        assert_eq!(done.0, "ok");
        assert!(outs.is_empty(), "a fully-committed chain re-runs nothing");
        assert!(drain_proc_commits().is_empty());

        // --- §4.3 durable saga records: a fresh abnormal run emits one
        // compensation_intent per completed compensable entity and one
        // compensation_done per compensated entity ---
        use crate::entity::state_sink::{
            COMPENSATION_DONE_PAYLOAD_ID as DONE, COMPENSATION_INTENT_PAYLOAD_ID as INTENT,
        };
        WAL_FRAMES.lock().unwrap().clear();
        let _ = drain_comp_log();
        // reserve + charge complete (both compensable ⇒ 2 intents); ship fails ⇒
        // both roll back (2 dones). Hookless ship_fails never completed ⇒ no record.
        assert!(node.start_workflow("test-saga-workflow".into(), "saga-rec".into(), "42".into()));
        let (_o, done) = drain(&rx, "saga-rec");
        assert_eq!(done.0, "error");
        assert_eq!(
            count_wal_frames(INTENT),
            2,
            "one intent per completed compensable entity"
        );
        assert_eq!(count_wal_frames(DONE), 2, "one done per compensated entity");

        // --- §4.3 crash-resume finishes the remaining hooks, skipping the ones
        // already recorded done, and does NOT re-run the forward chain ---
        WAL_FRAMES.lock().unwrap().clear();
        let _ = drain_comp_log();
        // Respawn a durable saga expecting one restore: the driver parks on the
        // gate while we seed the markers the RestoreInitState handler would carry.
        assert!(node.start_workflow_resumable(
            "test-resume-saga".into(),
            "resume-saga".into(),
            "x".into(),
            1,
            0,
            None,
        ));
        let a_id =
            crate::entity::flusher::encode_entity_identity("resume-saga", "resume_undo_a", "");
        let b_id =
            crate::entity::flusher::encode_entity_identity("resume-saga", "resume_undo_b", "");
        let (saga_inst, _slot) = node
            .find_workflow_entity(&a_id)
            .expect("a durable workflow saga Entity is reachable by the restore handler");
        // Original run: A + B both completed (intent); B was already undone before
        // the crash (done). Resume must undo A and SKIP B — in reverse — without
        // re-running the forward chain (so no fresh intents, no `a:`/`b:` output).
        saga_inst.note_compensation_markers(&a_id, true, false);
        saga_inst.note_compensation_markers(&b_id, true, true);
        saga_inst.note_restored();
        let (outs, done) = drain(&rx, "resume-saga");
        assert_eq!(
            done.0, "error",
            "a resumed compensation terminates with error"
        );
        assert!(
            outs.is_empty(),
            "compensation-resume does not re-run forward procs"
        );
        assert_eq!(
            drain_comp_log(),
            vec!["undo-A".to_string()],
            "A is undone; B (already done) is skipped"
        );
        assert_eq!(
            count_wal_frames(DONE),
            1,
            "only A emits a fresh compensation_done; B was skipped"
        );
        assert_eq!(
            count_wal_frames(INTENT),
            0,
            "a compensation-resume records no new intent (forward chain not re-run)"
        );

        // --- workflow-branching fan-in: the join runs once, after BOTH
        // producers committed, with the accumulated batch ---
        let _ = drain_diamond_log();
        let _ = drain_proc_commits();
        assert!(node.start_workflow(
            "test-diamond-workflow".into(),
            "diamond-1".into(),
            "x".into()
        ));
        let (outs, done) = drain(&rx, "diamond-1");
        assert_eq!(done.0, "ok");
        assert_eq!(
            outs,
            vec![b"j:b:a:x".to_vec(), b"j:c:a:x".to_vec()],
            "the join's input batch accumulates BOTH producers' outputs (B's first: sibling order)"
        );
        assert_eq!(
            drain_proc_commits()
                .iter()
                .map(|c| c.proc_idx)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3],
            "one commit per node — the join commits exactly once, after both producers"
        );
        let log = drain_diamond_log();
        let first_join = log
            .iter()
            .position(|e| e.starts_with("j:"))
            .expect("the join ran");
        assert!(
            log[..first_join].iter().any(|e| e.starts_with("b:"))
                && log[..first_join].iter().any(|e| e.starts_with("c:")),
            "both producers ran before the join was dispatched: {log:?}"
        );
        assert_eq!(
            log.iter().filter(|e| e.starts_with("j:")).count(),
            2,
            "the join processed each contribution exactly once"
        );

        // --- workflow-branching crash-resume over a join: one journaled and one
        // fresh producer — the committed branch never re-runs, and the join
        // accumulates B's JOURNALED handoff with C's fresh output ---
        let _ = drain_diamond_log();
        let da_id = crate::entity::flusher::encode_entity_identity("diamond-r", "diamond_a", "");
        let diamond_spec =
            registry::get_class("test-diamond-workflow").expect("diamond class registered");
        let diamond_topo = diamond_spec.topology.as_ref().expect("structured");
        let d_fan = diamond_topo.lookup_channel_id("d_fan").unwrap();
        let d_b = diamond_topo.lookup_channel_id("d_b").unwrap();
        let enc = |s: &str| postcard::to_allocvec(&s.to_string()).unwrap();
        // The journal a diamond-shaped chain leaves when A (the fan) and B (one
        // branch) committed before the crash: each producer's edge handoff, plus
        // the entities each of them ran. Shared by the branching resume below and
        // by both merge resumes — the shape is the same, only the channels and
        // the entities each proc ran differ.
        let ab_committed = |fan: u32, b: u32, a_segs, b_segs| instance::JournalResume {
            committed_proc_idxs: vec![0, 1],
            proc_handoffs: vec![
                instance::EdgeHandoff {
                    proc_idx: 0,
                    handoff: vec![(fan, enc("a:x"))],
                },
                instance::EdgeHandoff {
                    proc_idx: 1,
                    handoff: vec![(b, enc("b:a:x"))],
                },
            ],
            committed_procs: vec![
                instance::CommittedProc {
                    proc_idx: 0,
                    iter: 0,
                    entities_ran: a_segs,
                },
                instance::CommittedProc {
                    proc_idx: 1,
                    iter: 0,
                    entities_ran: b_segs,
                },
            ],
            ..Default::default()
        };
        assert!(node.start_workflow_resumable(
            "test-diamond-workflow".into(),
            "diamond-r".into(),
            "x".into(),
            1,
            0,
            Some(ab_committed(d_fan, d_b, vec![da_id.clone()], Vec::new()))
        ));
        let (diamond_inst, _slot) = node
            .find_workflow_entity(&da_id)
            .expect("a durable diamond Entity is reachable by the restore handler");
        diamond_inst.note_restored();
        let (outs, done) = drain(&rx, "diamond-r");
        assert_eq!(done.0, "ok");
        let log = drain_diamond_log();
        assert!(
            !log.iter()
                .any(|e| e.starts_with("a:") || e.starts_with("b:")),
            "committed producers do not re-run: {log:?}"
        );
        assert_eq!(
            log.iter().filter(|e| e.starts_with("c:")).count(),
            1,
            "only the uncommitted branch re-runs"
        );
        assert_eq!(
            outs,
            vec![b"j:b:a:x".to_vec(), b"j:c:a:x".to_vec()],
            "the join runs once, with B's journaled handoff banked and C's fresh output appended"
        );
        assert_eq!(
            drain_proc_commits()
                .iter()
                .map(|c| c.proc_idx)
                .collect::<Vec<_>>(),
            vec![2, 3],
            "the resume commits only C and the join"
        );

        // --- workflow-branching reverse-topological undo: the join is undone
        // before either producer, the shared root last ---
        let _ = drain_comp_log();
        let _ = drain_diamond_log();
        let _ = drain_proc_commits();
        assert!(node.start_workflow("test-diamond-saga".into(), "diamond-s".into(), "x".into()));
        let (_o, done) = drain(&rx, "diamond-s");
        assert_eq!(done.0, "error");
        assert_eq!(
            drain_comp_log(),
            vec![
                "undo-j:j:b:a:x".to_string(),
                "undo-j:j:c:a:x".to_string(),
                "undo-c:c:a:x".to_string(),
                "undo-b:b:a:x".to_string(),
                "undo-a:a:x".to_string(),
            ],
            "reverse-topological over the DAG: join first, shared root last"
        );

        // --- multi-input-procedures: the same fan-in wired as a merge. Each
        // producer's contribution reaches the join through ITS edge's mapping,
        // so a node whose Input is neither producer's Output still dispatches ---
        let _ = drain_diamond_log();
        let _ = drain_proc_commits();
        assert!(node.start_workflow("test-merge-workflow".into(), "merge-1".into(), "x".into()));
        let (outs, done) = drain(&rx, "merge-1");
        assert_eq!(done.0, "ok");
        assert_eq!(
            outs,
            vec![b"mb:b:a:x".to_vec(), b"mc:c:a:x".to_vec()],
            "each contribution arrives mapped by the edge it came in on"
        );

        // --- and the journal re-seed maps too: B's handoff is decoded with the
        // channel's codec, then lifted by the same mapping the live run used ---
        let _ = drain_diamond_log();
        let _ = drain_proc_commits();
        let ma_id = crate::entity::flusher::encode_entity_identity("merge-r", "diamond_a", "");
        let merge_spec =
            registry::get_class("test-merge-workflow").expect("merge class registered");
        let merge_topo = merge_spec.topology.as_ref().expect("structured");
        let m_fan = merge_topo.lookup_channel_id("m_fan").unwrap();
        let m_b = merge_topo.lookup_channel_id("m_b").unwrap();
        assert!(node.start_workflow_resumable(
            "test-merge-workflow".into(),
            "merge-r".into(),
            "x".into(),
            1,
            0,
            Some(ab_committed(m_fan, m_b, vec![ma_id.clone()], Vec::new()))
        ));
        let (merge_inst, _slot) = node
            .find_workflow_entity(&ma_id)
            .expect("a durable merge Entity is reachable by the restore handler");
        merge_inst.note_restored();
        let (outs, done) = drain(&rx, "merge-r");
        assert_eq!(done.0, "ok");
        assert_eq!(
            outs,
            vec![b"mb:b:a:x".to_vec(), b"mc:c:a:x".to_vec()],
            "the resumed join banks B's journaled handoff mapped, C's fresh output mapped"
        );

        // --- a panicking mapping is CONTAINED. It runs on the driver thread,
        // outside any entity invocation, so an unguarded unwind would kill the
        // thread before `emit_workflow_completed` and strand the instance ---
        let _ = drain_comp_log();
        let _ = drain_diamond_log();
        let _ = drain_proc_commits();
        assert!(node.start_workflow(
            "test-panic-merge-workflow".into(),
            "merge-panic".into(),
            "x".into()
        ));
        let (outs, done) = drain(&rx, "merge-panic");
        assert_eq!(
            done.0, "error",
            "the operative still REPORTS — the panic did not take the driver with it"
        );
        assert!(
            done.1.contains("edge transform panicked") && done.1.contains("merge mapping blew up"),
            "the terminal error names the mapping and carries the panic message: {:?}",
            done.1
        );
        assert!(outs.is_empty(), "the join never ran, so nothing shipped");
        // The saga fires over exactly what was RECORDED as completed when the
        // abort began: the producer whose edge blew up (B) and its parent (A),
        // reverse-topologically. C ran in the same level but the abort returns
        // before its stage record is pushed, so it is not undone — the same
        // mid-level shape a failing proc commit has always had. Asserted as an
        // exact list so the Java arm is pinned to it, not merely compatible.
        let comp = drain_comp_log();
        assert_eq!(
            comp,
            vec!["undo-b:b:a:x".to_string(), "undo-a:a:x".to_string()],
            "the contained mapping panic compensates the recorded stages, in reverse"
        );
        let panic_log = drain_diamond_log();
        assert!(
            panic_log.contains(&"c:a:x".to_string()),
            "the sibling DID run in the same level (and is deliberately not in the \
             undo list above): {panic_log:?}"
        );

        // --- the same containment reached WITHOUT a merge: a plain `.map` on
        // an inter-procedure edge is the same composed closure on the same
        // driver-thread seam ---
        let _ = drain_comp_log();
        let _ = drain_diamond_log();
        let _ = drain_proc_commits();
        assert!(node.start_workflow(
            "test-panic-transform-workflow".into(),
            "map-panic".into(),
            "x".into()
        ));
        let (outs, done) = drain(&rx, "map-panic");
        assert_eq!(
            done.0, "error",
            "a panicking edge map still REPORTS — the driver thread survived"
        );
        assert!(
            done.1.contains("edge transform panicked") && done.1.contains("edge map blew up"),
            "the terminal error carries the panic message: {:?}",
            done.1
        );
        assert!(
            outs.is_empty(),
            "the consumer never ran, so nothing shipped"
        );
        assert_eq!(
            drain_comp_log(),
            vec!["undo-a:a:x".to_string()],
            "the producer whose edge blew up is inside the undo scope"
        );

        // --- the SECOND mapping site: a crash-resume re-seed. `resumed_start`
        // maps a committed producer's JOURNALED handoff into its uncommitted
        // consumers, on the driver thread and before any stage runs, so it needs
        // the same containment as the forward release — and here the undo scope
        // is the journal's own stages ---
        let _ = drain_comp_log();
        let _ = drain_diamond_log();
        let _ = drain_proc_commits();
        let pa_id =
            crate::entity::flusher::encode_entity_identity("merge-panic-r", "diamond_a", "");
        let pb_id =
            crate::entity::flusher::encode_entity_identity("merge-panic-r", "diamond_b", "");
        let panic_spec =
            registry::get_class("test-panic-merge-workflow").expect("panic merge class registered");
        let panic_topo = panic_spec.topology.as_ref().expect("structured");
        let pm_fan = panic_topo.lookup_channel_id("pm_fan").unwrap();
        let pm_b = panic_topo.lookup_channel_id("pm_b").unwrap();
        WAL_FRAMES.lock().unwrap().clear();
        assert!(node.start_workflow_resumable(
            "test-panic-merge-workflow".into(),
            "merge-panic-r".into(),
            "x".into(),
            1,
            0,
            Some(ab_committed(
                pm_fan,
                pm_b,
                vec![pa_id.clone()],
                vec![pb_id.clone()],
            ))
        ));
        let (panic_inst, _slot) = node
            .find_workflow_entity(&pa_id)
            .expect("a durable resumed Entity is reachable by the restore handler");
        panic_inst.note_restored();
        let (outs, done) = drain(&rx, "merge-panic-r");
        assert_eq!(
            done.0, "error",
            "a mapping that panics while re-seeding still REPORTS"
        );
        assert!(
            done.1.contains("edge transform panicked") && done.1.contains("merge mapping blew up"),
            "the terminal error names the mapping: {:?}",
            done.1
        );
        assert!(outs.is_empty(), "no stage ran, so nothing shipped");
        // The journaled stages ARE the undo scope here — `drive_chain` seeds the
        // resumed iteration's completed list from the journal. The comp log is
        // NOT the signal to read: `diamond_op!` logs once per completed output
        // and a journal-rebuilt EntityRun carries none, so a real compensation
        // leaves it empty. The compensation-done markers the sink already
        // captures are, and they carry the entity they undid.
        let done_frames: Vec<Vec<u8>> = WAL_FRAMES
            .lock()
            .unwrap()
            .iter()
            .filter(|(pid, _)| pid == DONE)
            .map(|(_, frame)| frame.clone())
            .collect();
        let carries = |frame: &[u8], id: &[u8]| frame.windows(id.len()).any(|w| w == id);
        assert_eq!(
            done_frames.len(),
            2,
            "both journaled stages were compensated"
        );
        assert!(
            carries(&done_frames[0], &pb_id) && carries(&done_frames[1], &pa_id),
            "reverse-topological: B's entity is undone before A's"
        );

        // --- workflow-durable-loop: one iteration per inbox item, each ending in
        // a durable iteration commit; an idle inbox parks the instance ---
        std::env::set_var("FLEZHA_WORKFLOW_PARK_IDLE_MS", "300");
        let _ = drain_proc_commits();
        assert!(node.start_workflow("test-chat-loop".into(), "chat-1".into(), "ignored".into()));
        let chat = node
            .get_workflow("chat-1")
            .expect("the loop's instance is registered before its driver runs");
        // Two chat turns, at durable-inbox offsets 4 and 7 (the controller's
        // offsets are opaque to the driver — it only echoes them back).
        assert!(chat.push_inbox(b"hi".to_vec(), 4));
        assert!(chat.push_inbox(b"again".to_vec(), 7));

        let (outs, done) = drain(&rx, "chat-1");
        assert_eq!(
            done.0, "parked",
            "an idle inbox at a committed boundary parks — NOT a terminal completion"
        );
        assert_eq!(
            outs,
            vec![b"hi-1-2".to_vec(), b"again-1-2".to_vec()],
            "one full pass of the chain per inbox item"
        );

        let iters = drain_iter_commits();
        assert_eq!(
            iters.iter().map(|c| c.iter).collect::<Vec<_>>(),
            vec![0, 1],
            "one iteration commit per turn, in order"
        );
        assert_eq!(
            iters.iter().map(|c| c.consumed_offset).collect::<Vec<_>>(),
            vec![4, 7],
            "each commit records the inbox offset its iteration consumed"
        );
        assert_eq!(
            drain_proc_commits()
                .iter()
                .map(|c| (c.iter, c.proc_idx))
                .collect::<Vec<_>>(),
            vec![(0, 0), (0, 1), (1, 0), (1, 1)],
            "every proc commit is stamped with its enclosing iteration"
        );

        let parks = drain_iter_parks();
        assert_eq!(parks.len(), 1, "exactly one park");
        assert_eq!(
            parks[0].iter, 1,
            "the park names the last COMMITTED iteration"
        );
        assert_eq!(
            parks[0].wake_deadline_ms, 0,
            "no wake_after declared ⇒ park until the next input"
        );

        // --- resume-at-iteration: a journal recording iterations 0-1 committed
        // enters iteration 2 — the committed turns never re-run ---
        let s1_id = crate::entity::flusher::encode_entity_identity("chat-2", "workflow_stage1", "");
        assert!(node.start_workflow_resumable(
            "test-chat-loop".into(),
            "chat-2".into(),
            "ignored".into(),
            1,
            0,
            Some(instance::JournalResume {
                iter: 2,
                consumed_offset: 7,
                committed_procs: vec![
                    instance::CommittedProc {
                        proc_idx: 0,
                        iter: 0,
                        entities_ran: vec![s1_id.clone()],
                    },
                    instance::CommittedProc {
                        proc_idx: 1,
                        iter: 0,
                        entities_ran: Vec::new(),
                    },
                ],
                ..Default::default()
            })
        ));
        let (chat2, _slot) = node
            .find_workflow_entity(&s1_id)
            .expect("the loop's Entity is reachable by the restore handler");
        chat2.note_restored();
        assert!(chat2.push_inbox(b"third".to_vec(), 9));

        let (outs, done) = drain(&rx, "chat-2");
        assert_eq!(done.0, "parked");
        assert_eq!(
            outs,
            vec![b"third-1-2".to_vec()],
            "only the new turn runs; the journaled iterations do not re-run"
        );
        let iters = drain_iter_commits();
        assert_eq!(
            iters.iter().map(|c| c.iter).collect::<Vec<_>>(),
            vec![2],
            "the resumed run commits iteration 2 — never 0 or 1 again"
        );
        assert_eq!(iters[0].consumed_offset, 9);

        // --- workflow-durable-loop D2: KeepProgress/AllOrNothing are backed by
        // the JOURNAL — the `committed` iterations a crash-resumed run rolls
        // back come from `iter_committed` records, not from memory ---
        for (factory, id, expected) in [
            // All-or-nothing: the failing turn unwinds itself AND the journaled
            // turn below it. `undo-loop:1` is the in-flight iteration (outputs
            // retained in memory); `undo-loop:0` the journal-rebuilt one.
            (
                "test-loop-saga-aon",
                "loop-saga-aon",
                vec!["undo-loop:1".to_string(), "undo-loop:0".to_string()],
            ),
            // Keep-progress: the committed turn is kept; only the in-flight one
            // unwinds.
            (
                "test-loop-saga-keep",
                "loop-saga-keep",
                vec!["undo-loop:1".to_string()],
            ),
        ] {
            let _ = drain_comp_log();
            let seg = crate::entity::flusher::encode_entity_identity(id, "loop_reserve", "");
            // Journal: iteration 0 committed both of its procedures.
            assert!(node.start_workflow_resumable(
                factory.into(),
                id.into(),
                "ignored".into(),
                1,
                0,
                Some(instance::JournalResume {
                    iter: 1,
                    consumed_offset: 2,
                    committed_procs: vec![
                        instance::CommittedProc {
                            proc_idx: 0,
                            iter: 0,
                            entities_ran: vec![seg.clone()],
                        },
                        instance::CommittedProc {
                            proc_idx: 1,
                            iter: 0,
                            entities_ran: Vec::new(),
                        },
                    ],
                    ..Default::default()
                })
            ));
            let (inst, _slot) = node
                .find_workflow_entity(&seg)
                .expect("the loop saga's Entity is reachable by the restore handler");
            inst.note_restored();
            assert!(inst.push_inbox(b"boom".to_vec(), 5));

            let (_outs, done) = drain(&rx, id);
            assert_eq!(done.0, "error", "the failing turn terminates the run");
            assert_eq!(
                drain_comp_log(),
                expected,
                "{factory}: rollback scope across a journaled iteration boundary"
            );
        }

        // --- workflow-loop-rewind: a journal carrying `rewind_undo` unwinds those
        // iterations — reverse iteration, reverse stage — BEFORE the loop resumes
        // forward at the rewound-to iteration ---
        let _ = drain_comp_log();
        let _ = drain_proc_commits();
        // Keep-progress deliberately: a rewind unwinds committed iterations by
        // definition, so the undo must be forced all-or-nothing whatever the
        // class's crash rollback mode says.
        let id = "loop-rewind";
        let seg = crate::entity::flusher::encode_entity_identity(id, "loop_reserve", "");
        // Rewound to iteration 0: iteration 0 stays committed, iterations 1 and 2
        // are outstanding undo, and the loop resumes at 1.
        let undo_op =
            |proc_idx: u32, iter: u32, entities_ran: Vec<Vec<u8>>| instance::CommittedProc {
                proc_idx,
                iter,
                entities_ran,
            };
        assert!(node.start_workflow_resumable(
            "test-loop-saga-keep".into(),
            id.into(),
            "ignored".into(),
            1,
            0,
            Some(instance::JournalResume {
                iter: 1,
                consumed_offset: 1,
                committed_procs: vec![undo_op(0, 0, vec![seg.clone()]), undo_op(1, 0, Vec::new())],
                rewind_undo: vec![
                    undo_op(0, 1, vec![seg.clone()]),
                    undo_op(1, 1, Vec::new()),
                    undo_op(0, 2, vec![seg.clone()]),
                    undo_op(1, 2, Vec::new()),
                ],
                ..Default::default()
            })
        ));
        let (inst, _slot) = node
            .find_workflow_entity(&seg)
            .expect("the loop saga's Entity is reachable by the restore handler");
        inst.note_restored();
        // The resumed turn fails, so the run reaches a terminal state (the fake
        // controller must stay in `drain` to ack the undo commits).
        assert!(inst.push_inbox(b"boom".to_vec(), 2));

        let (_outs, done) = drain(&rx, id);
        assert_eq!(done.0, "error", "the resumed turn's own failure terminates");
        assert_eq!(
            drain_comp_log(),
            vec![
                "undo-loop:0".to_string(),
                "undo-loop:0".to_string(),
                "undo-loop:1".to_string()
            ],
            "both rewound iterations are undone first (journal-rebuilt ⇒ 0 retained \
             outputs), and only then does the resumed turn run and roll itself back \
             (in-flight ⇒ 1 retained output)"
        );
        let undo: Vec<(u32, u32)> = drain_proc_commits()
            .iter()
            .filter(|c| c.compensating)
            .map(|c| (c.proc_idx, c.iter))
            .collect();
        assert_eq!(
            undo,
            vec![(1, 2), (0, 2), (1, 1), (0, 1)],
            "reverse iteration, reverse stage — and the hookless terminal proc still \
             journals its (empty) `compensated` record, which is what drains it out \
             of the next fold's outstanding work"
        );

        std::env::remove_var("FLEZHA_WORKFLOW_PARK_IDLE_MS");

        // --- workflow-entity-retry: a transient entity failure recovers in
        // place — the sibling entity is NOT re-run and nothing compensates ---
        arm_flaky(1, false);
        assert!(node.start_workflow(
            "test-retry-workflow".into(),
            "retry-ok".into(),
            "a,bb".into()
        ));
        let (mut outs, done) = drain(&rx, "retry-ok");
        assert_eq!(done.0, "ok", "the ladder absorbed the transient failure");
        outs.sort();
        assert_eq!(
            outs,
            vec![b"even:bb".to_vec(), b"odd:a".to_vec()],
            "each entity's output lands exactly once — a retried attempt's \
             staged output is discarded, not double-emitted"
        );
        assert_eq!(
            ODD_ATTEMPTS.load(Ordering::SeqCst),
            2,
            "the failing entity ran twice (fail, then retry)"
        );
        assert_eq!(
            EVEN_RUNS.load(Ordering::SeqCst),
            1,
            "the sibling entity stayed buffered at the barrier: no re-run"
        );
        assert!(
            drain_comp_log().is_empty(),
            "a recovered retry must not compensate"
        );

        // --- retry exhaustion fails the procedure exactly as before: the
        // original error surfaces and completed procedures compensate ---
        arm_flaky(99, false);
        assert!(node.start_workflow(
            "test-retry-workflow".into(),
            "retry-exhausted".into(),
            "a,bb".into()
        ));
        let (_o, done) = drain(&rx, "retry-exhausted");
        assert_eq!(done.0, "error");
        assert!(
            done.1.contains("flaky attempt 3"),
            "the last attempt's error is the terminal one: {}",
            done.1
        );
        assert_eq!(
            ODD_ATTEMPTS.load(Ordering::SeqCst),
            3,
            "the default ladder is 3 attempts"
        );
        assert_eq!(
            drain_comp_log(),
            vec!["undo-retry-reserve".to_string()],
            "exhaustion runs the existing saga rollback"
        );

        // --- a non-retryable error skips the ladder entirely ---
        arm_flaky(99, true);
        assert!(node.start_workflow(
            "test-retry-workflow".into(),
            "retry-permanent".into(),
            "a,bb".into()
        ));
        let (_o, done) = drain(&rx, "retry-permanent");
        assert_eq!(done.0, "error");
        assert_eq!(
            ODD_ATTEMPTS.load(Ordering::SeqCst),
            1,
            "ProcedureError::non_retryable ⇒ no retry"
        );
        assert_eq!(drain_comp_log(), vec!["undo-retry-reserve".to_string()]);

        // --- retry_attempts() = 0 restores the pre-change fail-fast ---
        arm_flaky(99, false);
        assert!(node.start_workflow(
            "test-retry-failfast-workflow".into(),
            "retry-failfast".into(),
            "a,bb".into()
        ));
        let (_o, done) = drain(&rx, "retry-failfast");
        assert_eq!(done.0, "error");
        assert_eq!(
            ODD_ATTEMPTS.load(Ordering::SeqCst),
            1,
            "attempts = 0 ⇒ the first error is terminal"
        );
        assert_eq!(drain_comp_log(), vec!["undo-retry-reserve".to_string()]);

        // --- an external stop interrupts a waiting backoff (30 s base: the run
        // can only end inside `drain`'s deadline by observing the cancel in the
        // wait). The stop rides a helper thread because this thread is the fake
        // controller — it must stay in `drain` to ack the chain's proc commits ---
        arm_flaky(99, false);
        assert!(node.start_workflow(
            "test-retry-slow-workflow".into(),
            "retry-stop".into(),
            "a,bb".into()
        ));
        let stop_target = node
            .get_workflow("retry-stop")
            .expect("live instance accepts cancel");
        let stopper = std::thread::spawn(move || {
            while ODD_ATTEMPTS.load(Ordering::SeqCst) == 0 {
                std::thread::sleep(Duration::from_millis(5));
            }
            stop_target.request_cancel();
        });
        let (_o, done) = drain(&rx, "retry-stop");
        stopper.join().expect("stopper thread");
        assert_eq!(done.0, "cancelled", "the stop wins over the pending retry");
        assert_eq!(
            ODD_ATTEMPTS.load(Ordering::SeqCst),
            1,
            "the interrupted attempt is not retried"
        );
        assert_eq!(
            drain_comp_log(),
            vec!["undo-retry-reserve".to_string()],
            "a cancelled ladder takes the normal abnormal-exit path"
        );

        // --- a stage step is an activation the SDK's workers run, and the
        // backoff between two attempts holds none of them: every entity of this
        // stage backs off at once, and there are twice as many entities as
        // workers, so a backoff that occupied its worker would leave the pool
        // starved and the core would answer with threads above the base set ---
        STEP_ATTEMPTS.lock().unwrap().clear();
        assert!(node.start_workflow(
            "test-step-probe-workflow".into(),
            "workflow-steps".into(),
            "go".into()
        ));
        let (outs, done) = drain(&rx, "workflow-steps");
        assert_eq!((done.0.as_str(), done.1.as_str()), ("ok", ""));
        assert_eq!(
            outs.len(),
            probe_keys().len(),
            "every entity emitted once, after its own retry"
        );
        let attempts = STEP_ATTEMPTS.lock().unwrap().clone();
        assert_eq!(
            attempts.len(),
            probe_keys().len() * 2,
            "one failed attempt and one success per entity"
        );
        let base = crate::operative::poll_pool::base_worker_count();
        for (key, thread) in &attempts {
            let worker = thread.strip_prefix("flz-poll-").unwrap_or_else(|| {
                panic!("entity {key} ran its step on {thread}, not on an SDK poll worker")
            });
            assert!(
                worker.parse::<usize>().expect("poll worker id") < base,
                "the pool grew past its {base} base workers to serve {key}: a backoff was \
                 holding a worker instead of being timed at the driver's barrier"
            );
        }

        // --- workflow-branching: A→(B,C) runs to DONE — both branches receive
        // what A released on the channel they consume, and the operative is `ok`
        // exactly when every leaf has committed ---
        let _ = drain_proc_commits();
        assert!(node.start_workflow("test-tree-workflow".into(), "tree-1".into(), "x".into()));
        let (mut tree_outs, done) = drain(&rx, "tree-1");
        assert_eq!(
            done.0, "ok",
            "both leaves committed ⇒ the operative is done"
        );
        tree_outs.sort();
        assert_eq!(
            tree_outs,
            vec![b"echo:x-1".to_vec(), b"x-1-2".to_vec()],
            "each branch consumed Stage1's `mid` batch and emitted on its own sink"
        );
        let committed: Vec<u32> = drain_proc_commits().iter().map(|c| c.proc_idx).collect();
        assert_eq!(
            committed,
            vec![0, 1, 2],
            "the root commits before its children, which commit independently"
        );

        // --- workflow-branching resume: the journal records A and B committed, so
        // only C re-runs — seeded from A's journaled edge, not from B's ---
        let _ = drain_proc_commits();
        let id = "tree-resume";
        let root_seg = crate::entity::flusher::encode_entity_identity(id, "workflow_stage1", "");
        // Channel ids follow declaration order: `in` = 0, `mid` = 1.
        assert!(node.start_workflow_resumable(
            "test-tree-workflow".into(),
            id.into(),
            "ignored-args".into(),
            1,
            0,
            Some(instance::JournalResume {
                committed_proc_idxs: vec![0, 1],
                proc_handoffs: vec![instance::EdgeHandoff {
                    proc_idx: 0,
                    handoff: vec![(1, postcard::to_allocvec(&"resumed-1".to_string()).unwrap())],
                }],
                committed_procs: vec![
                    instance::CommittedProc {
                        proc_idx: 0,
                        iter: 0,
                        entities_ran: vec![root_seg.clone()],
                    },
                    instance::CommittedProc {
                        proc_idx: 1,
                        iter: 0,
                        entities_ran: Vec::new(),
                    },
                ],
                ..Default::default()
            })
        ));
        let (inst, _slot) = node
            .find_workflow_entity(&root_seg)
            .expect("the tree's root Entity is reachable by the restore handler");
        inst.note_restored();
        let (outs, done) = drain(&rx, id);
        assert_eq!(done.0, "ok");
        assert_eq!(
            outs,
            vec![b"echo:resumed-1".to_vec()],
            "only the uncommitted branch re-ran, seeded from its parent's edge"
        );
        assert_eq!(
            drain_proc_commits()
                .iter()
                .map(|c| c.proc_idx)
                .collect::<Vec<_>>(),
            vec![2],
            "the committed root and its committed sibling did NOT re-run"
        );

        // --- workflow-branching compensation: a failure under one branch unwinds
        // BOTH branches before their shared parent (reverse topological) ---
        let _ = drain_comp_log();
        assert!(node.start_workflow("test-tree-saga".into(), "tree-saga".into(), "o1".into()));
        let (_o, done) = drain(&rx, "tree-saga");
        assert_eq!(done.0, "error");
        assert_eq!(
            drain_comp_log(),
            vec![
                "undo-archive:archived:reserved:o1".to_string(),
                "undo-charge:charged:reserved:o1".to_string(),
                "undo-reserve:reserved:o1".to_string(),
            ],
            "descendants before ancestors: the sibling leaf and the failing branch's \
             committed procedure both unwind before the root they share"
        );

        // --- workflow-durable-path-throughput: an unacked barrier is re-sent
        // under the same reference_id, and the controller journals it once ---
        std::env::set_var("FLEZHA_WORKFLOW_ACK_TIMEOUT_MS", "300");
        let _ = drain_proc_commits();
        let _ = drain_barrier_sends();

        // The first ProcCommit's ack is lost: the re-send continues the chain.
        lose_acks("resend-1", 1);
        assert!(node.start_workflow("test-resend-chain".into(), "resend-1".into(), "x".into()));
        let (outs, done) = drain(&rx, "resend-1");
        assert_eq!(
            (done.0.as_str(), done.1.as_str()),
            ("ok", ""),
            "a lost ack delays the chain, it does not end it"
        );
        assert_eq!(
            outs,
            vec![b"x-1-2".to_vec()],
            "the journaled outbox reaches the plugins once"
        );
        let sends = barrier_sends_of("resend-1");
        assert_eq!(sends.len(), 3, "stage 0 twice, stage 1 once");
        assert_eq!(
            sends[0], sends[1],
            "the re-send carries the same reference_id"
        );
        assert_ne!(sends[1], sends[2]);
        assert_eq!(
            drain_proc_commits()
                .iter()
                .map(|c| c.proc_idx)
                .collect::<Vec<_>>(),
            vec![0, 1],
            "each barrier is journaled once"
        );

        // No ack ever arrives: the bounded ladder fails the run.
        lose_acks("resend-2", usize::MAX);
        assert!(node.start_workflow("test-resend-chain".into(), "resend-2".into(), "y".into()));
        let (_outs, done) = drain(&rx, "resend-2");
        assert_eq!(done.0, "error");
        assert!(
            done.1.contains("ack timed out after 3 attempt(s)"),
            "the exhausted ladder is the terminal error: {}",
            done.1
        );
        let sends = barrier_sends_of("resend-2");
        assert_eq!(sends.len(), 3, "the default ladder sends three times");
        assert!(sends.iter().all(|r| r == &sends[0]));
        let _ = drain_proc_commits();

        // A re-sent IterCommit is idempotent on the controller: the turn's
        // barriers each lose their first ack, and each is journaled once.
        std::env::set_var("FLEZHA_WORKFLOW_PARK_IDLE_MS", "300");
        let _ = drain_iter_commits();
        let _ = drain_iter_parks();
        lose_acks("resend-3", 3);
        assert!(node.start_workflow(
            "test-resend-loop".into(),
            "resend-3".into(),
            "ignored".into()
        ));
        let resend_loop = node
            .get_workflow("resend-3")
            .expect("the loop's instance is registered before its driver runs");
        assert!(resend_loop.push_inbox(b"hi".to_vec(), 4));
        let (outs, done) = drain(&rx, "resend-3");
        assert_eq!(done.0, "parked");
        assert_eq!(outs, vec![b"hi-1-2".to_vec()]);
        assert_eq!(
            drain_iter_commits()
                .iter()
                .map(|c| (c.iter, c.consumed_offset))
                .collect::<Vec<_>>(),
            vec![(0, 4)],
            "one iter_committed for two sends"
        );
        assert_eq!(
            drain_proc_commits()
                .iter()
                .map(|c| (c.iter, c.proc_idx))
                .collect::<Vec<_>>(),
            vec![(0, 0), (0, 1)]
        );
        let sends = barrier_sends_of("resend-3");
        assert_eq!(sends.len(), 6, "every barrier of the turn was sent twice");
        assert!(
            sends.chunks(2).all(|pair| pair[0] == pair[1]),
            "each re-send carries its barrier's own reference_id"
        );
        std::env::remove_var("FLEZHA_WORKFLOW_ACK_TIMEOUT_MS");
    }

    /// Respawn `test-chain-workflow` as a crash-resume carrying `journal` on the
    /// bind (exactly what the AddOperative handler passes), release the restore
    /// gate, and drain it to completion.
    fn resume_chain_with_journal(
        node: &RunnerNode,
        rx: &Receiver<RunnerMessage>,
        id: &str,
        journal: instance::JournalResume,
    ) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
        assert!(node.start_workflow_resumable(
            "test-chain-workflow".into(),
            id.into(),
            "ignored-args".into(),
            1,
            0,
            Some(journal)
        ));
        let s1_id = crate::entity::flusher::encode_entity_identity(id, "workflow_stage1", "");
        let (inst, _slot) = node
            .find_workflow_entity(&s1_id)
            .expect("chain entity reachable by the restore handler");
        inst.note_restored();
        drain(rx, id)
    }

    /// Proc-commits observed by the fake controller (`workflow-proc-journal`
    /// barrier): every `ProcCommit` the drain helpers ack is recorded here
    /// so the gating/handoff assertions can inspect what would be journaled.
    static PROC_COMMITS: Mutex<Vec<crate::pb::ProcCommit>> = Mutex::new(Vec::new());

    /// Iteration commits observed by the fake controller (`workflow-durable-loop`
    /// boundary) — what the journal would record as `iter_committed`.
    static ITER_COMMITS: Mutex<Vec<crate::pb::IterCommit>> = Mutex::new(Vec::new());

    /// Park notifications observed by the fake controller
    /// (`workflow-durable-suspension`) — the instance evicted itself at a
    /// committed boundary.
    static ITER_PARKS: Mutex<Vec<crate::pb::IterPark>> = Mutex::new(Vec::new());

    /// Every barrier send the fake controller saw, `(operative_id,
    /// reference_id)`, re-sends included.
    static BARRIER_SENDS: Mutex<Vec<(String, String)>> = Mutex::new(Vec::new());

    /// Reference ids the fake controller has journaled: a re-send under one of
    /// them is acked from the existing entry, never recorded twice.
    static JOURNALED_REFS: Mutex<Vec<String>> = Mutex::new(Vec::new());

    /// Per operative, how many barriers lose their first ack on the way back
    /// (`usize::MAX` = every send of every barrier). A lost ack is the real
    /// controller's: the entry is journaled and its outbox flushed, only the
    /// reply never reaches the driver.
    static LOST_ACKS: Mutex<Vec<(String, usize)>> = Mutex::new(Vec::new());

    /// Every `EmitWorkflowOutput` the drain helpers saw — output that went
    /// STRAIGHT to the wire, bypassing any proc boundary. A durable structured
    /// operative must produce none of these (it stages); `run`-sugar and
    /// opted-out classes still do.
    static WIRE_EMITS: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

    fn drain_proc_commits() -> Vec<crate::pb::ProcCommit> {
        std::mem::take(&mut *PROC_COMMITS.lock().unwrap())
    }

    fn drain_iter_commits() -> Vec<crate::pb::IterCommit> {
        std::mem::take(&mut *ITER_COMMITS.lock().unwrap())
    }

    fn drain_iter_parks() -> Vec<crate::pb::IterPark> {
        std::mem::take(&mut *ITER_PARKS.lock().unwrap())
    }

    fn drain_barrier_sends() -> Vec<(String, String)> {
        std::mem::take(&mut *BARRIER_SENDS.lock().unwrap())
    }

    /// The reference ids of `operative_id`'s barrier sends, in order.
    fn barrier_sends_of(operative_id: &str) -> Vec<String> {
        BARRIER_SENDS
            .lock()
            .unwrap()
            .iter()
            .filter(|(id, _)| id == operative_id)
            .map(|(_, reference_id)| reference_id.clone())
            .collect()
    }

    fn lose_acks(operative_id: &str, count: usize) {
        LOST_ACKS
            .lock()
            .unwrap()
            .push((operative_id.to_string(), count));
    }

    /// Whether this send's ack is lost: a barrier's first send consumes one of
    /// the operative's losses, a re-send is acked — unless every ack is lost.
    fn ack_lost(operative_id: &str, first_send: bool) -> bool {
        let mut lost = LOST_ACKS.lock().unwrap();
        match lost.iter_mut().find(|(id, _)| id == operative_id) {
            Some((_, n)) if *n == usize::MAX => true,
            Some((_, n)) if *n > 0 && first_send => {
                *n -= 1;
                true
            }
            _ => false,
        }
    }

    /// The controller's barrier handling: a first send is journaled (`record`)
    /// and its outbox released, a re-send is acked from the existing entry —
    /// unless this operative's ack is lost on the way back.
    fn journal_barrier(
        operative_id: &str,
        reference_id: &str,
        flushed: Vec<Vec<u8>>,
        record: impl FnOnce(),
    ) -> Vec<Vec<u8>> {
        BARRIER_SENDS
            .lock()
            .unwrap()
            .push((operative_id.to_string(), reference_id.to_string()));
        let fresh = {
            let mut refs = JOURNALED_REFS.lock().unwrap();
            let fresh = !refs.iter().any(|r| r == reference_id);
            if fresh {
                refs.push(reference_id.to_string());
            }
            fresh
        };
        if fresh {
            record();
        }
        if !ack_lost(operative_id, fresh) {
            crate::shutdown::signal_result(reference_id, Ok(String::new()));
        }
        if fresh {
            flushed
        } else {
            Vec::new()
        }
    }

    fn wire_emits_for(id: &str) -> Vec<Vec<u8>> {
        WIRE_EMITS
            .lock()
            .unwrap()
            .iter()
            .filter(|(oid, _)| oid == id)
            .map(|(_, p)| p.clone())
            .collect()
    }

    /// Fake-controller half of every SDK-initiated control request: record the
    /// frame and, for the two durability handshakes, wake the blocked driver
    /// with an ack success. Returns the outbox the (real) controller would flush
    /// to the plugins AFTER that durable point — the drain helpers append it to
    /// the observed output, so a staged operative's assertions still describe
    /// what a plugin finally sees. A park is one-way: nothing to ack.
    fn ack_control_request(
        reference_id: String,
        command: crate::pb::command_request::Command,
    ) -> Vec<Vec<u8>> {
        use crate::pb::command_request::Command;
        match command {
            Command::ProcCommit(commit) => {
                let flushed: Vec<Vec<u8>> =
                    commit.outbox.iter().map(|o| o.payload.clone()).collect();
                let operative_id = commit.operative_id.clone();
                journal_barrier(&operative_id, &reference_id, flushed, || {
                    PROC_COMMITS.lock().unwrap().push(commit)
                })
            }
            Command::IterCommit(commit) => {
                let operative_id = commit.operative_id.clone();
                journal_barrier(&operative_id, &reference_id, Vec::new(), || {
                    ITER_COMMITS.lock().unwrap().push(commit)
                })
            }
            Command::IterPark(park) => {
                ITER_PARKS.lock().unwrap().push(park);
                Vec::new()
            }
            _ => Vec::new(),
        }
    }

    /// The operative id an SDK-initiated control request belongs to.
    fn control_request_operative(command: &crate::pb::command_request::Command) -> String {
        use crate::pb::command_request::Command;
        match command {
            Command::ProcCommit(c) => c.operative_id.clone(),
            Command::IterCommit(c) => c.operative_id.clone(),
            Command::IterPark(p) => p.operative_id.clone(),
            _ => String::new(),
        }
    }

    /// Drain output frames until the `WorkflowCompleted` for `id`, returning the
    /// collected outputs and the `(status, error, result)` completion tuple.
    /// "Output" is what reaches the plugins: a `run`-sugar / opted-out emit
    /// straight off the wire, plus a structured operative's staged outbox once
    /// its proc commits (the fake controller flushes it on ack).
    fn drain(rx: &Receiver<RunnerMessage>, id: &str) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut outs = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let msg = rx
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("timed out waiting for workflow {id}"));
            match msg {
                RunnerMessage::EmitWorkflowOutput {
                    operative_id,
                    payload,
                } if operative_id == id => {
                    WIRE_EMITS
                        .lock()
                        .unwrap()
                        .push((operative_id, payload.clone()));
                    outs.push(payload);
                }
                RunnerMessage::EmitWorkflowCompleted {
                    operative_id,
                    status,
                    error,
                    result,
                } if operative_id == id => return (outs, (status, error, result)),
                RunnerMessage::EmitControlRequest {
                    reference_id,
                    command,
                } => {
                    let for_id = control_request_operative(&command) == id;
                    // A park tears the run down WITHOUT a completion, so the
                    // drain would otherwise wait out its deadline: report it as
                    // the terminal outcome of this run instead.
                    let parked =
                        matches!(command, crate::pb::command_request::Command::IterPark(_));
                    let flushed = ack_control_request(reference_id, command);
                    if for_id {
                        outs.extend(flushed);
                        if parked {
                            return (outs, ("parked".to_string(), String::new(), Vec::new()));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Block until a specific output frame for `id` is observed (used to confirm
    /// an interactive instance is live before stopping it).
    fn wait_for_output(rx: &Receiver<RunnerMessage>, id: &str, payload: &[u8]) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let msg = rx
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("timed out waiting for output from {id}"));
            match msg {
                RunnerMessage::EmitControlRequest {
                    reference_id,
                    command,
                } => {
                    let for_id = control_request_operative(&command) == id;
                    let flushed = ack_control_request(reference_id, command);
                    if for_id && flushed.iter().any(|p| p == payload) {
                        return;
                    }
                }
                RunnerMessage::EmitWorkflowOutput {
                    operative_id,
                    payload: p,
                } => {
                    if operative_id == id && p == payload {
                        return;
                    }
                }
                _ => {}
            }
        }
    }
}
