//! The **bounded** compute-unit kind: an on-demand, terminating,
//! possibly-interactive process.
//!
//! A bounded operative is spawned with `args`, runs to an exit trigger, and is
//! then torn down. It reuses the unbounded **building blocks** (Operations,
//! Channels, Segments, per-Segment state, `ctx.emit`) but drives them under a
//! bounded **lifecycle**:
//!
//! - **`run` sugar** — the trivial case is a bare
//!   [`run`](BoundedOperative::run) body: a single implicit Operation/Segment.
//! - **Structured** — override [`operations`](BoundedOperative::operations) to
//!   author Operations/Channels/Segments on the same `ChannelBuilder` a
//!   unbounded operative uses; the bounded driver runs them **sequentially in
//!   a linear chain** with a per-operation **count-based barrier** (segments
//!   within one operation still run in parallel) — see [`driver`].
//! - **Exit triggers** — the operative terminates on the first of: the chain
//!   running to quiescence, [`BoundedCtx::complete`], a loop's terminal
//!   predicate, a declared [`max_duration`](BoundedOperative::max_duration), or
//!   external `flz operative stop` ([`BoundedStop`](crate::pb::BoundedStop)).
//!   Every trigger converges on the single `BoundedCompleted` teardown.
//! - **Inbox** — a running operative receives input **after it starts** via
//!   [`BoundedCtx::recv`] (today's drop-on-arrival is gone).
//!
//! ```ignore
//! struct Greet;
//! impl flezha_runner::BoundedOperative for Greet {
//!     fn factory_id() -> &'static str { "greet" }
//!     fn run(ctx: &mut flezha_runner::BoundedCtx, args: &str) -> Result<(), flezha_runner::BoundedError> {
//!         ctx.complete(format!("hello {args}").into_bytes());
//!         Ok(())
//!     }
//! }
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! flezha_runner::Flezha::create_runner("my_app")?.define_bounded::<Greet>()?.run()?;
//! # Ok(())
//! # }
//! ```

pub(crate) mod compensation;
pub(crate) mod driver;
pub mod instance;
pub mod registry;
pub(crate) mod spec;

use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crossbeam::channel::{Receiver, RecvTimeoutError};

use self::instance::InboxItem;
use crate::channel::ChannelBuilder;

/// Frame-metadata key carrying a journaled input's durable-inbox offset
/// (`bounded-durable-suspension`). Stamped by the controller when it appends
/// the input to the instance's `inbox.wal`, before forwarding it.
pub(crate) const INBOX_OFFSET_KEY: &str = "flz.inbox_offset";

/// How a structured bounded chain repeats (`bounded-durable-loop`). Every
/// iteration ends with a durable iteration commit — journal, boundary snapshot,
/// outbox flush — so a crash resumes at the iteration **after** the last
/// committed one, never at the loop start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoopMode {
    /// Not a loop: one pass of the chain, seeded from the spawn args. The
    /// default, and the only mode a `run`-sugar class may declare.
    Once,
    /// One iteration per inbox item (the chat-turn shape): each iteration is
    /// seeded by the next post-start input through the entry channel. A
    /// durable instance whose inbox stays empty past the idle threshold at a
    /// committed boundary **parks** — it is evicted from the runner and
    /// rehydrated by the next input or its wake deadline
    /// (`bounded-durable-suspension`).
    PerInput,
    /// One iteration per pass of the chain, each seeded by the previous
    /// iteration's terminal handoff (the refinement-loop shape). Iteration 0 is
    /// seeded from the spawn args; the loop ends when an iteration releases an
    /// empty batch. Never parks — it always has its own next input.
    Chained,
}

/// One bounded operative class. The bare [`run`](Self::run) form is the
/// trivial single-Operation, single-Segment case; override
/// [`operations`](Self::operations) to grow into an explicit DAG of
/// Operations/Channels/Segments without rewriting unrelated code. Capabilities
/// attach to the kind, not here — choosing the bounded kind is sufficient.
pub trait BoundedOperative: Send + Sync + 'static {
    /// Stable, controller-facing factory identifier.
    fn factory_id() -> &'static str;

    /// Run-sugar body: the single-Operation, single-Segment case. Invoked once
    /// per started instance with `args` from `init_values`. Emit output via
    /// `ctx`, optionally call [`BoundedCtx::complete`] with a result, and
    /// return `Ok(())`; returning `Err` is a terminal failure — the retry
    /// ladder ([`retry_attempts`](Self::retry_attempts)) scopes to a structured
    /// operation's segments, and a `run` body has none.
    ///
    /// The default errors — a class must implement either `run` or
    /// `operations`. A structured operative (non-empty `operations`) never has
    /// `run` invoked.
    fn run(_ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
        Err(BoundedError::from(
            "BoundedOperative: neither run() nor operations() was implemented",
        ))
    }

    /// Structured form: declare a static **DAG** of Operations/Channels/
    /// Segments on the same graph builder an unbounded operative uses. The
    /// default is empty, selecting the [`run`](Self::run)-sugar path. The
    /// bounded driver runs the declaration in topological order: an operation
    /// becomes runnable only after **every** producer feeding it has committed
    /// — fan-out branches interleave independently, and a fan-in node is
    /// dispatched once, with the accumulated batch of all its producers.
    /// Cycles are rejected at registration.
    fn operations(_c: &ChannelBuilder) {}

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

    /// Loop shape of a structured chain (`bounded-durable-loop`). The default
    /// [`LoopMode::Once`] is the single pass. A looping class commits every
    /// iteration durably — journal + boundary snapshot + outbox flush — so a
    /// crash resumes at the iteration after the last committed one, and
    /// [`RollbackMode::KeepProgress`] keeps every committed iteration. Declaring
    /// a loop on a `run`-sugar class (no `operations`) is rejected at
    /// registration: there is no chain to iterate.
    fn loop_mode() -> LoopMode {
        LoopMode::Once
    }

    /// Durable wake deadline for a [`LoopMode::PerInput`] loop: how long after
    /// parking the instance is rehydrated even with no input
    /// (`bounded-durable-suspension`). `None` (the default) parks until the next
    /// input arrives. The timer is journaled with the park record, so it
    /// survives a controller redeploy; it wakes the loop with an **empty** entry
    /// payload, which a class declaring one must accept as its tick.
    fn wake_after() -> Option<Duration> {
        None
    }

    /// Total attempts a **structured** operation's segment gets before its
    /// error fails the operation (`bounded-segment-retry`): the first run plus
    /// the retries. Default 3; `0` or `1` restores fail-fast. Only the failed
    /// segment re-runs — siblings that completed stay buffered at the barrier —
    /// and the retry always happens **before** the operation commits, so a
    /// committed operation is never retried.
    ///
    /// A retried segment re-runs its whole batch for that operation, under the
    /// same at-least-once idempotency contract crash-resume imposes: a failed
    /// attempt's state changes are already applied, so `Operation::run` must
    /// tolerate re-running over them. Classify a permanent failure with
    /// [`OperationError::non_retryable`](crate::operation::OperationError::non_retryable)
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

    /// Saga rollback scope, applied when the operative terminates abnormally
    /// after partial progress. Default [`RollbackMode::AllOrNothing`] undoes
    /// every completed operation in reverse, back to the start;
    /// [`RollbackMode::KeepProgress`] keeps each finished loop iteration and
    /// undoes only the in-flight one. The mode is irrelevant for a single-pass
    /// chain (where both behave all-or-nothing) and for an operative with no
    /// compensation hooks.
    fn rollback_mode() -> RollbackMode {
        RollbackMode::AllOrNothing
    }

    /// Class-level persistence toggle (durable by default, mirroring
    /// [`UnboundedOperative::PERSISTENCE_ENABLED`](crate::operative::UnboundedOperative::PERSISTENCE_ENABLED)).
    /// When `true`, a structured bounded operative's per-Segment Changes are
    /// written to the WAL as they apply, so a crash resumes from persisted
    /// state. When `false`, the per-Segment flush drains and discards Changes
    /// with no serialization or wire I/O — the operative is in-memory only
    /// (a crash re-runs from the top). The controller resolves this
    /// durable-by-default with a per-class operator opt-out.
    const PERSISTENCE_ENABLED: bool = true;
}

/// Saga rollback scope chosen per bounded operative (see
/// [`BoundedOperative::rollback_mode`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackMode {
    /// Any failure undoes **all** completed operations in reverse, back to the
    /// start (the classic saga). The default.
    AllOrNothing,
    /// Each finished loop iteration commits (is kept); a failure undoes only the
    /// in-flight iteration's completed operations. Within a single iteration —
    /// or a no-loop pass — rollback is all-or-nothing.
    KeepProgress,
}

/// Type-erased run-sugar entrypoint stored in the class spec. `T::run` coerces
/// to this `fn` pointer because it takes no `self`.
pub type BoundedRunFn = fn(&mut BoundedCtx, &str) -> Result<(), BoundedError>;

/// A bounded failure. Retryable by default: inside a structured operation it
/// first passes through the class's per-segment retry ladder
/// (`bounded-segment-retry`), and only a `non_retryable` error — or one that
/// exhausted the ladder — fails the operation.
#[derive(Debug, Clone)]
pub struct BoundedError {
    pub message: String,
    /// Skip the retry ladder: this failure can never succeed on a re-run.
    pub non_retryable: bool,
}

impl BoundedError {
    /// Classify a failure as permanent (validation error, 4xx-class response):
    /// the segment is not retried and the operation fails immediately. Mirrors
    /// [`OperationError::non_retryable`](crate::operation::OperationError::non_retryable),
    /// which an operation body inside a segment returns to the same effect.
    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: true,
        }
    }

    /// Build with an explicit classification (dispatcher seam: carries an
    /// operation's own `NonRetryable` marker through to the bounded ladder).
    pub(crate) fn new(message: impl Into<String>, non_retryable: bool) -> Self {
        Self {
            message: message.into(),
            non_retryable,
        }
    }
}

impl fmt::Display for BoundedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl From<String> for BoundedError {
    fn from(s: String) -> Self {
        BoundedError::new(s, false)
    }
}

impl From<&str> for BoundedError {
    fn from(s: &str) -> Self {
        BoundedError::new(s, false)
    }
}

/// Collecting sink driven by [`crate::segment::OperationDispatcher::run_collecting`]:
/// inter-operation output is **collected** for handoff to the next stage at the
/// barrier; plugin-bound output is **staged** (serialized) for the operation's
/// transactional outbox. The bounded driver implements this per running segment.
pub trait BoundedCollectSink {
    /// Collect an inter-operation output value for the next stage's input batch.
    fn collect(&mut self, value: Arc<dyn Any + Send + Sync>);

    /// Take an already-serialized plugin-bound output produced on `channel`.
    /// For a durable structured operative the bytes are **staged** until the
    /// operation's barrier and travel to the controller inside `OpCommit`, so
    /// nothing reaches a plugin before the op's journal entry is durable
    /// (`bounded-transactional-outbox`); an opted-out class forwards straight
    /// to the wire (at-least-once, as before).
    fn emit_plugin_bytes(&mut self, channel: &str, bytes: Vec<u8>);
}

/// Lifecycle-aware context handed to [`BoundedOperative::run`] (the run-sugar
/// path). `emit` writes a frame tagged with the instance id on a secondary
/// stream that the controller fans out to the unit's plugins; `recv` drains
/// the inbox (input delivered after start); `loop_until` repeats a body until a
/// terminal predicate or cancellation (duration / external stop); `complete`
/// sets the terminal result.
pub struct BoundedCtx {
    instance_id: Arc<str>,
    cancel: Arc<AtomicBool>,
    completed: bool,
    result: Vec<u8>,
    inbox: Receiver<InboxItem>,
}

impl BoundedCtx {
    pub(crate) fn new(
        instance_id: Arc<str>,
        cancel: Arc<AtomicBool>,
        inbox: Receiver<InboxItem>,
    ) -> Self {
        Self {
            instance_id,
            cancel,
            completed: false,
            result: Vec::new(),
            inbox,
        }
    }

    /// Emit one output payload to the unit's bound plugins. Best-effort and
    /// at-least-once, like operative output. Routed through the coordinator so
    /// the stream write stays single-threaded.
    ///
    /// This is the `run`-sugar path only: with no operations there is no
    /// barrier to stage against and no journal entry to gate a flush on, so the
    /// emit goes straight to the wire. A structured operation's `ctx.emit`
    /// instead stages into the operation's transactional outbox
    /// (`bounded-transactional-outbox`).
    pub fn emit(&mut self, bytes: impl Into<Vec<u8>>) -> Result<(), BoundedError> {
        crate::runner::emit_bounded_output(self.instance_id.to_string(), bytes.into());
        Ok(())
    }

    /// Request terminal completion with `result`. The body should return
    /// shortly after; the driver tears down with status `ok` and `result` in
    /// the `BoundedCompleted` payload. Loops observe completion automatically.
    pub fn complete(&mut self, result: impl Into<Vec<u8>>) {
        self.result = result.into();
        self.completed = true;
    }

    /// True once an external stop / `max_duration` has tripped the
    /// cancellation token. Long-running bodies should check this and unwind.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::Acquire)
    }

    /// Block for the next inbox item (input delivered after start). Returns
    /// `None` when cancellation trips or the inbox is permanently closed.
    /// Sleeps while idle (≤200 ms wake granularity to honour cancellation) —
    /// never busy-spins.
    pub fn recv(&mut self) -> Option<Vec<u8>> {
        loop {
            if self.is_cancelled() || self.completed {
                return None;
            }
            match self.inbox.recv_timeout(Duration::from_millis(200)) {
                Ok(item) => return Some(item.payload),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return None,
            }
        }
    }

    /// Repeat `body` until `predicate` returns `true` OR the cancellation
    /// token trips (duration / external stop) OR [`complete`](Self::complete)
    /// is called. The construct observes cancellation automatically and blocks
    /// while idle (a body that waits on [`recv`](Self::recv) sleeps rather than
    /// spinning). `predicate` provides the required non-timeout exit, so a loop
    /// authored through this construct can always terminate without an external
    /// kill.
    pub fn loop_until<P, B>(&mut self, mut predicate: P, mut body: B) -> Result<(), BoundedError>
    where
        P: FnMut() -> bool,
        B: FnMut(&mut BoundedCtx) -> Result<(), BoundedError>,
    {
        while !self.is_cancelled() && !self.completed && !predicate() {
            body(self)?;
        }
        Ok(())
    }

    /// Consume the context, returning whether `complete` was called and the
    /// result payload. Driver-only.
    pub(crate) fn into_result(self) -> (bool, Vec<u8>) {
        (self.completed, self.result)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use crossbeam::channel::Receiver;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::command::CommandAction;
    use crate::operation::change::{Change, NoChange};
    use crate::operation::clock::WallClock;
    use crate::operation::{Operation, OperationCtx, OperationError, StateHandle};
    use crate::operative::registry::{Operative, OperativeRegistry};
    use crate::pb::{AddOperative, BoundedStop};
    use crate::runner::fast::RunnerNode;
    use crate::runner::{install_reconnect_trigger, RunnerMessage};
    use crate::segment::restore::RestoreInitPayload;

    static EMIT_RUNS: AtomicUsize = AtomicUsize::new(0);
    static FAIL_RUNS: AtomicUsize = AtomicUsize::new(0);

    // ---- run-sugar operatives ------------------------------------------------

    /// Run-sugar that echoes its args as plugin output (counts runs).
    struct EmitBounded;
    impl BoundedOperative for EmitBounded {
        fn factory_id() -> &'static str {
            "test-emit-bounded"
        }
        fn run(ctx: &mut BoundedCtx, args: &str) -> Result<(), BoundedError> {
            EMIT_RUNS.fetch_add(1, Ordering::SeqCst);
            ctx.emit(args.as_bytes().to_vec())?;
            Ok(())
        }
    }

    /// Run-sugar that always fails (counting its runs: a `run` body has no
    /// segments to scope a retry to, so it must run exactly once).
    struct FailBounded;
    impl BoundedOperative for FailBounded {
        fn factory_id() -> &'static str {
            "test-fail-bounded"
        }
        fn run(_ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
            FAIL_RUNS.fetch_add(1, Ordering::SeqCst);
            Err(BoundedError::from("boom"))
        }
    }

    struct PanicBounded;
    impl BoundedOperative for PanicBounded {
        fn factory_id() -> &'static str {
            "test-panic-bounded"
        }
        fn run(_ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
            panic!("kaboom")
        }
    }

    /// Run-sugar that returns a terminal result via `ctx.complete`.
    struct CompleteBounded;
    impl BoundedOperative for CompleteBounded {
        fn factory_id() -> &'static str {
            "test-complete-bounded"
        }
        fn run(ctx: &mut BoundedCtx, args: &str) -> Result<(), BoundedError> {
            ctx.complete(format!("result:{args}").into_bytes());
            Ok(())
        }
    }

    /// Interactive run-sugar: loops on the inbox, echoes `got:<msg>`, exits the
    /// loop on a `quit` message (terminal predicate) or on cancellation.
    struct LoopBounded;
    impl BoundedOperative for LoopBounded {
        fn factory_id() -> &'static str {
            "test-loop-bounded"
        }
        fn run(ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
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
    struct TimedBounded;
    impl BoundedOperative for TimedBounded {
        fn factory_id() -> &'static str {
            "test-timed-bounded"
        }
        fn max_duration() -> Option<Duration> {
            Some(Duration::from_millis(200))
        }
        fn run(ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
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
    struct ExternalStopOnlyBounded;
    impl BoundedOperative for ExternalStopOnlyBounded {
        fn factory_id() -> &'static str {
            "test-external-stop-only"
        }
        fn self_terminating() -> bool {
            false
        }
        fn run(_ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
            Ok(())
        }
    }

    // ---- structured operations + operatives ----------------------------------

    #[derive(Default)]
    struct EchoOp;
    impl Operation for EchoOp {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "bounded_echo_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("echo:{input}"))?;
            Ok(())
        }
    }

    /// Single-Operation structured form, equivalent to `EmitBounded`-style sugar.
    struct EchoStructured;
    impl BoundedOperative for EchoStructured {
        fn factory_id() -> &'static str {
            "test-echo-structured"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EchoOp>()
                .named("out")
                .output("sink");
        }
    }

    #[derive(Default)]
    struct Stage1;
    impl Operation for Stage1 {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "bounded_stage1"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("{input}-1"))?;
            Ok(())
        }
    }

    #[derive(Default)]
    struct Stage2;
    impl Operation for Stage2 {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "bounded_stage2"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("{input}-2"))?;
            Ok(())
        }
    }

    /// `bounded-branching`: Stage1 fans out to Stage2 and EchoOp on one channel,
    /// each branch ending at its own plugin sink. The edge is (channel,
    /// consumer), so both branches read exactly what Stage1 released on `mid`.
    fn tree_chain(c: &ChannelBuilder) {
        let mid = c
            .input::<String>("in")
            .into::<Stage1>()
            .named("mid")
            .codec();
        mid.into::<Stage2>().named("out_b").output("sink");
        mid.into::<EchoOp>().named("out_c").output("sink");
    }

    struct TreeBounded;
    impl BoundedOperative for TreeBounded {
        fn factory_id() -> &'static str {
            "test-tree-bounded"
        }
        fn operations(c: &ChannelBuilder) {
            tree_chain(c);
        }
    }

    /// Two-stage linear chain: Stage1 → Stage2 → plugin.
    struct ChainBounded;
    impl BoundedOperative for ChainBounded {
        fn factory_id() -> &'static str {
            "test-chain-bounded"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .codec()
                .into::<Stage2>()
                .named("out")
                .output("sink");
        }
    }

    /// The same chain, driven as a chat-style inbox loop: one iteration per
    /// post-start input, parking when the inbox goes quiet
    /// (`bounded-durable-loop` + `bounded-durable-suspension`).
    struct ChatLoop;
    impl BoundedOperative for ChatLoop {
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
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .codec()
                .into::<Stage2>()
                .named("out")
                .output("sink");
        }
    }

    /// Compensable stage-0 op of the loop saga. Its undo logs the number of
    /// retained outputs it was handed, which is how the test tells the two
    /// rollback sources apart: the in-flight iteration carries its outputs in
    /// memory (`1`), while an iteration rebuilt from the journal carries none
    /// (`0`) — the documented at-least-once contract, where a durable undo
    /// handle lives in Segment state.
    #[derive(Default)]
    struct LoopReserve;
    impl Operation for LoopReserve {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("reserved:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            comp_log(format!("undo-loop:{}", completed_output.len()));
            Ok(())
        }
    }

    /// Terminal op of the loop saga: fails the iteration whose turn says "boom".
    #[derive(Default)]
    struct LoopMaybeFail;
    impl Operation for LoopMaybeFail {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            if input.contains("boom") {
                return Err(OperationError::Custom("turn failed".into()));
            }
            ctx.emit(format!("done:{input}"))?;
            Ok(())
        }
    }

    /// Inbox-driven saga loop, all-or-nothing: a failing turn unwinds every
    /// committed turn too.
    struct LoopSagaAllOrNothing;
    impl BoundedOperative for LoopSagaAllOrNothing {
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
        fn operations(c: &ChannelBuilder) {
            loop_saga_chain(c);
        }
    }

    /// The same loop, keep-progress: a failing turn unwinds only itself.
    struct LoopSagaKeepProgress;
    impl BoundedOperative for LoopSagaKeepProgress {
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
        fn operations(c: &ChannelBuilder) {
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
            .output("sink");
    }

    /// A loop declared on a `run`-sugar class: no chain to iterate.
    struct LoopWithoutChain;
    impl BoundedOperative for LoopWithoutChain {
        fn factory_id() -> &'static str {
            "test-loop-no-chain"
        }
        fn loop_mode() -> LoopMode {
            LoopMode::PerInput
        }
        fn run(_ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
            Ok(())
        }
    }

    /// A durable handoff-chained loop whose terminal operation emits to a plugin
    /// (no codec), so iteration N+1 could not be re-seeded after a crash.
    struct ChainedLoopNoCodec;
    impl BoundedOperative for ChainedLoopNoCodec {
        fn factory_id() -> &'static str {
            "test-chained-loop-no-codec"
        }
        fn loop_mode() -> LoopMode {
            LoopMode::Chained
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .codec()
                .into::<Stage2>()
                .named("out")
                .output("sink");
        }
    }

    /// Splits the comma-separated args into individual items (one segment).
    #[derive(Default)]
    struct SplitOp;
    impl Operation for SplitOp {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "bounded_split_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
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

    /// Static-segmented operation: routes items to `even`/`odd` segments by
    /// length parity, counts per-segment (independent state), emits `<n>:<item>`.
    #[derive(Default)]
    struct FanOp;
    impl Operation for FanOp {
        type Input = String;
        type Output = String;
        type State = CountState;
        type Change = Inc;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "bounded_fan_op"
        }
        fn operation_keys() -> Vec<&'static str> {
            vec!["even", "odd"]
        }
        const SEGMENT_FOR_OVERRIDDEN: bool = true;
        fn segment_for(input: &String) -> Vec<&'static str> {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            state.apply(Inc);
            let n = state.n;
            ctx.emit(format!("{n}:{input}"))?;
            Ok(())
        }
    }

    /// Split → fan-out across static segments → plugin.
    struct FanBounded;
    impl BoundedOperative for FanBounded {
        fn factory_id() -> &'static str {
            "test-fan-bounded"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SplitOp>()
                .named("mid")
                .codec()
                .into::<FanOp>()
                .named("out")
                .output("sink");
        }
    }

    /// Opt-out twin of `FanBounded`: identical topology, persistence disabled —
    /// its Changes drain-and-discard instead of reaching the WAL.
    struct FanOptOutBounded;
    impl BoundedOperative for FanOptOutBounded {
        fn factory_id() -> &'static str {
            "test-fan-optout-bounded"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SplitOp>()
                .named("mid")
                .into::<FanOp>()
                .named("out")
                .output("sink");
        }
        const PERSISTENCE_ENABLED: bool = false;
    }

    #[derive(Default)]
    struct HybridStage1;
    impl Operation for HybridStage1 {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "bounded_hybrid_stage1"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("h:{input}"))?;
            Ok(())
        }
    }

    #[derive(Default)]
    struct HybridStage2;
    impl Operation for HybridStage2 {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "bounded_hybrid_stage2"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("final:{input}"))?;
            Ok(())
        }
    }

    /// Hybrid inter-operation channel (`also_output_with`): Stage1's output
    /// both feeds Stage2 AND is serialized to a plugin sink.
    struct HybridBounded;
    impl BoundedOperative for HybridBounded {
        fn factory_id() -> &'static str {
            "test-hybrid-bounded"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<HybridStage1>()
                .named("mid")
                .codec()
                .also_output_with("midsink", |s: &String| format!("mid:{s}").into_bytes())
                .into::<HybridStage2>()
                .named("out")
                .output("out");
        }
    }

    // ---- saga compensation operatives (Phase 2 §4) --------------------------

    /// Process-global compensation observation log. The driver-observation test
    /// owns it (like `EMIT_RUNS`); compensable operations append a marker so
    /// rollback order and per-segment firing are assertable without wire
    /// plumbing (a compensable inter-operation op's `ctx.emit` is collected, not
    /// wired).
    static COMPENSATION_LOG: Mutex<Vec<String>> = Mutex::new(Vec::new());

    // Captures the `(payload_id, frame)` pairs a durable bounded ships to the
    // reliable sink — `segment_state_change` from the flush plus the saga
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
    impl Operation for SagaReserve {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("reserved:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            for out in &completed_output {
                comp_log(format!("undo-reserve:{out}"));
            }
            Ok(())
        }
    }

    /// Compensable: charges (emits `charged:<input>`); undo logs `undo-charge`.
    #[derive(Default)]
    struct SagaCharge;
    impl Operation for SagaCharge {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("charged:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
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
    impl Operation for SagaShipFails {
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
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            Err(OperationError::Custom("ship failed".into()))
        }
    }

    /// Reserve → Charge → Ship(fails). Default all-or-nothing rollback.
    /// Compensable leaf of the fan-out branch: archives (emits `archived:<input>`);
    /// undo logs `undo-archive`.
    #[derive(Default)]
    struct SagaArchive;
    impl Operation for SagaArchive {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("archived:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            for out in &completed_output {
                comp_log(format!("undo-archive:{out}"));
            }
            Ok(())
        }
    }

    /// `bounded-branching` saga: reserve fans out to (charge → ship-fails) and
    /// archive. The failure sits under one branch, so the abort must unwind BOTH
    /// branches and only then their shared parent.
    struct TreeSaga;
    impl BoundedOperative for TreeSaga {
        fn factory_id() -> &'static str {
            "test-tree-saga"
        }
        fn operations(c: &ChannelBuilder) {
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
                .output("sink");
            // The archive leaf releases an inter-op batch nobody consumes: it
            // stays a leaf, and its retained output is what its undo reverses
            // (a plugin-bound leaf would stage instead of collect, leaving the
            // hook nothing to name).
            t1.into::<SagaArchive>().named("t_out").codec();
        }
    }

    struct SagaBounded;
    impl BoundedOperative for SagaBounded {
        fn factory_id() -> &'static str {
            "test-saga-bounded"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SagaReserve>()
                .named("c1")
                .codec()
                .into::<SagaCharge>()
                .named("c2")
                .codec()
                .into::<SagaShipFails>()
                .named("out")
                .output("sink");
        }
    }

    /// Same chain, keep-progress mode declared — for a single no-loop pass it
    /// unwinds identically (the mode only diverges with committed iterations,
    /// covered by the planner unit tests). Proves `rollback_mode()` is plumbed
    /// through the registration → driver path.
    struct KeepProgressSaga;
    impl BoundedOperative for KeepProgressSaga {
        fn factory_id() -> &'static str {
            "test-keep-progress-saga"
        }
        fn rollback_mode() -> RollbackMode {
            RollbackMode::KeepProgress
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SagaReserve>()
                .named("c1")
                .codec()
                .into::<SagaCharge>()
                .named("c2")
                .codec()
                .into::<SagaShipFails>()
                .named("out")
                .output("sink");
        }
    }

    /// Reserve → Charge → Echo(succeeds): a clean `ok` MUST NOT compensate.
    struct CleanSaga;
    impl BoundedOperative for CleanSaga {
        fn factory_id() -> &'static str {
            "test-clean-saga"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SagaReserve>()
                .named("c1")
                .codec()
                .into::<SagaCharge>()
                .named("c2")
                .codec()
                .into::<EchoOp>()
                .named("out")
                .output("sink");
        }
    }

    /// Static-segmented compensable op: stores its segment key in state, emits
    /// `<key>:<item>`, and compensates ONCE per segment (logs all that segment's
    /// outputs together) so per-segment firing is assertable.
    #[derive(Serialize, Deserialize, Clone, Default)]
    struct FanKeyState {
        key: String,
    }
    #[derive(Default)]
    struct SagaFan;
    impl Operation for SagaFan {
        type Input = String;
        type Output = String;
        type State = FanKeyState;
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "saga_fan"
        }
        fn operation_keys() -> Vec<&'static str> {
            vec!["even", "odd"]
        }
        const SEGMENT_FOR_OVERRIDDEN: bool = true;
        fn segment_for(input: &String) -> Vec<&'static str> {
            if input.len().is_multiple_of(2) {
                vec!["even"]
            } else {
                vec!["odd"]
            }
        }
        fn init_state(
            operation_key: Option<&str>,
            _: Option<RestoreInitPayload<'_>>,
        ) -> FanKeyState {
            FanKeyState {
                key: operation_key.unwrap_or("").to_string(),
            }
        }
        fn run(
            &mut self,
            input: String,
            state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("{}:{input}", state.key))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            state: StateHandle<'_, Self>,
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            let items: Vec<String> = completed_output.iter().map(|o| o.to_string()).collect();
            comp_log(format!("undo-fan:{}:[{}]", state.key, items.join(",")));
            Ok(())
        }
    }

    /// Terminal failing op for the per-segment rollback chain.
    #[derive(Default)]
    struct FailLast;
    impl Operation for FailLast {
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
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            Err(OperationError::Custom("fan ship failed".into()))
        }
    }

    /// Split(hookless) → Fan(compensable, even/odd) → FailLast. The hookless
    /// `SplitOp` is skipped during rollback; only `SagaFan` compensates, once
    /// per segment.
    struct FanSaga;
    impl BoundedOperative for FanSaga {
        fn factory_id() -> &'static str {
            "test-fan-saga"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<SplitOp>()
                .named("mid")
                .codec()
                .into::<SagaFan>()
                .named("mid2")
                .codec()
                .into::<FailLast>()
                .named("out")
                .output("sink");
        }
    }

    /// Compensable stage-0 op with a handshake so a cooperative stop lands after
    /// it completes but before stage 1 (deterministic forced-stop rollback).
    #[derive(Default)]
    struct StopReserve;
    impl Operation for StopReserve {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
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
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            for out in &completed_output {
                comp_log(format!("undo-stop-reserve:{out}"));
            }
            Ok(())
        }
    }

    /// StopReserve → Echo. A cooperative stop after StopReserve completes
    /// compensates only StopReserve; Echo never runs.
    struct StopSaga;
    impl BoundedOperative for StopSaga {
        fn factory_id() -> &'static str {
            "test-stop-saga"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<StopReserve>()
                .named("mid")
                .codec()
                .into::<EchoOp>()
                .named("out")
                .output("sink");
        }
    }

    // ---- saga crash-resume ops (§4.3) ---------------------------------------
    // Compensable ops whose `compensate` logs UNCONDITIONALLY (not per output),
    // so a crash-resume — which reconstructs completed stages with EMPTY outputs
    // (an operation's collected output was never durable) — is still observable.

    #[derive(Default)]
    struct ResumeUndoA;
    impl Operation for ResumeUndoA {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("a:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            _out: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            comp_log("undo-A".to_string());
            Ok(())
        }
    }

    #[derive(Default)]
    struct ResumeUndoB;
    impl Operation for ResumeUndoB {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("b:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            _out: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            comp_log("undo-B".to_string());
            Ok(())
        }
    }

    // ---- staged-commit / transactional-outbox ops --------------------------

    /// Emits plugin-bound output and THEN fails: the operation never commits,
    /// so its staged output must never reach a plugin (nothing leaked).
    #[derive(Default)]
    struct EmitThenFail;
    impl Operation for EmitThenFail {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "bounded_emit_then_fail"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("leak:{input}"))?;
            Err(OperationError::Custom("failed after emitting".into()))
        }
    }

    /// One operation that emits then fails, straight to a plugin sink.
    struct LeakBounded;
    impl BoundedOperative for LeakBounded {
        fn factory_id() -> &'static str {
            "test-leak-bounded"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EmitThenFail>()
                .named("out")
                .output("sink");
        }
    }

    /// Compensable op on a hybrid channel: its forward output feeds the next
    /// operation AND a plugin sink, so its `compensate` hook's `ctx.emit`
    /// produces plugin-bound undo output (staged for the `compensated` entry).
    #[derive(Default)]
    struct UndoEmit;
    impl Operation for UndoEmit {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "bounded_undo_emit"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            input: String,
            _state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(format!("did:{input}"))?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            for out in &completed_output {
                ctx.emit(format!("undo:{out}"))?;
            }
            Ok(())
        }
    }

    /// UndoEmit(hybrid, compensable) → FailLast. The failure rolls UndoEmit
    /// back; its undo output is staged and journaled in a `compensated` entry.
    struct UndoSaga;
    impl BoundedOperative for UndoSaga {
        fn factory_id() -> &'static str {
            "test-undo-saga"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<UndoEmit>()
                .named("mid")
                .codec()
                .also_output_with("undosink", |s: &String| s.clone().into_bytes())
                .into::<FailLast>()
                .named("out")
                .output("sink");
        }
    }

    /// A → B → ship(fails). Durable-by-default; both A and B are compensable.
    struct ResumeSaga;
    impl BoundedOperative for ResumeSaga {
        fn factory_id() -> &'static str {
            "test-resume-saga"
        }
        fn operations(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<ResumeUndoA>()
                .named("c1")
                .codec()
                .into::<ResumeUndoB>()
                .named("c2")
                .codec()
                .into::<SagaShipFails>()
                .named("out")
                .output("sink");
        }
    }

    // ---- per-segment retry ladder ops (`bounded-segment-retry`) -------------

    /// Attempts the `odd` segment has made, and items the `even` sibling has
    /// processed — a retry must re-run ONLY the failing segment.
    static ODD_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
    static EVEN_RUNS: AtomicUsize = AtomicUsize::new(0);
    /// How many of the `odd` segment's attempts fail, and how they are
    /// classified — set per scenario.
    static ODD_FAILURES: AtomicUsize = AtomicUsize::new(0);
    static ODD_NON_RETRYABLE: AtomicBool = AtomicBool::new(false);

    /// Compensable pass-through in front of the flaky stage: its undo proves
    /// whether a segment failure escalated to the operation (compensation) or
    /// was absorbed by the ladder.
    #[derive(Default)]
    struct RetryReserve;
    impl Operation for RetryReserve {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            ctx.emit(input)?;
            Ok(())
        }
        const COMPENSATE_OVERRIDDEN: bool = true;
        fn compensate(
            &mut self,
            _completed_output: Vec<Arc<String>>,
            _state: StateHandle<'_, Self>,
            _ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            comp_log("undo-retry-reserve".to_string());
            Ok(())
        }
    }

    /// Static-segmented flaky stage: the `odd` segment fails its first
    /// `ODD_FAILURES` attempts (retryably, or `NonRetryable` when the scenario
    /// says so) and then succeeds; the `even` segment always succeeds and
    /// counts the items it saw.
    #[derive(Default)]
    struct FlakyFan;
    impl Operation for FlakyFan {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "flaky_fan"
        }
        fn operation_keys() -> Vec<&'static str> {
            vec!["even", "odd"]
        }
        const SEGMENT_FOR_OVERRIDDEN: bool = true;
        fn segment_for(input: &String) -> Vec<&'static str> {
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
            ctx: &mut OperationCtx<'_>,
        ) -> Result<(), OperationError> {
            if input.len().is_multiple_of(2) {
                EVEN_RUNS.fetch_add(1, Ordering::SeqCst);
                ctx.emit(format!("even:{input}"))?;
                return Ok(());
            }
            let attempt = ODD_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= ODD_FAILURES.load(Ordering::SeqCst) {
                let message = format!("flaky attempt {attempt}");
                return Err(if ODD_NON_RETRYABLE.load(Ordering::SeqCst) {
                    OperationError::non_retryable(message)
                } else {
                    OperationError::Custom(message)
                });
            }
            ctx.emit(format!("odd:{input}"))?;
            Ok(())
        }
    }

    fn retry_chain(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SplitOp>()
            .named("mid")
            .codec()
            .into::<RetryReserve>()
            .named("mid2")
            .codec()
            .into::<FlakyFan>()
            .named("out")
            .output("sink");
    }

    /// Split → Reserve(compensable) → FlakyFan(even/odd). Default 3-attempt
    /// ladder, backoff shrunk so the scenarios do not sleep out a real one.
    struct RetryBounded;
    impl BoundedOperative for RetryBounded {
        fn factory_id() -> &'static str {
            "test-retry-bounded"
        }
        fn retry_backoff() -> Duration {
            Duration::from_millis(10)
        }
        fn operations(c: &ChannelBuilder) {
            retry_chain(c);
        }
    }

    /// The same chain with the ladder opted out: the first segment error fails
    /// the operation, exactly as before this capability existed.
    struct RetryFailFastBounded;
    impl BoundedOperative for RetryFailFastBounded {
        fn factory_id() -> &'static str {
            "test-retry-failfast-bounded"
        }
        fn retry_attempts() -> u32 {
            0
        }
        fn operations(c: &ChannelBuilder) {
            retry_chain(c);
        }
    }

    /// The same chain with a backoff long enough that a stop must interrupt the
    /// wait rather than outlast it.
    struct RetrySlowBackoffBounded;
    impl BoundedOperative for RetrySlowBackoffBounded {
        fn factory_id() -> &'static str {
            "test-retry-slow-bounded"
        }
        fn retry_backoff() -> Duration {
            Duration::from_secs(30)
        }
        fn operations(c: &ChannelBuilder) {
            retry_chain(c);
        }
    }

    /// Arm the flaky stage: `failures` attempts of the `odd` segment fail,
    /// classified retryable or not, with both attempt counters reset.
    fn arm_flaky(failures: usize, non_retryable: bool) {
        ODD_ATTEMPTS.store(0, Ordering::SeqCst);
        EVEN_RUNS.store(0, Ordering::SeqCst);
        ODD_FAILURES.store(failures, Ordering::SeqCst);
        ODD_NON_RETRYABLE.store(non_retryable, Ordering::SeqCst);
        let _ = drain_comp_log();
    }

    // ---- fan-in / diamond ops (`bounded-branching` fan-in accumulates) ------

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
            impl Operation for $ty {
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
                    ctx: &mut OperationCtx<'_>,
                ) -> Result<(), OperationError> {
                    diamond_log(format!("{}:{input}", $tag));
                    ctx.emit(format!("{}:{input}", $tag))?;
                    Ok(())
                }
                const COMPENSATE_OVERRIDDEN: bool = true;
                fn compensate(
                    &mut self,
                    completed_output: Vec<Arc<String>>,
                    _state: StateHandle<'_, Self>,
                    _ctx: &mut OperationCtx<'_>,
                ) -> Result<(), OperationError> {
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
    diamond_op!(DiamondJoin, "bounded_join_op", "j");

    /// The diamond A→(B,C)→D: both branch outputs accumulate into the one
    /// join node (two unnamed mentions unify), which closes plugin-bound.
    struct DiamondBounded;
    impl BoundedOperative for DiamondBounded {
        fn factory_id() -> &'static str {
            "test-diamond-bounded"
        }
        fn operations(c: &ChannelBuilder) {
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
                .output("sink");
        }
    }

    /// Kahn-shape twin used by the registration test (no driver run).
    struct JoinBounded;
    impl BoundedOperative for JoinBounded {
        fn factory_id() -> &'static str {
            "test-join-bounded-shape"
        }
        fn operations(c: &ChannelBuilder) {
            let mid = c
                .input::<String>("in")
                .into::<Stage1>()
                .named("mid")
                .codec();
            mid.into::<Stage2>()
                .named("join_b")
                .codec()
                .into::<DiamondJoin>();
            mid.into::<EchoOp>()
                .named("join_c")
                .codec()
                .into::<DiamondJoin>();
        }
    }

    /// Diamond whose join feeds a failing tail: A→(B,C)→D→ship(fails). The
    /// abort must undo the join BEFORE either producer, and the shared root
    /// last (reverse-topological compensation over a DAG).
    struct DiamondSaga;
    impl BoundedOperative for DiamondSaga {
        fn factory_id() -> &'static str {
            "test-diamond-saga"
        }
        fn operations(c: &ChannelBuilder) {
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
            join.into::<SagaShipFails>().named("d_out").output("sink");
        }
    }

    // ---- non-driver unit tests ----------------------------------------------

    #[test]
    fn bounded_error_conversions() {
        // String conversions stay retryable-by-default: the ladder is opt-out.
        let from_str: BoundedError = "x".into();
        assert_eq!(from_str.to_string(), "x");
        assert!(!from_str.non_retryable);
        let from_string: BoundedError = String::from("y").into();
        assert_eq!(from_string.message, "y");
        assert!(!from_string.non_retryable);

        let permanent = BoundedError::non_retryable("z");
        assert_eq!(permanent.to_string(), "z");
        assert!(permanent.non_retryable);
    }

    #[test]
    fn register_bounded_makes_factory_resolvable() {
        struct RegBounded;
        impl BoundedOperative for RegBounded {
            fn factory_id() -> &'static str {
                "test-getclass-bounded"
            }
            fn run(_c: &mut BoundedCtx, _a: &str) -> Result<(), BoundedError> {
                Ok(())
            }
        }
        assert!(registry::get_class("test-getclass-bounded").is_none());
        crate::register_bounded_class::<RegBounded>().unwrap();
        assert!(registry::get_class("test-getclass-bounded").is_some());
        assert!(registry::get_class("test-nonexistent-bounded").is_none());
    }

    #[test]
    fn registry_holds_bounded_as_sealed_variant() {
        let reg = OperativeRegistry::new();
        let (instance, _inbox_rx) = crate::bounded::instance::BoundedInstance::new(
            Arc::from("t1"),
            Arc::from("t1-factory"),
            Vec::new(),
            0,
            None,
        );
        reg.register_bounded(instance);

        assert!(reg.contains("t1"));
        // A bounded is not a operative: the operative-only accessor returns None.
        assert!(reg.get_unbounded("t1").is_none());
        match reg.get_operative("t1").as_deref() {
            Some(Operative::Bounded(inst)) => assert_eq!(&*inst.instance_id, "t1"),
            _ => panic!("expected a BoundedOperative variant"),
        }

        assert!(matches!(
            reg.remove("t1").as_deref(),
            Some(Operative::Bounded(_))
        ));
        assert!(!reg.contains("t1"));
    }

    #[test]
    fn run_sugar_spec_has_no_topology() {
        let spec = spec::build_spec::<EmitBounded>().expect("run-sugar spec builds");
        assert!(
            spec.topology.is_none(),
            "bare run() ⇒ run-sugar (no topology)"
        );
        assert!(spec.dag.order.is_empty());
    }

    #[test]
    fn structured_spec_resolves_linear_chain() {
        let spec = spec::build_spec::<ChainBounded>().expect("structured spec builds");
        let topology = spec.topology.as_ref().expect("structured ⇒ topology");
        // Chain is ordered entry-consumer → terminal: Stage1 then Stage2.
        let names: Vec<&str> = spec
            .dag
            .order
            .iter()
            .map(|&id| topology.operation(id).unwrap().name.as_ref())
            .collect();
        assert_eq!(names, vec!["bounded_stage1", "bounded_stage2"]);
    }

    #[test]
    fn external_stop_only_authoring_is_rejected() {
        let err = spec::build_spec::<ExternalStopOnlyBounded>()
            .expect_err("external-stop-only must be rejected at registration");
        assert!(
            matches!(err, spec::BuildBoundedError::ExternalStopOnly { .. }),
            "expected ExternalStopOnly, got {err:?}"
        );
        // Naming the missing terminal condition is part of the contract.
        assert!(err.to_string().contains("terminal condition"));
    }

    #[test]
    fn durable_structured_without_inter_op_codec_is_rejected() {
        struct NoCodec;
        impl BoundedOperative for NoCodec {
            fn factory_id() -> &'static str {
                "test-no-codec-bounded"
            }
            fn operations(c: &ChannelBuilder) {
                c.input::<String>("in")
                    .into::<Stage1>()
                    .named("mid")
                    .into::<Stage2>();
            }
        }
        let err = spec::build_spec::<NoCodec>().expect_err("durable class needs the codec");
        match err {
            spec::BuildBoundedError::MissingInterOpCodec { channel, .. } => {
                assert_eq!(channel, "mid", "the error names the codec-less channel");
            }
            other => panic!("expected MissingInterOpCodec, got {other:?}"),
        }

        // The opted-out shape of the same topology registers fine.
        struct NoCodecOptOut;
        impl BoundedOperative for NoCodecOptOut {
            fn factory_id() -> &'static str {
                "test-no-codec-optout-bounded"
            }
            const PERSISTENCE_ENABLED: bool = false;
            fn operations(c: &ChannelBuilder) {
                c.input::<String>("in")
                    .into::<Stage1>()
                    .named("mid")
                    .into::<Stage2>();
            }
        }
        spec::build_spec::<NoCodecOptOut>().expect("opted-out classes may omit the codec");
    }

    /// `bounded-durable-loop` registration validation: a loop needs a chain to
    /// iterate, and a durable handoff-chained loop needs a serializable terminal
    /// batch — otherwise a crash mid-loop would re-seed iteration N+1 with an
    /// empty batch and silently read as "converged".
    #[test]
    fn loop_authoring_is_validated() {
        match spec::build_spec::<LoopWithoutChain>().expect_err("run-sugar cannot loop a chain") {
            spec::BuildBoundedError::LoopWithoutChain { .. } => {}
            other => panic!("expected LoopWithoutChain, got {other:?}"),
        }

        match spec::build_spec::<ChainedLoopNoCodec>()
            .expect_err("a chained loop must be able to journal its terminal batch")
        {
            spec::BuildBoundedError::LoopTerminalWithoutCodec { operation, .. } => {
                assert_eq!(
                    operation, "bounded_stage2",
                    "the error names the terminal operation"
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

    /// `bounded-segment-retry` policy surface: the defaults land on the shared
    /// `RetryPolicy`, `retry_attempts() = 0` collapses to the one-attempt
    /// fail-fast, and a ladder without a backoff is rejected at registration.
    #[test]
    fn retry_policy_is_resolved_and_validated() {
        let spec = spec::build_spec::<ChainBounded>().expect("defaults register");
        assert_eq!(spec.retry.max_attempts, 3, "platform default is 3 attempts");
        assert_eq!(spec.retry.backoff_base_ms, 500);

        let fail_fast = spec::build_spec::<RetryFailFastBounded>().expect("attempts = 0 registers");
        assert_eq!(
            fail_fast.retry.max_attempts, 1,
            "attempts = 0 ⇒ the single fail-fast attempt"
        );

        struct HotLoop;
        impl BoundedOperative for HotLoop {
            fn factory_id() -> &'static str {
                "test-retry-hot-loop"
            }
            fn retry_backoff() -> Duration {
                Duration::ZERO
            }
            fn operations(c: &ChannelBuilder) {
                retry_chain(c);
            }
        }
        match spec::build_spec::<HotLoop>().expect_err("a retry ladder needs a backoff") {
            spec::BuildBoundedError::ZeroRetryBackoff { attempts, .. } => assert_eq!(attempts, 3),
            other => panic!("expected ZeroRetryBackoff, got {other:?}"),
        }
    }

    /// `bounded-branching` registration validation. A DAG is accepted (with its
    /// nodes in Kahn topological order, every producer before its consumers —
    /// the invariant that makes completion order topological and its reverse a
    /// valid compensation order); a cycle is refused, naming a node on it.
    #[test]
    fn dag_topology_is_validated() {
        // A→(B,C): one channel, two consumers. The edge is (channel, consumer),
        // so both branches read what Stage1 released on `mid`.
        let spec = spec::build_spec::<TreeBounded>().expect("a fan-out tree registers");
        let topology = spec.topology.as_ref().expect("structured ⇒ topology");
        let names: Vec<&str> = spec
            .dag
            .order
            .iter()
            .map(|&id| topology.operation(id).unwrap().name.as_ref())
            .collect();
        assert_eq!(
            names,
            vec!["bounded_stage1", "bounded_stage2", "bounded_echo_op"],
            "Kahn order: the root, then its children (= the former BFS numbering)"
        );
        assert_eq!(spec.dag.parents, vec![vec![], vec![0], vec![0]]);
        assert_eq!(spec.dag.children, vec![vec![1, 2], vec![], vec![]]);
        assert_eq!(spec.dag.leaves().collect::<Vec<_>>(), vec![1, 2]);
        assert!(
            spec.dag.single_leaf().is_none(),
            "two leaves ⇒ no single terminal operation"
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
        let linear = spec::build_spec::<ChainBounded>().expect("linear still registers");
        assert_eq!(linear.dag.children, vec![vec![1], vec![]]);
        assert_eq!(linear.dag.single_leaf(), Some(1));

        // A join — two streams feeding one node — registers, and Kahn numbers
        // BOTH producers before it (BFS would have enqueued the join on its
        // first producer).
        match spec::build_spec::<JoinBounded>() {
            Ok(spec) => {
                let topology = spec.topology.as_ref().expect("structured ⇒ topology");
                let names: Vec<&str> = spec
                    .dag
                    .order
                    .iter()
                    .map(|&id| topology.operation(id).unwrap().name.as_ref())
                    .collect();
                assert_eq!(
                    names,
                    vec![
                        "bounded_stage1",
                        "bounded_stage2",
                        "bounded_echo_op",
                        "bounded_join_op"
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
        impl BoundedOperative for Cyclic {
            fn factory_id() -> &'static str {
                "test-cyclic-bounded"
            }
            fn operations(c: &ChannelBuilder) {
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
            spec::BuildBoundedError::Topology { detail, .. } => {
                assert!(
                    detail.contains("Cycle") || detail.contains("cycle"),
                    "the refusal identifies the cycle: {detail}"
                );
            }
            other => panic!("expected the shared cycle refusal, got {other:?}"),
        }

        // A handoff-chained loop over a branching tree has no single terminal
        // batch to seed iteration N+1 from.
        struct BranchingChained;
        impl BoundedOperative for BranchingChained {
            fn factory_id() -> &'static str {
                "test-branching-chained-bounded"
            }
            fn loop_mode() -> LoopMode {
                LoopMode::Chained
            }
            fn operations(c: &ChannelBuilder) {
                tree_chain(c);
            }
        }
        match spec::build_spec::<BranchingChained>()
            .expect_err("a chained loop needs one terminal operation")
        {
            spec::BuildBoundedError::BranchingChainedLoop { leaves, .. } => {
                assert_eq!(leaves, 2);
            }
            other => panic!("expected BranchingChainedLoop, got {other:?}"),
        }
    }

    #[test]
    fn dynamic_segment_operation_is_rejected() {
        #[derive(Default)]
        struct DynOp;
        impl Operation for DynOp {
            type Input = String;
            type Output = String;
            type State = ();
            type Change = NoChange;
            type Clock = WallClock;
            type Config = ();
            fn name() -> &'static str {
                "bounded_dyn_op"
            }
            fn operation_key(input: &String) -> Option<String> {
                Some(input.clone())
            }
            const OPERATION_KEY_OVERRIDDEN: bool = true;
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
            fn run(
                &mut self,
                _input: String,
                _state: StateHandle<'_, Self>,
                _ctx: &mut OperationCtx<'_>,
            ) -> Result<(), OperationError> {
                Ok(())
            }
        }
        struct DynBounded;
        impl BoundedOperative for DynBounded {
            fn factory_id() -> &'static str {
                "test-dyn-bounded"
            }
            fn operations(c: &ChannelBuilder) {
                c.input::<String>("in").into::<DynOp>();
            }
        }
        let err = spec::build_spec::<DynBounded>().expect_err("dynamic segments must be rejected");
        assert!(
            matches!(err, spec::BuildBoundedError::DynamicSegments { .. }),
            "expected DynamicSegments, got {err:?}"
        );
    }

    // ---- single driver-observation test --------------------------------------
    //
    // Installs the process-global coordinator trigger so it can observe the
    // EmitBoundedOutput / EmitBoundedCompleted messages the driver routes
    // through it; the shared runner-state lock keeps that install exclusive
    // against the other tests that install one. Scenarios run strictly
    // sequentially (each drained to its completion before the next) so a
    // single shared channel stays unambiguous.
    #[test]
    fn bounded_driver_scenarios() {
        let _runner_guard = crate::runner::env_test_guard();
        let (tx, rx) = crossbeam::channel::unbounded::<RunnerMessage>();
        install_reconnect_trigger(tx);

        // Durable bounded operatives in this test ship WAL frames through the
        // process-global sink; hold the shared sink lock for the whole test so
        // it never races the segment-state sink tests, and install a capturing
        // sink to observe the frames.
        let _sink_guard = crate::segment::segment_state_sink::SINK_TEST_LOCK
            .lock()
            .unwrap();
        let wal_sink: crate::segment::flusher::SendFrameFn = Arc::new(
            |payload_id: &str, frame: &[u8]| -> Result<(), crate::segment::flusher::SendError> {
                WAL_FRAMES
                    .lock()
                    .unwrap()
                    .push((payload_id.to_string(), frame.to_vec()));
                Ok(())
            },
        );
        crate::segment::segment_state_sink::install_global_sink(wal_sink);

        crate::register_bounded_class::<EmitBounded>().unwrap();
        crate::register_bounded_class::<FailBounded>().unwrap();
        crate::register_bounded_class::<PanicBounded>().unwrap();
        crate::register_bounded_class::<CompleteBounded>().unwrap();
        crate::register_bounded_class::<LoopBounded>().unwrap();
        crate::register_bounded_class::<TimedBounded>().unwrap();
        crate::register_bounded_class::<EchoStructured>().unwrap();
        crate::register_bounded_class::<ChainBounded>().unwrap();
        crate::register_bounded_class::<TreeBounded>().unwrap();
        crate::register_bounded_class::<TreeSaga>().unwrap();
        crate::register_bounded_class::<FanBounded>().unwrap();
        crate::register_bounded_class::<FanOptOutBounded>().unwrap();
        crate::register_bounded_class::<HybridBounded>().unwrap();
        crate::register_bounded_class::<SagaBounded>().unwrap();
        crate::register_bounded_class::<KeepProgressSaga>().unwrap();
        crate::register_bounded_class::<CleanSaga>().unwrap();
        crate::register_bounded_class::<FanSaga>().unwrap();
        crate::register_bounded_class::<StopSaga>().unwrap();
        crate::register_bounded_class::<ResumeSaga>().unwrap();
        crate::register_bounded_class::<LeakBounded>().unwrap();
        crate::register_bounded_class::<UndoSaga>().unwrap();
        crate::register_bounded_class::<ChatLoop>().unwrap();
        crate::register_bounded_class::<LoopSagaAllOrNothing>().unwrap();
        crate::register_bounded_class::<LoopSagaKeepProgress>().unwrap();
        crate::register_bounded_class::<RetryBounded>().unwrap();
        crate::register_bounded_class::<RetryFailFastBounded>().unwrap();
        crate::register_bounded_class::<RetrySlowBackoffBounded>().unwrap();
        crate::register_bounded_class::<DiamondBounded>().unwrap();
        crate::register_bounded_class::<DiamondSaga>().unwrap();

        let mut node = RunnerNode::new("test-ns".to_string());

        // --- run-sugar: run once, args echoed, status "ok" ---
        EMIT_RUNS.store(0, Ordering::SeqCst);
        assert!(node.start_bounded(
            "test-emit-bounded".into(),
            "bounded-ok".into(),
            "hello".into()
        ));
        let (outs, done) = drain(&rx, "bounded-ok");
        assert_eq!(
            EMIT_RUNS.load(Ordering::SeqCst),
            1,
            "run executes exactly once"
        );
        assert_eq!(outs, vec![b"hello".to_vec()], "args echoed as output");
        assert_eq!((done.0.as_str(), done.1.as_str()), ("ok", ""));

        // Instance stays registered; a duplicate start is idempotent (no re-run).
        assert!(node.has_operative("bounded-ok"));
        assert!(
            node.get_unbounded("bounded-ok").is_none(),
            "a bounded is not a operative"
        );
        assert!(node.start_bounded(
            "test-emit-bounded".into(),
            "bounded-ok".into(),
            "hello".into()
        ));
        assert_eq!(
            EMIT_RUNS.load(Ordering::SeqCst),
            1,
            "duplicate start must not re-run"
        );

        // --- run-sugar: terminal error, with no retry ladder to climb ---
        FAIL_RUNS.store(0, Ordering::SeqCst);
        assert!(node.start_bounded("test-fail-bounded".into(), "bounded-err".into(), "".into()));
        let (_o, done) = drain(&rx, "bounded-err");
        assert_eq!(done.0, "error");
        assert!(done.1.contains("boom"));
        assert_eq!(
            FAIL_RUNS.load(Ordering::SeqCst),
            1,
            "a run-sugar body has no segments to retry: Err is terminal on the first failure"
        );

        // --- run-sugar: panic caught and reported as "error" ---
        assert!(node.start_bounded(
            "test-panic-bounded".into(),
            "bounded-panic".into(),
            "".into()
        ));
        let (_o, done) = drain(&rx, "bounded-panic");
        assert_eq!(done.0, "error");
        assert!(done.1.contains("kaboom"));

        // --- run-sugar: ctx.complete carries the result payload ---
        assert!(node.start_bounded(
            "test-complete-bounded".into(),
            "bounded-c".into(),
            "z".into()
        ));
        let (_o, done) = drain(&rx, "bounded-c");
        assert_eq!(done.0, "ok");
        assert_eq!(
            done.2,
            b"result:z".to_vec(),
            "complete() result reaches BoundedCompleted"
        );

        // --- inbox + loop: post-start input is processed, terminal predicate exits ok ---
        assert!(node.start_bounded("test-loop-bounded".into(), "loop-1".into(), "".into()));
        assert!(
            node.get_bounded("loop-1")
                .is_some_and(|i| i.push_inbox(b"alpha".to_vec(), 0)),
            "inbox delivers to running instance"
        );
        assert!(node
            .get_bounded("loop-1")
            .is_some_and(|i| i.push_inbox(b"beta".to_vec(), 0)));
        assert!(node
            .get_bounded("loop-1")
            .is_some_and(|i| i.push_inbox(b"quit".to_vec(), 0)));
        let (outs, done) = drain(&rx, "loop-1");
        assert_eq!(outs, vec![b"got:alpha".to_vec(), b"got:beta".to_vec()]);
        assert_eq!(done.0, "ok", "terminal predicate ⇒ ok");

        // --- external stop (BoundedStop handler) → cancelled ---
        assert!(node.start_bounded("test-loop-bounded".into(), "loop-2".into(), "".into()));
        assert!(node
            .get_bounded("loop-2")
            .is_some_and(|i| i.push_inbox(b"one".to_vec(), 0)));
        // Drain the echo first so the instance is definitely live before stop.
        wait_for_output(&rx, "loop-2", b"got:one");
        let stop = BoundedStop {
            operative_id: "loop-2".into(),
        };
        let mut errs = Vec::new();
        stop.handle(&mut node, "", &mut errs);
        assert!(errs.is_empty());
        let (_o, done) = drain(&rx, "loop-2");
        assert_eq!(done.0, "cancelled", "BoundedStop ⇒ cancelled");

        // --- hard removal (RemOperative → stop_operative) cancels a bounded ---
        assert!(node.start_bounded("test-loop-bounded".into(), "loop-3".into(), "".into()));
        assert!(node
            .get_bounded("loop-3")
            .is_some_and(|i| i.push_inbox(b"x".to_vec(), 0)));
        wait_for_output(&rx, "loop-3", b"got:x");
        assert!(
            node.stop_operative("loop-3"),
            "stop_operative removes the bounded"
        );
        assert!(!node.has_operative("loop-3"));
        let (_o, done) = drain(&rx, "loop-3");
        assert_eq!(
            done.0, "cancelled",
            "hard stop ⇒ driver cancels and tears down"
        );

        // --- max_duration backstop → cancelled ---
        assert!(node.start_bounded("test-timed-bounded".into(), "timed-1".into(), "".into()));
        let (_o, done) = drain(&rx, "timed-1");
        assert_eq!(done.0, "cancelled", "max_duration elapse ⇒ cancelled");

        // --- structured single-op equals run-sugar (§2.2 equivalence) ---
        assert!(node.start_bounded("test-echo-structured".into(), "echo-s".into(), "foo".into()));
        let (struct_outs, done) = drain(&rx, "echo-s");
        assert_eq!(done.0, "ok");
        assert_eq!(struct_outs, vec![b"echo:foo".to_vec()]);
        // The run-sugar equivalent (EmitBounded-style) would emit the same bytes
        // for the same transform — assert the structured single-op form matches
        // a hand-written one-operation echo.
        assert_eq!(struct_outs, vec![format!("echo:{}", "foo").into_bytes()]);

        // --- structured multi-op chain runs in order with a barrier ---
        assert!(node.start_bounded("test-chain-bounded".into(), "chain-1".into(), "x".into()));
        let (chain_outs, done) = drain(&rx, "chain-1");
        assert_eq!(done.0, "ok");
        assert_eq!(
            chain_outs,
            vec![b"x-1-2".to_vec()],
            "Stage1 then Stage2, in order"
        );

        // --- structured parallel static segments + barrier + per-segment state ---
        assert!(node.start_bounded(
            "test-fan-bounded".into(),
            "fan-1".into(),
            "a,bb,ccc,dddd".into(),
        ));
        let (mut fan_outs, done) = drain(&rx, "fan-1");
        assert_eq!(done.0, "ok");
        fan_outs.sort();
        // odd segment: "a"(1) then "ccc"(2); even segment: "bb"(1) then "dddd"(2).
        // Independent per-segment counters; the barrier collected all four.
        let mut expected: Vec<Vec<u8>> = vec![
            b"1:a".to_vec(),
            b"2:ccc".to_vec(),
            b"1:bb".to_vec(),
            b"2:dddd".to_vec(),
        ];
        expected.sort();
        assert_eq!(fan_outs, expected);

        // --- hybrid channel: inter-op output also routed to a plugin sink ---
        assert!(node.start_bounded("test-hybrid-bounded".into(), "hybrid-1".into(), "z".into()));
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
        assert!(node.start_bounded("test-saga-bounded".into(), "saga-1".into(), "X".into()));
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
        assert!(node.start_bounded(
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
        assert!(node.start_bounded("test-clean-saga".into(), "saga-ok".into(), "Z".into()));
        let (clean_outs, done) = drain(&rx, "saga-ok");
        assert_eq!(done.0, "ok");
        assert_eq!(clean_outs, vec![b"echo:charged:reserved:Z".to_vec()]);
        assert!(
            drain_comp_log().is_empty(),
            "a clean ok must not compensate"
        );

        // --- §7b.1 per-segment compensation: once per segment that ran;
        // hookless SplitOp is skipped ---
        let _ = drain_comp_log();
        assert!(node.start_bounded("test-fan-saga".into(), "fan-saga".into(), "a,bb,ccc".into()));
        let (_o, done) = drain(&rx, "fan-saga");
        assert_eq!(done.0, "error");
        let mut fan_comp = drain_comp_log();
        fan_comp.sort();
        // odd segment ran "a","ccc"; even ran "bb"; the hook fires once per
        // segment with that segment's own outputs. SplitOp (no hook) is skipped.
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
        assert!(node.start_bounded("test-stop-saga".into(), "stop-saga".into(), "W".into()));
        while !STAGE0_RAN.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(5));
        }
        node.get_bounded("stop-saga")
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

        // --- AddOperative --kind bounded routes to the bounded path ---
        let add = AddOperative {
            operative_id: "bounded-via-add".into(),
            factory_id: "test-emit-bounded".into(),
            init_values: "argz".into(),
            kind: "bounded".into(),
            resume_segment_count: 0,
            op_journal: None,
        };
        let mut errors = Vec::new();
        add.handle(&mut node, "", &mut errors);
        assert!(
            errors.is_empty(),
            "AddOperative --kind bounded should not error: {errors:?}"
        );
        let (outs, done) = drain(&rx, "bounded-via-add");
        assert_eq!(outs, vec![b"argz".to_vec()]);
        assert_eq!(done.0, "ok");

        // --- AddOperative --kind bounded for an unregistered factory reports it ---
        let add_bad = AddOperative {
            operative_id: "bounded-bad".into(),
            factory_id: "test-unregistered".into(),
            init_values: "".into(),
            kind: "bounded".into(),
            resume_segment_count: 0,
            op_journal: None,
        };
        let mut errors_bad = Vec::new();
        add_bad.handle(&mut node, "", &mut errors_bad);
        assert_eq!(errors_bad.len(), 1);
        assert!(errors_bad[0].contains("BoundedOperative class"));

        // --- AddOperative with an unknown kind literal is rejected ---
        let add_unknown = AddOperative {
            operative_id: "kind-bad".into(),
            factory_id: "test-emit-bounded".into(),
            init_values: "".into(),
            kind: "continuous".into(),
            resume_segment_count: 0,
            op_journal: None,
        };
        let mut errors_unknown = Vec::new();
        add_unknown.handle(&mut node, "", &mut errors_unknown);
        assert_eq!(errors_unknown.len(), 1);
        assert!(errors_unknown[0].contains("unknown compute-unit kind 'continuous'"));
        assert!(node.get_bounded("kind-bad").is_none());

        // --- §5b.1/§7c.1 durable bounded actually writes WAL; opt-out writes nothing ---
        // FanBounded is durable-by-default; its FanOp applies a Change per item,
        // which the driver flushes as a segment_state_change frame to the sink.
        WAL_FRAMES.lock().unwrap().clear();
        assert!(node.start_bounded("test-fan-bounded".into(), "fan-wal".into(), "a,bb".into()));
        let (_o, done) = drain(&rx, "fan-wal");
        assert_eq!(done.0, "ok");
        assert!(
            !WAL_FRAMES.lock().unwrap().is_empty(),
            "a durable bounded operative ships segment_state_change frames to the WAL"
        );

        // The opt-out twin does identical work with PERSISTENCE_ENABLED=false:
        // Changes drain-and-discard, nothing reaches the WAL.
        WAL_FRAMES.lock().unwrap().clear();
        assert!(node.start_bounded(
            "test-fan-optout-bounded".into(),
            "fan-optout".into(),
            "a,bb".into(),
        ));
        let (_o, done) = drain(&rx, "fan-optout");
        assert_eq!(done.0, "ok");
        assert!(
            WAL_FRAMES.lock().unwrap().is_empty(),
            "an opt-out bounded operative writes nothing to the WAL"
        );

        // --- §5b.2 crash-resume gate: a durable structured bounded holds its
        // first Operation until the Controller's Segment restores land, and its
        // Segments are reachable by the restore handler via wire identity ---
        WAL_FRAMES.lock().unwrap().clear();
        // Spawn as a "respawn" expecting one Segment restore: the driver builds
        // and registers its Segments, then parks on the resume gate — nothing
        // runs yet because no restore has landed.
        assert!(node.start_bounded_resumable(
            "test-fan-bounded".into(),
            "resume-inst".into(),
            "a,bb".into(),
            1,
            None,
        ));
        // The bounded Segments are now reachable by wire identity — the restore
        // handler could not see them before this change (the unbounded lookup
        // only scans UnboundedInstance segments).
        let even_id = crate::segment::flusher::encode_segment_identity(
            "resume-inst",
            "bounded_fan_op",
            "even",
        );
        let (resume_instance, _slot) = node
            .find_bounded_segment(&even_id)
            .expect("a durable bounded Segment is reachable by the restore handler");
        // Release one restore, exactly as the RestoreInitState handler does after
        // applying a persisted Segment: the parked driver wakes and runs to
        // completion (resume-not-cold-start is proven end-to-end in the Elixir
        // integration test, which does the real WAL round-trip).
        resume_instance.note_restored();
        let (_o, done) = drain(&rx, "resume-inst");
        assert_eq!(
            done.0, "ok",
            "the driver resumes once the expected Segment restore lands"
        );

        // --- bounded-op-journal §D3: a durable structured chain commits every
        // op boundary — one OpCommit per stage, gated on the (fake
        // controller's) durable ack, carrying the codec-serialized handoff ---
        let _ = drain_op_commits();
        assert!(node.start_bounded(
            "test-chain-bounded".into(),
            "chain-commit".into(),
            "x".into()
        ));
        let (outs, done) = drain(&rx, "chain-commit");
        assert_eq!(done.0, "ok");
        assert_eq!(outs, vec![b"x-1-2".to_vec()]);
        let commits = drain_op_commits();
        assert_eq!(
            commits.iter().map(|c| c.op_idx).collect::<Vec<_>>(),
            vec![0, 1],
            "one gated OpCommit per completed stage, in chain order"
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
            commits[0].segments_ran.len(),
            1,
            "one segment ran in stage 0"
        );

        // bounded-transactional-outbox: the terminal stage's plugin output did
        // NOT go to the wire — it was staged and rode INSIDE that stage's
        // commit, keyed by the producing segment and its per-op sequence.
        assert!(
            wire_emits_for("chain-commit").is_empty(),
            "a durable structured operative stages: nothing reaches the wire mid-op"
        );
        assert!(
            commits[0].outbox.is_empty(),
            "stage 0's output is inter-op (codec handoff), not plugin-bound"
        );
        assert_eq!(commits[1].outbox.len(), 1, "stage 1's plugin output staged");
        assert_eq!(commits[1].outbox[0].payload, b"x-1-2".to_vec());
        assert_eq!(commits[1].outbox[0].channel, "out");
        assert_eq!(commits[1].outbox[0].seq_in_op, 0);
        assert!(!commits[1].outbox[0].segment_id.is_empty());
        assert!(commits.iter().all(|c| !c.compensating));

        // The run-sugar path is untouched: no operations, no barrier to stage
        // against, so its emit goes straight to the wire (at-least-once).
        assert_eq!(
            wire_emits_for("bounded-ok"),
            vec![b"hello".to_vec()],
            "run-sugar keeps the direct wire path"
        );

        // Opt-out twin: no journal, no handshake — zero commits, and (having
        // nothing that could ever flush a staged outbox) it still emits direct.
        assert!(node.start_bounded(
            "test-fan-optout-bounded".into(),
            "optout-no-commit".into(),
            "a".into(),
        ));
        let (outs, done) = drain(&rx, "optout-no-commit");
        assert_eq!(done.0, "ok");
        assert!(
            drain_op_commits().is_empty(),
            "an opted-out class skips the op-commit handshake entirely"
        );
        assert_eq!(
            wire_emits_for("optout-no-commit"),
            outs,
            "an opted-out class emits straight to the wire, as before"
        );
        assert_eq!(outs, vec![b"1:a".to_vec()]);

        // --- bounded-transactional-outbox: a failed operation leaks nothing ---
        assert!(node.start_bounded("test-leak-bounded".into(), "leak-1".into(), "x".into()));
        let (outs, done) = drain(&rx, "leak-1");
        assert_eq!(done.0, "error");
        assert!(
            outs.is_empty() && wire_emits_for("leak-1").is_empty(),
            "the failing op emitted before it failed; that output must never be delivered"
        );
        assert!(
            drain_op_commits().is_empty(),
            "the op never committed, so its staged output was never journaled"
        );

        // --- bounded-transactional-outbox §D5: a compensate hook's undo output
        // is staged too, and rides a `compensating` commit for the operation it
        // reverses (the forward output already flushed under its own key) ---
        assert!(node.start_bounded("test-undo-saga".into(), "undo-1".into(), "u".into()));
        let (outs, done) = drain(&rx, "undo-1");
        assert_eq!(done.0, "error");
        assert!(
            wire_emits_for("undo-1").is_empty(),
            "undo output stages like forward output — never straight to the wire"
        );
        let commits = drain_op_commits();
        let forward: Vec<_> = commits.iter().filter(|c| !c.compensating).collect();
        let undo: Vec<_> = commits.iter().filter(|c| c.compensating).collect();
        assert_eq!(forward.len(), 1, "only UndoEmit completed forward");
        assert_eq!(
            forward[0].outbox[0].payload,
            b"did:u".to_vec(),
            "the hybrid channel staged the forward copy"
        );
        assert_eq!(undo.len(), 1, "one compensated segment ⇒ one undo record");
        assert_eq!(undo[0].op_idx, 0, "the compensated operation's index");
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

        // --- bounded-op-journal §D5: resume-skip — a shipped journal skips the
        // committed stages and seeds the next one from the journaled handoff ---
        let (outs, done) = resume_chain_with_journal(
            &node,
            &rx,
            "chain-resume",
            instance::JournalResume {
                committed_op_idxs: vec![0],
                // The seed is the COMMITTING operation's own handoff — keyed by
                // its op_idx, so a fan-out's children each take their own
                // parent's edge (`bounded-branching`).
                op_handoffs: vec![instance::EdgeHandoff {
                    op_idx: 0,
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
            drain_op_commits()
                .iter()
                .map(|c| c.op_idx)
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
                committed_op_idxs: vec![0, 1],
                ..Default::default()
            },
        );
        assert_eq!(done.0, "ok");
        assert!(outs.is_empty(), "a fully-committed chain re-runs nothing");
        assert!(drain_op_commits().is_empty());

        // --- §4.3 durable saga records: a fresh abnormal run emits one
        // compensation_intent per completed compensable segment and one
        // compensation_done per compensated segment ---
        use crate::segment::segment_state_sink::{
            COMPENSATION_DONE_PAYLOAD_ID as DONE, COMPENSATION_INTENT_PAYLOAD_ID as INTENT,
        };
        WAL_FRAMES.lock().unwrap().clear();
        let _ = drain_comp_log();
        // reserve + charge complete (both compensable ⇒ 2 intents); ship fails ⇒
        // both roll back (2 dones). Hookless ship_fails never completed ⇒ no record.
        assert!(node.start_bounded("test-saga-bounded".into(), "saga-rec".into(), "42".into()));
        let (_o, done) = drain(&rx, "saga-rec");
        assert_eq!(done.0, "error");
        assert_eq!(
            count_wal_frames(INTENT),
            2,
            "one intent per completed compensable segment"
        );
        assert_eq!(
            count_wal_frames(DONE),
            2,
            "one done per compensated segment"
        );

        // --- §4.3 crash-resume finishes the remaining hooks, skipping the ones
        // already recorded done, and does NOT re-run the forward chain ---
        WAL_FRAMES.lock().unwrap().clear();
        let _ = drain_comp_log();
        // Respawn a durable saga expecting one restore: the driver parks on the
        // gate while we seed the markers the RestoreInitState handler would carry.
        assert!(node.start_bounded_resumable(
            "test-resume-saga".into(),
            "resume-saga".into(),
            "x".into(),
            1,
            None,
        ));
        let a_id =
            crate::segment::flusher::encode_segment_identity("resume-saga", "resume_undo_a", "");
        let b_id =
            crate::segment::flusher::encode_segment_identity("resume-saga", "resume_undo_b", "");
        let (saga_inst, _slot) = node
            .find_bounded_segment(&a_id)
            .expect("a durable bounded saga Segment is reachable by the restore handler");
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
            "compensation-resume does not re-run forward ops"
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

        // --- bounded-branching fan-in: the join runs once, after BOTH
        // producers committed, with the accumulated batch ---
        let _ = drain_diamond_log();
        let _ = drain_op_commits();
        assert!(node.start_bounded(
            "test-diamond-bounded".into(),
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
            drain_op_commits()
                .iter()
                .map(|c| c.op_idx)
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

        // --- bounded-branching crash-resume over a join: one journaled and one
        // fresh producer — the committed branch never re-runs, and the join
        // accumulates B's JOURNALED handoff with C's fresh output ---
        let _ = drain_diamond_log();
        let da_id = crate::segment::flusher::encode_segment_identity("diamond-r", "diamond_a", "");
        let diamond_spec =
            registry::get_class("test-diamond-bounded").expect("diamond class registered");
        let diamond_topo = diamond_spec.topology.as_ref().expect("structured");
        let d_fan = diamond_topo.lookup_channel_id("d_fan").unwrap();
        let d_b = diamond_topo.lookup_channel_id("d_b").unwrap();
        let enc = |s: &str| postcard::to_allocvec(&s.to_string()).unwrap();
        let diamond_journal = instance::JournalResume {
            committed_op_idxs: vec![0, 1],
            op_handoffs: vec![
                instance::EdgeHandoff {
                    op_idx: 0,
                    handoff: vec![(d_fan, enc("a:x"))],
                },
                instance::EdgeHandoff {
                    op_idx: 1,
                    handoff: vec![(d_b, enc("b:a:x"))],
                },
            ],
            committed_ops: vec![
                instance::CommittedOp {
                    op_idx: 0,
                    iter: 0,
                    segments_ran: vec![da_id.clone()],
                },
                instance::CommittedOp {
                    op_idx: 1,
                    iter: 0,
                    segments_ran: Vec::new(),
                },
            ],
            ..Default::default()
        };
        assert!(node.start_bounded_resumable(
            "test-diamond-bounded".into(),
            "diamond-r".into(),
            "x".into(),
            1,
            Some(diamond_journal),
        ));
        let (diamond_inst, _slot) = node
            .find_bounded_segment(&da_id)
            .expect("a durable diamond Segment is reachable by the restore handler");
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
            drain_op_commits()
                .iter()
                .map(|c| c.op_idx)
                .collect::<Vec<_>>(),
            vec![2, 3],
            "the resume commits only C and the join"
        );

        // --- bounded-branching reverse-topological undo: the join is undone
        // before either producer, the shared root last ---
        let _ = drain_comp_log();
        let _ = drain_diamond_log();
        let _ = drain_op_commits();
        assert!(node.start_bounded("test-diamond-saga".into(), "diamond-s".into(), "x".into()));
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

        // --- bounded-durable-loop: one iteration per inbox item, each ending in
        // a durable iteration commit; an idle inbox parks the instance ---
        std::env::set_var("FLEZHA_BOUNDED_PARK_IDLE_MS", "300");
        let _ = drain_op_commits();
        assert!(node.start_bounded("test-chat-loop".into(), "chat-1".into(), "ignored".into()));
        let chat = node
            .get_bounded("chat-1")
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
            drain_op_commits()
                .iter()
                .map(|c| (c.iter, c.op_idx))
                .collect::<Vec<_>>(),
            vec![(0, 0), (0, 1), (1, 0), (1, 1)],
            "every op commit is stamped with its enclosing iteration"
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
        let s1_id =
            crate::segment::flusher::encode_segment_identity("chat-2", "bounded_stage1", "");
        assert!(node.start_bounded_resumable(
            "test-chat-loop".into(),
            "chat-2".into(),
            "ignored".into(),
            1,
            Some(instance::JournalResume {
                iter: 2,
                consumed_offset: 7,
                committed_ops: vec![
                    instance::CommittedOp {
                        op_idx: 0,
                        iter: 0,
                        segments_ran: vec![s1_id.clone()],
                    },
                    instance::CommittedOp {
                        op_idx: 1,
                        iter: 0,
                        segments_ran: Vec::new(),
                    },
                ],
                ..Default::default()
            }),
        ));
        let (chat2, _slot) = node
            .find_bounded_segment(&s1_id)
            .expect("the loop's Segment is reachable by the restore handler");
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

        // --- bounded-durable-loop D2: KeepProgress/AllOrNothing are backed by
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
            let seg = crate::segment::flusher::encode_segment_identity(id, "loop_reserve", "");
            // Journal: iteration 0 committed both of its operations.
            assert!(node.start_bounded_resumable(
                factory.into(),
                id.into(),
                "ignored".into(),
                1,
                Some(instance::JournalResume {
                    iter: 1,
                    consumed_offset: 2,
                    committed_ops: vec![
                        instance::CommittedOp {
                            op_idx: 0,
                            iter: 0,
                            segments_ran: vec![seg.clone()],
                        },
                        instance::CommittedOp {
                            op_idx: 1,
                            iter: 0,
                            segments_ran: Vec::new(),
                        },
                    ],
                    ..Default::default()
                }),
            ));
            let (inst, _slot) = node
                .find_bounded_segment(&seg)
                .expect("the loop saga's Segment is reachable by the restore handler");
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

        // --- bounded-loop-rewind: a journal carrying `rewind_undo` unwinds those
        // iterations — reverse iteration, reverse stage — BEFORE the loop resumes
        // forward at the rewound-to iteration ---
        let _ = drain_comp_log();
        let _ = drain_op_commits();
        // Keep-progress deliberately: a rewind unwinds committed iterations by
        // definition, so the undo must be forced all-or-nothing whatever the
        // class's crash rollback mode says.
        let id = "loop-rewind";
        let seg = crate::segment::flusher::encode_segment_identity(id, "loop_reserve", "");
        // Rewound to iteration 0: iteration 0 stays committed, iterations 1 and 2
        // are outstanding undo, and the loop resumes at 1.
        let undo_op = |op_idx: u32, iter: u32, segments_ran: Vec<Vec<u8>>| instance::CommittedOp {
            op_idx,
            iter,
            segments_ran,
        };
        assert!(node.start_bounded_resumable(
            "test-loop-saga-keep".into(),
            id.into(),
            "ignored".into(),
            1,
            Some(instance::JournalResume {
                iter: 1,
                consumed_offset: 1,
                committed_ops: vec![undo_op(0, 0, vec![seg.clone()]), undo_op(1, 0, Vec::new())],
                rewind_undo: vec![
                    undo_op(0, 1, vec![seg.clone()]),
                    undo_op(1, 1, Vec::new()),
                    undo_op(0, 2, vec![seg.clone()]),
                    undo_op(1, 2, Vec::new()),
                ],
                ..Default::default()
            }),
        ));
        let (inst, _slot) = node
            .find_bounded_segment(&seg)
            .expect("the loop saga's Segment is reachable by the restore handler");
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
        let undo: Vec<(u32, u32)> = drain_op_commits()
            .iter()
            .filter(|c| c.compensating)
            .map(|c| (c.op_idx, c.iter))
            .collect();
        assert_eq!(
            undo,
            vec![(1, 2), (0, 2), (1, 1), (0, 1)],
            "reverse iteration, reverse stage — and the hookless terminal op still \
             journals its (empty) `compensated` record, which is what drains it out \
             of the next fold's outstanding work"
        );

        std::env::remove_var("FLEZHA_BOUNDED_PARK_IDLE_MS");

        // --- bounded-segment-retry: a transient segment failure recovers in
        // place — the sibling segment is NOT re-run and nothing compensates ---
        arm_flaky(1, false);
        assert!(node.start_bounded(
            "test-retry-bounded".into(),
            "retry-ok".into(),
            "a,bb".into()
        ));
        let (mut outs, done) = drain(&rx, "retry-ok");
        assert_eq!(done.0, "ok", "the ladder absorbed the transient failure");
        outs.sort();
        assert_eq!(
            outs,
            vec![b"even:bb".to_vec(), b"odd:a".to_vec()],
            "each segment's output lands exactly once — a retried attempt's \
             staged output is discarded, not double-emitted"
        );
        assert_eq!(
            ODD_ATTEMPTS.load(Ordering::SeqCst),
            2,
            "the failing segment ran twice (fail, then retry)"
        );
        assert_eq!(
            EVEN_RUNS.load(Ordering::SeqCst),
            1,
            "the sibling segment stayed buffered at the barrier: no re-run"
        );
        assert!(
            drain_comp_log().is_empty(),
            "a recovered retry must not compensate"
        );

        // --- retry exhaustion fails the operation exactly as before: the
        // original error surfaces and completed operations compensate ---
        arm_flaky(99, false);
        assert!(node.start_bounded(
            "test-retry-bounded".into(),
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
        assert!(node.start_bounded(
            "test-retry-bounded".into(),
            "retry-permanent".into(),
            "a,bb".into()
        ));
        let (_o, done) = drain(&rx, "retry-permanent");
        assert_eq!(done.0, "error");
        assert_eq!(
            ODD_ATTEMPTS.load(Ordering::SeqCst),
            1,
            "OperationError::non_retryable ⇒ no retry"
        );
        assert_eq!(drain_comp_log(), vec!["undo-retry-reserve".to_string()]);

        // --- retry_attempts() = 0 restores the pre-change fail-fast ---
        arm_flaky(99, false);
        assert!(node.start_bounded(
            "test-retry-failfast-bounded".into(),
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
        // controller — it must stay in `drain` to ack the chain's op commits ---
        arm_flaky(99, false);
        assert!(node.start_bounded(
            "test-retry-slow-bounded".into(),
            "retry-stop".into(),
            "a,bb".into()
        ));
        let stop_target = node
            .get_bounded("retry-stop")
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

        // --- bounded-branching: A→(B,C) runs to DONE — both branches receive
        // what A released on the channel they consume, and the operative is `ok`
        // exactly when every leaf has committed ---
        let _ = drain_op_commits();
        assert!(node.start_bounded("test-tree-bounded".into(), "tree-1".into(), "x".into()));
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
        let committed: Vec<u32> = drain_op_commits().iter().map(|c| c.op_idx).collect();
        assert_eq!(
            committed,
            vec![0, 1, 2],
            "the root commits before its children, which commit independently"
        );

        // --- bounded-branching resume: the journal records A and B committed, so
        // only C re-runs — seeded from A's journaled edge, not from B's ---
        let _ = drain_op_commits();
        let id = "tree-resume";
        let root_seg = crate::segment::flusher::encode_segment_identity(id, "bounded_stage1", "");
        // Channel ids follow declaration order: `in` = 0, `mid` = 1.
        assert!(node.start_bounded_resumable(
            "test-tree-bounded".into(),
            id.into(),
            "ignored-args".into(),
            1,
            Some(instance::JournalResume {
                committed_op_idxs: vec![0, 1],
                op_handoffs: vec![instance::EdgeHandoff {
                    op_idx: 0,
                    handoff: vec![(1, postcard::to_allocvec(&"resumed-1".to_string()).unwrap())],
                }],
                committed_ops: vec![
                    instance::CommittedOp {
                        op_idx: 0,
                        iter: 0,
                        segments_ran: vec![root_seg.clone()],
                    },
                    instance::CommittedOp {
                        op_idx: 1,
                        iter: 0,
                        segments_ran: Vec::new(),
                    },
                ],
                ..Default::default()
            }),
        ));
        let (inst, _slot) = node
            .find_bounded_segment(&root_seg)
            .expect("the tree's root Segment is reachable by the restore handler");
        inst.note_restored();
        let (outs, done) = drain(&rx, id);
        assert_eq!(done.0, "ok");
        assert_eq!(
            outs,
            vec![b"echo:resumed-1".to_vec()],
            "only the uncommitted branch re-ran, seeded from its parent's edge"
        );
        assert_eq!(
            drain_op_commits()
                .iter()
                .map(|c| c.op_idx)
                .collect::<Vec<_>>(),
            vec![2],
            "the committed root and its committed sibling did NOT re-run"
        );

        // --- bounded-branching compensation: a failure under one branch unwinds
        // BOTH branches before their shared parent (reverse topological) ---
        let _ = drain_comp_log();
        assert!(node.start_bounded("test-tree-saga".into(), "tree-saga".into(), "o1".into()));
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
             committed operation both unwind before the root they share"
        );
    }

    /// Respawn `test-chain-bounded` as a crash-resume carrying `journal` on the
    /// bind (exactly what the AddOperative handler passes), release the restore
    /// gate, and drain it to completion.
    fn resume_chain_with_journal(
        node: &RunnerNode,
        rx: &Receiver<RunnerMessage>,
        id: &str,
        journal: instance::JournalResume,
    ) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
        assert!(node.start_bounded_resumable(
            "test-chain-bounded".into(),
            id.into(),
            "ignored-args".into(),
            1,
            Some(journal),
        ));
        let s1_id = crate::segment::flusher::encode_segment_identity(id, "bounded_stage1", "");
        let (inst, _slot) = node
            .find_bounded_segment(&s1_id)
            .expect("chain segment reachable by the restore handler");
        inst.note_restored();
        drain(rx, id)
    }

    /// Op-commits observed by the fake controller (`bounded-op-journal`
    /// barrier): every `OpCommit` the drain helpers ack is recorded here
    /// so the gating/handoff assertions can inspect what would be journaled.
    static OP_COMMITS: Mutex<Vec<crate::pb::OpCommit>> = Mutex::new(Vec::new());

    /// Iteration commits observed by the fake controller (`bounded-durable-loop`
    /// boundary) — what the journal would record as `iter_committed`.
    static ITER_COMMITS: Mutex<Vec<crate::pb::IterCommit>> = Mutex::new(Vec::new());

    /// Park notifications observed by the fake controller
    /// (`bounded-durable-suspension`) — the instance evicted itself at a
    /// committed boundary.
    static ITER_PARKS: Mutex<Vec<crate::pb::IterPark>> = Mutex::new(Vec::new());

    /// Every `EmitBoundedOutput` the drain helpers saw — output that went
    /// STRAIGHT to the wire, bypassing any op boundary. A durable structured
    /// operative must produce none of these (it stages); `run`-sugar and
    /// opted-out classes still do.
    static WIRE_EMITS: Mutex<Vec<(String, Vec<u8>)>> = Mutex::new(Vec::new());

    fn drain_op_commits() -> Vec<crate::pb::OpCommit> {
        std::mem::take(&mut *OP_COMMITS.lock().unwrap())
    }

    fn drain_iter_commits() -> Vec<crate::pb::IterCommit> {
        std::mem::take(&mut *ITER_COMMITS.lock().unwrap())
    }

    fn drain_iter_parks() -> Vec<crate::pb::IterPark> {
        std::mem::take(&mut *ITER_PARKS.lock().unwrap())
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
            Command::OpCommit(commit) => {
                let flushed: Vec<Vec<u8>> =
                    commit.outbox.iter().map(|o| o.payload.clone()).collect();
                OP_COMMITS.lock().unwrap().push(commit);
                crate::shutdown::signal_result(&reference_id, Ok(String::new()));
                flushed
            }
            Command::IterCommit(commit) => {
                ITER_COMMITS.lock().unwrap().push(commit);
                crate::shutdown::signal_result(&reference_id, Ok(String::new()));
                Vec::new()
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
            Command::OpCommit(c) => c.operative_id.clone(),
            Command::IterCommit(c) => c.operative_id.clone(),
            Command::IterPark(p) => p.operative_id.clone(),
            _ => String::new(),
        }
    }

    /// Drain output frames until the `BoundedCompleted` for `id`, returning the
    /// collected outputs and the `(status, error, result)` completion tuple.
    /// "Output" is what reaches the plugins: a `run`-sugar / opted-out emit
    /// straight off the wire, plus a structured operative's staged outbox once
    /// its op commits (the fake controller flushes it on ack).
    fn drain(rx: &Receiver<RunnerMessage>, id: &str) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut outs = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let msg = rx
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("timed out waiting for bounded {id}"));
            match msg {
                RunnerMessage::EmitBoundedOutput {
                    operative_id,
                    payload,
                } if operative_id == id => {
                    WIRE_EMITS
                        .lock()
                        .unwrap()
                        .push((operative_id, payload.clone()));
                    outs.push(payload);
                }
                RunnerMessage::EmitBoundedCompleted {
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
                RunnerMessage::EmitBoundedOutput {
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
