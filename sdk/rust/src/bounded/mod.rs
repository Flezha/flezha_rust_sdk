//! The **bounded** compute-unit kind: an on-demand, terminating,
//! possibly-interactive process.
//!
//! A bounded operative is spawned with `args`, runs to an exit trigger, and is
//! then torn down. It reuses the continuous **building blocks** (Operations,
//! Channels, Segments, per-Segment state, `ctx.emit`) but drives them under a
//! bounded **lifecycle**:
//!
//! - **`run` sugar** — the trivial case is a bare
//!   [`run`](BoundedOperative::run) body: a single implicit Operation/Segment.
//! - **Structured** — override [`operations`](BoundedOperative::operations) to
//!   author Operations/Channels/Segments on the same `ChannelBuilder` a
//!   continuous operative uses; the bounded driver runs them **sequentially in
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
//! impl flezha_executor::BoundedOperative for Greet {
//!     fn factory_id() -> &'static str { "greet" }
//!     fn run(ctx: &mut flezha_executor::BoundedCtx, args: &str) -> Result<(), flezha_executor::BoundedError> {
//!         ctx.complete(format!("hello {args}").into_bytes());
//!         Ok(())
//!     }
//! }
//! flezha_executor::register_bounded::<Greet>();
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

use crate::channel::ChannelBuilder;

/// One bounded operative class. The bare [`run`](Self::run) form is the
/// trivial single-Operation, single-Segment case; override
/// [`operations`](Self::operations) to grow into an explicit linear chain of
/// Operations/Channels/Segments without rewriting unrelated code. Capabilities
/// attach to the kind, not here — choosing the bounded kind is sufficient.
pub trait BoundedOperative: Send + Sync + 'static {
    /// Stable, controller-facing factory identifier.
    fn factory_id() -> &'static str;

    /// Run-sugar body: the single-Operation, single-Segment case. Invoked once
    /// per started instance with `args` from `init_values`. Emit output via
    /// `ctx`, optionally call [`BoundedCtx::complete`] with a result, and
    /// return `Ok(())`; returning `Err` is a terminal failure (no retry).
    ///
    /// The default errors — a class must implement either `run` or
    /// `operations`. A structured operative (non-empty `operations`) never has
    /// `run` invoked.
    fn run(_ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
        Err(BoundedError::from(
            "BoundedOperative: neither run() nor operations() was implemented",
        ))
    }

    /// Structured form: declare a **linear chain** of Operations/Channels/
    /// Segments on the same `ChannelBuilder` a continuous operative uses. The
    /// default is empty, selecting the [`run`](Self::run)-sugar path. A
    /// non-empty declaration is run sequentially by the bounded driver
    /// (operation `N+1` starts only when every segment of operation `N` has
    /// finished). Branching is rejected at registration.
    fn operations(_c: &mut ChannelBuilder) {}

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
    /// [`ContinuousOperative::PERSISTENCE_ENABLED`](crate::operative::ContinuousOperative::PERSISTENCE_ENABLED)).
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

/// A terminal bounded failure.
#[derive(Debug, Clone)]
pub struct BoundedError(pub String);

impl fmt::Display for BoundedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for BoundedError {
    fn from(s: String) -> Self {
        BoundedError(s)
    }
}

impl From<&str> for BoundedError {
    fn from(s: &str) -> Self {
        BoundedError(s.to_string())
    }
}

/// Collecting sink driven by [`crate::segment::OperationDispatcher::run_collecting`]:
/// inter-operation output is **collected** for handoff to the next stage at the
/// barrier; plugin-bound output is **routed** (serialized) to the wire. The
/// bounded driver implements this per running segment.
pub trait BoundedCollectSink {
    /// Collect an inter-operation output value for the next stage's input batch.
    fn collect(&mut self, value: Arc<dyn Any + Send + Sync>);

    /// Forward an already-serialized plugin-bound output to the unit's wire
    /// (best-effort, at-least-once — like all bounded/operative output).
    fn emit_plugin_bytes(&mut self, bytes: Vec<u8>);
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
    inbox: Receiver<Vec<u8>>,
}

impl BoundedCtx {
    pub(crate) fn new(
        instance_id: Arc<str>,
        cancel: Arc<AtomicBool>,
        inbox: Receiver<Vec<u8>>,
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
    pub fn emit(&mut self, bytes: impl Into<Vec<u8>>) -> Result<(), BoundedError> {
        crate::executor::emit_bounded_output(self.instance_id.to_string(), bytes.into());
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
                Ok(item) => return Some(item),
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
    use crate::executor::fast::ExecutorNode;
    use crate::executor::{install_reconnect_trigger, ExecutorMessage};
    use crate::operation::change::{Change, NoChange};
    use crate::operation::clock::WallClock;
    use crate::operation::{Operation, OperationCtx, OperationError, StateHandle};
    use crate::operative::registry::{Operative, OperativeRegistry};
    use crate::pb::{AddOperative, BoundedStop};
    use crate::segment::restore::RestoreInitPayload;

    static EMIT_RUNS: AtomicUsize = AtomicUsize::new(0);

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

    struct FailBounded;
    impl BoundedOperative for FailBounded {
        fn factory_id() -> &'static str {
            "test-fail-bounded"
        }
        fn run(_ctx: &mut BoundedCtx, _args: &str) -> Result<(), BoundedError> {
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
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<EchoOp>();
            c.channel::<String>("out")
                .from::<EchoOp>()
                .to_plugin("sink");
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

    /// Two-stage linear chain: Stage1 → Stage2 → plugin.
    struct ChainBounded;
    impl BoundedOperative for ChainBounded {
        fn factory_id() -> &'static str {
            "test-chain-bounded"
        }
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<Stage1>();
            c.channel::<String>("mid").from::<Stage1>().into::<Stage2>();
            c.channel::<String>("out")
                .from::<Stage2>()
                .to_plugin("sink");
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
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<SplitOp>();
            c.channel::<String>("mid").from::<SplitOp>().into::<FanOp>();
            c.channel::<String>("out").from::<FanOp>().to_plugin("sink");
        }
    }

    /// Opt-out twin of `FanBounded`: identical topology, persistence disabled —
    /// its Changes drain-and-discard instead of reaching the WAL.
    struct FanOptOutBounded;
    impl BoundedOperative for FanOptOutBounded {
        fn factory_id() -> &'static str {
            "test-fan-optout-bounded"
        }
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<SplitOp>();
            c.channel::<String>("mid").from::<SplitOp>().into::<FanOp>();
            c.channel::<String>("out").from::<FanOp>().to_plugin("sink");
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

    /// Hybrid inter-operation channel (`also_to_plugin_with`): Stage1's output
    /// both feeds Stage2 AND is serialized to a plugin sink.
    struct HybridBounded;
    impl BoundedOperative for HybridBounded {
        fn factory_id() -> &'static str {
            "test-hybrid-bounded"
        }
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<HybridStage1>();
            c.channel::<String>("mid")
                .from::<HybridStage1>()
                .also_to_plugin_with("midsink", |s: &String| format!("mid:{s}").into_bytes())
                .into::<HybridStage2>();
            c.channel::<String>("out")
                .from::<HybridStage2>()
                .to_plugin("out");
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
    struct SagaBounded;
    impl BoundedOperative for SagaBounded {
        fn factory_id() -> &'static str {
            "test-saga-bounded"
        }
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<SagaReserve>();
            c.channel::<String>("c1")
                .from::<SagaReserve>()
                .into::<SagaCharge>();
            c.channel::<String>("c2")
                .from::<SagaCharge>()
                .into::<SagaShipFails>();
            c.channel::<String>("out")
                .from::<SagaShipFails>()
                .to_plugin("sink");
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
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<SagaReserve>();
            c.channel::<String>("c1")
                .from::<SagaReserve>()
                .into::<SagaCharge>();
            c.channel::<String>("c2")
                .from::<SagaCharge>()
                .into::<SagaShipFails>();
            c.channel::<String>("out")
                .from::<SagaShipFails>()
                .to_plugin("sink");
        }
    }

    /// Reserve → Charge → Echo(succeeds): a clean `ok` MUST NOT compensate.
    struct CleanSaga;
    impl BoundedOperative for CleanSaga {
        fn factory_id() -> &'static str {
            "test-clean-saga"
        }
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<SagaReserve>();
            c.channel::<String>("c1")
                .from::<SagaReserve>()
                .into::<SagaCharge>();
            c.channel::<String>("c2")
                .from::<SagaCharge>()
                .into::<EchoOp>();
            c.channel::<String>("out")
                .from::<EchoOp>()
                .to_plugin("sink");
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
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<SplitOp>();
            c.channel::<String>("mid")
                .from::<SplitOp>()
                .into::<SagaFan>();
            c.channel::<String>("mid2")
                .from::<SagaFan>()
                .into::<FailLast>();
            c.channel::<String>("out")
                .from::<FailLast>()
                .to_plugin("sink");
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
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<StopReserve>();
            c.channel::<String>("mid")
                .from::<StopReserve>()
                .into::<EchoOp>();
            c.channel::<String>("out")
                .from::<EchoOp>()
                .to_plugin("sink");
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

    /// A → B → ship(fails). Durable-by-default; both A and B are compensable.
    struct ResumeSaga;
    impl BoundedOperative for ResumeSaga {
        fn factory_id() -> &'static str {
            "test-resume-saga"
        }
        fn operations(c: &mut ChannelBuilder) {
            c.input::<String>("in").into::<ResumeUndoA>();
            c.channel::<String>("c1")
                .from::<ResumeUndoA>()
                .into::<ResumeUndoB>();
            c.channel::<String>("c2")
                .from::<ResumeUndoB>()
                .into::<SagaShipFails>();
            c.channel::<String>("out")
                .from::<SagaShipFails>()
                .to_plugin("sink");
        }
    }

    // ---- non-driver unit tests ----------------------------------------------

    #[test]
    fn bounded_error_conversions() {
        let from_str: BoundedError = "x".into();
        assert_eq!(from_str.to_string(), "x");
        let from_string: BoundedError = String::from("y").into();
        assert_eq!(from_string.0, "y");
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
        crate::register_bounded::<RegBounded>();
        assert!(registry::get_class("test-getclass-bounded").is_some());
        assert!(registry::get_class("test-nonexistent-bounded").is_none());
    }

    #[test]
    fn registry_holds_bounded_as_sealed_variant() {
        let reg = OperativeRegistry::new();
        let (instance, _inbox_rx) =
            crate::bounded::instance::BoundedInstance::new(Arc::from("t1"), Vec::new(), 0);
        reg.register_bounded(instance);

        assert!(reg.contains("t1"));
        // A bounded is not a operative: the operative-only accessor returns None.
        assert!(reg.get_continuous("t1").is_none());
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
        assert!(spec.chain.is_empty());
    }

    #[test]
    fn structured_spec_resolves_linear_chain() {
        let spec = spec::build_spec::<ChainBounded>().expect("structured spec builds");
        let topology = spec.topology.as_ref().expect("structured ⇒ topology");
        // Chain is ordered entry-consumer → terminal: Stage1 then Stage2.
        let names: Vec<&str> = spec
            .chain
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
    fn branching_topology_is_rejected() {
        struct Branching;
        impl BoundedOperative for Branching {
            fn factory_id() -> &'static str {
                "test-branching-bounded"
            }
            fn operations(c: &mut ChannelBuilder) {
                // One channel feeding two downstream operations = branching.
                c.input::<String>("in").into::<Stage1>();
                c.channel::<String>("mid")
                    .from::<Stage1>()
                    .into::<Stage2>()
                    .into::<EchoOp>();
            }
        }
        let err = spec::build_spec::<Branching>().expect_err("branching must be rejected");
        assert!(
            matches!(err, spec::BuildBoundedError::Branching { .. }),
            "expected Branching, got {err:?}"
        );
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
            fn operations(c: &mut ChannelBuilder) {
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
    // Owns the process-global coordinator trigger (no other lib test installs
    // it) so it can observe the EmitBoundedOutput / EmitBoundedCompleted
    // messages the driver routes through the coordinator. Scenarios run
    // strictly sequentially (each drained to its completion before the next) so
    // a single shared channel stays unambiguous.
    #[test]
    fn bounded_driver_scenarios() {
        let (tx, rx) = crossbeam::channel::unbounded::<ExecutorMessage>();
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

        crate::register_bounded::<EmitBounded>();
        crate::register_bounded::<FailBounded>();
        crate::register_bounded::<PanicBounded>();
        crate::register_bounded::<CompleteBounded>();
        crate::register_bounded::<LoopBounded>();
        crate::register_bounded::<TimedBounded>();
        crate::register_bounded::<EchoStructured>();
        crate::register_bounded::<ChainBounded>();
        crate::register_bounded::<FanBounded>();
        crate::register_bounded::<FanOptOutBounded>();
        crate::register_bounded::<HybridBounded>();
        crate::register_bounded::<SagaBounded>();
        crate::register_bounded::<KeepProgressSaga>();
        crate::register_bounded::<CleanSaga>();
        crate::register_bounded::<FanSaga>();
        crate::register_bounded::<StopSaga>();
        crate::register_bounded::<ResumeSaga>();

        let mut node = ExecutorNode::new("test-ns".to_string());

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
            node.get_continuous("bounded-ok").is_none(),
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

        // --- run-sugar: terminal error ---
        assert!(node.start_bounded("test-fail-bounded".into(), "bounded-err".into(), "".into()));
        let (_o, done) = drain(&rx, "bounded-err");
        assert_eq!(done.0, "error");
        assert!(done.1.contains("boom"));

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
                .is_some_and(|i| i.push_inbox(b"alpha".to_vec())),
            "inbox delivers to running instance"
        );
        assert!(node
            .get_bounded("loop-1")
            .is_some_and(|i| i.push_inbox(b"beta".to_vec())));
        assert!(node
            .get_bounded("loop-1")
            .is_some_and(|i| i.push_inbox(b"quit".to_vec())));
        let (outs, done) = drain(&rx, "loop-1");
        assert_eq!(outs, vec![b"got:alpha".to_vec(), b"got:beta".to_vec()]);
        assert_eq!(done.0, "ok", "terminal predicate ⇒ ok");

        // --- external stop (BoundedStop handler) → cancelled ---
        assert!(node.start_bounded("test-loop-bounded".into(), "loop-2".into(), "".into()));
        assert!(node
            .get_bounded("loop-2")
            .is_some_and(|i| i.push_inbox(b"one".to_vec())));
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
            .is_some_and(|i| i.push_inbox(b"x".to_vec())));
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
        };
        let mut errors_bad = Vec::new();
        add_bad.handle(&mut node, "", &mut errors_bad);
        assert_eq!(errors_bad.len(), 1);
        assert!(errors_bad[0].contains("BoundedOperative class"));

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
        ));
        // The bounded Segments are now reachable by wire identity — the restore
        // handler could not see them before this change (the continuous lookup
        // only scans ContinuousInstance segments).
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
    }

    /// Drain output frames until the `BoundedCompleted` for `id`, returning the
    /// collected outputs and the `(status, error, result)` completion tuple.
    fn drain(
        rx: &Receiver<ExecutorMessage>,
        id: &str,
    ) -> (Vec<Vec<u8>>, (String, String, Vec<u8>)) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut outs = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let msg = rx
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("timed out waiting for bounded {id}"));
            match msg {
                ExecutorMessage::EmitBoundedOutput {
                    operative_id,
                    payload,
                } if operative_id == id => outs.push(payload),
                ExecutorMessage::EmitBoundedCompleted {
                    operative_id,
                    status,
                    error,
                    result,
                } if operative_id == id => return (outs, (status, error, result)),
                _ => {}
            }
        }
    }

    /// Block until a specific output frame for `id` is observed (used to confirm
    /// an interactive instance is live before stopping it).
    fn wait_for_output(rx: &Receiver<ExecutorMessage>, id: &str, payload: &[u8]) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let msg = rx
                .recv_timeout(remaining)
                .unwrap_or_else(|_| panic!("timed out waiting for output from {id}"));
            if let ExecutorMessage::EmitBoundedOutput {
                operative_id,
                payload: p,
            } = msg
            {
                if operative_id == id && p == payload {
                    return;
                }
            }
        }
    }
}
