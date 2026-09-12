//! Durable-loop workflow fixtures — the chat loop with suspension, its
//! stateless twin and the rewindable variant — and the chains and loop
//! `mix bench.workflow` drives.

use crate::workflow::{block_until_released, durable_step, Insert, SeenState};
use crate::*;

// `chat_loop_workflow`: the durable-loop + suspension shape
// (`workflow-durable-loop-and-suspension`). One iteration per inbox item: proc
// TURN commits the turn to Entity state (so a resumed iteration is visible in
// the WAL) and emits it; proc REPLY emits the answer to the sink. Between turns
// the loop finds an empty inbox at a committed boundary and PARKS — the
// instance is evicted from the runner and rehydrated by the next message.
//
// The turn whose payload is `mid-crash` BLOCKS while the release env is unset,
// so a SIGKILL lands with the earlier iterations durable and that one in flight.
// The resumed runner (release env set) runs it through. Keying the block on the
// payload — rather than an invocation count — keeps it deterministic across the
// park/rehydrate cycles, which re-run the driver but not the turns.
const CRASH_TURN: &str = "mid-crash";

#[derive(Default)]
struct ChatTurn;
impl Procedure for ChatTurn {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "chat_turn"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SeenState {
        SeenState::default()
    }
    fn apply_change(state: &mut SeenState, change: &Insert) {
        state.items.insert(change.0.clone());
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        if input == CRASH_TURN {
            block_until_released();
        }
        // Keyed by the turn, so the WAL record count is the number of DISTINCT
        // turns that ran — a re-run of a committed iteration would show up.
        state.apply(Insert(input.clone()));
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
pub(crate) struct ChatReply;
impl Procedure for ChatReply {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "chat_reply"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<String>(format!("reply:{input}"))
    }
}

// -- StatelessLoopWorkflow -------------------------------------------------------
//
// The same durable PER_INPUT loop as `ChatLoopWorkflow`, minus its stateful
// first procedure: `ChatReply` alone holds NO per-Entity state, so nothing
// writes an Entity WAL, the controller has nothing to restore, and it ships no
// `RestoreInitState` on a rehydrating bind. The instance's proc-journal therefore
// reaches the driver only if it rides the bind itself — deliver it on the
// restore frame and every turn after the first commits at iteration 0 again,
// collides with iteration 0's `outbox_flushed` marker, and has its staged
// output journaled but never delivered.

pub(crate) struct StatelessLoopWorkflow;
impl WorkflowOperative for StatelessLoopWorkflow {
    fn factory_id() -> &'static str {
        "stateless_loop_workflow"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(600))
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatReply>()
            .named("out")
            .sink("chat_sink");
    }
}

// -- ChatRewindWorkflow ----------------------------------------------------------
//
// `workflow-loop-rewind`: the same chat loop, but its replying procedure can be
// undone. The undo handle lives in Entity State — a rewind's compensation is
// rebuilt from the journal and is handed NO retained outputs (the at-least-once
// author contract) — so the hook reads the last thing it said, retracts it, and
// pops it. Unwinding in reverse iteration order therefore retracts the turns in
// reverse.

#[derive(Serialize, Deserialize, Clone, Default)]
struct ReplyLog {
    said: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
enum ReplyChange {
    Said(String),
    Retracted,
}
impl Change for ReplyChange {}

#[derive(Default)]
struct ChatReplyUndoable;
impl Procedure for ChatReplyUndoable {
    type Input = String;
    type Output = String;
    type State = ReplyLog;
    type Change = ReplyChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "chat_reply_undoable"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> ReplyLog {
        ReplyLog::default()
    }
    fn apply_change(state: &mut ReplyLog, change: &ReplyChange) {
        match change {
            ReplyChange::Said(turn) => state.said.push(turn.clone()),
            ReplyChange::Retracted => {
                state.said.pop();
            }
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(ReplyChange::Said(input.clone()));
        ctx.emit::<String>(format!("reply:{input}"))
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        _completed_output: Vec<std::sync::Arc<String>>,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let Some(turn) = state.said.last().cloned() else {
            return Ok(());
        };
        state.apply(ReplyChange::Retracted);
        ctx.emit::<String>(format!("retract:{turn}"))
    }
}

pub(crate) struct ChatRewindWorkflow;
impl WorkflowOperative for ChatRewindWorkflow {
    fn factory_id() -> &'static str {
        "chat_rewind_workflow"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(600))
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatTurn>()
            .named("mid")
            .into::<ChatReplyUndoable>()
            .named("out")
            .sink("chat_sink");
    }
}

pub(crate) struct ChatLoopWorkflow;
impl WorkflowOperative for ChatLoopWorkflow {
    fn factory_id() -> &'static str {
        "chat_loop_workflow"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    // A chat session ends on an external stop, not on its own; the duration
    // backstop keeps a stray e2e instance from outliving the test run.
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(600))
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatTurn>()
            .named("mid")
            .into::<ChatReply>()
            .named("out")
            .sink("chat_sink");
    }
}

// -- Bench workflow fixtures (`mix bench.workflow`) -------------------------------
//
// Durable K-procedure chains and a long-lived chat loop for the workflow
// operative bench. Every chain step is the `durable_step!` shape — one
// idempotent Change per proc, the payload passed through unchanged — so the
// sink receives the seed input and the bench reads its own seq + send
// timestamp back. The loop is the `chat_loop_workflow` shape with a bounded
// per-turn state, a day-long duration backstop (soaks outlive 600 s) and a
// wake deadline taken from `FLEZHA_BENCH_WAKE_AFTER_MS` for the timer-wake
// measurements.

durable_step!(BenchS0, "bench_s0");
durable_step!(BenchS1, "bench_s1");
durable_step!(BenchS2, "bench_s2");
durable_step!(BenchS3, "bench_s3");
durable_step!(BenchS4, "bench_s4");
durable_step!(BenchS5, "bench_s5");
durable_step!(BenchS6, "bench_s6");
durable_step!(BenchS7, "bench_s7");
durable_step!(BenchS8, "bench_s8");
durable_step!(BenchS9, "bench_s9");
durable_step!(BenchS10, "bench_s10");
durable_step!(BenchS11, "bench_s11");
durable_step!(BenchS12, "bench_s12");
durable_step!(BenchS13, "bench_s13");
durable_step!(BenchS14, "bench_s14");
durable_step!(BenchS15, "bench_s15");

// A linear chain `in → $first → $rest… → bench_sink`, every inter-proc edge
// codec'd (required for a durable structured class).
macro_rules! bench_chain {
    ($ty:ident, $factory:literal, persist: $persist:expr, $first:ident $(, $rest:ident)*) => {
        pub(crate) struct $ty;
        impl WorkflowOperative for $ty {
            const PERSISTENCE_ENABLED: bool = $persist;
            fn factory_id() -> &'static str {
                $factory
            }
            fn procedures(c: &ChannelBuilder) {
                let s = c.input::<String>("in").into::<$first>();
                $(
                    let s = s
                        .named(concat!("edge_", stringify!($rest)))
                        .into::<$rest>();
                )*
                s.named("out").sink("bench_sink");
            }
        }
    };
}

bench_chain!(BenchChain1, "bench_chain_1", persist: true, BenchS0);
bench_chain!(BenchChain4, "bench_chain_4", persist: true, BenchS0, BenchS1, BenchS2, BenchS3);
bench_chain!(
    BenchChain16,
    "bench_chain_16",
    persist: true,
    BenchS0,
    BenchS1,
    BenchS2,
    BenchS3,
    BenchS4,
    BenchS5,
    BenchS6,
    BenchS7,
    BenchS8,
    BenchS9,
    BenchS10,
    BenchS11,
    BenchS12,
    BenchS13,
    BenchS14,
    BenchS15
);
bench_chain!(
    BenchChainOptOut4,
    "bench_chain_optout_4",
    persist: false,
    BenchS0,
    BenchS1,
    BenchS2,
    BenchS3
);

// One turn's durable state: how many turns ran and the last one — bounded,
// so a million-turn soak snapshots the same few bytes at every iteration.
#[derive(Serialize, Deserialize, Clone, Default)]
struct BenchTurnState {
    turns: u64,
    last: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct BenchTurnSeen(String);
impl Change for BenchTurnSeen {}

#[derive(Default)]
struct BenchTurn;
impl Procedure for BenchTurn {
    type Input = String;
    type Output = String;
    type State = BenchTurnState;
    type Change = BenchTurnSeen;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "bench_turn"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> BenchTurnState {
        BenchTurnState::default()
    }
    fn apply_change(state: &mut BenchTurnState, change: &BenchTurnSeen) {
        state.turns += 1;
        state.last = change.0.clone();
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(BenchTurnSeen(input.clone()));
        ctx.emit::<String>(input)
    }
}

pub(crate) struct BenchLoopWorkflow;
impl WorkflowOperative for BenchLoopWorkflow {
    fn factory_id() -> &'static str {
        "bench_loop_workflow"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(24 * 3600))
    }
    fn wake_after() -> Option<Duration> {
        env::var("FLEZHA_BENCH_WAKE_AFTER_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<BenchTurn>()
            .named("mid")
            .into::<ChatReply>()
            .named("out")
            .sink("bench_sink");
    }
}
