//! Durable-loop steps fixtures — the chat loop with suspension, its
//! stateless twin and the rewindable variant — and the chains and loop
//! `mix bench.steps` drives.

use flezha_runner::tasklet::SpawnOptions;
use flezha_runner::Ctx;

use crate::steps::{
    block_until_released, durable_step, e2e_released, stateless_step, Insert, SeenState,
};
use crate::*;

// `chat_loop_steps`: the durable-loop + suspension shape
// (`steps-durable-loop-and-suspension`). One iteration per inbox item: task
// TURN commits the turn to Cell state (so a resumed iteration is visible in
// the WAL) and emits it; task REPLY emits the answer to the output. Between turns
// the loop finds an empty inbox at a committed boundary and PARKS — the
// instance is evicted from the runner and rehydrated by the next message.
//
// The turn whose payload is `mid-crash` BLOCKS while the release env is unset,
// so a SIGKILL lands with the earlier iterations durable and that one in flight.
// The resumed runner (release env set) runs it through. Keying the block on the
// payload — rather than an invocation count — keeps it deterministic across the
// park/rehydrate cycles, which re-run the driver but not the turns. The turn
// `bye` records nothing, still reaches REPLY, and completes the instance with
// the number of turns TURN holds, so the session ends once that iteration
// commits.
const CRASH_TURN: &str = "mid-crash";
const BYE_TURN: &str = "bye";

#[derive(Default)]
struct ChatTurn;
#[flezha_runner::tasklet(name = "chat_turn")]
impl Tasklet for ChatTurn {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if input == CRASH_TURN {
            block_until_released();
        }
        if input == BYE_TURN {
            ctx.complete(ctx.state().items.len().to_string())?;
        } else {
            // Keyed by the turn, so the WAL record count is the number of
            // DISTINCT turns that ran — a re-run of a committed iteration
            // would show up.
            ctx.change(Insert(input.clone()));
        }
        ctx.emit(input)
    }
}

stateless_step!(ChatReply, "chat_reply", |input, ctx| ctx
    .emit(format!("reply:{input}")));

// -- StatelessLoopSteps -------------------------------------------------------
//
// The same durable PER_INPUT loop as `ChatLoopSteps`, minus its stateful
// first tasklet: `ChatReply` alone holds NO per-Cell state, so its Cell's
// records carry its outputs and nothing to restore. The instance's
// task-journal therefore reaches the driver only if it rides the bind itself:
// deliver it on a restore frame alone and every turn after the first commits
// at iteration 0 again, colliding with iteration 0's entries.

pub(crate) struct StatelessLoopSteps;
impl Relay for StatelessLoopSteps {
    fn factory_id() -> &'static str {
        "stateless_loop_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatReply>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// -- KeyedLoopSteps -------------------------------------------------------
//
// `keyed_loop_steps`: a durable PER_INPUT loop keyed on its entry edge by
// the text before the first `:`, so one Cell counts one symbol's messages. The
// shape a partition group of steps relay members takes: each member mints Cells
// for its own key groups only, and a rescale moves them with their counts.

#[derive(Default)]
struct KeyedLoopCount;
#[flezha_runner::tasklet(name = "keyed_loop_count")]
impl Tasklet for KeyedLoopCount {
    type Input = String;
    type Output = String;
    type State = CounterState;
    type Change = CounterChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let symbol = key_prefix(&input);
        ctx.change(CounterChange { delta: 1 });
        ctx.emit(format!("{symbol}:{}", ctx.state().value))
    }
}

pub(crate) struct KeyedLoopSteps;
impl Relay for KeyedLoopSteps {
    fn factory_id() -> &'static str {
        "keyed_loop_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("trades")
            .key_by(|trade: &String| key_prefix(trade))
            .into::<KeyedLoopCount>()
            .output_named("keyed_loop_steps_out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// -- SpawnRequesterRelay -----------------------------------------------------
//
// `spawn_requester`: on each input it spawns `stateless_loop_steps` once with
// no id and once under the input as its id, and prints both results, because a
// handle's answer is observable nowhere but in the tasklet that read it.

#[derive(Default)]
struct SpawnRequester;
#[flezha_runner::tasklet(name = "spawn_requester")]
impl Tasklet for SpawnRequester {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let unnamed = ctx.spawn_steps(StatelessLoopSteps::factory_id(), input.clone());
        let named = ctx.spawn_steps_with(
            StatelessLoopSteps::factory_id(),
            input.clone(),
            SpawnOptions::id(input),
        );
        for (label, handle) in [("unnamed", unnamed), ("named", named)] {
            match handle.wait() {
                Ok(result) => println!(
                    "spawn_result {label} {} {}",
                    result.id,
                    result.outcome.as_wire()
                ),
                Err(e) => println!("spawn_result {label} error {e}"),
            }
        }
        Ok(())
    }
}

pub(crate) struct SpawnRequesterRelay;
impl Relay for SpawnRequesterRelay {
    fn factory_id() -> &'static str {
        "spawn_requester"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SpawnRequester>()
            .output_named("out");
    }
}

// -- SpawnHoldRequesterRelay ---------------------------------------------------
//
// `spawn_hold_requester`: an effectively-once stream whose tasklet spawns
// `stateless_loop_steps` with no id, prints the answer as
// `spawn_result hold <id> <outcome>`, then blocks until the release env is set,
// so a runner killed there dies with the spawn answered and its input not
// durably consumed.

#[derive(Default)]
struct SpawnHoldRequester;
#[flezha_runner::tasklet(name = "spawn_hold_requester")]
impl Tasklet for SpawnHoldRequester {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        match ctx
            .spawn_steps(StatelessLoopSteps::factory_id(), input)
            .wait()
        {
            Ok(result) => println!(
                "spawn_result hold {} {}",
                result.id,
                result.outcome.as_wire()
            ),
            Err(e) => println!("spawn_result hold error {e}"),
        }
        block_until_released();
        Ok(())
    }
}

pub(crate) struct SpawnHoldRequesterRelay;
impl Relay for SpawnHoldRequesterRelay {
    fn factory_id() -> &'static str {
        "spawn_hold_requester"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .on_full(OverflowPolicy::DropNothing)
            .into::<SpawnHoldRequester>()
            .keyed()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Flow {
                exactly: Exactly::effectively_once(),
            },
            ..RelaySettings::default()
        }
    }
}

// -- SpawnOnceRequesterRelay / TypedOnceSteps --------------------------------
//
// `spawn_once_requester`: on each input it spawns `typed_once_steps` once
// with `{"text": <input>}` as the payload and prints the handle's answer as
// `spawn_result once <id> <outcome>`. `typed_once_steps` is the `once`
// shape whose entry channel `order` decodes that JSON into a `Ticket`; its one
// tasklet prints `once_ran <text>`, emits `placed:<text>` to its output and
// completes with the text, so the pass ends on the payload it was spawned
// with (KB-381).

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct Ticket {
    text: String,
}

#[derive(Default)]
struct SpawnOnceRequester;
#[flezha_runner::tasklet(name = "spawn_once_requester")]
impl Tasklet for SpawnOnceRequester {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let ticket = serde_json::to_string(&Ticket { text: input }).expect("a ticket serializes");
        match ctx.spawn_steps(TypedOnceSteps::factory_id(), ticket).wait() {
            Ok(result) => println!(
                "spawn_result once {} {}",
                result.id,
                result.outcome.as_wire()
            ),
            Err(e) => println!("spawn_result once error {e}"),
        }
        Ok(())
    }
}

pub(crate) struct SpawnOnceRequesterRelay;
impl Relay for SpawnOnceRequesterRelay {
    fn factory_id() -> &'static str {
        "spawn_once_requester"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SpawnOnceRequester>()
            .output_named("out");
    }
}

#[derive(Default)]
struct OncePlaced;
#[flezha_runner::tasklet(name = "once_placed")]
impl Tasklet for OncePlaced {
    type Input = Ticket;
    type Output = String;
    fn run(&mut self, ticket: Ticket, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        println!("once_ran {}", ticket.text);
        ctx.complete(ticket.text.clone())?;
        ctx.emit(format!("placed:{}", ticket.text))
    }
}

pub(crate) struct TypedOnceSteps;
impl Relay for TypedOnceSteps {
    fn factory_id() -> &'static str {
        "typed_once_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<Ticket>("order")
            .into::<OncePlaced>()
            .output_named("placed");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// -- ReplyBlockLoopSteps -----------------------------------------------------
//
// `reply_block_loop_steps`: the chat loop with the block moved AFTER the
// root's commit (TURN commits its turn, REPLY blocks on the turn
// `reply-crash` while the release env is unset), so a SIGKILL lands between a
// root's `task_committed` and the pass's `iter_committed`, the shape that
// asks whether the re-forwarded message drives one iteration or two.
const REPLY_CRASH_TURN: &str = "reply-crash";

stateless_step!(BlockingReply, "blocking_reply", |input, ctx| {
    if input == REPLY_CRASH_TURN {
        block_until_released();
    }
    ctx.emit(format!("reply:{input}"))
});

pub(crate) struct ReplyBlockLoopSteps;
impl Relay for ReplyBlockLoopSteps {
    fn factory_id() -> &'static str {
        "reply_block_loop_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatTurn>()
            .named("mid")
            .into::<BlockingReply>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// -- ChatRewindSteps ----------------------------------------------------------
//
// `steps-loop-rewind`: the same chat loop, but its replying tasklet can be
// undone. The undo handle lives in Cell State — a rewind's compensation is
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
impl Change<ReplyLog> for ReplyChange {
    fn apply_to(&self, state: &mut ReplyLog) {
        match self {
            ReplyChange::Said(turn) => state.said.push(turn.clone()),
            ReplyChange::Retracted => {
                state.said.pop();
            }
        }
    }
}

#[derive(Default)]
struct ChatReplyUndoable;
#[flezha_runner::tasklet(name = "chat_reply_undoable")]
impl Tasklet for ChatReplyUndoable {
    type Input = String;
    type Output = String;
    type State = ReplyLog;
    type Change = ReplyChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(ReplyChange::Said(input.clone()));
        ctx.emit(format!("reply:{input}"))
    }
    fn compensate(
        &mut self,
        _completed_output: Vec<std::sync::Arc<String>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        let Some(turn) = ctx.state().said.last().cloned() else {
            return Ok(());
        };
        ctx.change(ReplyChange::Retracted);
        ctx.emit(format!("retract:{turn}"))
    }
}

pub(crate) struct ChatRewindSteps;
impl Relay for ChatRewindSteps {
    fn factory_id() -> &'static str {
        "chat_rewind_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatTurn>()
            .named("mid")
            .into::<ChatReplyUndoable>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

pub(crate) struct ChatLoopSteps;
impl Relay for ChatLoopSteps {
    fn factory_id() -> &'static str {
        "chat_loop_steps"
    }
    // A chat session ends itself on `bye`; the duration backstop keeps a
    // stray e2e instance from outliving the test run.
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatTurn>()
            .named("mid")
            .into::<ChatReply>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// -- FailingTurnLoopSteps -------------------------------------------------------
//
// `failing_turn_loop_steps`: a session whose turn can fail after `note`
// committed. `note` records each message in its state and its undo moves the
// latest one to `undone`; `answer` replies `re:<message>` on the output `answer`.
// `answer` fails every attempt on `boom`, on `boom-once` while the release env
// is unset (so it fails on the first runner of a crash drill only), and on
// `boom-until-healed` until a `heal` turn set its state.
// `failing_turn_undo_all_steps` is the same under all-or-nothing.

#[derive(Serialize, Deserialize, Clone, Default)]
struct NoteLog {
    notes: Vec<String>,
    undone: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
enum NoteChange {
    Noted(String),
    Undone,
}
impl Change<NoteLog> for NoteChange {
    fn apply_to(&self, state: &mut NoteLog) {
        match self {
            NoteChange::Noted(message) => state.notes.push(message.clone()),
            NoteChange::Undone => {
                if let Some(message) = state.notes.pop() {
                    state.undone.push(message);
                }
            }
        }
    }
}

#[derive(Default)]
struct Note;
#[flezha_runner::tasklet(name = "note")]
impl Tasklet for Note {
    type Input = String;
    type Output = String;
    type State = NoteLog;
    type Change = NoteChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(NoteChange::Noted(input.clone()));
        ctx.emit(input)
    }
    fn compensate(
        &mut self,
        _completed_output: Vec<std::sync::Arc<String>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        ctx.change(NoteChange::Undone);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct Healed {
    healed: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct Heal;
impl Change<Healed> for Heal {
    fn apply_to(&self, state: &mut Healed) {
        state.healed = true;
    }
}

#[derive(Default)]
struct Answer;
#[flezha_runner::tasklet(name = "answer")]
impl Tasklet for Answer {
    type Input = String;
    type Output = String;
    type State = Healed;
    type Change = Heal;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let fails = match input.as_str() {
            "boom" => true,
            "boom-once" => !e2e_released(),
            "boom-until-healed" => !ctx.state().healed,
            _ => false,
        };
        if fails {
            return Err(TaskletError::Custom(format!("answer refused {input}")));
        }
        if input == "heal" {
            ctx.change(Heal);
        }
        ctx.emit(format!("re:{input}"))
    }
}

macro_rules! failing_turn_loop {
    ($ty:ident, $factory:literal, $rollback:expr) => {
        pub(crate) struct $ty;
        impl Relay for $ty {
            fn factory_id() -> &'static str {
                $factory
            }
            fn channels(c: &ChannelBuilder) {
                c.input::<String>("in")
                    .into::<Note>()
                    .named("noted")
                    .into::<Answer>()
                    .output_named("answer");
            }

            fn settings() -> RelaySettings {
                RelaySettings {
                    run: Run::Steps {
                        each: Each::PerInput,
                        on_failure: $rollback,
                    },
                    max_duration: Some(Duration::from_secs(600)),
                    durable: fixture_durable(),
                    ..RelaySettings::default()
                }
            }
        }
    };
}

failing_turn_loop!(
    FailingTurnLoopSteps,
    "failing_turn_loop_steps",
    OnFailure::KeepProgress
);
failing_turn_loop!(
    FailingTurnUndoAllSteps,
    "failing_turn_undo_all_steps",
    OnFailure::UndoAll
);

// -- Bench steps fixtures (`mix bench.steps`) -------------------------------
//
// Durable K-tasklet chains and a long-lived chat loop for the steps relay
// relay bench. Every chain step is the `durable_step!` shape — one
// idempotent Change per task, the payload passed through unchanged — so the
// output receives the seed input and the bench reads its own seq + send
// timestamp back. The loop is the `chat_loop_steps` shape with a bounded
// per-turn state and a day-long duration backstop (soaks outlive 600 s); with
// `FLEZHA_BENCH_WAKE_AFTER_MS` set each turn arms a timer that far out, whose
// firing sends the bench's wake marker naming its scheduled time, for the
// timer-wake measurements.

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

// A linear chain `in → $first → $rest… → bench_sink`, every inter-task edge
// codec'd (required for a durable structured class).
macro_rules! bench_chain {
    ($ty:ident, $factory:literal, persist: $persist:expr, $first:ident $(, $rest:ident)*) => {
        pub(crate) struct $ty;
        impl Relay for $ty {
            fn factory_id() -> &'static str {
                $factory
            }
            fn channels(c: &ChannelBuilder) {
                let s = c.input::<String>("in").into::<$first>();
                $(
                    let s = s
                        .named(concat!("edge_", stringify!($rest)))
                        .into::<$rest>();
                )*
                s.output_named("out");
            }

            fn settings() -> RelaySettings {
                RelaySettings {
                    run: Run::steps(Each::Once),
                    durable: if $persist { Durable::local() } else { Durable::Memory },
                    ..RelaySettings::default()
                }
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
impl Change<BenchTurnState> for BenchTurnSeen {
    fn apply_to(&self, state: &mut BenchTurnState) {
        state.turns += 1;
        state.last = self.0.clone();
    }
}

#[derive(Default)]
struct BenchTurn;
#[flezha_runner::tasklet(name = "bench_turn")]
impl Tasklet for BenchTurn {
    type Input = String;
    type Output = String;
    type State = BenchTurnState;
    type Change = BenchTurnSeen;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(BenchTurnSeen(input.clone()));
        if let Some(after) = env::var("FLEZHA_BENCH_WAKE_AFTER_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
        {
            ctx.timer_after(Duration::from_millis(after))?;
        }
        ctx.emit(input)
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        _kind: flezha_runner::state::TimerKind,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        ctx.emit(format!("wake:{fired_at}"))
    }
}

pub(crate) struct BenchLoopSteps;
impl Relay for BenchLoopSteps {
    fn factory_id() -> &'static str {
        "bench_loop_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<BenchTurn>()
            .named("mid")
            .into::<ChatReply>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::steps(Each::PerInput),
            max_duration: Some(Duration::from_secs(24 * 3600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// -- ChainedSteps ---------------------------------------------------------------
//
// A durable chained loop: each pass prints `chained_pass <n>` and hands the
// next pass `n - 1` on its own channel; the pass at zero hands nothing, and
// the empty batch ends the instance.

stateless_step!(ChainedCountdown, "chained_countdown", |input, ctx| {
    println!("chained_pass {input}");
    match input.parse::<u32>().unwrap_or(0) {
        0 => Ok(()),
        n => ctx.emit((n - 1).to_string()),
    }
});

pub(crate) struct ChainedSteps;
impl Relay for ChainedSteps {
    fn factory_id() -> &'static str {
        "chained_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChainedCountdown>()
            .named("carry");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::steps(Each::Chained),
            ..RelaySettings::default()
        }
    }
}
