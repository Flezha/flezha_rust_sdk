//! Failure-mode probes: late-input correction, the dead-letter poison pill,
//! the dispatch wedge, resume and schema evolution.

use crate::*;

// -- CandleOperator (stream-late-input-correction e2e) -------------------
//
// A trade stream keyed by an EVENT time carried in the input itself
// ("<event_time>:<price>"), with a declared correction window. State records the
// event times in the order they were folded — which is the whole point of the
// change: a late trade must end up folded in event-time order, not arrival
// order, and the only way to prove that is to look at the order.
//
// Send 100, 300, then the late 200. Without correction the fold order is
// [100, 300, 200] and the candle is permanently wrong. With it, the engine
// rewinds behind 200, replays the retained input log sorted by event time, and
// the fold order becomes [100, 200, 300].

#[derive(Default, Serialize, Deserialize, Clone)]
struct CandleState {
    /// Event times in fold order. The assertion surface.
    folded: Vec<u64>,
}

#[derive(Serialize, Deserialize, Clone)]
enum CandleChange {
    Fold(u64),
}

impl Change<CandleState> for CandleChange {
    fn apply_to(&self, state: &mut CandleState) {
        match self {
            CandleChange::Fold(event_time) => state.folded.push(*event_time),
        }
    }
}

pub(crate) fn trade_event_time(input: &str) -> u64 {
    input
        .split_once(':')
        .map(|(ts, _)| ts)
        .unwrap_or(input)
        .parse()
        .unwrap_or(0)
}

#[derive(Default)]
struct CandleTasklet;
#[flezha_runner::tasklet(name = "candle_tasklet")]
impl Tasklet for CandleTasklet {
    type Input = String;
    type Output = String;
    type State = CandleState;
    type Change = CandleChange;
    fn event_time(input: &String) -> u64 {
        trade_event_time(input)
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        // `ctx.now()` IS the extracted event time under an event-time clock.
        let event_time = ctx.now();
        ctx.change(CandleChange::Fold(event_time));
        // One row per candle bucket, so a corrected replay overwrites by key
        // rather than appending a second row.
        let bucket = event_time / 1_000;
        ctx.emit_keyed(format!("candle/{bucket}"), format!("{bucket}={input}"))
    }
}

pub(crate) struct CandleOperator;
impl UnboundedOperator for CandleOperator {
    fn factory_id() -> &'static str {
        "candle"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.correction_window(600_000);
        c.input::<String>("input")
            .into::<CandleTasklet>()
            .keyed()
            .sink_named("sink");
    }
}

// -- PoisonProbeOperator (dead-letter-queue e2e) -------------------
//
// Tier-off retry-then-park probe: `run` fails on the input "poison" until an
// "antidote" input flips a process-global flag. The e2e parks the poison
// payload (durable DLQ entry, cell continues), sends the antidote, then
// replays the parked entry — which now succeeds and removes it.

static POISON_ANTIDOTE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[derive(Default)]
struct PoisonProbeTasklet;
#[flezha_runner::tasklet(name = "poison_probe_tasklet")]
impl Tasklet for PoisonProbeTasklet {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if input == "antidote" {
            POISON_ANTIDOTE.store(true, std::sync::atomic::Ordering::SeqCst);
        } else if input == "poison" && !POISON_ANTIDOTE.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(TaskletError::Custom("poison payload".into()));
        }
        ctx.emit(format!("ok:{input}"))
    }
}

pub(crate) struct PoisonProbeOperator;
impl UnboundedOperator for PoisonProbeOperator {
    fn factory_id() -> &'static str {
        "poison_probe"
    }
    fn channels(c: &ChannelBuilder) {
        // Small backoff keeps the park fast in the e2e; the budget stays
        // at the default 3 attempts.
        c.input::<String>("input")
            .retry_backoff_ms(25)
            .into::<PoisonProbeTasklet>()
            .sink_named("out");
    }
}

// -- WedgeProbeOperator (runner-dispatch-wedge e2e) -----------------------
//
// The `pool_wedge` fixture: the two ways a runner stops doing work, on one
// operator.
//
//   "wedge" — blocks the dispatch inside user code forever, so the SDK's
//     dispatch counter stops advancing while the heartbeat thread keeps
//     reporting the runner alive. That is the `runner_pool_wedged` signature:
//     connected, heartbeating, processing nothing. Teardown is the stdin-EOF
//     watcher.
//   "abort" — ends its activation as violently as this host allows, WITHOUT
//     emitting, once per process. Rust has no way for user code to kill a poll
//     worker outright: the dispatcher catches the unwind per item and the
//     panic rides the retry ladder. That is what the scenario asserts is
//     survivable — whatever the host does with a violent failure, the Cell
//     must be served again and the aborting item must not produce twice.
//
// Every other input emits `ok:<input>`, which is how the e2e proves the
// operator produces again once the remedy has landed.

static WEDGE_PROBE_ABORTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[derive(Default)]
struct WedgeProbeTasklet;
#[flezha_runner::tasklet(name = "wedge_probe_tasklet")]
impl Tasklet for WedgeProbeTasklet {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if input == "wedge" {
            loop {
                thread::sleep(Duration::from_secs(3600));
            }
        }
        if input == "abort" && !WEDGE_PROBE_ABORTED.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("wedge_probe: killing the worker holding this activation");
        }
        ctx.emit(format!("ok:{input}"))
    }
}

pub(crate) struct WedgeProbeOperator;
impl UnboundedOperator for WedgeProbeOperator {
    fn factory_id() -> &'static str {
        "wedge_probe"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<WedgeProbeTasklet>()
            .sink_named("out");
    }
}

// -- ResumeProbeOperator ---------------------------------------------------------
//
// Stateful counter that EMITS its running total on every input, so a Kafka
// plugin on the output makes the persisted value externally observable. Used
// by chaos/tests/stateful-resume.sh to prove WAL-backed state survives a hard
// controller kill / runner restart: after recovery the operator resumes from
// the persisted total (replayed changes), not from zero. Input is a decimal
// delta string; output is the new running total as ASCII bytes.

#[derive(Serialize, Deserialize, Clone, Default)]
struct ResumeProbeState {
    total: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum ResumeProbeChange {
    Add(i64),
}
impl Change<ResumeProbeState> for ResumeProbeChange {
    fn apply_to(&self, state: &mut ResumeProbeState) {
        match self {
            ResumeProbeChange::Add(d) => state.total = state.total.saturating_add(*d),
        }
    }
}

#[derive(Default)]
struct ResumeProbeTasklet;
#[flezha_runner::tasklet(name = "resume_probe_tasklet")]
impl Tasklet for ResumeProbeTasklet {
    type Input = String;
    type Output = Vec<u8>;
    type State = ResumeProbeState;
    type Change = ResumeProbeChange;
    fn init_state(_: Option<&str>) -> ResumeProbeState {
        // Default + change replay rebuilds the total on restore (same pattern
        // as stateful_counter); no snapshot fast-path needed.
        ResumeProbeState::default()
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let delta: i64 = input.parse().unwrap_or(0);
        let new_total = ctx.state().total.saturating_add(delta);
        ctx.change(ResumeProbeChange::Add(delta));
        ctx.emit(new_total.to_string().into_bytes())
    }
}

pub(crate) struct ResumeProbeOperator;
impl UnboundedOperator for ResumeProbeOperator {
    fn factory_id() -> &'static str {
        "resume_probe"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<ResumeProbeTasklet>()
            .sink_named("total");
    }
}

// -- SchemaEvolutionOperator -----------------------------------------------------
//
// The same class either side of a deploy that edits its own state type. Both
// halves declare the SAME tasklet name, so an operator started under v1 and
// restarted under v2 restores from the very WAL v1 wrote — which is what a
// deploy does, with the two binaries collapsed into one for the fixture.
//
// v1: `{total}`. v2: `{total, currency}` at SCHEMA_VERSION 2, reading down to
// 1 through a declared migration. Output is `total:currency` so the e2e can
// read the migrated field off the wire.

#[derive(Serialize, Deserialize, Clone, Default)]
struct EvolvingCartV1 {
    total: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum EvolvingCartChangeV1 {
    Add(i64),
}
impl Change<EvolvingCartV1> for EvolvingCartChangeV1 {
    fn apply_to(&self, state: &mut EvolvingCartV1) {
        match self {
            EvolvingCartChangeV1::Add(d) => state.total = state.total.saturating_add(*d),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
struct EvolvingCartV2 {
    total: i64,
    currency: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum EvolvingCartChangeV2 {
    Add { delta: i64, note: String },
}
impl Change<EvolvingCartV2> for EvolvingCartChangeV2 {
    fn apply_to(&self, state: &mut EvolvingCartV2) {
        match self {
            EvolvingCartChangeV2::Add { delta, .. } => {
                state.total = state.total.saturating_add(*delta)
            }
        }
    }
}

#[derive(Default)]
struct SchemaEvolutionV1Tasklet;
#[flezha_runner::tasklet(name = "evolving_cart")]
impl Tasklet for SchemaEvolutionV1Tasklet {
    type Input = String;
    type Output = Vec<u8>;
    type State = EvolvingCartV1;
    type Change = EvolvingCartChangeV1;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let delta: i64 = input.parse().unwrap_or(0);
        let total = ctx.state().total.saturating_add(delta);
        ctx.change(EvolvingCartChangeV1::Add(delta));
        ctx.emit(format!("{total}:").into_bytes())
    }
}

#[derive(Default)]
struct SchemaEvolutionV2Tasklet;
#[flezha_runner::tasklet(name = "evolving_cart")]
impl Tasklet for SchemaEvolutionV2Tasklet {
    type Input = String;
    type Output = Vec<u8>;
    type State = EvolvingCartV2;
    type Change = EvolvingCartChangeV2;
    const SCHEMA_VERSION: u32 = 2;
    const MIN_READABLE_SCHEMA_VERSION: u32 = 1;
    fn migrate_state(from_version: u32, bytes: &[u8]) -> Result<EvolvingCartV2, String> {
        match from_version {
            1 => {
                let old: EvolvingCartV1 = postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
                Ok(EvolvingCartV2 {
                    total: old.total,
                    currency: "EUR".to_string(),
                })
            }
            v => Err(format!("no migration from {v}")),
        }
    }
    fn migrate_change(from_version: u32, bytes: &[u8]) -> Result<EvolvingCartChangeV2, String> {
        match from_version {
            1 => {
                let old: EvolvingCartChangeV1 =
                    postcard::from_bytes(bytes).map_err(|e| e.to_string())?;
                let EvolvingCartChangeV1::Add(delta) = old;
                Ok(EvolvingCartChangeV2::Add {
                    delta,
                    note: "migrated".to_string(),
                })
            }
            v => Err(format!("no migration from {v}")),
        }
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let delta: i64 = input.parse().unwrap_or(0);
        let total = ctx.state().total.saturating_add(delta);
        let currency = if ctx.state().currency.is_empty() {
            "EUR".to_string()
        } else {
            ctx.state().currency.clone()
        };
        ctx.change(EvolvingCartChangeV2::Add {
            delta,
            note: "live".to_string(),
        });
        ctx.emit(format!("{total}:{currency}").into_bytes())
    }
}

pub(crate) struct SchemaEvolutionV1Operator;
impl UnboundedOperator for SchemaEvolutionV1Operator {
    fn factory_id() -> &'static str {
        "schema_evolution_v1"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<SchemaEvolutionV1Tasklet>()
            .sink_named("total");
    }
}

pub(crate) struct SchemaEvolutionV2Operator;
impl UnboundedOperator for SchemaEvolutionV2Operator {
    fn factory_id() -> &'static str {
        "schema_evolution_v2"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<SchemaEvolutionV2Tasklet>()
            .sink_named("total");
    }
}
