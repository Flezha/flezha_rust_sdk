//! Failure-mode probes: late-input correction, the dead-letter poison pill,
//! the dispatch wedge, resume and schema evolution.

use crate::*;

// -- CandleOperative (stream-late-input-correction e2e) -------------------
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

impl Change for CandleChange {}

struct TradeEventTime;
impl TimeExtractor<String> for TradeEventTime {
    fn extract(input: &String) -> u64 {
        input
            .split_once(':')
            .map(|(ts, _)| ts)
            .unwrap_or(input.as_str())
            .parse()
            .unwrap_or(0)
    }
}

#[derive(Default)]
struct CandleProcedure;
impl Procedure for CandleProcedure {
    type Input = String;
    type Output = String;
    type State = CandleState;
    type Change = CandleChange;
    type Clock = EventTimeFrom<TradeEventTime>;
    type Config = ();
    fn name() -> &'static str {
        "candle_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> CandleState {
        CandleState::default()
    }
    fn apply_change(state: &mut CandleState, change: &CandleChange) {
        match change {
            CandleChange::Fold(event_time) => state.folded.push(*event_time),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        // `ctx.now()` IS the extracted event time under an EventTimeFrom clock.
        let event_time = ctx.now();
        state.apply(CandleChange::Fold(event_time));
        // One row per candle bucket, so a corrected replay overwrites by key
        // rather than appending a second row.
        let bucket = event_time / 1_000;
        ctx.emit_keyed::<String>(format!("candle/{bucket}"), format!("{bucket}={input}"))
    }
}

pub(crate) struct CandleOperative;
impl StreamOperative for CandleOperative {
    fn factory_id() -> &'static str {
        "candle"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.correction_window(600_000);
        c.input::<String>("input")
            .into::<CandleProcedure>()
            .named("sink")
            .keyed()
            .sink("tier_sink");
    }
}

// -- PoisonProbeOperative (dead-letter-queue e2e) -------------------
//
// Tier-off retry-then-park probe: `run` fails on the input "poison" until an
// "antidote" input flips a process-global flag. The e2e parks the poison
// payload (durable DLQ entry, entity continues), sends the antidote, then
// replays the parked entry — which now succeeds and removes it.

static POISON_ANTIDOTE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[derive(Default)]
struct PoisonProbeProcedure;
impl Procedure for PoisonProbeProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "poison_probe_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        if input == "antidote" {
            POISON_ANTIDOTE.store(true, std::sync::atomic::Ordering::SeqCst);
        } else if input == "poison" && !POISON_ANTIDOTE.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ProcedureError::Custom("poison payload".into()));
        }
        ctx.emit(format!("ok:{input}"))
    }
}

pub(crate) struct PoisonProbeOperative;
impl StreamOperative for PoisonProbeOperative {
    fn factory_id() -> &'static str {
        "poison_probe"
    }
    fn channels(c: &ChannelBuilder) {
        // Small backoff keeps the park fast in the e2e; the budget stays
        // at the default 3 attempts.
        c.input::<String>("input")
            .retry_backoff_ms(25)
            .into::<PoisonProbeProcedure>()
            .named("out")
            .sink("probe_sink");
    }
}

// -- WedgeProbeOperative (runner-dispatch-wedge e2e) -----------------------
//
// The `pool_wedge` fixture: the two ways a runner stops doing work, on one
// operative.
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
//     survivable — whatever the host does with a violent failure, the Entity
//     must be served again and the aborting item must not produce twice.
//
// Every other input emits `ok:<input>`, which is how the e2e proves the
// operative produces again once the remedy has landed.

static WEDGE_PROBE_ABORTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

#[derive(Default)]
struct WedgeProbeProcedure;
impl Procedure for WedgeProbeProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "wedge_probe_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
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

pub(crate) struct WedgeProbeOperative;
impl StreamOperative for WedgeProbeOperative {
    fn factory_id() -> &'static str {
        "wedge_probe"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<WedgeProbeProcedure>()
            .named("out")
            .sink("wedge_sink");
    }
}

// -- ResumeProbeOperative ---------------------------------------------------------
//
// Stateful counter that EMITS its running total on every input, so a Kafka
// plugin on the output makes the persisted value externally observable. Used
// by chaos/tests/stateful-resume.sh to prove WAL-backed state survives a hard
// controller kill / runner restart: after recovery the operative resumes from
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
impl Change for ResumeProbeChange {}

#[derive(Default)]
struct ResumeProbeProcedure;
impl Procedure for ResumeProbeProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = ResumeProbeState;
    type Change = ResumeProbeChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "resume_probe_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> ResumeProbeState {
        // Default + change replay rebuilds the total on restore (same pattern
        // as stateful_counter); no snapshot fast-path needed.
        ResumeProbeState::default()
    }
    fn apply_change(state: &mut ResumeProbeState, change: &ResumeProbeChange) {
        match change {
            ResumeProbeChange::Add(d) => state.total = state.total.saturating_add(*d),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let delta: i64 = input.parse().unwrap_or(0);
        let new_total = state.total.saturating_add(delta);
        state.apply(ResumeProbeChange::Add(delta));
        ctx.emit::<Vec<u8>>(new_total.to_string().into_bytes())
    }
}

pub(crate) struct ResumeProbeOperative;
impl StreamOperative for ResumeProbeOperative {
    fn factory_id() -> &'static str {
        "resume_probe"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<ResumeProbeProcedure>()
            .named("total")
            .sink("resume_probe_out");
    }
}

// -- SchemaEvolutionOperative -----------------------------------------------------
//
// The same class either side of a deploy that edits its own state type. Both
// halves declare the SAME procedure name, so an operative started under v1 and
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
impl Change for EvolvingCartChangeV1 {}

#[derive(Serialize, Deserialize, Clone, Default)]
struct EvolvingCartV2 {
    total: i64,
    currency: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum EvolvingCartChangeV2 {
    Add { delta: i64, note: String },
}
impl Change for EvolvingCartChangeV2 {}

#[derive(Default)]
struct SchemaEvolutionV1Procedure;
impl Procedure for SchemaEvolutionV1Procedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = EvolvingCartV1;
    type Change = EvolvingCartChangeV1;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "evolving_cart"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> EvolvingCartV1 {
        EvolvingCartV1::default()
    }
    fn apply_change(state: &mut EvolvingCartV1, change: &EvolvingCartChangeV1) {
        match change {
            EvolvingCartChangeV1::Add(d) => state.total = state.total.saturating_add(*d),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let delta: i64 = input.parse().unwrap_or(0);
        let total = state.total.saturating_add(delta);
        state.apply(EvolvingCartChangeV1::Add(delta));
        ctx.emit::<Vec<u8>>(format!("{total}:").into_bytes())
    }
}

#[derive(Default)]
struct SchemaEvolutionV2Procedure;
impl Procedure for SchemaEvolutionV2Procedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = EvolvingCartV2;
    type Change = EvolvingCartChangeV2;
    type Clock = WallClock;
    type Config = ();
    const SCHEMA_VERSION: u32 = 2;
    const MIN_READABLE_SCHEMA_VERSION: u32 = 1;
    fn name() -> &'static str {
        "evolving_cart"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> EvolvingCartV2 {
        EvolvingCartV2::default()
    }
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
    fn apply_change(state: &mut EvolvingCartV2, change: &EvolvingCartChangeV2) {
        match change {
            EvolvingCartChangeV2::Add { delta, .. } => {
                state.total = state.total.saturating_add(*delta)
            }
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let delta: i64 = input.parse().unwrap_or(0);
        let total = state.total.saturating_add(delta);
        let currency = if state.currency.is_empty() {
            "EUR".to_string()
        } else {
            state.currency.clone()
        };
        state.apply(EvolvingCartChangeV2::Add {
            delta,
            note: "live".to_string(),
        });
        ctx.emit::<Vec<u8>>(format!("{total}:{currency}").into_bytes())
    }
}

pub(crate) struct SchemaEvolutionV1Operative;
impl StreamOperative for SchemaEvolutionV1Operative {
    fn factory_id() -> &'static str {
        "schema_evolution_v1"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<SchemaEvolutionV1Procedure>()
            .named("total")
            .sink("schema_evolution_out");
    }
}

pub(crate) struct SchemaEvolutionV2Operative;
impl StreamOperative for SchemaEvolutionV2Operative {
    fn factory_id() -> &'static str {
        "schema_evolution_v2"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<SchemaEvolutionV2Procedure>()
            .named("total")
            .sink("schema_evolution_out");
    }
}

// -- EntryIdleProbeOperative (entry-idle e2e) -------------------
//
// Makes a callback with no ctx observable. `IdleObserver` runs on the SDK's
// tick thread and cannot emit, so the observer counts episodes into process
// globals and a procedure reports them.
//
// Two entry channels fanning into ONE procedure, and the split is
// load-bearing. `flz_core` interns one observation cell per channel NAME — one
// name is one observed input however many classes declare it — so a frame on
// `idle_report` leaves `idle_input`'s stamp untouched, and the e2e can poll for
// an episode without ending the silence it is waiting for. That same rule is
// why the watched channel is not called `input`: twenty-five operatives in this
// app declare that name and would share its cell.
//
// Nothing counts deliveries, because an episode already proves one: a cold
// start never fires, so an episode can only follow a frame on `idle_input`.

pub(crate) const ENTRY_IDLE_WATCHED: &str = "idle_input";

/// The tick is a quarter of this, clamped to `[250 ms, 1 s]`, so an episode is
/// reported within 250 ms of the threshold passing.
const ENTRY_IDLE_THRESHOLD: Duration = Duration::from_millis(1_000);

static ENTRY_IDLE_EPISODES: AtomicUsize = AtomicUsize::new(0);
static ENTRY_IDLE_RESUMES: AtomicUsize = AtomicUsize::new(0);
static ENTRY_IDLE_ABSENT_AT_BOOT: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

struct EntryIdleCounter;
impl IdleObserver for EntryIdleCounter {
    fn idle(&self, _gap: Duration) {
        ENTRY_IDLE_EPISODES.fetch_add(1, Ordering::SeqCst);
    }
    fn resumed(&self, _gap: Duration) {
        ENTRY_IDLE_RESUMES.fetch_add(1, Ordering::SeqCst);
    }
}

/// Read the accessor before `run()`, then watch the channel. The stamp is taken
/// ahead of anything that can decline a frame, so "absent before the first
/// frame" is observable here and nowhere inside a procedure.
pub(crate) fn arm_entry_idle_probe(runner: &FlezhaRunner) {
    ENTRY_IDLE_ABSENT_AT_BOOT.store(
        runner.last_entry_delivery(ENTRY_IDLE_WATCHED).is_none(),
        Ordering::SeqCst,
    );
    runner.on_entry_idle(ENTRY_IDLE_WATCHED, ENTRY_IDLE_THRESHOLD, EntryIdleCounter);
}

#[derive(Default)]
struct EntryIdleProbeProcedure;
impl Procedure for EntryIdleProbeProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "entry_idle_probe_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!(
            "{}:{}:{}",
            ENTRY_IDLE_ABSENT_AT_BOOT.load(Ordering::SeqCst),
            ENTRY_IDLE_EPISODES.load(Ordering::SeqCst),
            ENTRY_IDLE_RESUMES.load(Ordering::SeqCst),
        ))
    }
}

pub(crate) struct EntryIdleProbeOperative;
impl StreamOperative for EntryIdleProbeOperative {
    fn factory_id() -> &'static str {
        "entry_idle_probe"
    }
    fn channels(c: &ChannelBuilder) {
        // Both channels wire the same unnamed class: one node, fan-in.
        c.input::<String>(ENTRY_IDLE_WATCHED)
            .into::<EntryIdleProbeProcedure>();
        c.input::<String>("idle_report")
            .into::<EntryIdleProbeProcedure>()
            .named("report")
            .sink("entry_idle_report");
    }
}
