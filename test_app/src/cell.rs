//! Unbounded fixtures distinguished by cell shape: dynamic cells, keyed
//! relays, session timeouts, static fan-out, the two pass-through classes and
//! the four-stage stress pipeline.

use crate::*;

// -- DynamicCellOperator ----------------------------------------------------
//
// Dynamic-mode operator shared by the e2e correctness test and `mix bench
// --shape dynamic`. Key = text before the first ':' (`"drop"` → None, drop).
// Eviction TTL is read from `DYN_TEST_TTL_MS` (the e2e sets 200ms to exercise
// reap + delete; the bench leaves it unset → no eviction, pure routing).

#[derive(Serialize, Deserialize, Clone, Default)]
struct DynState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum DynChange {
    Push(String),
}
impl Change<DynState> for DynChange {
    fn apply_to(&self, state: &mut DynState) {
        let DynChange::Push(s) = self;
        state.seen.push(s.clone());
    }
}

#[derive(Default)]
struct DynamicCellTasklet;
#[flezha_runner::tasklet(name = "dynamic_cell_tasklet")]
impl Tasklet for DynamicCellTasklet {
    type Input = String;
    type Output = Vec<u8>;
    type State = DynState;
    type Change = DynChange;
    fn cell_idle_ttl() -> Option<Duration> {
        env::var("DYN_TEST_TTL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Duration::from_millis)
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(DynChange::Push(input.clone()));
        ctx.emit(input.into_bytes())
    }
}

/// The text before the first `:`, the key every keyed string fixture routes on.
pub(crate) fn key_prefix(value: &str) -> String {
    value.split(':').next().unwrap_or_default().to_string()
}

pub(crate) struct DynamicCellOperator;
impl UnboundedOperator for DynamicCellOperator {
    fn factory_id() -> &'static str {
        "dynamic_cell"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .on_full(OverflowPolicy::DropNothing)
            .filter(|input: &String| input != "drop")
            .key_by(|input: &String| key_prefix(input))
            .into::<DynamicCellTasklet>()
            .sink_named("kafka");
    }
}

// -- PartitionRelayOperator --------------------------------------------------
//
// The partitioned-operators e2e's upstream: keyed on the wiring (so it can be
// partitioned at all) AND emitting keyed (so a partitioned forward target can
// resolve the owning member). Key = text before the first ':', the same
// convention as `dynamic_cell`, so one fixture's key groups line up with the
// other's.

#[derive(Serialize, Deserialize, Clone, Default)]
struct RelayState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum RelayChange {
    Push(String),
}
impl Change<RelayState> for RelayChange {
    fn apply_to(&self, state: &mut RelayState) {
        let RelayChange::Push(s) = self;
        state.seen.push(s.clone());
    }
}

#[derive(Default)]
struct PartitionRelayTasklet;
#[flezha_runner::tasklet(name = "partition_relay_tasklet")]
impl Tasklet for PartitionRelayTasklet {
    type Input = String;
    type Output = String;
    type State = RelayState;
    type Change = RelayChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let key = key_prefix(&input);
        ctx.change(RelayChange::Push(input.clone()));
        ctx.emit_keyed(key, input)
    }
}

pub(crate) struct PartitionRelayOperator;
impl UnboundedOperator for PartitionRelayOperator {
    fn factory_id() -> &'static str {
        "partition_relay"
    }
    fn channels(c: &ChannelBuilder) {
        // The plugin channel is named `input` because a forwarded frame keeps
        // its producing channel's name in metadata, and that is what selects
        // the receiving operator's channel — the downstream fixture's entry
        // channel. The entry channel here is `in` so the two names cannot
        // collide inside this class.
        c.input::<String>("in")
            .key_by(|input: &String| key_prefix(input))
            .into::<PartitionRelayTasklet>()
            .keyed()
            .sink_named("input");
    }
}

// -- SessionTimeoutOperator --------------------------------------------------
//
// The worked session-timeout shape (`cell-timers`): each input pushes the
// idle deadline out, and the firing closes the session. Single-cell so the
// e2e's crash-resume half exercises a durable Cell on both runtimes. The
// timeout is read from `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).

#[derive(Serialize, Deserialize, Clone, Default)]
struct SessionState {
    touched: Vec<String>,
    closed_at: Vec<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum SessionChange {
    Touched(String),
    Closed(u64),
}
impl Change<SessionState> for SessionChange {
    fn apply_to(&self, state: &mut SessionState) {
        match self {
            SessionChange::Touched(s) => state.touched.push(s.clone()),
            SessionChange::Closed(at) => state.closed_at.push(*at),
        }
    }
}

fn session_timeout_ms() -> u64 {
    env::var("TIMER_TEST_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000)
}

#[derive(Default)]
struct SessionTimeoutTasklet;
#[flezha_runner::tasklet(name = "session_timeout_tasklet")]
impl Tasklet for SessionTimeoutTasklet {
    type Input = String;
    type Output = Vec<u8>;
    type State = SessionState;
    type Change = SessionChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(SessionChange::Touched(input));
        // A processing-time timer, because the answer to "nothing has arrived"
        // cannot be an event-time one: nothing would advance the clock.
        ctx.timer_after(Duration::from_millis(session_timeout_ms()))?;
        Ok(())
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        _kind: flezha_runner::state::TimerKind,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        ctx.change(SessionChange::Closed(fired_at));
        ctx.emit(format!("closed@{fired_at}").into_bytes())
    }
}

pub(crate) struct SessionTimeoutOperator;
impl UnboundedOperator for SessionTimeoutOperator {
    fn factory_id() -> &'static str {
        "session_timeout"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<SessionTimeoutTasklet>()
            .sink_named("kafka");
    }
}

// -- DoNothingOperator -----------------------------------------------------------
//
// Echoes the input bytes on the plugin-bound `echo` channel. Stateless;
// inherits `Change = NoChange` so the runtime skips flush-buffer registration.

#[derive(Default)]
pub(crate) struct DoNothingTasklet;
#[flezha_runner::tasklet(name = "do_nothing_tasklet")]
impl Tasklet for DoNothingTasklet {
    type Input = String;
    type Output = Vec<u8>;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.into_bytes())
    }
}

pub(crate) struct DoNothingOperator;
impl UnboundedOperator for DoNothingOperator {
    fn factory_id() -> &'static str {
        "do_nothing"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<DoNothingTasklet>()
            .sink_named("echo");
    }
}

// -- StoreInputOperator ----------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct StoreInputState {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum StoreInputChange {
    PushText(String),
}
impl Change<StoreInputState> for StoreInputChange {
    fn apply_to(&self, state: &mut StoreInputState) {
        match self {
            StoreInputChange::PushText(s) => state.text_inputs.push(s.clone()),
        }
    }
}

#[derive(Default)]
struct StoreInputTasklet;
#[flezha_runner::tasklet(name = "store_input_tasklet")]
impl Tasklet for StoreInputTasklet {
    type Input = String;
    type Output = String;
    type State = StoreInputState;
    type Change = StoreInputChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(StoreInputChange::PushText(input.clone()));
        ctx.emit(input)
    }
}

pub(crate) struct StoreInputOperator;
impl UnboundedOperator for StoreInputOperator {
    fn factory_id() -> &'static str {
        "store_input"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<StoreInputTasklet>()
            .sink_named("sink");
    }
}

// -- CompareFilterOperator -------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct CompareFilterState {
    comparable: String,
    inverted: bool,
}

impl Default for CompareFilterState {
    fn default() -> Self {
        Self {
            comparable: "default".to_string(),
            inverted: false,
        }
    }
}

#[derive(Default)]
struct CompareFilterTasklet;
#[flezha_runner::tasklet(name = "compare_filter_tasklet")]
impl Tasklet for CompareFilterTasklet {
    type Input = String;
    type Output = String;
    type State = CompareFilterState;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if ctx.state().comparable == input {
            ctx.emit(input)?;
        }
        Ok(())
    }
}

pub(crate) struct CompareFilterOperator;
impl UnboundedOperator for CompareFilterOperator {
    fn factory_id() -> &'static str {
        "compare_filter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<CompareFilterTasklet>()
            .sink_named("sink");
    }
}

// -- StaticCellOperator -----------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct StaticEntryState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum StaticEntryChange {
    Push(String),
}
impl Change<StaticEntryState> for StaticEntryChange {
    fn apply_to(&self, state: &mut StaticEntryState) {
        match self {
            StaticEntryChange::Push(s) => state.seen.push(s.clone()),
        }
    }
}

#[derive(Default)]
struct StaticEntryTasklet;
#[flezha_runner::tasklet(name = "static_entry_tasklet", keys = ["a", "b", "c", "d"])]
impl Tasklet for StaticEntryTasklet {
    type Input = String;
    type Output = String;
    type State = StaticEntryState;
    type Change = StaticEntryChange;
    fn cells_for(input: &String) -> Vec<&'static str> {
        match input.chars().next() {
            Some('a') => vec!["a"],
            Some('b') => vec!["b"],
            Some('c') => vec!["c"],
            _ => vec!["d"],
        }
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(StaticEntryChange::Push(input.clone()));
        ctx.emit(input)
    }
}

#[derive(Default)]
struct StaticDownstreamTasklet;
#[flezha_runner::tasklet(name = "static_downstream_tasklet")]
impl Tasklet for StaticDownstreamTasklet {
    type Input = String;
    type Output = Vec<u8>;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.into_bytes())
    }
}

pub(crate) struct StaticCellOperator;
impl UnboundedOperator for StaticCellOperator {
    fn factory_id() -> &'static str {
        "static_cell"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<StaticEntryTasklet>()
            .named("hop")
            .into::<StaticDownstreamTasklet>()
            .sink_named("kafka");
    }
}

// -- CellStressOperator --------------------------------------------------

const STRESS_KEYS: &[&str] = &["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"];

#[derive(Default)]
struct StressTaskletA;
#[flezha_runner::tasklet(name = "stress_a")]
impl Tasklet for StressTaskletA {
    type Input = String;
    type Output = String;
    fn cell_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    fn cells_for(_: &String) -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input)
    }
}

#[derive(Default)]
struct StressTaskletB;
#[flezha_runner::tasklet(name = "stress_b")]
impl Tasklet for StressTaskletB {
    type Input = String;
    type Output = String;
    fn cell_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    fn cells_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input)
    }
}

#[derive(Default)]
struct StressTaskletC;
#[flezha_runner::tasklet(name = "stress_c")]
impl Tasklet for StressTaskletC {
    type Input = String;
    type Output = String;
    fn cell_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    fn cells_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input)
    }
}

#[derive(Default)]
struct StressTaskletD;
#[flezha_runner::tasklet(name = "stress_d")]
impl Tasklet for StressTaskletD {
    type Input = String;
    type Output = Vec<u8>;
    fn cell_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    fn cells_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.into_bytes())
    }
}

pub(crate) struct CellStressOperator;
impl UnboundedOperator for CellStressOperator {
    fn factory_id() -> &'static str {
        "cell_stress"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<StressTaskletA>()
            .named("a_to_b")
            .into::<StressTaskletB>()
            .named("b_to_c")
            .into::<StressTaskletC>()
            .named("c_to_d")
            .into::<StressTaskletD>()
            .sink_named("echo");
    }
}
