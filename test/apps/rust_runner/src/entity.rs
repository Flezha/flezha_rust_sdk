//! Stream fixtures distinguished by entity shape: dynamic entities, keyed
//! relays, session timeouts, static fan-out, the two pass-through classes and
//! the four-stage stress pipeline.

use crate::*;

// -- DynamicEntityOperative ----------------------------------------------------
//
// Dynamic-mode operative shared by the e2e correctness test and `mix bench
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
impl Change for DynChange {}

#[derive(Default)]
struct DynamicEntityProcedure;
impl Procedure for DynamicEntityProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = DynState;
    type Change = DynChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "dynamic_entity_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> DynState {
        DynState::default()
    }
    fn apply_change(state: &mut DynState, change: &DynChange) {
        let DynChange::Push(s) = change;
        state.seen.push(s.clone());
    }
    fn entity_idle_ttl() -> Option<Duration> {
        env::var("DYN_TEST_TTL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Duration::from_millis)
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(DynChange::Push(input.clone()));
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

pub(crate) struct DynamicEntityOperative;
impl StreamOperative for DynamicEntityOperative {
    fn factory_id() -> &'static str {
        "dynamic_entity"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .on_full(OverflowPolicy::DropNothing)
            .filter(|input: &String| input != "drop")
            .key_by(|input: &String| input.split(':').next().unwrap_or(input).to_string())
            .into::<DynamicEntityProcedure>()
            .sink_named("kafka");
    }
}

// -- PartitionRelayOperative --------------------------------------------------
//
// The partitioned-operatives e2e's upstream: keyed on the wiring (so it can be
// partitioned at all) AND emitting keyed (so a partitioned forward target can
// resolve the owning member). Key = text before the first ':', the same
// convention as `dynamic_entity`, so one fixture's key groups line up with the
// other's.

#[derive(Serialize, Deserialize, Clone, Default)]
struct RelayState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum RelayChange {
    Push(String),
}
impl Change for RelayChange {}

#[derive(Default)]
struct PartitionRelayProcedure;
impl Procedure for PartitionRelayProcedure {
    type Input = String;
    type Output = String;
    type State = RelayState;
    type Change = RelayChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "partition_relay_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> RelayState {
        RelayState::default()
    }
    fn apply_change(state: &mut RelayState, change: &RelayChange) {
        let RelayChange::Push(s) = change;
        state.seen.push(s.clone());
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let key = input
            .split(':')
            .next()
            .unwrap_or(input.as_str())
            .to_string();
        state.apply(RelayChange::Push(input.clone()));
        ctx.emit_keyed::<String>(key, input)
    }
}

pub(crate) struct PartitionRelayOperative;
impl StreamOperative for PartitionRelayOperative {
    fn factory_id() -> &'static str {
        "partition_relay"
    }
    fn channels(c: &ChannelBuilder) {
        // The plugin channel is named `input` because a forwarded frame keeps
        // its producing channel's name in metadata, and that is what selects
        // the receiving operative's channel — the downstream fixture's entry
        // channel. The entry channel here is `in` so the two names cannot
        // collide inside this class.
        c.input::<String>("in")
            .key_by(|input: &String| input.split(':').next().unwrap_or(input).to_string())
            .into::<PartitionRelayProcedure>()
            .keyed()
            .sink_named("input");
    }
}

// -- SessionTimeoutOperative --------------------------------------------------
//
// The worked session-timeout shape (`entity-timers`): each input pushes the
// idle deadline out, and the firing closes the session. Single-entity so the
// e2e's crash-resume half exercises a durable Entity on both runtimes. The
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
impl Change for SessionChange {}

fn session_timeout_ms() -> u64 {
    env::var("TIMER_TEST_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000)
}

#[derive(Default)]
struct SessionTimeoutProcedure;
impl Procedure for SessionTimeoutProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = SessionState;
    type Change = SessionChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "session_timeout_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SessionState {
        SessionState::default()
    }
    fn apply_change(state: &mut SessionState, change: &SessionChange) {
        match change {
            SessionChange::Touched(s) => state.touched.push(s.clone()),
            SessionChange::Closed(at) => state.closed_at.push(*at),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(SessionChange::Touched(input));
        // A processing-time timer, because the answer to "nothing has arrived"
        // cannot be an event-time one: nothing would advance the clock.
        ctx.timer_after(Duration::from_millis(session_timeout_ms()))?;
        Ok(())
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        _kind: flezha_runner::state::TimerKind,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(SessionChange::Closed(fired_at));
        ctx.emit::<Vec<u8>>(format!("closed@{fired_at}").into_bytes())
    }
}

pub(crate) struct SessionTimeoutOperative;
impl StreamOperative for SessionTimeoutOperative {
    fn factory_id() -> &'static str {
        "session_timeout"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<SessionTimeoutProcedure>()
            .sink_named("kafka");
    }
}

// -- DoNothingOperative -----------------------------------------------------------
//
// Echoes the input bytes on the plugin-bound `echo` channel. Stateless;
// inherits `Change = NoChange` so the runtime skips flush-buffer registration.

#[derive(Default)]
pub(crate) struct DoNothingProcedure;
impl Procedure for DoNothingProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "do_nothing_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

pub(crate) struct DoNothingOperative;
impl StreamOperative for DoNothingOperative {
    fn factory_id() -> &'static str {
        "do_nothing"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<DoNothingProcedure>()
            .sink_named("echo");
    }
}

// -- StoreInputOperative ----------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct StoreInputState {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum StoreInputChange {
    PushText(String),
}
impl Change for StoreInputChange {}

#[derive(Default)]
struct StoreInputProcedure;
impl Procedure for StoreInputProcedure {
    type Input = String;
    type Output = String;
    type State = StoreInputState;
    type Change = StoreInputChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "store_input_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> StoreInputState {
        StoreInputState::default()
    }
    fn apply_change(state: &mut StoreInputState, change: &StoreInputChange) {
        match change {
            StoreInputChange::PushText(s) => state.text_inputs.push(s.clone()),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(StoreInputChange::PushText(input.clone()));
        ctx.emit::<String>(input)
    }
}

pub(crate) struct StoreInputOperative;
impl StreamOperative for StoreInputOperative {
    fn factory_id() -> &'static str {
        "store_input"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<StoreInputProcedure>()
            .sink_named("sink");
    }
}

// -- CompareFilterOperative -------------------------------------------------------

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
struct CompareFilterProcedure;
impl Procedure for CompareFilterProcedure {
    type Input = String;
    type Output = String;
    type State = CompareFilterState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "compare_filter_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> CompareFilterState {
        CompareFilterState::default()
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        if state.comparable == input {
            ctx.emit::<String>(input)?;
        }
        Ok(())
    }
}

pub(crate) struct CompareFilterOperative;
impl StreamOperative for CompareFilterOperative {
    fn factory_id() -> &'static str {
        "compare_filter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<CompareFilterProcedure>()
            .sink_named("sink");
    }
}

// -- StaticEntityOperative -----------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct StaticEntryState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum StaticEntryChange {
    Push(String),
}
impl Change for StaticEntryChange {}

#[derive(Default)]
struct StaticEntryProcedure;
impl Procedure for StaticEntryProcedure {
    type Input = String;
    type Output = String;
    type State = StaticEntryState;
    type Change = StaticEntryChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_entry_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> StaticEntryState {
        StaticEntryState::default()
    }
    fn apply_change(state: &mut StaticEntryState, change: &StaticEntryChange) {
        match change {
            StaticEntryChange::Push(s) => state.seen.push(s.clone()),
        }
    }
    fn entity_keys() -> Vec<&'static str> {
        vec!["a", "b", "c", "d"]
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(input: &String) -> Vec<&'static str> {
        match input.chars().next() {
            Some('a') => vec!["a"],
            Some('b') => vec!["b"],
            Some('c') => vec!["c"],
            _ => vec!["d"],
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(StaticEntryChange::Push(input.clone()));
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StaticDownstreamProcedure;
impl Procedure for StaticDownstreamProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_downstream_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

pub(crate) struct StaticEntityOperative;
impl StreamOperative for StaticEntityOperative {
    fn factory_id() -> &'static str {
        "static_entity"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<StaticEntryProcedure>()
            .named("hop")
            .into::<StaticDownstreamProcedure>()
            .sink_named("kafka");
    }
}

// -- EntityStressOperative --------------------------------------------------

const STRESS_KEYS: &[&str] = &["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"];

#[derive(Default)]
struct StressProcedureA;
impl Procedure for StressProcedureA {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_a"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn entity_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_: &String) -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressProcedureB;
impl Procedure for StressProcedureB {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_b"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn entity_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressProcedureC;
impl Procedure for StressProcedureC {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_c"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn entity_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressProcedureD;
impl Procedure for StressProcedureD {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_d"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn entity_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

pub(crate) struct EntityStressOperative;
impl StreamOperative for EntityStressOperative {
    fn factory_id() -> &'static str {
        "entity_stress"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<StressProcedureA>()
            .named("a_to_b")
            .into::<StressProcedureB>()
            .named("b_to_c")
            .into::<StressProcedureC>()
            .named("c_to_d")
            .into::<StressProcedureD>()
            .sink_named("echo");
    }
}
