//! Fixtures whose subject is persisted Cell state: the effectively-once tier
//! store, checkpoint granularity and batched confirmation, the counter
//! reference operators and the persistence-off class, plus the
//! `mix bench.roundtrip` classes built on them.

use crate::cell::DoNothingTasklet;
use crate::*;

// -- TierStoreOperator (effectively-once) ----------------------------------------
//
// Effectively-once e2e fixture: inputs are "key:value" strings; state upserts
// the latest value per key; every input emits `key=value` under the natural
// key on a keyed plugin channel. Deterministic, so a crash-replay regenerates
// byte-identical outputs and a changelog sink converges to the last value per
// key — the §6.1 equivalence assertion.

#[derive(Serialize, Deserialize, Clone, Default)]
struct TierStoreState {
    values: std::collections::BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum TierStoreChange {
    Set(String, String),
}
impl Change for TierStoreChange {}

#[derive(Default)]
struct TierStoreTasklet;
impl Tasklet for TierStoreTasklet {
    type Input = String;
    type Output = String;
    type State = TierStoreState;
    type Change = TierStoreChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tier_store_tasklet"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> TierStoreState {
        TierStoreState::default()
    }
    fn apply_change(state: &mut TierStoreState, change: &TierStoreChange) {
        match change {
            TierStoreChange::Set(key, value) => {
                state.values.insert(key.clone(), value.clone());
            }
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let (key, value) = input.split_once(':').unwrap_or((input.as_str(), ""));
        let (key, value) = (key.to_string(), value.to_string());
        // Dead-letter e2e: a "poison" value fails every attempt, exhausting
        // the retry budget so the tier-on atomic park advances the cell
        // past it (and the min-watermark with it).
        if value == "poison" {
            return Err(TaskletError::Custom("poison value".into()));
        }
        state.apply(TierStoreChange::Set(key.clone(), value.clone()));
        ctx.emit_keyed::<String>(key.clone(), format!("{key}={value}"))
    }
}

pub(crate) struct TierStoreOperator;
impl UnboundedOperator for TierStoreOperator {
    fn factory_id() -> &'static str {
        "tier_store"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        // 300 ms backoff base keeps the min-watermark freeze observable
        // (~0.9 s from first failure to park) in the DLQ e2e.
        // Declared DropNothing: a durable-input-log operator that sheds at its
        // mailbox would hold the min-watermark on the shed offset forever
        // while the log behind it keeps growing.
        c.input::<String>("input")
            .on_full(OverflowPolicy::DropNothing)
            .retry_backoff_ms(300)
            .into::<TierStoreTasklet>()
            .keyed()
            .sink_named("sink");
    }
}

// -- TierChainOperator (effectively-once, inter-tasklet) ------------------------
//
// `tier_chain`: an effectively-once stream of two tasklets. `tier_relay` counts
// what it relays and passes each `key:value` on; `tier_tally` counts each key's
// inputs and emits `key=count`. Until the release env is set the tally fails
// the value `hold` and keeps retrying it, so a runner killed there has made the
// relay's record durable and not the tally's.

#[derive(Default)]
struct TierRelayTasklet;
impl Tasklet for TierRelayTasklet {
    type Input = String;
    type Output = String;
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tier_relay"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }
    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value += change.delta;
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        state.apply(CounterChange { delta: 1 });
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct TierTallyTasklet;
impl Tasklet for TierTallyTasklet {
    type Input = String;
    type Output = String;
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tier_tally"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }
    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value += change.delta;
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let (key, value) = input.split_once(':').unwrap_or((input.as_str(), ""));
        if value == "hold" && std::env::var("FLEZHA_BOUNDED_E2E_RELEASE").is_err() {
            return Err(TaskletError::Custom("held until released".into()));
        }
        state.apply(CounterChange { delta: 1 });
        ctx.emit_keyed::<String>(key.to_string(), format!("{key}={}", state.value))
    }
}

pub(crate) struct TierChainOperator;
impl UnboundedOperator for TierChainOperator {
    fn factory_id() -> &'static str {
        "tier_chain"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.input::<String>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<TierRelayTasklet>()
            .key_by(|input: &String| crate::cell::key_prefix(input))
            .retry_max(1_000)
            .into::<TierTallyTasklet>()
            .keyed()
            .sink_named("sink");
    }
}

// -- BenchKeyedOperator (mix bench.roundtrip, --workload keyed) -------------
//
// `tier_store` minus the effectively-once declaration, so the round-trip bench
// can measure the same keyed upsert under both durability postures.
// `c.effectively_once()` is a compile-time channel declaration, not a runtime
// flag, so the posture cannot be threaded through one factory.
// Echo with a declared DropNothing on its entry mailbox. `do_nothing` is the
// merge-blocking `mix bench` factory and must not change; a separate factory
// keeps the round-trip bench's back-pressure posture independent of it.
pub(crate) struct BenchEchoOperator;
impl UnboundedOperator for BenchEchoOperator {
    fn factory_id() -> &'static str {
        "bench_echo"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<DoNothingTasklet>()
            .sink_named("echo");
    }
}

pub(crate) struct BenchKeyedOperator;
impl UnboundedOperator for BenchKeyedOperator {
    fn factory_id() -> &'static str {
        "bench_keyed"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<TierStoreTasklet>()
            .keyed()
            .sink_named("sink");
    }
}

// Same again with local state persistence off. `bench_keyed` still writes a WAL
// record per input even with the durability tier off, because per-Cell
// persistence is a separate, default-on class property — so it is not a
// like-for-like against an engine whose keyed state is pure heap. This variant
// isolates that cost.
pub(crate) struct BenchKeyedNoPersistOperator;
impl UnboundedOperator for BenchKeyedNoPersistOperator {
    const PERSISTENCE_ENABLED: bool = false;

    fn factory_id() -> &'static str {
        "bench_keyed_nopersist"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<TierStoreTasklet>()
            .keyed()
            .sink_named("sink");
    }
}

// -- CheckpointStoreOperator (effectively-once-checkpoint-granularity e2e) --
//
// Same store as TierStoreOperator, but acknowledging only at an explicit
// checkpoint: a `commit:` input closes the interval, every other input folds
// into it. Emits still flush as they happen, so the sink sees an interval's
// outputs long before its offsets are acknowledged — which is exactly what the
// e2e crashes in the middle of.

#[derive(Default)]
struct CheckpointStoreTasklet;
impl Tasklet for CheckpointStoreTasklet {
    type Input = String;
    type Output = String;
    type State = TierStoreState;
    type Change = TierStoreChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "checkpoint_store_tasklet"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> TierStoreState {
        TierStoreState::default()
    }
    fn apply_change(state: &mut TierStoreState, change: &TierStoreChange) {
        match change {
            TierStoreChange::Set(key, value) => {
                state.values.insert(key.clone(), value.clone());
            }
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let (key, value) = input.split_once(':').unwrap_or((input.as_str(), ""));
        let (key, value) = (key.to_string(), value.to_string());
        if key == "commit" {
            ctx.checkpoint();
            return Ok(());
        }
        state.apply(TierStoreChange::Set(key.clone(), value.clone()));
        ctx.emit_keyed::<String>(key.clone(), format!("{key}={value}"))
    }
}

pub(crate) struct CheckpointStoreOperator;
impl UnboundedOperator for CheckpointStoreOperator {
    fn factory_id() -> &'static str {
        "checkpoint_store"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.checkpoint_ack();
        c.input::<String>("input")
            .into::<CheckpointStoreTasklet>()
            .keyed()
            .sink_named("sink");
    }
}

// -- CheckpointBatchedOperator (amortise-effectively-once-output-confirmation) --
//
// The same tasklet as `checkpoint_store`, declared `outputs_at_checkpoint`:
// the interval's outputs are held with its changes and ship inside its
// checkpoint record. `checkpoint_every` is deliberately absent so the test
// drives the close itself — with `commit:` for the batch case, and with the
// delay alone for the quiet-Cell case.
pub(crate) struct CheckpointBatchedOperator;
impl UnboundedOperator for CheckpointBatchedOperator {
    fn factory_id() -> &'static str {
        "checkpoint_batched_store"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.checkpoint_ack();
        c.outputs_at_checkpoint(1_500);
        c.input::<String>("input")
            .into::<CheckpointStoreTasklet>()
            .keyed()
            .sink_named("sink");
    }
}

// -- BenchCheckpointOperator (mix bench.roundtrip, --durability checkpoint) --
//
// `checkpoint_store` driven by a DECLARED cadence instead of a "commit:" input:
// the same keyed upsert, acknowledging every `FLEZHA_BENCH_CHECKPOINT_EVERY`
// inputs (default 500) per Cell. The amortised posture, comparable to other engines'
// checkpoint barrier. `FLEZHA_BENCH_CHECKPOINT_OUTPUTS=at_checkpoint` (with
// `FLEZHA_BENCH_CHECKPOINT_DELAY_MS`) additionally holds the interval's outputs
// until it closes, which is the part-2 opt-in under measurement.
#[derive(Default)]
struct BenchCheckpointTasklet;
impl Tasklet for BenchCheckpointTasklet {
    type Input = String;
    type Output = String;
    type State = TierStoreState;
    type Change = TierStoreChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "bench_checkpoint_tasklet"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> TierStoreState {
        TierStoreState::default()
    }
    fn apply_change(state: &mut TierStoreState, change: &TierStoreChange) {
        match change {
            TierStoreChange::Set(key, value) => {
                state.values.insert(key.clone(), value.clone());
            }
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let (key, value) = input.split_once(':').unwrap_or((input.as_str(), ""));
        let (key, value) = (key.to_string(), value.to_string());
        state.apply(TierStoreChange::Set(key.clone(), value.clone()));
        ctx.emit_keyed::<String>(key.clone(), format!("{key}={value}"))
    }
}

pub(crate) struct BenchCheckpointOperator;
impl UnboundedOperator for BenchCheckpointOperator {
    fn factory_id() -> &'static str {
        "bench_checkpoint"
    }
    fn channels(c: &ChannelBuilder) {
        fn env_u64(key: &str, default: u64) -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        }

        c.effectively_once();
        c.checkpoint_every(env_u64("FLEZHA_BENCH_CHECKPOINT_EVERY", 500) as u32);
        if std::env::var("FLEZHA_BENCH_CHECKPOINT_OUTPUTS").as_deref() == Ok("at_checkpoint") {
            c.outputs_at_checkpoint(env_u64("FLEZHA_BENCH_CHECKPOINT_DELAY_MS", 50));
        }
        c.input::<String>("input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<BenchCheckpointTasklet>()
            .keyed()
            .sink_named("sink");
    }
}

// -- StatefulCounterOperator (Section 6.1) --------------------------------------
//
// One Tasklet `CounterTasklet` with `Change = CounterChange { delta: i64 }`,
// `apply_change` mutating `state.value += change.delta`, `coalesce_key()`
// returning `None` (verbatim retention).

#[derive(Serialize, Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct CounterState {
    pub value: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CounterChange {
    pub delta: i64,
}
impl Change for CounterChange {}

#[derive(Default)]
struct CounterTasklet;
impl Tasklet for CounterTasklet {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "counter_tasklet"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }
    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value = state.value.saturating_add(change.delta);
    }
    fn run(
        &mut self,
        input: i64,
        mut state: StateHandle<'_, Self>,
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        state.apply(CounterChange { delta: input });
        Ok(())
    }
}

pub(crate) struct StatefulCounterOperator;
impl UnboundedOperator for StatefulCounterOperator {
    fn factory_id() -> &'static str {
        "stateful_counter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<CounterEntryTasklet>()
            .named("ticks")
            .into::<CounterTasklet>();
    }
}

#[derive(Default)]
struct CounterEntryTasklet;
impl Tasklet for CounterEntryTasklet {
    type Input = String;
    type Output = i64;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "counter_entry_tasklet"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let delta: i64 = input.parse().unwrap_or(0);
        ctx.emit::<i64>(delta)
    }
}

// -- SleepingCounterOperator -----------------------------------------------------
//
// An unbounded operator that sleeps when idle: `key:delta` inputs fold into one running
// total per key, and every input emits `key=total` on the `totals` sink. The
// sleep e2e reads the total after a wake to prove the Cell came back with the
// state it slept with.

#[derive(Default)]
struct SleepingCounterTasklet;
impl Tasklet for SleepingCounterTasklet {
    type Input = String;
    type Output = String;
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "sleeping_counter_tasklet"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }
    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value = state.value.saturating_add(change.delta);
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let (key, delta) = input.split_once(':').unwrap_or((input.as_str(), "0"));
        state.apply(CounterChange {
            delta: delta.parse().unwrap_or(0),
        });
        ctx.emit::<String>(format!("{key}={}", state.value))
    }
}

pub(crate) struct SleepingCounterOperator;
impl UnboundedOperator for SleepingCounterOperator {
    const SLEEP_WHEN_IDLE: bool = true;

    fn factory_id() -> &'static str {
        "sleeping_counter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .key_by(|input: &String| crate::cell::key_prefix(input))
            .into::<SleepingCounterTasklet>()
            .sink_named("totals");
    }
}

// -- CoalescingCounterOperator (Section 6.2) ------------------------------------
//
// `Change = TickChange { interval: u8, ts: u64, delta: i64 }` with
// `coalesce_key` returning `Some(CoalesceKey::from_hash(&(interval, ts)))`.

#[derive(Serialize, Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct TickState {
    pub last: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TickChange {
    pub interval: u8,
    pub ts: u64,
    pub delta: i64,
}
impl Change for TickChange {
    fn coalesce_key(&self) -> Option<CoalesceKey> {
        Some(CoalesceKey::from_hash(&(self.interval, self.ts)))
    }
}

#[derive(Default)]
struct TickTasklet;
impl Tasklet for TickTasklet {
    type Input = TickInput;
    type Output = ();
    type State = TickState;
    type Change = TickChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tick_tasklet"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> TickState {
        TickState::default()
    }
    fn apply_change(state: &mut TickState, change: &TickChange) {
        state.last = change.delta;
    }
    fn run(
        &mut self,
        input: TickInput,
        mut state: StateHandle<'_, Self>,
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        state.apply(TickChange {
            interval: input.interval,
            ts: input.ts,
            delta: input.delta,
        });
        Ok(())
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct TickInput {
    pub interval: u8,
    pub ts: u64,
    pub delta: i64,
}

#[derive(Default)]
struct TickEntryTasklet;
impl Tasklet for TickEntryTasklet {
    type Input = String;
    type Output = TickInput;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tick_entry_tasklet"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        // Wire format: `<interval>:<ts>:<delta>`.
        let mut parts = input.splitn(3, ':');
        let interval = parts.next().and_then(|s| s.parse::<u8>().ok()).unwrap_or(0);
        let ts = parts
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let delta = parts
            .next()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        ctx.emit::<TickInput>(TickInput {
            interval,
            ts,
            delta,
        })
    }
}

pub(crate) struct CoalescingCounterOperator;
impl UnboundedOperator for CoalescingCounterOperator {
    fn factory_id() -> &'static str {
        "coalescing_counter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<TickEntryTasklet>()
            .named("ticks")
            .into::<TickTasklet>();
    }
}

// -- PersistenceOffOperator (Section 6.3) ---------------------------------------
//
// Same Tasklet topology as `StatefulCounterOperator` but `PERSISTENCE_ENABLED`
// is `false`. Used by integration tests to verify the toggle-off drain
// path discards frames without invoking the wire stub.

pub(crate) struct PersistenceOffOperator;
impl UnboundedOperator for PersistenceOffOperator {
    fn factory_id() -> &'static str {
        "persistence_off_counter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<PersistenceOffEntryTasklet>()
            .named("ticks")
            .into::<PersistenceOffCounterTasklet>();
    }
    const PERSISTENCE_ENABLED: bool = false;
}

#[derive(Default)]
struct PersistenceOffEntryTasklet;
impl Tasklet for PersistenceOffEntryTasklet {
    type Input = String;
    type Output = i64;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "persistence_off_entry_tasklet"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        let delta: i64 = input.parse().unwrap_or(0);
        ctx.emit::<i64>(delta)
    }
}

#[derive(Default)]
struct PersistenceOffCounterTasklet;
impl Tasklet for PersistenceOffCounterTasklet {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "persistence_off_counter_tasklet"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }
    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value = state.value.saturating_add(change.delta);
    }
    fn run(
        &mut self,
        input: i64,
        mut state: StateHandle<'_, Self>,
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        state.apply(CounterChange { delta: input });
        Ok(())
    }
}
