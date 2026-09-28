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
impl Change<TierStoreState> for TierStoreChange {
    fn apply_to(&self, state: &mut TierStoreState) {
        match self {
            TierStoreChange::Set(key, value) => {
                state.values.insert(key.clone(), value.clone());
            }
        }
    }
}

#[derive(Default)]
struct TierStoreTasklet;
#[flezha_runner::tasklet(name = "tier_store_tasklet")]
impl Tasklet for TierStoreTasklet {
    type Input = String;
    type Output = String;
    type State = TierStoreState;
    type Change = TierStoreChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let (key, value) = input.split_once(':').unwrap_or((input.as_str(), ""));
        let (key, value) = (key.to_string(), value.to_string());
        // Dead-letter e2e: a "poison" value fails every attempt, exhausting
        // the retry budget so the tier-on atomic park advances the cell
        // past it (and the min-watermark with it).
        if value == "poison" {
            return Err(TaskletError::Custom("poison value".into()));
        }
        ctx.change(TierStoreChange::Set(key.clone(), value.clone()));
        ctx.emit_keyed(key.clone(), format!("{key}={value}"))
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
#[flezha_runner::tasklet(name = "tier_relay")]
impl Tasklet for TierRelayTasklet {
    type Input = String;
    type Output = String;
    type State = CounterState;
    type Change = CounterChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(CounterChange { delta: 1 });
        ctx.emit(input)
    }
}

#[derive(Default)]
struct TierTallyTasklet;
#[flezha_runner::tasklet(name = "tier_tally")]
impl Tasklet for TierTallyTasklet {
    type Input = String;
    type Output = String;
    type State = CounterState;
    type Change = CounterChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let (key, value) = input.split_once(':').unwrap_or((input.as_str(), ""));
        if value == "hold" && std::env::var("FLEZHA_BOUNDED_E2E_RELEASE").is_err() {
            return Err(TaskletError::Custom("held until released".into()));
        }
        ctx.change(CounterChange { delta: 1 });
        ctx.emit_keyed(key.to_string(), format!("{key}={}", ctx.state().value))
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
#[flezha_runner::tasklet(name = "checkpoint_store_tasklet")]
impl Tasklet for CheckpointStoreTasklet {
    type Input = String;
    type Output = String;
    type State = TierStoreState;
    type Change = TierStoreChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let (key, value) = input.split_once(':').unwrap_or((input.as_str(), ""));
        let (key, value) = (key.to_string(), value.to_string());
        if key == "commit" {
            ctx.checkpoint();
            return Ok(());
        }
        ctx.change(TierStoreChange::Set(key.clone(), value.clone()));
        ctx.emit_keyed(key.clone(), format!("{key}={value}"))
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
#[flezha_runner::tasklet(name = "bench_checkpoint_tasklet")]
impl Tasklet for BenchCheckpointTasklet {
    type Input = String;
    type Output = String;
    type State = TierStoreState;
    type Change = TierStoreChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let (key, value) = input.split_once(':').unwrap_or((input.as_str(), ""));
        let (key, value) = (key.to_string(), value.to_string());
        ctx.change(TierStoreChange::Set(key.clone(), value.clone()));
        ctx.emit_keyed(key.clone(), format!("{key}={value}"))
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
// whose `apply_to` adds the delta to `state.value`, and `coalesce_key()`
// returning `None` (verbatim retention).

#[derive(Serialize, Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct CounterState {
    pub value: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CounterChange {
    pub delta: i64,
}
impl Change<CounterState> for CounterChange {
    fn apply_to(&self, state: &mut CounterState) {
        state.value = state.value.saturating_add(self.delta);
    }
}

#[derive(Default)]
struct CounterTasklet;
#[flezha_runner::tasklet(name = "counter_tasklet")]
impl Tasklet for CounterTasklet {
    type Input = i64;
    type State = CounterState;
    type Change = CounterChange;
    fn run(&mut self, input: i64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(CounterChange { delta: input });
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
#[flezha_runner::tasklet(name = "counter_entry_tasklet")]
impl Tasklet for CounterEntryTasklet {
    type Input = String;
    type Output = i64;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let delta: i64 = input.parse().unwrap_or(0);
        ctx.emit(delta)
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
#[flezha_runner::tasklet(name = "sleeping_counter_tasklet")]
impl Tasklet for SleepingCounterTasklet {
    type Input = String;
    type Output = String;
    type State = CounterState;
    type Change = CounterChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let (key, delta) = input.split_once(':').unwrap_or((input.as_str(), "0"));
        ctx.change(CounterChange {
            delta: delta.parse().unwrap_or(0),
        });
        ctx.emit(format!("{key}={}", ctx.state().value))
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
impl Change<TickState> for TickChange {
    fn apply_to(&self, state: &mut TickState) {
        state.last = self.delta;
    }

    fn coalesce_key(&self) -> Option<CoalesceKey> {
        Some(CoalesceKey::from_hash(&(self.interval, self.ts)))
    }
}

#[derive(Default)]
struct TickTasklet;
#[flezha_runner::tasklet(name = "tick_tasklet")]
impl Tasklet for TickTasklet {
    type Input = TickInput;
    type State = TickState;
    type Change = TickChange;
    fn run(&mut self, input: TickInput, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(TickChange {
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
#[flezha_runner::tasklet(name = "tick_entry_tasklet")]
impl Tasklet for TickEntryTasklet {
    type Input = String;
    type Output = TickInput;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
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
        ctx.emit(TickInput {
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
#[flezha_runner::tasklet(name = "persistence_off_entry_tasklet")]
impl Tasklet for PersistenceOffEntryTasklet {
    type Input = String;
    type Output = i64;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let delta: i64 = input.parse().unwrap_or(0);
        ctx.emit(delta)
    }
}

#[derive(Default)]
struct PersistenceOffCounterTasklet;
#[flezha_runner::tasklet(name = "persistence_off_counter_tasklet")]
impl Tasklet for PersistenceOffCounterTasklet {
    type Input = i64;
    type State = CounterState;
    type Change = CounterChange;
    fn run(&mut self, input: i64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(CounterChange { delta: input });
        Ok(())
    }
}
