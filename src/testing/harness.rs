//! `TaskletTestHarness::<S>` — replay-equality test harness.
//!
//! Tests construct the harness with `new()` (requires `S: Default`; the
//! state comes from `init_state(None)`),
//! `with_initial_state(state)`, or `with_payload(cell_key, payload)`
//! (replicates the runtime's snapshot + change-log restoration flow).
//! The harness owns its state, drives `S::run` for each input via
//! `apply_inputs`, and records emitted values + Changes for inspection.
//!
//! The determinism gate is `replay_equals(&self) -> Result<(), ReplayMismatch>`:
//! it encodes the recorded changes as the runtime's flusher does, decodes them
//! as a restore does, folds them through `Change::apply_to` against a fresh
//! re-constructed initial state, and asserts equality with the harness's
//! terminal state. A change the runtime could not persist fails there, and
//! non-determinism surfaces as a `ReplayMismatch` carrying serialized
//! expected and actual states for hex-diff inspection.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;

use flz_core::header::{RestoreError, SchemaDecision};
use flz_core::host::output_identity;

use crate::cell::flusher::encode_change;
use crate::cell::restore::{decode_change, init_cell_with_payload, RestoreInitPayload};
use crate::cell::timer::{PendingTimers, TimerFiring, TimerKind, TimerOp, TimerSession};
use crate::tasklet::clock::{evaluate, wall_now_ms};
use crate::tasklet::ctx::effect::HarnessEffects;
use crate::tasklet::ctx::{EmitSink, PartRoute};
use crate::tasklet::{Change, Ctx, Tasklet, TaskletError};

/// Cell identity the harness stands in for the runtime's per-slot
/// identity: feeds `ctx.stable_id()` derivation and the `flz.cell`
/// stamp in keyed mode.
const HARNESS_CELL_IDENTITY: &[u8] = b"<harness>";

/// Recorded emit: type-erased value + optional plugin metadata.
struct Emit {
    value: Arc<dyn Any + Send + Sync>,
    meta: Option<HashMap<String, String>>,
}

impl Emit {
    /// The emitted value; `Ctx` types every emit as the tasklet's `Output`.
    fn output<S: Tasklet>(&self) -> &S::Output {
        let value: &dyn Any = &*self.value;
        value
            .downcast_ref::<S::Output>()
            .expect("Ctx emits only the tasklet's Output")
    }
}

/// Replay-equality failure. Carries the expected and actual serialized
/// states; `Display` renders a hex-diff for visual inspection.
#[derive(Debug)]
pub struct ReplayMismatch {
    pub expected_serialized: Vec<u8>,
    pub actual_serialized: Vec<u8>,
}

impl std::fmt::Display for ReplayMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "TaskletTestHarness::replay_equals: state divergence")?;
        writeln!(f, "  expected ({} bytes):", self.expected_serialized.len())?;
        write_hex(&self.expected_serialized, f)?;
        writeln!(f, "  actual   ({} bytes):", self.actual_serialized.len())?;
        write_hex(&self.actual_serialized, f)?;
        Ok(())
    }
}

impl std::error::Error for ReplayMismatch {}

fn write_hex(bytes: &[u8], f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    for chunk in bytes.chunks(16) {
        write!(f, "    ")?;
        for b in chunk {
            write!(f, "{b:02x} ")?;
        }
        writeln!(f)?;
    }
    Ok(())
}

pub struct TaskletTestHarness<S: Tasklet> {
    tasklet: S,
    state: S::State,
    /// Snapshot of the initial state captured at construction time.
    /// Replayed against `recorded_changes` by [`Self::replay_equals`];
    /// re-hydrated per pass by [`Self::replay_emits_equal`].
    initial_state_bytes: Vec<u8>,
    recorded_changes: Vec<S::Change>,
    recorded_emits: Vec<Emit>,
    /// Treat the output channel as `.keyed()`: `emit_keyed` is accepted
    /// and recorded emits carry `flz.*` identity stamps like the runtime's
    /// plugin frames.
    keyed: bool,
    /// The key the Cell under test was routed by, served to `ctx` exactly
    /// as the runtime serves a keyed Cell's own key. `None` stands for a
    /// single-cell Tasklet, where a keyed channel still refuses a
    /// key-less emit.
    cell_key: Option<String>,
    /// Injected wall-clock values served to wall-clock evaluation in
    /// order (last value repeats). `None` = real `SystemTime`.
    wall_script: Option<Vec<u64>>,
    wall_pos: Cell<usize>,
    /// Pending timers, folded exactly as the runtime folds them. Compared by
    /// [`TaskletTestHarness::replay_equals`] alongside the state: a timer is
    /// folded state, so a replay that rebuilds a different set is the same
    /// class of divergence as one that rebuilds a different balance.
    timers: PendingTimers,
    /// The set the harness started from, so a replay pass restarts where this
    /// one did.
    initial_timers: PendingTimers,
    /// Ops the harness's invocations committed, in order — the durable record
    /// a restore would fold.
    recorded_timer_ops: Vec<TimerOp>,
    /// The `ctx.effect` results this harness's invocations kept, which a
    /// replay pass serves; each input stands for the input-log offset of its
    /// place in the inputs.
    effects: RefCell<HarnessEffects>,
    inputs_taken: u64,
}

impl<S: Tasklet> TaskletTestHarness<S>
where
    S::State: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Construct a harness with state from `S::init_state(None)`.
    pub fn new() -> Self {
        let state = S::init_state(None);
        let initial_state_bytes = serialize_snapshot(&state);
        Self::from_state(state, initial_state_bytes)
    }
}

impl<S: Tasklet> Default for TaskletTestHarness<S>
where
    S::State: serde::Serialize + serde::de::DeserializeOwned,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Tasklet> TaskletTestHarness<S> {
    fn from_state(state: S::State, initial_state_bytes: Vec<u8>) -> Self {
        Self {
            tasklet: S::default(),
            state,
            initial_state_bytes,
            recorded_changes: Vec::new(),
            recorded_emits: Vec::new(),
            keyed: false,
            cell_key: None,
            wall_script: None,
            wall_pos: Cell::new(0),
            timers: PendingTimers::default(),
            initial_timers: PendingTimers::default(),
            recorded_timer_ops: Vec::new(),
            effects: RefCell::new(HarnessEffects::default()),
            inputs_taken: 0,
        }
    }

    /// Construct a harness with an explicit initial state.
    pub fn with_initial_state(state: S::State) -> Self
    where
        S::State: serde::Serialize + serde::de::DeserializeOwned,
    {
        let initial_state_bytes = serialize_snapshot(&state);
        Self::from_state(state, initial_state_bytes)
    }

    /// Construct a harness with state initialized from a
    /// `RestoreInitPayload`, replicating the runtime's snapshot +
    /// change-log restoration flow.
    pub fn with_payload(
        cell_key: Option<&str>,
        payload: RestoreInitPayload<'_>,
    ) -> Result<Self, RestoreError>
    where
        S::State: serde::de::DeserializeOwned + serde::Serialize,
    {
        let (state, timers, _outcome) = init_cell_with_payload::<S>(cell_key, Some(&payload))?;
        let initial_state_bytes = serialize_snapshot(&state);
        let mut harness = Self::from_state(state, initial_state_bytes);
        harness.cell_key = cell_key.map(str::to_string);
        harness.initial_timers = timers.clone();
        harness.timers = timers;
        Ok(harness)
    }

    /// Treat the output channel as `.keyed()`: `emit_keyed` is accepted
    /// and recorded emits carry `flz.*` identity stamps.
    pub fn with_keyed_channel(mut self) -> Self {
        self.keyed = true;
        self
    }

    /// Stand in for the Cell routed by `key`: `ctx.cell_key()` serves it,
    /// and a keyed-channel emit that supplies no key of its own is stamped
    /// with it. Without this the harness is a single-cell Tasklet, where
    /// that emit is refused.
    pub fn with_cell_key(mut self, key: impl Into<String>) -> Self {
        self.cell_key = Some(key.into());
        self
    }

    /// Inject a fixed wall-clock value: every wall-clock evaluation in
    /// this harness observes `ms` instead of `SystemTime::now()`.
    pub fn with_wall_clock(self, ms: u64) -> Self {
        self.with_wall_clock_script(vec![ms])
    }

    /// Inject a scripted wall clock: evaluations consume `script` in
    /// order, repeating the last value once exhausted.
    /// `replay_emits_equal` restarts the script for each pass.
    pub fn with_wall_clock_script(mut self, script: Vec<u64>) -> Self {
        assert!(
            !script.is_empty(),
            "with_wall_clock_script: script must not be empty"
        );
        self.wall_script = Some(script);
        self.wall_pos = Cell::new(0);
        self
    }

    /// Next injected wall value (script order, last repeats), or real
    /// wall time when no script is installed.
    fn next_wall_ms(&self) -> u64 {
        match &self.wall_script {
            Some(script) => {
                let pos = self.wall_pos.get();
                self.wall_pos.set(pos + 1);
                *script
                    .get(pos)
                    .unwrap_or_else(|| script.last().expect("wall script is non-empty"))
            }
            None => wall_now_ms(),
        }
    }

    /// Drive `S::run` for each input. Records emits + Changes on the
    /// harness; mutations on the harness's owned state are visible
    /// after the call. The declared clock is evaluated per invocation
    /// exactly like the dispatcher does: wall time from the injected
    /// script (or real time), logical time from the count of recorded
    /// changes, event time from the input.
    pub fn apply_inputs(&mut self, inputs: impl IntoIterator<Item = S::Input>) -> &mut Self {
        for input in inputs {
            let seq = self.recorded_changes.len() as u64;
            let now = evaluate::<S>(Some(&input), || self.next_wall_ms(), || seq);
            let offset = self.inputs_taken;
            self.inputs_taken += 1;
            self.invoke("run", now, None, Some(offset), |tasklet, ctx| {
                tasklet.run(input, ctx)
            });
        }
        self
    }

    /// One invocation of the author's code, as the dispatcher makes it: a
    /// context over the harness's state and change log, and a timer session
    /// seeded with `firing`, committed once the call returns. Errors are
    /// surfaced as panics; tests that want to exercise the error path drive
    /// the tasklet directly.
    fn invoke(
        &mut self,
        hook: &str,
        now: u64,
        firing: Option<(TimerKind, u64)>,
        offset: Option<u64>,
        call: impl FnOnce(&mut S, &mut Ctx<'_, S>) -> Result<(), TaskletError>,
    ) {
        let recorded = RefCell::new(std::mem::take(&mut self.recorded_changes));
        let invocation_seq = recorded.borrow().len() as u64;
        let session = self.open_timer_session(now, firing);
        let mut sink = RecordingEmitSink {
            emits: &mut self.recorded_emits,
            channel_name: "<harness>",
            keyed: self.keyed,
            seq: invocation_seq,
            ordinal: 0,
        };
        let mut ctx = Ctx::<S>::new(
            &mut self.state,
            &recorded,
            &mut sink,
            now,
            invocation_seq,
            HARNESS_CELL_IDENTITY,
        )
        .with_cell_key(self.cell_key.as_deref())
        .taking(offset)
        .with_timers(&session)
        .with_part(PartRoute::Harness)
        .with_harness_effects(&self.effects);
        if let Err(e) = call(&mut self.tasklet, &mut ctx) {
            panic!("TaskletTestHarness: Tasklet::{hook} returned Err: {e}");
        }
        self.recorded_timer_ops
            .extend(session.into_inner().commit(&mut self.timers));
        self.recorded_changes = recorded.into_inner();
    }

    /// Fire one pending timer through `S::on_timer`, exactly as the runtime
    /// does: the removal is recorded first, `ctx.now()` is the scheduled
    /// timestamp, and whatever the hook applies or registers is committed with
    /// it. Panics if no such timer is pending — a firing the runtime would
    /// never produce is not a test the harness should let you write.
    pub fn fire_timer(&mut self, kind: TimerKind, scheduled_at: u64) -> &mut Self {
        assert!(
            self.timers.contains(kind, scheduled_at),
            "TaskletTestHarness::fire_timer: no {} timer pending at {scheduled_at}",
            kind.as_str(),
        );
        self.invoke(
            "on_timer",
            scheduled_at,
            Some((kind, scheduled_at)),
            None,
            |tasklet, ctx| tasklet.on_timer(TimerFiring { kind, scheduled_at }, ctx),
        );
        self
    }

    /// Fire every pending event-time timer at or below `high_water`, ascending
    /// — the runtime's rule when an input advances the high-water.
    pub fn advance_event_time(&mut self, high_water: u64) -> &mut Self {
        for at in self.timers.due(TimerKind::EventTime, high_water) {
            self.fire_timer(TimerKind::EventTime, at);
        }
        self
    }

    /// The Cell's pending timers of one kind, ascending.
    pub fn pending_timers(&self, kind: TimerKind) -> Vec<u64> {
        self.timers.all(kind)
    }

    /// The durable timer ops the harness's invocations produced, in order.
    pub fn recorded_timer_ops(&self) -> &[TimerOp] {
        &self.recorded_timer_ops
    }

    fn open_timer_session(
        &self,
        now: u64,
        firing: Option<(TimerKind, u64)>,
    ) -> RefCell<TimerSession> {
        crate::cell::timer::timer_policy_for::<S>().session(&self.timers, now, firing)
    }

    /// Borrow the recorded Change log.
    pub fn recorded_changes(&self) -> &[S::Change] {
        &self.recorded_changes
    }

    /// Consuming assertion: recorded Changes equal `expected`.
    pub fn expect_changes(self, expected: &[S::Change]) -> Self
    where
        S::Change: PartialEq + std::fmt::Debug,
    {
        assert_eq!(
            self.recorded_changes.as_slice(),
            expected,
            "TaskletTestHarness::expect_changes: recorded changes diverge",
        );
        self
    }

    /// Consuming assertion: the recorded emits equal `expected`, in order.
    pub fn expect_emits(self, expected: &[S::Output]) -> Self
    where
        S::Output: PartialEq + std::fmt::Debug,
    {
        let observed: Vec<&S::Output> = self.recorded_emits.iter().map(Emit::output::<S>).collect();
        assert_eq!(
            observed.len(),
            expected.len(),
            "TaskletTestHarness::expect_emits: emit count diverges (observed {}, expected {})",
            observed.len(),
            expected.len(),
        );
        for (i, (o, e)) in observed.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                *o, e,
                "TaskletTestHarness::expect_emits: divergence at index {i}",
            );
        }
        self
    }

    /// Consuming assertion: terminal state equals `expected`.
    pub fn expect_state(self, expected: &S::State) -> Self
    where
        S::State: PartialEq + std::fmt::Debug,
    {
        assert_eq!(
            &self.state, expected,
            "TaskletTestHarness::expect_state: state diverges",
        );
        self
    }

    /// Replay a change log against an initial state, the way a restart does:
    /// each change is encoded as the flusher writes it, decoded as the restore
    /// fold reads it, and folded through [`Change::apply_to`] in order.
    /// Panics naming the change when it does not survive that round trip.
    pub fn replay(initial: S::State, log: &[S::Change]) -> S::State {
        let mut state = initial;
        for (i, change) in log.iter().enumerate() {
            let bytes = encode_change(change);
            decode_change::<S>(SchemaDecision::Decode, &bytes)
                .unwrap_or_else(|e| {
                    panic!("TaskletTestHarness: change {i} does not survive persistence: {e}")
                })
                .apply_to(&mut state);
        }
        state
    }

    /// Determinism gate: replay the recorded change log against a
    /// re-constructed initial state and assert equality with the harness's
    /// terminal state via a canonical-JSON round-trip.
    ///
    /// Comparison is canonical JSON (`serde_json::Value` orders map keys via
    /// `BTreeMap` and preserves array order) rather than raw bytes, so a
    /// `HashMap`-keyed state — whose iteration order is hash-seed dependent per
    /// instance — does not produce false divergences. Collection ordering
    /// inside arrays (`Vec` / `VecDeque`) is still significant and a genuine
    /// replay divergence still differs.
    pub fn replay_equals(&self) -> Result<(), ReplayMismatch>
    where
        S::State: serde::Serialize + serde::de::DeserializeOwned,
    {
        let initial: S::State = deserialize_snapshot::<S>(&self.initial_state_bytes);
        let replayed = <TaskletTestHarness<S>>::replay(initial, &self.recorded_changes);
        // The pending-timer set is folded state too, and its fold is the same
        // op replay a restore runs — so it belongs in the same gate.
        let mut replayed_timers = self.initial_timers.clone();
        for op in &self.recorded_timer_ops {
            replayed_timers.apply(op);
        }
        let expected = format!(
            "{}|{}",
            canonical_json(&self.state),
            canonical_json(&self.timers)
        );
        let actual = format!(
            "{}|{}",
            canonical_json(&replayed),
            canonical_json(&replayed_timers)
        );
        if expected == actual {
            Ok(())
        } else {
            Err(ReplayMismatch {
                expected_serialized: expected.into_bytes(),
                actual_serialized: actual.into_bytes(),
            })
        }
    }

    /// Determinism gate for the output side of the contract: run `inputs`
    /// twice from equivalent starting state (this harness's initial state)
    /// and assert the two serialized emit streams — output bytes plus
    /// metadata including any `flz.*` identity stamps — are identical.
    /// The harness's keyed / wall-clock configuration applies to both
    /// passes; a scripted wall clock restarts per pass. A Tasklet that
    /// reads ambient time, randomness, or external mutable state fails
    /// here because the passes emit different bytes.
    ///
    /// The second pass is the replay: every `ctx.effect` is served the result
    /// the first pass kept, and a function that runs there fails the check,
    /// naming its effect.
    pub fn replay_emits_equal(&self, inputs: &[S::Input]) -> Result<(), EmitStreamMismatch>
    where
        S::State: serde::de::DeserializeOwned,
        S::Output: serde::Serialize,
    {
        let (first, kept) = self.run_emit_pass(inputs, HarnessEffects::default())?;
        let replay = HarnessEffects {
            replaying: true,
            ..kept
        };
        let (second, replayed) = self.run_emit_pass(inputs, replay)?;
        if let Some(name) = replayed.ran.first() {
            return Err(EmitStreamMismatch {
                index: 0,
                detail: format!(
                    "effect `{name}` ran its function on the replay pass instead of \
                     returning the result the first pass kept"
                ),
            });
        }
        if let Some((name, _)) = replayed.kept.values().next() {
            return Err(EmitStreamMismatch {
                index: 0,
                detail: format!(
                    "effect `{name}` kept a result on the first pass that the replay \
                     pass never asked for: an outside call made around `ctx.effect`"
                ),
            });
        }
        if first.len() != second.len() {
            return Err(EmitStreamMismatch {
                index: first.len().min(second.len()),
                detail: format!(
                    "emit count diverges across replays: {} vs {}",
                    first.len(),
                    second.len()
                ),
            });
        }
        for (i, (a, b)) in first.iter().zip(second.iter()).enumerate() {
            if a != b {
                return Err(EmitStreamMismatch {
                    index: i,
                    detail: format!("emit {i} diverges across replays: {a:?} vs {b:?}"),
                });
            }
        }
        Ok(())
    }

    /// One `replay_emits_equal` pass: a fresh harness re-hydrated from
    /// this harness's initial state and configuration, run over `inputs`,
    /// emits serialized to comparable form.
    fn run_emit_pass(
        &self,
        inputs: &[S::Input],
        effects: HarnessEffects,
    ) -> Result<(Vec<SerializedEmit>, HarnessEffects), EmitStreamMismatch>
    where
        S::State: serde::de::DeserializeOwned,
        S::Output: serde::Serialize,
    {
        let mut pass = Self::from_state(
            deserialize_snapshot::<S>(&self.initial_state_bytes),
            self.initial_state_bytes.clone(),
        );
        pass.keyed = self.keyed;
        pass.wall_script = self.wall_script.clone();
        pass.initial_timers = self.initial_timers.clone();
        pass.timers = self.initial_timers.clone();
        pass.effects = RefCell::new(effects);
        pass.apply_inputs(inputs.iter().cloned());

        let mut out = Vec::with_capacity(pass.recorded_emits.len());
        for (i, emit) in pass.recorded_emits.iter().enumerate() {
            let bytes =
                postcard::to_allocvec(emit.output::<S>()).map_err(|e| EmitStreamMismatch {
                    index: i,
                    detail: format!("emit {i} failed to serialize: {e}"),
                })?;
            // BTreeMap: metadata compares (and debugs) in stable key order.
            let meta = emit.meta.as_ref().map(|m| m.clone().into_iter().collect());
            out.push(SerializedEmit { bytes, meta });
        }
        Ok((out, pass.effects.into_inner()))
    }

    /// Read the harness's terminal state.
    pub fn state(&self) -> &S::State {
        &self.state
    }

    /// Metadata recorded per emit, in emit order — including the `flz.*`
    /// identity stamps in keyed mode. `None` for stamp-free emits.
    pub fn emitted_meta(&self) -> Vec<Option<&HashMap<String, String>>> {
        self.recorded_emits
            .iter()
            .map(|e| e.meta.as_ref())
            .collect()
    }
}

/// One emit in comparable form: serialized output bytes + metadata
/// (including `flz.*` stamps in keyed mode).
#[derive(Debug, PartialEq, Eq)]
struct SerializedEmit {
    bytes: Vec<u8>,
    meta: Option<std::collections::BTreeMap<String, String>>,
}

/// Replay-emit divergence: the same inputs over equivalent state produced
/// different emit streams — a determinism-contract violation.
#[derive(Debug)]
pub struct EmitStreamMismatch {
    pub index: usize,
    pub detail: String,
}

impl std::fmt::Display for EmitStreamMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskletTestHarness::replay_emits_equal: {}", self.detail)
    }
}

impl std::error::Error for EmitStreamMismatch {}

fn serialize_snapshot<T: serde::Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("serialize_snapshot: postcard failed")
}

/// Canonical JSON for order-insensitive state comparison: `serde_json::Value`
/// keys are `BTreeMap`-ordered, so `HashMap` iteration order does not leak in.
fn canonical_json<T: serde::Serialize>(value: &T) -> String {
    let v: serde_json::Value =
        serde_json::to_value(value).expect("canonical_json: serde_json failed");
    v.to_string()
}

fn deserialize_snapshot<S: Tasklet>(bytes: &[u8]) -> S::State
where
    S::State: serde::de::DeserializeOwned,
{
    postcard::from_bytes(bytes).expect("deserialize_snapshot: postcard failed")
}

/// Emit output used by the harness — captures emits without dispatching
/// them. In keyed mode it applies the same `flz.*` identity stamps the
/// runtime's plugin frames carry, through the core's stamp.
struct RecordingEmitSink<'a> {
    emits: &'a mut Vec<Emit>,
    channel_name: &'static str,
    keyed: bool,
    /// Invocation-entry change count: the harness's stand-in for the
    /// durable change seq in `flz.seq` stamps.
    seq: u64,
    ordinal: u32,
}

impl<'a> EmitSink for RecordingEmitSink<'a> {
    fn emit(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), TaskletError> {
        let meta = if self.keyed {
            let mut metadata = meta.unwrap_or_default();
            let label = std::str::from_utf8(HARNESS_CELL_IDENTITY).expect("ascii");
            let (seq, ordinal) = (self.seq, &mut self.ordinal);
            output_identity(label, true, || seq, ordinal, key.as_deref(), &mut metadata);
            Some(metadata)
        } else {
            meta
        };
        self.emits.push(Emit { value, meta });
        Ok(())
    }

    fn channel_name(&self) -> &str {
        self.channel_name
    }

    /// Keyed harness mode stands in for a channel carrying an `output(...)`, so
    /// a keyed emit picks up its identity stamps and metadata is accepted.
    fn has_plugin_sink(&self) -> bool {
        self.keyed
    }

    fn is_keyed(&self) -> bool {
        self.keyed
    }

    /// The harness runs a steps relay's tasklet as readily as a flow relay's, so
    /// it accepts a completion and, like a checkpoint, records nothing.
    fn complete(&mut self, _result: Vec<u8>) -> Result<(), TaskletError> {
        Ok(())
    }
}
