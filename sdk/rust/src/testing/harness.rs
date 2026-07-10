//! `OperationTestHarness::<S>` — replay-equality test harness.
//!
//! Tests construct the harness with `new()` (requires `S: Default` and
//! a `Self::State` derivable from `init_state(None, None)`),
//! `with_initial_state(state)`, or `with_payload(operation_key, payload)`
//! (replicates the runtime's snapshot + change-log restoration flow).
//! The harness owns its state, drives `S::run` for each input via
//! `apply_inputs`, and records emitted values + Changes for inspection.
//!
//! The determinism gate is `replay_equals(&self) -> Result<(), ReplayMismatch>`:
//! it folds the recorded change log against a fresh re-constructed
//! initial state and asserts byte-equality with the harness's terminal
//! state (via `serde::Serialize` round-trip). Panics from
//! `Operation::apply_change` surface as test failures; non-determinism
//! surfaces as a `ReplayMismatch` carrying serialized expected and
//! actual states for hex-diff inspection.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::Arc;

use crate::operation::clock::{wall_now_ms, ClockFor};
use crate::operation::ctx::EmitSink;
use crate::operation::{Operation, OperationCtx, OperationError, StateHandle};
use crate::operative::dispatcher::apply_identity_stamps;
use crate::segment::restore::{init_segment_with_payload, RestoreError, RestoreInitPayload};

/// Segment identity the harness stands in for the runtime's per-slot
/// identity: feeds `ctx.stable_id()` derivation and the `flz.segment`
/// stamp in keyed mode.
const HARNESS_SEGMENT_IDENTITY: &[u8] = b"<harness>";

/// Recorded emit: type-erased value + optional plugin metadata.
struct Emit {
    value: Arc<dyn Any + Send + Sync>,
    meta: Option<HashMap<String, String>>,
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
        writeln!(f, "OperationTestHarness::replay_equals: state divergence")?;
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

pub struct OperationTestHarness<S: Operation> {
    operation: S,
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
    /// Injected wall-clock values served to `WallClock` evaluation in
    /// order (last value repeats). `None` = real `SystemTime`.
    wall_script: Option<Vec<u64>>,
    wall_pos: Cell<usize>,
}

impl<S: Operation> OperationTestHarness<S>
where
    S::State: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Construct a harness with state from `S::init_state(None, None)`.
    pub fn new() -> Self {
        let state = S::init_state(None, None);
        let initial_state_bytes = serialize_snapshot(&state);
        Self::from_state(state, initial_state_bytes)
    }
}

impl<S: Operation> Default for OperationTestHarness<S>
where
    S::State: serde::Serialize + serde::de::DeserializeOwned,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Operation> OperationTestHarness<S> {
    fn from_state(state: S::State, initial_state_bytes: Vec<u8>) -> Self {
        Self {
            operation: S::default(),
            state,
            initial_state_bytes,
            recorded_changes: Vec::new(),
            recorded_emits: Vec::new(),
            keyed: false,
            wall_script: None,
            wall_pos: Cell::new(0),
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
        operation_key: Option<&str>,
        payload: RestoreInitPayload<'_>,
    ) -> Result<Self, RestoreError>
    where
        S::State: serde::de::DeserializeOwned + serde::Serialize,
    {
        let state = init_segment_with_payload::<S>(operation_key, Some(&payload))?;
        let initial_state_bytes = serialize_snapshot(&state);
        Ok(Self::from_state(state, initial_state_bytes))
    }

    /// Treat the output channel as `.keyed()`: `emit_keyed` is accepted
    /// and recorded emits carry `flz.*` identity stamps.
    pub fn with_keyed_channel(mut self) -> Self {
        self.keyed = true;
        self
    }

    /// Inject a fixed wall-clock value: every `WallClock` evaluation in
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
    /// after the call. The declared Clock is evaluated per invocation
    /// exactly like the dispatcher does: wall time from the injected
    /// script (or real time), logical time from the count of recorded
    /// changes, event time from the input.
    pub fn apply_inputs(&mut self, inputs: impl IntoIterator<Item = S::Input>) -> &mut Self {
        let recorded = RefCell::new(std::mem::take(&mut self.recorded_changes));
        for input in inputs {
            let invocation_seq = recorded.borrow().len() as u64;
            let now = <S::Clock as ClockFor<S::Input>>::evaluate(
                Some(&input),
                || self.next_wall_ms(),
                || invocation_seq,
            );
            let mut sink = RecordingEmitSink {
                emits: &mut self.recorded_emits,
                channel_name: "<harness>",
                keyed: self.keyed,
                seq: invocation_seq,
                ordinal: 0,
            };
            let mut ctx =
                OperationCtx::new(&mut sink, now, invocation_seq, HARNESS_SEGMENT_IDENTITY);
            let handle = StateHandle::<S>::new(&mut self.state, &recorded);
            // Errors are surfaced as panics in the harness; tests that
            // want to exercise the error path should drive `S::run`
            // directly.
            if let Err(e) = self.operation.run(input, handle, &mut ctx) {
                panic!("OperationTestHarness: Operation::run returned Err: {e}");
            }
        }
        self.recorded_changes = recorded.into_inner();
        self
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
            "OperationTestHarness::expect_changes: recorded changes diverge",
        );
        self
    }

    /// Consuming assertion: recorded emits of type `C` equal `expected`.
    /// Emits of other types are ignored.
    pub fn expect_emits<C>(self, expected: &[C]) -> Self
    where
        C: 'static + PartialEq + std::fmt::Debug,
    {
        let observed: Vec<&C> = self
            .recorded_emits
            .iter()
            .filter_map(|e| {
                let any_ref: &dyn Any = &*e.value;
                any_ref.downcast_ref::<C>()
            })
            .collect();
        assert_eq!(
            observed.len(),
            expected.len(),
            "OperationTestHarness::expect_emits: emits of type {} count diverges \
             (observed {}, expected {})",
            std::any::type_name::<C>(),
            observed.len(),
            expected.len(),
        );
        for (i, (o, e)) in observed.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                *o, e,
                "OperationTestHarness::expect_emits: divergence at index {i}",
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
            "OperationTestHarness::expect_state: state diverges",
        );
        self
    }

    /// Replay a change log against an initial state. Static helper —
    /// useful for constructing expected states in tests.
    pub fn replay(initial: S::State, log: &[S::Change]) -> S::State {
        let mut state = initial;
        for change in log {
            S::apply_change(&mut state, change);
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
        let replayed = <OperationTestHarness<S>>::replay(initial, &self.recorded_changes);
        let expected = canonical_json(&self.state);
        let actual = canonical_json(&replayed);
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
    /// passes; a scripted wall clock restarts per pass. An Operation that
    /// reads ambient time, randomness, or external mutable state fails
    /// here because the passes emit different bytes.
    pub fn replay_emits_equal(&self, inputs: &[S::Input]) -> Result<(), EmitStreamMismatch>
    where
        S::State: serde::de::DeserializeOwned,
        S::Output: serde::Serialize,
    {
        let first = self.run_emit_pass(inputs)?;
        let second = self.run_emit_pass(inputs)?;
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
    fn run_emit_pass(&self, inputs: &[S::Input]) -> Result<Vec<SerializedEmit>, EmitStreamMismatch>
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
        pass.apply_inputs(inputs.iter().cloned());

        let mut out = Vec::with_capacity(pass.recorded_emits.len());
        for (i, emit) in pass.recorded_emits.iter().enumerate() {
            let any_ref: &dyn Any = &*emit.value;
            let typed = any_ref
                .downcast_ref::<S::Output>()
                .ok_or_else(|| EmitStreamMismatch {
                    index: i,
                    detail: format!(
                        "emit {i} is not the Operation's Output type and cannot be \
                         serialized for comparison"
                    ),
                })?;
            let bytes = postcard::to_allocvec(typed).map_err(|e| EmitStreamMismatch {
                index: i,
                detail: format!("emit {i} failed to serialize: {e}"),
            })?;
            // BTreeMap: metadata compares (and debugs) in stable key order.
            let meta = emit.meta.as_ref().map(|m| m.clone().into_iter().collect());
            out.push(SerializedEmit { bytes, meta });
        }
        Ok(out)
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
        write!(
            f,
            "OperationTestHarness::replay_emits_equal: {}",
            self.detail
        )
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

fn deserialize_snapshot<S: Operation>(bytes: &[u8]) -> S::State
where
    S::State: serde::de::DeserializeOwned,
{
    postcard::from_bytes(bytes).expect("deserialize_snapshot: postcard failed")
}

/// Emit sink used by the harness — captures emits without dispatching
/// them. In keyed mode it applies the same `flz.*` identity stamps the
/// runtime's plugin frames carry, via the shared stamp helper.
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
    fn emit_inter_operation(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Result<(), OperationError> {
        self.emits.push(Emit { value, meta: None });
        Ok(())
    }

    fn emit_plugin(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), OperationError> {
        let meta = if self.keyed {
            let mut metadata = meta.unwrap_or_default();
            apply_identity_stamps(
                &mut metadata,
                std::str::from_utf8(HARNESS_SEGMENT_IDENTITY).expect("ascii"),
                self.seq,
                &mut self.ordinal,
                key,
            );
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

    /// Keyed harness mode stands in for a plugin-bound channel so
    /// `emit_keyed` routes through `emit_plugin` and picks up stamps.
    fn is_plugin_bound(&self) -> bool {
        self.keyed
    }

    fn is_keyed(&self) -> bool {
        self.keyed
    }
}
