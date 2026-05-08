//! `ShardTestHarness::<S>` — replay-equality test harness.
//!
//! Tests construct the harness with `new()` (requires `S: Default` and
//! a `Self::State` derivable from `init_state(None, None)`),
//! `with_initial_state(state)`, or `with_payload(shard_key, payload)`
//! (replicates the runtime's snapshot + change-log restoration flow).
//! The harness owns its state, drives `S::run` for each input via
//! `apply_inputs`, and records emitted values + Changes for inspection.
//!
//! The determinism gate is `replay_equals(&self) -> Result<(), ReplayMismatch>`:
//! it folds the recorded change log against a fresh re-constructed
//! initial state and asserts byte-equality with the harness's terminal
//! state (via `serde::Serialize` round-trip). Panics from
//! `Shard::apply_change` surface as test failures; non-determinism
//! surfaces as a `ReplayMismatch` carrying serialized expected and
//! actual states for hex-diff inspection.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::partition::restore::{init_partition_with_payload, RestoreError, RestoreInitPayload};
use crate::shard::ctx::EmitSink;
use crate::shard::{Shard, ShardCtx, ShardError, StateHandle};

/// Recorded emit: type-erased value + optional plugin metadata.
struct Emit {
    value: Arc<dyn Any + Send + Sync>,
    // reason: meta is captured for replay-equality fixtures; tests assert through the public ShardTestHarness API which the compiler can't see at this struct level
    #[allow(dead_code)]
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
        writeln!(f, "ShardTestHarness::replay_equals: state divergence")?;
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

pub struct ShardTestHarness<S: Shard> {
    shard: S,
    state: S::State,
    /// Snapshot of the initial state captured at construction time.
    /// Replayed against `recorded_changes` by [`Self::replay_equals`].
    initial_state_bytes: Vec<u8>,
    recorded_changes: Vec<S::Change>,
    recorded_emits: Vec<Emit>,
}

impl<S: Shard> ShardTestHarness<S>
where
    S::State: serde::Serialize + serde::de::DeserializeOwned,
{
    /// Construct a harness with state from `S::init_state(None, None)`.
    pub fn new() -> Self {
        let state = S::init_state(None, None);
        let initial_state_bytes = serialize_snapshot(&state);
        Self {
            shard: S::default(),
            state,
            initial_state_bytes,
            recorded_changes: Vec::new(),
            recorded_emits: Vec::new(),
        }
    }
}

impl<S: Shard> Default for ShardTestHarness<S>
where
    S::State: serde::Serialize + serde::de::DeserializeOwned,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Shard> ShardTestHarness<S> {
    /// Construct a harness with an explicit initial state.
    pub fn with_initial_state(state: S::State) -> Self
    where
        S::State: serde::Serialize + serde::de::DeserializeOwned,
    {
        let initial_state_bytes = serialize_snapshot(&state);
        Self {
            shard: S::default(),
            state,
            initial_state_bytes,
            recorded_changes: Vec::new(),
            recorded_emits: Vec::new(),
        }
    }

    /// Construct a harness with state initialized from a
    /// `RestoreInitPayload`, replicating the runtime's snapshot +
    /// change-log restoration flow.
    pub fn with_payload(
        shard_key: Option<&str>,
        payload: RestoreInitPayload<'_>,
    ) -> Result<Self, RestoreError>
    where
        S::State: serde::de::DeserializeOwned + serde::Serialize,
    {
        let state = init_partition_with_payload::<S>(shard_key, Some(&payload))?;
        let initial_state_bytes = serialize_snapshot(&state);
        Ok(Self {
            shard: S::default(),
            state,
            initial_state_bytes,
            recorded_changes: Vec::new(),
            recorded_emits: Vec::new(),
        })
    }

    /// Drive `S::run` for each input. Records emits + Changes on the
    /// harness; mutations on the harness's owned state are visible
    /// after the call.
    pub fn apply_inputs(&mut self, inputs: impl IntoIterator<Item = S::Input>) -> &mut Self {
        for input in inputs {
            let mut sink = RecordingEmitSink {
                emits: &mut self.recorded_emits,
                channel_name: "<harness>",
            };
            let mut ctx = ShardCtx::new(&mut sink);
            let handle = StateHandle::<S>::new(&mut self.state, &mut self.recorded_changes);
            // Errors are surfaced as panics in the harness; tests that
            // want to exercise the error path should drive `S::run`
            // directly.
            if let Err(e) = self.shard.run(input, handle, &mut ctx) {
                panic!("ShardTestHarness: Shard::run returned Err: {e}");
            }
        }
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
            "ShardTestHarness::expect_changes: recorded changes diverge",
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
            "ShardTestHarness::expect_emits: emits of type {} count diverges \
             (observed {}, expected {})",
            std::any::type_name::<C>(),
            observed.len(),
            expected.len(),
        );
        for (i, (o, e)) in observed.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                *o, e,
                "ShardTestHarness::expect_emits: divergence at index {i}",
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
            "ShardTestHarness::expect_state: state diverges",
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
    /// re-constructed initial state and assert byte-equality with the
    /// harness's terminal state via `serde::Serialize` round-trip.
    pub fn replay_equals(&self) -> Result<(), ReplayMismatch>
    where
        S::State: serde::Serialize + serde::de::DeserializeOwned,
    {
        let initial: S::State = deserialize_snapshot::<S>(&self.initial_state_bytes);
        let replayed =
            <ShardTestHarness<S>>::replay(initial, &self.recorded_changes);
        let expected = serialize_snapshot(&self.state);
        let actual = serialize_snapshot(&replayed);
        if expected == actual {
            Ok(())
        } else {
            Err(ReplayMismatch {
                expected_serialized: expected,
                actual_serialized: actual,
            })
        }
    }

    /// Read the harness's terminal state.
    pub fn state(&self) -> &S::State {
        &self.state
    }
}

fn serialize_snapshot<T: serde::Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("serialize_snapshot: postcard failed")
}

fn deserialize_snapshot<S: Shard>(bytes: &[u8]) -> S::State
where
    S::State: serde::de::DeserializeOwned,
{
    postcard::from_bytes(bytes).expect("deserialize_snapshot: postcard failed")
}

/// Emit sink used by the harness — captures emits without dispatching
/// them. Plugin metadata is recorded but not validated.
struct RecordingEmitSink<'a> {
    emits: &'a mut Vec<Emit>,
    channel_name: &'static str,
}

impl<'a> EmitSink for RecordingEmitSink<'a> {
    fn emit_inter_shard(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
    ) -> Result<(), ShardError> {
        self.emits.push(Emit { value, meta: None });
        Ok(())
    }

    fn emit_plugin(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
    ) -> Result<(), ShardError> {
        self.emits.push(Emit { value, meta });
        Ok(())
    }

    fn channel_name(&self) -> &str {
        self.channel_name
    }

    fn is_plugin_bound(&self) -> bool {
        false
    }
}
