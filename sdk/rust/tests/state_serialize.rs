//! Round-trip tests for `serialize_operative_state` + `apply_init_values`
//! per `segment_state.rs` Section 4 / Decision 10.

use std::sync::Arc;

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::command::segment_state::{apply_init_values, serialize_operative_state};
use flezha_runner::operation::{Operation, OperationCtx, OperationError};
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::operative::UnboundedInstance;
use flezha_runner::state::{Change, NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::{UnboundedOperative, __dispatch_internals as bi};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

// ----- Single-mode Operation -----

#[derive(Default, Serialize, Deserialize, PartialEq, Eq, Debug)]
struct CounterState {
    value: i64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
struct CounterChange {
    delta: i64,
}
impl Change for CounterChange {}

#[derive(Default)]
struct CounterOperation;
impl Operation for CounterOperation {
    type Input = String;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "counter"
    }
    fn init_state(_: Option<&str>, _p: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }
    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value = state.value.saturating_add(change.delta);
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        Ok(())
    }
}

struct CounterOperative;
impl UnboundedOperative for CounterOperative {
    fn factory_id() -> &'static str {
        "counter_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<CounterOperation>();
    }
}

#[test]
fn single_mode_operation_serializes_under_operation_name_key() {
    let topology = build_topology_for::<CounterOperative>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = UnboundedInstance::new(topology, Arc::from("inst-1"), pool);

    // Cold start: state is the default.
    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 0}}));
}

#[test]
fn apply_init_values_seeds_single_mode_operation() {
    let topology = build_topology_for::<CounterOperative>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = UnboundedInstance::new(topology, Arc::from("inst-2"), pool);

    apply_init_values(&instance, "{\"counter\":{\"value\":42}}").unwrap();

    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 42}}));
}

// ----- Static-mode Operation -----

#[derive(Default, Serialize, Deserialize, PartialEq, Eq, Debug)]
struct OhlcState {
    high: i64,
}

#[derive(Default)]
struct OhlcOperation;
impl Operation for OhlcOperation {
    type Input = String;
    type Output = ();
    type State = OhlcState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "ohlc"
    }
    fn init_state(_: Option<&str>, _p: Option<RestoreInitPayload<'_>>) -> OhlcState {
        OhlcState::default()
    }
    fn operation_keys() -> Vec<&'static str> {
        vec!["1m", "1h"]
    }
    const SEGMENT_FOR_OVERRIDDEN: bool = true;
    fn segment_for(_: &String) -> Vec<&'static str> {
        vec!["1m"]
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        Ok(())
    }
}

struct OhlcOperative;
impl UnboundedOperative for OhlcOperative {
    fn factory_id() -> &'static str {
        "ohlc_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<OhlcOperation>();
    }
}

#[test]
fn static_mode_operation_serializes_under_keyed_object() {
    let topology = build_topology_for::<OhlcOperative>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = UnboundedInstance::new(topology, Arc::from("inst-3"), pool);

    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"ohlc": {"1m": {"high": 0}, "1h": {"high": 0}}}));
}

#[test]
fn apply_init_values_seeds_static_mode_operation_per_key() {
    let topology = build_topology_for::<OhlcOperative>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = UnboundedInstance::new(topology, Arc::from("inst-4"), pool);

    apply_init_values(
        &instance,
        "{\"ohlc\":{\"1m\":{\"high\":5},\"1h\":{\"high\":10}}}",
    )
    .unwrap();

    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"ohlc": {"1m": {"high": 5}, "1h": {"high": 10}}}));
}

#[test]
fn apply_init_values_empty_string_is_noop() {
    let topology = build_topology_for::<CounterOperative>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = UnboundedInstance::new(topology, Arc::from("inst-5"), pool);

    apply_init_values(&instance, "").unwrap();
    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 0}}));
}

#[test]
fn apply_init_values_unknown_operation_is_ignored() {
    let topology = build_topology_for::<CounterOperative>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = UnboundedInstance::new(topology, Arc::from("inst-6"), pool);

    apply_init_values(
        &instance,
        "{\"counter\":{\"value\":7},\"bogus_operation\":{\"x\":1}}",
    )
    .unwrap();
    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 7}}));
}

#[test]
fn round_trip_static_mode_through_serialize_then_apply() {
    let topology = build_topology_for::<OhlcOperative>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let src = UnboundedInstance::new(Arc::clone(&topology), Arc::from("src"), Arc::clone(&pool));
    apply_init_values(
        &src,
        "{\"ohlc\":{\"1m\":{\"high\":42},\"1h\":{\"high\":7}}}",
    )
    .unwrap();

    let bytes = serialize_operative_state(&src);
    let json_str = std::str::from_utf8(&bytes).unwrap();

    let dst = UnboundedInstance::new(topology, Arc::from("dst"), pool);
    apply_init_values(&dst, json_str).unwrap();

    let dst_bytes = serialize_operative_state(&dst);
    assert_eq!(bytes, dst_bytes);
}
