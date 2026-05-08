//! Round-trip tests for `serialize_relay_state` + `apply_init_values`
//! per `partition_state.rs` Section 4 / Decision 10.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use flezha_executor::__dispatch_internals as bi;
use flezha_executor::channel::ChannelBuilder;
use flezha_executor::command::partition_state::{apply_init_values, serialize_relay_state};
use flezha_executor::relay::builder::build_topology_for;
use flezha_executor::relay::RelayInstance;
use flezha_executor::shard::{Shard, ShardCtx, ShardError};
use flezha_executor::state::{Change, NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_executor::Relay;

// ----- Single-mode Shard -----

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
struct CounterShard;
impl Shard for CounterShard {
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
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        Ok(())
    }
}

struct CounterRelay;
impl Relay for CounterRelay {
    fn factory_id() -> &'static str {
        "counter_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<CounterShard>();
    }
}

#[test]
fn single_mode_shard_serializes_under_shard_name_key() {
    let topology = build_topology_for::<CounterRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = RelayInstance::new(topology, Arc::from("inst-1"), pool);

    // Cold start: state is the default.
    let bytes = serialize_relay_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 0}}));
}

#[test]
fn apply_init_values_seeds_single_mode_shard() {
    let topology = build_topology_for::<CounterRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = RelayInstance::new(topology, Arc::from("inst-2"), pool);

    apply_init_values(&instance, "{\"counter\":{\"value\":42}}").unwrap();

    let bytes = serialize_relay_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 42}}));
}

// ----- Static-mode Shard -----

#[derive(Default, Serialize, Deserialize, PartialEq, Eq, Debug)]
struct OhlcState {
    high: i64,
}

#[derive(Default)]
struct OhlcShard;
impl Shard for OhlcShard {
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
    fn shard_keys() -> Vec<&'static str> {
        vec!["1m", "1h"]
    }
    const PARTITION_FOR_OVERRIDDEN: bool = true;
    fn partition_for(_: &String) -> Vec<&'static str> {
        vec!["1m"]
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut ShardCtx<'_>,
    ) -> Result<(), ShardError> {
        Ok(())
    }
}

struct OhlcRelay;
impl Relay for OhlcRelay {
    fn factory_id() -> &'static str {
        "ohlc_relay"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input").into::<OhlcShard>();
    }
}

#[test]
fn static_mode_shard_serializes_under_keyed_object() {
    let topology = build_topology_for::<OhlcRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = RelayInstance::new(topology, Arc::from("inst-3"), pool);

    let bytes = serialize_relay_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        v,
        json!({"ohlc": {"1m": {"high": 0}, "1h": {"high": 0}}})
    );
}

#[test]
fn apply_init_values_seeds_static_mode_shard_per_key() {
    let topology = build_topology_for::<OhlcRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = RelayInstance::new(topology, Arc::from("inst-4"), pool);

    apply_init_values(
        &instance,
        "{\"ohlc\":{\"1m\":{\"high\":5},\"1h\":{\"high\":10}}}",
    )
    .unwrap();

    let bytes = serialize_relay_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        v,
        json!({"ohlc": {"1m": {"high": 5}, "1h": {"high": 10}}})
    );
}

#[test]
fn apply_init_values_empty_string_is_noop() {
    let topology = build_topology_for::<CounterRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = RelayInstance::new(topology, Arc::from("inst-5"), pool);

    apply_init_values(&instance, "").unwrap();
    let bytes = serialize_relay_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 0}}));
}

#[test]
fn apply_init_values_unknown_shard_is_ignored() {
    let topology = build_topology_for::<CounterRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let instance = RelayInstance::new(topology, Arc::from("inst-6"), pool);

    apply_init_values(
        &instance,
        "{\"counter\":{\"value\":7},\"bogus_shard\":{\"x\":1}}",
    )
    .unwrap();
    let bytes = serialize_relay_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 7}}));
}

#[test]
fn round_trip_static_mode_through_serialize_then_apply() {
    let topology = build_topology_for::<OhlcRelay>().expect("topology");
    let pool = Arc::new(bi::WorkStealingPool::with_size(1));
    let src = RelayInstance::new(Arc::clone(&topology), Arc::from("src"), Arc::clone(&pool));
    apply_init_values(&src, "{\"ohlc\":{\"1m\":{\"high\":42},\"1h\":{\"high\":7}}}").unwrap();

    let bytes = serialize_relay_state(&src);
    let json_str = std::str::from_utf8(&bytes).unwrap();

    let dst = RelayInstance::new(topology, Arc::from("dst"), pool);
    apply_init_values(&dst, json_str).unwrap();

    let dst_bytes = serialize_relay_state(&dst);
    assert_eq!(bytes, dst_bytes);
}
