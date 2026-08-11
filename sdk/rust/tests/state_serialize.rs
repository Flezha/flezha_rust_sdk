//! Round-trip tests for `serialize_operative_state` + `apply_init_values`
//! per `entity_state.rs` Section 4 / Decision 10.

use std::sync::Arc;

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::command::entity_state::{
    apply_init_values, serialize_operative_state, serialize_selected_state, StatePurpose,
    StateSelector, ENTITY_EVICTED, ENTITY_KEY_WITHOUT_PROCEDURE, ENTITY_NOT_FOUND,
    PROCEDURE_NOT_FOUND,
};
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::operative::UnboundedInstance;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{Change, NoChange, RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::UnboundedOperative;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

mod common;
use common::{dispatch_input, dummy_stream, setup, wait_until};

// ----- Single-mode Procedure -----

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
struct CounterProcedure;
impl Procedure for CounterProcedure {
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
        input: String,
        mut state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(CounterChange {
            delta: input.trim().parse().unwrap_or(0),
        });
        Ok(())
    }
}

struct CounterOperative;
impl UnboundedOperative for CounterOperative {
    fn factory_id() -> &'static str {
        "counter_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<CounterProcedure>();
    }
}

#[test]
fn single_mode_procedure_serializes_under_procedure_name_key() {
    let topology = build_topology_for::<CounterOperative>().expect("topology");
    let instance = UnboundedInstance::new(topology, Arc::from("inst-1"));

    // Cold start: state is the default.
    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 0}}));
}

#[test]
fn apply_init_values_seeds_single_mode_procedure() {
    let topology = build_topology_for::<CounterOperative>().expect("topology");
    let instance = UnboundedInstance::new(topology, Arc::from("inst-2"));

    apply_init_values(&instance, "{\"counter\":{\"value\":42}}").unwrap();

    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 42}}));
}

// ----- Static-mode Procedure -----

#[derive(Default, Serialize, Deserialize, PartialEq, Eq, Debug)]
struct OhlcState {
    high: i64,
}

#[derive(Default)]
struct OhlcProcedure;
impl Procedure for OhlcProcedure {
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
    fn entity_keys() -> Vec<&'static str> {
        vec!["1m", "1h"]
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_: &String) -> Vec<&'static str> {
        vec!["1m"]
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

struct OhlcOperative;
impl UnboundedOperative for OhlcOperative {
    fn factory_id() -> &'static str {
        "ohlc_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<OhlcProcedure>();
    }
}

#[test]
fn static_mode_procedure_serializes_under_keyed_object() {
    let topology = build_topology_for::<OhlcOperative>().expect("topology");
    let instance = UnboundedInstance::new(topology, Arc::from("inst-3"));

    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"ohlc": {"1m": {"high": 0}, "1h": {"high": 0}}}));
}

#[test]
fn apply_init_values_seeds_static_mode_procedure_per_key() {
    let topology = build_topology_for::<OhlcOperative>().expect("topology");
    let instance = UnboundedInstance::new(topology, Arc::from("inst-4"));

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
    let instance = UnboundedInstance::new(topology, Arc::from("inst-5"));

    apply_init_values(&instance, "").unwrap();
    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 0}}));
}

#[test]
fn apply_init_values_unknown_procedure_is_ignored() {
    let topology = build_topology_for::<CounterOperative>().expect("topology");
    let instance = UnboundedInstance::new(topology, Arc::from("inst-6"));

    apply_init_values(
        &instance,
        "{\"counter\":{\"value\":7},\"bogus_procedure\":{\"x\":1}}",
    )
    .unwrap();
    let bytes = serialize_operative_state(&instance);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v, json!({"counter": {"value": 7}}));
}

#[test]
fn round_trip_static_mode_through_serialize_then_apply() {
    let topology = build_topology_for::<OhlcOperative>().expect("topology");
    let src = UnboundedInstance::new(Arc::clone(&topology), Arc::from("src"));
    apply_init_values(
        &src,
        "{\"ohlc\":{\"1m\":{\"high\":42},\"1h\":{\"high\":7}}}",
    )
    .unwrap();

    let bytes = serialize_operative_state(&src);
    let json_str = std::str::from_utf8(&bytes).unwrap();

    let dst = UnboundedInstance::new(topology, Arc::from("dst"));
    apply_init_values(&dst, json_str).unwrap();

    let dst_bytes = serialize_operative_state(&dst);
    assert_eq!(bytes, dst_bytes);
}

// ----- Live read: selector, dynamic coverage, seq, refusals -----

/// Two Procedures on one Operative, so a `procedure` selector has something to
/// exclude.
struct PairOperative;
impl UnboundedOperative for PairOperative {
    fn factory_id() -> &'static str {
        "pair_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("counter_in").into::<CounterProcedure>();
        c.input::<String>("ohlc_in").into::<OhlcProcedure>();
    }
}

/// Dynamic-mode: Entities exist only once a key has been seen.
#[derive(Default, Serialize, Deserialize, PartialEq, Eq, Debug)]
struct CandleState {
    ticks: i64,
}

#[derive(Default)]
struct CandleProcedure;
impl Procedure for CandleProcedure {
    type Input = String;
    type Output = ();
    type State = CandleState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "candle"
    }
    fn init_state(_: Option<&str>, _p: Option<RestoreInitPayload<'_>>) -> CandleState {
        CandleState::default()
    }
    fn entity_idle_ttl() -> Option<std::time::Duration> {
        Some(std::time::Duration::from_millis(50))
    }
    fn run(
        &mut self,
        _: String,
        _: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

struct CandleOperative;
impl UnboundedOperative for CandleOperative {
    fn factory_id() -> &'static str {
        "candle_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .key_by(|s: &String| s.clone())
            .into::<CandleProcedure>();
    }
}

fn read(
    instance: &Arc<UnboundedInstance>,
    procedure: Option<&str>,
    key: Option<&str>,
) -> (Value, Option<u64>) {
    let (bytes, seq) = serialize_selected_state(
        instance,
        StateSelector {
            procedure,
            entity_key: key,
        },
        StatePurpose::LiveRead,
    )
    .expect("selected read");
    (serde_json::from_slice(&bytes).unwrap(), seq)
}

#[test]
fn a_selector_narrows_to_one_entity_of_many_and_carries_its_seq() {
    let (_handle, instance) = setup::<PairOperative>("sel-1");

    let (all, all_seq) = read(&instance, None, None);
    assert_eq!(
        all,
        json!({"counter": {"value": 0}, "ohlc": {"1m": {"high": 0}, "1h": {"high": 0}}}),
        "no selector covers every Procedure"
    );
    assert_eq!(all_seq, None, "a multi-Entity read names no single version");

    let (procedure_only, _) = read(&instance, Some("ohlc"), None);
    assert_eq!(
        procedure_only,
        json!({"ohlc": {"1m": {"high": 0}, "1h": {"high": 0}}}),
        "a procedure selector excludes the other Procedure entirely"
    );

    let (one, seq) = read(&instance, Some("ohlc"), Some("1m"));
    assert_eq!(one, json!({"ohlc": {"1m": {"high": 0}}}));
    assert_eq!(seq, Some(0), "one Entity ⇒ its change_seq is reported");
}

#[test]
fn a_single_mode_procedure_selector_names_exactly_one_entity() {
    let (_handle, instance) = setup::<PairOperative>("sel-2");
    let (value, seq) = read(&instance, Some("counter"), None);

    assert_eq!(value, json!({"counter": {"value": 0}}));
    assert_eq!(seq, Some(0));
}

#[test]
fn dynamic_mode_entities_serialize_under_their_live_keys() {
    let (_handle, instance) = setup::<CandleOperative>("dyn-read-1");
    let registry = instance.dynamic_registries[0]
        .as_ref()
        .expect("candle is dynamic");
    registry.get_or_create("BTCUSD@1m").expect("create");
    registry.get_or_create("ETHUSD@1m").expect("create");

    let (all, _) = read(&instance, None, None);
    assert_eq!(
        all,
        json!({"candle": {"BTCUSD@1m": {"ticks": 0}, "ETHUSD@1m": {"ticks": 0}}}),
        "a dynamic Procedure reports the keys its registry holds"
    );

    let (one, seq) = read(&instance, Some("candle"), Some("BTCUSD@1m"));
    assert_eq!(one, json!({"candle": {"BTCUSD@1m": {"ticks": 0}}}));
    assert_eq!(seq, Some(0));
}

#[test]
fn an_entity_key_without_a_procedure_is_refused() {
    let (_handle, instance) = setup::<PairOperative>("sel-3");
    let err = serialize_selected_state(
        &instance,
        StateSelector {
            procedure: None,
            entity_key: Some("1m"),
        },
        StatePurpose::LiveRead,
    )
    .expect_err("an Entity key is unique only within its Procedure");

    assert!(
        err.starts_with(ENTITY_KEY_WITHOUT_PROCEDURE),
        "refusal leads with its canonical token, got: {err}"
    );
}

#[test]
fn an_unknown_procedure_is_refused() {
    let (_handle, instance) = setup::<PairOperative>("sel-4");
    let err = serialize_selected_state(
        &instance,
        StateSelector {
            procedure: Some("nope"),
            entity_key: None,
        },
        StatePurpose::LiveRead,
    )
    .expect_err("no such Procedure");

    assert!(err.starts_with(PROCEDURE_NOT_FOUND), "got: {err}");
}

#[test]
fn an_unknown_key_is_refused_distinctly_from_an_evicted_one() {
    let (_handle, instance) = setup::<CandleOperative>("dyn-read-2");
    let registry = instance.dynamic_registries[0]
        .as_ref()
        .expect("candle is dynamic");
    registry.get_or_create("BTCUSD@1m").expect("create");

    let never = serialize_selected_state(
        &instance,
        StateSelector {
            procedure: Some("candle"),
            entity_key: Some("NEVER"),
        },
        StatePurpose::LiveRead,
    )
    .expect_err("no Entity by that key");
    assert!(never.starts_with(ENTITY_NOT_FOUND), "got: {never}");

    // Evict destructively, then read the same key back. Backdating the
    // activity stamp makes the sweep decide on the TTL, not on the clock.
    registry
        .entities
        .get("BTCUSD@1m")
        .expect("live before the sweep")
        .last_activity_at
        .store(-1_000, std::sync::atomic::Ordering::Relaxed);
    registry.reap_sweep(std::time::Duration::ZERO);
    assert!(registry.entities.is_empty(), "the idle Entity is evicted");

    let evicted = serialize_selected_state(
        &instance,
        StateSelector {
            procedure: Some("candle"),
            entity_key: Some("BTCUSD@1m"),
        },
        StatePurpose::LiveRead,
    )
    .expect_err("an evicted Entity is refused, not resurrected");
    assert!(
        evicted.starts_with(ENTITY_EVICTED),
        "eviction is a distinct diagnosis, got: {evicted}"
    );

    let (surviving, _) = read(&instance, Some("candle"), None);
    assert_eq!(
        surviving,
        json!({"candle": {}}),
        "the refused read created nothing"
    );
}

/// A read must see what `run` applied, whether or not the flusher has shipped
/// it. The Entity is deregistered from the flusher so "not yet flushed" is a
/// fact of the test rather than a race against the linger window, and its
/// `change_seq` is armed at 7 to stand for "records 0..6 are durable".
#[test]
fn a_read_reflects_an_applied_but_unflushed_change_and_reports_a_seq_above_the_persisted_one() {
    let (handle, instance) = setup::<CounterOperative>("unflushed-1");

    let slot = Arc::clone(&instance.entities[0][0]);
    let entry_id = slot
        .flusher_entry_id
        .expect("a changeful Entity is registered");
    flezha_runner::entity::flusher::EntityFlusher::instance().deregister(entry_id);
    slot.change_seq
        .store(7, std::sync::atomic::Ordering::Relaxed);

    dispatch_input(
        &handle,
        "unflushed-1",
        "input",
        b"5".to_vec(),
        &dummy_stream(),
    );

    assert!(
        wait_until(
            || read(&instance, Some("counter"), None).0 == json!({"counter": {"value": 5}}),
            std::time::Duration::from_secs(5)
        ),
        "the read must reflect the change `run` applied"
    );

    let (_, seq) = read(&instance, Some("counter"), None);
    assert_eq!(
        seq,
        Some(7),
        "the reported seq is the Entity's next WAL seq — above every persisted one"
    );
    assert!(
        !slot.flush_buffer.lock().unwrap().is_empty(),
        "the change is still buffered: the read reflected state ahead of the WAL"
    );
}

/// A read is not a compaction: it drains nothing, ships no snapshot, and moves
/// no seq. Those three are exactly what compaction does before it truncates.
#[test]
fn a_read_leaves_the_entity_undrained_and_its_seq_unchanged() {
    let (handle, instance) = setup::<CounterOperative>("noside-1");

    let slot = Arc::clone(&instance.entities[0][0]);
    let entry_id = slot
        .flusher_entry_id
        .expect("a changeful Entity is registered");
    flezha_runner::entity::flusher::EntityFlusher::instance().deregister(entry_id);
    slot.change_seq
        .store(3, std::sync::atomic::Ordering::Relaxed);

    dispatch_input(&handle, "noside-1", "input", b"1".to_vec(), &dummy_stream());
    assert!(
        wait_until(
            || !slot.flush_buffer.lock().unwrap().is_empty(),
            std::time::Duration::from_secs(5)
        ),
        "the dispatch must buffer a change to have something to preserve"
    );

    for _ in 0..3 {
        let _ = read(&instance, Some("counter"), None);
    }

    assert_eq!(
        slot.change_seq.load(std::sync::atomic::Ordering::Relaxed),
        3,
        "reads do not consume seqs"
    );
    assert!(
        !slot.flush_buffer.lock().unwrap().is_empty(),
        "reads do not drain the flush buffer"
    );
}

/// The selector rides two proto3 string fields, so leaving them unset encodes
/// to nothing — the migration/handover caller's frame is byte-identical to the
/// pre-selector one.
#[test]
fn a_selector_less_entity_state_command_encodes_exactly_as_before() {
    use prost::Message;

    let selectorless = flezha_runner::pb::EntityState {
        operative_id: "op-1".to_string(),
        procedure: String::new(),
        entity_key: String::new(),
    };

    // Field 1 (`operative_id`), length-delimited, four bytes of "op-1".
    assert_eq!(
        selectorless.encode_to_vec(),
        vec![0x0a, 0x04, b'o', b'p', b'-', b'1'],
    );

    let selected = flezha_runner::pb::EntityState {
        operative_id: "op-1".to_string(),
        procedure: "candle".to_string(),
        entity_key: "BTCUSD@1m".to_string(),
    };
    assert!(selected.encode_to_vec().len() > selectorless.encode_to_vec().len());
}

/// Pinned cross-language fixture: the identical literal is asserted by
/// `jsonShapeMatchesTheRustSdk` in the Java SDK's
/// `UnboundedOperativeStateSerializerTest`. Both encoders sort object keys, so
/// a controller decoding either runtime's reply sees one shape.
#[test]
fn the_json_shape_matches_the_java_sdk_byte_for_byte() {
    let (_handle, instance) = setup::<PairOperative>("cross-1");
    apply_init_values(
        &instance,
        "{\"counter\":{\"value\":1},\"ohlc\":{\"1m\":{\"high\":2},\"1h\":{\"high\":3}}}",
    )
    .unwrap();

    assert_eq!(
        String::from_utf8(serialize_operative_state(&instance)).unwrap(),
        "{\"counter\":{\"value\":1},\"ohlc\":{\"1h\":{\"high\":3},\"1m\":{\"high\":2}}}"
    );
}
