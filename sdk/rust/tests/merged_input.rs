//! `multi-input-procedures`: a procedure fed by two or more source channels,
//! each subscription mapping its payload into one sum-type `Input`.
//!
//! Covers the capability's contract end to end:
//! - two typed inter-procedure channels feeding one procedure, whose own
//!   output keeps behaving like any single-input procedure's
//! - entry channels as merge sources (no parser-procedure detour)
//! - per-source FIFO under interleaving, with nothing promised across sources
//! - a replayed slice mapped exactly as the live delivery mapped it
//! - `entities_for` evaluated on the MAPPED value
//! - the definition-time refusals (fewer than two sources, a duplicated source)

use flezha_runner::channel::builder::TopologyIssue;

mod common;
use common::prelude::*;
use common::{dispatch_input, dispatch_marked, dummy_stream, setup, wait_until};

// ----- Payloads -----

#[derive(Clone, Debug, PartialEq, Eq)]
struct Ticker(String);

#[derive(Clone, Debug, PartialEq, Eq)]
struct Candle(u64);

/// The author-declared sum type a merged procedure consumes.
#[derive(Clone, Debug, PartialEq, Eq)]
enum SignalInput {
    Ticker(Ticker),
    Candle(Candle),
}

/// The entry-fed merge's input: a data feed plus a live config channel — the
/// shape a runtime-reconfigurable procedure needs. Serde-derived only so one
/// validation case can declare it as an entry channel's payload.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum TunedInput {
    Data(String),
    Config(String),
}

// ----- 1: two typed channels feed one procedure -----

static SIGNALS: Mutex<Vec<SignalInput>> = Mutex::new(Vec::new());
static SIGNAL_SINK: Mutex<Vec<String>> = Mutex::new(Vec::new());

test_procedure!(TickerProcedure, "merge_ticker_procedure", String => Ticker, (), |input, _state, ctx| {
    ctx.emit(Ticker(input))
});

test_procedure!(CandleProcedure, "merge_candle_procedure", String => Candle, (), |input, _state, ctx| {
    ctx.emit(Candle(input.len() as u64))
});

test_procedure!(DetectorProcedure, "merge_detector_procedure", SignalInput => String, (), |input, _state, ctx| {
    SIGNALS.lock().unwrap().push(input.clone());
    let label = match input {
        SignalInput::Ticker(t) => format!("ticker:{}", t.0),
        SignalInput::Candle(c) => format!("candle:{}", c.0),
    };
    ctx.emit(label)
});

test_procedure!(SignalSinkProcedure, "merge_signal_sink_procedure", String => String, (), |input, _state, _ctx| {
    SIGNAL_SINK.lock().unwrap().push(input);
    Ok(())
});

struct TypedMergeOperative;
impl StreamOperative for TypedMergeOperative {
    fn factory_id() -> &'static str {
        "typed_merge_operative"
    }
    fn channels(c: &ChannelBuilder) {
        let raw = c.input::<String>("raw");
        let ticker = raw.into::<TickerProcedure>().named("ticker_out");
        let candles = raw.into::<CandleProcedure>().named("ohlc_out");
        c.merge::<SignalInput>()
            .from(ticker, SignalInput::Ticker)
            .from(candles, SignalInput::Candle)
            .into::<DetectorProcedure>()
            .into::<SignalSinkProcedure>();
    }
}

#[test]
fn two_typed_channels_feed_one_procedure() {
    SIGNALS.lock().unwrap().clear();
    SIGNAL_SINK.lock().unwrap().clear();
    let (handle, _instance) = setup::<TypedMergeOperative>("typed-merge-instance");
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "typed-merge-instance",
        "raw",
        b"abc".to_vec(),
        &stream,
    );

    // One raw frame fans out to both producers, so the merged procedure sees
    // one value per source — through its ordinary `run`, as one type.
    assert!(
        wait_until(
            || SIGNALS.lock().unwrap().len() == 2,
            Duration::from_secs(5)
        ),
        "merged delivery did not complete: {:?}",
        SIGNALS.lock().unwrap()
    );

    let mut observed = SIGNALS.lock().unwrap().clone();
    observed.sort_by_key(|s| matches!(s, SignalInput::Candle(_)));
    assert_eq!(
        observed,
        vec![
            SignalInput::Ticker(Ticker("abc".to_string())),
            SignalInput::Candle(Candle(3)),
        ]
    );

    // ...and the merged node's own output channel behaves like any other.
    assert!(
        wait_until(
            || SIGNAL_SINK.lock().unwrap().len() == 2,
            Duration::from_secs(5)
        ),
        "merged procedure's output did not reach its consumer: {:?}",
        SIGNAL_SINK.lock().unwrap()
    );
    let mut emitted = SIGNAL_SINK.lock().unwrap().clone();
    emitted.sort();
    assert_eq!(
        emitted,
        vec!["candle:3".to_string(), "ticker:abc".to_string()]
    );
}

// ----- 2: entry channels as merge sources -----

static TUNED: Mutex<Vec<TunedInput>> = Mutex::new(Vec::new());

/// The three entry-merge tests observe through one static, so they run one at
/// a time. Held across each test body; poison is recovered because a failing
/// test must not cascade into the others.
static TUNED_TEST: Mutex<()> = Mutex::new(());

fn tuned_test_guard() -> std::sync::MutexGuard<'static, ()> {
    let guard = TUNED_TEST.lock().unwrap_or_else(|e| e.into_inner());
    TUNED.lock().unwrap_or_else(|e| e.into_inner()).clear();
    guard
}

test_procedure!(TunedProcedure, "merge_tuned_procedure", TunedInput => String, (), |input, _state, _ctx| {
    TUNED.lock().unwrap().push(input);
    Ok(())
});

struct EntryMergeOperative;
impl StreamOperative for EntryMergeOperative {
    fn factory_id() -> &'static str {
        "entry_merge_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.merge::<TunedInput>()
            .from(c.input::<String>("data"), TunedInput::Data)
            .from(c.input::<String>("config"), TunedInput::Config)
            .into::<TunedProcedure>();
    }
}

#[test]
fn entry_channels_are_merge_sources() {
    let _serial = tuned_test_guard();
    let (handle, _instance) = setup::<EntryMergeOperative>("entry-merge-instance");
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "entry-merge-instance",
        "config",
        b"threshold=7".to_vec(),
        &stream,
    );
    dispatch_input(
        &handle,
        "entry-merge-instance",
        "data",
        b"tick-1".to_vec(),
        &stream,
    );

    assert!(
        wait_until(|| TUNED.lock().unwrap().len() == 2, Duration::from_secs(5)),
        "entry-fed merge did not deliver: {:?}",
        TUNED.lock().unwrap()
    );
    let observed = TUNED.lock().unwrap().clone();
    assert!(observed.contains(&TunedInput::Config("threshold=7".to_string())));
    assert!(observed.contains(&TunedInput::Data("tick-1".to_string())));
}

#[test]
fn per_source_order_holds_under_interleaving() {
    let _serial = tuned_test_guard();
    let (handle, _instance) = setup::<EntryMergeOperative>("entry-fifo-instance");
    let stream = dummy_stream();

    let n = 200usize;
    for i in 0..n {
        dispatch_input(
            &handle,
            "entry-fifo-instance",
            "data",
            format!("d{i}").into_bytes(),
            &stream,
        );
        if i % 20 == 0 {
            dispatch_input(
                &handle,
                "entry-fifo-instance",
                "config",
                format!("c{i}").into_bytes(),
                &stream,
            );
        }
    }
    let expected_configs = (0..n).filter(|i| i % 20 == 0).count();

    assert!(
        wait_until(
            || TUNED.lock().unwrap().len() == n + expected_configs,
            Duration::from_secs(5)
        ),
        "interleaved merge did not drain: {}",
        TUNED.lock().unwrap().len()
    );

    // Per source: exactly the order it was produced in. Across sources:
    // nothing is promised, and nothing is asserted.
    let observed = TUNED.lock().unwrap().clone();
    let data: Vec<String> = observed
        .iter()
        .filter_map(|v| match v {
            TunedInput::Data(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    let configs: Vec<String> = observed
        .iter()
        .filter_map(|v| match v {
            TunedInput::Config(s) => Some(s.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        data,
        (0..n).map(|i| format!("d{i}")).collect::<Vec<_>>(),
        "source `data` lost its FIFO order"
    );
    assert_eq!(
        configs,
        (0..n)
            .filter(|i| i % 20 == 0)
            .map(|i| format!("c{i}"))
            .collect::<Vec<_>>(),
        "source `config` lost its FIFO order"
    );
}

#[test]
fn a_replayed_merge_maps_exactly_as_the_live_run_did() {
    let _serial = tuned_test_guard();
    let (handle, _instance) = setup::<EntryMergeOperative>("entry-replay-instance");
    let stream = dummy_stream();

    let feed = [("data", "d0"), ("config", "c0"), ("data", "d1")];
    for (channel, payload) in feed {
        dispatch_input(
            &handle,
            "entry-replay-instance",
            channel,
            payload.as_bytes().to_vec(),
            &stream,
        );
    }
    assert!(
        wait_until(|| TUNED.lock().unwrap().len() == 3, Duration::from_secs(5)),
        "live run did not drain"
    );
    let live = TUNED.lock().unwrap().clone();

    // The same frames re-forwarded as a replayed slice: the mapping is the
    // channel's, not the delivery's, so a warm restart folds the same values.
    for (offset, (channel, payload)) in feed.iter().enumerate() {
        dispatch_marked(
            &handle,
            "entry-replay-instance",
            channel,
            payload.as_bytes().to_vec(),
            &stream,
            &[
                ("flz.replay", "1"),
                ("flz.offset", &(offset + 1).to_string()),
            ],
        );
    }
    dispatch_marked(
        &handle,
        "entry-replay-instance",
        "data",
        Vec::new(),
        &stream,
        &[("flz.replay_end", "1")],
    );

    assert!(
        wait_until(|| TUNED.lock().unwrap().len() == 6, Duration::from_secs(5)),
        "replayed slice did not drain: {:?}",
        TUNED.lock().unwrap()
    );
    let replayed = TUNED.lock().unwrap()[3..].to_vec();
    assert_eq!(
        replayed, live,
        "replay must map identically to live delivery"
    );
}

// ----- 3: entity derivation runs on the MAPPED value -----

static PER_ENTITY: Mutex<Vec<(String, TunedInput)>> = Mutex::new(Vec::new());

#[derive(Default)]
struct EntityTunedProcedure;
impl Procedure for EntityTunedProcedure {
    type Input = TunedInput;
    type Output = String;
    type State = String;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "merge_entity_tuned_procedure"
    }
    fn init_state(entity_key: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> String {
        entity_key.unwrap_or("?").to_string()
    }
    fn entity_keys() -> Vec<&'static str> {
        vec!["data", "config"]
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(input: &TunedInput) -> Vec<&'static str> {
        match input {
            TunedInput::Data(_) => vec!["data"],
            TunedInput::Config(_) => vec!["config"],
        }
    }
    fn run(
        &mut self,
        input: TunedInput,
        state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        PER_ENTITY.lock().unwrap().push((state.clone(), input));
        Ok(())
    }
}

struct EntityMergeOperative;
impl StreamOperative for EntityMergeOperative {
    fn factory_id() -> &'static str {
        "entity_merge_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.merge::<TunedInput>()
            .from(c.input::<String>("data"), TunedInput::Data)
            .from(c.input::<String>("config"), TunedInput::Config)
            .into::<EntityTunedProcedure>();
    }
}

#[test]
fn entities_for_is_evaluated_on_the_mapped_value() {
    PER_ENTITY.lock().unwrap().clear();
    let (handle, _instance) = setup::<EntityMergeOperative>("entity-merge-instance");
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "entity-merge-instance",
        "data",
        b"d0".to_vec(),
        &stream,
    );
    dispatch_input(
        &handle,
        "entity-merge-instance",
        "config",
        b"c0".to_vec(),
        &stream,
    );

    assert!(
        wait_until(
            || PER_ENTITY.lock().unwrap().len() == 2,
            Duration::from_secs(5)
        ),
        "entity merge did not drain: {:?}",
        PER_ENTITY.lock().unwrap()
    );

    // The routing key came from the sum type, not from the source channel:
    // `entities_for` never sees the pre-map payload.
    let mut observed = PER_ENTITY.lock().unwrap().clone();
    observed.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        observed,
        vec![
            ("config".to_string(), TunedInput::Config("c0".to_string())),
            ("data".to_string(), TunedInput::Data("d0".to_string())),
        ]
    );
}

// ----- 4: definition-time validation -----

struct OneSourceMergeOperative;
impl StreamOperative for OneSourceMergeOperative {
    fn factory_id() -> &'static str {
        "one_source_merge_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.merge::<TunedInput>()
            .from(c.input::<String>("data"), TunedInput::Data)
            .into::<TunedProcedure>();
    }
}

#[test]
fn a_merge_with_fewer_than_two_sources_is_refused() {
    let err = build_topology_for::<OneSourceMergeOperative>()
        .err()
        .expect("a one-source merge must fail definition");
    assert!(
        err.errors.iter().any(|e| matches!(
            e,
            TopologyIssue::MergeTooFewSources {
                procedure: "merge_tuned_procedure",
                sources: 1
            }
        )),
        "expected MergeTooFewSources, got {:?}",
        err.errors
    );
}

struct DuplicateSourceMergeOperative;
impl StreamOperative for DuplicateSourceMergeOperative {
    fn factory_id() -> &'static str {
        "duplicate_source_merge_operative"
    }
    fn channels(c: &ChannelBuilder) {
        let data = c.input::<String>("data");
        c.merge::<TunedInput>()
            .from(data, TunedInput::Data)
            .from(data, TunedInput::Config)
            .into::<TunedProcedure>();
    }
}

#[test]
fn a_duplicated_merge_source_is_refused_naming_the_channel() {
    let err = build_topology_for::<DuplicateSourceMergeOperative>()
        .err()
        .expect("a duplicated merge source must fail definition");
    assert!(
        err.errors
            .iter()
            .any(|e| matches!(e, TopologyIssue::DuplicateMergeSource { channel: "data" })),
        "expected DuplicateMergeSource naming `data`, got {:?}",
        err.errors
    );
    // ...and ONLY that. The author declared two sources; reporting the merge
    // as under-declared as well would send them to fix the wrong thing.
    assert!(
        !err.errors
            .iter()
            .any(|e| matches!(e, TopologyIssue::MergeTooFewSources { .. })),
        "a duplicated source must not also read as too few sources: {:?}",
        err.errors
    );
}

/// Two merges closing on ONE consumer that share a source channel — the shape
/// a graph assembled by helper functions falls into. The second `.from` cannot
/// be honoured (an edge carries one mapping), so it must be refused rather
/// than dropped: the procedure would otherwise receive the first declaration's
/// value forever, with nothing said at definition time or at runtime.
struct RewiredMergeSourceOperative;
impl StreamOperative for RewiredMergeSourceOperative {
    fn factory_id() -> &'static str {
        "rewired_merge_source_operative"
    }
    fn channels(c: &ChannelBuilder) {
        let data = c.input::<String>("data");
        c.merge::<TunedInput>()
            .from(data, TunedInput::Data)
            .from(c.input::<String>("config"), TunedInput::Config)
            .into::<TunedProcedure>();
        c.merge::<TunedInput>()
            .from(data, TunedInput::Config)
            .from(c.input::<String>("extra"), TunedInput::Data)
            .into::<TunedProcedure>();
    }
}

#[test]
fn a_merge_source_may_not_re_wire_an_edge_that_is_already_declared() {
    let err = build_topology_for::<RewiredMergeSourceOperative>()
        .err()
        .expect("re-wiring a declared edge with a second mapping must fail definition");
    assert!(
        err.errors.iter().any(|e| matches!(
            e,
            TopologyIssue::EdgeAlreadyWired {
                channel: "data",
                procedure: "merge_tuned_procedure"
            }
        )),
        "expected EdgeAlreadyWired naming `data`, got {:?}",
        err.errors
    );
}

/// The same edge claimed first by a plain `.into` and then by a merge: the
/// channel's payload cannot both BE the input and be mapped into it.
struct PlainThenMergedOperative;
impl StreamOperative for PlainThenMergedOperative {
    fn factory_id() -> &'static str {
        "plain_then_merged_operative"
    }
    fn channels(c: &ChannelBuilder) {
        let data = c.input::<TunedInput>("data");
        data.into::<TunedProcedure>();
        c.merge::<TunedInput>()
            .from(data, |v: TunedInput| v)
            .from(c.input::<String>("config"), TunedInput::Config)
            .into::<TunedProcedure>();
    }
}

#[test]
fn a_merge_may_not_re_wire_an_edge_a_plain_into_already_declared() {
    let err = build_topology_for::<PlainThenMergedOperative>()
        .err()
        .expect("merging onto an edge a plain `.into` declared must fail definition");
    assert!(
        err.errors.iter().any(|e| matches!(
            e,
            TopologyIssue::EdgeAlreadyWired {
                channel: "data",
                procedure: "merge_tuned_procedure"
            }
        )),
        "expected EdgeAlreadyWired naming `data`, got {:?}",
        err.errors
    );
}
