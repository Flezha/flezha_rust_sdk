//! `stream-transforms`: edge-closure value transforms (`map`, `filter`,
//! `flat_map`) and edge-side Cell keying (`key_by`) on the graph-authoring
//! surface.
//!
//! Covers the capability's contract end to end:
//! - a chain of transforms is ONE closure on ONE edge, traversed once
//! - `flat_map` fans one input out across distinct Cells, each keyed
//!   independently
//! - `key_by` sees the post-transform value
//! - a replayed slice runs the same closures the live delivery ran
//! - a panicking transform survives routing and parks to the dead-letter queue
//! - every definition-time refusal

use flezha_runner::channel::builder::TopologyIssue;

mod common;
use common::prelude::*;
use common::{
    dispatch_input, dispatch_marked, dummy_stream, install_auto_acking_sink, setup, wait_until,
};

/// Serializes every case that drives real dispatch here: they share
/// `ChainOperator`'s process-global observation statics, and the parking case
/// holds a poll worker inside `park_blocking` until its ack thread answers —
/// concurrently with a replay drain, that worker is one the drain never gets.
static CHAIN_LOCK: Mutex<()> = Mutex::new(());

/// Take a process-global test lock, tolerating a poisoned one — a failed
/// assertion elsewhere must not cascade into an unrelated case.
fn guard(lock: &'static Mutex<()>) -> std::sync::MutexGuard<'static, ()> {
    lock.lock().unwrap_or_else(|e| e.into_inner())
}

// ----- Payloads -----

#[derive(Clone, Debug, PartialEq, Eq)]
struct Trade {
    symbol: String,
    qty: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Slot {
    symbol: String,
    timeframe: &'static str,
}

const TIMEFRAMES: [&str; 4] = ["1m", "5m", "1h", "1d"];

fn parse_trade(raw: &str) -> Trade {
    let (symbol, qty) = raw.split_once(':').unwrap_or((raw, "1"));
    Trade {
        symbol: symbol.to_string(),
        qty: qty.parse().unwrap_or(0),
    }
}

// ----- 1: a chain of transforms is one closure on one edge -----

/// Every value the keyed consumer observed. The Cell each reached is read
/// off the dynamic registry, which is the runtime's own record of the keys.
static OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// Bumped once per traversal of the composed closure, proving the chain did
/// not become three edges the runtime walks in turn.
static TRAVERSALS: AtomicUsize = AtomicUsize::new(0);

test_tasklet!(Recorder, "transform_recorder", String => (), (), |input, _ctx| {
    OBSERVED.lock().unwrap().push(input);
    Ok(())
});

/// `filter` → `map` → `map` → `key_by`, all on the one entry→Recorder edge.
struct ChainOperator;
impl UnboundedOperator for ChainOperator {
    fn factory_id() -> &'static str {
        "transform_chain_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .filter(|raw: &String| {
                TRAVERSALS.fetch_add(1, Ordering::Relaxed);
                !raw.starts_with('#')
            })
            .map(|raw: String| parse_trade(&raw))
            .map(|trade: Trade| format!("{}x{}", trade.symbol, trade.qty))
            .key_by(|shaped: &String| shaped.split('x').next().unwrap_or("?").to_string())
            .into::<Recorder>();
    }
}

#[test]
fn a_transform_chain_is_one_closure_on_one_edge() {
    let _serial = guard(&CHAIN_LOCK);
    let topology = build_topology_for::<ChainOperator>().expect("topology");
    assert_eq!(
        topology.tasklets.len(),
        1,
        "four transforms added no Tasklet node"
    );
    let edges = &topology.channels[0].consumers;
    assert_eq!(edges.len(), 1);
    assert!(
        edges[0].transform.is_some() && edges[0].key.is_some(),
        "the chain collapsed onto the one consumer edge"
    );

    OBSERVED.lock().unwrap().clear();
    TRAVERSALS.store(0, Ordering::Relaxed);
    let (handle, instance) = setup::<ChainOperator>("chain-1");
    let stream = dummy_stream();
    for raw in ["AAPL:3", "#comment", "MSFT:7"] {
        dispatch_input(&handle, "chain-1", "in", raw.as_bytes().to_vec(), &stream);
    }

    assert!(
        wait_until(
            || OBSERVED.lock().unwrap().len() == 2,
            Duration::from_secs(5)
        ),
        "the filtered value must not reach the consumer: {:?}",
        OBSERVED.lock().unwrap()
    );
    let mut seen = OBSERVED.lock().unwrap().clone();
    seen.sort();
    assert_eq!(
        seen,
        vec!["AAPLx3".to_string(), "MSFTx7".to_string()],
        "the consumer observed the value the two maps produced"
    );
    assert_eq!(
        cell_keys(&instance),
        vec!["AAPL".to_string(), "MSFT".to_string()],
        "the key ran on the MAPPED value — it split `AAPLx3`, not `AAPL:3`"
    );
    assert_eq!(
        TRAVERSALS.load(Ordering::Relaxed),
        3,
        "one traversal per input — the chain is one closure, not three edges"
    );
}

// ----- 2: flat_map fans one input out across distinct Cells -----

static FANNED: Mutex<Vec<String>> = Mutex::new(Vec::new());

test_tasklet!(FanRecorder, "transform_fan_recorder", Slot => (), (), |input, _ctx| {
    FANNED.lock().unwrap().push(input.timeframe.to_string());
    Ok(())
});

struct FanOutOperator;
impl UnboundedOperator for FanOutOperator {
    fn factory_id() -> &'static str {
        "transform_fanout_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .flat_map(|raw: String| {
                let trade = parse_trade(&raw);
                TIMEFRAMES.iter().map(move |tf| Slot {
                    symbol: trade.symbol.clone(),
                    timeframe: tf,
                })
            })
            .key_by(|slot: &Slot| format!("{}@{}", slot.symbol, slot.timeframe))
            .into::<FanRecorder>();
    }
}

#[test]
fn a_fan_out_keys_every_yielded_value_independently() {
    FANNED.lock().unwrap().clear();
    let (handle, instance) = setup::<FanOutOperator>("fan-1");
    let stream = dummy_stream();
    dispatch_input(&handle, "fan-1", "in", b"AAPL:3".to_vec(), &stream);

    assert!(
        wait_until(|| FANNED.lock().unwrap().len() == 4, Duration::from_secs(5)),
        "one trade yields one value per timeframe: {:?}",
        FANNED.lock().unwrap()
    );
    assert_eq!(
        cell_keys(&instance),
        vec!["AAPL@1d", "AAPL@1h", "AAPL@1m", "AAPL@5m"],
        "four yielded values reached four distinct Cells, each keyed on its own"
    );
}

// ----- 3: replay runs the same closures the live delivery ran -----

#[test]
fn a_replayed_slice_transforms_exactly_as_the_live_run_did() {
    let _serial = guard(&CHAIN_LOCK);
    OBSERVED.lock().unwrap().clear();
    TRAVERSALS.store(0, Ordering::Relaxed);
    let (handle, instance) = setup::<ChainOperator>("chain-replay");
    let stream = dummy_stream();

    let feed = ["AAPL:1", "#skip", "MSFT:2"];
    for raw in feed {
        dispatch_input(
            &handle,
            "chain-replay",
            "in",
            raw.as_bytes().to_vec(),
            &stream,
        );
    }
    assert!(wait_until(
        || OBSERVED.lock().unwrap().len() == 2,
        Duration::from_secs(5)
    ));
    let mut live = OBSERVED.lock().unwrap().clone();
    live.sort();

    // The same frames re-forwarded as a replayed slice. The closures live on
    // the channel's edge, not on the delivery, so a warm restart folds the
    // identical values into the identical Cells.
    for (offset, raw) in feed.iter().enumerate() {
        dispatch_marked(
            &handle,
            "chain-replay",
            "in",
            raw.as_bytes().to_vec(),
            &stream,
            &[
                ("flz.replay", "1"),
                ("flz.offset", &(offset + 1).to_string()),
            ],
        );
    }
    dispatch_marked(
        &handle,
        "chain-replay",
        "in",
        Vec::new(),
        &stream,
        &[("flz.replay_end", "1")],
    );

    assert!(
        wait_until(
            || OBSERVED.lock().unwrap().len() == 4,
            Duration::from_secs(5)
        ),
        "replayed slice did not drain: {:?}",
        OBSERVED.lock().unwrap()
    );
    // Two Cells drain concurrently, so only the per-Cell content is
    // promised — the set of values, and the set of Cells they built.
    let mut replayed = OBSERVED.lock().unwrap()[2..].to_vec();
    replayed.sort();
    assert_eq!(
        replayed, live,
        "replay must fold the same values the live run folded"
    );
    assert_eq!(
        cell_keys(&instance),
        vec!["AAPL".to_string(), "MSFT".to_string()],
        "replay must reach the same Cells, creating no others"
    );
}

// ----- 4: a panicking transform is contained and parks -----

static AFTER_PARK: Mutex<Vec<String>> = Mutex::new(Vec::new());

test_tasklet!(PoisonRecorder, "transform_poison_recorder", String => (), (), |input, _ctx| {
    AFTER_PARK.lock().unwrap().push(input);
    Ok(())
});

struct PanickingTransformOperator;
impl UnboundedOperator for PanickingTransformOperator {
    fn factory_id() -> &'static str {
        "transform_panic_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .retry_max(2)
            .retry_backoff_ms(1)
            .map(|raw: String| {
                if raw == "poison" {
                    panic!("transform blew up");
                }
                raw
            })
            .key_by(|raw: &String| raw.clone())
            .into::<PoisonRecorder>();
    }
}

#[test]
fn a_panicking_transform_parks_and_routing_continues() {
    let _serial = guard(&CHAIN_LOCK);
    AFTER_PARK.lock().unwrap().clear();
    let parked = install_auto_acking_sink();

    let (handle, _instance) = setup::<PanickingTransformOperator>("panic-1");
    let stream = dummy_stream();
    for raw in ["ok-1", "poison", "ok-2"] {
        dispatch_input(&handle, "panic-1", "in", raw.as_bytes().to_vec(), &stream);
    }

    assert!(
        wait_until(
            || AFTER_PARK.lock().unwrap().len() == 2,
            Duration::from_secs(10)
        ),
        "the routing thread survived and kept serving later inputs: {:?}",
        AFTER_PARK.lock().unwrap()
    );
    assert!(
        wait_until(
            || parked.lock().unwrap().len() == 1,
            Duration::from_secs(10)
        ),
        "the poison value parked to the dead-letter queue"
    );
    let entry = parked.lock().unwrap()[0].entry.clone();
    assert_eq!(entry.channel, "in");
    assert_eq!(entry.attempts, 2, "the channel's retry budget was spent");
    assert!(
        entry.error.contains("edge transform panicked")
            && entry.error.contains("transform blew up"),
        "the park record carries the contained panic: {}",
        entry.error
    );
}

// ----- 5: definition-time refusals -----

test_tasklet!(PlainTarget, "transform_plain_target", String => Vec<u8>, (), |input, ctx| {
    ctx.emit(input.into_bytes())
});

macro_rules! refusal {
    ($operator:ident, $factory:literal, |$c:ident| $body:block) => {
        struct $operator;
        impl UnboundedOperator for $operator {
            fn factory_id() -> &'static str {
                $factory
            }
            fn channels($c: &ChannelBuilder) $body
        }
    };
}

refusal!(DuplicateKeyByOperator, "refuse_duplicate_key", |c| {
    c.input::<String>("in")
        .key_by(|s: &String| s.clone())
        .key_by(|s: &String| s.clone())
        .into::<Recorder>();
});

refusal!(DanglingOperator, "refuse_dangling", |c| {
    c.input::<String>("in").into::<Recorder>();
    let _dangling = c
        .input::<String>("other")
        .map(|s: String| s.len().to_string());
});

refusal!(AfterTerminalOperator, "refuse_after_terminal", |c| {
    c.input::<String>("in")
        .into::<PlainTarget>()
        .sink()
        .filter(|_b: &Vec<u8>| true)
        .into::<Recorder2>();
});

test_tasklet!(Recorder2, "transform_recorder_2", Vec<u8> => (), (), |_i, _c| { Ok(()) });

refusal!(AfterKeyByOperator, "refuse_after_key", |c| {
    c.input::<String>("in")
        .key_by(|s: &String| s.clone())
        .map(|s: String| s.to_uppercase())
        .into::<Recorder>();
});

refusal!(ConflictingRedeclarationOperator, "refuse_redeclared", |c| {
    let entry = c.input::<String>("in");
    entry.key_by(|s: &String| s.clone()).into::<Recorder>();
    entry
        .key_by(|s: &String| s.to_uppercase())
        .into::<Recorder>();
});

refusal!(MixedKeyingOperator, "refuse_mixed_keying", |c| {
    c.input::<String>("first")
        .key_by(|s: &String| s.clone())
        .into::<Recorder>();
    c.input::<String>("second").into::<Recorder>();
});

refusal!(
    BareRedeclarationOperator,
    "accept_bare_redeclaration",
    |c| {
        let entry = c.input::<String>("in");
        entry.into::<Recorder>();
        entry.into::<Recorder>();
    }
);

/// The live Cell keys of the operator's one dynamic Tasklet, sorted —
/// the runtime's own record of what the edge key produced.
fn cell_keys(instance: &Arc<flezha_runner::operator::OperatorInstance>) -> Vec<String> {
    let registry = instance.dynamic_registries[0]
        .as_ref()
        .expect("the keyed edge made the consumer dynamic");
    let mut keys: Vec<String> = registry.cells.iter().map(|e| e.key().clone()).collect();
    keys.sort();
    keys
}

fn issues<R: UnboundedOperator>() -> Vec<TopologyIssue> {
    build_topology_for::<R>()
        .err()
        .map(|e| e.errors)
        .unwrap_or_default()
}

#[test]
fn transform_declarations_are_validated() {
    assert!(
        issues::<DuplicateKeyByOperator>()
            .iter()
            .any(|e| matches!(e, TopologyIssue::DuplicateKeyBy { channel } if channel == "in")),
        "two key functions on one edge are refused"
    );
    assert!(
        issues::<DanglingOperator>().iter().any(|e| matches!(
            e,
            TopologyIssue::DanglingTransformChain { channel } if channel == "other"
        )),
        "a chain no `.into` closed is refused, naming the stream"
    );
    assert!(
        issues::<AfterTerminalOperator>()
            .iter()
            .any(|e| matches!(e, TopologyIssue::TransformAfterTerminal { .. })),
        "a transform behind a plugin sink is refused"
    );
    assert!(
        issues::<AfterKeyByOperator>().iter().any(
            |e| matches!(e, TopologyIssue::TransformAfterKeyBy { channel } if channel == "in")
        ),
        "a transform after the key is refused — the key reads the final value"
    );
    assert!(
        issues::<ConflictingRedeclarationOperator>()
            .iter()
            .any(|e| matches!(e, TopologyIssue::EdgeAlreadyWired { .. })),
        "re-declaring a keyed edge is refused rather than keeping either key"
    );
    assert!(
        issues::<MixedKeyingOperator>()
            .iter()
            .any(|e| matches!(e, TopologyIssue::UnkeyedEdgeIntoDynamicTasklet { .. })),
        "a dynamic consumer has no keyless path, so every edge into it must key"
    );

    // The transform-free re-declaration stays the legal fan-in no-op.
    let topology =
        build_topology_for::<BareRedeclarationOperator>().expect("bare re-declaration is legal");
    assert_eq!(topology.channels[0].consumers.len(), 1);
}

// ----- key_by onto a static-mode consumer -----

#[derive(Default)]
struct StaticFan;
#[flezha_runner::tasklet(name = "transform_static_fan", keys = ["a", "b"])]
impl Tasklet for StaticFan {
    type Input = String;
    fn cells_for(_input: &String) -> Vec<&'static str> {
        vec!["a", "b"]
    }
    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

refusal!(KeyByOnStaticOperator, "refuse_key_on_static", |c| {
    c.input::<String>("in")
        .key_by(|s: &String| s.clone())
        .into::<StaticFan>();
});

#[test]
fn keying_a_static_mode_consumer_is_refused() {
    assert!(
        issues::<KeyByOnStaticOperator>().iter().any(|e| matches!(
            e,
            TopologyIssue::KeyByOnUnkeyableTarget { tasklet, .. }
                if tasklet == "transform_static_fan"
        )),
        "static keying and edge keying are contradictory, not a precedence question"
    );
}

// ----- park-sink plumbing (mirrors `dlq_retry`'s controller stand-in) -----
