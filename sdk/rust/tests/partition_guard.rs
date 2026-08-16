//! The partition dispatch guard (`partitioned-operatives` D6).
//!
//! A partitioned operative's correctness rests on one convention: the routing
//! item — the source's subscription string, the emitter's `flz.key` — equals
//! what `key_by` extracts. Nothing enforces that upstream, so the guard turns
//! every violation into a first-frame park naming the contract, before any
//! state exists for the key. In-range traffic must be untouched, or
//! partitioning would cost every operative that uses it.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operative::KeyGroupRange;
use flezha_runner::StreamOperative;

mod common;
use common::{
    dispatch_input, dummy_stream, install_auto_acking_sink, setup, setup_partitioned, wait_until,
};

/// The two cases driving `KeyedOperative` share its statics — and one of them
/// installs the process-global park sink — so they run one at a time.
static KEYED_LOCK: Mutex<()> = Mutex::new(());

static SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());
static RUNS: AtomicUsize = AtomicUsize::new(0);

test_procedure!(KeyedProcedure, "keyed_procedure", String => String, (), |input, _state, _ctx| {
    RUNS.fetch_add(1, Ordering::SeqCst);
    SEEN.lock().unwrap().push(input);
    Ok(())
});

struct KeyedOperative;
impl StreamOperative for KeyedOperative {
    fn factory_id() -> &'static str {
        "partition_guard_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .retry_max(1)
            .key_by(|s: &String| s.clone())
            .into::<KeyedProcedure>();
    }
}

// A statically keyed class: every member declares all the keys, and the ones
// outside its range simply never receive input.
static STATIC_SEEN: Mutex<Vec<String>> = Mutex::new(Vec::new());

test_procedure!(StaticProcedure, "static_procedure", String => String, (), |input, _state, _ctx| {
    STATIC_SEEN.lock().unwrap().push(input);
    Ok(())
});

struct StaticOperative;
impl StreamOperative for StaticOperative {
    fn factory_id() -> &'static str {
        "partition_static_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<StaticProcedure>();
    }
}

/// Range of member `index` of `count` under the controller's even split —
/// `Flezha.Partition.ranges/1`, which `partition_test.exs` pins.
fn member(index: u32, count: u32) -> KeyGroupRange {
    let from = index * 256 / count;
    let to = (index + 1) * 256 / count - 1;
    KeyGroupRange {
        index,
        count,
        from,
        to,
    }
}

fn reset() {
    RUNS.store(0, Ordering::SeqCst);
    SEEN.lock().unwrap().clear();
}

/// The entry parser for a `String` input is a passthrough, so the payload
/// bytes ARE the key — which is what makes the key-group fixtures directly
/// comparable with the controller's.
fn payload(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

#[test]
fn a_mis_routed_key_parks_naming_the_contract_and_creates_no_entity() {
    let _guard = KEYED_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    reset();
    let parked = install_auto_acking_sink();

    // BTC-USD hashes to key group 129, which member 0 of 3 (groups 0..84) does
    // not own — the exact shape a source that was not partition-filtered
    // produces.
    let (handle, instance) = setup_partitioned::<KeyedOperative>("guard-p0", member(0, 3));
    let stream = dummy_stream();

    dispatch_input(&handle, "guard-p0", "input", payload("BTC-USD"), &stream);

    assert!(
        wait_until(
            || !parked.lock().unwrap().is_empty(),
            Duration::from_secs(5)
        ),
        "a key the member does not own must be parked, not folded into state"
    );

    let body = parked.lock().unwrap()[0].clone();
    let error = body.entry.error;
    assert!(error.contains("BTC-USD"), "{error}");
    assert!(error.contains("key group 129"), "{error}");
    assert!(error.contains("partition 0 of 3"), "{error}");
    assert!(error.contains("groups 0..84"), "{error}");
    assert!(
        error.contains("key_by"),
        "the park must state the contract that was violated: {error}"
    );

    assert_eq!(RUNS.load(Ordering::SeqCst), 0, "the procedure never ran");

    let registry = instance.dynamic_registries[0]
        .as_ref()
        .expect("dynamic registry");
    assert_eq!(
        registry.entities.len(),
        0,
        "no Entity may be created for a key this member does not own"
    );
}

#[test]
fn in_range_traffic_is_identical_to_an_unpartitioned_operative() {
    let _guard = KEYED_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    reset();
    let stream = dummy_stream();

    // SOL-USD hashes to group 60 — member 0 of 3 owns 0..84.
    let (partitioned, _) = setup_partitioned::<KeyedOperative>("guard-in", member(0, 3));
    dispatch_input(
        &partitioned,
        "guard-in",
        "input",
        payload("SOL-USD"),
        &stream,
    );

    assert!(wait_until(
        || RUNS.load(Ordering::SeqCst) == 1,
        Duration::from_secs(5)
    ));
    let partitioned_seen = SEEN.lock().unwrap().clone();

    reset();
    let (plain, _) = setup::<KeyedOperative>("guard-plain");
    dispatch_input(&plain, "guard-plain", "input", payload("SOL-USD"), &stream);

    assert!(wait_until(
        || RUNS.load(Ordering::SeqCst) == 1,
        Duration::from_secs(5)
    ));
    assert_eq!(partitioned_seen, *SEEN.lock().unwrap());
}

#[test]
fn every_key_is_owned_by_exactly_one_member() {
    let members: Vec<KeyGroupRange> = (0..3).map(|index| member(index, 3)).collect();

    for key in [
        "BTC-USD",
        "ETH-USD",
        "SOL-USD",
        "",
        "a",
        "x".repeat(300).as_str(),
    ] {
        let owners: Vec<u32> = members
            .iter()
            .filter(|m| m.admit(key).is_ok())
            .map(|m| m.index)
            .collect();

        assert_eq!(owners.len(), 1, "key {key:?} is owned by {owners:?}");
    }

    // The ranges themselves tile the space exactly once.
    let mut covered = HashSet::new();
    for m in &members {
        for group in m.from..=m.to {
            assert!(covered.insert(group), "key group {group} owned twice");
        }
    }
    assert_eq!(covered.len(), 256);
}

#[test]
fn an_unpartitioned_instance_owns_everything() {
    let whole = KeyGroupRange::UNPARTITIONED;
    assert!(!whole.partitioned());

    for key in ["BTC-USD", "ETH-USD", "SOL-USD", ""] {
        assert!(whole.owns(key));
    }
}

#[test]
fn a_static_class_serves_only_its_own_declared_keys() {
    STATIC_SEEN.lock().unwrap().clear();
    let stream = dummy_stream();

    // The single-entity class has no keys at all, so a partitioned member
    // still serves it: partitioning subtracts declared keys, it does not
    // silence a procedure that has none.
    let (handle, _) = setup_partitioned::<StaticOperative>("guard-static", member(1, 3));
    dispatch_input(
        &handle,
        "guard-static",
        "input",
        payload("anything"),
        &stream,
    );

    assert!(wait_until(
        || STATIC_SEEN.lock().unwrap().len() == 1,
        Duration::from_secs(5)
    ));
}
