//! Section 3.2 of the authoring contract: the `key = fn` opt-in on
//! `#[flezha::procedure]`. A `.keyed()` output channel accepts only
//! `emit_keyed`, so without this opt-in the sugar could reach unkeyed sinks
//! only and every keyed-output procedure would have to be hand-written.

use flezha_runner::testing::ProcedureTestHarness;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Reading {
    symbol: String,
    value: f64,
}

fn reading_key(out: &Reading) -> String {
    out.symbol.clone()
}

#[flezha_runner::procedure(id = "keyed_sugar", key = reading_key)]
fn keyed_sugar(input: Reading) -> Option<Reading> {
    (input.value > 0.0).then_some(input)
}

#[flezha_runner::procedure(id = "unkeyed_sugar")]
fn unkeyed_sugar(input: Reading) -> Option<Reading> {
    Some(input)
}

fn reading(symbol: &str, value: f64) -> Reading {
    Reading {
        symbol: symbol.to_string(),
        value,
    }
}

/// fails when: a `key = fn` sugared procedure stops stamping the natural key
/// on its emits — the Kafka producer plugin would then publish keyless
/// records and per-key ordering across a partitioned group's members is lost.
#[test]
fn key_opt_in_stamps_the_natural_key_on_every_emit() {
    let mut harness = ProcedureTestHarness::<KeyedSugar>::new().with_keyed_channel();
    harness.apply_inputs(vec![reading("BTC/USD", 1.0), reading("ETH/USD", 2.0)]);

    let keys: Vec<String> = harness
        .emitted_meta()
        .iter()
        .map(|m| {
            m.expect("keyed emit records stamped metadata")
                .get("flz.key")
                .expect("flz.key stamp")
                .clone()
        })
        .collect();
    assert_eq!(keys, vec!["BTC/USD".to_string(), "ETH/USD".to_string()]);
}

/// fails when: the `None` arm starts emitting — a warmup gate that emitted
/// anyway would publish an unwarmed indicator value downstream.
#[test]
fn a_skipped_output_emits_nothing_to_key() {
    let mut harness = ProcedureTestHarness::<KeyedSugar>::new().with_keyed_channel();
    harness.apply_inputs(vec![reading("BTC/USD", -1.0)]);
    assert!(harness.emitted_meta().is_empty());
}

/// fails when: the sugar's plain `emit` is accepted on a keyed channel — the
/// guard that makes `key` load-bearing would be gone and a keyless record
/// would ship silently.
#[test]
#[should_panic(expected = "is declared .keyed()")]
fn without_the_key_opt_in_a_keyed_channel_refuses_the_emit() {
    let mut harness = ProcedureTestHarness::<UnkeyedSugar>::new().with_keyed_channel();
    harness.apply_inputs(vec![reading("BTC/USD", 1.0)]);
}
