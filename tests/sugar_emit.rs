//! The emit surface of an attributed tasklet: a keyed emit names the
//! output's own natural key, a plain emit inside a keyed Cell carries the
//! Cell's key, and a single-cell tasklet has no key to fall back on. The body
//! owns every emit, so one input can emit several values.

use flezha_runner::tasklet::{Ctx, Tasklet, TaskletError};
use flezha_runner::testing::TaskletTestHarness;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Reading {
    symbol: String,
    value: f64,
}

/// Emits positive readings under their own symbol.
#[derive(Default)]
struct KeyedEmit;

#[flezha_runner::tasklet(name = "keyed_emit")]
impl Tasklet for KeyedEmit {
    type Input = Reading;
    type Output = Reading;

    fn run(&mut self, input: Reading, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if input.value > 0.0 {
            ctx.emit_keyed(input.symbol.clone(), input)?;
        }
        Ok(())
    }
}

/// Emits every reading with no key of its own.
#[derive(Default)]
struct PlainEmit;

#[flezha_runner::tasklet(name = "plain_emit")]
impl Tasklet for PlainEmit {
    type Input = Reading;
    type Output = Reading;

    fn run(&mut self, input: Reading, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input)
    }
}

fn reading(symbol: &str, value: f64) -> Reading {
    Reading {
        symbol: symbol.to_string(),
        value,
    }
}

/// fails when: a keyed emit stops stamping the natural key
/// on its emits — the Kafka producer plugin would then publish keyless
/// records and per-key ordering across a partitioned group's members is lost.
#[test]
fn key_opt_in_stamps_the_natural_key_on_every_emit() {
    let mut harness = TaskletTestHarness::<KeyedEmit>::new().with_keyed_channel();
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

/// fails when: an input the tasklet skips starts emitting: a warmup gate that
/// emitted anyway would publish an unwarmed indicator value downstream.
#[test]
fn a_skipped_output_emits_nothing_to_key() {
    let mut harness = TaskletTestHarness::<KeyedEmit>::new().with_keyed_channel();
    harness.apply_inputs(vec![reading("BTC/USD", -1.0)]);
    assert!(harness.emitted_meta().is_empty());
}

/// fails when: a single-cell tasklet's plain `emit` is accepted on a
/// keyed channel: it has no Cell key to fall back on, so a keyless record
/// would ship silently.
#[test]
#[should_panic(expected = "is keyed and this tasklet is single-cell")]
fn a_keyed_channel_refuses_a_single_cell_plain_emit() {
    let mut harness = TaskletTestHarness::<PlainEmit>::new().with_keyed_channel();
    harness.apply_inputs(vec![reading("BTC/USD", 1.0)]);
}

/// fails when: a key-less emit inside a keyed Cell stops carrying that
/// Cell's own key, or stamps it differently from the explicit spelling: the
/// two are one path, so a keyed tasklet never has to re-supply (and so can
/// never drift from) its routing key.
#[test]
fn a_keyed_cell_stamps_its_own_key_on_a_key_less_emit() {
    let mut defaulted = TaskletTestHarness::<PlainEmit>::new()
        .with_keyed_channel()
        .with_cell_key("BTC/USD");
    defaulted.apply_inputs(vec![reading("BTC/USD", 1.0)]);

    let mut explicit = TaskletTestHarness::<KeyedEmit>::new()
        .with_keyed_channel()
        .with_cell_key("BTC/USD");
    explicit.apply_inputs(vec![reading("BTC/USD", 1.0)]);

    assert_eq!(
        defaulted.emitted_meta()[0]
            .expect("keyed emit records stamped metadata")
            .get("flz.key"),
        Some(&"BTC/USD".to_string())
    );
    assert_eq!(
        defaulted.emitted_meta(),
        explicit.emitted_meta(),
        "the defaulted emit and `emit_keyed` with the same key stamp identically"
    );
    defaulted.expect_emits(&[reading("BTC/USD", 1.0)]);
}

// ---- Several emits per input ---------------------------------------------

/// One input, three emits.
fn split(input: &Reading) -> Vec<(String, Reading)> {
    (1..=3)
        .map(|step| {
            (
                format!("{}#{step}", input.symbol),
                reading(&input.symbol, input.value * f64::from(step)),
            )
        })
        .collect()
}

#[derive(Default)]
struct Fanout;

#[flezha_runner::tasklet(name = "fanout")]
impl Tasklet for Fanout {
    type Input = Reading;
    type Output = Reading;

    fn run(&mut self, input: Reading, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        for (key, out) in split(&input) {
            ctx.emit_keyed(key, out)?;
        }
        Ok(())
    }
}

/// fails when: an emit inside a loop is lost or reordered, or a keyed emit
/// stops stamping each value's own key.
#[test]
fn one_input_emits_every_value_in_order_under_its_own_key() {
    let inputs = vec![reading("BTC/USD", 1.0), reading("ETH/USD", 2.0)];

    let mut harness = TaskletTestHarness::<Fanout>::new().with_keyed_channel();
    harness.apply_inputs(inputs.clone());

    let expected: Vec<(String, Reading)> = inputs.iter().flat_map(split).collect();
    assert_eq!(expected.len(), 6, "one input fans out to three emits");
    let keys: Vec<String> = harness
        .emitted_meta()
        .iter()
        .map(|m| m.expect("stamped").get("flz.key").expect("flz.key").clone())
        .collect();
    let expected_keys: Vec<String> = expected.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, expected_keys);
    let values: Vec<Reading> = expected.into_iter().map(|(_, o)| o).collect();
    harness.expect_emits(&values);
}
