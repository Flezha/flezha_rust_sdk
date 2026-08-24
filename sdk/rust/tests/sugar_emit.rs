//! The sugar's emit surface: the `key = fn` opt-in, which names the output's
//! own natural key for the output whose key is not the key its Entity was
//! routed by (that one the keyed channel already supplies, and a single-entity
//! procedure — which has no key at all — needs the opt-in or its emit is
//! refused); and the context-parameter shape, where the body owns every emit
//! and `emits = Type` declares what it emits.

use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
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

/// fails when: a single-entity procedure's plain `emit` is accepted on a
/// keyed channel — it has no Entity key to fall back on, so the guard that
/// makes `key` load-bearing there would be gone and a keyless record would
/// ship silently.
#[test]
#[should_panic(expected = "is declared .keyed()")]
fn without_the_key_opt_in_a_keyed_channel_refuses_a_single_entity_emit() {
    let mut harness = ProcedureTestHarness::<UnkeyedSugar>::new().with_keyed_channel();
    harness.apply_inputs(vec![reading("BTC/USD", 1.0)]);
}

/// fails when: a key-less emit inside a keyed Entity stops carrying that
/// Entity's own key, or stamps it differently from the explicit spelling —
/// the whole point is that the two are one path, so a keyed procedure never
/// has to re-supply (and so can never drift from) its routing key.
#[test]
fn a_keyed_entity_stamps_its_own_key_on_a_key_less_emit() {
    let mut defaulted = ProcedureTestHarness::<UnkeyedSugar>::new()
        .with_keyed_channel()
        .with_entity_key("BTC/USD");
    defaulted.apply_inputs(vec![reading("BTC/USD", 1.0)]);

    let mut explicit = ProcedureTestHarness::<KeyedSugar>::new()
        .with_keyed_channel()
        .with_entity_key("BTC/USD");
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

// ---- The context-parameter shape (multi-emit) ----------------------------

/// One input, three emits — the shape the return-type sugar cannot express.
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

#[flezha_runner::procedure(id = "fanout_sugar", emits = Reading)]
fn fanout_sugar(input: Reading, ctx: &mut ProcedureCtx) -> Result<(), ProcedureError> {
    for (key, out) in split(&input) {
        ctx.emit_keyed(key, out)?;
    }
    Ok(())
}

/// The expansion written out by hand — what "ejecting" the sugar produces.
#[derive(Default)]
struct FanoutManual;

impl Procedure for FanoutManual {
    type Input = Reading;
    type Output = Reading;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "fanout_manual"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> Self::State {}

    fn run(
        &mut self,
        input: Self::Input,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        for (key, out) in split(&input) {
            ctx.emit_keyed(key, out)?;
        }
        Ok(())
    }
}

/// fails when: the context-parameter expansion stops being the hand-written
/// implementation it claims to be — the escape hatch (§1) is that a sugared
/// procedure can be pasted out as a full impl and behave identically, and a
/// multi-emit body is where the two could most easily diverge.
#[test]
fn a_context_parameter_emits_every_value_exactly_as_the_hand_written_twin() {
    let inputs = vec![reading("BTC/USD", 1.0), reading("ETH/USD", 2.0)];

    let mut sugared = ProcedureTestHarness::<FanoutSugar>::new().with_keyed_channel();
    sugared.apply_inputs(inputs.clone());
    let mut manual = ProcedureTestHarness::<FanoutManual>::new().with_keyed_channel();
    manual.apply_inputs(inputs.clone());

    let expected: Vec<Reading> = inputs.iter().flat_map(split).map(|(_, o)| o).collect();
    assert_eq!(expected.len(), 6, "one input fans out to three emits");
    assert_eq!(sugared.emitted_meta(), manual.emitted_meta());
    sugared.expect_emits(&expected);
    manual.expect_emits(&expected);
}
