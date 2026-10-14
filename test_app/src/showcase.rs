//! The `showcase` fixture: a reference-only relay whose announced graph holds
//! every shape the console's relay graph draws — a fan-in of two entries, a
//! four-way fan-out, a straight run of four, hybrid channels read by a second
//! fan-in, a call end with its answer channel, single, static and dynamic
//! Cells, and five outputs. It runs on the testnet generator's trades and its
//! order-book snapshots as quotes. It is not a conformance scenario, so
//! `conformance.json` does not list it.
//!
//! Every tasklet passes a line it cannot read on unchanged, so a malformed
//! input never stalls it. `volume_window`, `audit_sample` and `detect_spike`
//! emit the first of every five, twenty and fifty inputs a Cell takes, which
//! makes the rates downstream differ visibly.

use std::collections::HashMap;

use flezha_runner::{CallTarget, Response};

use crate::*;

/// The deadline of the pricing call.
const DEADLINE_MS: u64 = 30_000;

/// The windows `volume_window` keeps, one static Cell each.
const WINDOWS: &[&str] = &["1m", "5m", "15m"];

/// The text after `"symbol":"` up to the next `"`, or `unknown`.
fn symbol_of(line: &str) -> &str {
    line.split_once(r#""symbol":""#)
        .and_then(|(_, rest)| rest.split_once('"'))
        .map_or("unknown", |(symbol, _)| symbol)
}

/// The number after `"price":`, when the line carries one.
fn price_of(line: &str) -> Option<f64> {
    let (_, rest) = line.split_once(r#""price":"#)?;
    rest.split([',', '}']).next()?.trim().parse().ok()
}

/// The `call_pricing` instance answering `symbol`, prefixed so it cannot
/// collide with another relay's instance id.
fn pricing_instance(line: &str) -> String {
    let symbol: String = symbol_of(line)
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    format!("showcase_pricing_{symbol}")
}

/// Whether a Cell that has taken `seen` inputs emits the next one.
fn sampled(seen: &mut u64, every: u64) -> bool {
    let emit = seen.is_multiple_of(every);
    *seen += 1;
    emit
}

/// Hands each line on trimmed: the nodes whose only job is their place in the
/// graph, each placed under its own name.
#[derive(Default)]
struct PassOn;
#[flezha_runner::tasklet(name = "pass_on")]
impl Tasklet for PassOn {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.trim().to_string())
    }
}

#[derive(Default)]
struct VolumeWindow {
    seen: u64,
}
#[flezha_runner::tasklet(name = "volume_window")]
impl Tasklet for VolumeWindow {
    type Input = String;
    type Output = String;
    fn cell_keys() -> Vec<&'static str> {
        WINDOWS.to_vec()
    }
    fn cells_for(_: &String) -> Vec<&'static str> {
        WINDOWS.to_vec()
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if !sampled(&mut self.seen, 5) {
            return Ok(());
        }
        let window = ctx.cell_key().unwrap_or_default().to_string();
        ctx.emit(format!("{window} {input}"))
    }
}

#[derive(Default)]
struct TagVenue;
#[flezha_runner::tasklet(name = "tag_venue")]
impl Tasklet for TagVenue {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("testnet {input}"))
    }
}

#[derive(Default)]
struct ScoreRisk;
#[flezha_runner::tasklet(name = "score_risk")]
impl Tasklet for ScoreRisk {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("{} {input}", symbol_of(&input)))
    }
}

#[derive(Default)]
struct SettleOrder;
#[flezha_runner::tasklet(name = "settle_order")]
impl Tasklet for SettleOrder {
    type Input = Response<String, String>;
    type Output = String;
    fn run(
        &mut self,
        input: Response<String, String>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        let symbol = symbol_of(&input.request);
        ctx.emit(match &input.result {
            Ok(answer) => format!("{symbol} settled {answer}"),
            Err(error) => format!("{symbol} unsettled {}", error.code),
        })
    }
}

#[derive(Default)]
struct AuditSample {
    seen: u64,
}
#[flezha_runner::tasklet(name = "audit_sample")]
impl Tasklet for AuditSample {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if !sampled(&mut self.seen, 20) {
            return Ok(());
        }
        ctx.emit(input)
    }
}

/// One Cell reads every symbol, so the last price is kept per symbol.
#[derive(Default)]
struct DetectSpike {
    seen: u64,
    last_price: HashMap<String, f64>,
}
#[flezha_runner::tasklet(name = "detect_spike")]
impl Tasklet for DetectSpike {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let moved = price_of(&input).is_some_and(|now| {
            let last = self.last_price.insert(symbol_of(&input).to_string(), now);
            last.is_some_and(|last| last != 0.0 && ((now - last) / last).abs() > 0.01)
        });
        if !sampled(&mut self.seen, 50) && !moved {
            return Ok(());
        }
        ctx.emit(format!("spike {input}"))
    }
}

pub(crate) struct ShowcaseRelay;
impl Relay for ShowcaseRelay {
    fn factory_id() -> &'static str {
        "showcase"
    }
    fn channels(c: &ChannelBuilder) {
        let trades = c
            .input::<String>("trades")
            .into_named::<PassOn>("normalize_trade");
        let quotes = c
            .input::<String>("quotes")
            .into_named::<PassOn>("normalize_quote");
        let ticks = c
            .merge::<String>()
            .from(trades, |line| line)
            .from(quotes, |line| line)
            .into_named::<PassOn>("merge_ticks")
            .named("ticks");

        let prices = ticks
            .key_by(|line: &String| symbol_of(line).to_string())
            .into_named::<PassOn>("price_by_symbol")
            .output_named("prices");
        let volumes = ticks.into::<VolumeWindow>().output_named("volumes");
        ticks
            .into_named::<PassOn>("validate_tick")
            .into::<TagVenue>()
            .into_named::<PassOn>("convert_currency")
            .into::<ScoreRisk>()
            .named("quote")
            .key_by(|line: &String| pricing_instance(line))
            .call::<String>(CallTarget::factory("call_pricing"), DEADLINE_MS)
            .into::<SettleOrder>()
            .output_named("orders");
        ticks.into::<AuditSample>().output_named("audit");

        // A key or a mapping on a channel behind its output is refused, so the
        // hybrid channels fan in to `detect_spike` unkeyed.
        prices.into::<DetectSpike>().output_named("alerts");
        volumes.into::<DetectSpike>();
    }
}

#[cfg(test)]
mod tests {
    use flezha_runner::relay::builder::build_topology_for;
    use flezha_runner::relay::registry::{advertised_for_factory, register_class};

    use super::*;

    #[test]
    fn showcase_announces_every_graph_shape() {
        register_class(
            ShowcaseRelay::factory_id(),
            build_topology_for::<ShowcaseRelay>().expect("topology"),
        );
        let graph = advertised_for_factory(ShowcaseRelay::factory_id())
            .graph
            .expect("a registered class announces a graph");

        let channels: Vec<[String; 6]> = graph
            .channels
            .iter()
            .map(|c| {
                [
                    c.name.clone(),
                    c.kind.clone(),
                    c.producer.clone(),
                    c.consumers.join(","),
                    c.plugin_end.clone(),
                    c.call_target.clone(),
                ]
            })
            .collect();
        let expected = [
            // The fan-in of the two entries.
            ["trades", "entry", "", "normalize_trade", "none", ""],
            ["quotes", "entry", "", "normalize_quote", "none", ""],
            [
                "normalize_trade",
                "inter_tasklet",
                "normalize_trade",
                "merge_ticks",
                "none",
                "",
            ],
            [
                "normalize_quote",
                "inter_tasklet",
                "normalize_quote",
                "merge_ticks",
                "none",
                "",
            ],
            // The fan-out.
            [
                "ticks",
                "inter_tasklet",
                "merge_ticks",
                "price_by_symbol,volume_window,validate_tick,audit_sample",
                "none",
                "",
            ],
            // The hybrid channels, read by the second fan-in.
            [
                "prices",
                "inter_tasklet",
                "price_by_symbol",
                "detect_spike",
                "output",
                "",
            ],
            [
                "volumes",
                "inter_tasklet",
                "volume_window",
                "detect_spike",
                "output",
                "",
            ],
            // The straight run of four, ending in the call.
            [
                "validate_tick",
                "inter_tasklet",
                "validate_tick",
                "tag_venue",
                "none",
                "",
            ],
            [
                "tag_venue",
                "inter_tasklet",
                "tag_venue",
                "convert_currency",
                "none",
                "",
            ],
            [
                "convert_currency",
                "inter_tasklet",
                "convert_currency",
                "score_risk",
                "none",
                "",
            ],
            [
                "quote",
                "plugin_bound",
                "score_risk",
                "",
                "call",
                "factory:call_pricing",
            ],
            ["quote.response", "entry", "", "settle_order", "none", ""],
            // The outputs that are nothing else.
            ["orders", "plugin_bound", "settle_order", "", "output", ""],
            ["alerts", "plugin_bound", "detect_spike", "", "output", ""],
            ["audit", "plugin_bound", "audit_sample", "", "output", ""],
        ];
        for row in expected.map(|row| row.map(str::to_string)) {
            assert!(channels.contains(&row), "{row:?} not in {channels:?}");
        }
        assert_eq!(channels.len(), expected.len(), "{channels:?}");

        let mut tasklets: Vec<_> = graph
            .tasklets
            .iter()
            .map(|t| (t.name.as_str(), t.mode.as_str()))
            .collect();
        tasklets.sort_unstable();
        assert_eq!(
            tasklets,
            [
                ("audit_sample", "single"),
                ("convert_currency", "single"),
                ("detect_spike", "single"),
                ("merge_ticks", "single"),
                ("normalize_quote", "single"),
                ("normalize_trade", "single"),
                ("price_by_symbol", "dynamic"),
                ("score_risk", "single"),
                ("settle_order", "single"),
                ("tag_venue", "single"),
                ("validate_tick", "single"),
                ("volume_window", "static"),
            ]
        );
    }
}
