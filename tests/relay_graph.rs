//! A relay's graph as the runner announces it: every channel with its kind,
//! producer, consumers, plugin end and overflow, and every tasklet with its
//! cell mode, read off the typed class's topology.

mod common;
use common::contract::contract_file;
use common::prelude::*;
use flezha_runner::channel::{ChannelBuilder, OverflowPolicy};
use flezha_runner::relay::builder::build_topology_for;
use flezha_runner::relay::registry::{advertised_for_factory, register_class};
use flezha_runner::{CallTarget, Each, OnFailure, RelaySettings, Response, Run};

test_tasklet!(Ohlc, "graph_ohlc", String => String, (), |input, ctx| {
    ctx.emit(input)
});
test_tasklet!(Trades, "graph_trades", String => String, (), |input, ctx| {
    ctx.emit(input)
});
test_tasklet!(Atr, "graph_atr", String => String, (), |input, ctx| {
    ctx.emit(input)
});
test_tasklet!(Read, "graph_read", Response<String, String> => String, (), |_input, _ctx| {
    Ok(())
});

/// An entry fanning out to two tasklets, a hybrid channel, a fan-in and a call.
struct Market;
impl Relay for Market {
    fn factory_id() -> &'static str {
        "relay_graph_market"
    }
    fn channels(c: &ChannelBuilder) {
        let trades_in = c.input::<String>("trades_in");
        trades_in
            .into::<Ohlc>()
            .output_named("ohlc_out")
            .into::<Atr>()
            .named("ask")
            .call::<String>(CallTarget::factory("pricing"), 500)
            .into::<Read>();
        trades_in
            .into::<Trades>()
            .named("trades_out")
            .on_full(OverflowPolicy::DropOldest)
            .into::<Atr>();
    }
}

#[test]
fn the_announcement_carries_every_channel_and_tasklet() {
    register_class(
        Market::factory_id(),
        build_topology_for::<Market>().expect("topology"),
    );
    let graph = advertised_for_factory(Market::factory_id())
        .graph
        .expect("a registered class announces a graph");

    let channels: Vec<[String; 7]> = graph
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
                c.overflow.clone(),
            ]
        })
        .collect();
    let expected = [
        [
            "trades_in",
            "entry",
            "",
            "graph_ohlc,graph_trades",
            "none",
            "",
            "drop_nothing",
        ],
        [
            "ohlc_out",
            "inter_tasklet",
            "graph_ohlc",
            "graph_atr",
            "output",
            "",
            "drop_nothing",
        ],
        [
            "trades_out",
            "inter_tasklet",
            "graph_trades",
            "graph_atr",
            "none",
            "",
            "drop_oldest",
        ],
        [
            "ask",
            "plugin_bound",
            "graph_atr",
            "",
            "call",
            "factory:pricing",
            "drop_nothing",
        ],
        [
            "ask.response",
            "entry",
            "",
            "graph_read",
            "none",
            "",
            "drop_nothing",
        ],
    ];
    for row in expected.map(|row| row.map(str::to_string)) {
        assert!(channels.contains(&row), "{row:?} not in {channels:?}");
    }
    assert_eq!(channels.len(), 5, "{channels:?}");

    // In declaration order: the first chain declares three of them.
    let tasklets: Vec<_> = graph
        .tasklets
        .iter()
        .map(|t| (t.name.as_str(), t.mode.as_str(), t.mailbox_capacity > 0))
        .collect();
    assert_eq!(
        tasklets,
        [
            ("graph_ohlc", "single", true),
            ("graph_atr", "single", true),
            ("graph_read", "single", true),
            ("graph_trades", "single", true),
        ]
    );
}

#[test]
fn an_undeclared_factory_announces_no_graph() {
    assert_eq!(
        advertised_for_factory("relay_graph_never_declared").graph,
        None
    );
}

// The two reference classes of `test/fixtures/reference_topologies.txt`, in
// this SDK's spelling.
test_tasklet!(HybridProducer, "hybrid_producer_tasklet", String => u32, (), |_input, ctx| {
    ctx.emit(1)
});
test_tasklet!(HybridConsumer, "hybrid_consumer_tasklet", u32 => u32, (), |_input, _ctx| {
    Ok(())
});
test_tasklet!(CommitA, "tier_commit_a", String => String, (), |input, ctx| {
    ctx.emit(input)
});
test_tasklet!(BlockB, "block_b", String => String, (), |_input, _ctx| {
    Ok(())
});

struct HybridChannel;
impl Relay for HybridChannel {
    fn factory_id() -> &'static str {
        "relay_graph_hybrid_channel"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<HybridProducer>()
            .output_named_with("hybrid_indicator", |v: &u32| v.to_be_bytes().to_vec())
            .into::<HybridConsumer>();
    }
}

struct CommitBlockChain;
impl Relay for CommitBlockChain {
    fn factory_id() -> &'static str {
        "relay_graph_commit_block_chain"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .output_named_with("mid", |s: &String| s.as_bytes().to_vec())
            .into::<BlockB>();
    }
    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(60)),
            ..RelaySettings::default()
        }
    }
}

/// The fixture's six columns after the topology id for `topology`.
fn fixture_lines(topology: &str) -> Vec<Vec<String>> {
    let text = std::fs::read_to_string(contract_file("test/fixtures/reference_topologies.txt"))
        .expect("the fixture is readable");
    text.lines()
        .map(|line| {
            line.split('|')
                .map(|col| col.trim().to_string())
                .collect::<Vec<_>>()
        })
        .filter(|cols| cols.len() == 7 && cols[0] == topology)
        .map(|cols| cols[1..].to_vec())
        .collect()
}

/// The announced graph in the fixture's columns: tasklets by index, the kind
/// as the record spells it, and the sink as the channel an output ends.
fn announced_lines(factory: &str) -> Vec<Vec<String>> {
    let graph = advertised_for_factory(factory).graph.expect("graph");
    let index = |name: &str| {
        graph
            .tasklets
            .iter()
            .position(|t| t.name == name)
            .map_or("-".to_string(), |i| i.to_string())
    };
    graph
        .channels
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let consumers: Vec<String> = c.consumers.iter().map(|t| index(t)).collect();
            vec![
                i.to_string(),
                c.name.clone(),
                match c.kind.as_str() {
                    "inter_tasklet" => "inter_task",
                    "plugin_bound" => "plugin",
                    kind => kind,
                }
                .to_string(),
                index(&c.producer),
                if consumers.is_empty() {
                    "-".to_string()
                } else {
                    consumers.join(",")
                },
                if c.plugin_end == "output" {
                    c.name.clone()
                } else {
                    "-".to_string()
                },
            ]
        })
        .collect()
}

#[test]
fn the_reference_classes_announce_the_fixture_graph() {
    register_class(
        HybridChannel::factory_id(),
        build_topology_for::<HybridChannel>().expect("topology"),
    );
    register_class(
        CommitBlockChain::factory_id(),
        build_topology_for::<CommitBlockChain>().expect("topology"),
    );

    for (factory, topology) in [
        (HybridChannel::factory_id(), "hybrid_channel"),
        (CommitBlockChain::factory_id(), "commit_block_chain"),
    ] {
        let expected = fixture_lines(topology);
        assert!(
            !expected.is_empty(),
            "the fixture has no rows for {topology}"
        );
        assert_eq!(announced_lines(factory), expected, "{topology}");
    }
}
