//! `route_latest`: the packaged router's delivery contract, driven through the
//! replay-determinism harness, plus the expansion it declares on the topology.

use serde::{Deserialize, Serialize};

use super::{LatestRouter, RouteLatestInput};
use crate::channel::builder::TopologyIssue;
use crate::channel::ChannelBuilder;
use crate::operative::builder::build_topology_for;
use crate::operative::topology::EntityMode;
use crate::operative::StreamOperative;
use crate::procedure::{Procedure, ProcedureCtx, ProcedureError};
use crate::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};
use crate::testing::ProcedureTestHarness;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Thresholds {
    limit: u64,
}

fn thresholds(limit: u64) -> Thresholds {
    Thresholds { limit }
}

/// A document arrival on the router's merged input.
fn document(limit: u64) -> RouteLatestInput<Thresholds> {
    RouteLatestInput::Document(thresholds(limit))
}

/// One traffic item for `key` — post-transform, so it is the key alone.
fn traffic(key: &str) -> RouteLatestInput<Thresholds> {
    RouteLatestInput::Traffic(key.to_string())
}

type Delivery = (String, Thresholds);

fn delivery(key: &str, limit: u64) -> Delivery {
    (key.to_string(), thresholds(limit))
}

fn harness() -> ProcedureTestHarness<LatestRouter<Thresholds>> {
    ProcedureTestHarness::<LatestRouter<Thresholds>>::new()
}

/// fails when: a changed document stops reaching a key on that key's own next
/// traffic item — every per-key consumer of an operative-wide document would
/// keep computing against a stale one, silently.
#[test]
fn a_changed_document_reaches_each_key_on_its_own_next_traffic() {
    let mut h = harness();
    h.apply_inputs(vec![
        document(10),
        traffic("AAPL"),
        traffic("MSFT"),
        // A second document version: both keys are owed a delivery again, each
        // on its own next item.
        document(20),
        traffic("MSFT"),
        traffic("AAPL"),
    ]);
    h.expect_emits::<Delivery>(&[
        delivery("AAPL", 10),
        delivery("MSFT", 10),
        delivery("MSFT", 20),
        delivery("AAPL", 20),
    ]);
}

/// fails when: a re-delivered identical document bumps the version — a
/// document source that re-polls after every reconnect would then re-deliver to
/// every key on every reconnect, which is exactly the noise the retained
/// comparison exists to absorb.
#[test]
fn a_repeated_identical_document_is_inert() {
    let mut h = harness();
    h.apply_inputs(vec![
        document(10),
        traffic("AAPL"),
        // The same document again, twice, with traffic in between.
        document(10),
        traffic("AAPL"),
        document(10),
        traffic("AAPL"),
    ]);
    h.expect_emits::<Delivery>(&[delivery("AAPL", 10)]);
}

/// fails when: a key first seen after the document was retained never receives
/// it (a late-born Entity starts up config-less) or receives it repeatedly.
#[test]
fn a_first_seen_key_receives_the_current_document_once() {
    let mut h = harness();
    h.apply_inputs(vec![
        document(10),
        traffic("AAPL"),
        // NVDA is seen for the first time here, long after the document.
        traffic("NVDA"),
        traffic("NVDA"),
        traffic("NVDA"),
    ]);
    h.expect_emits::<Delivery>(&[delivery("AAPL", 10), delivery("NVDA", 10)]);
}

/// fails when: a delivery is made for a key that produced no traffic — a
/// remembered-set fan-out. After a rescale (or one out-of-range item seen
/// during a reconcile window) such a key is no longer this member's, and every
/// delivery to it parks against the partition guard.
#[test]
fn only_keys_proven_by_traffic_are_emitted() {
    let mut h = harness();
    h.apply_inputs(vec![
        document(10),
        traffic("AAPL"),
        traffic("MSFT"),
        // Both keys are owed version 2, but only AAPL trades again.
        document(20),
        traffic("AAPL"),
    ]);
    h.expect_emits::<Delivery>(&[
        delivery("AAPL", 10),
        delivery("MSFT", 10),
        delivery("AAPL", 20),
    ]);
}

/// fails when: the router's state or its emit stream stops being a pure
/// function of its inputs — a crash-resume, a correction replay or a rewind
/// would then rebuild a router that owes a different set of deliveries than the
/// live run did.
#[test]
fn router_state_replays_deterministically() {
    let inputs = vec![
        document(10),
        traffic("AAPL"),
        traffic("MSFT"),
        document(10),
        traffic("AAPL"),
        document(20),
        traffic("AAPL"),
        traffic("NVDA"),
    ];
    let mut h = harness();
    h.apply_inputs(inputs.clone());
    if let Err(mismatch) = h.replay_equals() {
        panic!("route_latest state does not replay: {mismatch}");
    }
    if let Err(mismatch) = h.replay_emits_equal(&inputs) {
        panic!("route_latest emits do not replay: {mismatch}");
    }
}

/// fails when: traffic arriving before any document produces a delivery — there
/// is nothing to deliver, and an emitted `None`-shaped pair would be a
/// downstream parse failure.
#[test]
fn traffic_before_the_first_document_delivers_nothing() {
    let mut h = harness();
    h.apply_inputs(vec![traffic("AAPL"), traffic("MSFT")]);
    h.expect_emits::<Delivery>(&[]);
}

// ----- The expansion -----

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Trade {
    symbol: String,
    qty: u64,
}

/// Stands in for the per-key consumer the routed document is for.
#[derive(Default)]
struct Detector;

impl Procedure for Detector {
    type Input = Delivery;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "detector"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

    fn run(
        &mut self,
        _input: Self::Input,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

/// The canonical shape: a document stream, a traffic stream, a keyed consumer.
struct RoutedOperative;

impl StreamOperative for RoutedOperative {
    fn factory_id() -> &'static str {
        "route_latest_expansion"
    }

    fn channels(c: &ChannelBuilder) {
        let docs = c.input::<Thresholds>("thresholds");
        let trades = c.input::<Trade>("trades");
        c.route_latest(docs, trades, |t: &Trade| t.symbol.clone())
            .key_by(|(key, _): &Delivery| key.clone())
            .into::<Detector>();
    }
}

/// fails when: the block stops expanding to one single-entity node fed by both
/// streams with the key function on the traffic edge. Every guarantee the block
/// carries rests on that shape: single-entity (one retained document per
/// operative), and the traffic payload replaced by its key at the routing seam
/// (so the router never holds application values).
#[test]
fn the_block_expands_to_one_single_entity_node_fed_by_both_streams() {
    let topology = build_topology_for::<RoutedOperative>().expect("topology");

    let router_id = topology.procedure_name_to_id["route_latest"];
    let router = &topology.procedures[router_id as usize];
    assert_eq!(router.mode, EntityMode::Single);
    assert_eq!(router.input_channel_ids.len(), 2);

    let docs = &topology.channels[topology.name_to_id["thresholds"] as usize];
    let trades = &topology.channels[topology.name_to_id["trades"] as usize];
    for source in [docs, trades] {
        let edge = source
            .consumers
            .iter()
            .find(|e| e.procedure == router_id)
            .expect("both streams feed the router");
        // The merge mapping (and, on traffic, the key function fused into it)
        // is one closure on the edge — no extra node, no extra hop.
        assert!(edge.transform.is_some());
        assert!(edge.key.is_none(), "the router is single-entity");
    }

    // The routed pair carries its own key, which is what lets the consumer be
    // dynamically keyed off it.
    let out = &topology.channels[router.output_channel_id.expect("output channel") as usize];
    let detector = &out.consumers[0];
    assert_eq!(
        topology.procedures[detector.procedure as usize]
            .name
            .as_ref(),
        "detector"
    );
    assert!(detector.key.is_some());
    assert_eq!(
        topology.procedures[detector.procedure as usize].mode,
        EntityMode::Dynamic
    );
}

struct TwoUnnamedRouterOperative;

impl StreamOperative for TwoUnnamedRouterOperative {
    fn factory_id() -> &'static str {
        "route_latest_twice"
    }

    fn channels(c: &ChannelBuilder) {
        let docs = c.input::<Thresholds>("thresholds");
        let alt = c.input::<Thresholds>("alt_thresholds");
        let trades = c.input::<Trade>("trades");
        // Two unnamed mentions of one class are ONE node by the node-identity
        // rule, so the second declaration re-wires the traffic edge it already
        // owns.
        c.route_latest(docs, trades, |t: &Trade| t.symbol.clone())
            .sealed();
        c.route_latest(alt, trades, |t: &Trade| t.symbol.clone())
            .sealed();
    }
}

struct TwoNamedRouterOperative;

impl StreamOperative for TwoNamedRouterOperative {
    fn factory_id() -> &'static str {
        "route_latest_twice_named"
    }

    fn channels(c: &ChannelBuilder) {
        let docs = c.input::<Thresholds>("thresholds");
        let trades = c.input::<Trade>("trades");
        let alt = c.input::<Thresholds>("alt_thresholds");
        c.route_latest_named(docs, trades, |t: &Trade| t.symbol.clone(), "primary")
            .named("primary_out")
            .sealed();
        c.route_latest_named(alt, trades, |t: &Trade| t.symbol.clone(), "secondary")
            .named("secondary_out")
            .sealed();
    }
}

/// Declaration errors this operative's `channels(...)` collected.
fn issues<R: StreamOperative>() -> Vec<TopologyIssue> {
    build_topology_for::<R>()
        .err()
        .map(|e| e.errors)
        .unwrap_or_default()
}

/// fails when: two routers over one traffic stream silently collapse into one
/// node (they would share one retained document and one delivered map, so each
/// document stream would overwrite the other's) or a named pair stops being two.
#[test]
fn a_second_router_needs_a_node_name() {
    assert!(
        issues::<TwoUnnamedRouterOperative>()
            .iter()
            .any(|e| matches!(
                e,
                TopologyIssue::EdgeAlreadyWired {
                    channel: "trades",
                    procedure: "route_latest"
                }
            )),
        "the shared traffic edge must be refused, not silently re-wired"
    );

    let two = build_topology_for::<TwoNamedRouterOperative>().expect("topology");
    assert!(two.procedure_name_to_id.contains_key("primary"));
    assert!(two.procedure_name_to_id.contains_key("secondary"));
    assert!(!two.procedure_name_to_id.contains_key("route_latest"));
}

struct SelfRoutedOperative;

impl StreamOperative for SelfRoutedOperative {
    fn factory_id() -> &'static str {
        "route_latest_self"
    }

    fn channels(c: &ChannelBuilder) {
        let both = c.input::<Trade>("trades");
        c.route_latest(both, both, |t: &Trade| t.symbol.clone())
            .sealed();
    }
}

/// fails when: one channel serving as both the document and the traffic stream
/// is accepted — the router would retain traffic items as documents and deliver
/// them back to their own keys.
#[test]
fn one_channel_cannot_be_both_streams() {
    assert!(
        issues::<SelfRoutedOperative>()
            .iter()
            .any(|e| matches!(e, TopologyIssue::DuplicateMergeSource { channel: "trades" })),
        "one channel serving as both streams must be refused"
    );
}
