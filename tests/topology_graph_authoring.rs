//! Graph authoring: the shapes the wiring accepts, the refusals it answers
//! with, and how a typed entry channel decodes at the edge.

mod common;
use common::prelude::*;
use flezha_runner::Flezha;

// ---- sdk-graph-authoring pins -------------------------------------------

test_tasklet!(GraphStage1, "graph_stage1", String => String, (), |_input, _ctx| {
    Ok(())
});
test_tasklet!(GraphStage2, "graph_stage2", String => String, (), |_input, _ctx| {
    Ok(())
});
test_tasklet!(GraphMerge, "graph_merge", String => String, (), |_input, _ctx| {
    Ok(())
});

/// One channel flattened to `(name, kind, producer, consumers)`.
type ChannelShape = (
    String,
    flezha_runner::channel::ChannelKind,
    Option<u32>,
    Vec<u32>,
);

/// Flatten a frozen topology to a comparable shape: channels as
/// `(name, kind, producer, consumers)` plus the tasklet identity list.
fn topo_shape(t: &flezha_runner::operator::OperatorTopology) -> (Vec<ChannelShape>, Vec<String>) {
    (
        t.channels
            .iter()
            .map(|c| {
                (
                    c.name.to_string(),
                    c.kind,
                    c.producer,
                    c.consumer_ids().collect::<Vec<_>>(),
                )
            })
            .collect(),
        t.tasklets.iter().map(|o| o.name.to_string()).collect(),
    )
}

/// `branch(...)` is pure sugar: the frozen topology is identical to the
/// handle-reuse spelling (same channels, consumers, derived names).
#[test]
fn branch_builds_the_identical_topology() {
    struct ByHandles;
    impl Operator for ByHandles {
        fn factory_id() -> &'static str {
            "graph_by_handles"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoTasklet>();
            validated.into::<GraphStage1>();
            validated.into::<GraphStage2>();
        }
}
    struct ByBranch;
    impl Operator for ByBranch {
        fn factory_id() -> &'static str {
            "graph_by_branch"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<EchoTasklet>().branch([
                &|b| {
                    b.into::<GraphStage1>();
                },
                &|b| {
                    b.into::<GraphStage2>();
                },
            ]);
        }
}
    let handles = build_topology_for::<ByHandles>().expect("handle-reuse builds");
    let branch = build_topology_for::<ByBranch>().expect("branch builds");
    assert_eq!(topo_shape(&handles), topo_shape(&branch));
}

#[test]
fn branch_with_fewer_than_two_arms_is_refused() {
    struct OneArm;
    impl Operator for OneArm {
        fn factory_id() -> &'static str {
            "graph_one_arm"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in").into::<EchoTasklet>().branch([&|b| {
                b.into::<GraphStage1>();
            }]);
        }
}
    let err = match build_topology_for::<OneArm>() {
        Ok(_) => panic!("one arm must fail"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::BranchTooFewArms { arms: 1 }
    )));
}

/// Node identity: an explicit node name creates a distinct node whose
/// identity replaces `NAME` everywhere — descriptor name and the derived
/// channel name included — so one class serves several graph positions.
#[test]
fn named_nodes_reuse_one_class_at_two_positions() {
    struct TwoFilters;
    impl Operator for TwoFilters {
        fn factory_id() -> &'static str {
            "graph_two_filters"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoTasklet>();
            validated
                .into_named::<GraphStage1>("fraud_filter")
                .into::<GraphMerge>();
            validated
                .into_named::<GraphStage1>("spam_filter")
                .into::<GraphMerge>();
        }
}
    let t = build_topology_for::<TwoFilters>().expect("named nodes build");
    let names: Vec<&str> = t.tasklets.iter().map(|o| o.name.as_ref()).collect();
    assert_eq!(
        names,
        vec!["echo_tasklet", "fraud_filter", "graph_merge", "spam_filter"]
    );
    // Derived channel names are the node identities.
    assert!(t.lookup_channel_id("fraud_filter").is_some());
    assert!(t.lookup_channel_id("spam_filter").is_some());
    // The merge node accumulates both named nodes' channels: fan-in.
    let merge_id = *t.tasklet_name_to_id.get("graph_merge").unwrap();
    assert_eq!(t.tasklets[merge_id as usize].input_channel_ids.len(), 2);
}

/// Two unnamed mentions of one class resolve to the same node — which is
/// exactly how fan-in is spelled.
#[test]
fn unnamed_mentions_unify_into_a_fan_in() {
    struct FanInGraph;
    impl Operator for FanInGraph {
        fn factory_id() -> &'static str {
            "graph_fan_in"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoTasklet>();
            validated.into::<GraphStage1>().into::<GraphMerge>();
            validated.into::<GraphStage2>().into::<GraphMerge>();
        }
}
    let t = build_topology_for::<FanInGraph>().expect("fan-in builds");
    assert_eq!(t.tasklets.len(), 4, "one Merge node, not two");
    let merge_id = *t.tasklet_name_to_id.get("graph_merge").unwrap();
    assert_eq!(
        t.tasklets[merge_id as usize].input_channel_ids.len(),
        2,
        "the join consumes both producers' channels"
    );
}

#[test]
fn node_identity_collision_is_refused() {
    struct Colliding;
    impl Operator for Colliding {
        fn factory_id() -> &'static str {
            "graph_identity_collision"
        }
        fn channels(c: &ChannelBuilder) {
            let validated = c.input::<String>("in").into::<EchoTasklet>();
            // A node name equal to another tasklet's `NAME`.
            validated.into_named::<GraphStage2>("graph_stage1");
            validated.into::<GraphStage1>();
        }
}
    let err = match build_topology_for::<Colliding>() {
        Ok(_) => panic!("identity collision must fail"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::NodeIdentityCollision { name }
            if name == "graph_stage1"
    )));
}

/// The channel out of a node is named by the node's identity unless
/// `.named(...)` overrides it; all names share one namespace with a
/// duplicate check.
#[test]
fn channel_names_derive_from_the_producer_identity() {
    struct Derived;
    impl Operator for Derived {
        fn factory_id() -> &'static str {
            "graph_derived_names"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<EchoTasklet>()
                .into::<GraphStage1>()
                .named("orders.validated")
                .into::<GraphStage2>();
        }
}
    let t = build_topology_for::<Derived>().expect("derived names build");
    assert!(
        t.lookup_channel_id("echo_tasklet").is_some(),
        "no override ⇒ the producer's identity"
    );
    assert!(
        t.lookup_channel_id("orders.validated").is_some(),
        ".named(...) overrides the derived name"
    );

    struct NameClash;
    impl Operator for NameClash {
        fn factory_id() -> &'static str {
            "graph_name_clash"
        }
        fn channels(c: &ChannelBuilder) {
            // The entry claims the name the derived channel would take.
            c.input::<String>("echo_tasklet")
                .into::<EchoTasklet>()
                .into::<GraphStage1>();
        }
}
    let err = match build_topology_for::<NameClash>() {
        Ok(_) => panic!("name collision must fail"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::DuplicateChannel(name)
            if name == "echo_tasklet"
    )));
}

#[test]
fn a_cycle_is_refused() {
    struct Cyclic;
    impl Operator for Cyclic {
        fn factory_id() -> &'static str {
            "graph_cyclic"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in")
                .into::<GraphStage1>()
                .into::<GraphStage2>()
                .into::<GraphStage1>();
        }
}
    let err = match build_topology_for::<Cyclic>() {
        Ok(_) => panic!("a cycle must fail"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::Cycle { .. }
    )));
}

#[test]
fn multiple_entry_channels_build() {
    // A single operator may declare several entry channels — one per external
    // source (e.g. a market pipeline reading a trade feed and an order-book
    // feed on distinct channels). The controller routes each frame to its
    // entry by `metadata["channel"]`. Both entries and their subgraphs resolve.
    struct TwoEntries;
    impl Operator for TwoEntries {
        fn factory_id() -> &'static str {
            "graph_two_entries"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("in_a").into::<GraphStage1>();
            c.input::<String>("in_b").into::<GraphStage2>();
        }
}
    let topology = build_topology_for::<TwoEntries>()
        .expect("multiple entry channels must build a valid topology");
    let entries: Vec<&str> = topology
        .channels
        .iter()
        .filter(|c| c.kind == flezha_runner::channel::ChannelKind::Entry)
        .map(|c| &*c.name)
        .collect();
    assert_eq!(entries.len(), 2, "both entry channels are retained");
    assert!(entries.contains(&"in_a") && entries.contains(&"in_b"));
}

// ---- typed entry inputs ---------------------------------------------------

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
struct OrderEvent {
    id: u64,
    sku: String,
}

test_tasklet!(OrderConsumer, "order_consumer", OrderEvent => String, (), |_input, _ctx| {
    Ok(())
});

/// `input::<T>` for a non-passthrough type decodes JSON at the entry edge;
/// the descriptor is flagged as a real parser so a failing decode rides the
/// retry→DLQ ladder.
#[test]
fn typed_entry_decodes_json_at_the_edge() {
    struct TypedEntry;
    impl Operator for TypedEntry {
        fn factory_id() -> &'static str {
            "graph_typed_entry"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<OrderEvent>("orders").into::<OrderConsumer>();
        }
}
    let t = build_topology_for::<TypedEntry>().expect("typed entry builds");
    let entry = &t.channels[0];
    assert!(entry.entry_parsed, "a typed entry is a real parser");
    let wrap = entry.wrap_input.as_ref().expect("entry wrapper present");

    let ok = wrap(br#"{"id":7,"sku":"X1"}"#.to_vec()).expect("valid JSON decodes");
    let decoded = ok.downcast_ref::<OrderEvent>().expect("typed payload");
    assert_eq!(
        decoded,
        &OrderEvent {
            id: 7,
            sku: "X1".into()
        }
    );
    assert!(
        wrap(b"not json".to_vec()).is_err(),
        "a failing parse errs (and rides the channel's retry→DLQ ladder)"
    );
}

/// `input_with` replaces the default JSON codec with the supplied parser.
#[test]
fn a_custom_parser_replaces_the_default_json() {
    struct CustomEntry;
    impl Operator for CustomEntry {
        fn factory_id() -> &'static str {
            "graph_custom_entry"
        }
        fn channels(c: &ChannelBuilder) {
            c.input_with::<OrderEvent, _>("orders", |bytes| {
                let s = String::from_utf8(bytes).map_err(|e| e.to_string())?;
                let (id, sku) = s.split_once(':').ok_or("expected id:sku")?;
                Ok(OrderEvent {
                    id: id.parse().map_err(|_| "bad id".to_string())?,
                    sku: sku.to_string(),
                })
            })
            .into::<OrderConsumer>();
        }
}
    let t = build_topology_for::<CustomEntry>().expect("custom parser builds");
    let entry = &t.channels[0];
    assert!(entry.entry_parsed);
    let wrap = entry.wrap_input.as_ref().unwrap();
    // The custom format decodes; JSON (the default it replaced) does not.
    let ok = wrap(b"9:ABC".to_vec()).expect("custom format decodes");
    assert_eq!(ok.downcast_ref::<OrderEvent>().unwrap().sku, "ABC");
    assert!(wrap(br#"{"id":9,"sku":"ABC"}"#.to_vec()).is_err());
}

/// `Vec<u8>`/`String` entries keep the pre-typed-entry contract byte-identical:
/// passthrough / UTF-8 decode, flagged as non-parsers (invalid input keeps
/// the drop-and-log path, not the DLQ ladder).
#[test]
fn bytes_and_string_entries_stay_passthrough() {
    let t = build_topology_for::<SingleOperator>().expect("string entry builds");
    let entry = &t.channels[0];
    assert!(!entry.entry_parsed, "String is not a parser");
    let wrap = entry.wrap_input.as_ref().unwrap();
    let ok = wrap(b"plain".to_vec()).expect("utf-8 decodes");
    assert_eq!(ok.downcast_ref::<String>().unwrap(), "plain");
    assert!(wrap(vec![0xff, 0xfe]).is_err(), "invalid UTF-8 still errs");

    test_tasklet!(BytesConsumer, "bytes_consumer", Vec<u8> => String, (), |_input, _ctx| {
        Ok(())
    });
    struct BytesEntry;
    impl Operator for BytesEntry {
        fn factory_id() -> &'static str {
            "graph_bytes_entry"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<Vec<u8>>("raw").into::<BytesConsumer>();
        }
}
    let t = build_topology_for::<BytesEntry>().expect("bytes entry builds");
    let entry = &t.channels[0];
    assert!(!entry.entry_parsed);
    let wrap = entry.wrap_input.as_ref().unwrap();
    let ok = wrap(vec![1, 2, 3]).expect("bytes pass through");
    assert_eq!(ok.downcast_ref::<Vec<u8>>().unwrap(), &vec![1, 2, 3]);
}

// ---- the sugar and the hand-written spelling name one channel -----------

/// A topology's channel names in declaration order; the entry channel is
/// first, so a shorthand's whole naming answer is one vector.
fn channel_names(t: &flezha_runner::operator::OperatorTopology) -> Vec<String> {
    t.channels.iter().map(|c| c.name.to_string()).collect()
}

/// The short form: `sink = "..."` is the only naming call the author writes,
/// and it is the channel's name.
#[flezha_runner::operator(id = "generated_naming", sink = "items_output")]
fn generated_naming(input: String) -> Option<String> {
    Some(input)
}

/// The same operator spelled by hand, around the short form's own tasklet.
struct HandWrittenNaming;
impl Operator for HandWrittenNaming {
    fn factory_id() -> &'static str {
        "hand_written_naming"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<GeneratedNamingTasklet>()
            .sink_named("items_output");
    }
}

/// fails when: the generator and a hand-written declaration disagree about a
/// channel's name again — the disagreement that made a sugared operator ship
/// frames under one name and a hand-written one under another.
#[test]
fn the_generated_and_hand_written_spellings_name_one_channel() {
    let generated = build_topology_for::<GeneratedNaming>().expect("the sugar builds");
    let by_hand = build_topology_for::<HandWrittenNaming>().expect("the hand-written twin builds");

    assert_eq!(channel_names(&generated), vec!["input", "items_output"]);
    assert_eq!(channel_names(&generated), channel_names(&by_hand));
}

/// Which roles `T` plays, read at compile time: an inherent associated const
/// shadows the trait's default only when its impl's bound holds.
struct Roles<T>(std::marker::PhantomData<T>);
trait NoRole {
    const TASKLET: bool = false;
    const OPERATOR: bool = false;
}
impl<T> NoRole for Roles<T> {}
impl<T: flezha_runner::Tasklet> Roles<T> {
    const TASKLET: bool = true;
}
impl<T: Operator> Roles<T> {
    const OPERATOR: bool = true;
}

/// fails when: the short form makes one type play both roles again, so the
/// name the author registers is also a tasklet.
#[test]
fn a_short_form_is_an_operator_beside_its_tasklet() {
    const {
        assert!(<Roles<GeneratedNaming>>::OPERATOR);
        assert!(!<Roles<GeneratedNaming>>::TASKLET);
        assert!(<Roles<GeneratedNamingTasklet>>::TASKLET);
        assert!(!<Roles<GeneratedNamingTasklet>>::OPERATOR);
    }
    // The tasklet is named after the factory id, so ejecting to the full
    // form keeps every cell's state path.
    assert_eq!(
        <GeneratedNamingTasklet as flezha_runner::Tasklet>::NAME,
        "generated_naming"
    );
}

/// The short form's entry channel carries the overflow policy it declares.
#[flezha_runner::operator(id = "shedding_entry", sink = "shed_items", on_full = drop_oldest)]
fn shedding_entry(input: String) -> Vec<u8> {
    input.into_bytes()
}

/// fails when: `on_full` on a short form stops reaching the entry channel it
/// describes.
#[test]
fn a_short_form_sets_its_entry_channels_overflow_policy() {
    let t = build_topology_for::<SheddingEntry>().expect("the short form builds");
    assert_eq!(
        t.channels[0].overflow_policy,
        flezha_runner::OverflowPolicy::DropOldest
    );
}

// ---- the short form with no sink ------------------------------------------

#[derive(serde::Serialize, serde::Deserialize, Clone, Default, PartialEq, Debug)]
struct CounterState {
    seen: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Seen(String);

impl flezha_runner::Change<CounterState> for Seen {
    fn apply_to(&self, state: &mut CounterState) {
        state.seen.push(self.0.clone());
    }
}

/// A short form that only keeps state: no `sink`, so no plugin channel.
#[derive(Default)]
struct SinklessCounter;

#[flezha_runner::operator(id = "sinkless_counter", input = "ticks")]
impl flezha_runner::Tasklet for SinklessCounter {
    type Input = String;
    type State = CounterState;
    type Change = Seen;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Seen(input));
        Ok(())
    }
}

/// The same operator spelled by hand: a channel ending at a tasklet.
struct HandWrittenCounter;
impl Operator for HandWrittenCounter {
    fn factory_id() -> &'static str {
        "hand_written_counter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("ticks").into::<SinklessCounter>();
    }
}

/// fails when: a short form with no `sink` stops declaring exactly the entry
/// channel into its tasklet, which is what the full form declares.
#[test]
fn a_short_form_with_no_sink_declares_only_its_entry_channel() {
    let generated =
        build_topology_for::<SinklessCounter>().expect("the sinkless short form builds");
    let by_hand = build_topology_for::<HandWrittenCounter>().expect("the hand-written twin builds");

    assert_eq!(channel_names(&generated), vec!["ticks"]);
    assert_eq!(channel_names(&generated), channel_names(&by_hand));
    assert!(generated.channels.iter().all(|c| c.plugin_sink.is_none()));

    let runner = Flezha::create_runner("ns_sinkless_short_form").expect("create");
    runner
        .register_factory(SinklessCounter)
        .expect("a flow short form registers through the one method");
}

// ---- the shorthand's entry channel takes the name the author writes ------

/// The attribute shorthand, naming its entry channel.
#[flezha_runner::operator(
    id = "named_entry_attribute",
    sink = "attr_items",
    input = "prices"
)]
fn named_entry_attribute(input: String) -> Vec<u8> {
    input.into_bytes()
}

/// The attribute shorthand with no name written: the entry channel keeps the
/// default address every existing operator already ships under.
#[flezha_runner::operator(id = "default_entry_attribute", sink = "default_items")]
fn default_entry_attribute(input: String) -> Vec<u8> {
    input.into_bytes()
}

#[derive(Default)]
struct PipelineHop;

#[flezha_runner::tasklet(name = "pipeline_hop")]
impl flezha_runner::Tasklet for PipelineHop {
    type Input = String;
    type Output = Vec<u8>;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.into_bytes())
    }
}

flezha_runner::pipeline! {
    pub struct NamedEntryPipeline;
    id: "named_entry_pipeline",
    pipeline: input::<String>("quotes") -> PipelineHop -> plugin::<Vec<u8>>("pipeline_items")
}

flezha_runner::pipeline! {
    pub struct DefaultEntryPipeline;
    id: "default_entry_pipeline",
    pipeline: input::<String> -> PipelineHop -> plugin::<Vec<u8>>("pipeline_items")
}

/// fails when: the shorthand stops honouring the entry channel name the author
/// wrote, so the address a user injects on is not the one declared.
#[test]
fn both_shorthands_name_their_entry_channel() {
    let attr =
        build_topology_for::<NamedEntryAttribute>().expect("the named attribute shorthand builds");
    assert_eq!(channel_names(&attr), vec!["prices", "attr_items"]);

    let pipeline =
        build_topology_for::<NamedEntryPipeline>().expect("the named pipeline shorthand builds");
    assert_eq!(channel_names(&pipeline), vec!["quotes", "pipeline_items"]);
}

/// fails when: an unnamed shorthand stops naming its entry channel `input` —
/// the address §6's conformance table and every existing operator ship under.
#[test]
fn an_unnamed_shorthand_keeps_the_default_entry_channel_name() {
    let attr = build_topology_for::<DefaultEntryAttribute>()
        .expect("the unnamed attribute shorthand builds");
    assert_eq!(channel_names(&attr), vec!["input", "default_items"]);

    let pipeline = build_topology_for::<DefaultEntryPipeline>()
        .expect("the unnamed pipeline shorthand builds");
    assert_eq!(channel_names(&pipeline), vec!["input", "pipeline_items"]);
}

/// fails when: the name the author wrote is not the address a frame travels
/// to — the declaration would name one channel and the wire another.
#[test]
fn a_frame_reaches_a_shorthand_on_the_channel_it_named() {
    let (handle, _instance) = setup::<NamedEntryAttribute>("named-entry");
    let (stream, mut observer) = stream_pair();

    dispatch_input(&handle, "named-entry", "prices", b"hello".to_vec(), &stream);

    let frames = read_frames(&mut observer, 1, Duration::from_secs(5));
    assert_eq!(frames[0].message, b"hello");
}

/// fails when: an unnamed shorthand stops accepting a frame addressed to
/// `input` — every operator in the tree is reached that way.
#[test]
fn a_frame_reaches_an_unnamed_shorthand_on_input() {
    let (handle, _instance) = setup::<DefaultEntryAttribute>("default-entry");
    let (stream, mut observer) = stream_pair();

    dispatch_input(
        &handle,
        "default-entry",
        "input",
        b"hello".to_vec(),
        &stream,
    );

    let frames = read_frames(&mut observer, 1, Duration::from_secs(5));
    assert_eq!(frames[0].message, b"hello");
}

// ---- the steps short form -------------------------------------------

/// A steps short form: one declaration is a whole `once` steps operator, with
/// every steps setting at its default.
#[flezha_runner::operator(id = "place_order", sink = "placed", input = "order", run = steps)]
fn place_order(order: String) -> String {
    order
}

/// fails when: the steps attribute stops producing a steps operator, or the
/// entry and sink names the declaration wrote stop reaching the spec — the
/// short form would then declare a shape the author never wrote.
#[test]
fn a_steps_short_form_is_a_once_steps_wiring_its_entry_to_its_sink() {
    let class =
        flezha_runner::steps::spec::build_spec::<PlaceOrder>().expect("the short form builds");
    let spec = class.steps.as_ref().expect("a steps class");

    assert_eq!(channel_names(&class), vec!["order", "placed"]);
    assert_eq!(spec.each, flezha_runner::Each::Once);
    assert!(class.durable.keeps());
    assert_eq!(class.max_duration, None);

    // The run is read from the declaration, never named by the call.
    let runner = Flezha::create_runner("ns_steps_short_form").expect("create");
    runner
        .register_factory(PlaceOrder)
        .expect("a steps short form registers through the one method");
}
