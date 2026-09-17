//! Channel-declaration fixtures: the three bounded-mailbox overflow policies,
//! the hybrid plugin/inter-procedure channel, and the two stream-graph shapes
//! every SDK carries: a merge and a transform chain.

use crate::*;

// -- BoundedMailboxOperative ------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct WorkflowProcedureState {
    pub processed: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum WorkflowChange {
    Inc,
}
impl Change for WorkflowChange {}

fn apply_inc(state: &mut WorkflowProcedureState, _: &WorkflowChange) {
    state.processed += 1;
}

#[derive(Default)]
struct DropNothingTestProcedure;
impl Procedure for DropNothingTestProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = WorkflowProcedureState;
    type Change = WorkflowChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_nothing_test_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> WorkflowProcedureState {
        WorkflowProcedureState::default()
    }
    fn apply_change(state: &mut WorkflowProcedureState, change: &WorkflowChange) {
        apply_inc(state, change);
    }
    fn mailbox_capacity() -> usize {
        4
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(WorkflowChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

#[derive(Default)]
struct DropNewestTestProcedure;
impl Procedure for DropNewestTestProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = WorkflowProcedureState;
    type Change = WorkflowChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_newest_test_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> WorkflowProcedureState {
        WorkflowProcedureState::default()
    }
    fn apply_change(state: &mut WorkflowProcedureState, change: &WorkflowChange) {
        apply_inc(state, change);
    }
    fn mailbox_capacity() -> usize {
        4
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(WorkflowChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

#[derive(Default)]
struct DropOldestTestProcedure;
impl Procedure for DropOldestTestProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = WorkflowProcedureState;
    type Change = WorkflowChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_oldest_test_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> WorkflowProcedureState {
        WorkflowProcedureState::default()
    }
    fn apply_change(state: &mut WorkflowProcedureState, change: &WorkflowChange) {
        apply_inc(state, change);
    }
    fn mailbox_capacity() -> usize {
        4
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(WorkflowChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

// One operative per overflow policy: the graph-shape rule
// (`sdk-graph-authoring`) allows exactly one entry channel per operative, so
// the former three-entry BoundedMailboxOperative is now three classes.
pub(crate) struct BoundedMailboxDropNothingOperative;
impl StreamOperative for BoundedMailboxDropNothingOperative {
    fn factory_id() -> &'static str {
        "bounded_mailbox_drop_nothing"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("drop_nothing_input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<DropNothingTestProcedure>()
            .sink_named("drop_nothing_echo");
    }
}

pub(crate) struct BoundedMailboxDropNewestOperative;
impl StreamOperative for BoundedMailboxDropNewestOperative {
    fn factory_id() -> &'static str {
        "bounded_mailbox_drop_newest"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("drop_newest_input")
            .on_full(OverflowPolicy::DropNewest)
            .into::<DropNewestTestProcedure>()
            .sink_named("drop_newest_echo");
    }
}

pub(crate) struct BoundedMailboxDropOldestOperative;
impl StreamOperative for BoundedMailboxDropOldestOperative {
    fn factory_id() -> &'static str {
        "bounded_mailbox_drop_oldest"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("drop_oldest_input")
            .on_full(OverflowPolicy::DropOldest)
            .into::<DropOldestTestProcedure>()
            .sink_named("drop_oldest_echo");
    }
}

// -- HybridChannelOperative -------------------------------------------------------
//
// Demonstrates `sink_with(...)`: the producer Procedure emits a
// `u32` value that fans out to an inter-procedure consumer and is also
// serialized to bytes for the `hybrid_indicators` plugin. The
// serializer panics on the sentinel `0xDEADBEEF` so an operator can
// observe the panic-isolation guarantee end-to-end.

#[derive(Default)]
struct HybridProducerProcedure;
impl Procedure for HybridProducerProcedure {
    type Input = String;
    type Output = u32;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid_producer_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let n: u32 = input.parse().unwrap_or(0);
        match ctx.emit::<u32>(n) {
            Ok(()) => Ok(()),
            Err(ProcedureError::PluginSerializerPanic { channel, message }) => {
                eprintln!("hybrid_producer_procedure: serializer panic on `{channel}`: {message}");
                Ok(())
            }
            Err(other) => Err(other),
        }
    }
}

#[derive(Default)]
struct HybridConsumerProcedure;
impl Procedure for HybridConsumerProcedure {
    type Input = u32;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid_consumer_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: u32,
        _: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        println!("hybrid_consumer_procedure: received {input}");
        Ok(())
    }
}

pub(crate) struct HybridChannelOperative;
impl StreamOperative for HybridChannelOperative {
    fn factory_id() -> &'static str {
        "hybrid_channel"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<HybridProducerProcedure>()
            .sink_named_with("hybrid_indicator", |v: &u32| {
                if *v == 0xDEAD_BEEF {
                    panic!("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF");
                }
                v.to_be_bytes().to_vec()
            })
            .into::<HybridConsumerProcedure>();
    }
}

// -- MergedInputOperative ---------------------------------------------------------
//
// Two entry channels merged into one procedure. Each source's mapping tags the
// value with the channel it came from, the merge keys on the symbol the mapping
// read, and the emitted line is `<source>:<payload>`.

#[derive(Clone, Debug)]
pub(crate) struct MergedInput {
    source: &'static str,
    symbol: String,
    payload: String,
}

fn tagged(source: &'static str) -> impl Fn(String) -> MergedInput + Send + Sync + 'static {
    move |payload: String| MergedInput {
        source,
        symbol: key_prefix(&payload),
        payload,
    }
}

#[derive(Default)]
struct MergeLabelProcedure;
impl Procedure for MergeLabelProcedure {
    type Input = MergedInput;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "merge_label_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: MergedInput,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(format!("{}:{}", input.source, input.payload))
    }
}

pub(crate) struct MergedInputOperative;
impl StreamOperative for MergedInputOperative {
    fn factory_id() -> &'static str {
        "merged_input"
    }
    fn channels(c: &ChannelBuilder) {
        let ticks = c.input::<String>("ticks");
        let quotes = c.input::<String>("quotes");
        c.merge::<MergedInput>()
            .from(ticks, tagged("ticks"))
            .from(quotes, tagged("quotes"))
            .key_by(|v: &MergedInput| v.symbol.clone())
            .into::<MergeLabelProcedure>()
            .keyed()
            .sink_named("merged");
    }
}

// -- TransformChainOperative ------------------------------------------------------
//
// Every value transform on one edge: `drop` is filtered out, a comma list fans
// out into one value per element, each is upper-cased and keyed on its prefix.

#[derive(Default)]
struct TransformLabelProcedure;
impl Procedure for TransformLabelProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "transform_label_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit(input)
    }
}

pub(crate) struct TransformChainOperative;
impl StreamOperative for TransformChainOperative {
    fn factory_id() -> &'static str {
        "transform_chain"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("trades")
            .filter(|t: &String| t != "drop")
            .flat_map(|t: String| t.split(',').map(str::to_string).collect::<Vec<_>>())
            .map(|t: String| t.to_uppercase())
            .key_by(|t: &String| key_prefix(t))
            .into::<TransformLabelProcedure>()
            .keyed()
            .sink_named("transformed");
    }
}
