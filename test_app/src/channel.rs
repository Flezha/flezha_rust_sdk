//! Channel-declaration fixtures: the three bounded-mailbox overflow policies,
//! the hybrid plugin/inter-tasklet channel, and the two stream-graph shapes
//! every SDK carries: a merge and a transform chain.

use crate::*;

// -- BoundedMailboxOperator ------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct BoundedTaskletState {
    pub processed: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum BoundedChange {
    Inc,
}
impl Change<BoundedTaskletState> for BoundedChange {
    fn apply_to(&self, state: &mut BoundedTaskletState) {
        state.processed += 1;
    }
}

#[derive(Default)]
struct DropNothingTestTasklet;
#[flezha_runner::tasklet(name = "drop_nothing_test_tasklet")]
impl Tasklet for DropNothingTestTasklet {
    type Input = String;
    type Output = Vec<u8>;
    type State = BoundedTaskletState;
    type Change = BoundedChange;
    fn mailbox_capacity() -> usize {
        4
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(BoundedChange::Inc);
        ctx.emit(input.into_bytes())
    }
}

#[derive(Default)]
struct DropNewestTestTasklet;
#[flezha_runner::tasklet(name = "drop_newest_test_tasklet")]
impl Tasklet for DropNewestTestTasklet {
    type Input = String;
    type Output = Vec<u8>;
    type State = BoundedTaskletState;
    type Change = BoundedChange;
    fn mailbox_capacity() -> usize {
        4
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(BoundedChange::Inc);
        ctx.emit(input.into_bytes())
    }
}

#[derive(Default)]
struct DropOldestTestTasklet;
#[flezha_runner::tasklet(name = "drop_oldest_test_tasklet")]
impl Tasklet for DropOldestTestTasklet {
    type Input = String;
    type Output = Vec<u8>;
    type State = BoundedTaskletState;
    type Change = BoundedChange;
    fn mailbox_capacity() -> usize {
        4
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(BoundedChange::Inc);
        ctx.emit(input.into_bytes())
    }
}

// One operator per overflow policy: the graph-shape rule
// (`sdk-graph-authoring`) allows exactly one entry channel per operator, so
// the former three-entry BoundedMailboxOperator is now three classes.
pub(crate) struct BoundedMailboxDropNothingOperator;
impl UnboundedOperator for BoundedMailboxDropNothingOperator {
    fn factory_id() -> &'static str {
        "bounded_mailbox_drop_nothing"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("drop_nothing_input")
            .on_full(OverflowPolicy::DropNothing)
            .into::<DropNothingTestTasklet>()
            .sink_named("drop_nothing_echo");
    }
}

pub(crate) struct BoundedMailboxDropNewestOperator;
impl UnboundedOperator for BoundedMailboxDropNewestOperator {
    fn factory_id() -> &'static str {
        "bounded_mailbox_drop_newest"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("drop_newest_input")
            .on_full(OverflowPolicy::DropNewest)
            .into::<DropNewestTestTasklet>()
            .sink_named("drop_newest_echo");
    }
}

pub(crate) struct BoundedMailboxDropOldestOperator;
impl UnboundedOperator for BoundedMailboxDropOldestOperator {
    fn factory_id() -> &'static str {
        "bounded_mailbox_drop_oldest"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("drop_oldest_input")
            .on_full(OverflowPolicy::DropOldest)
            .into::<DropOldestTestTasklet>()
            .sink_named("drop_oldest_echo");
    }
}

// -- HybridChannelOperator -------------------------------------------------------
//
// Demonstrates `sink_with(...)`: the producer Tasklet emits a
// `u32` value that fans out to an inter-tasklet consumer and is also
// serialized to bytes for the `hybrid_indicators` plugin. The
// serializer panics on the sentinel `0xDEADBEEF` so a user can
// observe the panic-isolation guarantee end-to-end.

#[derive(Default)]
struct HybridProducerTasklet;
#[flezha_runner::tasklet(name = "hybrid_producer_tasklet")]
impl Tasklet for HybridProducerTasklet {
    type Input = String;
    type Output = u32;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let n: u32 = input.parse().unwrap_or(0);
        match ctx.emit(n) {
            Ok(()) => Ok(()),
            Err(TaskletError::PluginSerializerPanic { channel, message }) => {
                eprintln!("hybrid_producer_tasklet: serializer panic on `{channel}`: {message}");
                Ok(())
            }
            Err(other) => Err(other),
        }
    }
}

#[derive(Default)]
struct HybridConsumerTasklet;
#[flezha_runner::tasklet(name = "hybrid_consumer_tasklet")]
impl Tasklet for HybridConsumerTasklet {
    type Input = u32;
    fn run(&mut self, input: u32, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        println!("hybrid_consumer_tasklet: received {input}");
        Ok(())
    }
}

pub(crate) struct HybridChannelOperator;
impl UnboundedOperator for HybridChannelOperator {
    fn factory_id() -> &'static str {
        "hybrid_channel"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<HybridProducerTasklet>()
            .sink_named_with("hybrid_indicator", |v: &u32| {
                if *v == 0xDEAD_BEEF {
                    panic!("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF");
                }
                v.to_be_bytes().to_vec()
            })
            .into::<HybridConsumerTasklet>();
    }
}

// -- MergedInputOperator ---------------------------------------------------------
//
// Two entry channels merged into one tasklet. Each source's mapping tags the
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
struct MergeLabelTasklet;
#[flezha_runner::tasklet(name = "merge_label_tasklet")]
impl Tasklet for MergeLabelTasklet {
    type Input = MergedInput;
    type Output = String;
    fn run(&mut self, input: MergedInput, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("{}:{}", input.source, input.payload))
    }
}

pub(crate) struct MergedInputOperator;
impl UnboundedOperator for MergedInputOperator {
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
            .into::<MergeLabelTasklet>()
            .keyed()
            .sink_named("merged");
    }
}

// -- TransformChainOperator ------------------------------------------------------
//
// Every value transform on one edge: `drop` is filtered out, a comma list fans
// out into one value per element, each is upper-cased and keyed on its prefix.

#[derive(Default)]
struct TransformLabelTasklet;
#[flezha_runner::tasklet(name = "transform_label_tasklet")]
impl Tasklet for TransformLabelTasklet {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input)
    }
}

pub(crate) struct TransformChainOperator;
impl UnboundedOperator for TransformChainOperator {
    fn factory_id() -> &'static str {
        "transform_chain"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("trades")
            .filter(|t: &String| t != "drop")
            .flat_map(|t: String| t.split(',').map(str::to_string).collect::<Vec<_>>())
            .map(|t: String| t.to_uppercase())
            .key_by(|t: &String| key_prefix(t))
            .into::<TransformLabelTasklet>()
            .keyed()
            .sink_named("transformed");
    }
}
