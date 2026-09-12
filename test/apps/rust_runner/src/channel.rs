//! Channel-declaration fixtures: the three bounded-mailbox overflow policies
//! and the hybrid plugin/inter-procedure channel.

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
