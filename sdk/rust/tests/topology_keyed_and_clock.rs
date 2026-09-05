//! Keyed-channel declaration and the build-time clock check: what a keyed
//! edge must carry, and which clock a class may ask for.

use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle};

mod common;
use common::prelude::*;

// ----- Keyed channel declaration + build-time clock validation -----

test_procedure!(KeyedProducerProcedure, "keyed_producer_procedure", String => String, (), |input, _state, ctx| {
    ctx.emit_keyed::<String>(input.clone(), input)?;
    Ok(())
});

test_procedure!(KeyedConsumerProcedure, "keyed_consumer_procedure", String => (), (), |_input, _state, _ctx| {
    Ok(())
});

/// `.keyed()` with a pure plugin wire is accepted and recorded.
struct KeyedPluginOperative;
impl StreamOperative for KeyedPluginOperative {
    fn factory_id() -> &'static str {
        "keyed_plugin_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp")
            .into::<KeyedProducerProcedure>()
            .named("out")
            .keyed()
            .sink("kafka");
    }
}

#[test]
fn keyed_plugin_channel_is_accepted_and_marked() {
    let topology = build_topology_for::<KeyedPluginOperative>().expect("topology");
    let out = topology.lookup_channel_id("out").unwrap();
    assert!(topology.channels[out as usize].keyed);
    assert!(!topology.channels[0].keyed, "entry channel is not keyed");
}

/// `.keyed()` on a hybrid (`sink_with`) channel is accepted.
struct KeyedHybridOperative;
impl StreamOperative for KeyedHybridOperative {
    fn factory_id() -> &'static str {
        "keyed_hybrid_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp")
            .into::<KeyedProducerProcedure>()
            .named("out")
            .sink_with("kafka", |v: &String| v.clone().into_bytes())
            .keyed()
            .into::<KeyedConsumerProcedure>();
    }
}

#[test]
fn keyed_hybrid_channel_is_accepted_and_marked() {
    let topology = build_topology_for::<KeyedHybridOperative>().expect("topology");
    let out = topology.lookup_channel_id("out").unwrap();
    assert!(topology.channels[out as usize].keyed);
    assert!(topology.channels[out as usize].plugin_sink.is_some());
}

/// `.keyed()` on a channel with no plugin wire fails the build with a
/// typed error naming the channel.
struct KeyedInterProcedureOperative;
impl StreamOperative for KeyedInterProcedureOperative {
    fn factory_id() -> &'static str {
        "keyed_inter_procedure_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp")
            .into::<KeyedProducerProcedure>()
            .named("hop")
            .keyed()
            .into::<KeyedConsumerProcedure>();
    }
}

#[test]
fn keyed_channel_without_plugin_wire_is_rejected() {
    let err = match build_topology_for::<KeyedInterProcedureOperative>() {
        Ok(_) => panic!("keyed inter-procedure channel must fail the build"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::KeyedChannelWithoutPluginWire { channel }
            if channel == "hop"
    )));
}

/// LogicalClock with `Change = NoChange` cannot be served (the durable
/// change seq never advances) and fails the build naming the clock.
#[derive(Default)]
struct FrozenLogicalProcedure;
impl Procedure for FrozenLogicalProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::state::LogicalClock;
    type Config = ();
    fn name() -> &'static str {
        "frozen_logical_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: String,
        _state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Ok(())
    }
}

struct FrozenLogicalOperative;
impl StreamOperative for FrozenLogicalOperative {
    fn factory_id() -> &'static str {
        "frozen_logical_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp").into::<FrozenLogicalProcedure>();
    }
}

#[test]
fn logical_clock_without_persistent_changes_is_rejected_at_build() {
    let err = match build_topology_for::<FrozenLogicalOperative>() {
        Ok(_) => panic!("LogicalClock + NoChange must fail the build"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::UnservableClock {
            procedure,
            clock_type,
        } if procedure == "frozen_logical_procedure" && clock_type == "logical"
    )));
}
