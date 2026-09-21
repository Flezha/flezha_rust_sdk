//! Keyed-channel declaration and the build-time clock check: what a keyed
//! edge must carry, and which clock a class may ask for.

use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle};

mod common;
use common::prelude::*;

// ----- Keyed channel declaration + build-time clock validation -----

test_tasklet!(KeyedProducerTasklet, "keyed_producer_tasklet", String => String, (), |input, _state, ctx| {
    ctx.emit_keyed::<String>(input.clone(), input)?;
    Ok(())
});

test_tasklet!(KeyedConsumerTasklet, "keyed_consumer_tasklet", String => (), (), |_input, _state, _ctx| {
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
            .into::<KeyedProducerTasklet>()
            .keyed()
            .sink_named("out");
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
            .into::<KeyedProducerTasklet>()
            .sink_named_with("out", |v: &String| v.clone().into_bytes())
            .keyed()
            .into::<KeyedConsumerTasklet>();
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
struct KeyedInterTaskletOperative;
impl StreamOperative for KeyedInterTaskletOperative {
    fn factory_id() -> &'static str {
        "keyed_inter_tasklet_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp")
            .into::<KeyedProducerTasklet>()
            .named("hop")
            .keyed()
            .into::<KeyedConsumerTasklet>();
    }
}

#[test]
fn keyed_channel_without_plugin_wire_is_rejected() {
    let err = match build_topology_for::<KeyedInterTaskletOperative>() {
        Ok(_) => panic!("keyed inter-tasklet channel must fail the build"),
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
struct FrozenLogicalTasklet;
impl Tasklet for FrozenLogicalTasklet {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::state::LogicalClock;
    type Config = ();
    fn name() -> &'static str {
        "frozen_logical_tasklet"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _: String,
        _state: StateHandle<'_, Self>,
        _: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}

struct FrozenLogicalOperative;
impl StreamOperative for FrozenLogicalOperative {
    fn factory_id() -> &'static str {
        "frozen_logical_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp").into::<FrozenLogicalTasklet>();
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
            tasklet,
            clock_type,
        } if tasklet == "frozen_logical_tasklet" && clock_type == "logical"
    )));
}
