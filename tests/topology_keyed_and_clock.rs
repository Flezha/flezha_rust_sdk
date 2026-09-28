//! Keyed-channel declaration and the build-time clock check: what a keyed
//! edge must carry, and which clock a class may ask for.

mod common;
use common::prelude::*;

// ----- Keyed channel declaration + build-time clock validation -----

test_tasklet!(KeyedProducerTasklet, "keyed_producer_tasklet", String => String, (), |input, ctx| {
    ctx.emit_keyed(input.clone(), input)?;
    Ok(())
});

test_tasklet!(KeyedConsumerTasklet, "keyed_consumer_tasklet", String => (), (), |_input, _ctx| {
    Ok(())
});

/// `.keyed()` with a pure plugin wire is accepted and recorded.
struct KeyedPluginOperator;
impl UnboundedOperator for KeyedPluginOperator {
    fn factory_id() -> &'static str {
        "keyed_plugin_operator"
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
    let topology = build_topology_for::<KeyedPluginOperator>().expect("topology");
    let out = topology.lookup_channel_id("out").unwrap();
    assert!(topology.channels[out as usize].keyed);
    assert!(!topology.channels[0].keyed, "entry channel is not keyed");
}

/// `.keyed()` on a hybrid (`sink_with`) channel is accepted.
struct KeyedHybridOperator;
impl UnboundedOperator for KeyedHybridOperator {
    fn factory_id() -> &'static str {
        "keyed_hybrid_operator"
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
    let topology = build_topology_for::<KeyedHybridOperator>().expect("topology");
    let out = topology.lookup_channel_id("out").unwrap();
    assert!(topology.channels[out as usize].keyed);
    assert!(topology.channels[out as usize].plugin_sink.is_some());
}

/// `.keyed()` on a channel with no plugin wire fails the build with a
/// typed error naming the channel.
struct KeyedInterTaskletOperator;
impl UnboundedOperator for KeyedInterTaskletOperator {
    fn factory_id() -> &'static str {
        "keyed_inter_tasklet_operator"
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
    let err = match build_topology_for::<KeyedInterTaskletOperator>() {
        Ok(_) => panic!("keyed inter-tasklet channel must fail the build"),
        Err(e) => e,
    };
    assert!(err.errors.iter().any(|e| matches!(
        e,
        flezha_runner::channel::builder::TopologyIssue::KeyedChannelWithoutPluginWire { channel }
            if channel == "hop"
    )));
}

/// A logical clock with `Change = NoChange` cannot be served (the durable
/// change seq never advances) and fails the build naming the clock.
#[derive(Default)]
struct FrozenLogicalTasklet;
#[flezha_runner::tasklet(name = "frozen_logical_tasklet", logical_clock)]
impl Tasklet for FrozenLogicalTasklet {
    type Input = String;
    type Output = String;
    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

struct FrozenLogicalOperator;
impl UnboundedOperator for FrozenLogicalOperator {
    fn factory_id() -> &'static str {
        "frozen_logical_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("inp").into::<FrozenLogicalTasklet>();
    }
}

#[test]
fn logical_clock_without_persistent_changes_is_rejected_at_build() {
    let err = match build_topology_for::<FrozenLogicalOperator>() {
        Ok(_) => panic!("a logical clock with NoChange must fail the build"),
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
