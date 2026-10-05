//! The attribute and the reference form are one tasklet model: a
//! `#[flezha::tasklet]` block and a block that writes every item by hand
//! register, dispatch and journal the same way.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::relay::builder::build_topology_for;
use flezha_runner::state::Change;
use flezha_runner::tasklet::{ClockKind, Ctx, NoChange, Tasklet, TaskletError};
use flezha_runner::testing::TaskletTestHarness;
use flezha_runner::Relay;

#[derive(Default, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Totals {
    count: u64,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct Added(u64);

impl Change<Totals> for Added {
    fn apply_to(&self, state: &mut Totals) {
        state.count += self.0;
    }
}

/// The everyday form: the types and `run`, the rest from the attribute.
#[derive(Default)]
struct Counter;

#[flezha_runner::tasklet(name = "counter")]
impl Tasklet for Counter {
    type Input = u64;
    type Output = u64;
    type State = Totals;
    type Change = Added;

    fn run(&mut self, input: u64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Added(input));
        ctx.emit(ctx.state().count)
    }
}

/// The same tasklet in the reference form: every item written out.
#[derive(Default)]
struct ManualCounter;

impl Tasklet for ManualCounter {
    type Input = u64;
    type Output = u64;
    type State = Totals;
    type Change = Added;
    const NAME: &'static str = "counter";
    const CLOCK: ClockKind = ClockKind::Wall;

    fn init_state(_: Option<&str>) -> Totals {
        Totals::default()
    }

    fn run(&mut self, input: u64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Added(input));
        ctx.emit(ctx.state().count)
    }
}

/// A stateless tasklet that leaves everything but `Input` and `run` out.
#[derive(Default)]
struct Drain;

#[flezha_runner::tasklet(name = "drain")]
impl Tasklet for Drain {
    type Input = u64;

    fn run(&mut self, _input: u64, _ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

/// Both forms in one graph.
struct MixedRelay;

impl Relay for MixedRelay {
    fn factory_id() -> &'static str {
        "mixed_relay"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<u64>("counts")
            .into::<ManualCounter>()
            .into::<Drain>();
    }
}

/// fails when: the attribute stops writing the documented defaults, so a
/// block that leaves them out is a different tasklet from one that writes them.
#[test]
fn the_attribute_writes_what_the_block_leaves_out() {
    fn stateless<P: Tasklet<State = (), Change = NoChange, Output = ()>>() {}
    stateless::<Drain>();
    assert_eq!(<Drain as Tasklet>::NAME, "drain");
    assert_eq!(<Drain as Tasklet>::CLOCK, ClockKind::Wall);
    assert_eq!(<Counter as Tasklet>::NAME, <ManualCounter as Tasklet>::NAME);
    assert_eq!(
        <Counter as Tasklet>::CLOCK,
        <ManualCounter as Tasklet>::CLOCK
    );
    assert_eq!(<Counter as Tasklet>::init_state(None), Totals::default());
}

/// fails when: a relay can no longer mix an attributed tasklet with one
/// written in the reference form.
#[test]
fn an_attributed_and_a_reference_tasklet_wire_together() {
    let topology = build_topology_for::<MixedRelay>().expect("build topology");
    let names: Vec<&str> = topology.tasklets.iter().map(|t| &*t.name).collect();
    assert_eq!(names, vec!["counter", "drain"]);
}

/// fails when: dispatch through the two forms diverges in state, emits or the
/// journaled change log, the three things a tasklet is answerable for.
#[test]
fn the_two_forms_behave_identically() {
    let inputs = vec![3_u64, 4, 5];

    let mut sugared = TaskletTestHarness::<Counter>::new();
    sugared.apply_inputs(inputs.clone());
    let mut manual = TaskletTestHarness::<ManualCounter>::new();
    manual.apply_inputs(inputs);

    assert_eq!(sugared.state(), manual.state());
    assert_eq!(sugared.recorded_changes(), manual.recorded_changes());
    sugared.replay_equals().expect("replay equality");
    sugared.expect_emits(&[3, 7, 12]);
    manual.expect_emits(&[3, 7, 12]);
}
