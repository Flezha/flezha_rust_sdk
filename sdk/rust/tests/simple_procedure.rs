//! The simplified `Procedure` tier. `SimpleProcedure` declares what a
//! procedure is about — Input, Output, State — and inherits `Clock`, `Config`
//! and the derived `Change`. A blanket impl lifts it to `Procedure`, so the
//! claim under test is that "simplified" costs nothing: the same registration,
//! the same dispatch, the same results as the hand-written twin.

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::procedure::{
    DerivedState, Procedure, ProcedureCtx, ProcedureError, SimpleProcedure, StateHandle,
};
use flezha_runner::state::{RestoreInitPayload, WallClock};
use flezha_runner::testing::ProcedureTestHarness;
use flezha_runner::StreamOperative;

#[derive(Default, Debug, PartialEq, serde::Serialize, serde::Deserialize, flezha_runner::State)]
struct Totals {
    count: u64,
}

/// The whole declaration: three types, a name, a state factory and `run`.
#[derive(Default)]
struct SimpleCounter;

impl SimpleProcedure for SimpleCounter {
    type Input = u64;
    type Output = u64;
    type State = Totals;

    fn name() -> &'static str {
        "counter"
    }

    fn init_state(_entity_key: Option<&str>) -> Totals {
        Totals::default()
    }

    fn run(
        &mut self,
        input: u64,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.set_count(state.count + input);
        ctx.emit::<u64>(state.count)
    }
}

/// The same procedure spelled at the full tier — the three defaults written out.
#[derive(Default)]
struct ManualCounter;

impl Procedure for ManualCounter {
    type Input = u64;
    type Output = u64;
    type State = Totals;
    type Change = TotalsChange;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "counter"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> Totals {
        Totals::default()
    }

    fn apply_change(state: &mut Totals, change: &TotalsChange) {
        <Totals as DerivedState>::apply_change(state, change);
    }

    fn run(
        &mut self,
        input: u64,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.set_count(state.count + input);
        ctx.emit::<u64>(state.count)
    }
}

struct SimpleTierOperative;

impl StreamOperative for SimpleTierOperative {
    fn factory_id() -> &'static str {
        "simple_tier_operative"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<u64>("counts")
            .into::<SimpleCounter>()
            .named("totals")
            .sink("totals");
    }
}

/// fails when: the blanket impl stops resolving the three defaults — a
/// `SimpleProcedure` that came out with a different clock, a typed config or
/// an unrelated change type would be a second, subtly different procedure
/// model rather than shorthand for the one that exists.
#[test]
fn the_simplified_tier_resolves_to_the_documented_defaults() {
    fn assert_tail_types<P>()
    where
        P: Procedure<Clock = WallClock, Config = (), Change = TotalsChange, State = Totals>,
    {
    }
    assert_tail_types::<SimpleCounter>();
    assert_eq!(
        <SimpleCounter as Procedure>::name(),
        <ManualCounter as Procedure>::name()
    );
}

/// fails when: a `SimpleProcedure` stops being registrable wherever a
/// `Procedure` is — the tier is only worth having if the wiring cannot tell
/// which tier authored a node.
#[test]
fn a_simplified_procedure_wires_like_any_other() {
    let topology = build_topology_for::<SimpleTierOperative>().expect("build topology");
    assert_eq!(topology.procedures.len(), 1);
    assert_eq!(&*topology.procedures[0].name, "counter");
    assert_eq!(topology.procedures[0].input_channel_ids, vec![0]);
}

/// fails when: dispatch through the blanket impl diverges from the
/// hand-written twin in state, emits or the journaled change log — the three
/// things a procedure is answerable for.
#[test]
fn a_simplified_procedure_behaves_identically_to_its_hand_written_twin() {
    let inputs = vec![3_u64, 4, 5];

    let mut simple = ProcedureTestHarness::<SimpleCounter>::new();
    simple.apply_inputs(inputs.clone());
    let mut manual = ProcedureTestHarness::<ManualCounter>::new();
    manual.apply_inputs(inputs);

    assert_eq!(simple.state(), manual.state());
    assert_eq!(simple.recorded_changes(), manual.recorded_changes());
    simple.replay_equals().expect("replay equality");
    simple.expect_emits(&[3_u64, 7, 12]);
    manual.expect_emits(&[3_u64, 7, 12]);
}
