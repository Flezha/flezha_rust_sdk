//! The short-form attributes on an `impl Tasklet` block, a function short
//! form's optional output, and a `#[flezha::tasklet]` block composing with a
//! hand-written one, each run locally through the graph it declares.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use flezha_runner::bounded::LoopMode;
use flezha_runner::{
    tasklet, BoundedOperator, Change, ChannelBuilder, ClockKind, Ctx, LocalOperator, NoChange,
    Tasklet, TaskletError, UnboundedOperator,
};
use serde::{Deserialize, Serialize};

/// Every value released on `sink` while `feed` runs through `R`'s graph.
fn released<R: UnboundedOperator>(entry: &str, sink: &str, feed: &[&str]) -> Vec<String> {
    let out = Arc::new(Mutex::new(Vec::new()));
    let into = Arc::clone(&out);
    let run = LocalOperator::start::<R>().expect("the run starts");
    run.on_sink(sink, move |o| {
        into.lock()
            .unwrap()
            .push(String::from_utf8(o.payload).unwrap());
    })
    .expect("the sink is declared");
    for value in feed {
        run.feed(entry, value).expect("the entry channel accepts");
    }
    run.run_until_idle(Duration::from_secs(20))
        .expect("the graph settles");
    let seen = out.lock().unwrap().clone();
    seen
}

#[derive(Default, Serialize, Deserialize)]
struct StoreInputState {
    inputs: Vec<String>,
}

#[derive(Serialize, Deserialize)]
struct Stored(String);

impl Change<StoreInputState> for Stored {
    fn apply_to(&self, state: &mut StoreInputState) {
        state.inputs.push(self.0.clone());
    }
}

/// A stateful one-tasklet operator: the author's type is both.
#[derive(Default)]
struct StoreInput;

#[flezha_runner::unbounded_operator(id = "store_input", sink = "stored", max_cells = 10)]
impl Tasklet for StoreInput {
    type Input = String;
    type Output = String;
    type State = StoreInputState;
    type Change = Stored;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Stored(input));
        ctx.emit(ctx.state().inputs.join(","))
    }
}

#[test]
fn a_stateful_short_form_sits_on_the_impl_block() {
    assert_eq!(
        <StoreInput as UnboundedOperator>::factory_id(),
        "store_input"
    );
    assert_eq!(StoreInput::NAME, "store_input");
    assert_eq!(StoreInput::max_cells(), 10);
    assert_eq!(
        released::<StoreInput>("input", "stored", &["a", "b"]),
        ["a", "a,b"]
    );
}

/// No `sink`: the entry channel feeds the tasklet and nothing else is declared.
#[derive(Default)]
struct Ticks;

#[flezha_runner::unbounded_operator(id = "ticks_counter", input = "ticks")]
impl Tasklet for Ticks {
    type Input = String;

    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

#[test]
fn a_short_form_with_no_sink_wires_only_the_entry_channel() {
    let run = LocalOperator::start::<Ticks>().expect("the run starts");
    run.feed("ticks", "1")
        .expect("`ticks` is the entry channel");
    assert!(run.feed("input", "1").is_err(), "no default entry channel");
    run.run_until_idle(Duration::from_secs(20))
        .expect("the graph settles");
}

#[derive(Default)]
struct PlaceIt;

#[flezha_runner::bounded_operator(
    id = "place_it",
    sink = "placed",
    input = "order",
    loop_mode = "per_input"
)]
impl Tasklet for PlaceIt {
    type Input = String;
    type Output = String;

    fn run(&mut self, order: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(order)
    }
}

#[test]
fn a_bounded_short_form_on_the_impl_block_implements_the_bounded_trait() {
    assert_eq!(<PlaceIt as BoundedOperator>::factory_id(), "place_it");
    assert_eq!(PlaceIt::loop_mode(), LoopMode::PerInput);
    assert_eq!(PlaceIt::NAME, "place_it");
    assert_eq!(PlaceIt::CLOCK, ClockKind::Wall);
}

#[flezha_runner::unbounded_operator(id = "evens_only", sink = "evens")]
fn evens_only(n: String) -> Option<String> {
    (n.parse::<u64>().ok()? % 2 == 0).then_some(n)
}

#[test]
fn a_function_short_form_emits_on_present_and_skips_on_absent() {
    assert_eq!(EvensOnlyTasklet::NAME, "evens_only");
    assert_eq!(
        released::<EvensOnly>("input", "evens", &["1", "2", "3", "4"]),
        ["2", "4"]
    );
}

#[derive(Default)]
struct Shout;

#[tasklet(name = "shout")]
impl Tasklet for Shout {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.to_uppercase())
    }
}

/// The reference form: every item the attribute would add, written by hand.
#[derive(Default)]
struct Bang;

impl Tasklet for Bang {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    const NAME: &'static str = "bang";
    const CLOCK: ClockKind = ClockKind::Wall;

    fn init_state(_: Option<&str>) {}

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("{input}!"))
    }
}

struct Mixed;

impl UnboundedOperator for Mixed {
    fn factory_id() -> &'static str {
        "mixed_forms"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("words")
            .into::<Shout>()
            .into::<Bang>()
            .sink_named("loud");
    }
}

#[test]
fn a_sugared_and_a_hand_written_tasklet_compose() {
    assert_eq!(released::<Mixed>("words", "loud", &["hi"]), ["HI!"]);
}

/// The author's own type is what either kind of operator names.
struct PlaceShouted;

impl BoundedOperator for PlaceShouted {
    fn factory_id() -> &'static str {
        "place_shouted"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("order")
            .into::<Shout>()
            .sink_named("placed");
    }
}

#[test]
fn the_authors_type_is_named_in_a_bounded_topology() {
    let runner = flezha_runner::Flezha::create_runner("impl_short_form").expect("a runner");
    runner
        .register_factory(PlaceShouted)
        .expect("the bounded class registers");
}
