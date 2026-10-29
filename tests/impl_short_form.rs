//! The short-form attributes on an `impl Tasklet` block, a function short
//! form's optional output, and a `#[flezha::tasklet]` block composing with a
//! hand-written one, each run locally through the graph it declares.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use flezha_runner::{
    tasklet, Change, ChannelBuilder, ClockKind, Ctx, Each, LocalRelay, NoChange, OnFailure, Relay,
    RelaySettings, Run, Tasklet, TaskletError,
};
use serde::{Deserialize, Serialize};

/// Every value released on `output` while `feed` runs through `R`'s graph.
fn released<R: Relay>(entry: &str, sink: &str, feed: &[&str]) -> Vec<String> {
    let out = Arc::new(Mutex::new(Vec::new()));
    let into = Arc::clone(&out);
    let run = LocalRelay::start::<R>().expect("the run starts");
    run.on_output(sink, move |o| {
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

/// A stateful one-tasklet relay: the author's type is both.
#[derive(Default)]
struct StoreInput;

#[flezha_runner::relay(factory_id = "store_input", output = "stored", max_cells = 10)]
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
    assert_eq!(<StoreInput as Relay>::factory_id(), "store_input");
    assert_eq!(StoreInput::NAME, "store_input");
    assert_eq!(StoreInput::max_cells(), 10);
    assert_eq!(
        released::<StoreInput>("input", "stored", &["a", "b"]),
        ["a", "a,b"]
    );
}

/// No `output`: the entry channel feeds the tasklet and nothing else is declared.
#[derive(Default)]
struct Ticks;

#[flezha_runner::relay(factory_id = "ticks_counter", input = "ticks")]
impl Tasklet for Ticks {
    type Input = String;

    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

#[test]
fn a_short_form_with_no_output_wires_only_the_entry_channel() {
    let run = LocalRelay::start::<Ticks>().expect("the run starts");
    run.feed("ticks", "1")
        .expect("`ticks` is the entry channel");
    assert!(run.feed("input", "1").is_err(), "no default entry channel");
    run.run_until_idle(Duration::from_secs(20))
        .expect("the graph settles");
}

#[derive(Default)]
struct PlaceIt;

#[flezha_runner::relay(
    factory_id = "place_it",
    output = "placed",
    input = "order",
    run = steps(each = per_input),
    durable = local(sleep_after = false, keep_finished = 24)
)]
impl Tasklet for PlaceIt {
    type Input = String;
    type Output = String;

    fn run(&mut self, order: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(order)
    }
}

#[test]
fn a_steps_short_form_on_the_impl_block_declares_its_run_and_durability() {
    assert_eq!(<PlaceIt as Relay>::factory_id(), "place_it");
    assert_eq!(PlaceIt::settings().run, Run::steps(Each::PerInput));
    assert_eq!(
        flezha_runner::relay::settings::durable_json(&PlaceIt::settings().durable),
        serde_json::json!({"local": {"sleep_after": false, "keep_finished": 24}})
    );
    assert_eq!(PlaceIt::NAME, "place_it");
    assert_eq!(PlaceIt::CLOCK, ClockKind::Wall);
}

#[flezha_runner::relay(factory_id = "evens_only", output = "evens")]
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

impl Relay for Mixed {
    fn factory_id() -> &'static str {
        "mixed_forms"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("words")
            .into::<Shout>()
            .into::<Bang>()
            .output_named("loud");
    }
}

#[test]
fn a_sugared_and_a_hand_written_tasklet_compose() {
    assert_eq!(released::<Mixed>("words", "loud", &["hi"]), ["HI!"]);
}

/// The author's own type is what either kind of relay names.
struct PlaceShouted;

impl Relay for PlaceShouted {
    fn factory_id() -> &'static str {
        "place_shouted"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("order")
            .into::<Shout>()
            .output_named("placed");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            ..RelaySettings::default()
        }
    }
}

#[test]
fn the_authors_type_is_named_in_a_steps_topology() {
    let runner = flezha_runner::Flezha::create_runner("impl_short_form").expect("a runner");
    runner
        .register_factory(PlaceShouted)
        .expect("the steps class registers");
}
