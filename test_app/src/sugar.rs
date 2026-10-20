//! Authoring-sugar twins. Each mirrors a hand-written relay to prove the
//! macros expand to the same trait surface and interoperate with raw
//! tasklets.

use crate::*;

// Single-tasklet stateless short form, twin of `DoNothingTasklet` /
// `DoNothingRelay`. One declaration is the whole relay: it generates
// the relay `SugarDoNothing`, which `main.rs` registers, and the tasklet
// `SugarDoNothingTasklet` beside it.
#[flezha_runner::relay(factory_id = "sugar_do_nothing", output = "sugar_do_nothing_echo")]
pub(crate) fn sugar_do_nothing(input: String) -> Vec<u8> {
    input.into_bytes()
}

// Single-tasklet stateful short form on the author's own type, twin of
// `StoreInputTasklet` / `StoreInputRelay`: `SugarStoreInput` is both the
// tasklet and the relay `main.rs` registers.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
pub(crate) struct SugarStoreState {
    text_inputs: Vec<String>,
}

/// One input kept.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub(crate) struct Kept(String);

impl Change<SugarStoreState> for Kept {
    fn apply_to(&self, state: &mut SugarStoreState) {
        state.text_inputs.push(self.0.clone());
    }
}

#[derive(Default)]
pub(crate) struct SugarStoreInput;

#[flezha_runner::relay(factory_id = "sugar_store_input", output = "sugar_store_sink")]
impl Tasklet for SugarStoreInput {
    type Input = String;
    type Output = String;
    type State = SugarStoreState;
    type Change = Kept;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Kept(input.clone()));
        ctx.emit(input)
    }
}

// The short form naming its entry channel: `prices`, not the default `input`.
#[flezha_runner::relay(
    factory_id = "sugar_named_input",
    output = "sugar_named_input_sink",
    input = "prices"
)]
pub(crate) fn sugar_named_input(input: String) -> Vec<u8> {
    input.into_bytes()
}

// The steps short form: one declaration is a whole `once` steps relay, run on
// its first message on `order` and completing after one run.
#[flezha_runner::relay(
    factory_id = "sugar_steps",
    output = "sugar_steps_placed",
    input = "order",
    run = steps
)]
pub(crate) fn sugar_steps(order: String) -> Vec<u8> {
    order.into_bytes()
}

// A steps short form that loops: one pass per message on `input`, and the
// instance stays after each.
#[flezha_runner::relay(
    factory_id = "sugar_steps_per_input",
    output = "sugar_steps_per_input_out",
    run = steps(each = per_input)
)]
pub(crate) fn sugar_steps_per_input(message: String) -> Vec<u8> {
    message.into_bytes()
}

// The short form with no output: `ticks` feeds a tasklet that only keeps state.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
pub(crate) struct SugarStateOnlyState {
    seen: Vec<String>,
}

/// One tick seen.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub(crate) struct Seen(String);

impl Change<SugarStateOnlyState> for Seen {
    fn apply_to(&self, state: &mut SugarStateOnlyState) {
        state.seen.push(self.0.clone());
    }
}

#[derive(Default)]
pub(crate) struct SugarStateOnly;

#[flezha_runner::relay(factory_id = "sugar_state_only", input = "ticks")]
impl Tasklet for SugarStateOnly {
    type Input = String;
    type State = SugarStateOnlyState;
    type Change = Seen;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Seen(input));
        Ok(())
    }
}

// `pipeline!` linear-pipeline twin of `StaticCellRelay`.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
struct MacroStaticEntryState {
    seen: Vec<String>,
}

/// One input the entry Cell saw.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct EntrySeen(String);

impl Change<MacroStaticEntryState> for EntrySeen {
    fn apply_to(&self, state: &mut MacroStaticEntryState) {
        state.seen.push(self.0.clone());
    }
}

#[derive(Default)]
struct MacroStaticEntry;

#[flezha_runner::tasklet(name = "macro_static_entry_tasklet", cell_keys = ["a", "b", "c", "d"])]
impl Tasklet for MacroStaticEntry {
    type Input = String;
    type Output = String;
    type State = MacroStaticEntryState;
    type Change = EntrySeen;

    fn cells_for(input: &String) -> Vec<&'static str> {
        match input.chars().next() {
            Some('a') => vec!["a"],
            Some('b') => vec!["b"],
            Some('c') => vec!["c"],
            _ => vec!["d"],
        }
    }

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(EntrySeen(input.clone()));
        ctx.emit(input)
    }
}

#[derive(Default)]
struct MacroStaticDownstream;

#[flezha_runner::tasklet(name = "macro_static_downstream_tasklet")]
impl Tasklet for MacroStaticDownstream {
    type Input = String;
    type Output = Vec<u8>;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.into_bytes())
    }
}

flezha_runner::pipeline! {
    pub(crate) struct SugarStaticCell;
    id: "sugar_static_cell",
    pipeline: input::<String> -> MacroStaticEntry -> MacroStaticDownstream -> plugin::<Vec<u8>>("test_kafka")
}

// Mixed relay: one `#[flezha::tasklet]` tasklet wired to one tasklet written
// in the reference form, in an explicit `channels()` body.
#[derive(Default)]
struct MixedMacroEntry;

#[flezha_runner::tasklet(name = "mixed_macro_entry_tasklet")]
impl Tasklet for MixedMacroEntry {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input)
    }
}

#[derive(Default)]
struct MixedHandDownstream;
impl Tasklet for MixedHandDownstream {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    const NAME: &'static str = "mixed_hand_downstream";
    const CLOCK: ClockKind = ClockKind::Wall;
    fn init_state(_: Option<&str>) {}
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.into_bytes())
    }
}

pub(crate) struct MixedRelay;
impl Relay for MixedRelay {
    fn factory_id() -> &'static str {
        "mixed_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<MixedMacroEntry>()
            .named("hop")
            .into::<MixedHandDownstream>()
            .output_named("kafka");
    }
}

#[cfg(test)]
mod macro_twin_tests {
    use std::collections::{HashMap, VecDeque};

    use flezha_runner::testing::TaskletTestHarness;

    use super::*;
    use crate::cell::DoNothingTasklet;

    // A rolling per-key window: one change per mutation, and an input that
    // has not filled its window yet emits nothing.
    const ROLLING_WINDOW_N: usize = 3;

    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
    struct RollingWindowState {
        window: HashMap<String, VecDeque<f64>>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    enum RollingWindowChange {
        PushBack(String, f64),
        PopFront(String, usize),
    }

    impl Change<RollingWindowState> for RollingWindowChange {
        fn apply_to(&self, state: &mut RollingWindowState) {
            match self {
                RollingWindowChange::PushBack(key, value) => {
                    state
                        .window
                        .entry(key.clone())
                        .or_default()
                        .push_back(*value);
                }
                RollingWindowChange::PopFront(key, n) => {
                    if let Some(w) = state.window.get_mut(key) {
                        let n = (*n).min(w.len());
                        w.drain(..n);
                    }
                }
            }
        }
    }

    #[derive(Default)]
    struct RollingWindow;

    #[flezha_runner::tasklet(name = "rolling_window")]
    impl Tasklet for RollingWindow {
        type Input = (String, f64);
        type Output = f64;
        type State = RollingWindowState;
        type Change = RollingWindowChange;

        fn run(
            &mut self,
            input: (String, f64),
            ctx: &mut Ctx<'_, Self>,
        ) -> Result<(), TaskletError> {
            let (key, value) = input;
            ctx.change(RollingWindowChange::PushBack(key.clone(), value));
            let len = ctx.state().window.get(&key).map(|w| w.len()).unwrap_or(0);
            if len > ROLLING_WINDOW_N {
                ctx.change(RollingWindowChange::PopFront(
                    key.clone(),
                    len - ROLLING_WINDOW_N,
                ));
            }
            let Some(w) = ctx.state().window.get(&key) else {
                return Ok(());
            };
            if w.len() < ROLLING_WINDOW_N {
                return Ok(());
            }
            let mean = w.iter().sum::<f64>() / ROLLING_WINDOW_N as f64;
            ctx.emit(mean)
        }
    }

    // A written `init_state` maps the static cell key to per-cell config the
    // attribute's default (`Default::default()`) cannot supply.
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
    struct WindowedState {
        window: usize,
        closes: HashMap<String, Vec<f64>>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    enum WindowedChange {
        Push(String, f64),
        DrainFront(String, usize),
    }

    impl Change<WindowedState> for WindowedChange {
        fn apply_to(&self, state: &mut WindowedState) {
            match self {
                WindowedChange::Push(key, value) => {
                    state.closes.entry(key.clone()).or_default().push(*value);
                }
                WindowedChange::DrainFront(key, n) => {
                    if let Some(c) = state.closes.get_mut(key) {
                        let n = (*n).min(c.len());
                        c.drain(..n);
                    }
                }
            }
        }
    }

    #[derive(Default)]
    struct Windowed;

    #[flezha_runner::tasklet(name = "windowed", cell_keys = ["win_2", "win_3"])]
    impl Tasklet for Windowed {
        type Input = (String, f64);
        type Output = f64;
        type State = WindowedState;
        type Change = WindowedChange;

        fn init_state(cell_key: Option<&str>) -> WindowedState {
            WindowedState {
                window: if cell_key == Some("win_2") { 2 } else { 3 },
                closes: HashMap::new(),
            }
        }

        fn cells_for(_: &(String, f64)) -> Vec<&'static str> {
            vec!["win_2", "win_3"]
        }

        fn run(
            &mut self,
            input: (String, f64),
            ctx: &mut Ctx<'_, Self>,
        ) -> Result<(), TaskletError> {
            let (key, value) = input;
            let window = ctx.state().window;
            ctx.change(WindowedChange::Push(key.clone(), value));
            let len = ctx.state().closes.get(&key).map(|c| c.len()).unwrap_or(0);
            if len > window {
                ctx.change(WindowedChange::DrainFront(key.clone(), len - window));
            }
            let Some(c) = ctx.state().closes.get(&key) else {
                return Ok(());
            };
            if c.len() < window {
                return Ok(());
            }
            let mean = c.iter().sum::<f64>() / window as f64;
            ctx.emit(mean)
        }
    }

    // The reference relay's tasklets carry exactly these associated types.
    // A divergence is a compile error, not a silent drift.
    fn _reference_relay_conformance()
    where
        MacroStaticEntry: Tasklet<
            Input = String,
            Output = String,
            State = MacroStaticEntryState,
            Change = EntrySeen,
        >,
        MacroStaticDownstream:
            Tasklet<Input = String, Output = Vec<u8>, State = (), Change = NoChange>,
    {
    }

    #[test]
    fn sugar_do_nothing_emits_like_hand_written() {
        let inputs = vec!["alpha".to_string(), "beta".to_string()];
        let expected: Vec<Vec<u8>> = vec![b"alpha".to_vec(), b"beta".to_vec()];

        let mut sugar = TaskletTestHarness::<SugarDoNothingTasklet>::new();
        sugar.apply_inputs(inputs.clone());
        sugar.expect_emits(&expected);

        let mut hand = TaskletTestHarness::<DoNothingTasklet>::new();
        hand.apply_inputs(inputs);
        hand.expect_emits(&expected);
    }

    #[test]
    fn sugar_store_input_journals_and_emits() {
        let inputs = vec!["one".to_string(), "two".to_string()];

        let mut sugar = TaskletTestHarness::<SugarStoreInput>::new();
        sugar.apply_inputs(inputs.clone());
        assert_eq!(sugar.state().text_inputs, inputs);
        sugar.replay_equals().expect("change replay diverged");
        sugar.expect_emits(&inputs);
    }

    #[test]
    fn sugar_state_only_journals_and_emits_nothing() {
        let inputs = vec!["one".to_string(), "two".to_string()];

        let mut sugar = TaskletTestHarness::<SugarStateOnly>::new();
        sugar.apply_inputs(inputs.clone());
        assert_eq!(sugar.state().seen, inputs);
        sugar.replay_equals().expect("change replay diverged");
        assert!(
            sugar.emitted_meta().is_empty(),
            "a body with no output emits nothing"
        );
    }

    #[test]
    fn macro_static_entry_cells_and_journals() {
        // `cells_for` routes by first char; the tasklet journals every input.
        assert_eq!(MacroStaticEntry::cells_for(&"apple".to_string()), vec!["a"]);
        assert_eq!(MacroStaticEntry::cells_for(&"zebra".to_string()), vec!["d"]);
        assert_eq!(MacroStaticEntry::cell_keys(), vec!["a", "b", "c", "d"]);

        let inputs = vec!["apple".to_string(), "avocado".to_string()];
        let mut h = TaskletTestHarness::<MacroStaticEntry>::new();
        h.apply_inputs(inputs.clone());
        assert_eq!(h.state().seen, inputs);
        h.replay_equals().expect("change replay diverged");
        h.expect_emits(&inputs);
    }

    #[test]
    fn mixed_macro_and_hand_tasklets_run() {
        // The macro entry tasklet passes through; the hand tasklet turns it to bytes.
        let mut entry = TaskletTestHarness::<MixedMacroEntry>::new();
        entry.apply_inputs(vec!["hi".to_string()]);
        entry.expect_emits(&["hi".to_string()]);

        let mut down = TaskletTestHarness::<MixedHandDownstream>::new();
        down.apply_inputs(vec!["hi".to_string()]);
        down.expect_emits(&[b"hi".to_vec()]);
    }

    #[test]
    fn sugar_relays_register() {
        // Nothing is harvested: every short form and `pipeline!` type is a
        // named value the author hands to `register_factory`, exactly as a
        // hand-written relay is.
        let runner = Flezha::create_runner("sugar_test").unwrap();
        runner
            .register_factory(SugarDoNothing)
            .unwrap()
            .register_factory(SugarStoreInput)
            .unwrap()
            .register_factory(SugarNamedInput)
            .unwrap()
            .register_factory(SugarSteps)
            .unwrap()
            .register_factory(SugarStepsPerInput)
            .unwrap()
            .register_factory(SugarStateOnly)
            .unwrap()
            .register_factory(SugarStaticCell)
            .unwrap()
            .register_factory(MixedRelay)
            .unwrap();

        let ids = flezha_runner::relay::registry::class_factory_ids();
        for id in [
            "sugar_do_nothing",
            "sugar_store_input",
            "sugar_named_input",
            "sugar_state_only",
            "sugar_static_cell",
            "mixed_relay",
        ] {
            assert!(ids.contains(&id), "registered relay `{id}` missing");
        }
        let class = flezha_runner::relay::registry::get_class;
        assert!(
            class("sugar_steps").is_some_and(|c| c.steps.is_some()),
            "the steps short form registers as a steps class"
        );
        let per_input = class("sugar_steps_per_input").expect("the looping short form registers");
        assert_eq!(
            per_input.steps.as_ref().expect("a steps class").each,
            Each::PerInput,
            "the short form carries the loop mode it wrote"
        );
    }

    #[test]
    fn rolling_window_journals_one_change_per_mutation_and_replays() {
        let inputs = vec![
            ("a".to_string(), 1.0),
            ("a".to_string(), 2.0),
            ("a".to_string(), 3.0),
            ("a".to_string(), 4.0),
            ("b".to_string(), 10.0),
            ("b".to_string(), 20.0),
            ("b".to_string(), 30.0),
        ];

        let mut h = TaskletTestHarness::<RollingWindow>::new();
        h.apply_inputs(inputs);
        h.replay_equals().expect("rolling window replay diverged");
        assert_eq!(h.state().window["a"], VecDeque::from(vec![2.0, 3.0, 4.0]));
        assert_eq!(h.recorded_changes().len(), 8, "seven pushes and one pop");
        h.expect_emits(&[2.0_f64, 3.0, 20.0]);
    }

    #[test]
    fn tasklet_init_maps_cell_key_to_state() {
        // A written `init_state` receives the static cell key, so each cell
        // starts from key-derived config the attribute's default can't give.
        assert_eq!(Windowed::init_state(Some("win_2")).window, 2);
        assert_eq!(Windowed::init_state(Some("win_3")).window, 3);
        // Cold-start / harness default (key absent) falls through to the else arm.
        assert_eq!(Windowed::init_state(None).window, 3);

        // Behaviour differs per cell: window 2 emits one bar earlier than
        // window 3 for the same input stream, and replay stays deterministic.
        let inputs = vec![
            ("a".to_string(), 2.0),
            ("a".to_string(), 4.0),
            ("a".to_string(), 6.0),
        ];

        let mut w2 =
            TaskletTestHarness::<Windowed>::with_initial_state(Windowed::init_state(Some("win_2")));
        w2.apply_inputs(inputs.clone());
        w2.replay_equals().expect("windowed(win_2) replay diverged");
        w2.expect_emits(&[3.0_f64, 5.0]); // means of [2,4] then [4,6]

        let mut w3 =
            TaskletTestHarness::<Windowed>::with_initial_state(Windowed::init_state(Some("win_3")));
        w3.apply_inputs(inputs);
        w3.replay_equals().expect("windowed(win_3) replay diverged");
        w3.expect_emits(&[4.0_f64]); // single mean of [2,4,6]
    }
}
