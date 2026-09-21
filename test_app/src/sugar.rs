//! Authoring-sugar twins. Each mirrors a hand-written operative to prove the
//! macros expand to the same trait surface and interoperate with raw
//! tasklets.

use crate::*;

// ---------------------------------------------------------------------------
// Authoring-sugar twins (`simplify-sdk-authoring`). Each mirrors a
// hand-written operative above to prove the macros expand to the same trait
// surface and interoperate with raw tasklets. See
// `docs/sdk/authoring_contract.md`.
// ---------------------------------------------------------------------------

// Single-tasklet stateless short form, twin of `DoNothingTasklet` /
// `DoNothingOperative`. One declaration is the whole operative: it generates
// the operative `SugarDoNothing`, which `main.rs` registers, and the tasklet
// `SugarDoNothingTasklet` beside it.
#[flezha_runner::stream_operative(id = "sugar_do_nothing", sink = "sugar_do_nothing_echo")]
pub(crate) fn sugar_do_nothing(input: String) -> Vec<u8> {
    input.into_bytes()
}

// Single-tasklet stateful sugar with `#[derive(flezha::State)]`, twin of
// `StoreInputTasklet` / `StoreInputOperative`.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
pub(crate) struct SugarStoreState {
    text_inputs: Vec<String>,
}

#[flezha_runner::stream_operative(id = "sugar_store_input", sink = "sugar_store_sink")]
pub(crate) fn sugar_store_input(input: String, state: &mut SugarStoreState) -> String {
    state.push_text_inputs(input.clone());
    input
}

// The short form naming its entry channel: `prices`, not the default `input`.
#[flezha_runner::stream_operative(
    id = "sugar_named_input",
    sink = "sugar_named_input_sink",
    input = "prices"
)]
pub(crate) fn sugar_named_input(input: String) -> Vec<u8> {
    input.into_bytes()
}

// The workflow short form: one declaration is a whole `once` workflow, seeded
// on `order` from its start values and completing after one run.
#[flezha_runner::workflow_operative(
    id = "sugar_workflow",
    sink = "sugar_workflow_placed",
    input = "order"
)]
pub(crate) fn sugar_workflow(order: String) -> Vec<u8> {
    order.into_bytes()
}

// The short form with no sink: `ticks` feeds a tasklet that only keeps state.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
pub(crate) struct SugarStateOnlyState {
    seen: Vec<String>,
}

#[flezha_runner::stream_operative(id = "sugar_state_only", input = "ticks")]
pub(crate) fn sugar_state_only(input: String, state: &mut SugarStateOnlyState) {
    state.push_seen(input);
}

// `operative!` linear-pipeline twin of `StaticCellOperative`.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
struct MacroStaticEntryState {
    seen: Vec<String>,
}

fn macro_static_part(input: &str) -> Vec<&'static str> {
    match input.chars().next() {
        Some('a') => vec!["a"],
        Some('b') => vec!["b"],
        Some('c') => vec!["c"],
        _ => vec!["d"],
    }
}

#[flezha_runner::tasklet(
    id = "macro_static_entry_tasklet",
    keys = ["a", "b", "c", "d"],
    cells_for = macro_static_part
)]
fn macro_static_entry(input: String, state: &mut MacroStaticEntryState) -> String {
    state.push_seen(input.clone());
    input
}

#[flezha_runner::tasklet(id = "macro_static_downstream_tasklet")]
fn macro_static_downstream(input: String) -> Vec<u8> {
    input.into_bytes()
}

flezha_runner::operative! {
    pub(crate) struct SugarStaticCell;
    id: "sugar_static_cell",
    pipeline: input::<String> -> MacroStaticEntry -> MacroStaticDownstream -> plugin::<Vec<u8>>("test_kafka")
}

// Mixed operative: one `#[flezha::tasklet]` tasklet wired to one hand-written
// `impl Tasklet` tasklet in an explicit `channels()` body.
#[flezha_runner::tasklet(id = "mixed_macro_entry_tasklet")]
fn mixed_macro_entry(input: String) -> String {
    input
}

#[derive(Default)]
struct MixedHandDownstream;
impl Tasklet for MixedHandDownstream {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "mixed_hand_downstream"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut TaskletCtx<'_>,
    ) -> Result<(), TaskletError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

pub(crate) struct MixedOperative;
impl StreamOperative for MixedOperative {
    fn factory_id() -> &'static str {
        "mixed_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<MixedMacroEntry>()
            .named("hop")
            .into::<MixedHandDownstream>()
            .sink_named("kafka");
    }
}

#[cfg(test)]
mod macro_twin_tests {
    use std::collections::{HashMap, VecDeque};

    use flezha_runner::testing::TaskletTestHarness;

    use super::*;
    use crate::cell::DoNothingTasklet;

    // -----------------------------------------------------------------------
    // Canonical keyed-collection reference tasklet (`expressive-state-journaling`,
    // authoring-contract §3 / §6): a rolling per-key window. The sugared form
    // uses `#[journal(map_deque)]` + an `Option<Output>` warmup gate;
    // `RollingWindowTwin` is the equivalent hand-written `Tasklet`/`Change`. The
    // tests below assert the two are behaviourally identical and that the
    // sugared form survives `replay_equals`.
    // -----------------------------------------------------------------------

    const ROLLING_WINDOW_N: usize = 3;

    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
    struct RollingWindowState {
        #[journal(map_deque)]
        window: HashMap<String, VecDeque<f64>>,
    }

    #[flezha_runner::tasklet(id = "rolling_window")]
    fn rolling_window(input: (String, f64), state: &mut RollingWindowState) -> Option<f64> {
        let (key, value) = input;
        state.push_back_window(key.clone(), value);
        let len = state.window.get(&key).map(|w| w.len()).unwrap_or(0);
        if len > ROLLING_WINDOW_N {
            state.pop_front_window(key.clone(), len - ROLLING_WINDOW_N);
        }
        let w = state.window.get(&key)?;
        if w.len() < ROLLING_WINDOW_N {
            return None;
        }
        Some(w.iter().sum::<f64>() / ROLLING_WINDOW_N as f64)
    }

    // Hand-written twin: the exact `Tasklet` / `Change` impl the sugar expands to
    // (variant names differ; behaviour does not).
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
    struct RollingWindowTwinState {
        window: HashMap<String, VecDeque<f64>>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    enum RollingWindowTwinChange {
        PushBack(String, f64),
        PopFront(String, usize),
    }
    impl Change for RollingWindowTwinChange {}

    #[derive(Default)]
    struct RollingWindowTwin;
    impl Tasklet for RollingWindowTwin {
        type Input = (String, f64);
        type Output = f64;
        type State = RollingWindowTwinState;
        type Change = RollingWindowTwinChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "rolling_window_twin"
        }
        fn init_state(
            _: Option<&str>,
            _: Option<RestoreInitPayload<'_>>,
        ) -> RollingWindowTwinState {
            RollingWindowTwinState::default()
        }
        fn apply_change(state: &mut RollingWindowTwinState, change: &RollingWindowTwinChange) {
            match change {
                RollingWindowTwinChange::PushBack(k, v) => {
                    state.window.entry(k.clone()).or_default().push_back(*v);
                }
                RollingWindowTwinChange::PopFront(k, n) => {
                    let e = state.window.entry(k.clone()).or_default();
                    for _ in 0..(*n).min(e.len()) {
                        e.pop_front();
                    }
                }
            }
        }
        fn run(
            &mut self,
            input: (String, f64),
            mut state: StateHandle<'_, Self>,
            ctx: &mut TaskletCtx<'_>,
        ) -> Result<(), TaskletError> {
            let (key, value) = input;
            state.apply(RollingWindowTwinChange::PushBack(key.clone(), value));
            let len = state.window.get(&key).map(|w| w.len()).unwrap_or(0);
            if len > ROLLING_WINDOW_N {
                state.apply(RollingWindowTwinChange::PopFront(
                    key.clone(),
                    len - ROLLING_WINDOW_N,
                ));
            }
            let Some(w) = state.window.get(&key) else {
                return Ok(());
            };
            if w.len() < ROLLING_WINDOW_N {
                return Ok(());
            }
            let mean = w.iter().sum::<f64>() / ROLLING_WINDOW_N as f64;
            ctx.emit::<f64>(mean)
        }
    }

    // Per-mode coverage for the remaining journaling modes (`map_set`,
    // `map_vec`), composed with a flat field on one `Change` enum.
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
    struct KeyedCollectionsState {
        #[journal(map_set)]
        last: HashMap<String, f64>,
        #[journal(map_vec)]
        closes: HashMap<String, Vec<f64>>,
        ticks: u64,
    }

    #[flezha_runner::tasklet(id = "keyed_collections")]
    fn keyed_collections(input: (String, f64), state: &mut KeyedCollectionsState) {
        let (key, value) = input;
        state.set_last(key.clone(), value);
        state.push_closes(key.clone(), value);
        // Keep at most three closes per key; drain the overflow off the front.
        let len = state.closes.get(&key).map(|c| c.len()).unwrap_or(0);
        if len > 3 {
            state.drain_closes_front(key.clone(), len - 3);
        }
        state.set_ticks(state.ticks + 1);
    }

    // `init = fn` maps the static cell key to per-cell config the
    // macro's default (`Default::default()`) cannot supply. Mirrors the market
    // `sma`/`ohlc` tasklets whose window / bucket come from the cell key.
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
    struct WindowedState {
        window: usize,
        #[journal(map_vec)]
        closes: HashMap<String, Vec<f64>>,
    }

    fn windowed_init(cell_key: Option<&str>) -> WindowedState {
        WindowedState {
            window: if cell_key == Some("win_2") { 2 } else { 3 },
            closes: HashMap::new(),
        }
    }

    fn windowed_part(_: &(String, f64)) -> Vec<&'static str> {
        vec!["win_2", "win_3"]
    }

    #[flezha_runner::tasklet(
        id = "windowed",
        init = windowed_init,
        keys = ["win_2", "win_3"],
        cells_for = windowed_part
    )]
    fn windowed(input: (String, f64), state: &mut WindowedState) -> Option<f64> {
        let (key, value) = input;
        let window = state.window;
        state.push_closes(key.clone(), value);
        let len = state.closes.get(&key).map(|c| c.len()).unwrap_or(0);
        if len > window {
            state.drain_closes_front(key.clone(), len - window);
        }
        let c = state.closes.get(&key)?;
        if c.len() < window {
            return None;
        }
        Some(c.iter().sum::<f64>() / window as f64)
    }

    // Cross-SDK conformance (`docs/sdk/authoring_contract.md` §6): the reference
    // operative's tasklets expand to this exact associated-type mapping. A divergence
    // is a compile error, not a silent drift. The Java SugarTwinTest asserts the
    // same mapping at runtime.
    fn _reference_operative_conformance()
    where
        MacroStaticEntry: Tasklet<
            Input = String,
            Output = String,
            State = MacroStaticEntryState,
            Clock = WallClock,
            Config = (),
        >,
        MacroStaticDownstream: Tasklet<
            Input = String,
            Output = Vec<u8>,
            State = (),
            Change = NoChange,
            Clock = WallClock,
            Config = (),
        >,
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

        let mut sugar = TaskletTestHarness::<SugarStoreInputTasklet>::new();
        sugar.apply_inputs(inputs.clone());
        assert_eq!(sugar.state().text_inputs, inputs);
        sugar
            .replay_equals()
            .expect("derived change replay diverged");
        sugar.expect_emits(&inputs);
    }

    #[test]
    fn sugar_state_only_journals_and_emits_nothing() {
        let inputs = vec!["one".to_string(), "two".to_string()];

        let mut sugar = TaskletTestHarness::<SugarStateOnlyTasklet>::new();
        sugar.apply_inputs(inputs.clone());
        assert_eq!(sugar.state().seen, inputs);
        sugar
            .replay_equals()
            .expect("derived change replay diverged");
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
        h.replay_equals().expect("derived change replay diverged");
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
    fn sugar_operatives_register() {
        // Nothing is harvested: every short form and `operative!` type is a
        // named value the author hands to `register_factory`, exactly as a
        // hand-written operative is.
        let runner = Flezha::create_runner("sugar_test").unwrap();
        runner
            .register_factory(SugarDoNothing)
            .unwrap()
            .register_factory(SugarStoreInput)
            .unwrap()
            .register_factory(SugarNamedInput)
            .unwrap()
            .register_factory(SugarWorkflow)
            .unwrap()
            .register_factory(SugarStateOnly)
            .unwrap()
            .register_factory(SugarStaticCell)
            .unwrap()
            .register_factory(MixedOperative)
            .unwrap();

        let ids = flezha_runner::operative::registry::class_factory_ids();
        for id in [
            "sugar_do_nothing",
            "sugar_store_input",
            "sugar_named_input",
            "sugar_state_only",
            "sugar_static_cell",
            "mixed_operative",
        ] {
            assert!(ids.contains(&id), "registered operative `{id}` missing");
        }
        assert!(
            flezha_runner::workflow::registry::get_class("sugar_workflow").is_some(),
            "the workflow short form registers in the workflow registry"
        );
    }

    #[test]
    fn rolling_window_matches_hand_written_twin() {
        let inputs = vec![
            ("a".to_string(), 1.0),
            ("a".to_string(), 2.0),
            ("a".to_string(), 3.0),
            ("a".to_string(), 4.0),
            ("b".to_string(), 10.0),
            ("b".to_string(), 20.0),
            ("b".to_string(), 30.0),
        ];

        let mut sugar = TaskletTestHarness::<RollingWindow>::new();
        sugar.apply_inputs(inputs.clone());

        let mut twin = TaskletTestHarness::<RollingWindowTwin>::new();
        twin.apply_inputs(inputs);

        // The generated journaling replays deterministically (4.3).
        sugar.replay_equals().expect("sugared replay diverged");

        // Behaviourally identical to the hand-written twin: same state (4.2).
        assert_eq!(sugar.state().window, twin.state().window);

        // ...and the same emits (warmup gate via `Option<Output>`).
        let expected = [2.0_f64, 3.0, 20.0];
        sugar.expect_emits(&expected);
        twin.expect_emits(&expected);
    }

    #[test]
    fn keyed_collections_modes_journal_and_replay() {
        let inputs = vec![
            ("x".to_string(), 1.0),
            ("x".to_string(), 2.0),
            ("x".to_string(), 3.0),
            ("x".to_string(), 4.0),
            ("y".to_string(), 9.0),
        ];
        let mut h = TaskletTestHarness::<KeyedCollections>::new();
        h.apply_inputs(inputs);

        // map_set keeps the latest per key.
        assert_eq!(h.state().last.get("x"), Some(&4.0));
        assert_eq!(h.state().last.get("y"), Some(&9.0));
        // map_vec push + drain_front keeps the last three per key.
        assert_eq!(h.state().closes.get("x"), Some(&vec![2.0, 3.0, 4.0]));
        assert_eq!(h.state().closes.get("y"), Some(&vec![9.0]));
        // The flat field composes into the same `Change` enum.
        assert_eq!(h.state().ticks, 5);

        h.replay_equals()
            .expect("keyed-collections replay diverged");
    }

    #[test]
    fn tasklet_init_maps_cell_key_to_state() {
        // The `init = fn` hook receives the static cell key, so each
        // cell starts from key-derived config the macro default can't give.
        assert_eq!(Windowed::init_state(Some("win_2"), None).window, 2);
        assert_eq!(Windowed::init_state(Some("win_3"), None).window, 3);
        // Cold-start / harness default (key absent) falls through to the else arm.
        assert_eq!(Windowed::init_state(None, None).window, 3);

        // Behaviour differs per cell: window 2 emits one bar earlier than
        // window 3 for the same input stream, and replay stays deterministic.
        let inputs = vec![
            ("a".to_string(), 2.0),
            ("a".to_string(), 4.0),
            ("a".to_string(), 6.0),
        ];

        let mut w2 =
            TaskletTestHarness::<Windowed>::with_initial_state(windowed_init(Some("win_2")));
        w2.apply_inputs(inputs.clone());
        w2.replay_equals().expect("windowed(win_2) replay diverged");
        w2.expect_emits(&[3.0_f64, 5.0]); // means of [2,4] then [4,6]

        let mut w3 =
            TaskletTestHarness::<Windowed>::with_initial_state(windowed_init(Some("win_3")));
        w3.apply_inputs(inputs);
        w3.replay_equals().expect("windowed(win_3) replay diverged");
        w3.expect_emits(&[4.0_f64]); // single mean of [2,4,6]
    }
}
