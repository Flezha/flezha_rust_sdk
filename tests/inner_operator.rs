//! An operator inside an operator, run locally: a flow class whose keyed edge
//! ends in a steps class runs one stepped scope of it per key, each with its
//! own state, and the scope's exit output continues on the outer graph.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flezha_runner::local::SinkOutput;
use flezha_runner::tasklet::{Ctx, Tasklet, TaskletError};
use flezha_runner::{
    ChannelBuilder, Durable, Each, Embedding, ErrorModel, LocalOperator, OnFailure, Operator,
    OperatorSettings, Run,
};
use serde::{Deserialize, Serialize};

#[derive(Default, Serialize, Deserialize)]
struct Turns {
    turns: u64,
}

#[derive(Serialize, Deserialize)]
struct OneMore;

impl flezha_runner::Change<Turns> for OneMore {
    fn apply_to(&self, state: &mut Turns) {
        state.turns += 1;
    }
}

/// One turn of a session: `key:text` answered as `key#turns:text`.
#[derive(Default)]
struct Turn;

#[flezha_runner::tasklet(name = "turn")]
impl Tasklet for Turn {
    type Input = String;
    type Output = String;
    type State = Turns;
    type Change = OneMore;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let (key, text) = input.split_once(':').unwrap_or((&input, ""));
        ctx.change(OneMore);
        ctx.emit(format!("{key}#{}:{text}", ctx.state().turns))
    }
}

#[derive(Default)]
struct Reply;

#[flezha_runner::tasklet(name = "reply")]
impl Tasklet for Reply {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input)
    }
}

/// Trims a line.
#[derive(Default)]
struct Trim;

#[flezha_runner::tasklet(name = "trim")]
impl Tasklet for Trim {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.trim().to_string())
    }
}

/// Upper-cases a line.
#[derive(Default)]
struct Upper;

#[flezha_runner::tasklet(name = "upper")]
impl Tasklet for Upper {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(input.to_uppercase())
    }
}

/// A flow class of two tasklets, inlined wherever it is embedded.
struct Cleanup;

impl Operator for Cleanup {
    fn factory_id() -> &'static str {
        "inner_cleanup_rust"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("raw").into::<Trim>().into::<Upper>();
    }
}

struct Inlining;

impl Operator for Inlining {
    fn factory_id() -> &'static str {
        "inner_inlining_rust"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("requests")
            .into_operator::<Cleanup, String>()
            .into::<Reply>()
            .sink_named("out");
    }
}

/// A flow class with two entry channels.
struct TwoEntries;

impl Operator for TwoEntries {
    fn factory_id() -> &'static str {
        "inner_two_entries_rust"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("a").into::<Trim>();
        c.input::<String>("b").into::<Upper>();
    }
}

struct AmbiguousEntry;

impl Operator for AmbiguousEntry {
    fn factory_id() -> &'static str {
        "inner_ambiguous_entry_rust"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into_operator::<TwoEntries, String>()
            .into::<Reply>();
    }
}

struct Session;

impl Operator for Session {
    fn factory_id() -> &'static str {
        "inner_session_rust"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("turns").into::<Turn>();
    }

    fn settings() -> OperatorSettings {
        OperatorSettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            durable: Durable::Memory,
            ..OperatorSettings::default()
        }
    }
}

struct Gateway;

impl Operator for Gateway {
    fn factory_id() -> &'static str {
        "inner_gateway_rust"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("requests")
            .key_by(|line: &String| line.split(':').next().unwrap_or_default().to_string())
            .into_operator_with::<Session, String>(Embedding {
                max_cells: Some(2),
                ..Embedding::default()
            })
            .into::<Reply>()
            .sink_named("out");
    }
}

struct StepsOuter;

impl Operator for StepsOuter {
    fn factory_id() -> &'static str {
        "inner_steps_outer_rust"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into_operator::<Session, String>()
            .into::<Reply>();
    }

    fn settings() -> OperatorSettings {
        OperatorSettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            ..OperatorSettings::default()
        }
    }
}

/// The first `count` outputs on `out` after feeding `lines` to `O`.
fn run<O: Operator>(lines: &[&str], count: usize) -> Vec<String> {
    let run = LocalOperator::start::<O>().expect("the operator starts");
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let sink = Arc::clone(&seen);
    run.on_sink("out", move |output: SinkOutput| {
        sink.lock()
            .unwrap()
            .push(String::from_utf8(output.payload).expect("utf-8"));
    })
    .expect("the sink is declared");
    for line in lines {
        run.feed("requests", line).expect("fed");
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    while seen.lock().unwrap().len() < count {
        assert!(Instant::now() < deadline, "{:?}", seen.lock().unwrap());
        std::thread::yield_now();
    }
    run.run_until_idle(Duration::from_secs(5)).expect("idle");
    let mut outputs = seen.lock().unwrap().clone();
    outputs.sort();
    outputs
}

#[test]
fn every_key_runs_its_own_scope_and_the_exit_continues_the_outer_graph() {
    assert_eq!(
        run::<Gateway>(&["alice:hi", "bob:yo", "alice:again"], 3),
        vec!["alice#1:hi", "alice#2:again", "bob#1:yo"]
    );
}

#[test]
fn a_steps_class_embedding_a_steps_class_is_refused_naming_both() {
    let refused = flezha_runner::Flezha::create_runner("inner-refusal")
        .expect("a runner")
        .register_factory(StepsOuter)
        .err()
        .expect("refused");
    assert_eq!(
        refused.to_error().code,
        "topology.nested_steps",
        "{refused}"
    );
}

#[test]
fn a_flow_class_is_inlined_under_prefixed_names() {
    assert_eq!(run::<Inlining>(&["  hi  "], 1), vec!["HI"]);
    let class = flz_core::operator::registry::get_class("inner_inlining_rust").expect("registered");
    let mut names: Vec<&str> = class.tasklets.iter().map(|t| &*t.name).collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "inner_cleanup_rust.trim",
            "inner_cleanup_rust.upper",
            "reply"
        ]
    );
    assert!(class.tasklets.iter().all(|t| t.scope.is_none()));
}

#[test]
fn an_inlined_class_with_two_entries_and_no_input_is_refused_naming_both() {
    let refused = flezha_runner::Flezha::create_runner("inner-two-entries")
        .expect("a runner")
        .register_factory(AmbiguousEntry)
        .err()
        .expect("refused");
    let error = refused.to_error();
    assert_eq!(error.code, "topology.embedded_entry_ambiguous", "{refused}");
    assert!(refused.to_string().contains("a, b"), "{refused}");
}
