//! The Rust SDK's local mode: a user's own operative type, its declared
//! two-procedure graph, and no controller anywhere.
//!
//! The graph and its expected sink output are read from
//! `test/fixtures/local_mode_word_count.txt`, which the Java and Elixir
//! local-mode suites read too — so the Elixir SDK's own core and `flz_core`
//! are compared against one recorded expectation instead of by inspection.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flezha_runner::local::SinkOutput;
use flezha_runner::procedure::{ProcedureCtx, ProcedureError, SimpleProcedure};
use flezha_runner::state::StateHandle;
use flezha_runner::{ChannelBuilder, Flezha, LocalOperative, StreamOperative};
use serde::{Deserialize, Serialize};

/// One line into its words: the hop to `Tally` is the runtime's job.
/// Declared at the simplified tier, which is not what
/// `examples/local_word_count.rs` shows — the two spellings of one graph are
/// both meant to run locally.
#[derive(Default)]
struct Split;

impl SimpleProcedure for Split {
    type Input = String;
    type Output = String;
    type State = ();

    fn name() -> &'static str {
        "split"
    }

    fn init_state(_entity_key: Option<&str>) {}

    fn run(
        &mut self,
        line: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        for word in line.split_whitespace() {
            ctx.emit(word.to_lowercase())?;
        }
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize, flezha_runner::State)]
struct Count {
    seen: u64,
}

/// One Entity per word, minted on first sight of it.
#[derive(Default)]
struct Tally;

impl SimpleProcedure for Tally {
    type Input = String;
    type Output = String;
    type State = Count;

    fn name() -> &'static str {
        "tally"
    }

    fn init_state(_entity_key: Option<&str>) -> Count {
        Count::default()
    }

    fn run(
        &mut self,
        word: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.set_seen(state.seen + 1);
        ctx.emit(format!("{word} {}", state.seen))
    }
}

struct WordCount;

impl StreamOperative for WordCount {
    fn factory_id() -> &'static str {
        "local_word_count_rust"
    }

    fn channels(c: &ChannelBuilder) {
        c.input::<String>("lines")
            .into::<Split>()
            .key_by(|word: &String| word.clone())
            .into::<Tally>()
            .keyed()
            .sink_named("totals");
    }
}

/// The scenario every core runs: the lines to feed, and the sink values to
/// release as a multiset.
fn scenario() -> (Vec<String>, BTreeMap<String, usize>) {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../test/fixtures/local_mode_word_count.txt"
    );
    let text = std::fs::read_to_string(path).expect("the shared local-mode scenario");
    let mut lines = Vec::new();
    let mut totals = BTreeMap::new();
    for row in text.lines() {
        if let Some(line) = row.strip_prefix("lines: ") {
            lines.push(line.to_string());
        } else if let Some(total) = row.strip_prefix("totals: ") {
            *totals.entry(total.to_string()).or_default() += 1;
        }
    }
    (lines, totals)
}

#[test]
fn a_declared_graph_runs_locally_and_releases_the_recorded_output() {
    let (lines, expected) = scenario();
    let released: Arc<Mutex<Vec<SinkOutput>>> = Arc::new(Mutex::new(Vec::new()));
    let into = Arc::clone(&released);

    let run = LocalOperative::start::<WordCount>().expect("the run starts");
    // `sink_named("totals")` is the declaration's ONLY naming call, and this
    // registers under that same string: local mode addresses a sink by the
    // channel name, and a sink names the channel, so `sdk-local-mode`'s "the
    // sink's declared name" and the implementation are one answer.
    run.on_sink("totals", move |output| {
        into.lock().unwrap_or_else(|p| p.into_inner()).push(output);
    })
    .expect("`totals` is declared");
    for line in &lines {
        run.feed("lines", line).expect("`lines` accepts");
    }
    run.run_until_idle(Duration::from_secs(20))
        .expect("the graph settles");

    let seen = released.lock().unwrap_or_else(|p| p.into_inner());
    let mut counted: BTreeMap<String, usize> = BTreeMap::new();
    for output in seen.iter() {
        *counted
            .entry(String::from_utf8_lossy(&output.payload).to_string())
            .or_default() += 1;
    }
    assert_eq!(
        counted, expected,
        "both procedures ran, the runtime performed the hop, and one Entity per word counted"
    );

    let first = seen.first().expect("a released value");
    assert_eq!(first.channel, "totals");
    assert!(
        first.metadata.contains_key("flz.entity")
            && first.metadata.contains_key("flz.seq")
            && first.metadata.contains_key("flz.key"),
        "a keyed sink carries the identity stamps a deployed run stamps: {:?}",
        first.metadata
    );

    // The same class, with no local-mode member on it, is what a runner
    // declares against a controller.
    Flezha::create_runner("default")
        .expect("a runner is created")
        .define_stream::<WordCount>()
        .expect("the identical declaration registers for deployment");
}

#[test]
fn a_channel_the_class_does_not_declare_is_refused_by_name() {
    let run = LocalOperative::start::<WordCount>().expect("the run starts");
    let refusal = run
        .feed("nope", "1")
        .expect_err("an undeclared channel is refused");
    assert!(
        refusal.to_string().contains("nope") && refusal.to_string().contains("lines"),
        "the refusal names the channel and what is declared: {refusal}"
    );
}
