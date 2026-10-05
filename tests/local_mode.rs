//! The Rust SDK's local mode: a user's own relay type, its declared
//! two-tasklet graph, and no controller anywhere.
//!
//! The graph and its expected sink output are read from
//! `test/fixtures/local_mode_word_count.txt`, which the Java and Elixir
//! local-mode suites read too — so the Elixir SDK's own core and `flz_core`
//! are compared against one recorded expectation instead of by inspection.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flezha_runner::local::SinkOutput;
use flezha_runner::state::Change;
use flezha_runner::tasklet::{Ctx, Tasklet, TaskletError};
use flezha_runner::{ChannelBuilder, Flezha, LocalRelay, Relay};
use serde::{Deserialize, Serialize};

mod common;
use common::contract::{contract_file, contract_root, MANIFEST};

/// One line into its words: the hop to `Tally` is the runtime's job.
#[derive(Default)]
struct Split;

#[flezha_runner::tasklet(name = "split")]
impl Tasklet for Split {
    type Input = String;
    type Output = String;

    fn run(&mut self, line: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        for word in line.split_whitespace() {
            ctx.emit(word.to_lowercase())?;
        }
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize)]
struct Count {
    seen: u64,
}

/// One more sighting of the Cell's word.
#[derive(Serialize, Deserialize)]
struct Seen;

impl Change<Count> for Seen {
    fn apply_to(&self, state: &mut Count) {
        state.seen += 1;
    }
}

/// One Cell per word, minted on first sight of it.
#[derive(Default)]
struct Tally;

#[flezha_runner::tasklet(name = "tally")]
impl Tasklet for Tally {
    type Input = String;
    type Output = String;
    type State = Count;
    type Change = Seen;

    fn run(&mut self, word: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Seen);
        ctx.emit(format!("{word} {}", ctx.state().seen))
    }
}

struct WordCount;

impl Relay for WordCount {
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

#[test]
fn an_in_tree_contract_copy_does_not_shadow_the_checkout() {
    let checkout = std::env::temp_dir().join(format!("flezha-contract-{}", std::process::id()));
    let package = checkout.join("sdk/rust");
    let carry_manifest = |tree: &std::path::Path| {
        std::fs::create_dir_all(tree.join("test/fixtures")).unwrap();
        std::fs::write(tree.join(MANIFEST), "").unwrap();
    };
    carry_manifest(&checkout);
    carry_manifest(&package.join("contract"));

    let found = contract_root(None, &package);
    std::fs::remove_file(checkout.join(MANIFEST)).unwrap();
    let vendored = contract_root(None, &package);
    std::fs::remove_dir_all(&checkout).unwrap();

    assert_eq!(found, checkout);
    assert_eq!(vendored, package.join("contract"));
}

/// The scenario every core runs: the lines to feed, and the sink values to
/// release as a multiset.
fn scenario() -> (Vec<String>, BTreeMap<String, usize>) {
    let path = contract_file("test/fixtures/local_mode_word_count.txt");
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

    let run = LocalRelay::start::<WordCount>().expect("the run starts");
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
        "both tasklets ran, the runtime performed the hop, and one Cell per word counted"
    );

    let first = seen.first().expect("a released value");
    assert_eq!(first.channel, "totals");
    assert!(
        first.metadata.contains_key("flz.cell")
            && first.metadata.contains_key("flz.seq")
            && first.metadata.contains_key("flz.key"),
        "a keyed sink carries the identity stamps a deployed run stamps: {:?}",
        first.metadata
    );

    // The same class, with no local-mode member on it, is what a runner
    // declares against a controller.
    Flezha::create_runner("default")
        .expect("a runner is created")
        .register_factory(WordCount)
        .expect("the identical declaration registers for deployment");
}

#[test]
fn a_channel_the_class_does_not_declare_is_refused_by_name() {
    let run = LocalRelay::start::<WordCount>().expect("the run starts");
    let refusal = run
        .feed("nope", "1")
        .expect_err("an undeclared channel is refused");
    assert!(
        refusal.to_string().contains("nope") && refusal.to_string().contains("lines"),
        "the refusal names the channel and what is declared: {refusal}"
    );
}
