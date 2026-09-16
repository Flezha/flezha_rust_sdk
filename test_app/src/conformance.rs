//! The conformance run: every row of `test/fixtures/reference_topologies.txt`
//! driven through THIS SDK's builder.
//!
//! `flz_core/src/host/check_tests.rs` is the same table driven against the
//! record directly, because the core has no builder. This one maps the same
//! step vocabulary onto the typed graph builder, so what is measured here is
//! what an author of a Rust operative actually writes.
//!
//! Two entry points, both off `main`:
//!
//! - `FLEZHA_CONFORMANCE_REPORT=<path>` (or `--conformance-report <path>`)
//!   drives every row and writes the report the drill reads
//!   (`test/system/topology_refusal_e2e_test.exs`), then exits without
//!   connecting to a controller. The drill names the path in the environment,
//!   which reaches the app whatever shell a manifest's launch line goes
//!   through.
//! - `FLEZHA_REFUSAL_FIXTURE=<topology id>` registers ONLY that row's topology
//!   at startup, so a refused one kills the process with the refusal's own
//!   name in its output.
//!
//! The palette procedures below exist only for this run. They are not in the
//! app's declared fixture set: nothing registers them but a row that names
//! them.
//!
//! What the typed builder cannot author, and why the report skips it: the
//! producer-side links (`named`, `sink`, `keyed`, `branch`) live on
//! `Stream<'b, T, NodeTarget>` and an entry channel's handle is a different
//! type, so a row applying one to an entry channel has no Rust spelling; and a
//! stream class has no `sync_durability` knob at all — it is a workflow
//! declaration here, which is why the record's flag is written `false`
//! (`sdk/rust/src/operative/builder.rs`). The fixture's `needs` column names
//! both.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use flezha_runner::channel::{
    ChannelDescriptor, ChannelKind, EntryStream, Merge, NodeTarget, Stream, StreamTarget,
};
use flezha_runner::operative::builder::{build_topology_for, DEFAULT_MAILBOX_CAPACITY};
use flezha_runner::operative::topology::OperativeTopology;
use flezha_runner::procedure::LogicalClock;
use flezha_runner::workflow::spec::build_spec;
use flezha_runner::FlezhaRunner;
use serde_json::{json, Value};

use crate::channel::HybridChannelOperative;
use crate::workflow::TierCommitBlockWorkflow;
use crate::*;

/// The fixture, relative to the repository root the app is launched from.
const FIXTURE: &str = "test/fixtures/reference_topologies.txt";

/// The authoring surfaces this builder carries — the same three the app's
/// `conformance.json` declares. A row needing anything else is skipped BY NAME,
/// which is what makes the skip checkable.
const SURFACES: [&str; 3] = ["merge", "branch", "workflow"];

/// A row of the four-column refusal table. The refusal NAME is the fixture's
/// to state and the seam test's to check; this run reports the outcome it read,
/// never a name it went looking for.
pub(crate) struct Row {
    topology: String,
    steps: String,
    needs: String,
}

/// The refusal table, in file order.
fn rows() -> Result<Vec<Row>, String> {
    let path = fixture_path()?;
    let body = std::fs::read_to_string(&path)
        .map_err(|e| format!("the fixture is the contract and must be readable: {e}"))?;
    Ok(body
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let cols: Vec<&str> = line.split('|').map(str::trim).collect();
            (cols.len() == 4).then(|| Row {
                topology: cols[1].to_string(),
                steps: cols[2].to_string(),
                needs: cols[3].to_string(),
            })
        })
        .collect())
}

/// The fixture as seen from wherever the app was launched: the drill runs it
/// from the repository root, a developer runs it from the app's own directory.
fn fixture_path() -> Result<PathBuf, String> {
    let mut base = PathBuf::from(".");
    for _ in 0..5 {
        let candidate = base.join(FIXTURE);
        if candidate.exists() {
            return Ok(candidate);
        }
        base = base.join("..");
    }
    Err(format!(
        "{FIXTURE} is not under any parent of the working directory"
    ))
}

/// `verb(arg)` → `("verb", "arg")`; a bare word → `("word", "")`.
fn split_call(text: &str) -> (&str, &str) {
    match text.split_once('(') {
        Some((verb, rest)) => (verb.trim(), rest.trim_end_matches(')').trim()),
        None => (text.trim(), ""),
    }
}

// ---------------------------------------------------------------------------
// The palette. One fixture procedure per role the fixture header names; the
// facts a role declares are the facts a rule reads.

thread_local! {
    /// The mailbox capacity a row declared per role, read by the palette's
    /// `mailbox_capacity()` as the procedure is minted.
    static MAILBOX: RefCell<HashMap<String, usize>> = RefCell::new(HashMap::new());
}

fn mailbox_of(role: &str) -> usize {
    MAILBOX.with(|m| {
        m.borrow()
            .get(role)
            .copied()
            .unwrap_or(DEFAULT_MAILBOX_CAPACITY)
    })
}

/// One palette procedure: `String → String`, with the role's own facts.
macro_rules! palette_procedure {
    ($ty:ident, $role:literal, $name:literal, { $($facts:tt)* }) => {
        #[derive(Default)]
        pub(crate) struct $ty;

        impl Procedure for $ty {
            type Input = String;
            type Output = String;
            type State = ();
            type Config = ();
            $($facts)*

            fn name() -> &'static str {
                $name
            }

            // A row declares the capacity on the role; the resolver reads it
            // here, which is the only place a Rust procedure can carry one.
            const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;

            fn mailbox_capacity() -> usize {
                mailbox_of($role)
            }

            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}

            fn run(
                &mut self,
                _input: String,
                _state: StateHandle<'_, Self>,
                _ctx: &mut ProcedureCtx<'_>,
            ) -> Result<(), ProcedureError> {
                Ok(())
            }
        }
    };
}

palette_procedure!(Plain, "plain", "plain", {
    type Change = NoChange;
    type Clock = WallClock;
});

palette_procedure!(Second, "second", "second", {
    type Change = NoChange;
    type Clock = WallClock;
});

// A distinct class declaring the SAME name as `plain`: that IS the collision.
palette_procedure!(Twin, "twin", "plain", {
    type Change = NoChange;
    type Clock = WallClock;
});

palette_procedure!(StaticKeys, "static", "static", {
    type Change = NoChange;
    type Clock = WallClock;

    fn entity_keys() -> Vec<&'static str> {
        vec!["a", "b"]
    }
});

palette_procedure!(Routed, "routed", "routed", {
    type Change = NoChange;
    type Clock = WallClock;

    fn entity_keys() -> Vec<&'static str> {
        vec!["a", "b"]
    }

    const ENTITIES_FOR_OVERRIDDEN: bool = true;

    fn entities_for(_input: &String) -> Vec<&'static str> {
        vec!["a"]
    }
});

// Dynamic mode is read off the WIRING in this SDK — a procedure reached by a
// keyed chain is dynamic — so the role's own declaration is a plain one.
palette_procedure!(Dynamic, "dynamic", "dynamic", {
    type Change = NoChange;
    type Clock = WallClock;
});

palette_procedure!(NoChangeLogical, "nochange", "nochange", {
    type Change = NoChange;
    type Clock = LogicalClock;
});

/// The event-time extractor the `eventtime` role's clock is served by.
pub(crate) struct FirstByte;
impl TimeExtractor<String> for FirstByte {
    fn extract(input: &String) -> u64 {
        input.len() as u64
    }
}

palette_procedure!(EventTime, "eventtime", "eventtime", {
    type Change = NoChange;
    type Clock = EventTimeFrom<FirstByte>;
});

// ---------------------------------------------------------------------------
// The row under construction. `channels()` takes no argument but the builder,
// so the row it declares is the one this thread most recently loaded.

#[derive(Default)]
struct Plan {
    factory_id: &'static str,
    steps: String,
}

thread_local! {
    static PLAN: RefCell<Plan> = RefCell::new(Plan::default());
}

/// A row's class. `class.persistent` selects between this and its volatile
/// twin, which is the only class fact a Rust operative declares as a const
/// rather than on the builder.
struct RowClass;

impl StreamOperative for RowClass {
    fn factory_id() -> &'static str {
        PLAN.with(|p| p.borrow().factory_id)
    }

    fn channels(c: &ChannelBuilder) {
        interpret(c, &PLAN.with(|p| p.borrow().steps.clone()));
    }
}

struct VolatileRowClass;

impl StreamOperative for VolatileRowClass {
    const PERSISTENCE_ENABLED: bool = false;

    fn factory_id() -> &'static str {
        RowClass::factory_id()
    }

    fn channels(c: &ChannelBuilder) {
        RowClass::channels(c);
    }
}

/// Load `row` as the class the next `build_topology_for` declares.
fn load(row: &Row) {
    let mailbox = mailbox_steps(&row.steps);
    MAILBOX.with(|m| *m.borrow_mut() = mailbox);
    PLAN.with(|p| {
        *p.borrow_mut() = Plan {
            factory_id: leak(&row.topology),
            steps: row.steps.clone(),
        }
    });
}

/// The capacities a row declares, read BEFORE the steps run: a procedure is
/// minted by the `into` that first names it, and its capacity is read then —
/// which is one step earlier than the `mailbox` step that declares it.
fn mailbox_steps(steps: &str) -> HashMap<String, usize> {
    let mut capacities = HashMap::new();
    for step in steps.split(';').map(str::trim) {
        if let Some((role, rest)) = step.split_once(".mailbox(") {
            let value = rest.trim_end_matches(')');
            if let Ok(capacity) = value.parse() {
                capacities.insert(role.to_string(), capacity);
            }
        }
    }
    capacities
}

/// A `&'static str` for a name the fixture supplied at run time. The builder
/// takes `&'static str` because a topology outlives its declaration; a run
/// declares 39 rows once and exits.
fn leak(text: &str) -> &'static str {
    Box::leak(text.to_string().into_boxed_str())
}

// ---------------------------------------------------------------------------
// The interpreter.

/// A channel the steps address: an entry channel by name, or a procedure's
/// output channel by the role that produces it. The two are different types —
/// which is what puts the producer-side links out of an entry channel's reach.
#[derive(Clone, Copy)]
enum Handle<'b> {
    Entry(EntryStream<'b, String>),
    Node(Stream<'b, String>),
}

struct Interp<'b> {
    c: &'b ChannelBuilder,
    entries: HashMap<String, EntryStream<'b, String>>,
    nodes: HashMap<String, Stream<'b, String>>,
}

fn interpret(c: &ChannelBuilder, steps: &str) {
    let mut interp = Interp {
        c,
        entries: HashMap::new(),
        nodes: HashMap::new(),
    };
    for step in steps.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        interp.step(step);
    }
}

impl<'b> Interp<'b> {
    fn step(&mut self, step: &str) {
        let (verb, arg) = split_call(step);
        match verb {
            "entry" => {
                let handle = self.c.input::<String>(leak(arg));
                self.entries.insert(arg.to_string(), handle);
            }
            "merge" => self.merge(arg),
            "branch" => self.branch(arg),
            _ => self.dotted(step),
        }
    }

    /// The channel `name` addresses. A name neither an entry nor a wired role
    /// is the `detached` surface, which no fluent builder can author.
    fn handle(&self, name: &str) -> Handle<'b> {
        if let Some(entry) = self.entries.get(name) {
            return Handle::Entry(*entry);
        }
        match self.nodes.get(name) {
            Some(node) => Handle::Node(*node),
            None => panic!("the row addresses `{name}`, which nothing has declared"),
        }
    }

    /// `<handle>.<link>…` — a chain declared off a handle, a channel knob, or
    /// a class flag.
    fn dotted(&mut self, step: &str) {
        let (head, rest) = step.split_once('.').expect("a step is a call or a chain");
        if head == "class" {
            return self.class_flag(rest);
        }
        let links: Vec<&str> = rest.split('.').collect();
        let (last, reshaping) = links.split_last().expect("a chain declares a link");
        // A chain whose tip is a reshaping link is one nothing closes; it
        // reaches the record as written and the core answers it.
        if is_reshape(last) {
            match self.handle(head) {
                Handle::Entry(s) => drop(reshape(s, &links)),
                Handle::Node(s) => drop(reshape(s, &links)),
            }
            return;
        }
        match self.handle(head) {
            Handle::Entry(s) => {
                let s = reshape(s, reshaping);
                if !self.terminate(s, last) {
                    panic!(
                        "`{head}.{last}` is a producer-side link on an entry channel, which \
                         this SDK's typed builder does not carry"
                    );
                }
            }
            Handle::Node(s) => {
                let s = reshape(s, reshaping);
                if !self.terminate(s, last) {
                    producer_link(s, last);
                }
            }
        }
    }

    /// The links both handle kinds carry. `false` selects the producer-side
    /// set, which only a procedure's output channel has.
    fn terminate<K: StreamTarget>(&mut self, s: Stream<'b, String, K>, link: &str) -> bool {
        let (verb, arg) = split_call(link);
        match verb {
            "into" => {
                let node = wire(s, arg);
                self.nodes.insert(arg.to_string(), node);
            }
            "retry_max" => {
                s.retry_max(arg.parse().expect("an attempt ceiling"));
            }
            "on_full" => {
                s.on_full(overflow(arg));
            }
            // Read by `mailbox_capacity()` as the procedure was minted.
            "mailbox" => {}
            _ => return false,
        }
        true
    }

    /// `merge(<proc>, <x>, <x>…)` — one merge over the listed sources.
    fn merge(&mut self, arg: &str) {
        let parts: Vec<&str> = arg.split(',').map(str::trim).collect();
        let (role, sources) = parts.split_first().expect("a merge names its consumer");
        let mut merge = self.c.merge::<String>();
        for source in sources {
            merge = match self.handle(source) {
                Handle::Entry(s) => merge.from(s, |v: String| v),
                Handle::Node(s) => merge.from(s, |v: String| v),
            };
        }
        let node = close_merge(merge, role);
        self.nodes.insert((*role).to_string(), node);
    }

    /// `branch(<x>, <n>)` — the branch sugar's arity. The arms are empty: what
    /// the row states is how many there are.
    fn branch(&mut self, arg: &str) {
        let parts: Vec<&str> = arg.split(',').map(str::trim).collect();
        let Handle::Node(s) = self.handle(parts[0]) else {
            panic!(
                "this SDK's branch sugar is a producer-side link; `{}` is an entry channel",
                parts[0]
            );
        };
        let arm: &dyn Fn(Stream<'b, String>) = &|_| {};
        match parts[1].parse::<usize>().expect("a branch arity") {
            1 => s.branch([arm]),
            2 => s.branch([arm, arm]),
            3 => s.branch([arm, arm, arm]),
            n => panic!("the fixture declares a branch of {n} arms; add the arity here"),
        };
    }

    /// `class.<flag>[(<value>)]` — the facts the class declares on the builder.
    /// `persistent` is a const on the class, read by `load`; `workflow` and
    /// `sync_durability` are declarations a stream class does not carry, and
    /// every row needing one is skipped before it reaches here.
    fn class_flag(&mut self, rest: &str) {
        let (flag, value) = split_call(rest);
        match flag {
            "persistent" => {}
            "effectively_once" => self.c.effectively_once(),
            "checkpoint_ack" => self.c.checkpoint_ack(),
            "checkpoint_every" => self.c.checkpoint_every(value.parse().expect("a cadence")),
            "correction_window" => self.c.correction_window(value.parse().expect("a window")),
            "outputs_at_checkpoint" => self
                .c
                .outputs_at_checkpoint(value.parse().expect("a max delay")),
            other => {
                panic!("the fixture spells a class flag this interpreter does not map: {other}")
            }
        }
    }
}

/// Whether a link reshapes the values on a channel rather than closing it.
fn is_reshape(link: &str) -> bool {
    matches!(link, "map" | "filter" | "flat_map" | "key")
}

/// The transform and key links declared before the terminator.
fn reshape<'b, K: StreamTarget>(
    mut s: Stream<'b, String, K>,
    links: &[&str],
) -> Stream<'b, String, K> {
    for link in links {
        s = match *link {
            "map" | "filter" | "flat_map" => s.map(|v: String| v),
            "key" => s.key_by(|v: &String| v.clone()),
            other => panic!("the fixture spells a link this interpreter does not map: {other}"),
        };
    }
    s
}

/// The producer-side links, which only a procedure's output channel carries.
fn producer_link(s: Stream<'_, String, NodeTarget>, link: &str) {
    let (verb, arg) = split_call(link);
    match verb {
        "named" => s.named(leak(arg)),
        "sink" if arg.is_empty() => s.sink(),
        "sink" => s.sink_named(leak(arg)),
        "keyed" => s.keyed(),
        other => panic!("the fixture spells a link this interpreter does not map: {other}"),
    };
}

/// `<x>.into(<role>)` — the palette role bound to its fixture procedure. The
/// consumer is a type argument here, so the binding is a match and not a table.
fn wire<'b, K: StreamTarget>(s: Stream<'b, String, K>, role: &str) -> Stream<'b, String> {
    match role {
        "plain" => s.into::<Plain>(),
        "second" => s.into::<Second>(),
        "twin" => s.into::<Twin>(),
        "static" => s.into::<StaticKeys>(),
        "routed" => s.into::<Routed>(),
        "dynamic" => s.into::<Dynamic>(),
        "nochange" => s.into::<NoChangeLogical>(),
        "eventtime" => s.into::<EventTime>(),
        other => panic!("the fixture names a palette role that is not one: {other}"),
    }
}

/// The same binding, for the merge terminator.
fn close_merge<'b>(m: Merge<'b, String>, role: &str) -> Stream<'b, String> {
    match role {
        "plain" => m.into::<Plain>(),
        "second" => m.into::<Second>(),
        "twin" => m.into::<Twin>(),
        "static" => m.into::<StaticKeys>(),
        "routed" => m.into::<Routed>(),
        "dynamic" => m.into::<Dynamic>(),
        "nochange" => m.into::<NoChangeLogical>(),
        "eventtime" => m.into::<EventTime>(),
        other => panic!("the fixture names a palette role that is not one: {other}"),
    }
}

fn overflow(policy: &str) -> OverflowPolicy {
    match policy {
        "drop_nothing" => OverflowPolicy::DropNothing,
        "drop_newest" => OverflowPolicy::DropNewest,
        "drop_oldest" => OverflowPolicy::DropOldest,
        other => {
            panic!("the fixture spells an overflow policy this interpreter does not map: {other}")
        }
    }
}

// ---------------------------------------------------------------------------
// Driving a row, and the report.

/// What a row's topology answered: the derived declaration, or the refusal
/// names the core handed back as data.
fn topology_of(row: &Row) -> Result<Arc<OperativeTopology>, Vec<String>> {
    if row.steps == "cited above" {
        return cited(&row.topology);
    }
    load(row);
    let built = if row.steps.contains("class.persistent") {
        build_topology_for::<RowClass>()
    } else {
        build_topology_for::<VolatileRowClass>()
    };
    built.map_err(|e| {
        e.errors
            .iter()
            .map(|issue| issue.name().to_string())
            .collect()
    })
}

/// The two twins the fixture cites rather than re-spells: this app's own
/// fixtures, which is where the seven-column table was transcribed from.
fn cited(topology: &str) -> Result<Arc<OperativeTopology>, Vec<String>> {
    match topology {
        "hybrid_channel" => build_topology_for::<HybridChannelOperative>()
            .map_err(|e| e.errors.iter().map(|i| i.name().to_string()).collect()),
        "commit_block_chain" => build_spec::<TierCommitBlockWorkflow>()
            .map_err(|e| vec![e.to_string()])
            .map(|spec| {
                spec.topology
                    .expect("a structured workflow declares a topology")
            }),
        other => panic!("the fixture cites a topology this app does not declare: {other}"),
    }
}

/// The derived declaration, in the columns the fixture's seven-column table
/// states: what the core answered, as the topology materialised it.
fn derived(topology: &OperativeTopology) -> Value {
    let channels: Vec<Value> = topology
        .channels
        .iter()
        .enumerate()
        .map(|(index, channel)| {
            json!({
                "index": index,
                "name": channel.name.to_string(),
                "kind": kind_name(channel),
                "producer": channel.producer,
                "consumers": channel.consumer_ids().collect::<Vec<_>>(),
                "sink": channel.plugin_sink.as_ref().map(|_| channel.name.to_string()),
            })
        })
        .collect();
    Value::Array(channels)
}

/// The kind the core derived, spelled the way the record spells it.
fn kind_name(channel: &ChannelDescriptor) -> &'static str {
    match channel.kind {
        ChannelKind::Entry => "entry",
        ChannelKind::InterProcedure => "inter_proc",
        ChannelKind::PluginBound => "plugin",
    }
}

/// One row's verdict, in the report's shape.
fn judge(row: &Row) -> Value {
    if row.needs != "-" && !SURFACES.contains(&row.needs.as_str()) {
        return json!({"topology": row.topology, "outcome": "skipped", "needs": row.needs});
    }
    match topology_of(row) {
        Ok(topology) => {
            json!({"topology": row.topology, "outcome": "accepted", "derived": derived(&topology)})
        }
        Err(names) => json!({"topology": row.topology, "outcome": "refused", "names": names}),
    }
}

/// Drive the whole table and write the report. Nothing here connects to a
/// controller: the run is the builder's answer to a file.
pub(crate) fn write_report(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let rows = rows()?;
    let judged: Vec<Value> = rows.iter().map(judge).collect();
    let skipped = judged
        .iter()
        .filter(|row| row["outcome"] == "skipped")
        .count();
    std::fs::write(
        path,
        serde_json::to_string_pretty(&json!({"runtime": "rust", "rows": judged}))?,
    )?;
    eprintln!(
        "conformance: {} rows, {} ran, {skipped} skipped",
        rows.len(),
        rows.len() - skipped
    );
    Ok(())
}

/// Register ONLY the named row's topology, the way the app registers any
/// class. A refused row never reaches a connected runner: the process dies
/// here, naming the refusal.
pub(crate) fn register_only(
    runner: &FlezhaRunner,
    topology: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let rows = rows()?;
    let row = rows
        .iter()
        .find(|row| row.topology == topology)
        .ok_or_else(|| format!("{FIXTURE} has no row `{topology}`"))?;

    if row.needs != "-" && !SURFACES.contains(&row.needs.as_str()) {
        eprintln!("{topology}: this SDK's builder carries no `{}`, so the row is skipped and nothing is registered", row.needs);
        std::process::exit(1);
    }

    if row.steps == "cited above" {
        cited(&row.topology).map_err(|names| format!("topology rejected {names:?}"))?;
        return Ok(());
    }

    load(row);
    let defined = if row.steps.contains("class.persistent") {
        runner.define_stream::<RowClass>()
    } else {
        runner.define_stream::<VolatileRowClass>()
    };
    if let Err(refusal) = defined {
        eprintln!("{topology}: {refusal}");
        std::process::exit(1);
    }
    Ok(())
}
