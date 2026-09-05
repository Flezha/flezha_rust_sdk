# flezha_runner

Rust SDK for the Flezha runner platform.

## Local mode: run a graph with no controller

The shortest way to see an operative work. `LocalOperative::start::<R>()` takes
the class you would deploy, runs its declared `channels()` graph in this
process, and hands sink output to a callback — no controller, no socket, no
container:

```rust
use std::time::Duration;
use flezha_runner::LocalOperative;

let run = LocalOperative::start::<WordCount>()?;
run.on_sink("totals", |out| println!("{}", String::from_utf8_lossy(&out.payload)))?;
run.feed("lines", "the quick brown fox")?;
run.run_until_idle(Duration::from_secs(5))?;
```

`on_sink` registers against the sink's **channel** name, which is the producing
procedure's name unless the declaration `named` it. `feed` takes the wire bytes
the channel's declared decoder parses, and blocks while the consumer's mailbox
is full. Worked example: `cargo run -p flezha_runner --example local_word_count`.

**State is held in memory and nothing is durable** — no write-ahead log, no
snapshots, no dead-letter queue. Every run says so on stderr and it is not
suppressible. Durability, effectively-once delivery, plugins, placement and
migration, the dead-letter queue, observability export, authz and multi-runner
scale need a controller; a declaration asking for one of those is refused at
startup, naming it. Local mode also ships in the Java and Elixir SDKs; Node,
Python, Ruby and PHP do not carry it yet. Walk-through:
`docs/quickstart.md`.

## Quickstart: connect to a controller

```rust
use flezha_runner::Flezha;

fn main() -> Result<(), flezha_runner::FlezhaError> {
    Flezha::create_runner("my_app")?        // the namespace
        .define_stream::<MyOperative>()? // legal before AND after start()/run()
        .run()?;                            // connect + block until shutdown
    Ok(())
}
```

`run()` is `start()` + `await_termination()` — the one-liner for a standalone
binary. Embedding into a larger app? `start()` keeps your thread — bind the
runner (`let runner = Flezha::create_runner(ns)?.start()?;`, a chained
temporary would Drop and shut it down at statement end); dropping it runs the
graceful-shutdown sequence, and `Flezha::current_runner()` reaches it from
anywhere. Types are dynamic: `define_stream`/`define_workflow` and their
inverses `remove_stream`/`remove_workflow` are legal before and after
`start()` (a post-start change ships `UpdateFactories` to the controller);
starting with zero declared types is legal. At most one runner is started per
process: a second `start()` returns
`Err(FlezhaError::AlreadyRunning { .. })` naming the running namespace. Every
fallible call returns `Result<_, FlezhaError>` — the whole chain compiles
under a single `?`; invalid topologies surface at the `define_stream` call
site — no panics. Lifecycle vocabulary: `docs/sdk/naming_conventions.md`.

## Authoring a topology

Graphs are declared producer-first (`sdk-graph-authoring`): `.into::<Proc>()`
wires a consumer and returns **that procedure's** output stream handle, so a
linear pipeline is one expression. The channel between two procedures carries
the producer's declared `Output` type — no type token, no channel name unless
you want one (`.named(...)` overrides the name derived from the producer's
identity; entry names are explicit, they are the operative's address).

```rust
struct OrdersOperative;
impl StreamOperative for OrdersOperative {
    fn factory_id() -> &'static str { "orders" }
    fn channels(c: &ChannelBuilder) {
        // Typed entry: OrderEvent is deserialized from JSON at the edge
        // (Vec<u8>/String stay passthrough; input_with takes a custom
        // bytes → T parser). A failing parse rides the channel's
        // retry→DLQ ladder.
        c.input::<OrderEvent>("orders")
            .into::<Validate>()
            .into::<Alert>()
            .sink("alerts"); // plugin egress; terminal — nothing wired after it
    }
}
```

Branching is handle reuse — hold the handle and wire it twice — or the
equivalent `branch(...)` sugar; fan-in is two streams naming one (unnamed)
class, whose node accumulates both contributions:

**Variable spelling** — hold the handle, wire it as many times as you like.
This is the general form: the handle is `Copy`, so it can cross helper
functions, loops, or conditionals, and each arm can keep chaining
independently:

```rust
fn channels(c: &ChannelBuilder) {
    let validated = c.input::<OrderEvent>("orders").into::<Validate>();

    // Two consumers on one channel: both receive every value Validate
    // releases. Each arm is an ordinary statement that keeps its own chain.
    validated.into::<FraudCheck>().into::<Verdicts>();
    validated.into::<Receipt>().into::<Verdicts>(); // same class ⇒ same node: a join
}
```

**`branch(...)` spelling** — pure sugar over the same mechanism: each arm
closure receives a copy of the handle, and the frozen topology is *identical*
to the variable form (pinned by a descriptor-equality test). Use it when the
fan-out is the last thing you do with the stream and a variable would only
add a name:

```rust
fn channels(c: &ChannelBuilder) {
    c.input::<OrderEvent>("orders").into::<Validate>().branch([
        &|b| {
            b.into::<FraudCheck>().into::<Verdicts>();
        },
        &|b| {
            b.into::<Receipt>().into::<Verdicts>();
        },
    ]);
}
```

The two spellings mix freely — `branch` returns the same handle, so you can
tap it, name it, or wire a third consumer after the call. Fewer than two arms
is refused at registration.

One class can sit at several graph positions via named nodes
(`into_named::<Filter>("spam_filter")`) — each named node keeps its own state
identity. Graphs must be acyclic; per-channel config (`codec`, `keyed`,
`on_full`, `retry_max`, `retry_backoff_ms`) chains on the handle; the hybrid
tap is the SAME method — `sink(name)` / `sink_with(name, serializer)` returns
the handle, so a consumer wires after it. A channel is terminal by having none;
`.sealed()` states that in the type.

A procedure that needs **two feeds** — a data stream plus a live config
stream, a join of two computed streams — subscribes them with `merge`, which
maps each source's payload into one author-declared sum-type `Input`:

```rust
c.merge::<DetectorInput>()
    .from(readings, DetectorInput::Reading)
    .from(config, DetectorInput::Threshold)
    .into::<Detector>();
```

Per-source FIFO is preserved and nothing is ordered across sources; see
`docs/sdk/multi_input_procedures.md`.

When that config stream is **operative-wide** and the consumer is keyed, a merge
alone is not enough: a keyed procedure cannot receive a broadcast, since the key
function is total and inside a partitioned member the computed key is admitted
against that member's key-group range. `route_latest` is the packaged bridge —
every key seen on the traffic stream receives each distinct document exactly
once, on that key's own next traffic item:

```rust
let routed = c.route_latest(config, readings, |r: &Reading| r.sensor.clone());
c.merge::<DetectorInput>()
    .from(readings, DetectorInput::Reading)
    .from(routed, |(key, doc)| DetectorInput::Threshold { key, doc })
    .key_by(DetectorInput::key)
    .into::<Detector>();
```

Delivering on the key's own traffic — rather than fanning a new document out
over a remembered key set — is what keeps every emitted key one this member
owns. The price is one tick of latency per key; §4.1 of
`docs/sdk/authoring_contract.md` has the full contract.

### A fuller graph: named nodes, a hybrid tap, a keyed sink

Everything composes on the same handles. Here one `Filter` class serves two
positions with independent state, the validated stream is tapped to an audit
plugin *and* consumed downstream, the join accumulates both filters'
verdicts, and the terminal channel carries output identity for a
changelog-style sink:

```rust
struct RiskOperative;
impl StreamOperative for RiskOperative {
    fn factory_id() -> &'static str { "risk" }
    fn channels(c: &ChannelBuilder) {
        // Entry-side config chains BEFORE .into: this channel retries a
        // failing consumer (or a failing typed parse) 5 times, 20 ms base.
        let validated = c
            .input::<OrderEvent>("orders")
            .retry_max(5)
            .retry_backoff_ms(20)
            .into::<Validate>()
            .named("orders.validated")           // overrides the derived name
            .sink("audit");               // tap: default JSON to `audit`

        // One class, two nodes: each keeps its own per-entity state under
        // its node identity (state dirs, journals, diagnostics). Both arms
        // name the same (unnamed) Verdicts class, so their verdicts fan IN —
        // either arm's `.into::<Verdicts>()` returns the one join's handle.
        let verdicts = validated.into_named::<Filter>("fraud_filter").into::<Verdicts>();
        validated.into_named::<Filter>("spam_filter").into::<Verdicts>();

        // The join's output closes plugin-bound and keyed: every emit calls
        // ctx.emit_keyed(key, value) and frames carry flz.* identity stamps,
        // so a changelog sink upserts instead of appending.
        verdicts.named("risk.verdicts").keyed().sink("verdict_sink");
    }
}
```

Reading order is flow order: every statement starts at a handle you already
hold, and the only names in the graph are the ones something external needs —
the entry (`orders`), the tap (`audit`), the sink (`verdict_sink`), and any
channel you chose to `.named(...)`.

### A structured workflow DAG

Workflow operatives author on the same builder. A durable class journals each
proc barrier, so inter-proc channels declare `.codec()`; a fan-in node runs once,
after **all** its producers committed, with the accumulated batch — and on an
abnormal exit the saga unwinds reverse-topologically (the join before either
producer, the shared root last):

```rust
struct SettleWorkflow;
impl WorkflowOperative for SettleWorkflow {
    fn factory_id() -> &'static str { "settle" }
    fn retry_attempts() -> u32 { 3 } // per-entity ladder before the saga fires
    fn procedures(c: &ChannelBuilder) {
        // The diamond: Reserve fans out to Charge and Ledger, whose outputs
        // accumulate into one Reconcile join.
        let reserved = c
            .input::<SettleJob>("in")
            .into::<Reserve>()
            .named("reserved")
            .codec();
        reserved.into::<Charge>().named("charged").codec().into::<Reconcile>();
        reserved.into::<Ledger>().named("booked").codec().into::<Reconcile>()
            .named("reconciled")
            .sink("settlement_sink");
    }
}
```

A crash between the two producers resumes with the committed producer's
**journaled** handoff banked and only the uncommitted arm re-run; `Reconcile`
still runs exactly once. Cycles are refused at registration; a chained loop
(`LoopMode::Chained`) additionally requires the graph to converge on a single
codec'd terminal.

Parameterized procedures use the `Config` associated type: declare
`type Config = MyConfig` and build the instance from it in `from_config` —
the sanctioned path for one class serving several configurations (see the
`Procedure::from_config` rustdoc for a compiling worked example).

### Declaring less: `SimpleProcedure`

Stable Rust has no associated-type defaults, so a `Procedure` impl spells all
six even when three of them are the near-universal values. `SimpleProcedure` is
the shorthand — `Input`, `Output`, `State` plus `name`, `init_state` and `run` —
lifted to `Procedure` by a blanket impl with `Clock = WallClock`,
`Config = ()` and the `State` type's derived change (`type State = ()` for a
stateless procedure). It registers and dispatches exactly like any other
`Procedure`; the wiring cannot tell which tier authored a node.

Implement `Procedure` directly for anything the shorthand does not cover — a
non-default clock, a typed config, a hand-written `Change`, static entity keys.
A type cannot do both: the two impls overlap and the compiler says so.

## Public API workflow

The public API surface baseline lives at `sdk/rust/public-api.txt`.
After any public-API-affecting change:

1. Install the tool: `cargo install cargo-public-api` (one-time).
2. Re-record the baseline:

   ```
   cargo public-api -p flezha_runner > sdk/rust/public-api.txt
   ```

3. Review the diff in your PR. Treat new pub items as additive;
   removals or signature changes are breaking and require a SemVer bump
   (the library is unpublished today; see umbrella decision on
   "no-legacy-shims" before adding compatibility code).

## Features

- `bench-internals` — exposes internal dispatch types for the crate's
  own benches/tests. NOT a stable API.
- `testing` — exposes `flezha_runner::testing::ProcedureTestHarness`
  for downstream crates that want replay-equality tests.

## Architecture

This crate is the typed layer over the shared runner kernel: the `flz_core`
workspace crate owns dispatch, durability, retries, timers and transport,
and this crate's poll workers drive it (`docs/sdk/poll_core.md`). See
`docs/sdk/rust_operative_architecture.md` for the dispatch pipeline,
`docs/sdk/state_persistence.md` for the state-persistence trait
surface,
and `docs/sdk/protocol_spec.md` for the wire format.

## License

Apache-2.0 — [`LICENSES/Apache-2.0.txt`](../../LICENSES/Apache-2.0.txt). The
per-path map for the whole repository is [`REUSE.toml`](../../REUSE.toml).
