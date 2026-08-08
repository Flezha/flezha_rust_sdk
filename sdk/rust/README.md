# flezha_runner

Rust SDK for the Flezha runner platform.

## Quickstart

```rust
use flezha_runner::Flezha;

fn main() -> Result<(), flezha_runner::FlezhaError> {
    Flezha::create_runner("my_app")?        // the namespace
        .define_unbounded::<MyOperative>()? // legal before AND after start()/run()
        .run()?;                            // connect + block until shutdown
    Ok(())
}
```

`run()` is `start()` + `await_termination()` — the one-liner for a standalone
binary. Embedding into a larger app? `start()` keeps your thread — bind the
runner (`let runner = Flezha::create_runner(ns)?.start()?;`, a chained
temporary would Drop and shut it down at statement end); dropping it runs the
graceful-shutdown sequence, and `Flezha::current_runner()` reaches it from
anywhere. Types are dynamic: `define_unbounded`/`define_bounded` and their
inverses `remove_unbounded`/`remove_bounded` are legal before and after
`start()` (a post-start change ships `UpdateFactories` to the controller);
starting with zero declared types is legal. At most one runner is started per
process: a second `start()` returns
`Err(FlezhaError::AlreadyRunning { .. })` naming the running namespace. Every
fallible call returns `Result<_, FlezhaError>` — the whole chain compiles
under a single `?`; invalid topologies surface at the `define_unbounded` call
site — no panics. Lifecycle vocabulary: `docs/sdk_naming_conventions.md`.

## Authoring a topology

Graphs are declared producer-first (`sdk-graph-authoring`): `.into::<Proc>()`
wires a consumer and returns **that procedure's** output stream handle, so a
linear pipeline is one expression. The channel between two procedures carries
the producer's declared `Output` type — no type token, no channel name unless
you want one (`.named(...)` overrides the name derived from the producer's
identity; entry names are explicit, they are the operative's address).

```rust
struct OrdersOperative;
impl UnboundedOperative for OrdersOperative {
    fn factory_id() -> &'static str { "orders" }
    fn channels(c: &ChannelBuilder) {
        // Typed entry: OrderEvent is deserialized from JSON at the edge
        // (Vec<u8>/String stay passthrough; input_with takes a custom
        // bytes → T parser). A failing parse rides the channel's
        // retry→DLQ ladder.
        c.input::<OrderEvent>("orders")
            .into::<Validate>()
            .into::<Alert>()
            .output("alerts"); // plugin-bound terminal (was `to_plugin`)
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
tap is `also_output_with(name, serializer)` / `also_output(name)`.

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
`docs/multi_input_procedures.md`.

### A fuller graph: named nodes, a hybrid tap, a keyed sink

Everything composes on the same handles. Here one `Filter` class serves two
positions with independent state, the validated stream is tapped to an audit
plugin *and* consumed downstream, the join accumulates both filters'
verdicts, and the terminal channel carries output identity for a
changelog-style sink:

```rust
struct RiskOperative;
impl UnboundedOperative for RiskOperative {
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
            .also_output("audit");               // tap: default JSON to `audit`

        // One class, two nodes: each keeps its own per-entity state under
        // its node identity (state dirs, journals, diagnostics). Both arms
        // name the same (unnamed) Verdicts class, so their verdicts fan IN —
        // either arm's `.into::<Verdicts>()` returns the one join's handle.
        let verdicts = validated.into_named::<Filter>("fraud_filter").into::<Verdicts>();
        validated.into_named::<Filter>("spam_filter").into::<Verdicts>();

        // The join's output closes plugin-bound and keyed: every emit calls
        // ctx.emit_keyed(key, value) and frames carry flz.* identity stamps,
        // so a changelog sink upserts instead of appending.
        verdicts.named("risk.verdicts").keyed().output("verdict_sink");
    }
}
```

Reading order is flow order: every statement starts at a handle you already
hold, and the only names in the graph are the ones something external needs —
the entry (`orders`), the tap (`audit`), the sink (`verdict_sink`), and any
channel you chose to `.named(...)`.

### A structured bounded DAG

Bounded operatives author on the same builder. A durable class journals each
proc barrier, so inter-proc channels declare `.codec()`; a fan-in node runs once,
after **all** its producers committed, with the accumulated batch — and on an
abnormal exit the saga unwinds reverse-topologically (the join before either
producer, the shared root last):

```rust
struct SettleBounded;
impl BoundedOperative for SettleBounded {
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
            .output("settlement_sink");
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
and this crate's poll workers drive it (`docs/sdk_poll_core.md`). See
`docs/operative_architecture.md` for the dispatch pipeline,
`docs/operative_state_persistence.md` for the state-persistence trait
surface,
and `docs/sdk_protocol_spec.md` for the wire format.
