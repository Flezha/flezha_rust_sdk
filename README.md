# flezha_runner

Rust SDK for the Flezha runner platform.

**Support tier:**
[tier 1](https://github.com/flezha/flezha/blob/main/docs/public/publishing/maturity.md#sdk-rust),
run by the maintainer in a live deployment, passes the full conformance run in CI and is
permanently supported.

## Install

```toml
[dependencies]
flezha_runner = "0.1.0"
```

`flezha_runner` is not on crates.io yet, and the core it builds on has no
release tag yet. Until both exist, point Cargo at a checkout as
[`examples/echo`](examples/echo/README.md#building-from-a-checkout) shows.

## Supported Rust versions

A stable Rust toolchain. The crate is edition 2021 and declares no minimum
Rust version.

## The native library

There is none to install. The kernel, `flz_core`, is an ordinary Cargo
dependency at a git tag, compiled into your binary with the rest of the crate,
so nothing is loaded at run time.

## Local mode, testing and the relay API

- **Local mode**: `LocalRelay::start::<R>()` runs a relay's declared graph in
  your own process, with no controller. See
  [Local mode](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#local-mode).
- **Testing**: `flezha_runner::testing::TaskletTestHarness`, behind the
  `testing` feature, drives one tasklet and checks its replay. See
  [Check a tasklet on its own](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#check-a-tasklet-on-its-own).
- **The relay API**: a relay's entry channels, answering a caller, `call` to
  another relay, and `runner.send` and `runner.call` from application code.
  See [Sending and calling](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/sending_and_calling.md).

## Quickstart: connect to a controller

```rust
use flezha_runner::Flezha;

fn main() -> Result<(), flezha_runner::FlezhaError> {
    Flezha::create_runner("my_app")?        // the namespace
        .register_factory(MyRelay)?     // legal before AND after start()/run()
        .run()?;                            // connect + block until shutdown
    Ok(())
}
```

`run()` is `start()` + `await_termination()` — the one-liner for a standalone
binary. Embedding into a larger app? `start()` keeps your thread — bind the
runner (`let runner = Flezha::create_runner(ns)?;` then `runner.start()?;`, a
chained temporary would Drop and shut it down at statement end); dropping it
runs the graceful-shutdown sequence, and `Flezha::current_runner()` reaches it
from anywhere. Types are dynamic: one `register_factory` takes a relay of
either kind — the kind is read from the value, never named by the call — and
`unregister_factory` is its inverse; both are legal before and after `start()`
(a post-start change ships `UpdateFactories` to the controller). Starting with
zero declared types is legal, whatever short-form or `relay!` types the
binary contains: nothing is harvested. At most one runner is started per
process: a second `start()` returns
`Err(FlezhaError::AlreadyRunning { .. })` naming the running namespace. Every
fallible call returns `Result<_, FlezhaError>` — the whole chain compiles
under a single `?`; invalid topologies surface at the `register_factory` call
site — no panics. Lifecycle vocabulary: `docs/internal/sdk/naming_conventions.md`.

## Authoring a topology

Graphs are declared producer-first (`sdk-graph-authoring`): `.into::<Task>()`
wires a consumer and returns **that tasklet's** output stream handle, so a
linear pipeline is one expression. The channel between two tasklets carries
the producer's declared `Output` type — no type token, no channel name unless
you want one (`.output_named(...)` names the channel it terminates and
`.named(...)` names one that has no output; either overrides the name derived
from the producer's identity, and declaring both on one channel is refused.
Entry names are explicit, they are the relay's address).

```rust
struct OrdersRelay;
impl Relay for OrdersRelay {
    fn factory_id() -> &'static str { "orders" }
    fn channels(c: &ChannelBuilder) {
        // Typed entry: OrderEvent is deserialized from JSON at the edge
        // (Vec<u8>/String stay passthrough; input_with takes a custom
        // bytes → T parser). A failing parse rides the channel's
        // retry→DLQ ladder.
        c.input::<OrderEvent>("orders")
            .into::<Validate>()
            .into::<Alert>()
            .output_named("alerts"); // plugin egress; terminal — nothing wired after it
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
identity. Graphs must be acyclic; per-channel config (`keyed`, `on_full`,
`retry_max`, `retry_backoff_ms`) chains on the handle; the hybrid
tap is the SAME method — `output()` / `output_named(name)` / `output_with(serializer)`
/ `output_named_with(name, serializer)` all return the handle, so a consumer wires
after it. A channel is terminal by having none.

A tasklet that needs **two feeds** — a data stream plus a live config
stream, a join of two computed streams — subscribes them with `merge`, which
maps each source's payload into one author-declared sum-type `Input`:

```rust
c.merge::<DetectorInput>()
    .from(readings, DetectorInput::Reading)
    .from(config, DetectorInput::Threshold)
    .into::<Detector>();
```

Per-source FIFO is preserved and nothing is ordered across sources; see
`docs/internal/sdk/multi_input_tasklets.md`.

When that config stream is **relay-wide** and the consumer is keyed, a merge
alone is not enough: a keyed tasklet cannot receive a broadcast, since the key
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
`docs/internal/sdk/authoring_contract.md` has the full contract.

### A fuller graph: named nodes, a hybrid tap, a keyed output

Everything composes on the same handles. Here one `Filter` class serves two
positions with independent state, the validated stream is tapped to an audit
plugin *and* consumed downstream, the join accumulates both filters'
verdicts, and the terminal channel carries output identity for a
changelog-style output:

```rust
struct RiskRelay;
impl Relay for RiskRelay {
    fn factory_id() -> &'static str { "risk" }
    fn channels(c: &ChannelBuilder) {
        // Entry-side config chains BEFORE .into: this channel retries a
        // failing consumer (or a failing typed parse) 5 times, 20 ms base.
        let validated = c
            .input::<OrderEvent>("orders")
            .retry_max(5)
            .retry_backoff_ms(20)
            .into::<Validate>()
            // tap: default JSON, on a channel this call names
            .output_named("orders.validated");

        // One class, two nodes: each keeps its own per-cell state under
        // its node identity (state dirs, journals, diagnostics). Both arms
        // name the same (unnamed) Verdicts class, so their verdicts fan IN —
        // either arm's `.into::<Verdicts>()` returns the one join's handle.
        let verdicts = validated.into_named::<Filter>("fraud_filter").into::<Verdicts>();
        validated.into_named::<Filter>("spam_filter").into::<Verdicts>();

        // The join's output closes plugin-bound and keyed: every emit calls
        // ctx.emit_keyed(key, value) and frames carry flz.* identity stamps,
        // so a changelog output upserts instead of appending.
        verdicts.keyed().output_named("risk.verdicts");
    }
}
```

Reading order is flow order: every statement starts at a handle you already
hold, and the only names in the graph are the ones something external needs —
the entry (`orders`), the tap (`orders.validated`), the terminal
(`risk.verdicts`), and any channel you chose to `.named(...)`.

### A structured steps relay DAG

Steps relays author on the same builder. A durable class journals each
task barrier, and the runtime derives the codec for that from each tasklet's
declared `Output`, so a chain declares nothing about it; a fan-in node runs once,
after **all** its producers committed, with the accumulated batch — and on an
abnormal exit the saga unwinds reverse-topologically (the join before either
producer, the shared root last):

```rust
struct SettleSteps;
impl Relay for SettleSteps {
    fn factory_id() -> &'static str { "settle" }
    fn channels(c: &ChannelBuilder) {
        // The diamond: Reserve fans out to Charge and Ledger, whose outputs
        // accumulate into one Reconcile join.
        let reserved = c
            .input::<SettleJob>("in")
            .retry_max(3) // per-cell ladder before the saga fires
            .into::<Reserve>()
            .named("reserved");
        reserved.into::<Charge>().named("charged").into::<Reconcile>();
        reserved.into::<Ledger>().named("booked").into::<Reconcile>()
            .output_named("reconciled");
    }
}
```

A crash between the two producers resumes with the committed producer's
**journaled** handoff banked and only the uncommitted arm re-run; `Reconcile`
still runs exactly once. Cycles are refused at registration; a chained loop
(`Each::Chained`) additionally requires the graph to converge on a single
terminal that releases an output channel, since that handoff is what seeds the
next iteration.

### Writing a tasklet

A tasklet is your own type: `#[flezha::tasklet(name = "...")]` on its
`impl Tasklet` block (`examples/echo/src/tasklet.rs`). The attribute generates
no type. It writes the items the block leaves out: `NAME`, the settings given
as keys, the flags that say which hooks the block writes, the clock, a default
`init_state`, and `State = ()`, `Change = NoChange`, `Output = ()`.

```rust
#[derive(Default)]
struct CountTrades;

#[tasklet(name = "count_trades")]
impl Tasklet for CountTrades {
    type Input = Trade;
    type Output = u64;
    type State = Totals;
    type Change = Counted;

    fn run(&mut self, _: Trade, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Counted);                 // applied through `Counted::apply_to`, journaled
        ctx.emit(ctx.state().count)
    }
}
```

`ctx.state()` reads the Cell's state; `ctx.change(..)` is the only way to
change it, so a mutation the runtime did not record does not compile. A hook
is declared by writing it: `event_time` (which makes the clock event time),
`on_timer`, `compensate`, `cells_for`, `init_state`, `migrate_state`,
`migrate_change`. A one-tasklet relay is shorter still: the short-form
attributes go on a stateless function, or on the `impl Tasklet` block of one
with state. `docs/internal/sdk/rust_relay_architecture.md` has the whole surface,
including the block written without the attribute.

`ctx.part(value)` sends a preview of the output while `run` is still working,
encoded as `ctx.emit(value)` would be. Listeners attached to the relay see
it before the output; an output never does. It never waits: a part that cannot
leave at once is dropped. A channel that does not end in an output refuses it
with `part.no_output`.

## Public API workflow

The public API surface baseline lives at `sdk/rust/public-api.txt`, recorded
with the `testing` feature so the test harness is part of it.
`tests/public_names.rs` holds it to the cross-SDK names in
`test/fixtures/sdk_public_names.txt`. After any public-API-affecting change:

1. Install the tool: `cargo install cargo-public-api` (one-time).
2. Re-record the baseline:

   ```
   cargo public-api -p flezha_runner --features testing > sdk/rust/public-api.txt
   ```

3. Review the diff in your PR. Treat new pub items as additive;
   removals or signature changes are breaking and require a SemVer bump
   (the library is unpublished today; see umbrella decision on
   "no-legacy-shims" before adding compatibility code).

## Features

- `bench-internals` — exposes internal dispatch types for the crate's
  own benches/tests. NOT a stable API.
- `testing` — exposes `flezha_runner::testing::TaskletTestHarness`
  for downstream crates that want replay-equality tests.
- `schemars`: `derive_schema()` on a channel handle, the channel's JSON
  Schema in the relay's API derived from a `schemars::JsonSchema` type. Off by
  default.

## Architecture

This crate is the typed layer over the shared runner kernel: the `flz_core`
workspace crate owns dispatch, durability, retries, timers and transport,
and this crate's poll workers drive it (`docs/internal/sdk/poll_core.md`). See
`docs/internal/sdk/rust_relay_architecture.md` for the dispatch pipeline,
`docs/internal/sdk/state_persistence.md` for the state-persistence trait
surface,
and `docs/internal/sdk/protocol_spec.md` for the wire format.

## License

Apache-2.0, [`LICENSE`](LICENSE).
