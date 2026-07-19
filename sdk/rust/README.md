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
- `testing` — exposes `flezha_runner::testing::OperationTestHarness`
  for downstream crates that want replay-equality tests.

## Architecture

See `docs/operative_architecture.md` for the dispatch pipeline,
`docs/operative_state_persistence.md` for the state-persistence trait
surface, and `docs/sdk_protocol_spec.md` for the wire format.
