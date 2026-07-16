# flezha_runner

Rust SDK for the Flezha runner platform.

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
