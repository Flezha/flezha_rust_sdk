# Changelog — `flezha_runner` (Rust SDK)

All notable changes to the Rust SDK crate.

## Unreleased

### Operation-requested spawn (`add-operation-requested-spawn`)

- **`OperationCtx::spawn_bounded(instance_id, factory_id, payload, if_running)`**
  — spawn-or-deliver a bounded instance from inside any operation. A free id
  starts `factory_id` with `payload` as its first input; a taken id receives it
  as one more input (`IfRunning::Deliver`) or reports
  `SpawnError::AlreadyRunning` (`IfRunning::Error`). Always in the requester's
  own namespace.
- **Fire-and-forget with a result**: returns a `SpawnHandle` immediately. Every
  failure resolves that handle rather than the operation's `run` — `wait()` /
  `try_result()` read it, and a handle dropped unobserved reports a failure to
  the log via one process-wide reaper thread.
- **Live processing only**: a replayed invocation (crash resume, late-input
  correction) issues no request and answers `SpawnOutcome::SkippedOnReplay`.
- New public items: `IfRunning`, `SpawnOutcome`, `SpawnError`, `SpawnHandle`,
  re-exported from `flezha_runner::operation`. Example:
  `examples/spawn_bounded_worker.rs`.
- **Wire**: additive `SpawnBoundedRequest` (tag 34) / `SpawnBoundedResponse`
  (tag 35). Requires a controller of the same release.

### Determinism contract + output identity (`deterministic-effects-and-output-identity`)

- **BREAKING: `Clock::now()` removed.** The runtime evaluates the declared
  Clock once per `run` invocation and serves it via `OperationCtx::now()`
  (`u64`: epoch ms for `WallClock`, extracted input timestamp for
  `EventTimeFrom<F>` — now fully wired via the new `TimeExtractor<Input>`
  trait — and the durable change seq for `LogicalClock`). The `Operation`
  trait bound tightened to `type Clock: ClockFor<Self::Input>`, making an
  unservable extractor a compile error; `LogicalClock` with
  `Change = NoChange` fails registration with
  `BuildTopologyError::UnservableClock`.
- **`OperationCtx::stable_id()`** — replay-stable 128-bit identifiers
  (frozen FNV-1a-128 over segment identity ‖ change seq ‖ call ordinal);
  the determinism-contract replacement for `Uuid::new_v4()`.
- **Keyed channels**: `.keyed()` on plugin-wired channels +
  `ctx.emit_keyed(key, value)` stamp `flz.segment` / `flz.seq` / `flz.key`
  on every plugin frame; the `flz.` metadata namespace is reserved
  (`ReservedMetadataKey`). Non-keyed channels are byte-identical to before
  (criterion dispatch regression bench: no change detected).
- **Harness**: `replay_emits_equal` (output-side determinism gate),
  injectable fixed/scripted wall clock, `with_keyed_channel`,
  `emitted_meta`.

### Performance

- **Dispatch core rewritten behind the `fast-dispatch` cargo feature.** The
  single-coordinator dispatch loop is replaced with per-operative lock-free
  mailboxes (`crossbeam::queue::SegQueue` + `AtomicBool inflight`) drained
  by a Chase-Lev work-stealing pool (built on `crossbeam-deque`). The live
  operative registry is backed by `DashMap` instead of
  `RwLock<HashMap>`. Per-message dispatch-overhead drops from ~9 to ≤3
  synchronization operations on the hot path.
  - Criterion dispatch micro-bench (100 operatives × 10k messages, pure
    dispatch): ~862k–907k msg/s on a 16-core dev box (baseline: ~90k msg/s).
  - End-to-end `mix bench --runtime rust -n 100 -m 10000`: improvement
    scales with hardware parallelism; see
    `openspec/changes/optimize-rust-sdk-dispatch/baseline.txt` for recorded
    numbers.
  - Rolled out behind the `fast-dispatch` feature flag (default off on the
    published crate during the rollout window, on for this repo's test
    apps). The legacy path is retained behind
    `#[cfg(not(feature = "fast-dispatch"))]`.

### Preserved invariants (no observable behavior change)

- Sticky per-operative ordering under `OperativeRunner::ThreadPool`: two
  invocations of the same operative's `process(...)` never overlap, inputs
  are observed in arrival order.
- `OperativeRunner::ThreadSpawn`: per-invocation OS thread spawn retained.
- Reconnect semantics: exponential backoff (200 ms → 30 s cap), state
  reset on unexpected drop, no reconnect on `RunnerDisconnect`.
- Command dispatch (`AssignRunnerId`, `ConnectSocket`,
  `AddOperative`, `RemOperative`, `SegmentState`,
  `MigrationSerialization`, `MigrationDeserialization`,
  `RunnerDisconnect`) produces byte-identical `CommandResponse`
  bytes.
- Public API surface unchanged: `new`, `Flezha`, `register_factory`,
  `OperativeHandler`, `OperativeInput` / `OperativeOutput` /
  `OperativeResult`, `OperativeSettings`, `FactoryMetadata` keep the same
  signatures.

### Added (internal)

- `runner/mailbox.rs`, `runner/work_stealing_pool.rs`,
  `runner/fast.rs` — the fast-dispatch internals.
- `operative::factory_registry::OperativeRegistry` /
  `OperativeSlot` — DashMap-backed live registry.
- `benches/dispatch.rs` — Criterion throughput + latency micro-benches.
- `benches/correctness.rs` — contention-adder, sticky-ordering, thread-spawn
  invariant checks.
- `tests/loom_mailbox.rs` — loom model check over the mailbox CAS dance
  (`cfg(loom)` gated).

### Dependencies

- Added `dashmap` (optional, under `fast-dispatch`).
- Added `crossbeam-deque` (optional, under `fast-dispatch`).
- Added `criterion` as `[dev-dependencies]`.
