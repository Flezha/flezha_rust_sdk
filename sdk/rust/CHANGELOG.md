# Changelog — `flezha_executor` (Rust SDK)

All notable changes to the Rust SDK crate.

## Unreleased

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
  reset on unexpected drop, no reconnect on `ExecutorDisconnect`.
- Command dispatch (`AssignExecutorId`, `ConnectSocket`,
  `AddOperative`, `RemOperative`, `SegmentState`,
  `MigrationSerialization`, `MigrationDeserialization`,
  `ExecutorDisconnect`) produces byte-identical `CommandResponse`
  bytes.
- Public API surface unchanged: `new`, `Flezha`, `register_factory`,
  `OperativeHandler`, `OperativeInput` / `OperativeOutput` /
  `OperativeResult`, `OperativeSettings`, `FactoryMetadata` keep the same
  signatures.

### Added (internal)

- `executor/mailbox.rs`, `executor/work_stealing_pool.rs`,
  `executor/fast.rs` — the fast-dispatch internals.
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
