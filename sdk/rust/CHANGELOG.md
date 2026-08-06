# Changelog — `flezha_runner` (Rust SDK)

All notable changes to the Rust SDK crate.

## Unreleased

### Live Entity read (`entity-state-live-query`)

- **The `EntityState` command gained a selector**, so the state an operator asks
  for is the only state serialized: `EntityState { operative_id, procedure,
  entity_key }`, where an empty string means absent. No selector is the
  whole-Operative migration/handover shape and encodes byte-identically to
  before. `entity_key` without `procedure` is refused — an Entity key is unique
  only within its Procedure.
- **Dynamic-mode Entities are serialized**, keyed by whatever the live registry
  holds. The whole-Operative shape previously omitted them entirely.
- **`command::entity_state::serialize_selected_state`** is the public entry
  point: it returns `(json, Option<seq>)`, the seq present only when the selector
  named exactly one Entity. `serialize_operative_state` keeps its signature and
  is now that call with an empty selector. Refusal strings lead with a canonical
  token (`entity_key_without_procedure`, `procedure_not_found`,
  `entity_not_found`, `entity_evicted`).
- **Serialization takes the Entity's state lock and does NOT drain pending
  changes**, so the value returned includes changes not yet flushed to the WAL —
  the read reports `flz.change_seq` on the reply frame's metadata rather than
  hiding that. It triggers no compaction, snapshot or truncation.
- **`CommandAction` gained a defaulted `reply`** returning
  `(payload_id, raw_data, HashMap<String, String>)` — the frame metadata a
  command's answer stamps. Every handler but `EntityState` inherits the default,
  which forwards to `handle` with no metadata, so their frames are unchanged.
- **`DynamicEntityRegistry::recently_evicted`** exposes a bounded (1024-key)
  trace of what the reaper evicted, so a read can answer "idle-evicted" rather
  than "never existed". Beyond the cap it degrades to the latter, which is the
  safe direction.
- Example: `sdk/rust/examples/live_state_read.rs`. Docs:
  `docs/entity_state_live_query.md`.

### Stream transforms and edge keying (`stream-dsl-keyby-and-transforms`)

- **BREAKING — `Procedure::entity_key` and `const ENTITY_KEY_OVERRIDDEN` are
  removed.** Dynamic Entity keying is declared on the wiring instead:
  `.key_by(|v: &T| ..)` on the stream that feeds the procedure. There is no
  shim, so every dynamic-mode Procedure is a compile error until its key moves
  to the edge — which is how the compiler enumerates the migration sites. The
  static pair (`entity_keys` / `entities_for`) and the dynamic knobs
  (`entity_idle_ttl`, `max_entities`) are unchanged. A former `None` return
  becomes a `.filter(..)` declared before the keying; the
  `inputs_dropped_no_key_total` counter is retired with the path it measured.
- **`map` / `filter` / `flat_map` / `key_by` on both stream handles.** They add
  no Procedure, Entity, mailbox or dispatch hop: every transform declared
  before the closing `.into` composes into ONE closure stored on that consumer
  edge and evaluated at the routing seam — the same seam a merge mapping has
  always used, which is why replay determinism cost no new code. Fusion is
  structural: `.into` IS the boundary, so there is no optimizer and no
  `disable_chaining`.
- **`EntryStream<'b, T>` is now a type alias for `Stream<'b, T, EntryTarget>`.**
  One struct carries both handles so their shared surface is defined once;
  producer-side methods (`named`, `codec`, `keyed`, `also_output`, `output`,
  `branch`) remain available only on `Stream<'b, T, NodeTarget>`.
- **`ConsumerEdge.map: Option<MergeMapFn>` → `.transform: Option<EdgeTransformFn>`
  plus `.key: Option<EdgeKeyFn>`.** `MergeMapFn` is renamed `EdgeTransformFn`
  and re-signatured `Fn(&Arc<dyn Any + Send + Sync>) -> EdgeYield` — it is no
  longer merge-specific, and taking the `Arc` is what lets a surviving `filter`
  forward by pointer. `MergeSource::merge_channel_id` → `merge_site`.
- **Edge closures are contained.** A panicking transform or key does not take
  the routing thread down: the value climbs the channel's existing retry→DLQ
  ladder. On a bounded chain it becomes a terminal chain error the saga
  compensates, so the instance still reports.
- New `TopologyIssue` variants `KeyByOnUnkeyableTarget`, `DuplicateKeyBy`,
  `TransformAfterTerminal`, `TransformAfterKeyBy`, `DanglingTransformChain`,
  `UnkeyedEdgeIntoDynamicProcedure`; `StaticAndDynamic` is removed (the
  contradiction it named is now `KeyByOnUnkeyableTarget`) and
  `MergeSourceAlreadyWired` is renamed `EdgeAlreadyWired` — it now guards any
  re-declaration where either side carries a transform or a key.

### Multi-input procedures (`add-multi-input-procedures`)

- **`ChannelBuilder::merge::<I>()`** — subscribe two or more source channels to
  ONE procedure whose `Input` is an author-declared sum type, each `.from(source,
  map)` supplying the mapping from that channel's payload into it:
  `c.merge::<SignalInput>().from(ticker, SignalInput::Ticker).from(config,
  SignalInput::Config).into::<Detector>()`. Entry streams and inter-procedure
  streams are both valid sources on an unbounded operative (`MergeSource`,
  implemented for `EntryStream` and `Stream`), freely mixed; see the bounded
  restriction below. Single-input `.into` is unchanged.
- **Purely a subscription-time construct**: `Procedure` is untouched, so
  dispatch, state, `entities_for`, retry/DLQ and the emit APIs all see one type.
  `entities_for` / `entity_key` are evaluated on the **mapped** value.
- **A merge mapping is contained on the bounded driver thread.** It is the only
  author closure on a bounded chain running outside an entity invocation, so
  `extend_mapped` catches its unwind and reports it as a terminal chain error, and
  the instance still reports. An unguarded one would skip
  `emit_bounded_completed` and strand it. At the **forward** accumulate site the
  saga unwinds the stages recorded as completed — the producer whose edge blew
  up among them, since the mapping runs after that stage's record and commit —
  and Java unwinds the same set in the same order; a sibling still in flight in
  the same level is not unwound, as for any other mid-level abort. At the
  **crash-resume re-seed** site Rust also unwinds the journaled stages (the
  resumed iteration's completed list is seeded from the journal); the Java arm
  does not, a pre-existing gap in its resumed chains recorded under "Known
  limitation" in `docs/multi_input_procedures.md`. `Merge` is `#[must_use]`: a merge left unclosed by
  `.into` declares no edges at all.
- **Per-source FIFO is preserved; nothing is ordered across sources** — the
  guarantee two independent consumers already had. Mappings MUST be pure: they
  run on live delivery and again on WAL replay, the same contract entry parsers
  carry.
- New `TopologyIssue` variants `MergeTooFewSources`, `DuplicateMergeSource` and
  `EdgeAlreadyWired` (a source may not re-wire an edge `.into` or another
  merge already declared — the mapping would otherwise be silently dropped); a
  mapped type that is not the procedure's `Input` is a compile error at
  `.into::<Proc>()`.
- **Bounded merges consume inter-procedure channels only.** The bounded seed is
  a bare payload naming no channel, so an entry channel as one merge source
  among several is refused at registration with
  `BuildBoundedError::MergeFromEntryChannel` rather than accepted and failing
  every invocation. Unbounded merges accept entry channels.
- `ChannelDescriptor::consumers` is now `Vec<ConsumerEdge>` (procedure id plus
  the optional mapping); `consumer_ids()` is the id-only view.
- No wire change and no controller change. Example: `examples/merged_input.rs`.

### Procedure-requested spawn (`procedure-requested-spawn`)

- **`ProcedureCtx::spawn_bounded(instance_id, factory_id, payload, if_running)`**
  — spawn-or-deliver a bounded instance from inside any procedure. A free id
  starts `factory_id` with `payload` as its first input; a taken id receives it
  as one more input (`IfRunning::Deliver`) or reports
  `SpawnError::AlreadyRunning` (`IfRunning::Error`). Always in the requester's
  own namespace.
- **Fire-and-forget with a result**: returns a `SpawnHandle` immediately. Every
  failure resolves that handle rather than the procedure's `run` — `wait()` /
  `try_result()` read it, and a handle dropped unobserved reports a failure to
  the log via one process-wide reaper thread.
- **Live processing only**: a replayed invocation (crash resume, late-input
  correction) issues no request and answers `SpawnOutcome::SkippedOnReplay`.
- New public items: `IfRunning`, `SpawnOutcome`, `SpawnError`, `SpawnHandle`,
  re-exported from `flezha_runner::procedure`. Example:
  `examples/spawn_bounded_worker.rs`.
- **Wire**: additive `SpawnBoundedRequest` (tag 34) / `SpawnBoundedResponse`
  (tag 35). Requires a controller of the same release.

### Determinism contract + output identity (`deterministic-effects-and-output-identity`)

- **BREAKING: `Clock::now()` removed.** The runtime evaluates the declared
  Clock once per `run` invocation and serves it via `ProcedureCtx::now()`
  (`u64`: epoch ms for `WallClock`, extracted input timestamp for
  `EventTimeFrom<F>` — now fully wired via the new `TimeExtractor<Input>`
  trait — and the durable change seq for `LogicalClock`). The `Procedure`
  trait bound tightened to `type Clock: ClockFor<Self::Input>`, making an
  unservable extractor a compile error; `LogicalClock` with
  `Change = NoChange` fails registration with
  `BuildTopologyError::UnservableClock`.
- **`ProcedureCtx::stable_id()`** — replay-stable 128-bit identifiers
  (frozen FNV-1a-128 over entity identity ‖ change seq ‖ call ordinal);
  the determinism-contract replacement for `Uuid::new_v4()`.
- **Keyed channels**: `.keyed()` on plugin-wired channels +
  `ctx.emit_keyed(key, value)` stamp `flz.entity` / `flz.seq` / `flz.key`
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
  `AddOperative`, `RemOperative`, `EntityState`,
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
