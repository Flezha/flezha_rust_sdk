# Changelog — `flezha_runner` (Rust SDK)

All notable changes to the Rust SDK crate.

## Unreleased

### BREAKING — the operative kinds are `stream` and `workflow`

<!-- kind-naming-record:begin -->
`UnboundedOperative` → `StreamOperative` and `BoundedOperative` →
`WorkflowOperative`, with the whole family: `BoundedError` → `WorkflowError`,
`BoundedCtx` → `WorkflowCtx`, `BuildBoundedError` → `BuildWorkflowError`,
`UnboundedInstance` → `StreamInstance`, `BoundedInstance` → `WorkflowInstance`,
`FlezhaError::InvalidBounded` → `InvalidWorkflow`, module `bounded` → module
`workflow`. Entry points: `define_unbounded`/`define_bounded` →
`define_stream`/`define_workflow`, `remove_unbounded`/`remove_bounded` →
`remove_stream`/`remove_workflow`, `ctx.spawn_bounded` → `ctx.spawn_workflow`.
Env vars `FLEZHA_BOUNDED_{DRIVER_STACK_KIB,PARK_IDLE_MS,RETRY_FAILS}` →
`FLEZHA_WORKFLOW_*`.

No shim, no dual acceptance: the old names are gone. Nothing about execution,
durability or the wire tag numbers changes. The old words were Flink's terms
for a property of the *data*, while the distinction here is whether one run
terminates — and `bounded` was false for a durable `PerInput` operative, whose
input is unbounded and whose *iteration* is the only bounded thing. The channel
handle `Stream<'b, T, K>` is unrelated and unchanged; a stream operative is
precisely the kind that consumes streams.
<!-- kind-naming-record:end -->

### Entry-input idle signal (`expose-entry-input-idle-signal`)

The SDK owns the IO threads, so when an input goes quiet the application sees
nothing: procedures simply stop being called. Two additions on `FlezhaRunner`
close that blind spot without deciding anything for the application.

- `last_entry_delivery(channel) -> Option<Instant>` — when a frame last reached
  that entry channel. Absent (never a start-time value) before the first frame,
  and for a name no declared operative has.
- `on_entry_idle(channel, threshold, observer) -> &Self` — legal before AND
  after `start()`. `observer` is any `IdleObserver`; a bare
  `Fn(Duration)` closure is one, and `resumed` defaults to a no-op.

The observer fires **once per idle episode**, when the gap since the last frame
first exceeds `threshold`, and again (as `resumed`) when a frame ends it,
re-arming the latch. Silence before the channel's first frame ever is not an
episode — an operative waiting for its subscriptions to take is not an
incident. Evaluation runs on one background tick per runner
(`threshold / 4`, clamped to `[250 ms, 1 s]`), never on the delivery path,
where the whole cost is one relaxed atomic store; a panicking observer is
contained and logged on `flezha::entry_idle`.

No default threshold and no default action ship: the *signal* is the SDK's, the
*policy* is the application's. `docs/entry_input_observability.md`.

### A dead secondary stream can no longer stall a runner permanently and silently

Two defects in `flz_core` let a runner whose secondary (state-write) stream had
died sit unrecovered indefinitely while its control channel stayed healthy: the
controller flags `output_stall`, the process looks alive from every angle, and
nothing is ever produced again. Both are fixed at the transport seam; no public
API, wire or persistence change.

- **The reconnect request re-arms.** `state_sink`'s broken-pipe flood set a
  `reconnect_requested` latch that only `install_global_sink` cleared. If the
  requested `Reconnect` never landed, **no further reconnect was ever
  requested** and every subsequent state change was silently absorbed by the
  1024-frame ring for the life of the process. The latch is now
  `reconnect_requested_at` and re-arms after
  `FLEZHA_RECONNECT_REQUEST_RETRY_MS` (default 30 000). Re-armed requests
  re-enter the existing warn, so a permanently dead secondary reports its ring
  depth every retry window instead of logging one line and going quiet.
- **An undelivered data heartbeat is reported.** `send_data_heartbeat` iterated
  the stream map with `let _ = write_all(..)`, so an empty map or an all-failed
  write was a silent no-op. It now returns whether it reached any stream, and
  `heartbeat_tick` feeds a miss into the same flood ladder via
  `state_sink::note_transport_failure()`. This is the only recovery signal an
  Operative that writes no state changes has — the flood ladder alone never
  fires for it.

New env knob: `FLEZHA_RECONNECT_REQUEST_RETRY_MS`. New `#[doc(hidden)]` test
support on `state_sink`: `reset_for_test`, `reconnect_requested_for_test`.

### The kernel moved into `flz_core` (`extract-sdk-poll-core`)

- **Public API unchanged** (`public-api.txt`: internal-re-export note). The
  type-erased kernel — entity slots, mailbox/drain gates, flusher, credit,
  DLQ park/ladder, timers, routing, commands, workflow driver, transport
  loop — now lives in the `flz_core` crate; this crate's typed layer
  consumes it as the first poll host. Dispatch runs on dedicated poll
  workers (`poll() → run one attempt per item → complete`); admission,
  late-input classification, event-time due-timer generation, stale-timer
  validation and the retry→park ladder are engine-side. Event-time-clocked
  classes are handed one admitted input per activation (chained due-timer
  determinism); wall/logical classes batch freely.
- **Renamed (internal surfaces)**: `HeaderTemplate::for_procedure::<S>()` →
  `entity::header::header_template_for::<S>()`; `TimerPolicy::
  for_procedure::<S>()` → `entity::timer::timer_policy_for::<S>()`
  (inherent impls cannot span crates). `ProcedureDispatcher::dispatch` →
  `dispatch_item` (single attempt, disposition-driven).

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
  ladder. On a workflow chain it becomes a terminal chain error the saga
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
  streams are both valid sources on a stream operative (`MergeSource`,
  implemented for `EntryStream` and `Stream`), freely mixed; see the workflow
  restriction below. Single-input `.into` is unchanged.
- **Purely a subscription-time construct**: `Procedure` is untouched, so
  dispatch, state, `entities_for`, retry/DLQ and the emit APIs all see one type.
  `entities_for` / `entity_key` are evaluated on the **mapped** value.
- **A merge mapping is contained on the workflow driver thread.** It is the only
  author closure on a workflow chain running outside an entity invocation, so
  `extend_mapped` catches its unwind and reports it as a terminal chain error, and
  the instance still reports. An unguarded one would skip
  `emit_workflow_completed` and strand it. At the **forward** accumulate site the
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
- **Workflow merges consume inter-procedure channels only.** The workflow seed is
  a bare payload naming no channel, so an entry channel as one merge source
  among several is refused at registration with
  `BuildWorkflowError::MergeFromEntryChannel` rather than accepted and failing
  every invocation. Stream merges accept entry channels.
- `ChannelDescriptor::consumers` is now `Vec<ConsumerEdge>` (procedure id plus
  the optional mapping); `consumer_ids()` is the id-only view.
- No wire change and no controller change. Example: `examples/merged_input.rs`.

### Procedure-requested spawn (`procedure-requested-spawn`)

- **`ProcedureCtx::spawn_workflow(instance_id, factory_id, payload, if_running)`**
  — spawn-or-deliver a workflow instance from inside any procedure. A free id
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
  `examples/spawn_workflow_worker.rs`.
- **Wire**: additive `SpawnWorkflowRequest` (tag 34) / `SpawnWorkflowResponse`
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
