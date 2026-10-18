# Changelog — `flezha_runner` (Rust SDK)

All notable changes to the Rust SDK crate.

## Unreleased

### Changed: a steps class's DAG shape is refused by the core's check

**Breaking.** A tasklet producing two channels, a `chained` loop over a chain
that branches, and a durable `chained` loop whose last tasklet hands nothing on
are refused by the same check every SDK submits to, so `build_spec` answers
`BuildStepsError::Topology` with the codes `topology.multiple_output_channels`,
`topology.branching_chained_loop` and
`topology.chained_loop_terminal_without_handoff`. The variants
`MultipleOutputChannels`, `BranchingChainedLoop` and
`LoopTerminalWithoutHandoff` are removed; match on the error's code instead.

### Changed: `Control::cell_state` takes the controller's selector

**Breaking.** `cell_state(relay_id, cell)` takes `cell: Option<&str>`, the
`<tasklet>[/<cell_key>]` selector the controller reads: `Some("count/home")`
reads one Cell, `Some("count")` a tasklet's Cells, and `None` every Cell of
the relay. A bare Cell key was refused as `state.tasklet_not_found`.

### Added: the runner announces each relay's graph

The runner announces each factory's graph with its API: every channel with its
kind, producer, consumers, plugin end, call target and overflow policy, and every
tasklet with its cell mode, cell keys and mailbox capacity. Nothing in the
crate's API changes; `flz relay graph <relay_id>` prints it.

### Changed: plugins are enabled by name and bound to relays

**Breaking.** `Admin::enable_plugin(name, &PluginSpec)` enables a plugin by
name, of a provider and on its options, with no relay; `PluginSpec::bind`
binds one in the same call. `Admin::bind_plugin` and `Admin::unbind_plugin`
are new, `disable_plugin` takes the plugin's name, and `list_plugins` answers
the plugins enabled in the namespace.

### Added: `Stream::call` and `Response`

A chain ends on a call to another relay with
`.call::<Answer>(CallTarget::id("pricing") | CallTarget::factory("agent"), deadline_ms)`,
answering an `EntryStream<Response<T, Answer>>` on `<channel>.response`. The call
end keeps its chain's transform and key, and each request leaves stamped with
the call's facts. A `:once` or `:chained` steps class that calls is refused
`BuildStepsError::CallOutsidePerInputLoop`.

### Removed: the `sealed()` terminator and `Channel<T>`

**Breaking.** `Stream::sealed()` and the `Channel<T>` handle it returned are
removed, with no replacement. A channel is terminal by having no further
wiring: end the chain at `sink_named("...")` (or any other `sink` form) and
drop the `.sealed()` call. The declared topology is unchanged.

### Changed: one tasklet form, on the author's own type (`give-rust-one-tasklet-form`)

**Breaking.** A tasklet is the author's own type, `impl Tasklet for T` under
`#[flezha::tasklet(name = "...")]`. The attribute moves from the function to
the `impl` block and generates no type: it writes `NAME`, the settings given as
keys (`keys`, `capacity`, `max_cells`, `idle_ttl_seconds`, `schema_version`,
`min_readable_schema_version`, `compact_after_bytes`, `max_timers_per_cell`,
`logical_clock`), the `*_OVERRIDDEN` flags from the hooks the block writes,
`CLOCK`, a default `init_state`, and `State = ()`, `Change = NoChange`,
`Output = ()`. `run`, `on_timer` and `compensate` take one `Ctx<'_, Self>`,
which reads the state with `state()`, changes it with `change(..)` and emits
values typed as `Output`. `Change<S>` applies itself with `apply_to`; the
tasklet has no `apply_change`. Writing `event_time(&Input) -> u64` makes the
clock event time, and `compensate` is available in the everyday form.
`init_state(cell_key)` loses the restore payload. The short-form attributes
take either a stateless function, as before, or an `impl Tasklet` block, where
they implement the user trait on the author's type; a function with a
`&mut` parameter is refused with `ShortFormStateOnFunction`.
`TaskletTestHarness::replay` folds each change encoded and decoded as the
runtime persists it, and `expect_emits` is typed as `Output`.

Removed: `#[flezha::tasklet]` on a function and its `id`, `init`, `key`,
`emits`, `cells_for`, `idle_ttl` and `clock` keys, `SimpleTasklet`,
`#[derive(State)]` and its `#[journal]` modes, `DerivedState`, the `Clock` and
`Config` associated types, `from_config`, `name()`, `EventTimeFrom`,
`TimeExtractor`, `ClockFor`, `WallClock`, `LogicalClock`, `StateHandle` and
`TaskletCtx`. Saved state is keyed by `NAME`, never by the type, so renaming a
type keeps it. Local saved state whose change type came from `#[derive(State)]`
must be cleared once.

### Changed: a steps relay is declared like a flow relay

**Breaking.** `Relay::tasklets` is renamed `channels`, required as
`Relay::channels` is. The class-level `retry_attempts`,
`retry_backoff` and `barrier_retry_attempts` are deleted with
`BuildStepsError::ZeroRetryBackoff`: a stage's retry ladder is the consuming
edge's `retry_max` / `retry_backoff_ms` (3 attempts from 50 ms by default), and
a commit barrier is re-sent a fixed three times. `build_spec` refuses a
steps relay whose `channels` set `effectively_once`, a checkpoint flag or
`correction_window`, as `EffectivelyOnceOnSteps`, `CheckpointOnSteps` or
`CorrectionWindowOnSteps`; a steps cell writes no effect record. The
runner announces each factory's run with its id, so no start names one, and a
spawn naming a flow factory resolves `SpawnError::FactoryNotSteps`.
`#[flezha::relay]` accepts `each = "once" | "per_input" |
"chained"`. `TaskletError::TimersUnavailable` names itself in its message.

### Changed: a steps relay reads messages on its entry channels

**Breaking.** A steps relay's `init_values` are each tasklet's starting state, as
a flow relay's are; they no longer feed the entry channel. Every loop mode takes
its input as messages through the entrance a flow relay's frame takes: the channel
a message names is resolved (none takes the first entry channel, an undeclared
one is parked), the entry parser and the entry edge's transforms run, and an
unreadable message rides the entry channel's retry ladder to the dead-letter
queue without ending the run. A `once` pass waits for its message, and a
durable one parks while it waits. Several entry channels are allowed, each
starting only its own chain, and a merge may take entry channels as sources:
`BuildStepsError::EntryRoot` and `BuildStepsError::MergeFromEntryChannel`
are deleted. A spawn's payload is an ordinary first message.

### Changed: the short form is a relay declaration

**Breaking.** The single-tasklet short form moves from `#[flezha::tasklet]`
to the attribute `#[flezha::relay]`. It takes a required
`id` (the factory id), an optional `sink` and an optional `input`, and emits two
types: the relay under the function's PascalCase name, which
`register_factory(Echo)` registers, and the tasklet `EchoTasklet` beside it,
named after the `id`, so it can be wired into another relay's topology and
ejecting to the full form keeps every cell's state path. `sink` is optional:
with none, the entry channel feeds the tasklet and no plugin channel is
declared. `#[flezha::tasklet]` declares a tasklet only and refuses `kind`,
`sink`, `input` and `on_full`, which describe the relay's entry channel. The refusals are `SinkWithoutOutput` (a `sink` on a body
with no output) and rustc's conflicting-impl error for a short form given a
hand-written topology body (`ShortFormWithTopology` in the contract).

### Removed: the steps relay `run` body; added `TaskletCtx::complete`

**Breaking.** `Relay::run` and the `StepsCtx` export are deleted
with no alias. A steps relay declares `tasklets`; one that declares none is
refused with `BuildStepsError::NoTasklets`, which replaces
`LoopWithoutChain`. `TaskletCtx::complete(result)` ends a steps instance
once its iteration commits; in a flow relay it fails the input naming
`CompleteOutsideSteps`. The testing harness accepts a completion and records
nothing.

### Changed — one `register_factory`, and the short form covers both runs

**Breaking.** `runner.register_factory(Echo)` takes a relay of either run,
passed as a value; `unregister_factory(Echo)` is its inverse.

Registration is explicit. The `inventory` harvest is gone with the `inventory`
dependency, `AutoRelay` and `register_inventory_relays()`: `start()` no
longer merges linked short forms into the runner's declared set, so a duplicate
factory id is refused at the second `register_factory` instead of being skipped
in silence. `FlezhaError::DuplicateFactoryId`'s display now begins with the
refusal's contract name.

A steps short form is a `once` steps relay wiring its entry channel into the
tasklet and the tasklet to the sink.

`flezha::relay!` now begins with the type it declares —
`flezha::relay! { pub struct Name; id: "...", pipeline: ... }` — and that
type is what the author registers.

### Changed — a spawn and a start take the factory first and an optional id

**Breaking.** `TaskletCtx::spawn_steps(factory_id, payload)` spawns under an id the
controller generates, and `spawn_steps_with(factory_id, payload, SpawnOptions::id(id))`
spawns or delivers under yours, `.if_running(IfRunning::Error)` refusing a running one. A
`SpawnHandle` resolves to a `SpawnResult { id, outcome }`, so the id a spawn ran under is
readable whoever chose it. `RelaySpec::new(factory_id).id(id)` starts from application
code, the id optional, and `Admin::start_relay` answers a `StartResult { id, outcome }`
whose outcome is `Started` or `AlreadyRunning`. The handle and its reaper live in `flz_core`,
re-exported here unchanged in name.

### Removed — `codec()` and `codec_with(...)` on the stream handle (`derive-the-handoff-codec`)

**Breaking.** Both methods are gone from `Stream`. A chain that calls either no
longer compiles; delete the call, nothing replaces it. The runtime attaches the
journal codec itself, where a tasklet's output channel is created, derived
from that tasklet's declared `Output`, so there is no declaration left to
forget.

`Tasklet::Output` and `SimpleTasklet::Output` therefore carry a new bound,
`JournalValue`, satisfied by every `Serialize + DeserializeOwned` type through a
blanket implementation. A tasklet whose output has no serde derives stops
compiling, and the message names the type and says the output is journaled at a
steps relay's commit barrier rather than naming a serde trait the author never
wrote. `Input` and `State` are unchanged.

The bound is unconditional on purpose: whether a class journals is a user's
per-class persistence setting at deploy time, not a fact available where the
code is compiled, so a steps-only bound could not be honest. A stream
relay pays a compile-time requirement and nothing at runtime.

`BuildStepsError::MissingInterTaskCodec` is deleted, and
`LoopTerminalWithoutCodec` becomes `LoopTerminalWithoutHandoff`: a durable
chained loop seeds each iteration from its terminal tasklet's journaled
handoff, and only an inter-tasklet channel carries one.

**Breaking, second:** a durable `Chained` loop whose terminal tasklet's only
output is a plugin sink is refused. The journal codec rides an inter-tasklet
channel and no other kind — the rule `flz_core` has always applied to a host
that ships bytes — so such a loop shipped an empty handoff and a crash-resumed
run read the empty seed as convergence and stopped. It worked here only while
the author declared the codec by hand, and truncated silently in every other
SDK. Take the sink off the terminal's own channel — a producer with no consumer
is a legal channel, and is what a retained loop output already is — or declare
`Each::PerInput`. Wiring another tasklet behind the sink does not help on
its own: it becomes the new terminal and owes the same handoff.

Same postcard bytes as before on every channel that still carries a codec, so a
journal written by an older build re-seeds under this one.


### Added — `SimpleTasklet` and the multi-emit sugar shape (`add-tiered-tasklet-authoring`)

`SimpleTasklet` declares only `Input`, `Output`, `State` plus `name`,
`init_state` and `run`; `impl<T: SimpleTasklet> Tasklet for T` supplies
`Clock = WallClock`, `Config = ()` and `<T::State as DerivedState>::Change`.
Stable Rust has no associated-type defaults, hence a tier rather than defaults
on the trait itself — and the blanket impl means everything downstream still
sees one runtime-facing trait. Implementing `Tasklet` directly stays fully
supported; a type doing both is a coherence error, which is what makes
"explicit wins" a fact rather than a precedence rule to remember.
`impl DerivedState for ()` is new so `type State = ()` is the stateless
spelling in that tier.

`#[flezha::tasklet]` accepts a trailing `ctx: &mut TaskletCtx` parameter
with a unit / `Result<(), _>` return: the body then owns every emit, and
`emits = Type` declares the output type the return value no longer can.
`key = …` alongside it, a value return alongside it, and `emits` without it are
each refused with a named error.

Fixed in passing: the `Result`-returning sugar arms now annotate the body with
the declared return type, so `?` inside it resolves against the author's error
type instead of being left uninferrable.

### Added — `route_latest`, the latest-document router building block (`add-route-latest-building-block`)

`ChannelBuilder::route_latest(documents, traffic, key_fn)` (and
`route_latest_named(.., node_name)`) returns a `Stream<(String, D)>`: every key
seen on `traffic` receives each distinct document exactly once, on that key's own
next traffic item.

A keyed tasklet cannot receive a broadcast — every keying declaration is
total, and inside a partitioned member the computed key is admitted against that
member's key-group range. The block is the bridge, with the safety property that
is easy to get wrong encoded once: it delivers on the key's **own traffic**,
never by fanning out over a remembered key set (which outlives key-group
ownership and would park later deliveries against the partition guard). Cost:
one tick of latency per key, and a key whose traffic has stopped is never
delivered to.

It is sugar — a merge into one packaged single-cell tasklet whose
`{document, version, delivered}` state rides the standard change journal. The
packaged tasklet is deliberately not public: the two methods are the whole
surface, which is what keeps the guarantee un-bypassable. A document equal to the
retained one is inert; a differing one bumps the version. The document is
retained and compared with `Eq`, never parsed.

### BREAKING — one plugin-sink method: `sink` + `sealed` (`unify-plugin-sink-api`)

`also_output(name)` / `also_output_with(name, f)` and the terminal
`output(name)` collapse into ONE method: `sink(name)` / `sink_with(name, f)`,
always returning the stream handle. A channel is the plugin-bound terminal by
having no consumers wired after it, not by which method declared it — the
runtime derives that from the wiring, which is what the two spellings were
encoding by hand. `.sealed()` is the optional terminator, returning the
non-chainable `Channel<T>` the old `output` returned; it is purely a
compile-time statement that nothing follows and changes no topology.

No shim, no coexistence. Migration is mechanical: `also_output_with` →
`sink_with`, `also_output` → `sink`, `output(name)` → `sink(name)` (append
`.sealed()` where the non-chainable return type was relied on). Wire frames,
descriptor field names, plugin bindings and error identifiers are unchanged; a
terminal declaration's frames are byte-identical to what `output` wrote, and a
unit fixture holds both shapes against those literals.

Consequences worth knowing: `ChannelDescriptor` lost `plugin_name` (dead) and
`plugin_serializer` — `plugin_sink` is now the one representation for both
shapes. A sink declared on a channel that a *later* statement also wires a
consumer into is now correctly hybrid; before, the terminal `output` had
already frozen the kind and those consumers silently received nothing.
`.on_full(...)` after `sink(...)` compiles now (it did not after `output`) and
is still never consulted on a channel with no mailbox — `.sealed()` refuses it
as before.

### A keyed channel's plain emit carries the Cell's own key (`default-emit-to-cell-key`)

In a keyed Cell the natural key of essentially every output *is* the key its
input routed by, and the SDK already holds it — it created the Cell from it.
`ctx.emit(value)` / `ctx.emit_with_meta(value, meta)` on a `.keyed()` channel
now emit under that key instead of failing with `KeyRequiredOnKeyedChannel`, so
`flz.key` equals the routing key by construction — the tasklet never
re-derives it, and the two cannot drift apart. Both spellings share one
stamping path, so a defaulted emit and `ctx.emit_keyed(key, value)` with the
same key are byte-identical.

- `ctx.cell_key() -> Option<&str>` — the key this Cell was routed by, the
  one `init_state` was handed. `None` for a single-cell Tasklet.
- `ctx.emit_keyed` is unchanged: it is now for the output whose natural key
  differs from its Cell's.
- `TaskletTestHarness::with_cell_key(key)` stands the harness in for that
  Cell, so a test observes the defaulted stamp.

Unchanged refusals: a single-cell Tasklet's key-less emit on a keyed
channel (there is no Cell key to default to) and `emit_keyed` on a channel
not declared `.keyed()`. Every currently-legal call keeps its behaviour.

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
  `state_sink::note_transport_failure()`. This is the only recovery signal a
  Relay that writes no state changes has — the flood ladder alone never
  fires for it.

New env knob: `FLEZHA_RECONNECT_REQUEST_RETRY_MS`. New `#[doc(hidden)]` test
support on `state_sink`: `reset_for_test`, `reconnect_requested_for_test`.

### The kernel moved into `flz_core` (`extract-sdk-poll-core`)

- **Public API unchanged** (`public-api.txt`: internal-re-export note). The
  type-erased kernel — cell slots, mailbox/drain gates, flusher, credit,
  DLQ park/ladder, timers, routing, commands, steps driver, transport
  loop — now lives in the `flz_core` crate; this crate's typed layer
  consumes it as the first poll host. Dispatch runs on dedicated poll
  workers (`poll() → run one attempt per item → complete`); admission,
  late-input classification, event-time due-timer generation, stale-timer
  validation and the retry→park ladder are engine-side. Event-time-clocked
  classes are handed one admitted input per activation (chained due-timer
  determinism); wall/logical classes batch freely.
- **Renamed (internal surfaces)**: `HeaderTemplate::for_tasklet::<S>()` →
  `cell::header::header_template_for::<S>()`; `TimerPolicy::
  for_tasklet::<S>()` → `cell::timer::timer_policy_for::<S>()`
  (inherent impls cannot span crates). `TaskletDispatcher::dispatch` →
  `dispatch_item` (single attempt, disposition-driven).

### Live Cell read (`cell-state-live-query`)

- **The `CellState` command gained a selector**, so the state a user asks
  for is the only state serialized: `CellState { relay_id, tasklet,
  cell_key }`, where an empty string means absent. No selector is the
  whole-Relay migration/handover shape and encodes byte-identically to
  before. `cell_key` without `tasklet` is refused — a Cell key is unique
  only within its Tasklet.
- **Dynamic-mode Cells are serialized**, keyed by whatever the live registry
  holds. The whole-Relay shape previously omitted them entirely.
- **`command::cell_state::serialize_selected_state`** is the public entry
  point: it returns `(json, Option<seq>)`, the seq present only when the selector
  named exactly one Cell. `serialize_relay_state` keeps its signature and
  is now that call with an empty selector. Refusal strings lead with a canonical
  token (`cell_key_without_tasklet`, `tasklet_not_found`,
  `cell_not_found`, `cell_evicted`).
- **Serialization takes the Cell's state lock and does NOT drain pending
  changes**, so the value returned includes changes not yet flushed to the WAL —
  the read reports `flz.change_seq` on the reply frame's metadata rather than
  hiding that. It triggers no compaction, snapshot or truncation.
- **`CommandAction` gained a defaulted `reply`** returning
  `(payload_id, raw_data, HashMap<String, String>)` — the frame metadata a
  command's answer stamps. Every handler but `CellState` inherits the default,
  which forwards to `handle` with no metadata, so their frames are unchanged.
- **`DynamicCellRegistry::recently_evicted`** exposes a bounded (1024-key)
  trace of what the reaper evicted, so a read can answer "idle-evicted" rather
  than "never existed". Beyond the cap it degrades to the latter, which is the
  safe direction.
- Example: `sdk/rust/examples/live_state_read.rs`. Docs:
  `docs/internal/state_persistence.md`.

### Stream transforms and edge keying (`stream-dsl-keyby-and-transforms`)

- **BREAKING — `Tasklet::cell_key` and `const CELL_KEY_OVERRIDDEN` are
  removed.** Dynamic Cell keying is declared on the wiring instead:
  `.key_by(|v: &T| ..)` on the stream that feeds the tasklet. There is no
  shim, so every dynamic-mode Tasklet is a compile error until its key moves
  to the edge — which is how the compiler enumerates the migration sites. The
  static pair (`cell_keys` / `cells_for`) and the dynamic knobs
  (`cell_idle_ttl`, `max_cells`) are unchanged. A former `None` return
  becomes a `.filter(..)` declared before the keying; the
  `inputs_dropped_no_key_total` counter is retired with the path it measured.
- **`map` / `filter` / `flat_map` / `key_by` on both stream handles.** They add
  no Tasklet, Cell, mailbox or dispatch hop: every transform declared
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
  ladder. On a steps chain it becomes a terminal chain error the saga
  compensates, so the instance still reports.
- New `TopologyIssue` variants `KeyByOnUnkeyableTarget`, `DuplicateKeyBy`,
  `TransformAfterTerminal`, `TransformAfterKeyBy`, `DanglingTransformChain`,
  `UnkeyedEdgeIntoDynamicTasklet`; `StaticAndDynamic` is removed (the
  contradiction it named is now `KeyByOnUnkeyableTarget`) and
  `MergeSourceAlreadyWired` is renamed `EdgeAlreadyWired` — it now guards any
  re-declaration where either side carries a transform or a key.

### Multi-input tasklets (`add-multi-input-tasklets`)

- **`ChannelBuilder::merge::<I>()`** — subscribe two or more source channels to
  ONE tasklet whose `Input` is an author-declared sum type, each `.from(source,
  map)` supplying the mapping from that channel's payload into it:
  `c.merge::<SignalInput>().from(ticker, SignalInput::Ticker).from(config,
  SignalInput::Config).into::<Detector>()`. Entry streams and inter-tasklet
  streams are both valid sources on a flow relay (`MergeSource`,
  implemented for `EntryStream` and `Stream`), freely mixed; see the steps relay
  restriction below. Single-input `.into` is unchanged.
- **Purely a subscription-time construct**: `Tasklet` is untouched, so
  dispatch, state, `cells_for`, retry/DLQ and the emit APIs all see one type.
  `cells_for` / `cell_key` are evaluated on the **mapped** value.
- **A merge mapping is contained on the steps driver thread.** It is the only
  author closure on a steps chain running outside a cell invocation, so
  `extend_mapped` catches its unwind and reports it as a terminal chain error, and
  the instance still reports. An unguarded one would skip
  `emit_relay_completed` and strand it. At the **forward** accumulate site the
  saga unwinds the stages recorded as completed — the producer whose edge blew
  up among them, since the mapping runs after that stage's record and commit —
  and Java unwinds the same set in the same order; a sibling still in flight in
  the same level is not unwound, as for any other mid-level abort. At the
  **crash-resume re-seed** site Rust also unwinds the journaled stages (the
  resumed iteration's completed list is seeded from the journal); the Java arm
  does not, a pre-existing gap in its resumed chains recorded under "Known
  limitation" in `docs/internal/sdk/multi_input_tasklets.md`. `Merge` is `#[must_use]`: a merge left unclosed by
  `.into` declares no edges at all.
- **Per-source FIFO is preserved; nothing is ordered across sources** — the
  guarantee two independent consumers already had. Mappings MUST be pure: they
  run on live delivery and again on WAL replay, the same contract entry parsers
  carry.
- New `TopologyIssue` variants `MergeTooFewSources`, `DuplicateMergeSource` and
  `EdgeAlreadyWired` (a source may not re-wire an edge `.into` or another
  merge already declared — the mapping would otherwise be silently dropped); a
  mapped type that is not the tasklet's `Input` is a compile error at
  `.into::<Task>()`.
- Merges accept entry channels as sources, in a stream and in a steps relay.
- `ChannelDescriptor::consumers` is now `Vec<ConsumerEdge>` (tasklet id plus
  the optional mapping); `consumer_ids()` is the id-only view.
- No wire change and no controller change. Example: `examples/merged_input.rs`.

### Tasklet-requested spawn (`tasklet-requested-spawn`)

- **`TaskletCtx::spawn_steps(instance_id, factory_id, payload, if_running)`**
  — spawn-or-deliver a steps instance from inside any tasklet. A free id
  starts `factory_id` with `payload` as its first input; a taken id receives it
  as one more input (`IfRunning::Deliver`) or reports
  `SpawnError::AlreadyRunning` (`IfRunning::Error`). Always in the requester's
  own namespace.
- **Fire-and-forget with a result**: returns a `SpawnHandle` immediately. Every
  failure resolves that handle rather than the tasklet's `run` — `wait()` /
  `try_result()` read it, and a handle dropped unobserved reports a failure to
  the log via one process-wide reaper thread.
- **Live processing only**: a replayed invocation (crash resume, late-input
  correction) issues no request and answers `SpawnOutcome::SkippedOnReplay`.
- New public items: `IfRunning`, `SpawnOutcome`, `SpawnError`, `SpawnHandle`,
  re-exported from `flezha_runner::tasklet`. Example:
  `examples/spawn_steps_worker.rs`.
- **Wire**: additive `SpawnStepsRequest` (tag 34) / `SpawnStepsResponse`
  (tag 35). Requires a controller of the same release.

### Determinism contract + output identity (`deterministic-effects-and-output-identity`)

- **BREAKING: `Clock::now()` removed.** The runtime evaluates the declared
  Clock once per `run` invocation and serves it via `TaskletCtx::now()`
  (`u64`: epoch ms for `WallClock`, extracted input timestamp for
  `EventTimeFrom<F>` — now fully wired via the new `TimeExtractor<Input>`
  trait — and the durable change seq for `LogicalClock`). The `Tasklet`
  trait bound tightened to `type Clock: ClockFor<Self::Input>`, making an
  unservable extractor a compile error; `LogicalClock` with
  `Change = NoChange` fails registration with
  `BuildTopologyError::UnservableClock`.
- **`TaskletCtx::stable_id()`** — replay-stable 128-bit identifiers
  (frozen FNV-1a-128 over cell identity ‖ change seq ‖ call ordinal);
  the determinism-contract replacement for `Uuid::new_v4()`.
- **Keyed channels**: `.keyed()` on plugin-wired channels +
  `ctx.emit_keyed(key, value)` stamp `flz.cell` / `flz.seq` / `flz.key`
  on every plugin frame; the `flz.` metadata namespace is reserved
  (`ReservedMetadataKey`). Non-keyed channels are byte-identical to before
  (criterion dispatch regression bench: no change detected).
- **Harness**: `replay_emits_equal` (output-side determinism gate),
  injectable fixed/scripted wall clock, `with_keyed_channel`,
  `emitted_meta`.

### Performance

- **Dispatch core rewritten behind the `fast-dispatch` cargo feature.** The
  single-coordinator dispatch loop is replaced with per-relay lock-free
  mailboxes (`crossbeam::queue::SegQueue` + `AtomicBool inflight`) drained
  by a Chase-Lev work-stealing pool (built on `crossbeam-deque`). The live
  relay registry is backed by `DashMap` instead of
  `RwLock<HashMap>`. Per-message dispatch-overhead drops from ~9 to ≤3
  synchronization operations on the hot path.
  - Criterion dispatch micro-bench (100 relays × 10k messages, pure
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

- Sticky per-relay ordering under `RelayRunner::ThreadPool`: two
  invocations of the same relay's `process(...)` never overlap, inputs
  are observed in arrival order.
- `RelayRunner::ThreadSpawn`: per-invocation OS thread spawn retained.
- Reconnect semantics: exponential backoff (200 ms → 30 s cap), state
  reset on unexpected drop, no reconnect on `RunnerDisconnect`.
- Command dispatch (`AssignRunnerId`, `ConnectSocket`,
  `AddRelay`, `RemRelay`, `CellState`,
  `MigrationSerialization`, `MigrationDeserialization`,
  `RunnerDisconnect`) produces byte-identical `CommandResponse`
  bytes.
- Public API surface unchanged: `new`, `Flezha`, `register_factory`,
  `RelayHandler`, `RelayInput` / `RelayOutput` /
  `RelayResult`, `RelaySettings`, `FactoryMetadata` keep the same
  signatures.

### Added (internal)

- `runner/mailbox.rs`, `runner/work_stealing_pool.rs`,
  `runner/fast.rs` — the fast-dispatch internals.
- `relay::factory_registry::RelayRegistry` /
  `RelaySlot` — DashMap-backed live registry.
- `benches/dispatch.rs` — Criterion throughput + latency micro-benches.
- `benches/correctness.rs` — contention-adder, sticky-ordering, thread-spawn
  invariant checks.
- `tests/loom_mailbox.rs` — loom model check over the mailbox CAS dance
  (`cfg(loom)` gated).

### Dependencies

- Added `dashmap` (optional, under `fast-dispatch`).
- Added `crossbeam-deque` (optional, under `fast-dispatch`).
- Added `criterion` as `[dev-dependencies]`.
