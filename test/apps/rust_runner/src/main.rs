use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::{env, io, thread};

use flezha_runner::procedure::{
    EventTimeFrom, Procedure, ProcedureCtx, ProcedureError, TimeExtractor,
};
use flezha_runner::state::{
    Change, CoalesceKey, NoChange, RestoreInitPayload, StateHandle, WallClock,
};
use flezha_runner::{
    ChannelBuilder, Flezha, LoopMode, OverflowPolicy, StreamOperative, WorkflowCtx, WorkflowError,
    WorkflowOperative,
};
use serde::{Deserialize, Serialize};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    spawn_stdin_eof_exit();
    Flezha::create_runner(namespace())?
        .define_stream::<DoNothingOperative>()?
        .define_stream::<StoreInputOperative>()?
        .define_stream::<CompareFilterOperative>()?
        .define_stream::<StaticEntityOperative>()?
        .define_stream::<EntityStressOperative>()?
        .define_stream::<BoundedMailboxBlockOperative>()?
        .define_stream::<BoundedMailboxDropNewestOperative>()?
        .define_stream::<BoundedMailboxDropOldestOperative>()?
        .define_stream::<StatefulCounterOperative>()?
        .define_stream::<CoalescingCounterOperative>()?
        .define_stream::<PersistenceOffOperative>()?
        .define_stream::<HybridChannelOperative>()?
        .define_stream::<ResumeProbeOperative>()?
        .define_stream::<DynamicEntityOperative>()?
        .define_stream::<PartitionRelayOperative>()?
        .define_stream::<SessionTimeoutOperative>()?
        .define_stream::<MixedOperative>()?
        .define_stream::<TierStoreOperative>()?
        .define_stream::<CheckpointStoreOperative>()?
        .define_stream::<PoisonProbeOperative>()?
        .define_stream::<WedgeProbeOperative>()?
        .define_stream::<CandleOperative>()?
        .define_workflow::<EchoWorkflow>()?
        .define_workflow::<CommitBlockWorkflow>()?
        .define_workflow::<SyncDurableWorkflow>()?
        .define_workflow::<OptOutCommitBlockWorkflow>()?
        .define_workflow::<TierCommitBlockWorkflow>()?
        .define_workflow::<SagaBlockWorkflow>()?
        .define_workflow::<OutboxBlockWorkflow>()?
        .define_workflow::<UndoOutboxWorkflow>()?
        .define_workflow::<ChatLoopWorkflow>()?
        .define_workflow::<StatelessLoopWorkflow>()?
        .define_workflow::<ChatRewindWorkflow>()?
        .define_workflow::<RetryWorkflow>()?
        .define_workflow::<BranchWorkflow>()?
        .define_workflow::<DiamondWorkflow>()?
        .run()?;
    Ok(())
}

fn namespace() -> String {
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        return args[1].clone();
    }
    "default".to_string()
}

/// The Elixir e2e suite manages this app as an Erlang Port and terminates it
/// by closing the port (= our stdin). Exit on EOF so `Port.close/1` reliably
/// tears the process down; keep-alive itself is `run()`'s job.
fn spawn_stdin_eof_exit() {
    thread::spawn(|| {
        let mut buf = [0u8; 64];
        loop {
            match io::stdin().read(&mut buf) {
                Ok(0) | Err(_) => std::process::exit(0),
                Ok(_) => {}
            }
        }
    });
}

// -- EchoWorkflow -----------------------------------------------------------------
//
// Run-once workflow operative exercised by the `workflow-kind` e2e: emits its args back as
// output once, then returns Ok so the SDK ships `TaskCompleted` and the
// controller deregisters it.
struct EchoWorkflow;

impl WorkflowOperative for EchoWorkflow {
    fn factory_id() -> &'static str {
        "echo_workflow"
    }

    fn run(ctx: &mut WorkflowCtx, args: &str) -> Result<(), WorkflowError> {
        ctx.emit(format!("echo:{args}").into_bytes())?;
        Ok(())
    }
}

// -- Durable structured workflow operatives (crash-resume e2e, §7c.3) ------------
//
// `commit_block_workflow` and `saga_block_workflow` are durable-by-default,
// multi-procedure workflow operatives that BLOCK mid-chain on the
// `FLEZHA_WORKFLOW_E2E_RELEASE` env var so a SIGKILL of the runner lands with
// earlier procedures' Entity state already committed to the WAL. The resumed
// runner is launched WITH the env set, so the respawned instance restores the
// committed state (crash-resume) and runs to completion. A shared gate keeps the
// crash window deterministic — no busy-spin, no wall-clock races.

const E2E_RELEASE_ENV: &str = "FLEZHA_WORKFLOW_E2E_RELEASE";

fn e2e_released() -> bool {
    env::var(E2E_RELEASE_ENV).is_ok()
}

// Park a pool worker (never the driver thread) until the release env is set.
fn block_until_released() {
    while !e2e_released() {
        thread::sleep(Duration::from_millis(20));
    }
}

// SET-style Entity state: `apply` is idempotent, so re-running a procedure
// from the same args on resume is a no-op over the restored state (Model A
// idempotent re-run). `BTreeSet` keeps snapshot bytes deterministic.
#[derive(Serialize, Deserialize, Clone, Default)]
struct SeenState {
    items: std::collections::BTreeSet<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct Insert(String);
impl Change for Insert {}

// A durable `String -> String` step: commits one idempotent Change to the WAL
// (keyed by the proc name, so a resume re-run is a no-op) and passes the input on.
// With `compensate: |ctx| ...` it declares a saga hook (the given body is the
// hook, returning `Result`; naming the ctx lets an undo emit its own record);
// the `commit_a` form omits it so a clean crash-resume re-runs forward instead
// of undoing.
macro_rules! durable_step {
    ($ty:ident, $name:literal $(, compensate: |$ctx:ident| $comp:expr)?) => {
        #[derive(Default)]
        struct $ty;
        impl Procedure for $ty {
            type Input = String;
            type Output = String;
            type State = SeenState;
            type Change = Insert;
            type Clock = WallClock;
            type Config = ();
            fn name() -> &'static str {
                $name
            }
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SeenState {
                SeenState::default()
            }
            fn apply_change(state: &mut SeenState, change: &Insert) {
                state.items.insert(change.0.clone());
            }
            fn run(
                &mut self,
                input: String,
                mut state: StateHandle<'_, Self>,
                ctx: &mut ProcedureCtx<'_>,
            ) -> Result<(), ProcedureError> {
                state.apply(Insert($name.to_string()));
                ctx.emit::<String>(input)
            }
            $(
                // A crash-resumed compensation reconstructs completed stages with
                // EMPTY outputs (a procedure's collected output was never
                // durable), so a hook that records an undo does so unconditionally.
                const COMPENSATE_OVERRIDDEN: bool = true;
                fn compensate(
                    &mut self,
                    _out: Vec<std::sync::Arc<String>>,
                    _state: StateHandle<'_, Self>,
                    $ctx: &mut ProcedureCtx<'_>,
                ) -> Result<(), ProcedureError> {
                    $comp
                }
            )?
        }
    };
}

// A stateless terminal `String` step whose whole behaviour is `$run` (ignores
// input + ctx): either blocks the chain or fails it.
macro_rules! stateless_step {
    ($ty:ident, $name:literal, $run:expr) => {
        #[derive(Default)]
        struct $ty;
        impl Procedure for $ty {
            type Input = String;
            type Output = String;
            type State = ();
            type Change = NoChange;
            type Clock = WallClock;
            type Config = ();
            fn name() -> &'static str {
                $name
            }
            fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
            fn run(
                &mut self,
                _input: String,
                _: StateHandle<'_, Self>,
                _ctx: &mut ProcedureCtx<'_>,
            ) -> Result<(), ProcedureError> {
                $run
            }
        }
    };
}

// `commit_block_workflow`: proc A commits durable state, proc B blocks until the
// resumed runner sets the release env — so a SIGKILL lands with proc A's state
// already on the WAL and proc B never finished. Completion is observed
// controller-side via the finished-job record.
durable_step!(CommitA, "commit_a");
stateless_step!(BlockB, "block_b", {
    block_until_released();
    Ok(())
});

struct CommitBlockWorkflow;
impl WorkflowOperative for CommitBlockWorkflow {
    fn factory_id() -> &'static str {
        "commit_block_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .named("mid")
            .codec()
            .into::<BlockB>();
    }
}

// `sync_durable_workflow`: the `commit_block_workflow` shape declaring
// SYNCHRONOUS remote durability (`remote-durability-tier`). The controller must
// have proc A's journal barrier on the durability backend before it acks the
// commit — which is what lets the kill-the-disk drill destroy the whole state
// directory and still resume with proc A committed rather than re-run.
struct SyncDurableWorkflow;
impl WorkflowOperative for SyncDurableWorkflow {
    const SYNC_DURABILITY: bool = true;
    fn factory_id() -> &'static str {
        "sync_durable_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .named("mid")
            .codec()
            .into::<BlockB>();
    }
}

// `optout_commit_block_workflow`: the opt-out twin of `commit_block_workflow`
// with a live mid-chain signal — proc A's output also reaches a plugin sink, so
// the crash-resume e2e can observe the mid-run point without durable state
// (there is none). A crash + resume re-runs the whole chain from the top, so
// the sink sees proc A's marker once per run and no journal/WAL file exists.
struct OptOutCommitBlockWorkflow;
impl WorkflowOperative for OptOutCommitBlockWorkflow {
    fn factory_id() -> &'static str {
        "optout_commit_block"
    }
    const PERSISTENCE_ENABLED: bool = false;
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .named("mid")
            .also_output_with("optout_sink", |s: &String| {
                format!("a-ran:{s}").into_bytes()
            })
            .into::<BlockB>();
    }
}

// `tier_commit_block_workflow`: the effectively-once workflow fixture. Proc A
// commits durable state AND emits `committed:<input>` under a stable key on a
// keyed hybrid channel (captured into the atomic effect record); proc B blocks
// until the resumed runner sets the release env. A SIGKILL lands with A's
// effect record durable and B unfinished; the resume re-runs A (idempotent
// change, deterministic re-emit) and completes.
#[derive(Default)]
struct TierCommitA;
impl Procedure for TierCommitA {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tier_commit_a"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SeenState {
        SeenState::default()
    }
    fn apply_change(state: &mut SeenState, change: &Insert) {
        state.items.insert(change.0.clone());
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(Insert("tier_commit_a".to_string()));
        ctx.emit_keyed::<String>("job".to_string(), format!("committed:{input}"))
    }
}

struct TierCommitBlockWorkflow;
impl WorkflowOperative for TierCommitBlockWorkflow {
    fn factory_id() -> &'static str {
        "tier_commit_block_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.effectively_once();
        c.input::<String>("in")
            .into::<TierCommitA>()
            .named("mid")
            .codec()
            .keyed()
            .also_output_with("tier_sink", |s: &String| s.as_bytes().to_vec())
            .into::<BlockB>();
    }
}

// `saga_block_workflow`: SA + SB commit durable state and compensate; SC fails,
// triggering reverse-order rollback (SB then SA). SA's compensate BLOCKS so a
// SIGKILL lands mid-rollback with SB's done-marker already durable — the respawn
// finishes SA and SKIPS the already-done SB.
durable_step!(SagaA, "saga_a", compensate: |_ctx| {
    block_until_released();
    Ok(())
});
durable_step!(SagaB, "saga_b", compensate: |_ctx| Ok(()));
stateless_step!(
    SagaC,
    "saga_c",
    Err(ProcedureError::Custom("saga_c always fails".into()))
);

struct SagaBlockWorkflow;
impl WorkflowOperative for SagaBlockWorkflow {
    fn factory_id() -> &'static str {
        "saga_block_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SagaA>()
            .named("s1")
            .codec()
            .into::<SagaB>()
            .named("s2")
            .codec()
            .into::<SagaC>();
    }
}

// `outbox_block_workflow`: the transactional-outbox fixture
// (`workflow-staged-commit-and-outbox`). Proc A commits durable state AND emits
// plugin-bound output on a hybrid channel — STAGED, so it reaches the sink only
// after the controller has journaled and fsynced A's `proc_committed` entry (with
// the output inside it) and flushed the outbox. Proc B blocks, so a SIGKILL lands
// with A committed + flushed and B unfinished.
struct OutboxBlockWorkflow;
impl WorkflowOperative for OutboxBlockWorkflow {
    fn factory_id() -> &'static str {
        "outbox_block_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .named("mid")
            .codec()
            .also_output_with("outbox_sink", |s: &String| {
                format!("a-out:{s}").into_bytes()
            })
            .into::<BlockB>();
    }
}

// `undo_outbox_workflow`: the compensation half of the transactional outbox. Proc
// UA commits, emits its forward output on a hybrid plugin channel, and declares
// a `compensate` hook that EMITS an undo record — plugin-bound, so it is staged
// and rides a `compensated` journal entry rather than the wire. Proc C fails
// immediately, so the chain always rolls back; the hook BLOCKS until the release
// env is set, so on the first runner the undo is emitted-but-never-journaled
// when the SIGKILL lands — proving staged undo output cannot leak. The resumed
// runner re-runs the outstanding hook (its done marker never landed) and the
// undo is delivered exactly once, under its comp-marked key.
durable_step!(UndoOutboxA, "undo_outbox_a", compensate: |ctx| {
    block_until_released();
    ctx.emit::<String>("undone".to_string())
});

stateless_step!(
    UndoC,
    "undo_outbox_c",
    Err(ProcedureError::Custom("undo_outbox_c always fails".into()))
);

struct UndoOutboxWorkflow;
impl WorkflowOperative for UndoOutboxWorkflow {
    fn factory_id() -> &'static str {
        "undo_outbox_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<UndoOutboxA>()
            .named("mid")
            .codec()
            .also_output_with("undo_sink", |s: &String| format!("out:{s}").into_bytes())
            .into::<UndoC>();
    }
}

// `branch_workflow`: the procedure TREE (`workflow-branching`).
//
//     BranchA ─┬─▶ BranchB ─▶ BranchD
//              └─▶ BranchC
//
// Every procedure commits durable state and compensates by emitting an undo
// record, so both tree behaviours are observable at one sink:
//
//   * crash-resume — BranchD BLOCKS until the resumed runner sets the release
//     env, so the SIGKILL lands with A, B and C committed and only D's subtree
//     outstanding. The respawn re-runs D alone, seeded from B's journaled edge;
//     the sibling branch's `c:` marker must therefore appear exactly ONCE across
//     both runs.
//   * compensation — with `FLEZHA_BRANCH_FAIL` armed, BranchD fails instead of
//     blocking. A, B and C have all completed, so the rollback must unwind the
//     two branches before the root they share: `undo-c`, `undo-b`, `undo-a`.
//
// The inter-proc channels are hybrid (`also_output_with`), which is what makes
// a non-leaf procedure's forward and undo output visible at the sink.
const BRANCH_FAIL_ENV: &str = "FLEZHA_BRANCH_FAIL";

durable_step!(BranchA, "branch_a", compensate: |ctx| ctx
    .emit::<String>("undo-a".to_string()));
durable_step!(BranchB, "branch_b", compensate: |ctx| ctx
    .emit::<String>("undo-b".to_string()));

// The sibling leaf: emits straight to the sink, and its undo does too.
#[derive(Default)]
struct BranchC;
impl Procedure for BranchC {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "branch_c"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SeenState {
        SeenState::default()
    }
    fn apply_change(state: &mut SeenState, change: &Insert) {
        state.items.insert(change.0.clone());
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(Insert("branch_c".to_string()));
        ctx.emit::<String>(format!("c:{input}"))
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        _out: Vec<std::sync::Arc<String>>,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<String>("undo-c".to_string())
    }
}

// The deep leaf under BranchB: blocks (crash-resume), or fails (compensation).
#[derive(Default)]
struct BranchD;
impl Procedure for BranchD {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "branch_d"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        if env::var(BRANCH_FAIL_ENV).is_ok() {
            return Err(ProcedureError::Custom("branch_d failed".into()));
        }
        block_until_released();
        ctx.emit::<String>(format!("d:{input}"))
    }
}

struct BranchWorkflow;
impl WorkflowOperative for BranchWorkflow {
    fn factory_id() -> &'static str {
        "branch_workflow"
    }
    // Fail fast: the compensation scenario is about the rollback order, not the
    // ladder that precedes it.
    fn retry_attempts() -> u32 {
        0
    }
    fn procedures(c: &ChannelBuilder) {
        // One channel, two consumers: THE fan-out. Both branches read exactly
        // what BranchA released on it (held handle, wired twice).
        let mid = c
            .input::<String>("in")
            .into::<BranchA>()
            .named("mid")
            .codec()
            .also_output_with("branch_sink", |s: &String| format!("a:{s}").into_bytes());
        mid.into::<BranchB>()
            .named("bmid")
            .codec()
            .also_output_with("branch_sink", |s: &String| format!("b:{s}").into_bytes())
            .into::<BranchD>()
            .named("d_out")
            .output("branch_sink");
        mid.into::<BranchC>().named("c_out").output("branch_sink");
    }
}

// `diamond_workflow`: the fan-in DAG (`workflow-branching` D4).
//
//     DiamA ─┬─▶ DiamB1 ─▶ DiamB2(blocks) ─┬─▶ DiamJoin
//            └─▶ DiamC ────────────────────┘
//
// The join accumulates BOTH arms' outputs and is dispatched exactly once,
// after both commit. DiamB2 BLOCKS until the resumed runner sets the release
// env — one level BELOW its sibling arm, because siblings commit at one level
// barrier — so a SIGKILL lands with A, B1 and C committed and B2 mid-run: the
// respawn re-runs B2 alone, and the join runs once with C's JOURNALED handoff
// banked and B2's fresh output appended — `c:` appears exactly once across
// both runs.
durable_step!(DiamA, "diam_a");
durable_step!(DiamB1, "diam_b1");

// Blocks BEFORE its durable change + emit, so the kill window is deterministic:
// B2 is always the uncommitted producer while the sibling arm committed.
#[derive(Default)]
struct DiamB2;
impl Procedure for DiamB2 {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "diam_b2"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SeenState {
        SeenState::default()
    }
    fn apply_change(state: &mut SeenState, change: &Insert) {
        state.items.insert(change.0.clone());
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        block_until_released();
        state.apply(Insert("diam_b".to_string()));
        ctx.emit::<String>(format!("b:{input}"))
    }
}

#[derive(Default)]
struct DiamC;
impl Procedure for DiamC {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "diam_c"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SeenState {
        SeenState::default()
    }
    fn apply_change(state: &mut SeenState, change: &Insert) {
        state.items.insert(change.0.clone());
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(Insert("diam_c".to_string()));
        ctx.emit::<String>(format!("c:{input}"))
    }
}

// The join: one `j:`-tagged record per accumulated contribution.
#[derive(Default)]
struct DiamJoin;
impl Procedure for DiamJoin {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "diam_join"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<String>(format!("j:{input}"))
    }
}

struct DiamondWorkflow;
impl WorkflowOperative for DiamondWorkflow {
    fn factory_id() -> &'static str {
        "diamond_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        // Held handle wired twice = fan-out; two streams naming DiamJoin
        // (unnamed, same class) = the fan-in.
        let fan = c
            .input::<String>("in")
            .into::<DiamA>()
            .named("dfan")
            .codec()
            .also_output_with("diamond_sink", |s: &String| format!("a:{s}").into_bytes());
        fan.into::<DiamB1>()
            .named("db1")
            .codec()
            .into::<DiamB2>()
            .named("db2")
            .codec()
            .also_output_with("diamond_sink", |s: &String| s.clone().into_bytes())
            .into::<DiamJoin>();
        fan.into::<DiamC>()
            .named("dc")
            .codec()
            .also_output_with("diamond_sink", |s: &String| s.clone().into_bytes())
            .into::<DiamJoin>()
            .named("j_out")
            .output("diamond_sink");
    }
}

// `retry_workflow`: the per-entity retry ladder (`workflow-entity-retry`). Proc RA
// commits durable state, forwards on a hybrid channel, and compensates by
// emitting an undo record; proc `retry_flaky` fails its first
// `FLEZHA_WORKFLOW_RETRY_FAILS` attempts (0 by default — the env is what each
// scenario arms) and then emits `attempt:<n>:<input>`. One armed failure is
// absorbed by the ladder (the run completes `ok`, the sink sees attempt 2);
// arming more failures than the ladder has attempts exhausts it — RA rolls back
// and the run reports `error`.
const RETRY_FAILS_ENV: &str = "FLEZHA_WORKFLOW_RETRY_FAILS";

// Attempts the flaky procedure has made in this process (a retry re-runs it in
// place, so an in-memory counter is the whole ladder's view).
static RETRY_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

fn armed_failures() -> usize {
    env::var(RETRY_FAILS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

durable_step!(RetryA, "retry_a", compensate: |ctx| ctx
    .emit::<String>("undone".to_string()));

#[derive(Default)]
struct RetryFlaky;
impl Procedure for RetryFlaky {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "retry_flaky"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let attempt = RETRY_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt <= armed_failures() {
            return Err(ProcedureError::Custom(format!("flaky attempt {attempt}")));
        }
        ctx.emit::<String>(format!("attempt:{attempt}:{input}"))
    }
}

struct RetryWorkflow;
impl WorkflowOperative for RetryWorkflow {
    fn factory_id() -> &'static str {
        "retry_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<RetryA>()
            .named("mid")
            .codec()
            .also_output_with("retry_sink", |s: &String| format!("res:{s}").into_bytes())
            .into::<RetryFlaky>()
            .named("out")
            .output("retry_sink");
    }
}

// `chat_loop_workflow`: the durable-loop + suspension shape
// (`workflow-durable-loop-and-suspension`). One iteration per inbox item: proc
// TURN commits the turn to Entity state (so a resumed iteration is visible in
// the WAL) and emits it; proc REPLY emits the answer to the sink. Between turns
// the loop finds an empty inbox at a committed boundary and PARKS — the
// instance is evicted from the runner and rehydrated by the next message.
//
// The turn whose payload is `mid-crash` BLOCKS while the release env is unset,
// so a SIGKILL lands with the earlier iterations durable and that one in flight.
// The resumed runner (release env set) runs it through. Keying the block on the
// payload — rather than an invocation count — keeps it deterministic across the
// park/rehydrate cycles, which re-run the driver but not the turns.
const CRASH_TURN: &str = "mid-crash";

#[derive(Default)]
struct ChatTurn;
impl Procedure for ChatTurn {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "chat_turn"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SeenState {
        SeenState::default()
    }
    fn apply_change(state: &mut SeenState, change: &Insert) {
        state.items.insert(change.0.clone());
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        if input == CRASH_TURN {
            block_until_released();
        }
        // Keyed by the turn, so the WAL record count is the number of DISTINCT
        // turns that ran — a re-run of a committed iteration would show up.
        state.apply(Insert(input.clone()));
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct ChatReply;
impl Procedure for ChatReply {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "chat_reply"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<String>(format!("reply:{input}"))
    }
}

// -- StatelessLoopWorkflow -------------------------------------------------------
//
// The same durable PER_INPUT loop as `ChatLoopWorkflow`, minus its stateful
// first procedure: `ChatReply` alone holds NO per-Entity state, so nothing
// writes an Entity WAL, the controller has nothing to restore, and it ships no
// `RestoreInitState` on a rehydrating bind. The instance's proc-journal therefore
// reaches the driver only if it rides the bind itself — deliver it on the
// restore frame and every turn after the first commits at iteration 0 again,
// collides with iteration 0's `outbox_flushed` marker, and has its staged
// output journaled but never delivered.

struct StatelessLoopWorkflow;
impl WorkflowOperative for StatelessLoopWorkflow {
    fn factory_id() -> &'static str {
        "stateless_loop_workflow"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(600))
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatReply>()
            .named("out")
            .output("chat_sink");
    }
}

// -- ChatRewindWorkflow ----------------------------------------------------------
//
// `workflow-loop-rewind`: the same chat loop, but its replying procedure can be
// undone. The undo handle lives in Entity State — a rewind's compensation is
// rebuilt from the journal and is handed NO retained outputs (the at-least-once
// author contract) — so the hook reads the last thing it said, retracts it, and
// pops it. Unwinding in reverse iteration order therefore retracts the turns in
// reverse.

#[derive(Serialize, Deserialize, Clone, Default)]
struct ReplyLog {
    said: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
enum ReplyChange {
    Said(String),
    Retracted,
}
impl Change for ReplyChange {}

#[derive(Default)]
struct ChatReplyUndoable;
impl Procedure for ChatReplyUndoable {
    type Input = String;
    type Output = String;
    type State = ReplyLog;
    type Change = ReplyChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "chat_reply_undoable"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> ReplyLog {
        ReplyLog::default()
    }
    fn apply_change(state: &mut ReplyLog, change: &ReplyChange) {
        match change {
            ReplyChange::Said(turn) => state.said.push(turn.clone()),
            ReplyChange::Retracted => {
                state.said.pop();
            }
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(ReplyChange::Said(input.clone()));
        ctx.emit::<String>(format!("reply:{input}"))
    }
    const COMPENSATE_OVERRIDDEN: bool = true;
    fn compensate(
        &mut self,
        _completed_output: Vec<std::sync::Arc<String>>,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let Some(turn) = state.said.last().cloned() else {
            return Ok(());
        };
        state.apply(ReplyChange::Retracted);
        ctx.emit::<String>(format!("retract:{turn}"))
    }
}

struct ChatRewindWorkflow;
impl WorkflowOperative for ChatRewindWorkflow {
    fn factory_id() -> &'static str {
        "chat_rewind_workflow"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(600))
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatTurn>()
            .named("mid")
            .codec()
            .into::<ChatReplyUndoable>()
            .named("out")
            .output("chat_sink");
    }
}

struct ChatLoopWorkflow;
impl WorkflowOperative for ChatLoopWorkflow {
    fn factory_id() -> &'static str {
        "chat_loop_workflow"
    }
    fn loop_mode() -> LoopMode {
        LoopMode::PerInput
    }
    // A chat session ends on an external stop, not on its own; the duration
    // backstop keeps a stray e2e instance from outliving the test run.
    fn self_terminating() -> bool {
        false
    }
    fn max_duration() -> Option<Duration> {
        Some(Duration::from_secs(600))
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ChatTurn>()
            .named("mid")
            .codec()
            .into::<ChatReply>()
            .named("out")
            .output("chat_sink");
    }
}

// -- DynamicEntityOperative ----------------------------------------------------
//
// Dynamic-mode operative shared by the e2e correctness test and `mix bench
// --shape dynamic`. Key = text before the first ':' (`"drop"` → None, drop).
// Eviction TTL is read from `DYN_TEST_TTL_MS` (the e2e sets 200ms to exercise
// reap + delete; the bench leaves it unset → no eviction, pure routing).

#[derive(Serialize, Deserialize, Clone, Default)]
struct DynState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum DynChange {
    Push(String),
}
impl Change for DynChange {}

#[derive(Default)]
struct DynamicEntityProcedure;
impl Procedure for DynamicEntityProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = DynState;
    type Change = DynChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "dynamic_entity_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> DynState {
        DynState::default()
    }
    fn apply_change(state: &mut DynState, change: &DynChange) {
        let DynChange::Push(s) = change;
        state.seen.push(s.clone());
    }
    fn entity_idle_ttl() -> Option<Duration> {
        env::var("DYN_TEST_TTL_MS")
            .ok()
            .and_then(|s| s.parse().ok())
            .map(Duration::from_millis)
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(DynChange::Push(input.clone()));
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct DynamicEntityOperative;
impl StreamOperative for DynamicEntityOperative {
    fn factory_id() -> &'static str {
        "dynamic_entity"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .on_full(OverflowPolicy::Block)
            .filter(|input: &String| input != "drop")
            .key_by(|input: &String| input.split(':').next().unwrap_or(input).to_string())
            .into::<DynamicEntityProcedure>()
            .named("kafka")
            .output("test_kafka");
    }
}

// -- PartitionRelayOperative --------------------------------------------------
//
// The partitioned-operatives e2e's upstream: keyed on the wiring (so it can be
// partitioned at all) AND emitting keyed (so a partitioned forward target can
// resolve the owning member). Key = text before the first ':', the same
// convention as `dynamic_entity`, so one fixture's key groups line up with the
// other's.

#[derive(Serialize, Deserialize, Clone, Default)]
struct RelayState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum RelayChange {
    Push(String),
}
impl Change for RelayChange {}

#[derive(Default)]
struct PartitionRelayProcedure;
impl Procedure for PartitionRelayProcedure {
    type Input = String;
    type Output = String;
    type State = RelayState;
    type Change = RelayChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "partition_relay_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> RelayState {
        RelayState::default()
    }
    fn apply_change(state: &mut RelayState, change: &RelayChange) {
        let RelayChange::Push(s) = change;
        state.seen.push(s.clone());
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let key = input
            .split(':')
            .next()
            .unwrap_or(input.as_str())
            .to_string();
        state.apply(RelayChange::Push(input.clone()));
        ctx.emit_keyed::<String>(key, input)
    }
}

struct PartitionRelayOperative;
impl StreamOperative for PartitionRelayOperative {
    fn factory_id() -> &'static str {
        "partition_relay"
    }
    fn channels(c: &ChannelBuilder) {
        // The plugin channel is named `input` because a forwarded frame keeps
        // its producing channel's name in metadata, and that is what selects
        // the receiving operative's channel — the downstream fixture's entry
        // channel. The entry channel here is `in` so the two names cannot
        // collide inside this class.
        c.input::<String>("in")
            .key_by(|input: &String| input.split(':').next().unwrap_or(input).to_string())
            .into::<PartitionRelayProcedure>()
            .named("input")
            .keyed()
            .output("relay_out");
    }
}

// -- SessionTimeoutOperative --------------------------------------------------
//
// The worked session-timeout shape (`entity-timers`): each input pushes the
// idle deadline out, and the firing closes the session. Single-entity so the
// e2e's crash-resume half exercises a durable Entity on both runtimes. The
// timeout is read from `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).

#[derive(Serialize, Deserialize, Clone, Default)]
struct SessionState {
    touched: Vec<String>,
    closed_at: Vec<u64>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum SessionChange {
    Touched(String),
    Closed(u64),
}
impl Change for SessionChange {}

fn session_timeout_ms() -> u64 {
    env::var("TIMER_TEST_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000)
}

#[derive(Default)]
struct SessionTimeoutProcedure;
impl Procedure for SessionTimeoutProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = SessionState;
    type Change = SessionChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "session_timeout_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> SessionState {
        SessionState::default()
    }
    fn apply_change(state: &mut SessionState, change: &SessionChange) {
        match change {
            SessionChange::Touched(s) => state.touched.push(s.clone()),
            SessionChange::Closed(at) => state.closed_at.push(*at),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(SessionChange::Touched(input));
        // A processing-time timer, because the answer to "nothing has arrived"
        // cannot be an event-time one: nothing would advance the clock.
        ctx.timer_after(Duration::from_millis(session_timeout_ms()))?;
        Ok(())
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        _kind: flezha_runner::state::TimerKind,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(SessionChange::Closed(fired_at));
        ctx.emit::<Vec<u8>>(format!("closed@{fired_at}").into_bytes())
    }
}

struct SessionTimeoutOperative;
impl StreamOperative for SessionTimeoutOperative {
    fn factory_id() -> &'static str {
        "session_timeout"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<SessionTimeoutProcedure>()
            .named("kafka")
            .output("test_kafka");
    }
}

// -- DoNothingOperative -----------------------------------------------------------
//
// Echoes the input bytes on the plugin-bound `echo` channel. Stateless;
// inherits `Change = NoChange` so the runtime skips flush-buffer registration.

#[derive(Default)]
struct DoNothingProcedure;
impl Procedure for DoNothingProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "do_nothing_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct DoNothingOperative;
impl StreamOperative for DoNothingOperative {
    fn factory_id() -> &'static str {
        "do_nothing"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<DoNothingProcedure>()
            .named("echo")
            .output("do_nothing_echo");
    }
}

// -- StoreInputOperative ----------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct StoreInputState {
    binary_inputs: Vec<Vec<u8>>,
    text_inputs: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum StoreInputChange {
    PushText(String),
}
impl Change for StoreInputChange {}

#[derive(Default)]
struct StoreInputProcedure;
impl Procedure for StoreInputProcedure {
    type Input = String;
    type Output = String;
    type State = StoreInputState;
    type Change = StoreInputChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "store_input_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> StoreInputState {
        StoreInputState::default()
    }
    fn apply_change(state: &mut StoreInputState, change: &StoreInputChange) {
        match change {
            StoreInputChange::PushText(s) => state.text_inputs.push(s.clone()),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(StoreInputChange::PushText(input.clone()));
        ctx.emit::<String>(input)
    }
}

struct StoreInputOperative;
impl StreamOperative for StoreInputOperative {
    fn factory_id() -> &'static str {
        "store_input"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<StoreInputProcedure>()
            .named("sink")
            .output("store_input_sink");
    }
}

// -- TierStoreOperative (effectively-once) ----------------------------------------
//
// Effectively-once e2e fixture: inputs are "key:value" strings; state upserts
// the latest value per key; every input emits `key=value` under the natural
// key on a keyed plugin channel. Deterministic, so a crash-replay regenerates
// byte-identical outputs and a changelog sink converges to the last value per
// key — the §6.1 equivalence assertion.

#[derive(Serialize, Deserialize, Clone, Default)]
struct TierStoreState {
    values: std::collections::BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum TierStoreChange {
    Set(String, String),
}
impl Change for TierStoreChange {}

#[derive(Default)]
struct TierStoreProcedure;
impl Procedure for TierStoreProcedure {
    type Input = String;
    type Output = String;
    type State = TierStoreState;
    type Change = TierStoreChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tier_store_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> TierStoreState {
        TierStoreState::default()
    }
    fn apply_change(state: &mut TierStoreState, change: &TierStoreChange) {
        match change {
            TierStoreChange::Set(key, value) => {
                state.values.insert(key.clone(), value.clone());
            }
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let (key, value) = input.split_once(':').unwrap_or((input.as_str(), ""));
        let (key, value) = (key.to_string(), value.to_string());
        // Dead-letter e2e: a "poison" value fails every attempt, exhausting
        // the retry budget so the tier-on atomic park advances the entity
        // past it (and the min-watermark with it).
        if value == "poison" {
            return Err(ProcedureError::Custom("poison value".into()));
        }
        state.apply(TierStoreChange::Set(key.clone(), value.clone()));
        ctx.emit_keyed::<String>(key.clone(), format!("{key}={value}"))
    }
}

struct TierStoreOperative;
impl StreamOperative for TierStoreOperative {
    fn factory_id() -> &'static str {
        "tier_store"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        // 300 ms backoff base keeps the min-watermark freeze observable
        // (~0.9 s from first failure to park) in the DLQ e2e.
        c.input::<String>("input")
            .retry_backoff_ms(300)
            .into::<TierStoreProcedure>()
            .named("sink")
            .keyed()
            .output("tier_sink");
    }
}

// -- CheckpointStoreOperative (effectively-once-checkpoint-granularity e2e) --
//
// Same store as TierStoreOperative, but acknowledging only at an explicit
// checkpoint: a `commit:` input closes the interval, every other input folds
// into it. Emits still flush as they happen, so the sink sees an interval's
// outputs long before its offsets are acknowledged — which is exactly what the
// e2e crashes in the middle of.

#[derive(Default)]
struct CheckpointStoreProcedure;
impl Procedure for CheckpointStoreProcedure {
    type Input = String;
    type Output = String;
    type State = TierStoreState;
    type Change = TierStoreChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "checkpoint_store_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> TierStoreState {
        TierStoreState::default()
    }
    fn apply_change(state: &mut TierStoreState, change: &TierStoreChange) {
        match change {
            TierStoreChange::Set(key, value) => {
                state.values.insert(key.clone(), value.clone());
            }
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let (key, value) = input.split_once(':').unwrap_or((input.as_str(), ""));
        let (key, value) = (key.to_string(), value.to_string());
        if key == "commit" {
            ctx.checkpoint();
            return Ok(());
        }
        state.apply(TierStoreChange::Set(key.clone(), value.clone()));
        ctx.emit_keyed::<String>(key.clone(), format!("{key}={value}"))
    }
}

struct CheckpointStoreOperative;
impl StreamOperative for CheckpointStoreOperative {
    fn factory_id() -> &'static str {
        "checkpoint_store"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.checkpoint_ack();
        c.input::<String>("input")
            .into::<CheckpointStoreProcedure>()
            .named("sink")
            .keyed()
            .output("tier_sink");
    }
}

// -- CandleOperative (stream-late-input-correction e2e) -------------------
//
// A trade stream keyed by an EVENT time carried in the input itself
// ("<event_time>:<price>"), with a declared correction window. State records the
// event times in the order they were folded — which is the whole point of the
// change: a late trade must end up folded in event-time order, not arrival
// order, and the only way to prove that is to look at the order.
//
// Send 100, 300, then the late 200. Without correction the fold order is
// [100, 300, 200] and the candle is permanently wrong. With it, the engine
// rewinds behind 200, replays the retained input log sorted by event time, and
// the fold order becomes [100, 200, 300].

#[derive(Default, Serialize, Deserialize, Clone)]
struct CandleState {
    /// Event times in fold order. The assertion surface.
    folded: Vec<u64>,
}

#[derive(Serialize, Deserialize, Clone)]
enum CandleChange {
    Fold(u64),
}

impl Change for CandleChange {}

struct TradeEventTime;
impl TimeExtractor<String> for TradeEventTime {
    fn extract(input: &String) -> u64 {
        input
            .split_once(':')
            .map(|(ts, _)| ts)
            .unwrap_or(input.as_str())
            .parse()
            .unwrap_or(0)
    }
}

#[derive(Default)]
struct CandleProcedure;
impl Procedure for CandleProcedure {
    type Input = String;
    type Output = String;
    type State = CandleState;
    type Change = CandleChange;
    type Clock = EventTimeFrom<TradeEventTime>;
    type Config = ();
    fn name() -> &'static str {
        "candle_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> CandleState {
        CandleState::default()
    }
    fn apply_change(state: &mut CandleState, change: &CandleChange) {
        match change {
            CandleChange::Fold(event_time) => state.folded.push(*event_time),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        // `ctx.now()` IS the extracted event time under an EventTimeFrom clock.
        let event_time = ctx.now();
        state.apply(CandleChange::Fold(event_time));
        // One row per candle bucket, so a corrected replay overwrites by key
        // rather than appending a second row.
        let bucket = event_time / 1_000;
        ctx.emit_keyed::<String>(format!("candle/{bucket}"), format!("{bucket}={input}"))
    }
}

struct CandleOperative;
impl StreamOperative for CandleOperative {
    fn factory_id() -> &'static str {
        "candle"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.correction_window(600_000);
        c.input::<String>("input")
            .into::<CandleProcedure>()
            .named("sink")
            .keyed()
            .output("tier_sink");
    }
}

// -- PoisonProbeOperative (dead-letter-queue e2e) -------------------
//
// Tier-off retry-then-park probe: `run` fails on the input "poison" until an
// "antidote" input flips a process-global flag. The e2e parks the poison
// payload (durable DLQ entry, entity continues), sends the antidote, then
// replays the parked entry — which now succeeds and removes it.

static POISON_ANTIDOTE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

#[derive(Default)]
struct PoisonProbeProcedure;
impl Procedure for PoisonProbeProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "poison_probe_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        if input == "antidote" {
            POISON_ANTIDOTE.store(true, std::sync::atomic::Ordering::SeqCst);
        } else if input == "poison" && !POISON_ANTIDOTE.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ProcedureError::Custom("poison payload".into()));
        }
        ctx.emit(format!("ok:{input}"))
    }
}

struct PoisonProbeOperative;
impl StreamOperative for PoisonProbeOperative {
    fn factory_id() -> &'static str {
        "poison_probe"
    }
    fn channels(c: &ChannelBuilder) {
        // Small backoff keeps the park fast in the e2e; the budget stays
        // at the default 3 attempts.
        c.input::<String>("input")
            .retry_backoff_ms(25)
            .into::<PoisonProbeProcedure>()
            .named("out")
            .output("probe_sink");
    }
}

// -- WedgeProbeOperative (control-plane silent-wedge e2e) ------------------
//
// Reproduces the 2026-06-10 production freeze: the input "wedge" blocks the
// dispatch inside user code forever, so the SDK's dispatch counter stops
// advancing while the heartbeat thread keeps reporting the runner alive.
// That is exactly the `runner_pool_wedged` signature the classifier looks
// for — a runner that is connected, heartbeating, and processing nothing.
// Every other input emits `ok:<input>`, which is how the e2e proves the
// operative produces again once it has been rebound elsewhere.

#[derive(Default)]
struct WedgeProbeProcedure;
impl Procedure for WedgeProbeProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "wedge_probe_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        if input == "wedge" {
            // Never returns. The runner stays connected and heartbeating; only
            // its dispatch progress dies. Teardown is the stdin-EOF watcher.
            loop {
                thread::sleep(Duration::from_secs(3600));
            }
        }
        ctx.emit(format!("ok:{input}"))
    }
}

struct WedgeProbeOperative;
impl StreamOperative for WedgeProbeOperative {
    fn factory_id() -> &'static str {
        "wedge_probe"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<WedgeProbeProcedure>()
            .named("out")
            .output("wedge_sink");
    }
}

// -- ResumeProbeOperative ---------------------------------------------------------
//
// Stateful counter that EMITS its running total on every input, so a Kafka
// plugin on the output makes the persisted value externally observable. Used
// by chaos/tests/stateful-resume.sh to prove WAL-backed state survives a hard
// controller kill / runner restart: after recovery the operative resumes from
// the persisted total (replayed changes), not from zero. Input is a decimal
// delta string; output is the new running total as ASCII bytes.

#[derive(Serialize, Deserialize, Clone, Default)]
struct ResumeProbeState {
    total: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum ResumeProbeChange {
    Add(i64),
}
impl Change for ResumeProbeChange {}

#[derive(Default)]
struct ResumeProbeProcedure;
impl Procedure for ResumeProbeProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = ResumeProbeState;
    type Change = ResumeProbeChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "resume_probe_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> ResumeProbeState {
        // Default + change replay rebuilds the total on restore (same pattern
        // as stateful_counter); no snapshot fast-path needed.
        ResumeProbeState::default()
    }
    fn apply_change(state: &mut ResumeProbeState, change: &ResumeProbeChange) {
        match change {
            ResumeProbeChange::Add(d) => state.total = state.total.saturating_add(*d),
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let delta: i64 = input.parse().unwrap_or(0);
        let new_total = state.total.saturating_add(delta);
        state.apply(ResumeProbeChange::Add(delta));
        ctx.emit::<Vec<u8>>(new_total.to_string().into_bytes())
    }
}

struct ResumeProbeOperative;
impl StreamOperative for ResumeProbeOperative {
    fn factory_id() -> &'static str {
        "resume_probe"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<ResumeProbeProcedure>()
            .named("total")
            .output("resume_probe_out");
    }
}

// -- CompareFilterOperative -------------------------------------------------------

#[derive(Serialize, Deserialize, Clone)]
struct CompareFilterState {
    comparable: String,
    inverted: bool,
}

impl Default for CompareFilterState {
    fn default() -> Self {
        Self {
            comparable: "default".to_string(),
            inverted: false,
        }
    }
}

#[derive(Default)]
struct CompareFilterProcedure;
impl Procedure for CompareFilterProcedure {
    type Input = String;
    type Output = String;
    type State = CompareFilterState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "compare_filter_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> CompareFilterState {
        CompareFilterState::default()
    }
    fn run(
        &mut self,
        input: String,
        state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        if state.comparable == input {
            ctx.emit::<String>(input)?;
        }
        Ok(())
    }
}

struct CompareFilterOperative;
impl StreamOperative for CompareFilterOperative {
    fn factory_id() -> &'static str {
        "compare_filter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<CompareFilterProcedure>()
            .named("sink")
            .output("compare_filter_sink");
    }
}

// -- StaticEntityOperative -----------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct StaticEntryState {
    seen: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum StaticEntryChange {
    Push(String),
}
impl Change for StaticEntryChange {}

#[derive(Default)]
struct StaticEntryProcedure;
impl Procedure for StaticEntryProcedure {
    type Input = String;
    type Output = String;
    type State = StaticEntryState;
    type Change = StaticEntryChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_entry_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> StaticEntryState {
        StaticEntryState::default()
    }
    fn apply_change(state: &mut StaticEntryState, change: &StaticEntryChange) {
        match change {
            StaticEntryChange::Push(s) => state.seen.push(s.clone()),
        }
    }
    fn entity_keys() -> Vec<&'static str> {
        vec!["a", "b", "c", "d"]
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(input: &String) -> Vec<&'static str> {
        match input.chars().next() {
            Some('a') => vec!["a"],
            Some('b') => vec!["b"],
            Some('c') => vec!["c"],
            _ => vec!["d"],
        }
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(StaticEntryChange::Push(input.clone()));
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StaticDownstreamProcedure;
impl Procedure for StaticDownstreamProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "static_downstream_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct StaticEntityOperative;
impl StreamOperative for StaticEntityOperative {
    fn factory_id() -> &'static str {
        "static_entity"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<StaticEntryProcedure>()
            .named("hop")
            .into::<StaticDownstreamProcedure>()
            .named("kafka")
            .output("test_kafka");
    }
}

// -- EntityStressOperative --------------------------------------------------

const STRESS_KEYS: &[&str] = &["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"];

#[derive(Default)]
struct StressProcedureA;
impl Procedure for StressProcedureA {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_a"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn entity_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_: &String) -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressProcedureB;
impl Procedure for StressProcedureB {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_b"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn entity_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressProcedureC;
impl Procedure for StressProcedureC {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_c"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn entity_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<String>(input)
    }
}

#[derive(Default)]
struct StressProcedureD;
impl Procedure for StressProcedureD {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "stress_d"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn entity_keys() -> Vec<&'static str> {
        STRESS_KEYS.to_vec()
    }
    const ENTITIES_FOR_OVERRIDDEN: bool = true;
    fn entities_for(_: &String) -> Vec<&'static str> {
        vec!["p0"]
    }
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct EntityStressOperative;
impl StreamOperative for EntityStressOperative {
    fn factory_id() -> &'static str {
        "entity_stress"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<StressProcedureA>()
            .named("a_to_b")
            .into::<StressProcedureB>()
            .named("b_to_c")
            .into::<StressProcedureC>()
            .named("c_to_d")
            .into::<StressProcedureD>()
            .named("echo")
            .output("entity_stress_echo");
    }
}

// -- BoundedMailboxOperative ------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Default)]
struct WorkflowProcedureState {
    pub processed: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
enum WorkflowChange {
    Inc,
}
impl Change for WorkflowChange {}

fn apply_inc(state: &mut WorkflowProcedureState, _: &WorkflowChange) {
    state.processed += 1;
}

#[derive(Default)]
struct BlockTestProcedure;
impl Procedure for BlockTestProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = WorkflowProcedureState;
    type Change = WorkflowChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "block_test_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> WorkflowProcedureState {
        WorkflowProcedureState::default()
    }
    fn apply_change(state: &mut WorkflowProcedureState, change: &WorkflowChange) {
        apply_inc(state, change);
    }
    fn mailbox_capacity() -> usize {
        4
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(WorkflowChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

#[derive(Default)]
struct DropNewestTestProcedure;
impl Procedure for DropNewestTestProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = WorkflowProcedureState;
    type Change = WorkflowChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_newest_test_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> WorkflowProcedureState {
        WorkflowProcedureState::default()
    }
    fn apply_change(state: &mut WorkflowProcedureState, change: &WorkflowChange) {
        apply_inc(state, change);
    }
    fn mailbox_capacity() -> usize {
        4
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(WorkflowChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

#[derive(Default)]
struct DropOldestTestProcedure;
impl Procedure for DropOldestTestProcedure {
    type Input = String;
    type Output = Vec<u8>;
    type State = WorkflowProcedureState;
    type Change = WorkflowChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "drop_oldest_test_procedure"
    }
    fn init_state(
        _: Option<&str>,
        _init_payload: Option<RestoreInitPayload<'_>>,
    ) -> WorkflowProcedureState {
        WorkflowProcedureState::default()
    }
    fn apply_change(state: &mut WorkflowProcedureState, change: &WorkflowChange) {
        apply_inc(state, change);
    }
    fn mailbox_capacity() -> usize {
        4
    }
    const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(WorkflowChange::Inc);
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

// One operative per overflow policy: the graph-shape rule
// (`sdk-graph-authoring`) allows exactly one entry channel per operative, so
// the former three-entry BoundedMailboxOperative is now three classes.
struct BoundedMailboxBlockOperative;
impl StreamOperative for BoundedMailboxBlockOperative {
    fn factory_id() -> &'static str {
        "bounded_mailbox_block"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("block_input")
            .on_full(OverflowPolicy::Block)
            .into::<BlockTestProcedure>()
            .named("block_echo")
            .output("bounded_mailbox_echo");
    }
}

struct BoundedMailboxDropNewestOperative;
impl StreamOperative for BoundedMailboxDropNewestOperative {
    fn factory_id() -> &'static str {
        "bounded_mailbox_drop_newest"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("drop_newest_input")
            .into::<DropNewestTestProcedure>()
            .named("drop_newest_echo")
            .output("bounded_mailbox_echo");
    }
}

struct BoundedMailboxDropOldestOperative;
impl StreamOperative for BoundedMailboxDropOldestOperative {
    fn factory_id() -> &'static str {
        "bounded_mailbox_drop_oldest"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("drop_oldest_input")
            .on_full(OverflowPolicy::DropOldest)
            .into::<DropOldestTestProcedure>()
            .named("drop_oldest_echo")
            .output("bounded_mailbox_echo");
    }
}

// -- StatefulCounterOperative (Section 6.1) --------------------------------------
//
// One Procedure `CounterProcedure` with `Change = CounterChange { delta: i64 }`,
// `apply_change` mutating `state.value += change.delta`, `coalesce_key()`
// returning `None` (verbatim retention).

#[derive(Serialize, Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct CounterState {
    pub value: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct CounterChange {
    pub delta: i64,
}
impl Change for CounterChange {}

#[derive(Default)]
struct CounterProcedure;
impl Procedure for CounterProcedure {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "counter_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }
    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value = state.value.saturating_add(change.delta);
    }
    fn run(
        &mut self,
        input: i64,
        mut state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(CounterChange { delta: input });
        Ok(())
    }
}

struct StatefulCounterOperative;
impl StreamOperative for StatefulCounterOperative {
    fn factory_id() -> &'static str {
        "stateful_counter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<CounterEntryProcedure>()
            .named("ticks")
            .into::<CounterProcedure>();
    }
}

#[derive(Default)]
struct CounterEntryProcedure;
impl Procedure for CounterEntryProcedure {
    type Input = String;
    type Output = i64;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "counter_entry_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let delta: i64 = input.parse().unwrap_or(0);
        ctx.emit::<i64>(delta)
    }
}

// -- CoalescingCounterOperative (Section 6.2) ------------------------------------
//
// `Change = TickChange { interval: u8, ts: u64, delta: i64 }` with
// `coalesce_key` returning `Some(CoalesceKey::from_hash(&(interval, ts)))`.

#[derive(Serialize, Deserialize, Clone, Default, Debug, PartialEq, Eq)]
pub struct TickState {
    pub last: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TickChange {
    pub interval: u8,
    pub ts: u64,
    pub delta: i64,
}
impl Change for TickChange {
    fn coalesce_key(&self) -> Option<CoalesceKey> {
        Some(CoalesceKey::from_hash(&(self.interval, self.ts)))
    }
}

#[derive(Default)]
struct TickProcedure;
impl Procedure for TickProcedure {
    type Input = TickInput;
    type Output = ();
    type State = TickState;
    type Change = TickChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tick_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> TickState {
        TickState::default()
    }
    fn apply_change(state: &mut TickState, change: &TickChange) {
        state.last = change.delta;
    }
    fn run(
        &mut self,
        input: TickInput,
        mut state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(TickChange {
            interval: input.interval,
            ts: input.ts,
            delta: input.delta,
        });
        Ok(())
    }
}

#[derive(Clone)]
pub struct TickInput {
    pub interval: u8,
    pub ts: u64,
    pub delta: i64,
}

#[derive(Default)]
struct TickEntryProcedure;
impl Procedure for TickEntryProcedure {
    type Input = String;
    type Output = TickInput;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "tick_entry_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        // Wire format: `<interval>:<ts>:<delta>`.
        let mut parts = input.splitn(3, ':');
        let interval = parts.next().and_then(|s| s.parse::<u8>().ok()).unwrap_or(0);
        let ts = parts
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let delta = parts
            .next()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);
        ctx.emit::<TickInput>(TickInput {
            interval,
            ts,
            delta,
        })
    }
}

struct CoalescingCounterOperative;
impl StreamOperative for CoalescingCounterOperative {
    fn factory_id() -> &'static str {
        "coalescing_counter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<TickEntryProcedure>()
            .named("ticks")
            .into::<TickProcedure>();
    }
}

// -- PersistenceOffOperative (Section 6.3) ---------------------------------------
//
// Same Procedure topology as `StatefulCounterOperative` but `PERSISTENCE_ENABLED`
// is `false`. Used by integration tests to verify the toggle-off drain
// path discards frames without invoking the wire stub.

struct PersistenceOffOperative;
impl StreamOperative for PersistenceOffOperative {
    fn factory_id() -> &'static str {
        "persistence_off_counter"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<PersistenceOffEntryProcedure>()
            .named("ticks")
            .into::<PersistenceOffCounterProcedure>();
    }
    const PERSISTENCE_ENABLED: bool = false;
}

#[derive(Default)]
struct PersistenceOffEntryProcedure;
impl Procedure for PersistenceOffEntryProcedure {
    type Input = String;
    type Output = i64;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "persistence_off_entry_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let delta: i64 = input.parse().unwrap_or(0);
        ctx.emit::<i64>(delta)
    }
}

#[derive(Default)]
struct PersistenceOffCounterProcedure;
impl Procedure for PersistenceOffCounterProcedure {
    type Input = i64;
    type Output = ();
    type State = CounterState;
    type Change = CounterChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "persistence_off_counter_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) -> CounterState {
        CounterState::default()
    }
    fn apply_change(state: &mut CounterState, change: &CounterChange) {
        state.value = state.value.saturating_add(change.delta);
    }
    fn run(
        &mut self,
        input: i64,
        mut state: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(CounterChange { delta: input });
        Ok(())
    }
}

// -- HybridChannelOperative -------------------------------------------------------
//
// Demonstrates `also_output_with(...)`: the producer Procedure emits a
// `u32` value that fans out to an inter-procedure consumer and is also
// serialized to bytes for the `hybrid_indicators` plugin. The
// serializer panics on the sentinel `0xDEADBEEF` so an operator can
// observe the panic-isolation guarantee end-to-end.

#[derive(Default)]
struct HybridProducerProcedure;
impl Procedure for HybridProducerProcedure {
    type Input = String;
    type Output = u32;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid_producer_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        let n: u32 = input.parse().unwrap_or(0);
        match ctx.emit::<u32>(n) {
            Ok(()) => Ok(()),
            Err(ProcedureError::PluginSerializerPanic { channel, message }) => {
                eprintln!("hybrid_producer_procedure: serializer panic on `{channel}`: {message}");
                Ok(())
            }
            Err(other) => Err(other),
        }
    }
}

#[derive(Default)]
struct HybridConsumerProcedure;
impl Procedure for HybridConsumerProcedure {
    type Input = u32;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "hybrid_consumer_procedure"
    }
    fn init_state(_: Option<&str>, _init_payload: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: u32,
        _: StateHandle<'_, Self>,
        _: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        println!("hybrid_consumer_procedure: received {input}");
        Ok(())
    }
}

struct HybridChannelOperative;
impl StreamOperative for HybridChannelOperative {
    fn factory_id() -> &'static str {
        "hybrid_channel"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<HybridProducerProcedure>()
            .named("hybrid_indicator")
            .also_output_with("hybrid_indicators", |v: &u32| {
                if *v == 0xDEAD_BEEF {
                    panic!("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF");
                }
                v.to_be_bytes().to_vec()
            })
            .into::<HybridConsumerProcedure>();
    }
}

// ---------------------------------------------------------------------------
// Authoring-sugar twins (`simplify-sdk-authoring`). Each mirrors a
// hand-written operative above to prove the macros expand to the same trait
// surface and interoperate with raw procedures. See
// `docs/sdk_authoring_contract.md`.
// ---------------------------------------------------------------------------

// Single-procedure stateless sugar — twin of `DoNothingProcedure` / `DoNothingOperative`.
// Auto-wires input -> procedure -> plugin and auto-registers (no `register_stream`).
#[flezha_runner::procedure(id = "sugar_do_nothing", sink = "sugar_do_nothing_echo")]
fn sugar_do_nothing(input: String) -> Vec<u8> {
    input.into_bytes()
}

// Single-procedure stateful sugar with `#[derive(flezha::State)]` — twin of
// `StoreInputProcedure` / `StoreInputOperative`.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
struct SugarStoreState {
    text_inputs: Vec<String>,
}

#[flezha_runner::procedure(id = "sugar_store_input", sink = "sugar_store_sink")]
fn sugar_store_input(input: String, state: &mut SugarStoreState) -> String {
    state.push_text_inputs(input.clone());
    input
}

// `operative!` linear-pipeline twin of `StaticEntityOperative`.
#[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
struct MacroStaticEntryState {
    seen: Vec<String>,
}

fn macro_static_part(input: &str) -> Vec<&'static str> {
    match input.chars().next() {
        Some('a') => vec!["a"],
        Some('b') => vec!["b"],
        Some('c') => vec!["c"],
        _ => vec!["d"],
    }
}

#[flezha_runner::procedure(
    id = "macro_static_entry_procedure",
    keys = ["a", "b", "c", "d"],
    entities_for = macro_static_part
)]
fn macro_static_entry(input: String, state: &mut MacroStaticEntryState) -> String {
    state.push_seen(input.clone());
    input
}

#[flezha_runner::procedure(id = "macro_static_downstream_procedure")]
fn macro_static_downstream(input: String) -> Vec<u8> {
    input.into_bytes()
}

flezha_runner::operative! {
    id: "sugar_static_entity",
    pipeline: input::<String> -> MacroStaticEntry -> MacroStaticDownstream -> plugin::<Vec<u8>>("test_kafka")
}

// Mixed operative: one `#[flezha::procedure]` procedure wired to one hand-written
// `impl Procedure` procedure in an explicit `channels()` body.
#[flezha_runner::procedure(id = "mixed_macro_entry_procedure")]
fn mixed_macro_entry(input: String) -> String {
    input
}

#[derive(Default)]
struct MixedHandDownstream;
impl Procedure for MixedHandDownstream {
    type Input = String;
    type Output = Vec<u8>;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "mixed_hand_downstream"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.emit::<Vec<u8>>(input.into_bytes())
    }
}

struct MixedOperative;
impl StreamOperative for MixedOperative {
    fn factory_id() -> &'static str {
        "mixed_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<MixedMacroEntry>()
            .named("hop")
            .into::<MixedHandDownstream>()
            .named("kafka")
            .output("test_kafka");
    }
}

#[cfg(test)]
mod macro_twin_tests {
    use std::collections::{HashMap, VecDeque};

    use flezha_runner::testing::ProcedureTestHarness;

    use super::*;

    // -----------------------------------------------------------------------
    // Canonical keyed-collection reference procedure (`expressive-state-journaling`,
    // authoring-contract §3 / §6): a rolling per-key window. The sugared form
    // uses `#[journal(map_deque)]` + an `Option<Output>` warmup gate;
    // `RollingWindowTwin` is the equivalent hand-written `Procedure`/`Change`. The
    // tests below assert the two are behaviourally identical and that the
    // sugared form survives `replay_equals`.
    // -----------------------------------------------------------------------

    const ROLLING_WINDOW_N: usize = 3;

    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
    struct RollingWindowState {
        #[journal(map_deque)]
        window: HashMap<String, VecDeque<f64>>,
    }

    #[flezha_runner::procedure(id = "rolling_window")]
    fn rolling_window(input: (String, f64), state: &mut RollingWindowState) -> Option<f64> {
        let (key, value) = input;
        state.push_back_window(key.clone(), value);
        let len = state.window.get(&key).map(|w| w.len()).unwrap_or(0);
        if len > ROLLING_WINDOW_N {
            state.pop_front_window(key.clone(), len - ROLLING_WINDOW_N);
        }
        let w = state.window.get(&key)?;
        if w.len() < ROLLING_WINDOW_N {
            return None;
        }
        Some(w.iter().sum::<f64>() / ROLLING_WINDOW_N as f64)
    }

    // Hand-written twin: the exact `Procedure` / `Change` impl the sugar expands to
    // (variant names differ; behaviour does not).
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug)]
    struct RollingWindowTwinState {
        window: HashMap<String, VecDeque<f64>>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    enum RollingWindowTwinChange {
        PushBack(String, f64),
        PopFront(String, usize),
    }
    impl Change for RollingWindowTwinChange {}

    #[derive(Default)]
    struct RollingWindowTwin;
    impl Procedure for RollingWindowTwin {
        type Input = (String, f64);
        type Output = f64;
        type State = RollingWindowTwinState;
        type Change = RollingWindowTwinChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "rolling_window_twin"
        }
        fn init_state(
            _: Option<&str>,
            _: Option<RestoreInitPayload<'_>>,
        ) -> RollingWindowTwinState {
            RollingWindowTwinState::default()
        }
        fn apply_change(state: &mut RollingWindowTwinState, change: &RollingWindowTwinChange) {
            match change {
                RollingWindowTwinChange::PushBack(k, v) => {
                    state.window.entry(k.clone()).or_default().push_back(*v);
                }
                RollingWindowTwinChange::PopFront(k, n) => {
                    let e = state.window.entry(k.clone()).or_default();
                    for _ in 0..(*n).min(e.len()) {
                        e.pop_front();
                    }
                }
            }
        }
        fn run(
            &mut self,
            input: (String, f64),
            mut state: StateHandle<'_, Self>,
            ctx: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            let (key, value) = input;
            state.apply(RollingWindowTwinChange::PushBack(key.clone(), value));
            let len = state.window.get(&key).map(|w| w.len()).unwrap_or(0);
            if len > ROLLING_WINDOW_N {
                state.apply(RollingWindowTwinChange::PopFront(
                    key.clone(),
                    len - ROLLING_WINDOW_N,
                ));
            }
            let Some(w) = state.window.get(&key) else {
                return Ok(());
            };
            if w.len() < ROLLING_WINDOW_N {
                return Ok(());
            }
            let mean = w.iter().sum::<f64>() / ROLLING_WINDOW_N as f64;
            ctx.emit::<f64>(mean)
        }
    }

    // Per-mode coverage for the remaining journaling modes (`map_set`,
    // `map_vec`), composed with a flat field on one `Change` enum.
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
    struct KeyedCollectionsState {
        #[journal(map_set)]
        last: HashMap<String, f64>,
        #[journal(map_vec)]
        closes: HashMap<String, Vec<f64>>,
        ticks: u64,
    }

    #[flezha_runner::procedure(id = "keyed_collections")]
    fn keyed_collections(input: (String, f64), state: &mut KeyedCollectionsState) {
        let (key, value) = input;
        state.set_last(key.clone(), value);
        state.push_closes(key.clone(), value);
        // Keep at most three closes per key; drain the overflow off the front.
        let len = state.closes.get(&key).map(|c| c.len()).unwrap_or(0);
        if len > 3 {
            state.drain_closes_front(key.clone(), len - 3);
        }
        state.set_ticks(state.ticks + 1);
    }

    // `init = fn` maps the static entity key to per-entity config the
    // macro's default (`Default::default()`) cannot supply. Mirrors the market
    // `sma`/`ohlc` procedures whose window / bucket come from the procedure key.
    #[derive(Serialize, Deserialize, Clone, Default, PartialEq, Debug, flezha_runner::State)]
    struct WindowedState {
        window: usize,
        #[journal(map_vec)]
        closes: HashMap<String, Vec<f64>>,
    }

    fn windowed_init(entity_key: Option<&str>) -> WindowedState {
        WindowedState {
            window: if entity_key == Some("win_2") { 2 } else { 3 },
            closes: HashMap::new(),
        }
    }

    fn windowed_part(_: &(String, f64)) -> Vec<&'static str> {
        vec!["win_2", "win_3"]
    }

    #[flezha_runner::procedure(
        id = "windowed",
        init = windowed_init,
        keys = ["win_2", "win_3"],
        entities_for = windowed_part
    )]
    fn windowed(input: (String, f64), state: &mut WindowedState) -> Option<f64> {
        let (key, value) = input;
        let window = state.window;
        state.push_closes(key.clone(), value);
        let len = state.closes.get(&key).map(|c| c.len()).unwrap_or(0);
        if len > window {
            state.drain_closes_front(key.clone(), len - window);
        }
        let c = state.closes.get(&key)?;
        if c.len() < window {
            return None;
        }
        Some(c.iter().sum::<f64>() / window as f64)
    }

    // Cross-SDK conformance (`docs/sdk_authoring_contract.md` §6): the reference
    // operative's procedures expand to this exact associated-type mapping. A divergence
    // is a compile error, not a silent drift. The Java SugarTwinTest asserts the
    // same mapping at runtime.
    fn _reference_operative_conformance()
    where
        MacroStaticEntry: Procedure<
            Input = String,
            Output = String,
            State = MacroStaticEntryState,
            Clock = WallClock,
            Config = (),
        >,
        MacroStaticDownstream: Procedure<
            Input = String,
            Output = Vec<u8>,
            State = (),
            Change = NoChange,
            Clock = WallClock,
            Config = (),
        >,
    {
    }

    #[test]
    fn sugar_do_nothing_emits_like_hand_written() {
        let inputs = vec!["alpha".to_string(), "beta".to_string()];
        let expected: Vec<Vec<u8>> = vec![b"alpha".to_vec(), b"beta".to_vec()];

        let mut sugar = ProcedureTestHarness::<SugarDoNothing>::new();
        sugar.apply_inputs(inputs.clone());
        sugar.expect_emits(&expected);

        let mut hand = ProcedureTestHarness::<DoNothingProcedure>::new();
        hand.apply_inputs(inputs);
        hand.expect_emits(&expected);
    }

    #[test]
    fn sugar_store_input_journals_and_emits() {
        let inputs = vec!["one".to_string(), "two".to_string()];

        let mut sugar = ProcedureTestHarness::<SugarStoreInput>::new();
        sugar.apply_inputs(inputs.clone());
        assert_eq!(sugar.state().text_inputs, inputs);
        sugar
            .replay_equals()
            .expect("derived change replay diverged");
        sugar.expect_emits(&inputs);
    }

    #[test]
    fn macro_static_entry_entities_and_journals() {
        // `entities_for` routes by first char; the procedure journals every input.
        assert_eq!(
            MacroStaticEntry::entities_for(&"apple".to_string()),
            vec!["a"]
        );
        assert_eq!(
            MacroStaticEntry::entities_for(&"zebra".to_string()),
            vec!["d"]
        );
        assert_eq!(MacroStaticEntry::entity_keys(), vec!["a", "b", "c", "d"]);

        let inputs = vec!["apple".to_string(), "avocado".to_string()];
        let mut h = ProcedureTestHarness::<MacroStaticEntry>::new();
        h.apply_inputs(inputs.clone());
        assert_eq!(h.state().seen, inputs);
        h.replay_equals().expect("derived change replay diverged");
        h.expect_emits(&inputs);
    }

    #[test]
    fn mixed_macro_and_hand_procedures_run() {
        // The macro entry procedure passes through; the hand procedure turns it to bytes.
        let mut entry = ProcedureTestHarness::<MixedMacroEntry>::new();
        entry.apply_inputs(vec!["hi".to_string()]);
        entry.expect_emits(&["hi".to_string()]);

        let mut down = ProcedureTestHarness::<MixedHandDownstream>::new();
        down.apply_inputs(vec!["hi".to_string()]);
        down.expect_emits(&[b"hi".to_vec()]);
    }

    #[test]
    fn sugar_operatives_register() {
        // Auto-wire + `operative!` operatives are harvested without `.define_stream()`.
        flezha_runner::register_inventory_operatives();
        let ids = flezha_runner::operative::registry::class_factory_ids();
        for id in [
            "sugar_do_nothing",
            "sugar_store_input",
            "sugar_static_entity",
        ] {
            assert!(
                ids.contains(&id),
                "auto-registered operative `{id}` missing"
            );
        }
        // The mixed operative is declared explicitly, as before.
        Flezha::create_runner("sugar_test")
            .unwrap()
            .define_stream::<MixedOperative>()
            .unwrap();
        assert!(flezha_runner::operative::registry::get_class("mixed_operative").is_some());
    }

    #[test]
    fn rolling_window_matches_hand_written_twin() {
        let inputs = vec![
            ("a".to_string(), 1.0),
            ("a".to_string(), 2.0),
            ("a".to_string(), 3.0),
            ("a".to_string(), 4.0),
            ("b".to_string(), 10.0),
            ("b".to_string(), 20.0),
            ("b".to_string(), 30.0),
        ];

        let mut sugar = ProcedureTestHarness::<RollingWindow>::new();
        sugar.apply_inputs(inputs.clone());

        let mut twin = ProcedureTestHarness::<RollingWindowTwin>::new();
        twin.apply_inputs(inputs);

        // The generated journaling replays deterministically (4.3).
        sugar.replay_equals().expect("sugared replay diverged");

        // Behaviourally identical to the hand-written twin: same state (4.2).
        assert_eq!(sugar.state().window, twin.state().window);

        // ...and the same emits (warmup gate via `Option<Output>`).
        let expected = [2.0_f64, 3.0, 20.0];
        sugar.expect_emits(&expected);
        twin.expect_emits(&expected);
    }

    #[test]
    fn keyed_collections_modes_journal_and_replay() {
        let inputs = vec![
            ("x".to_string(), 1.0),
            ("x".to_string(), 2.0),
            ("x".to_string(), 3.0),
            ("x".to_string(), 4.0),
            ("y".to_string(), 9.0),
        ];
        let mut h = ProcedureTestHarness::<KeyedCollections>::new();
        h.apply_inputs(inputs);

        // map_set keeps the latest per key.
        assert_eq!(h.state().last.get("x"), Some(&4.0));
        assert_eq!(h.state().last.get("y"), Some(&9.0));
        // map_vec push + drain_front keeps the last three per key.
        assert_eq!(h.state().closes.get("x"), Some(&vec![2.0, 3.0, 4.0]));
        assert_eq!(h.state().closes.get("y"), Some(&vec![9.0]));
        // The flat field composes into the same `Change` enum.
        assert_eq!(h.state().ticks, 5);

        h.replay_equals()
            .expect("keyed-collections replay diverged");
    }

    #[test]
    fn procedure_init_maps_entity_key_to_state() {
        // The `init = fn` hook receives the static entity key, so each
        // entity starts from key-derived config the macro default can't give.
        assert_eq!(Windowed::init_state(Some("win_2"), None).window, 2);
        assert_eq!(Windowed::init_state(Some("win_3"), None).window, 3);
        // Cold-start / harness default (key absent) falls through to the else arm.
        assert_eq!(Windowed::init_state(None, None).window, 3);

        // Behaviour differs per entity: window 2 emits one bar earlier than
        // window 3 for the same input stream, and replay stays deterministic.
        let inputs = vec![
            ("a".to_string(), 2.0),
            ("a".to_string(), 4.0),
            ("a".to_string(), 6.0),
        ];

        let mut w2 =
            ProcedureTestHarness::<Windowed>::with_initial_state(windowed_init(Some("win_2")));
        w2.apply_inputs(inputs.clone());
        w2.replay_equals().expect("windowed(win_2) replay diverged");
        w2.expect_emits(&[3.0_f64, 5.0]); // means of [2,4] then [4,6]

        let mut w3 =
            ProcedureTestHarness::<Windowed>::with_initial_state(windowed_init(Some("win_3")));
        w3.apply_inputs(inputs);
        w3.replay_equals().expect("windowed(win_3) replay diverged");
        w3.expect_emits(&[4.0_f64]); // single mean of [2,4,6]
    }
}
