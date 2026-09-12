//! Durable structured workflow fixtures: the crash-resume family (commit,
//! saga, transactional outbox, branch, diamond and the retry ladder) plus the
//! run-once echo, and the release gate every blocking step parks on.

use crate::*;

// -- EchoWorkflow -----------------------------------------------------------------
//
// Run-once workflow operative exercised by the `workflow-kind` e2e: emits its args back as
// output once, then returns Ok so the SDK ships `TaskCompleted` and the
// controller deregisters it.
pub(crate) struct EchoWorkflow;

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
pub(crate) fn block_until_released() {
    while !e2e_released() {
        thread::sleep(Duration::from_millis(20));
    }
}

// SET-style Entity state: `apply` is idempotent, so re-running a procedure
// from the same args on resume is a no-op over the restored state (Model A
// idempotent re-run). `BTreeSet` keeps snapshot bytes deterministic.
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct SeenState {
    pub(crate) items: std::collections::BTreeSet<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Insert(pub(crate) String);
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

pub(crate) use durable_step;

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

pub(crate) struct CommitBlockWorkflow;
impl WorkflowOperative for CommitBlockWorkflow {
    fn factory_id() -> &'static str {
        "commit_block_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .named("mid")
            .into::<BlockB>();
    }
}

// `sync_durable_workflow`: the `commit_block_workflow` shape declaring
// SYNCHRONOUS remote durability (`remote-durability-tier`). The controller must
// have proc A's journal barrier on the durability backend before it acks the
// commit — which is what lets the kill-the-disk drill destroy the whole state
// directory and still resume with proc A committed rather than re-run.
pub(crate) struct SyncDurableWorkflow;
impl WorkflowOperative for SyncDurableWorkflow {
    const SYNC_DURABILITY: bool = true;
    fn factory_id() -> &'static str {
        "sync_durable_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .named("mid")
            .into::<BlockB>();
    }
}

// `optout_commit_block_workflow`: the opt-out twin of `commit_block_workflow`
// with a live mid-chain signal — proc A's output also reaches a plugin sink, so
// the crash-resume e2e can observe the mid-run point without durable state
// (there is none). A crash + resume re-runs the whole chain from the top, so
// the sink sees proc A's marker once per run and no journal/WAL file exists.
pub(crate) struct OptOutCommitBlockWorkflow;
impl WorkflowOperative for OptOutCommitBlockWorkflow {
    fn factory_id() -> &'static str {
        "optout_commit_block"
    }
    const PERSISTENCE_ENABLED: bool = false;
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .named("mid")
            .sink_with("optout_sink", |s: &String| {
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

pub(crate) struct TierCommitBlockWorkflow;
impl WorkflowOperative for TierCommitBlockWorkflow {
    fn factory_id() -> &'static str {
        "tier_commit_block_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.effectively_once();
        c.input::<String>("in")
            .into::<TierCommitA>()
            .named("mid")
            .keyed()
            .sink_with("tier_sink", |s: &String| s.as_bytes().to_vec())
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

pub(crate) struct SagaBlockWorkflow;
impl WorkflowOperative for SagaBlockWorkflow {
    fn factory_id() -> &'static str {
        "saga_block_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SagaA>()
            .named("s1")
            .into::<SagaB>()
            .named("s2")
            .into::<SagaC>();
    }
}

// `outbox_block_workflow`: the transactional-outbox fixture
// (`workflow-staged-commit-and-outbox`). Proc A commits durable state AND emits
// plugin-bound output on a hybrid channel — STAGED, so it reaches the sink only
// after the controller has journaled and fsynced A's `proc_committed` entry (with
// the output inside it) and flushed the outbox. Proc B blocks, so a SIGKILL lands
// with A committed + flushed and B unfinished.
pub(crate) struct OutboxBlockWorkflow;
impl WorkflowOperative for OutboxBlockWorkflow {
    fn factory_id() -> &'static str {
        "outbox_block_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .named("mid")
            .sink_with("outbox_sink", |s: &String| {
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

pub(crate) struct UndoOutboxWorkflow;
impl WorkflowOperative for UndoOutboxWorkflow {
    fn factory_id() -> &'static str {
        "undo_outbox_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<UndoOutboxA>()
            .named("mid")
            .sink_with("undo_sink", |s: &String| format!("out:{s}").into_bytes())
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
// The inter-proc channels are hybrid (`sink_with`), which is what makes
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

pub(crate) struct BranchWorkflow;
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
            .sink_with("branch_sink", |s: &String| format!("a:{s}").into_bytes());
        mid.into::<BranchB>()
            .named("bmid")
            .sink_with("branch_sink", |s: &String| format!("b:{s}").into_bytes())
            .into::<BranchD>()
            .named("d_out")
            .sink("branch_sink");
        mid.into::<BranchC>().named("c_out").sink("branch_sink");
    }
}

// `diamond_workflow`: the fan-in DAG.
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

pub(crate) struct DiamondWorkflow;
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
            .sink_with("diamond_sink", |s: &String| format!("a:{s}").into_bytes());
        fan.into::<DiamB1>()
            .named("db1")
            .into::<DiamB2>()
            .named("db2")
            .sink_with("diamond_sink", |s: &String| s.clone().into_bytes())
            .into::<DiamJoin>();
        fan.into::<DiamC>()
            .named("dc")
            .sink_with("diamond_sink", |s: &String| s.clone().into_bytes())
            .into::<DiamJoin>()
            .named("j_out")
            .sink("diamond_sink");
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

pub(crate) struct RetryWorkflow;
impl WorkflowOperative for RetryWorkflow {
    fn factory_id() -> &'static str {
        "retry_workflow"
    }
    fn procedures(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<RetryA>()
            .named("mid")
            .sink_with("retry_sink", |s: &String| format!("res:{s}").into_bytes())
            .into::<RetryFlaky>()
            .named("out")
            .sink("retry_sink");
    }
}
