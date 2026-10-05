//! Durable structured steps fixtures: the crash-resume family (commit,
//! saga, output identity, branch, diamond and the retry ladder) plus the
//! run-once echo, and the release gate every blocking step parks on.

use crate::*;

// -- Durable structured steps relays (crash-resume e2e, §7c.3) ------------
//
// `commit_block_steps` and `saga_block_steps` are durable-by-default,
// multi-tasklet steps relays that BLOCK mid-chain on the
// `FLEZHA_STEPS_E2E_RELEASE` env var so a SIGKILL of the runner lands with
// earlier tasklets' Cell state already committed to the WAL. The resumed
// runner is launched WITH the env set, so the respawned instance restores the
// committed state (crash-resume) and runs to completion. A shared gate keeps the
// crash window deterministic — no busy-spin, no wall-clock races.

const E2E_RELEASE_ENV: &str = "FLEZHA_STEPS_E2E_RELEASE";

pub(crate) fn e2e_released() -> bool {
    env::var(E2E_RELEASE_ENV).is_ok()
}

// Park a pool worker (never the driver thread) until the release env is set.
pub(crate) fn block_until_released() {
    while !e2e_released() {
        thread::sleep(Duration::from_millis(20));
    }
}

// SET-style Cell state: `apply` is idempotent, so re-running a tasklet
// from the same args on resume is a no-op over the restored state (Model A
// idempotent re-run). `BTreeSet` keeps snapshot bytes deterministic.
#[derive(Serialize, Deserialize, Clone, Default)]
pub(crate) struct SeenState {
    pub(crate) items: std::collections::BTreeSet<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct Insert(pub(crate) String);
impl Change<SeenState> for Insert {
    fn apply_to(&self, state: &mut SeenState) {
        state.items.insert(self.0.clone());
    }
}

// A durable `String -> String` step: commits one idempotent Change to the WAL
// (keyed by the task name, so a resume re-run is a no-op) and passes the input on.
// With `compensate: |ctx| ...` it declares a saga hook (the given body is the
// hook, returning `Result`; naming the ctx lets an undo emit its own record);
// the `commit_a` form omits it so a clean crash-resume re-runs forward instead
// of undoing.
macro_rules! durable_step {
    ($ty:ident, $name:tt $(, compensate: |$ctx:ident| $comp:expr)?) => {
        #[derive(Default)]
        struct $ty;
        #[flezha_runner::tasklet(name = $name)]
        impl Tasklet for $ty {
            type Input = String;
            type Output = String;
            type State = SeenState;
            type Change = Insert;
            fn run(
                &mut self,
                input: String,
                ctx: &mut Ctx<'_, Self>,
            ) -> Result<(), TaskletError> {
                ctx.change(Insert($name.to_string()));
                ctx.emit(input)
            }
            $(
                // A crash-resumed compensation reconstructs completed stages with
                // EMPTY outputs (a tasklet's collected output was never
                // durable), so a hook that records an undo does so unconditionally.
                fn compensate(
                    &mut self,
                    _out: Vec<std::sync::Arc<String>>,
                    $ctx: &mut Ctx<'_, Self>,
                ) -> Result<(), TaskletError> {
                    $comp
                }
            )?
        }
    };
}

pub(crate) use durable_step;

// A stateless `String -> String` step whose whole behaviour is `$run`, over
// the input and ctx it names.
macro_rules! stateless_step {
    ($ty:ident, $name:tt, |$input:ident, $ctx:ident| $run:expr) => {
        #[derive(Default)]
        struct $ty;
        #[flezha_runner::tasklet(name = $name)]
        impl Tasklet for $ty {
            type Input = String;
            type Output = String;
            fn run(
                &mut self,
                $input: String,
                $ctx: &mut Ctx<'_, Self>,
            ) -> Result<(), TaskletError> {
                $run
            }
        }
    };
}

pub(crate) use stateless_step;

// -- EchoSteps -----------------------------------------------------------------
//
// One tasklet, run once (the `steps-kind` e2e): emits its first message
// back, so the pass ends and the controller deregisters the instance.
stateless_step!(EchoStep, "echo", |input, ctx| ctx
    .emit(format!("echo:{input}")));

pub(crate) struct EchoSteps;
impl Relay for EchoSteps {
    fn factory_id() -> &'static str {
        "echo_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<EchoStep>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `static_cell_steps`: a per-input loop whose one tasklet declares three
// static cells and routes every message to two of them; each cell that runs
// emits `<cell key>:<message>`, so one message yields exactly two outputs.
#[derive(Default)]
struct StaticCellStep;
#[flezha_runner::tasklet(name = "static_cell_step", keys = ["a", "b", "c"])]
impl Tasklet for StaticCellStep {
    type Input = String;
    type Output = String;
    fn cells_for(_: &String) -> Vec<&'static str> {
        vec!["a", "b"]
    }
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let key = ctx.cell_key().unwrap_or_default().to_string();
        ctx.emit(format!("{key}:{input}"))
    }
}

pub(crate) struct StaticCellSteps;
impl Relay for StaticCellSteps {
    fn factory_id() -> &'static str {
        "static_cell_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<StaticCellStep>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::PerInput,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(600)),
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `commit_block_steps`: task A commits durable state, task B blocks until the
// resumed runner sets the release env — so a SIGKILL lands with task A's state
// already on the WAL and task B never finished. Completion is observed
// controller-side via the finished-job record.
durable_step!(CommitA, "commit_a");
stateless_step!(BlockB, "block_b", |_input, _ctx| {
    block_until_released();
    Ok(())
});

pub(crate) struct CommitBlockSteps;
impl Relay for CommitBlockSteps {
    fn factory_id() -> &'static str {
        "commit_block_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .named("mid")
            .into::<BlockB>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `sync_durable_steps`: the `commit_block_steps` shape declaring
// SYNCHRONOUS remote durability (`remote-durability-tier`). The controller must
// have task A's journal barrier on the durability backend before it acks the
// commit — which is what lets the kill-the-disk drill destroy the whole state
// directory and still resume with task A committed rather than re-run.
pub(crate) struct SyncDurableSteps;
impl Relay for SyncDurableSteps {
    fn factory_id() -> &'static str {
        "sync_durable_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .named("mid")
            .into::<BlockB>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: Durable::remote(),
            ..RelaySettings::default()
        }
    }
}

// `optout_commit_block_steps`: the opt-out twin of `commit_block_steps`
// with a live mid-chain signal — task A's output also reaches a plugin output, so
// the crash-resume e2e can observe the mid-run point without durable state
// (there is none). A crash + resume re-runs the whole chain from the top, so
// the output sees task A's marker once per run and no journal/WAL file exists.
pub(crate) struct OptOutCommitBlockSteps;
impl Relay for OptOutCommitBlockSteps {
    fn factory_id() -> &'static str {
        "optout_commit_block"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<CommitA>()
            .output_named_with("mid", |s: &String| format!("a-ran:{s}").into_bytes())
            .into::<BlockB>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: Durable::Memory,
            ..RelaySettings::default()
        }
    }
}

// `tier_commit_block_steps`: the hybrid mid-chain shape the conformance
// fixture's `commit_block_chain` twin is transcribed from. Task A commits
// durable state AND emits `committed:<input>` on a channel that both reaches a
// plugin output and feeds task B, which blocks until the resumed runner sets the
// release env.
#[derive(Default)]
struct TierCommitA;
#[flezha_runner::tasklet(name = "tier_commit_a")]
impl Tasklet for TierCommitA {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Insert("tier_commit_a".to_string()));
        ctx.emit(format!("committed:{input}"))
    }
}

pub(crate) struct TierCommitBlockSteps;
impl Relay for TierCommitBlockSteps {
    fn factory_id() -> &'static str {
        "tier_commit_block_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<TierCommitA>()
            .output_named_with("mid", |s: &String| s.as_bytes().to_vec())
            .into::<BlockB>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `saga_block_steps`: SA + SB commit durable state and compensate; SC fails,
// triggering reverse-order rollback (SB then SA). SA's compensate BLOCKS so a
// SIGKILL lands mid-rollback with SB's done-marker already durable — the respawn
// finishes SA and SKIPS the already-done SB.
durable_step!(SagaA, "saga_a", compensate: |_ctx| {
    block_until_released();
    Ok(())
});
durable_step!(SagaB, "saga_b", compensate: |_ctx| Ok(()));
stateless_step!(SagaC, "saga_c", |_input, _ctx| Err(TaskletError::Custom(
    "saga_c always fails".into()
)));

pub(crate) struct SagaBlockSteps;
impl Relay for SagaBlockSteps {
    fn factory_id() -> &'static str {
        "saga_block_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<SagaA>()
            .named("s1")
            .into::<SagaB>()
            .named("s2")
            .into::<SagaC>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `keyed_block_steps` / `keyed_saga_steps`: the entry's comma-separated
// key list is split into one value per key, and a `key_by` on that value mints
// one Cell per key of a durable compensable tasklet that declares no keys; each
// Cell commits one idempotent change. The third tasklet blocks until the
// release env, or fails, so the keyed stage is rolled back once per key.
stateless_step!(KeySplit, "key_split", |input, ctx| {
    for key in input.split(',') {
        ctx.emit(key.to_string())?;
    }
    Ok(())
});
durable_step!(KeyedCommit, "keyed_commit", compensate: |_ctx| Ok(()));
stateless_step!(KeyedFail, "keyed_fail", |_input, _ctx| Err(
    TaskletError::Custom("keyed_fail always fails".into())
));

fn keyed_commits(c: &ChannelBuilder) -> Stream<'_, String> {
    c.input::<String>("in")
        .into::<KeySplit>()
        .named("keys")
        .key_by(|key: &String| key.clone())
        .into::<KeyedCommit>()
        .named("committed")
}

pub(crate) struct KeyedBlockSteps;
impl Relay for KeyedBlockSteps {
    fn factory_id() -> &'static str {
        "keyed_block_steps"
    }
    fn channels(c: &ChannelBuilder) {
        keyed_commits(c).into::<BlockB>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

pub(crate) struct KeyedSagaSteps;
impl Relay for KeyedSagaSteps {
    fn factory_id() -> &'static str {
        "keyed_saga_steps"
    }
    fn channels(c: &ChannelBuilder) {
        keyed_commits(c).into::<KeyedFail>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `outbox_block_steps`: a steps output's identity. Task A commits
// durable state AND emits once, keyed `k` with one metadata pair, on a keyed
// hybrid channel; the output rides A's Cell record and reaches the output once
// A's stage commits. Task B blocks, so a SIGKILL lands with A committed and B
// unfinished.
#[derive(Default)]
struct OutboxA;
#[flezha_runner::tasklet(name = "commit_a")]
impl Tasklet for OutboxA {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Insert("commit_a".to_string()));
        let meta = std::collections::HashMap::from([("trace".to_string(), "t1".to_string())]);
        ctx.emit_keyed_with_meta("k", input, meta)
    }
}

pub(crate) struct OutboxBlockSteps;
impl Relay for OutboxBlockSteps {
    fn factory_id() -> &'static str {
        "outbox_block_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<OutboxA>()
            .keyed()
            .output_named_with("mid", |s: &String| format!("a-out:{s}").into_bytes())
            .into::<BlockB>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `undo_outbox_steps`: a compensation's undo output. Task UA commits, emits
// its forward output on a hybrid plugin channel, and declares a `compensate`
// hook that EMITS an undo record, which rides UA's Cell's own record and is
// released once the `compensated` entry naming it is durable. Task C fails
// immediately, so the chain always rolls back; the hook BLOCKS until the
// release env is set, so on the first runner the SIGKILL lands before the undo
// was written. The resumed runner re-runs the outstanding hook (its done marker
// never landed) and the undo is delivered exactly once, as a newer record of
// UA's Cell.
durable_step!(UndoOutboxA, "undo_outbox_a", compensate: |ctx| {
    block_until_released();
    ctx.emit("undone".to_string())
});

stateless_step!(UndoC, "undo_outbox_c", |_input, _ctx| Err(
    TaskletError::Custom("undo_outbox_c always fails".into())
));

pub(crate) struct UndoOutboxSteps;
impl Relay for UndoOutboxSteps {
    fn factory_id() -> &'static str {
        "undo_outbox_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<UndoOutboxA>()
            .output_named_with("mid", |s: &String| format!("out:{s}").into_bytes())
            .into::<UndoC>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `failed_stage_steps`: a failed stage delivers nothing. The entry's two
// values reach one `failed_stage` Cell in order: the first emits on a plugin
// channel, so the Cell's record carries an output; the second fails, so the
// stage never commits and no marker ever names that record.
stateless_step!(FailedStage, "failed_stage", |input, ctx| {
    if input == "b" {
        return Err(TaskletError::Custom("failed_stage fails on b".into()));
    }
    ctx.emit(input)
});

pub(crate) struct FailedStageSteps;
impl Relay for FailedStageSteps {
    fn factory_id() -> &'static str {
        "failed_stage_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<KeySplit>()
            .into::<FailedStage>()
            .output_named_with("mid", |s: &String| format!("lost:{s}").into_bytes());
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `branch_steps`: the tasklet TREE (`steps-branching`).
//
//     BranchA ─┬─▶ BranchB ─▶ BranchD
//              └─▶ BranchC
//
// Every tasklet commits durable state and compensates by emitting an undo
// record, so both tree behaviours are observable at one output:
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
// The inter-task channels are hybrid (`output_with`), which is what makes
// a non-leaf tasklet's forward and undo output visible at the output.
const BRANCH_FAIL_ENV: &str = "FLEZHA_BRANCH_FAIL";

durable_step!(BranchA, "branch_a", compensate: |ctx| ctx
    .emit("undo-a".to_string()));
durable_step!(BranchB, "branch_b", compensate: |ctx| ctx
    .emit("undo-b".to_string()));

// The sibling leaf: emits straight to the output, and its undo does too.
#[derive(Default)]
struct BranchC;
#[flezha_runner::tasklet(name = "branch_c")]
impl Tasklet for BranchC {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Insert("branch_c".to_string()));
        ctx.emit(format!("c:{input}"))
    }
    fn compensate(
        &mut self,
        _out: Vec<std::sync::Arc<String>>,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        ctx.emit("undo-c".to_string())
    }
}

// The deep leaf under BranchB: blocks (crash-resume), or fails (compensation).
#[derive(Default)]
struct BranchD;
#[flezha_runner::tasklet(name = "branch_d")]
impl Tasklet for BranchD {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if env::var(BRANCH_FAIL_ENV).is_ok() {
            return Err(TaskletError::Custom("branch_d failed".into()));
        }
        block_until_released();
        ctx.emit(format!("d:{input}"))
    }
}

pub(crate) struct BranchSteps;
impl Relay for BranchSteps {
    fn factory_id() -> &'static str {
        "branch_steps"
    }
    fn channels(c: &ChannelBuilder) {
        // One channel, two consumers: THE fan-out. Both branches read exactly
        // what BranchA released on it (held handle, wired twice).
        let mid = c
            .input::<String>("in")
            .into::<BranchA>()
            .output_named_with("mid", |s: &String| format!("a:{s}").into_bytes());
        // Fail fast: the compensation scenario is about the rollback order,
        // not the ladder that precedes it.
        mid.into::<BranchB>()
            .output_named_with("bmid", |s: &String| format!("b:{s}").into_bytes())
            .retry_max(1)
            .into::<BranchD>()
            .output_named("d_out");
        mid.into::<BranchC>().output_named("c_out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `diamond_steps`: the fan-in DAG.
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
#[flezha_runner::tasklet(name = "diam_b2")]
impl Tasklet for DiamB2 {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        block_until_released();
        ctx.change(Insert("diam_b".to_string()));
        ctx.emit(format!("b:{input}"))
    }
}

#[derive(Default)]
struct DiamC;
#[flezha_runner::tasklet(name = "diam_c")]
impl Tasklet for DiamC {
    type Input = String;
    type Output = String;
    type State = SeenState;
    type Change = Insert;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Insert("diam_c".to_string()));
        ctx.emit(format!("c:{input}"))
    }
}

// The join: one `j:`-tagged record per accumulated contribution.
#[derive(Default)]
struct DiamJoin;
#[flezha_runner::tasklet(name = "diam_join")]
impl Tasklet for DiamJoin {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("j:{input}"))
    }
}

pub(crate) struct DiamondSteps;
impl Relay for DiamondSteps {
    fn factory_id() -> &'static str {
        "diamond_steps"
    }
    fn channels(c: &ChannelBuilder) {
        // Held handle wired twice = fan-out; two streams naming DiamJoin
        // (unnamed, same class) = the fan-in.
        let fan = c
            .input::<String>("in")
            .into::<DiamA>()
            .output_named_with("dfan", |s: &String| format!("a:{s}").into_bytes());
        fan.into::<DiamB1>()
            .named("db1")
            .into::<DiamB2>()
            .output_named_with("db2", |s: &String| s.clone().into_bytes())
            .into::<DiamJoin>();
        fan.into::<DiamC>()
            .output_named_with("dc", |s: &String| s.clone().into_bytes())
            .into::<DiamJoin>()
            .output_named("j_out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}

// `retry_steps`: the per-cell retry ladder (`steps-cell-retry`). Task RA
// commits durable state, forwards on a hybrid channel, and compensates by
// emitting an undo record; task `retry_flaky` fails its first
// `FLEZHA_STEPS_RETRY_FAILS` attempts (0 by default — the env is what each
// scenario arms) and then emits `attempt:<n>:<input>`. One armed failure is
// absorbed by the ladder (the run completes `ok`, the output sees attempt 2);
// arming more failures than the ladder has attempts exhausts it — RA rolls back
// and the run reports `error`.
const RETRY_FAILS_ENV: &str = "FLEZHA_STEPS_RETRY_FAILS";

// Attempts the flaky tasklet has made in this process (a retry re-runs it in
// place, so an in-memory counter is the whole ladder's view).
static RETRY_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

fn armed_failures() -> usize {
    env::var(RETRY_FAILS_ENV)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

durable_step!(RetryA, "retry_a", compensate: |ctx| ctx
    .emit("undone".to_string()));

#[derive(Default)]
struct RetryFlaky;
#[flezha_runner::tasklet(name = "retry_flaky")]
impl Tasklet for RetryFlaky {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let attempt = RETRY_ATTEMPTS.fetch_add(1, Ordering::SeqCst) + 1;
        if attempt <= armed_failures() {
            return Err(TaskletError::Custom(format!("flaky attempt {attempt}")));
        }
        ctx.emit(format!("attempt:{attempt}:{input}"))
    }
}

pub(crate) struct RetrySteps;
impl Relay for RetrySteps {
    fn factory_id() -> &'static str {
        "retry_steps"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<RetryA>()
            .output_named_with("mid", |s: &String| format!("res:{s}").into_bytes())
            .into::<RetryFlaky>()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            durable: fixture_durable(),
            ..RelaySettings::default()
        }
    }
}
