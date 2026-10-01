//! The Rust reference runner the e2e suite and the benches drive. Every
//! fixture family lives in its own module; this file is the entry point and
//! the registration table. The re-exports below are the one import list all of
//! them share, reached from a module as `use crate::*`.

pub(crate) use std::io::Read;
pub(crate) use std::sync::atomic::{AtomicUsize, Ordering};
pub(crate) use std::time::Duration;
pub(crate) use std::{env, io, thread};

pub(crate) use flezha_runner::state::{postcard, Change, ClockKind, CoalesceKey, NoChange};
pub(crate) use flezha_runner::tasklet::{Ctx, Tasklet, TaskletError};
pub(crate) use flezha_runner::{
    AckGranularity, BeyondWindowPolicy, ChannelBuilder, CheckpointOutputs, CheckpointPolicy,
    Durable, Each, Exactly, Flezha, FlezhaRunner, OnFailure, Operator, OperatorSettings,
    OverflowPolicy, Run, SleepAfter, Stream,
};
pub(crate) use serde::{Deserialize, Serialize};

/// Where a sleeping fixture keeps its work: locally, sleeping after the class
/// default unless the drill or the bench driving it shortens the interval
/// through `FLEZHA_FIXTURE_SLEEP_AFTER_MS`.
pub(crate) fn fixture_durable() -> Durable {
    let sleep_after = env::var("FLEZHA_FIXTURE_SLEEP_AFTER_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(
            flezha_runner::operator::DEFAULT_SLEEP_AFTER,
            Duration::from_millis,
        );
    Durable::Local {
        sleep_after: SleepAfter::After(sleep_after),
    }
}

mod call;
mod cell;
mod channel;
mod conformance;
mod inner;
mod operator_call;
mod part;
mod probe;
mod state;
mod steps;
mod steps_loop;
mod steps_timer;
mod sugar;

use crate::call::*;
use crate::cell::*;
use crate::channel::*;
use crate::inner::*;
use crate::operator_call::*;
use crate::part::*;
use crate::probe::*;
use crate::state::*;
use crate::steps::*;
use crate::steps_loop::*;
use crate::steps_timer::*;
use crate::sugar::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // The SDK installs no subscriber of its own, so without this every
    // `tracing` event it and the shared kernel emit — the reconnect ring's,
    // the flood ladder's, the mailbox stall warning — is written to a
    // subscriber that does not exist, and the drills that capture this
    // process's output see none of them.
    flezha_runner::diag::init_default_tracing();
    // The conformance run answers a file and exits; it never connects. The
    // drill names the path in the environment, which reaches the app whatever
    // shell the manifest's launch line goes through.
    if let Some(path) = env::var("FLEZHA_CONFORMANCE_REPORT")
        .ok()
        .filter(|path| !path.is_empty())
        .or_else(|| flag_value("--conformance-report"))
    {
        return conformance::write_report(&path);
    }
    spawn_stdin_eof_exit();
    let runner = Flezha::create_runner(namespace())?;
    // One row of the refusal table instead of the fixture set: a refused
    // topology must not reach a connected runner.
    if let Ok(topology) = env::var("FLEZHA_REFUSAL_FIXTURE") {
        conformance::register_only(&runner, &topology)?;
        runner.run()?;
        return Ok(());
    }
    // The admin drill: the e2e names a JSON array of `{"verb", "args"}` calls
    // in the environment and reads the answers off stdout, so what it watches
    // is an application administering its own namespace rather than a test
    // calling the controller directly.
    if let Ok(script) = env::var("FLEZHA_ADMIN_SCRIPT") {
        run_admin_script(&runner, script);
    }
    runner
        .register_factory(DoNothingOperator)?
        .register_factory(CompleteFlowOperator)?
        .register_factory(StoreInputOperator)?
        .register_factory(CompareFilterOperator)?
        .register_factory(StaticCellOperator)?
        .register_factory(CellStressOperator)?
        .register_factory(BoundedMailboxDropNothingOperator)?
        .register_factory(BoundedMailboxDropNewestOperator)?
        .register_factory(BoundedMailboxDropOldestOperator)?
        .register_factory(StatefulCounterOperator)?
        .register_factory(CoalescingCounterOperator)?
        .register_factory(PersistenceOffOperator)?
        .register_factory(SleepingCounterOperator)?
        .register_factory(GatewayOperator)?
        .register_factory(LoopGatewayOperator)?
        .register_factory(HybridChannelOperator)?
        .register_factory(MergedInputOperator)?
        .register_factory(TransformChainOperator)?
        .register_factory(CallEchoOperator)?
        .register_factory(CallDownstreamOperator)?
        .register_factory(CallSplitOperator)?
        .register_factory(CallTurnSteps)?
        .register_factory(CallPricingOperator)?
        .register_factory(CallOrdersOperator)?
        .register_factory(CallAgentSteps)?
        .register_factory(CallGatewayOperator)?
        .register_factory(CallToolSessionSteps)?
        .register_factory(PartEchoOperator)?
        .register_factory(PartEchoSteps)?
        .register_factory(ResumeProbeOperator)?
        .register_factory(SchemaEvolutionV1Operator)?
        .register_factory(SchemaEvolutionV2Operator)?
        .register_factory(DynamicCellOperator)?
        .register_factory(PartitionRelayOperator)?
        .register_factory(SessionTimeoutOperator)?
        .register_factory(MixedOperator)?
        .register_factory(TierStoreOperator)?
        .register_factory(RemoteTierStoreOperator)?
        .register_factory(TierChainOperator)?
        .register_factory(BenchEchoOperator)?
        .register_factory(BenchKeyedOperator)?
        .register_factory(BenchKeyedNoPersistOperator)?
        .register_factory(CheckpointStoreOperator)?
        .register_factory(CheckpointBatchedOperator)?
        .register_factory(BenchCheckpointOperator)?
        .register_factory(PoisonProbeOperator)?
        .register_factory(WedgeProbeOperator)?
        .register_factory(CandleOperator)?
        .register_factory(SpawnRequesterOperator)?
        .register_factory(SpawnHoldRequesterOperator)?
        .register_factory(SpawnOnceRequesterOperator)?
        .register_factory(SugarDoNothing)?
        .register_factory(SugarStoreInput)?
        .register_factory(SugarNamedInput)?
        .register_factory(SugarStaticCell)?
        .register_factory(SugarSteps)?
        .register_factory(SugarStepsPerInput)?
        .register_factory(SugarStateOnly)?
        .register_factory(EchoSteps)?
        .register_factory(TimerLoopSteps)?
        .register_factory(EventTimerSteps)?
        .register_factory(StaticCellSteps)?
        .register_factory(CommitBlockSteps)?
        .register_factory(SyncDurableSteps)?
        .register_factory(OptOutCommitBlockSteps)?
        .register_factory(TierCommitBlockSteps)?
        .register_factory(SagaBlockSteps)?
        .register_factory(KeyedBlockSteps)?
        .register_factory(KeyedSagaSteps)?
        .register_factory(OutboxBlockSteps)?
        .register_factory(UndoOutboxSteps)?
        .register_factory(FailedStageSteps)?
        .register_factory(ChatLoopSteps)?
        .register_factory(ChainedSteps)?
        .register_factory(StatelessLoopSteps)?
        .register_factory(KeyedLoopSteps)?
        .register_factory(ChatRewindSteps)?
        .register_factory(FailingTurnLoopSteps)?
        .register_factory(FailingTurnUndoAllSteps)?
        .register_factory(ReplyBlockLoopSteps)?
        .register_factory(TypedOnceSteps)?
        .register_factory(RetrySteps)?
        .register_factory(BranchSteps)?
        .register_factory(DiamondSteps)?
        .register_factory(BenchChain1)?
        .register_factory(BenchChain4)?
        .register_factory(BenchChain16)?
        .register_factory(BenchChainOptOut4)?
        .register_factory(BenchLoopSteps)?
        .run()?;
    Ok(())
}

/// Run the admin drill on its own thread once the control plane is up. The
/// first calls race the connect, which answers `Unavailable`; retrying on that
/// one refusal is what synchronises with it, and every other answer is the
/// controller's and is printed.
fn run_admin_script(runner: &FlezhaRunner, script: String) {
    let admin = runner.admin();
    thread::spawn(move || {
        let calls: Vec<serde_json::Value> =
            serde_json::from_str(&script).expect("FLEZHA_ADMIN_SCRIPT is a JSON array of calls");
        for call in calls {
            // `after_ms` holds the drill between two calls, which is what lets
            // a scenario change the role binding mid-session and see the next
            // call answer differently with no reconnect.
            if let Some(pause) = call["after_ms"].as_u64() {
                thread::sleep(Duration::from_millis(pause));
            }
            let verb = call["verb"].as_str().unwrap_or_default().to_string();
            let args: Vec<serde_json::Value> = call["args"].as_array().cloned().unwrap_or_default();
            println!("admin {verb} {}", admin_answer(&admin, &verb, &args));
        }
    });
}

fn admin_answer(admin: &flezha_runner::Admin, verb: &str, args: &[serde_json::Value]) -> String {
    for _ in 0..600 {
        match admin.call(verb, args) {
            Ok(value) => return format!("ok {value}"),
            Err(flezha_runner::admin::AdminError::Unavailable) => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return format!("err {}: {e}", flezha_runner::ErrorModel::code(&e)),
        }
    }
    "err no control plane within 30s".to_string()
}

fn namespace() -> String {
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 {
        return args[1].clone();
    }
    "default".to_string()
}

/// The value of `--flag <value>`, wherever it sits in the argument list — the
/// namespace is positional and every mode is selected after it.
fn flag_value(flag: &str) -> Option<String> {
    let mut args = env::args().skip_while(|arg| arg != flag);
    args.next()?;
    args.next()
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
