//! The Rust reference runner the e2e suite and the benches drive. Every
//! fixture family lives in its own module; this file is the entry point and
//! the registration table. The re-exports below are the one import list all of
//! them share, reached from a module as `use crate::*`.

pub(crate) use std::io::Read;
pub(crate) use std::sync::atomic::{AtomicUsize, Ordering};
pub(crate) use std::time::Duration;
pub(crate) use std::{env, io, thread};

pub(crate) use flezha_runner::state::{
    postcard, Change, CoalesceKey, NoChange, RestoreInitPayload, StateHandle, WallClock,
};
pub(crate) use flezha_runner::tasklet::{
    EventTimeFrom, Tasklet, TaskletCtx, TaskletError, TimeExtractor,
};
pub(crate) use flezha_runner::{
    ChannelBuilder, Flezha, FlezhaRunner, LoopMode, OverflowPolicy, Stream, StreamOperative,
    WorkflowOperative,
};
pub(crate) use serde::{Deserialize, Serialize};

mod cell;
mod channel;
mod conformance;
mod probe;
mod state;
mod sugar;
mod workflow;
mod workflow_loop;

use crate::cell::*;
use crate::channel::*;
use crate::probe::*;
use crate::state::*;
use crate::sugar::*;
use crate::workflow::*;
use crate::workflow_loop::*;

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
        .register_factory(DoNothingOperative)?
        .register_factory(StoreInputOperative)?
        .register_factory(CompareFilterOperative)?
        .register_factory(StaticCellOperative)?
        .register_factory(CellStressOperative)?
        .register_factory(BoundedMailboxDropNothingOperative)?
        .register_factory(BoundedMailboxDropNewestOperative)?
        .register_factory(BoundedMailboxDropOldestOperative)?
        .register_factory(StatefulCounterOperative)?
        .register_factory(CoalescingCounterOperative)?
        .register_factory(PersistenceOffOperative)?
        .register_factory(SleepingCounterOperative)?
        .register_factory(HybridChannelOperative)?
        .register_factory(MergedInputOperative)?
        .register_factory(TransformChainOperative)?
        .register_factory(ResumeProbeOperative)?
        .register_factory(SchemaEvolutionV1Operative)?
        .register_factory(SchemaEvolutionV2Operative)?
        .register_factory(DynamicCellOperative)?
        .register_factory(PartitionRelayOperative)?
        .register_factory(SessionTimeoutOperative)?
        .register_factory(MixedOperative)?
        .register_factory(TierStoreOperative)?
        .register_factory(BenchEchoOperative)?
        .register_factory(BenchKeyedOperative)?
        .register_factory(BenchKeyedNoPersistOperative)?
        .register_factory(CheckpointStoreOperative)?
        .register_factory(CheckpointBatchedOperative)?
        .register_factory(BenchCheckpointOperative)?
        .register_factory(PoisonProbeOperative)?
        .register_factory(WedgeProbeOperative)?
        .register_factory(CandleOperative)?
        .register_factory(SpawnRequesterOperative)?
        .register_factory(SpawnOnceRequesterOperative)?
        .register_factory(SugarDoNothing)?
        .register_factory(SugarStoreInput)?
        .register_factory(SugarNamedInput)?
        .register_factory(SugarStaticCell)?
        .register_factory(SugarWorkflow)?
        .register_factory(SugarWorkflowPerInput)?
        .register_factory(SugarStateOnly)?
        .register_factory(EchoWorkflow)?
        .register_factory(TimerLoopWorkflow)?
        .register_factory(EventTimerWorkflow)?
        .register_factory(StaticCellWorkflow)?
        .register_factory(CommitBlockWorkflow)?
        .register_factory(SyncDurableWorkflow)?
        .register_factory(OptOutCommitBlockWorkflow)?
        .register_factory(TierCommitBlockWorkflow)?
        .register_factory(SagaBlockWorkflow)?
        .register_factory(KeyedBlockWorkflow)?
        .register_factory(KeyedSagaWorkflow)?
        .register_factory(OutboxBlockWorkflow)?
        .register_factory(UndoOutboxWorkflow)?
        .register_factory(ChatLoopWorkflow)?
        .register_factory(StatelessLoopWorkflow)?
        .register_factory(ChatRewindWorkflow)?
        .register_factory(ReplyBlockLoopWorkflow)?
        .register_factory(TypedOnceWorkflow)?
        .register_factory(RetryWorkflow)?
        .register_factory(BranchWorkflow)?
        .register_factory(DiamondWorkflow)?
        .register_factory(BenchChain1)?
        .register_factory(BenchChain4)?
        .register_factory(BenchChain16)?
        .register_factory(BenchChainOptOut4)?
        .register_factory(BenchLoopWorkflow)?
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
            Err(e) => return format!("err {e}"),
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
