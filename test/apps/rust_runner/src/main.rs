//! The Rust reference runner the e2e suite and the benches drive. Every
//! fixture family lives in its own module; this file is the entry point and
//! the registration table. The re-exports below are the one import list all of
//! them share, reached from a module as `use crate::*`.

pub(crate) use std::io::Read;
pub(crate) use std::sync::atomic::{AtomicUsize, Ordering};
pub(crate) use std::time::Duration;
pub(crate) use std::{env, io, thread};

pub(crate) use flezha_runner::procedure::{
    EventTimeFrom, Procedure, ProcedureCtx, ProcedureError, TimeExtractor,
};
pub(crate) use flezha_runner::state::{
    postcard, Change, CoalesceKey, NoChange, RestoreInitPayload, StateHandle, WallClock,
};
pub(crate) use flezha_runner::{
    ChannelBuilder, Flezha, FlezhaRunner, IdleObserver, LoopMode, OverflowPolicy, StreamOperative,
    WorkflowCtx, WorkflowError, WorkflowOperative,
};
pub(crate) use serde::{Deserialize, Serialize};

mod channel;
mod conformance;
mod entity;
mod probe;
mod state;
mod sugar;
mod workflow;
mod workflow_loop;

use crate::channel::*;
use crate::entity::*;
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
    arm_entry_idle_probe(&runner);
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
        .define_stream::<DoNothingOperative>()?
        .define_stream::<StoreInputOperative>()?
        .define_stream::<CompareFilterOperative>()?
        .define_stream::<StaticEntityOperative>()?
        .define_stream::<EntityStressOperative>()?
        .define_stream::<BoundedMailboxDropNothingOperative>()?
        .define_stream::<BoundedMailboxDropNewestOperative>()?
        .define_stream::<BoundedMailboxDropOldestOperative>()?
        .define_stream::<StatefulCounterOperative>()?
        .define_stream::<CoalescingCounterOperative>()?
        .define_stream::<PersistenceOffOperative>()?
        .define_stream::<HybridChannelOperative>()?
        .define_stream::<ResumeProbeOperative>()?
        .define_stream::<SchemaEvolutionV1Operative>()?
        .define_stream::<SchemaEvolutionV2Operative>()?
        .define_stream::<DynamicEntityOperative>()?
        .define_stream::<PartitionRelayOperative>()?
        .define_stream::<SessionTimeoutOperative>()?
        .define_stream::<MixedOperative>()?
        .define_stream::<TierStoreOperative>()?
        .define_stream::<BenchEchoOperative>()?
        .define_stream::<BenchKeyedOperative>()?
        .define_stream::<BenchKeyedNoPersistOperative>()?
        .define_stream::<CheckpointStoreOperative>()?
        .define_stream::<CheckpointBatchedOperative>()?
        .define_stream::<BenchCheckpointOperative>()?
        .define_stream::<PoisonProbeOperative>()?
        .define_stream::<WedgeProbeOperative>()?
        .define_stream::<CandleOperative>()?
        .define_stream::<EntryIdleProbeOperative>()?
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
        .define_workflow::<BenchChain1>()?
        .define_workflow::<BenchChain4>()?
        .define_workflow::<BenchChain16>()?
        .define_workflow::<BenchChainOptOut4>()?
        .define_workflow::<BenchLoopWorkflow>()?
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
