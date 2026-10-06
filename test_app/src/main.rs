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
    Durable, Each, Exactly, Flezha, FlezhaRunner, OnFailure, OverflowPolicy, Relay, RelaySettings,
    Run, SleepAfter, Stream,
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
            flezha_runner::relay::DEFAULT_SLEEP_AFTER,
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
mod part;
mod probe;
mod relay_call;
mod state;
mod steps;
mod steps_loop;
mod steps_timer;
mod sugar;

use crate::call::*;
use crate::cell::*;
use crate::channel::*;
use crate::part::*;
use crate::probe::*;
use crate::relay_call::*;
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
    // The local-send drill: `{"relay", "channel", "message", "when"}` sends
    // this app's own code makes with `local`, each once the file `when` names
    // exists, so the e2e can prove the relay's output live first.
    if let Ok(script) = env::var("FLEZHA_LOCAL_SEND_SCRIPT") {
        run_local_sends(script);
    }
    runner
        .register_factory(DoNothingRelay)?
        .register_factory(CompleteFlowRelay)?
        .register_factory(StoreInputRelay)?
        .register_factory(CompareFilterRelay)?
        .register_factory(StaticCellRelay)?
        .register_factory(CellStressRelay)?
        .register_factory(BoundedMailboxDropNothingRelay)?
        .register_factory(BoundedMailboxDropNewestRelay)?
        .register_factory(BoundedMailboxDropOldestRelay)?
        .register_factory(StatefulCounterRelay)?
        .register_factory(CoalescingCounterRelay)?
        .register_factory(PersistenceOffRelay)?
        .register_factory(SleepingCounterRelay)?
        .register_factory(HybridChannelRelay)?
        .register_factory(MergedInputRelay)?
        .register_factory(TransformChainRelay)?
        .register_factory(OrdersRelay)?
        .register_factory(CallEchoRelay)?
        .register_factory(CallDownstreamRelay)?
        .register_factory(CallSplitRelay)?
        .register_factory(CallTurnSteps)?
        .register_factory(CallPricingRelay)?
        .register_factory(CallOrdersRelay)?
        .register_factory(CallAgentSteps)?
        .register_factory(CallGatewayRelay)?
        .register_factory(CallToolSessionSteps)?
        .register_factory(PartEchoRelay)?
        .register_factory(PartEchoSteps)?
        .register_factory(ResumeProbeRelay)?
        .register_factory(SchemaEvolutionV1Relay)?
        .register_factory(SchemaEvolutionV2Relay)?
        .register_factory(DynamicCellRelay)?
        .register_factory(PartitionPassRelay)?
        .register_factory(SessionTimeoutRelay)?
        .register_factory(MixedRelay)?
        .register_factory(TierStoreRelay)?
        .register_factory(RemoteTierStoreRelay)?
        .register_factory(TierChainRelay)?
        .register_factory(BenchEchoRelay)?
        .register_factory(BenchKeyedRelay)?
        .register_factory(BenchKeyedNoPersistRelay)?
        .register_factory(CheckpointStoreRelay)?
        .register_factory(CheckpointBatchedRelay)?
        .register_factory(BenchCheckpointRelay)?
        .register_factory(PoisonProbeRelay)?
        .register_factory(WedgeProbeRelay)?
        .register_factory(CandleRelay)?
        .register_factory(SpawnRequesterRelay)?
        .register_factory(SpawnHoldRequesterRelay)?
        .register_factory(SpawnOnceRequesterRelay)?
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
        .register_factory(EffectBlockSteps)?
        .register_factory(EffectFlow)?
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
    let control = runner.control();
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
            println!("admin {verb} {}", admin_answer(&control, &verb, &args));
        }
    });
}

fn admin_answer(
    control: &flezha_runner::Control,
    verb: &str,
    args: &[serde_json::Value],
) -> String {
    for _ in 0..600 {
        match control.call(verb, args) {
            Ok(value) => return format!("ok {value}"),
            Err(flezha_runner::ControlError::Unavailable) => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => return format!("err {}: {e}", flezha_runner::ErrorModel::code(&e)),
        }
    }
    "err no control plane within 30s".to_string()
}

/// Each local send of the drill once its trigger file exists, printing
/// `local_send <relay> ok` or `local_send <relay> err <code>`; an entry with
/// `call` calls the channel with its `value` through the controller instead,
/// printing `call <relay> ok <answer>` or `call <relay> err <code>`.
fn run_local_sends(script: String) {
    thread::spawn(move || {
        let sends: Vec<serde_json::Value> =
            serde_json::from_str(&script).expect("FLEZHA_LOCAL_SEND_SCRIPT is a JSON array");
        for send in sends {
            let field = |name: &str| send[name].as_str().unwrap_or_default().to_string();
            let relay = field("relay");
            while !std::path::Path::new(&field("when")).exists() {
                thread::sleep(Duration::from_millis(50));
            }
            let runner = Flezha::current_runner().expect("the runner is serving");
            if send["call"] == true {
                let called: Result<serde_json::Value, _> =
                    runner.call(&relay, &field("channel"), &send["value"]);
                match called {
                    Ok(answer) => println!("call {relay} ok {answer}"),
                    Err(e) => println!("call {relay} err {}", flezha_runner::ErrorModel::code(&e)),
                }
                continue;
            }
            let opts = flezha_runner::SendOpts::new().local();
            match runner.send(&relay, &field("message"), &field("channel"), &opts) {
                Ok(_) => println!("local_send {relay} ok"),
                Err(e) => println!(
                    "local_send {relay} err {}",
                    flezha_runner::ErrorModel::code(&e)
                ),
            }
        }
    });
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
