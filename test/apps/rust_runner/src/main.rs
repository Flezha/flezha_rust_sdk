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
    ChannelBuilder, Flezha, LoopMode, OverflowPolicy, StreamOperative, WorkflowCtx, WorkflowError,
    WorkflowOperative,
};
pub(crate) use serde::{Deserialize, Serialize};

mod channel;
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
    spawn_stdin_eof_exit();
    Flezha::create_runner(namespace())?
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
