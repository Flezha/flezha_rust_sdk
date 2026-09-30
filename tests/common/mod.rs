//! Shared test-support for the integration test binaries. Each test file
//! does `mod common;` + `use common::*;`. Collapses the build/register/
//! dispatch ritual that was copy-pasted across every dispatch test.

// Every binary does `mod common; use common::*;` and none of them uses all
// of it, so an unused item here is the normal case, not a defect.
#![allow(dead_code, unused_imports)]

pub mod contract;
mod tier;
mod timers;
mod wire;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flezha_runner::__dispatch_internals::OperatorDispatchHandle;
use flezha_runner::cell::flusher::{SendError, SendFrameFn};
use flezha_runner::cell::park::{self, ParkBody};
use flezha_runner::cell::state_sink::{install_global_sink, PARK_PAYLOAD_PAYLOAD_ID};
use flezha_runner::operator::builder::build_topology_for;
use flezha_runner::operator::registry::OperatorRegistry;
use flezha_runner::operator::{OperatorInstance, UnboundedOperator};
use flz_transport::transport::FlezhaStream;
pub use tier::*;
pub use timers::*;
pub use wire::*;

/// The header every dispatch binary would otherwise repeat: the authoring
/// surface, the dispatch internals, and this module's own fixtures, in one
/// `use common::prelude::*;`.
pub mod prelude {
    pub use std::collections::HashMap;
    pub use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    pub use std::sync::{Arc, Mutex};
    pub use std::time::{Duration, Instant};

    pub use flezha_runner::__dispatch_internals::OperatorDispatchHandle;
    pub use flezha_runner::channel::{ChannelBuilder, OverflowPolicy};
    pub use flezha_runner::operator::builder::build_topology_for;
    pub use flezha_runner::operator::registry::OperatorRegistry;
    pub use flezha_runner::operator::{OperatorInstance, UnboundedOperator};
    pub use flezha_runner::state::{NoChange, RestoreInitPayload};
    pub use flezha_runner::tasklet::{Ctx, Tasklet, TaskletError};

    pub use super::*;
}

/// A throwaway secondary stream for dispatch calls that never write back.
pub fn dummy_stream() -> Arc<FlezhaStream> {
    flz_core::bench_support::stream_pair().0
}

/// Spin-wait until `predicate` holds, or `timeout` elapses. The one polling
/// loop in these tests: a state read is the settle signal of last resort, and
/// its interval is the suite's one deliberate sleep.
pub fn wait_until<F: Fn() -> bool>(predicate: F, timeout: Duration) -> bool {
    let start = Instant::now();
    while !predicate() {
        if start.elapsed() > timeout {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

/// `true` when `happened` never holds inside `window` — the time-boxed negative
/// check that follows a positive settle, returning early the moment it fails.
pub fn stays_quiet<F: Fn() -> bool>(happened: F, window: Duration) -> bool {
    !wait_until(happened, window)
}

/// Build + register a single UnboundedOperator instance, returning a dispatch handle
/// and the live instance (for counter / state assertions).
pub fn setup<R: UnboundedOperator>(id: &str) -> (OperatorDispatchHandle, Arc<OperatorInstance>) {
    setup_partitioned::<R>(id, flezha_runner::operator::KeyGroupRange::UNPARTITIONED)
}

/// [`setup`] for one member of a partition group: the instance is built with
/// the key-group range its `AddOperator` would have carried, so the dispatch
/// guard is armed exactly as it is in production.
pub fn setup_partitioned<R: UnboundedOperator>(
    id: &str,
    partition: flezha_runner::operator::KeyGroupRange,
) -> (OperatorDispatchHandle, Arc<OperatorInstance>) {
    let topology = build_topology_for::<R>().expect("topology");
    let registry = Arc::new(OperatorRegistry::new());
    let instance = OperatorInstance::with_options(topology, Arc::from(id), None, partition);
    registry.register(Arc::clone(&instance));
    (OperatorDispatchHandle { registry }, instance)
}

/// Declare a trivial test Tasklet: `State` built via `Default`, no cell
/// keys / change type. The run body sees `(input, ctx)`. Use the `cap N` form
/// to set a non-default mailbox capacity.
#[macro_export]
macro_rules! test_tasklet {
    ($tasklet:ident, $name:tt, $input:ty => $output:ty, $state:ty,
     |$inp:tt, $ctx:tt| $body:block) => {
        #[derive(Default)]
        struct $tasklet;
        #[flezha_runner::tasklet(name = $name)]
        impl flezha_runner::tasklet::Tasklet for $tasklet {
            type Input = $input;
            type Output = $output;
            type State = $state;
            fn run(
                &mut self,
                $inp: $input,
                $ctx: &mut flezha_runner::Ctx<'_, Self>,
            ) -> ::std::result::Result<(), flezha_runner::tasklet::TaskletError> $body
        }
    };
    ($tasklet:ident, $name:tt, $input:ty => $output:ty, $state:ty, cap $cap:literal,
     |$inp:tt, $ctx:tt| $body:block) => {
        #[derive(Default)]
        struct $tasklet;
        #[flezha_runner::tasklet(name = $name, capacity = $cap)]
        impl flezha_runner::tasklet::Tasklet for $tasklet {
            type Input = $input;
            type Output = $output;
            type State = $state;
            fn run(
                &mut self,
                $inp: $input,
                $ctx: &mut flezha_runner::Ctx<'_, Self>,
            ) -> ::std::result::Result<(), flezha_runner::tasklet::TaskletError> $body
        }
    };
}

/// Dispatch one wire frame to `operator_id` on `channel`.
pub fn dispatch_input(
    handle: &OperatorDispatchHandle,
    operator_id: &str,
    channel: &str,
    payload: Vec<u8>,
    stream: &Arc<FlezhaStream>,
) {
    dispatch_marked(handle, operator_id, channel, payload, stream, &[]);
}

/// Dispatch one wire frame carrying extra metadata — the controller's
/// `flz.replay` / `flz.offset` / `flz.replay_end` marks ride there.
pub fn dispatch_marked(
    handle: &OperatorDispatchHandle,
    operator_id: &str,
    channel: &str,
    payload: Vec<u8>,
    stream: &Arc<FlezhaStream>,
    marks: &[(&str, &str)],
) {
    let mut md = HashMap::new();
    md.insert("channel".to_string(), channel.to_string());
    for (k, v) in marks {
        md.insert(k.to_string(), v.to_string());
    }
    handle.dispatch(operator_id, payload, &md, Arc::clone(stream));
}

/// Install a recording global sink whose park frames are auto-acked from a
/// watcher thread — the controller's fsync-then-ack, minus the controller.
/// Returns the `ParkBody`s it collected. Shared by every suite that drives a
/// payload to the dead-letter queue.
pub fn install_auto_acking_sink() -> Arc<Mutex<Vec<ParkBody>>> {
    let parked: Arc<Mutex<Vec<ParkBody>>> = Arc::new(Mutex::new(Vec::new()));
    let parked_c = Arc::clone(&parked);
    let sink: SendFrameFn = Arc::new(move |payload_id: &str, frame: &[u8]| {
        if payload_id == PARK_PAYLOAD_PAYLOAD_ID {
            let body = decode_park_body(frame).map_err(SendError::Other)?;
            let park_id = body.park_id.clone();
            let mut seen = parked_c.lock().unwrap();
            if !seen.iter().any(|b| b.park_id == park_id) {
                seen.push(body);
            }
            drop(seen);
            // Ack from another thread: the caller is blocked inside
            // `park_blocking` while this sink call returns.
            std::thread::spawn(move || park::complete_ack(&park_id));
        }
        Ok(())
    });
    install_global_sink(sink);
    parked
}

/// Parse the outer cell frame and postcard-decode the `ParkBody`.
/// Layout: `[pid_len:u16][pid][seq:u64][header:16][body_len:u32][body][crc:u32]`.
pub fn decode_park_body(frame: &[u8]) -> Result<ParkBody, String> {
    let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    let mut off = 2 + pid_len + 8 + 16;
    let body_len =
        u32::from_be_bytes([frame[off], frame[off + 1], frame[off + 2], frame[off + 3]]) as usize;
    off += 4;
    let body = &frame[off..off + body_len];
    postcard::from_bytes(body).map_err(|e| format!("park body decode: {e}"))
}

/// A String -> String no-op. Written out rather than declared through
/// `test_tasklet!` because a shared fixture has to be `pub` and the macro's
/// associated `type Input` may not leak a private one.
#[derive(Default)]
pub struct EchoTasklet;
#[flezha_runner::tasklet(name = "echo_tasklet")]
impl flezha_runner::tasklet::Tasklet for EchoTasklet {
    type Input = String;
    type Output = String;
    fn run(
        &mut self,
        _input: String,
        _ctx: &mut flezha_runner::Ctx<'_, Self>,
    ) -> Result<(), flezha_runner::tasklet::TaskletError> {
        Ok(())
    }
}

/// The smallest complete class: one entry channel into one echo tasklet,
/// single-cell. What a topology assertion reaches for when the topology
/// itself is not the subject.
pub struct SingleOperator;
impl UnboundedOperator for SingleOperator {
    fn factory_id() -> &'static str {
        "single_operator"
    }
    fn channels(c: &flezha_runner::channel::ChannelBuilder) {
        c.input::<String>("inp").into::<EchoTasklet>();
    }
}
