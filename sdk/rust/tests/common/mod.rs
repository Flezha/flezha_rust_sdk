//! Shared test-support for the integration test binaries. Each test file
//! does `mod common;` + `use common::*;`. Collapses the build/register/
//! dispatch ritual that was copy-pasted across every dispatch test.

// Every binary does `mod common; use common::*;` and none of them uses all
// of it, so an unused item here is the normal case, not a defect.
#![allow(dead_code, unused_imports)]

mod tier;
mod timers;
mod wire;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flezha_runner::__dispatch_internals::OperativeDispatchHandle;
use flezha_runner::entity::flusher::{SendError, SendFrameFn};
use flezha_runner::entity::park::{self, ParkBody};
use flezha_runner::entity::state_sink::{install_global_sink, PARK_PAYLOAD_PAYLOAD_ID};
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::operative::registry::OperativeRegistry;
use flezha_runner::operative::{StreamInstance, StreamOperative};
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

    pub use flezha_runner::__dispatch_internals::OperativeDispatchHandle;
    pub use flezha_runner::channel::{ChannelBuilder, OverflowPolicy};
    pub use flezha_runner::operative::builder::build_topology_for;
    pub use flezha_runner::operative::registry::OperativeRegistry;
    pub use flezha_runner::operative::{StreamInstance, StreamOperative};
    pub use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
    pub use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

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

/// Build + register a single StreamOperative instance, returning a dispatch handle
/// and the live instance (for counter / state assertions).
pub fn setup<R: StreamOperative>(id: &str) -> (OperativeDispatchHandle, Arc<StreamInstance>) {
    setup_partitioned::<R>(id, flezha_runner::operative::KeyGroupRange::UNPARTITIONED)
}

/// [`setup`] for one member of a partition group: the instance is built with
/// the key-group range its `AddOperative` would have carried, so the dispatch
/// guard is armed exactly as it is in production.
pub fn setup_partitioned<R: StreamOperative>(
    id: &str,
    partition: flezha_runner::operative::KeyGroupRange,
) -> (OperativeDispatchHandle, Arc<StreamInstance>) {
    let topology = build_topology_for::<R>().expect("topology");
    let registry = Arc::new(OperativeRegistry::new());
    let instance = StreamInstance::with_options(topology, Arc::from(id), None, partition);
    registry.register_stream(Arc::clone(&instance));
    (OperativeDispatchHandle { registry }, instance)
}

/// Declare a trivial test Procedure: `State` built via `Default`, no entity
/// keys / change type. The run body sees `(input, state, ctx)`. Use the
/// `cap N` form to set a non-default mailbox capacity.
#[macro_export]
macro_rules! test_procedure {
    ($procedure:ident, $name:literal, $input:ty => $output:ty, $state:ty,
     |$inp:tt, $st:tt, $ctx:tt| $body:block) => {
        #[derive(Default)]
        struct $procedure;
        impl flezha_runner::procedure::Procedure for $procedure {
            type Input = $input;
            type Output = $output;
            type State = $state;
            type Change = flezha_runner::state::NoChange;
            type Clock = flezha_runner::state::WallClock;
            type Config = ();
            fn name() -> &'static str { $name }
            fn init_state(
                _: ::std::option::Option<&str>,
                _: ::std::option::Option<flezha_runner::state::RestoreInitPayload<'_>>,
            ) -> $state {
                ::std::default::Default::default()
            }
            fn run(
                &mut self,
                $inp: $input,
                $st: flezha_runner::procedure::StateHandle<'_, Self>,
                $ctx: &mut flezha_runner::procedure::ProcedureCtx<'_>,
            ) -> ::std::result::Result<(), flezha_runner::procedure::ProcedureError> $body
        }
    };
    ($procedure:ident, $name:literal, $input:ty => $output:ty, $state:ty, cap $cap:literal,
     |$inp:tt, $st:tt, $ctx:tt| $body:block) => {
        #[derive(Default)]
        struct $procedure;
        impl flezha_runner::procedure::Procedure for $procedure {
            type Input = $input;
            type Output = $output;
            type State = $state;
            type Change = flezha_runner::state::NoChange;
            type Clock = flezha_runner::state::WallClock;
            type Config = ();
            fn name() -> &'static str { $name }
            fn init_state(
                _: ::std::option::Option<&str>,
                _: ::std::option::Option<flezha_runner::state::RestoreInitPayload<'_>>,
            ) -> $state {
                ::std::default::Default::default()
            }
            fn mailbox_capacity() -> usize { $cap }
            const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
            fn run(
                &mut self,
                $inp: $input,
                $st: flezha_runner::procedure::StateHandle<'_, Self>,
                $ctx: &mut flezha_runner::procedure::ProcedureCtx<'_>,
            ) -> ::std::result::Result<(), flezha_runner::procedure::ProcedureError> $body
        }
    };
}

/// Dispatch one wire frame to `operative_id` on `channel`.
pub fn dispatch_input(
    handle: &OperativeDispatchHandle,
    operative_id: &str,
    channel: &str,
    payload: Vec<u8>,
    stream: &Arc<FlezhaStream>,
) {
    dispatch_marked(handle, operative_id, channel, payload, stream, &[]);
}

/// Dispatch one wire frame carrying extra metadata — the controller's
/// `flz.replay` / `flz.offset` / `flz.replay_end` marks ride there.
pub fn dispatch_marked(
    handle: &OperativeDispatchHandle,
    operative_id: &str,
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
    handle.dispatch(operative_id, payload, &md, Arc::clone(stream));
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

/// Parse the outer entity frame and postcard-decode the `ParkBody`.
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
/// `test_procedure!` because a shared fixture has to be `pub` and the macro's
/// associated `type Input` may not leak a private one.
#[derive(Default)]
pub struct EchoProcedure;
impl flezha_runner::procedure::Procedure for EchoProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = flezha_runner::state::NoChange;
    type Clock = flezha_runner::state::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "echo_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<flezha_runner::state::RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: String,
        _state: flezha_runner::procedure::StateHandle<'_, Self>,
        _ctx: &mut flezha_runner::procedure::ProcedureCtx<'_>,
    ) -> Result<(), flezha_runner::procedure::ProcedureError> {
        Ok(())
    }
}

/// The smallest complete class: one entry channel into one echo procedure,
/// single-entity. What a topology assertion reaches for when the topology
/// itself is not the subject.
pub struct SingleOperative;
impl StreamOperative for SingleOperative {
    fn factory_id() -> &'static str {
        "single_operative"
    }
    fn channels(c: &flezha_runner::channel::ChannelBuilder) {
        c.input::<String>("inp").into::<EchoProcedure>();
    }
}
