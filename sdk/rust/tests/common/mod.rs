//! Shared test-support for the integration test binaries. Each test file
//! does `mod common;` + `use common::*;`. Collapses the build/register/
//! dispatch ritual that was copy-pasted across every dispatch test.

#![allow(dead_code)]

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flezha_runner::__dispatch_internals::OperativeDispatchHandle;
use flezha_runner::entity::flusher::{SendError, SendFrameFn};
use flezha_runner::entity::park::{self, ParkBody};
use flezha_runner::entity::state_sink::{install_global_sink, PARK_PAYLOAD_PAYLOAD_ID};
use flezha_runner::operative::builder::build_topology_for;
use flezha_runner::operative::registry::OperativeRegistry;
use flezha_runner::operative::{UnboundedInstance, UnboundedOperative};
use flz_transport::transport::FlezhaStream;

/// A throwaway secondary stream for dispatch calls that never write back.
pub fn dummy_stream() -> Arc<FlezhaStream> {
    let (a, _b) = UnixStream::pair().expect("UnixStream::pair");
    Arc::new(FlezhaStream::Unix(a))
}

/// Spin-wait until `predicate` holds, or `timeout` elapses.
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

/// Build + register a single UnboundedOperative instance, returning a dispatch handle
/// and the live instance (for counter / state assertions).
pub fn setup<R: UnboundedOperative>(id: &str) -> (OperativeDispatchHandle, Arc<UnboundedInstance>) {
    let topology = build_topology_for::<R>().expect("topology");
    let registry = Arc::new(OperativeRegistry::new());
    let instance = UnboundedInstance::new(topology, Arc::from(id));
    registry.register_unbounded(Arc::clone(&instance));
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
