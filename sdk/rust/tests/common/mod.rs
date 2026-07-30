//! Shared test-support for the integration test binaries. Each test file
//! does `mod common;` + `use common::*;`. Collapses the build/register/
//! dispatch ritual that was copy-pasted across every dispatch test.

#![allow(dead_code)]

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flezha_runner::__dispatch_internals::{OperativeDispatchHandle, WorkStealingPool};
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
pub fn setup<R: UnboundedOperative>(
    id: &str,
    pool_size: usize,
) -> (OperativeDispatchHandle, Arc<UnboundedInstance>) {
    let topology = build_topology_for::<R>().expect("topology");
    let pool = Arc::new(WorkStealingPool::with_size(pool_size));
    let registry = Arc::new(OperativeRegistry::new());
    let instance = UnboundedInstance::new(topology, Arc::from(id), pool);
    registry.register_unbounded(Arc::clone(&instance));
    (OperativeDispatchHandle { registry }, instance)
}

/// Declare a trivial test Operation: `State` built via `Default`, no segment
/// keys / change type. The run body sees `(input, state, ctx)`. Use the
/// `cap N` form to set a non-default mailbox capacity.
#[macro_export]
macro_rules! test_operation {
    ($operation:ident, $name:literal, $input:ty => $output:ty, $state:ty,
     |$inp:tt, $st:tt, $ctx:tt| $body:block) => {
        #[derive(Default)]
        struct $operation;
        impl flezha_runner::operation::Operation for $operation {
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
                $st: flezha_runner::operation::StateHandle<'_, Self>,
                $ctx: &mut flezha_runner::operation::OperationCtx<'_>,
            ) -> ::std::result::Result<(), flezha_runner::operation::OperationError> $body
        }
    };
    ($operation:ident, $name:literal, $input:ty => $output:ty, $state:ty, cap $cap:literal,
     |$inp:tt, $st:tt, $ctx:tt| $body:block) => {
        #[derive(Default)]
        struct $operation;
        impl flezha_runner::operation::Operation for $operation {
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
                $st: flezha_runner::operation::StateHandle<'_, Self>,
                $ctx: &mut flezha_runner::operation::OperationCtx<'_>,
            ) -> ::std::result::Result<(), flezha_runner::operation::OperationError> $body
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
