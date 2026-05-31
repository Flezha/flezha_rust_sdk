//! Shared test-support for the integration test binaries. Each test file
//! does `mod common;` + `use common::*;`. Collapses the build/register/
//! dispatch ritual that was copy-pasted across every dispatch test.

#![allow(dead_code)]

use std::collections::HashMap;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::time::{Duration, Instant};

use flezha_executor::__dispatch_internals::{RelayDispatchHandle, WorkStealingPool};
use flezha_executor::relay::builder::build_topology_for;
use flezha_executor::relay::registry::RelayInstanceRegistry;
use flezha_executor::relay::{Relay, RelayInstance};
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

/// Build + register a single Relay instance, returning a dispatch handle
/// and the live instance (for counter / state assertions).
pub fn setup<R: Relay>(id: &str, pool_size: usize) -> (RelayDispatchHandle, Arc<RelayInstance>) {
    let topology = build_topology_for::<R>().expect("topology");
    let pool = Arc::new(WorkStealingPool::with_size(pool_size));
    let registry = Arc::new(RelayInstanceRegistry::new());
    let instance = RelayInstance::new(topology, Arc::from(id), pool);
    registry.register(Arc::clone(&instance));
    (RelayDispatchHandle { registry }, instance)
}

/// Declare a trivial test Shard: `State` built via `Default`, no partition
/// keys / change type. The run body sees `(input, state, ctx)`. Use the
/// `cap N` form to set a non-default mailbox capacity.
#[macro_export]
macro_rules! test_shard {
    ($shard:ident, $name:literal, $input:ty => $output:ty, $state:ty,
     |$inp:tt, $st:tt, $ctx:tt| $body:block) => {
        #[derive(Default)]
        struct $shard;
        impl flezha_executor::shard::Shard for $shard {
            type Input = $input;
            type Output = $output;
            type State = $state;
            type Change = flezha_executor::state::NoChange;
            type Clock = flezha_executor::state::WallClock;
            type Config = ();
            fn name() -> &'static str { $name }
            fn init_state(
                _: ::std::option::Option<&str>,
                _: ::std::option::Option<flezha_executor::state::RestoreInitPayload<'_>>,
            ) -> $state {
                ::std::default::Default::default()
            }
            fn run(
                &mut self,
                $inp: $input,
                $st: flezha_executor::shard::StateHandle<'_, Self>,
                $ctx: &mut flezha_executor::shard::ShardCtx<'_>,
            ) -> ::std::result::Result<(), flezha_executor::shard::ShardError> $body
        }
    };
    ($shard:ident, $name:literal, $input:ty => $output:ty, $state:ty, cap $cap:literal,
     |$inp:tt, $st:tt, $ctx:tt| $body:block) => {
        #[derive(Default)]
        struct $shard;
        impl flezha_executor::shard::Shard for $shard {
            type Input = $input;
            type Output = $output;
            type State = $state;
            type Change = flezha_executor::state::NoChange;
            type Clock = flezha_executor::state::WallClock;
            type Config = ();
            fn name() -> &'static str { $name }
            fn init_state(
                _: ::std::option::Option<&str>,
                _: ::std::option::Option<flezha_executor::state::RestoreInitPayload<'_>>,
            ) -> $state {
                ::std::default::Default::default()
            }
            fn mailbox_capacity() -> usize { $cap }
            const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
            fn run(
                &mut self,
                $inp: $input,
                $st: flezha_executor::shard::StateHandle<'_, Self>,
                $ctx: &mut flezha_executor::shard::ShardCtx<'_>,
            ) -> ::std::result::Result<(), flezha_executor::shard::ShardError> $body
        }
    };
}

/// Dispatch one wire frame to `relay_id` on `channel`.
pub fn dispatch_input(
    handle: &RelayDispatchHandle,
    relay_id: &str,
    channel: &str,
    payload: Vec<u8>,
    stream: &Arc<FlezhaStream>,
) {
    let mut md = HashMap::new();
    md.insert("channel".to_string(), channel.to_string());
    handle.dispatch(relay_id, payload, &md, Arc::clone(stream));
}
