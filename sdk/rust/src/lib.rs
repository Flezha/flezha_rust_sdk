//! Rust SDK for the Flezha executor platform.
//!
//! # Dispatch model
//!
//! Stream readers push directly into per-Partition lock-free mailboxes
//! (`crossbeam::queue::SegQueue` + `AtomicBool inflight`). A Chase-Lev
//! work-stealing pool drains the mailboxes. A `DashMap`-backed
//! [`relay::registry::RelayInstanceRegistry`] holds live Relay
//! instances; the central coordinator runs but only handles
//! control-plane traffic.
//!
//! # Worked example (sticky ordering, with persistent state)
//!
//! ```no_run
//! use flezha_executor::{
//!     register_relay, ChannelBuilder, Relay, Shard, ShardCtx, ShardError,
//! };
//! use flezha_executor::state::{
//!     Change, NoChange, RestoreInitPayload, StateHandle, WallClock,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Serialize, Deserialize, Clone, Default)]
//! struct AppendState { seen: Vec<String> }
//!
//! #[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
//! struct AppendChange(String);
//! impl Change for AppendChange {}
//!
//! #[derive(Default)]
//! struct AppendShard;
//! impl Shard for AppendShard {
//!     type Input = String;
//!     type Output = String;
//!     type State = AppendState;
//!     type Change = AppendChange;
//!     type Clock = WallClock;
//!     type Config = ();
//!     fn name() -> &'static str { "append_shard" }
//!     fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> AppendState {
//!         AppendState { seen: vec![] }
//!     }
//!     fn apply_change(state: &mut AppendState, change: &AppendChange) {
//!         state.seen.push(change.0.clone());
//!     }
//!     fn run(
//!         &mut self,
//!         input: String,
//!         mut state: StateHandle<'_, Self>,
//!         _: &mut ShardCtx<'_>,
//!     ) -> Result<(), ShardError> {
//!         state.apply(AppendChange(input));
//!         Ok(())
//!     }
//! }
//!
//! struct AppendRelay;
//! impl Relay for AppendRelay {
//!     fn factory_id() -> &'static str { "appender" }
//!     fn channels(c: &mut ChannelBuilder) {
//!         c.input::<String>("input").into::<AppendShard>();
//!     }
//! }
//!
//! register_relay::<AppendRelay>();
//! let _ = flezha_executor::new("my_namespace".to_string());
//! # fn _ref() -> Option<NoChange> { None }
//! ```
//!
//! When 10,000 messages targeting one `appender` Relay instance arrive
//! on the entry channel, the final `state.seen` vector contains those
//! 10,000 inputs in arrival order — the per-Partition inflight CAS
//! guarantees no two `Shard::run` calls overlap on the same partition.
//!
//! # Reconnect & idempotency
//!
//! The SDK's reconnect loop runs forever with capped exponential
//! backoff. On every successful reconnect the controller re-issues
//! `AddRelay` for every Relay instance ProcessHub still has tagged
//! to this executor's profile. The handler is idempotent on duplicate
//! `relay_id`: if the local registry already contains the instance
//! it logs and returns without re-registering, so the existing
//! per-Partition state is preserved across the reconnect.
//!
//! # Tuning
//!
//! - `FLEZHA_WORKER_POOL_SIZE` — number of work-stealing threads
//!   (default: `std::thread::available_parallelism()`).
//! - `FLEZHA_TRANSPORT` — `"uds"` (default) or `"quic"`.
//! - `FLEZHA_QUIC_ADDRESS` — QUIC endpoint address.

pub mod channel;
mod command;
mod executor;
pub mod partition;
pub mod relay;
pub mod shard;
mod shutdown;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

/// One-stop import path for the `relay-state-persistence-rust` trait
/// surface. Bring everything into scope with
/// `use flezha_executor::state::*;` to write Shards that declare
/// `Change` types and route mutations through `state.apply(...)`.
pub mod state {
    pub use crate::partition::restore::{RestoreError, RestoreInitPayload};
    pub use crate::shard::change::{Change, CoalesceKey, NoChange};
    pub use crate::shard::clock::{Clock, EventTimeFrom, LogicalClock, WallClock};
    pub use crate::shard::state_handle::StateHandle;
}

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crossbeam::channel::{Receiver, Sender};

pub use channel::{Channel, ChannelBuilder, OverflowPolicy};
pub use partition::{RestoreError, RestoreInitPayload};
pub use relay::Relay;
pub use shard::{
    Change, Clock, CoalesceKey, EventTimeFrom, LogicalClock, NoChange, Shard, ShardCtx,
    ShardError, StateHandle, WallClock,
};

use crate::{
    executor::{ClientResponseMessage, ExecutorMessage},
    relay::instance::RelayInstance,
    shutdown::ShutdownBridge,
};

pub use flz_transport::transport;
pub use flz_transport::service;

pub mod pb {
    #[cfg(not(rust_analyzer))]
    include!("generated/executor_commands.rs");
    #[cfg(rust_analyzer)]
    include!("generated/executor_commands.rs");
}

/// Register a Relay class with the SDK. Calls `R::channels(...)` once,
/// freezes the resulting topology in the global class registry, and
/// makes the class spawnable via the controller's `flz relay start
/// <factory_id>` command (which routes to `start_relay` on the
/// `ExecutorNode`).
///
/// Idempotent on the same `factory_id` — re-registration overwrites
/// the prior topology. Panics if `R::channels` produced an invalid
/// topology (duplicate channel names, unwired channels, missing
/// `partition_for` overrides, etc.).
pub fn register_relay<R: Relay>() {
    let topology = relay::builder::build_topology_for::<R>()
        .expect("register_relay: invalid topology");
    relay::registry::register_class(R::factory_id(), topology);
}

pub struct Flezha {
    id: String,
    executor_tx: Arc<Mutex<crossbeam::channel::Sender<ExecutorMessage>>>,
    client_rx: Arc<Mutex<crossbeam::channel::Receiver<ClientResponseMessage>>>,
    _executor_thread: Option<std::thread::JoinHandle<()>>,
    shutdown_bridge: Arc<ShutdownBridge>,
}

impl Flezha {
    fn new(id: String) -> Flezha {
        let (otx, orx): (Sender<ExecutorMessage>, Receiver<ExecutorMessage>) =
            crossbeam::channel::unbounded();
        let (ctx, irx): (
            Sender<ClientResponseMessage>,
            Receiver<ClientResponseMessage>,
        ) = crossbeam::channel::unbounded();

        let shutdown_bridge = ShutdownBridge::new();
        shutdown::register_bridge(Arc::clone(&shutdown_bridge));

        let executor_thread = executor::start(
            id.clone(),
            (otx.clone(), orx),
            ctx,
            Arc::clone(&shutdown_bridge),
        );

        match irx
            .recv()
            .expect("Failed to receive namespace confirmation")
        {
            ClientResponseMessage::ExecutorIdConfirmation => {}
            _ => panic!("Unexpected message type received"),
        }

        Flezha {
            id: id.clone(),
            executor_tx: Arc::new(Mutex::new(otx)),
            client_rx: Arc::new(Mutex::new(irx)),
            _executor_thread: Some(executor_thread),
            shutdown_bridge,
        }
    }

    /// Convenience pass-through to the free [`register_relay`] function.
    pub fn register_relay<R: Relay>(&self) {
        register_relay::<R>()
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    /// Snapshot of the live Relay-instance registry. Returned shape
    /// mirrors the pre-`relay-mvp-rust` `get_relays` for backward
    /// compatibility.
    pub fn get_relays(
        &self,
    ) -> Arc<RwLock<HashMap<String, Arc<Mutex<Arc<RelayInstance>>>>>> {
        self.executor_tx
            .lock()
            .unwrap()
            .send(ExecutorMessage::ReturnRelays)
            .expect("Failed to send ReturnRelays message");

        let response = self
            .client_rx
            .lock()
            .unwrap()
            .recv()
            .expect("Failed to receive relays");

        match response {
            ClientResponseMessage::ReturnRelays(relays) => relays,
            _ => panic!("Unexpected message type received"),
        }
    }
}

pub fn new(namespace: String) -> Flezha {
    Flezha::new(namespace)
}

/// Drop trigger for the graceful-shutdown sequence.
impl Drop for Flezha {
    fn drop(&mut self) {
        self.shutdown_bridge.run_graceful_shutdown();
    }
}

// ---------------------------------------------------------------------------
// Internal dispatch surface for benches/tests. NOT a stable API. Only
// available under the `bench-internals` feature, which is enabled by
// this crate's `[[bench]]` targets via `required-features`. Downstream
// consumers never enable this feature, so the surface below is not part
// of the crate's public API for version-compat purposes.
// ---------------------------------------------------------------------------
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod __dispatch_internals {
    pub use crate::executor::fast::RelayDispatchHandle;
    pub use crate::executor::mailbox::Mailbox;
    pub use crate::executor::work_stealing_pool::WorkStealingPool;
}
