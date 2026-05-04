//! Rust SDK for the Flezha compute-engine platform.
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
//! # Worked example (sticky ordering)
//!
//! ```no_run
//! use flezha_compute_engine::{
//!     register_relay, ChannelBuilder, Relay, Shard, ShardCtx, ShardError,
//! };
//! # #[derive(serde::Serialize, serde::Deserialize, Clone)]
//! # struct AppendState { seen: Vec<String> }
//!
//! #[derive(Default)]
//! struct AppendShard;
//! impl Shard for AppendShard {
//!     type Input = String;
//!     type Output = String;
//!     type State = AppendState;
//!     fn name() -> &'static str { "append_shard" }
//!     fn init_state(_: Option<&str>) -> AppendState {
//!         AppendState { seen: vec![] }
//!     }
//!     fn run(
//!         &mut self,
//!         input: String,
//!         state: &mut AppendState,
//!         _: &mut ShardCtx<'_>,
//!     ) -> Result<(), ShardError> {
//!         state.seen.push(input);
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
//! let _ = flezha_compute_engine::new("my_namespace".to_string());
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
//! backoff. On every successful reconnect the agent re-issues
//! `AddProcessor` for every Relay instance ProcessHub still has tagged
//! to this operator's profile. The handler is idempotent on duplicate
//! `processor_id`: if the local registry already contains the instance
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
mod compute_engine;
pub mod partition;
pub mod relay;
pub mod shard;
mod shutdown;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crossbeam::channel::{Receiver, Sender};

pub use channel::{Channel, ChannelBuilder};
pub use relay::Relay;
pub use shard::{Shard, ShardCtx, ShardError};

use crate::{
    compute_engine::{ClientResponseMessage, ComputeEngineMessage},
    relay::instance::RelayInstance,
    shutdown::ShutdownBridge,
};

pub use flz_transport::transport;
pub use flz_transport::service;

pub mod pb {
    #[cfg(not(rust_analyzer))]
    include!("generated/compute_engine_commands.rs");
    #[cfg(rust_analyzer)]
    include!("generated/compute_engine_commands.rs");
}

/// Register a Relay class with the SDK. Calls `R::channels(...)` once,
/// freezes the resulting topology in the global class registry, and
/// makes the class spawnable via the agent's `flz processor start
/// <factory_id>` command (which routes to `start_relay` on the
/// `ComputeEngineNode`).
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
    compute_engine_tx: Arc<Mutex<crossbeam::channel::Sender<ComputeEngineMessage>>>,
    client_rx: Arc<Mutex<crossbeam::channel::Receiver<ClientResponseMessage>>>,
    _compute_engine_thread: Option<std::thread::JoinHandle<()>>,
    shutdown_bridge: Arc<ShutdownBridge>,
}

impl Flezha {
    fn new(id: String) -> Flezha {
        let (otx, orx): (Sender<ComputeEngineMessage>, Receiver<ComputeEngineMessage>) =
            crossbeam::channel::unbounded();
        let (ctx, irx): (
            Sender<ClientResponseMessage>,
            Receiver<ClientResponseMessage>,
        ) = crossbeam::channel::unbounded();

        let shutdown_bridge = ShutdownBridge::new();
        shutdown::register_bridge(Arc::clone(&shutdown_bridge));

        let compute_engine_thread = compute_engine::start(
            id.clone(),
            (otx.clone(), orx),
            ctx,
            Arc::clone(&shutdown_bridge),
        );

        match irx
            .recv()
            .expect("Failed to receive namespace confirmation")
        {
            ClientResponseMessage::ComputeEngineIdConfirmation => {}
            _ => panic!("Unexpected message type received"),
        }

        Flezha {
            id: id.clone(),
            compute_engine_tx: Arc::new(Mutex::new(otx)),
            client_rx: Arc::new(Mutex::new(irx)),
            _compute_engine_thread: Some(compute_engine_thread),
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
    /// mirrors the pre-`relay-mvp-rust` `get_processors` for backward
    /// compatibility.
    pub fn get_processors(
        &self,
    ) -> Arc<RwLock<HashMap<String, Arc<Mutex<Arc<RelayInstance>>>>>> {
        self.compute_engine_tx
            .lock()
            .unwrap()
            .send(ComputeEngineMessage::ReturnProcessors)
            .expect("Failed to send ReturnProcessors message");

        let response = self
            .client_rx
            .lock()
            .unwrap()
            .recv()
            .expect("Failed to receive processors");

        match response {
            ClientResponseMessage::ReturnProcessors(processors) => processors,
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
    pub use crate::compute_engine::fast::RelayDispatchHandle;
    pub use crate::compute_engine::mailbox::Mailbox;
    pub use crate::compute_engine::work_stealing_pool::WorkStealingPool;
}
