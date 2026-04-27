//! Rust SDK for the Flezha compute-engine platform.
//!
//! # Dispatch model
//!
//! The SDK picks one of two internal dispatch backends at compile time:
//!
//! - **Legacy** (default; `fast-dispatch` feature off). Every inbound
//!   message crosses a single coordinator channel; a central `ComputeEngineNode`
//!   loop performs lock-heavy routing. Preserved behind
//!   `#[cfg(not(feature = "fast-dispatch"))]` during the
//!   optimize-rust-sdk-dispatch rollout.
//! - **Fast-dispatch** (`fast-dispatch` feature on). Stream readers push
//!   directly into a per-processor lock-free mailbox
//!   (`crossbeam::queue::SegQueue` + `AtomicBool inflight`). A Chase-Lev
//!   work-stealing pool drains the mailboxes. A `DashMap`-backed
//!   [`processor::factory_registry::ProcessorRegistry`] replaces
//!   `RwLock<HashMap>` for live processor lookup. The central coordinator
//!   still runs but only handles control-plane traffic.
//!
//! Both backends are **observationally equivalent** to SDK users:
//!
//! - Public API — `new`, [`Flezha`], [`Flezha::get_id`],
//!   [`Flezha::get_processors`], [`register_factory`], [`ProcessorHandler`],
//!   [`ProcessorInput`] / [`ProcessorOutput`] / [`ProcessorResult`],
//!   [`ProcessorSettings`], [`FactoryMetadata`] — has identical signatures
//!   across both features.
//! - Sticky per-processor ordering under `ProcessorRunner::ThreadPool` is
//!   guaranteed under both: no two invocations of the same processor's
//!   `process(...)` ever overlap, and inputs are observed in the order
//!   they arrived on the primary stream.
//! - `ProcessorRunner::ThreadSpawn` retains per-invocation thread spawn
//!   semantics.
//! - Reconnect, migration, and command-dispatch behavior are preserved
//!   byte-for-byte (see `docs/sdk_protocol_spec.md` §5, §8).
//!
//! # Worked example (sticky ordering with fast-dispatch)
//!
//! ```no_run
//! use flezha_compute_engine::{
//!     register_factory, FactoryMetadata, ProcessorHandler, ProcessorSettings,
//! };
//! use flezha_compute_engine::processor::ProcessorError;
//! # #[derive(serde::Serialize, serde::Deserialize, Clone)]
//! # struct MyState { seen: Vec<String> }
//!
//! struct Appender;
//! impl ProcessorHandler<MyState, String, String> for Appender {
//!     fn process(&mut self, input: String, state: &mut MyState)
//!         -> Result<String, ProcessorError>
//!     {
//!         state.seen.push(input.clone());
//!         Ok(input)
//!     }
//! }
//!
//! register_factory(
//!     "appender".to_string(),
//!     ProcessorSettings::<MyState, String, String>::default(),
//!     MyState { seen: vec![] },
//!     || Box::new(Appender),
//!     FactoryMetadata::default(),
//! );
//!
//! let _ = flezha_compute_engine::new("my_namespace".to_string());
//! ```
//!
//! When 10,000 messages targeting a single "appender" processor arrive on
//! a primary stream, the final `state.seen` vector contains those 10,000
//! inputs in the exact order they arrived, regardless of the worker-pool
//! size. The mailbox's inflight flag ensures `process(...)` never runs
//! concurrently for the same processor.
//!
//! # Reconnect & idempotency
//!
//! The SDK's reconnect loop runs forever with capped exponential backoff. On
//! every successful reconnect the agent re-issues `AddProcessor` for every
//! processor ProcessHub still has assigned to this operator's profile. The
//! `AddProcessor` handler is idempotent on duplicate `processor_id`: if the
//! local registry already contains the processor it logs
//! `"AddProcessor: processor <id> already registered, skipping"` and
//! returns without re-registering, so the existing processor's state is
//! preserved across the reconnect.
//!
//! # Graceful shutdown
//!
//! The SDK installs a SIGTERM handler at construction time and runs the
//! same routine from the `Flezha` handle's `Drop`. On graceful shutdown
//! it walks every running processor, calls `json_serialize_state()`
//! synchronously (the same code path used for `MigrationSerialization`),
//! and emits one `OperatorShutdownNotification{processor_id,
//! serialized_state}` per processor on the control channel. The agent
//! overwrites `Overwatch.state.snapshot` with the binary and emits an
//! `OperatorShutdownAck`. The SDK then exits — the per-processor ack
//! wait is bounded at **2 s**, parallelised across processors so total
//! wait is bounded by 2 s (not 2N s). The serializer step is capped at
//! the agent's `cross_node_request_timeout` ceiling (**10 s**); concrete
//! `Processor` impls with multi-second serializers should refactor to
//! stay well below it. Consumers do not need to call this manually —
//! both `Drop` and SIGTERM trigger it automatically. SIGKILL bypasses
//! the routine by definition; the agent falls back to the most recent
//! periodic snapshot exactly as today. See
//! `openspec/changes/ship-fresh-state-on-sdk-shutdown` for the full
//! contract.
//!
//! # Tuning
//!
//! - `FLEZHA_WORKER_POOL_SIZE` — number of work-stealing threads (default:
//!   `std::thread::available_parallelism()`).
//! - `FLEZHA_TRANSPORT` — `"uds"` (default) or `"quic"`.
//! - `FLEZHA_QUIC_ADDRESS` — QUIC endpoint address.
//!
//! # Feature flags
//!
//! | Feature           | Default | Description                                            |
//! |-------------------|---------|--------------------------------------------------------|
//! | `fast-dispatch`   | off     | Per-processor mailboxes + work-stealing pool.          |
//! | `bench-internals` | off     | Exposes internal dispatch types for this crate's benches. NOT stable. |

mod command;
mod compute_engine;
pub mod processor;
mod shutdown;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
};

use crossbeam::channel::{Receiver, Sender};

pub use processor::{
    register_factory, FactoryMetadata, ProcessorHandler, ProcessorInput, ProcessorOutput,
    ProcessorResult, ProcessorSettings,
};

use crate::{
    compute_engine::{ClientResponseMessage, ComputeEngineMessage},
    processor::factory_registry::ProcessorBox,
    shutdown::ShutdownBridge,
};

// Re-export transport layer from shared library
pub use flz_transport::transport;
pub use flz_transport::service;

pub mod pb {
    #[cfg(not(rust_analyzer))]
    include!("generated/compute_engine_commands.rs");
    #[cfg(rust_analyzer)]
    include!("generated/compute_engine_commands.rs");
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
        // Register up-front so SIGTERM during the brief
        // start-and-handshake window still picks up this SDK. The bridge
        // returns early from `run_graceful_shutdown` if its
        // `control_stream` / `snapshot_fn` are still unset, so this is
        // safe even before the coordinator wires them in.
        shutdown::register_bridge(Arc::clone(&shutdown_bridge));

        let compute_engine_thread = compute_engine::start(
            id.clone(),
            (otx.clone(), orx),
            ctx,
            Arc::clone(&shutdown_bridge),
        );

        // Make sure the namespace is passed to the agent and confirmed
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

    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn get_processors(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>> {
        self.compute_engine_tx
            .lock()
            .unwrap()
            .send(ComputeEngineMessage::ReturnProcessors)
            .expect("Failed to send ReturnProcessors message");

        // Wait for the response from the compute_engine
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

/// Drop trigger for the graceful-shutdown sequence — covers explicit
/// `disconnect()` flows where the user lets the `Flezha` handle go out
/// of scope and any test path that creates a transient SDK. The SIGTERM
/// path runs the same routine via the global bridge registry; the
/// `shutdown_started` AtomicBool guards against double execution if
/// both fire.
impl Drop for Flezha {
    fn drop(&mut self) {
        self.shutdown_bridge.run_graceful_shutdown();
    }
}

// ---------------------------------------------------------------------------
// Internal dispatch surface for benches/tests. NOT a stable API. Only
// available under the `bench-internals` feature, which is enabled by this
// crate's `[[bench]]` targets via `required-features`. Downstream consumers
// never enable this feature, so the surface below is not part of the crate's
// public API for version-compat purposes.
// ---------------------------------------------------------------------------
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod __dispatch_internals {
    pub use crate::compute_engine::fast::FastDispatchHandle;
    pub use crate::compute_engine::mailbox::{
        new_processor_mailbox, Mailbox, ProcessorMailbox, ProcessorPayload,
    };
    pub use crate::compute_engine::work_stealing_pool::WorkStealingPool;
    pub use crate::processor::factory_registry::{
        ProcessorBox, ProcessorRegistry, ProcessorSlot,
    };

    use std::sync::Arc;

    /// Build a registry + pool pair sharing lifetime with a new FastDispatchHandle.
    pub fn new_handle_with_pool_size(pool_size: usize) -> (FastDispatchHandle, Arc<WorkStealingPool>) {
        let registry = Arc::new(ProcessorRegistry::new());
        let pool = Arc::new(WorkStealingPool::with_size(pool_size));
        (
            FastDispatchHandle {
                registry: Arc::clone(&registry),
                pool: Arc::clone(&pool),
            },
            pool,
        )
    }

    /// Manually register a processor into a registry (bypasses the factory
    /// registry). Returns the Arc<ProcessorSlot> so benches can grab the
    /// mailbox and push directly or inspect the processor state.
    pub fn register_direct(
        registry: &ProcessorRegistry,
        processor_id: String,
        processor: ProcessorBox,
    ) -> Arc<ProcessorSlot> {
        registry.register(processor_id, processor)
    }
}
