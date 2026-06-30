//! Rust SDK for the Flezha executor platform.
//!
//! # Dispatch model
//!
//! Stream readers push directly into per-Segment lock-free mailboxes
//! (`crossbeam::queue::SegQueue` + `AtomicBool inflight`). A Chase-Lev
//! work-stealing pool drains the mailboxes. A `DashMap`-backed
//! [`operative::registry::OperativeRegistry`] holds live ContinuousOperative
//! instances; the central coordinator runs but only handles
//! control-plane traffic.
//!
//! # Worked example (sticky ordering, with persistent state)
//!
//! ```no_run
//! use flezha_executor::{
//!     register_continuous, ChannelBuilder, ContinuousOperative, Operation, OperationCtx, OperationError,
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
//! struct AppendOperation;
//! impl Operation for AppendOperation {
//!     type Input = String;
//!     type Output = String;
//!     type State = AppendState;
//!     type Change = AppendChange;
//!     type Clock = WallClock;
//!     type Config = ();
//!     fn name() -> &'static str { "append_operation" }
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
//!         _: &mut OperationCtx<'_>,
//!     ) -> Result<(), OperationError> {
//!         state.apply(AppendChange(input));
//!         Ok(())
//!     }
//! }
//!
//! struct AppendOperative;
//! impl ContinuousOperative for AppendOperative {
//!     fn factory_id() -> &'static str { "appender" }
//!     fn channels(c: &mut ChannelBuilder) {
//!         c.input::<String>("input").into::<AppendOperation>();
//!     }
//! }
//!
//! register_continuous::<AppendOperative>();
//! let _ = flezha_executor::new("my_namespace".to_string());
//! # fn _ref() -> Option<NoChange> { None }
//! ```
//!
//! When 10,000 messages targeting one `appender` ContinuousOperative instance arrive
//! on the entry channel, the final `state.seen` vector contains those
//! 10,000 inputs in arrival order — the per-Segment inflight CAS
//! guarantees no two `Operation::run` calls overlap on the same segment.
//!
//! # Reconnect & idempotency
//!
//! The SDK's reconnect loop runs forever with capped exponential
//! backoff. On every successful reconnect the controller re-issues
//! `AddOperative` for every ContinuousOperative instance ProcessHub still has tagged
//! to this executor's profile. The handler is idempotent on duplicate
//! `operative_id`: if the local registry already contains the instance
//! it logs and returns without re-registering, so the existing
//! per-Segment state is preserved across the reconnect.
//!
//! # Tuning
//!
//! - `FLEZHA_WORKER_POOL_SIZE` — number of work-stealing threads
//!   (default: `std::thread::available_parallelism()`).
//! - `FLEZHA_TRANSPORT` — `"uds"` (default) or `"quic"`.
//! - `FLEZHA_QUIC_ADDRESS` — QUIC endpoint address.

pub mod channel;
pub mod command;
mod executor;
pub mod segment;
pub mod operative;
pub mod operation;
pub mod bounded;
mod shutdown;
mod util;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

/// One-stop import path for the `operative-state-persistence-rust` trait
/// surface. Bring everything into scope with
/// `use flezha_executor::state::*;` to write Operations that declare
/// `Change` types and route mutations through `state.apply(...)`.
pub mod state {
    pub use crate::segment::restore::{RestoreError, RestoreInitPayload};
    pub use crate::operation::change::{Change, CoalesceKey, NoChange};
    pub use crate::operation::clock::{Clock, EventTimeFrom, LogicalClock, WallClock};
    pub use crate::operation::state_handle::StateHandle;
}

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

pub use channel::{Channel, ChannelBuilder, OverflowPolicy};
use crossbeam::channel::{Receiver, Sender};
pub use flz_transport::{service, transport};
pub use segment::{RestoreError, RestoreInitPayload};
pub use operative::ContinuousOperative;
pub use bounded::{BoundedOperative, BoundedCtx, BoundedError};
pub use operation::{
    Change, Clock, CoalesceKey, DerivedState, EventTimeFrom, LogicalClock, NoChange, Operation,
    OperationCtx, OperationError, StateHandle, WallClock,
};

/// Authoring sugar. `#[flezha::operation]`, `#[derive(flezha::State)]`, and
/// `flezha::operative!` expand to the trait surface above; see
/// `docs/sdk_authoring_contract.md`.
pub use flezha_macros::{operative, operation, State};

/// Re-exported so macro-generated code can `submit!` into the auto-operative
/// collection without the user adding an `inventory` dependency.
pub use inventory;

/// An operative using the single-operation auto-wire path (`#[flezha::operation(sink = …)]`) or
/// `flezha::operative!` registers without an explicit `register_continuous` call.
/// Generated code `submit!`s one of these; [`register_inventory_operatives`]
/// harvests them at startup.
pub struct AutoOperative {
    /// The class's controller-facing factory id (for duplicate detection).
    pub factory_id: fn() -> &'static str,
    /// Registers the class topology (`register_continuous::<R>()`).
    pub register: fn(),
}

impl AutoOperative {
    pub const fn new(factory_id: fn() -> &'static str, register: fn()) -> Self {
        Self {
            factory_id,
            register,
        }
    }
}

inventory::collect!(AutoOperative);

/// Harvest every auto-registered operative (single-operation auto-wire +
/// `flezha::operative!`) into the class registry. Called once at startup. Panics
/// on a duplicate `factory_id` — across two auto-registered operatives or against
/// a operative already registered manually via [`register_continuous`].
pub fn register_inventory_operatives() {
    for auto in inventory::iter::<AutoOperative> {
        let factory_id = (auto.factory_id)();
        if operative::registry::get_class(factory_id).is_some() {
            panic!(
                "flezha: duplicate factory_id `{factory_id}`: an auto-registered operative collides \
                 with another auto-registered or manually-registered operative"
            );
        }
        (auto.register)();
    }
}

use crate::executor::{ClientResponseMessage, ExecutorMessage};
use crate::operative::instance::ContinuousInstance;
use crate::shutdown::ShutdownBridge;

pub mod pb {
    #[cfg(not(rust_analyzer))]
    include!("generated/executor_commands.rs");
    #[cfg(rust_analyzer)]
    include!("generated/executor_commands.rs");
}

/// Register a ContinuousOperative class with the SDK. Calls `R::channels(...)` once,
/// freezes the resulting topology in the global class registry, and
/// makes the class spawnable via the controller's `flz operative start
/// <factory_id>` command (which routes to `start_operative` on the
/// `ExecutorNode`).
///
/// Idempotent on the same `factory_id` — re-registration overwrites
/// the prior topology. Panics if `R::channels` produced an invalid
/// topology (duplicate channel names, unwired channels, missing
/// `segment_for` overrides, etc.).
pub fn register_continuous<R: ContinuousOperative>() {
    let topology =
        operative::builder::build_topology_for::<R>().expect("register_continuous: invalid topology");
    operative::registry::register_class(R::factory_id(), topology);
}

/// Register a bounded class with the SDK. Sibling of [`register_continuous`]: makes
/// the class spawnable via `flz operative start <factory_id> --kind bounded`.
/// Builds and freezes the class spec — the run-sugar body or the structured
/// linear chain declared via `BoundedOperative::operations` — and validates it
/// (linear chain, no dynamic segments, terminable). Idempotent on the same
/// `factory_id`. Panics if the authoring is invalid (mirrors
/// [`register_continuous`]); use [`bounded::spec::build_spec`] to handle the
/// error explicitly.
pub fn register_bounded<T: BoundedOperative>() {
    let spec =
        bounded::spec::build_spec::<T>().expect("register_bounded: invalid bounded operative");
    bounded::registry::register_class(T::factory_id(), spec);
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
        // Harvest single-operation auto-wire + `operative!` operatives before the executor
        // starts so the class registry is complete by the time AddOperative arrives.
        register_inventory_operatives();

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

    /// Convenience pass-through to the free [`register_continuous`] function.
    pub fn register_continuous<R: ContinuousOperative>(&self) {
        register_continuous::<R>()
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    /// Snapshot of the live ContinuousOperative-instance registry. Returned shape
    /// mirrors the pre-`operative-mvp-rust` `get_operatives` for backward
    /// compatibility.
    // reason: public API; refactor would break external callers (test apps, observability)
    #[allow(clippy::type_complexity)]
    pub fn get_operatives(&self) -> Arc<RwLock<HashMap<String, Arc<Mutex<Arc<ContinuousInstance>>>>>> {
        self.executor_tx
            .lock()
            .unwrap()
            .send(ExecutorMessage::ReturnOperatives)
            .expect("Failed to send ReturnOperatives message");

        let response = self
            .client_rx
            .lock()
            .unwrap()
            .recv()
            .expect("Failed to receive operatives");

        match response {
            ClientResponseMessage::ReturnOperatives(operatives) => operatives,
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
    pub use crate::executor::fast::OperativeDispatchHandle;
    pub use crate::executor::mailbox::Mailbox;
    pub use crate::executor::work_stealing_pool::WorkStealingPool;
}
