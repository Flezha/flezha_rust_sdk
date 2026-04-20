mod control_unit;

#[cfg(not(feature = "fast-dispatch"))]
mod legacy;
#[cfg(not(feature = "fast-dispatch"))]
mod worker_pool;

#[cfg(feature = "fast-dispatch")]
pub(crate) mod fast;
#[cfg(feature = "fast-dispatch")]
pub(crate) mod mailbox;
#[cfg(feature = "fast-dispatch")]
pub(crate) mod work_stealing_pool;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::processor::factory_registry::ProcessorBox;

#[cfg(not(feature = "fast-dispatch"))]
use flz_transport::transport::FlezhaStream;
#[cfg(not(feature = "fast-dispatch"))]
use crate::ProcessorInput;

/// Messages routed through the coordinator channel.
///
/// Under `fast-dispatch`, data-plane variants (`WorkerInitCheckIn`,
/// `WorkerCheckIn`, `WorkerCheckOut`) are intentionally absent: stream
/// readers push directly into per-processor mailboxes. The coordinator only
/// handles control-plane traffic.
#[cfg(not(feature = "fast-dispatch"))]
pub enum ComputeEngineMessage {
    WorkerInitCheckIn(u16),
    WorkerCheckIn(u16, String),
    WorkerCheckOut(String, ProcessorInput, Arc<FlezhaStream>),
    HandleCommandAction(String, Vec<u8>, HashMap<String, String>),
    ReturnProcessors,
    Reconnect,
}

#[cfg(feature = "fast-dispatch")]
pub enum ComputeEngineMessage {
    HandleCommandAction(String, Vec<u8>, HashMap<String, String>),
    ReturnProcessors,
    Reconnect,
}

pub enum ComputeEngineStatus {
    Starting,
    Running,
    Stopped,
}

pub enum ClientResponseMessage {
    ComputeEngineIdConfirmation,
    ReturnProcessors(Arc<RwLock<HashMap<String, Arc<Mutex<ProcessorBox>>>>>),
}

// Re-export the active `ComputeEngineNode` and `start()` from whichever
// backend the feature flag selected. Call sites in `command/*.rs` import
// these via `crate::compute_engine::ComputeEngineNode` and `::start`, so
// the backend is transparent to them.

#[cfg(not(feature = "fast-dispatch"))]
pub use legacy::{start, ComputeEngineNode};

#[cfg(feature = "fast-dispatch")]
pub use fast::{start, ComputeEngineNode};
