//! `PartitionSlot`: per-(shard, partition) state + mailbox + dispatcher.
//!
//! Replaces the old `ProcessorSlot` (`processor/factory_registry.rs`).
//! The inflight CAS on `Mailbox` guarantees that at most one drain task
//! is active per slot; under that exclusion the typed dispatcher
//! downcasts state and runs `Shard::run`.

use std::any::Any;
use std::sync::{Arc, Mutex, Weak};

use flz_transport::transport::FlezhaStream;

use crate::channel::ShardId;
use crate::compute_engine::mailbox::Mailbox;
use crate::compute_engine::work_stealing_pool::WorkStealingPool;

/// Erased payload moving through the inter-shard mailbox. The `value`
/// is `Arc<dyn Any + Send + Sync>` (the consumer downcasts to its own
/// `S::Input`); the `stream` is the secondary FlezhaStream of the
/// connection that the original wire input arrived on, propagated
/// downstream so any plugin-bound emit lands on the same connection.
pub struct PartitionPayload {
    pub value: Arc<dyn Any + Send + Sync>,
    pub stream: Arc<FlezhaStream>,
}

pub type PartitionMailbox = Arc<Mailbox<PartitionPayload>>;

pub struct PartitionSlot {
    pub state: Mutex<Box<dyn Any + Send>>,
    pub mailbox: PartitionMailbox,
    pub dispatcher: Arc<dyn ShardDispatcher>,
    pub shard_id: ShardId,
    pub shard_key: Option<Arc<str>>,
    pub pool: Arc<WorkStealingPool>,
    pub instance: Weak<crate::relay::instance::RelayInstance>,
    pub topology: Arc<crate::relay::topology::RelayTopology>,
}

pub trait ShardDispatcher: Send + Sync + 'static {
    fn dispatch(
        &self,
        payload: PartitionPayload,
        state: &mut Box<dyn Any + Send>,
        instance: &Weak<crate::relay::instance::RelayInstance>,
    );

    fn shard_id(&self) -> ShardId;
}

/// Forward to the section-2 dispatcher implementation.
pub fn build_partition_slot<S: crate::shard::Shard>(
    args: crate::channel::builder::BuildPartitionArgs<'_>,
) -> Arc<PartitionSlot> {
    crate::relay::dispatcher::build_partition_slot::<S>(args)
}
