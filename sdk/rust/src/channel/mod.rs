//! Channel authoring surface — typed end-to-end with per-operative
//! namespacing. The runtime kernel (descriptors, edges, policies) lives in
//! `flz_core::channel` and is re-exported here; the `Channel<T>` handle is
//! phantom-typed so wiring errors fire at compile time. Channel names are
//! de-duplicated and assigned sequential IDs at `define_stream::<R>()`
//! time and frozen with the topology.

use std::marker::PhantomData;

pub mod builder;

pub use builder::{
    ChannelBuilder, EntryStream, EntryTarget, Merge, MergeSource, NodeTarget, Stream, StreamTarget,
};
pub use flz_core::channel::*;

/// Phantom-typed channel handle. The `id` is the index into
/// `OperativeTopology::channels`; the `T` parameter exists only for
/// compile-time wiring checks via the builder methods.
pub struct Channel<T> {
    pub id: ChannelId,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Channel<T> {
    pub(crate) fn new(id: ChannelId) -> Self {
        Self {
            id,
            _phantom: PhantomData,
        }
    }
}

impl<T> Clone for Channel<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Channel<T> {}
