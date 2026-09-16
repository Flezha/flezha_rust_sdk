//! Channel authoring surface — typed end-to-end with per-operative
//! namespacing. The runtime kernel (descriptors, edges, policies) lives in
//! `flz_core::channel` and is re-exported here; the `Channel<T>` handle is
//! phantom-typed so wiring errors fire at compile time. Channel names are
//! de-duplicated and assigned sequential IDs at `define_stream::<R>()`
//! time and frozen with the topology.

use std::marker::PhantomData;

pub mod builder;
mod route_latest;

pub use builder::{
    ChannelBuilder, EntryStream, EntryTarget, Merge, MergeSource, NodeTarget, Stream, StreamTarget,
};
pub use flz_core::channel::*;

/// The finalized channel handle returned by [`Stream::sealed`]: a
/// phantom-typed marker exposing no builder methods, so a consumer or a
/// transform declared after it is a compile error.
///
/// It carries no state, which is what makes "sealing changes no topology"
/// true by construction rather than by convention — the handle cannot reach
/// the builder to record anything, not even the output channel a leaf node
/// deliberately does not have.
pub struct Channel<T> {
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Channel<T> {
    pub(crate) fn new() -> Self {
        Self {
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
