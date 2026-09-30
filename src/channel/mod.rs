//! Channel authoring surface — typed end-to-end with per-operator
//! namespacing. The runtime kernel (descriptors, edges, policies) lives in
//! `flz_core::channel` and is re-exported here; the stream handles are typed
//! by payload so wiring errors fire at compile time. Channel names are
//! de-duplicated and assigned sequential IDs at `register_factory(R)`
//! time and frozen with the topology.

pub mod builder;
pub(crate) mod call;
mod route_latest;

pub use builder::{
    ChannelBuilder, EntryStream, EntryTarget, Merge, MergeSource, NodeTarget, Stream, StreamTarget,
};
pub use call::{CallTarget, Response};
pub use flz_core::channel::*;
