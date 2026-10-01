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

/// How an edge embeds an operator class
/// ([`Stream::into_operator_with`](builder::Stream::into_operator_with)):
/// the entry channel it feeds when the class declares several, the node's
/// name (the class's factory id by default), and the most scopes a keyed edge
/// keeps at once.
#[derive(Debug, Clone, Copy, Default)]
pub struct Embedding {
    pub input: Option<&'static str>,
    pub name: Option<&'static str>,
    pub max_cells: Option<usize>,
}
