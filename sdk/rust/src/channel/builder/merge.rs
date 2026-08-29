//! Merged subscriptions: one procedure fed by two or more source channels,
//! each supplying the mapping that lifts its payload into the sum type.

use std::marker::PhantomData;

use super::issue::TopologyIssue;
use super::state::{erase, erase_merge_map};
use super::stream::{Stream, StreamTarget};
use super::ChannelBuilder;
use crate::channel::{ChannelId, EdgeKeyFn};
use crate::procedure::Procedure;

/// A channel handle usable as a [`merge`](ChannelBuilder::merge) source:
/// either an [`EntryStream`] (sealed payload off the wire) or a [`Stream`]
/// (another procedure's output). One impl covers both, so the two are freely
/// mixed in one merge.
pub trait MergeSource<'b> {
    /// The payload the source channel carries — the mapping's argument type,
    /// after any transform declared on the source handle.
    type Payload: Clone + Send + Sync + 'static;

    /// The channel this source subscribes on and the transform chain declared
    /// on the handle, materializing a producer's output channel if it did not
    /// exist yet. The mapping composes *after* that chain, which is why a
    /// transform on a source sees the source's own payload type.
    fn merge_site(self) -> (ChannelId, Option<u32>);
}

impl<'b, T: Clone + Send + Sync + 'static, K: StreamTarget> MergeSource<'b> for Stream<'b, T, K> {
    type Payload = T;

    fn merge_site(self) -> (ChannelId, Option<u32>) {
        (self.with_channel(|_, cid| cid), self.pending)
    }
}

/// A merged subscription under construction: the sources declared so far for
/// one consumer whose `Input` is the sum type `I`. Built by
/// [`ChannelBuilder::merge`] and closed by [`into`](Merge::into).
///
/// Nothing is recorded on the topology until `into`, so a merge left unclosed
/// declares no edges at all — silently. `must_use` is what makes that a
/// compiler warning rather than a graph that is quietly missing a consumer.
#[must_use = "a merge declares nothing until it is closed with `.into::<Proc>()`"]
pub struct Merge<'b, I> {
    pub(super) builder: &'b ChannelBuilder,
    /// Per source: its channel and the pending chain that ends in this merge's
    /// mapping.
    pub(super) sources: Vec<(ChannelId, u32)>,
    /// Every `.from` the author wrote, including ones already refused. The
    /// arity check reads this so a duplicated source is reported once, as a
    /// duplicate — not also as a merge that declared too few.
    pub(super) declared: usize,
    /// The one key function a merge declares, applied to the merged value on
    /// every source's edge.
    pub(super) key: Option<EdgeKeyFn>,
    pub(super) duplicate_key: bool,
    pub(super) _phantom: PhantomData<fn() -> I>,
}

impl<'b, I: Send + Sync + 'static> Merge<'b, I> {
    /// Add one source channel with the mapping that lifts its payload into the
    /// merged input. Subscribing the same channel twice is a registration
    /// error naming that channel.
    pub fn from<S, F>(mut self, source: S, map: F) -> Self
    where
        S: MergeSource<'b>,
        F: Fn(S::Payload) -> I + Send + Sync + 'static,
    {
        let (channel_id, source_pending) = source.merge_site();
        self.declared += 1;
        let mut st = self.builder.state.borrow_mut();
        if self
            .sources
            .iter()
            .any(|(existing, _)| *existing == channel_id)
        {
            let channel = st.channels[channel_id as usize].name;
            st.errors
                .push(TopologyIssue::DuplicateMergeSource { channel });
            return self;
        }
        let link = erase_merge_map::<S::Payload, I, F>(map);
        let chain = st.extend_pending(channel_id, source_pending, Some(link), None);
        drop(st);
        self.sources.push((channel_id, chain));
        self
    }

    /// Declare the Entity key for the merged procedure — once, on the merge,
    /// because by the time keying runs every source has been mapped into the
    /// one `Input` type. Yields the key for a value of the merged sum type.
    pub fn key_by<F>(mut self, key: F) -> Self
    where
        F: Fn(&I) -> String + Send + Sync + 'static,
    {
        if self.key.is_some() {
            self.duplicate_key = true;
        }
        self.key = Some(erase::<I, _, _>("key_by", key));
        self
    }

    /// Close the merge on its consumer and return **the consumer's** output
    /// stream handle, so a merged node chains like any other. Compile-time
    /// bound: `S::Input == I`.
    pub fn into<S: Procedure<Input = I>>(self) -> Stream<'b, S::Output> {
        self.wire::<S>(None)
    }

    /// Like [`into`](Merge::into), but the consumer is a distinct named node
    /// (the `sdk-graph-authoring` node-identity rule).
    pub fn into_named<S: Procedure<Input = I>>(
        self,
        node_name: &'static str,
    ) -> Stream<'b, S::Output> {
        self.wire::<S>(Some(node_name))
    }

    /// Close the merge on `S`, named or not — the one body behind both public
    /// spellings, and the seam a packaged building block
    /// ([`ChannelBuilder::route_latest`]) closes its own merge with instead of
    /// re-deriving the named/unnamed dispatch.
    pub(crate) fn wire<S: Procedure<Input = I>>(
        self,
        node_name: Option<&'static str>,
    ) -> Stream<'b, S::Output> {
        let mut st = self.builder.state.borrow_mut();
        let node = st.resolve_node::<S>(node_name);
        if self.declared < 2 {
            let procedure = st.procedures[node as usize].name;
            st.errors.push(TopologyIssue::MergeTooFewSources {
                procedure,
                sources: self.declared,
            });
        }
        if self.duplicate_key {
            if let Some(&(channel_id, _)) = self.sources.first() {
                let channel = st.channels[channel_id as usize].name;
                st.errors.push(TopologyIssue::DuplicateKeyBy { channel });
            }
        }
        for (channel_id, chain) in self.sources {
            // The key rides each source's chain, so it is evaluated on the
            // mapped value — and a key declared on a *source* handle instead
            // trips the transform-after-key refusal, which is exactly the
            // "declared once, on the merge" rule.
            let chain = match self.key.clone() {
                Some(key) => st.extend_pending(channel_id, Some(chain), None, Some(key)),
                None => chain,
            };
            st.attach_consumer(channel_id, node, Some(chain));
        }
        drop(st);
        Stream {
            builder: self.builder,
            target: node,
            pending: None,
            _phantom: PhantomData,
        }
    }
}
