//! `route_latest`: the latest-document router building block.
//!
//! A dynamically-keyed procedure cannot receive a broadcast — every merge key
//! is total, and inside a partitioned member a computed key is admitted against
//! that member's key-group range. So "one operative-wide document every keyed
//! Entity must eventually see" (a threshold table, a feature flag, a config
//! document) needs a bridge, and the safe bridge is not the obvious one:
//! delivering by fanning out over a remembered key set emits keys this member
//! may no longer own, which parks against the partition guard after a rescale
//! or a stray out-of-range item seen during a reconcile window.
//!
//! This block delivers on the key's **own next traffic item** instead, which
//! makes over-delivery impossible by construction: the only keys emitted are
//! keys whose traffic just proved this member owns them. The price is one tick
//! of latency per key, and it is the whole design.
//!
//! The expansion is a merge into one single-entity procedure with journaled
//! state — existing primitives only, so there is no new runtime semantics, no
//! wire change and no new replay path.

use std::collections::HashMap;
use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::channel::builder::{ChannelBuilder, Stream, StreamTarget};
use crate::entity::restore::RestoreInitPayload;
use crate::procedure::{Change, Procedure, ProcedureCtx, ProcedureError, StateHandle, WallClock};

impl ChannelBuilder {
    /// Turn an operative-wide document stream into per-key deliveries: every
    /// key seen on `traffic` receives each distinct document exactly once, on
    /// that key's own next traffic item, as a `(key, document)` pair the
    /// consumer keys by.
    ///
    /// ```ignore
    /// let thresholds = c.input::<Thresholds>("thresholds");
    /// let trades = c.input::<Trade>("trades");
    /// let routed = c.route_latest(thresholds, trades, |t: &Trade| t.symbol.clone());
    ///
    /// c.merge::<DetectorInput>()
    ///     .from(trades, DetectorInput::Trade)
    ///     .from(routed, |(key, doc)| DetectorInput::Thresholds { key, doc })
    ///     .key_by(DetectorInput::key)
    ///     .into::<Detector>();
    /// ```
    ///
    /// Contract:
    ///
    /// - a document equal to the retained one is **inert** — a re-poll after a
    ///   reconnect re-delivers the same document, and nothing downstream sees
    ///   it;
    /// - a differing document replaces it and bumps the version, so every key
    ///   is owed one delivery again;
    /// - a key never seen in traffic receives **nothing**. There is no
    ///   remembered-set fan-out, which is what keeps every emitted key one this
    ///   member owns (see the module docs);
    /// - **latency is one tick per key**: a key learns of a new document on its
    ///   next traffic item, not when the document arrives. A key whose traffic
    ///   has stopped is never delivered to — deliberately, that is the same
    ///   property stated from the other side.
    ///
    /// The document is kept **opaque**: it is retained and compared with `Eq`
    /// and never parsed, so parsing (and its failure mode) stays with the
    /// consumer exactly as when the consumer read the document stream directly.
    ///
    /// The block expands to a merge into one SDK-packaged single-entity
    /// procedure whose `{document, version, delivered}` state rides the
    /// standard change journal — it is authoring sugar, not a runtime feature.
    /// The node is named `route_latest` like any procedure node, so a second
    /// router in one operative needs [`route_latest_named`] to name it. The
    /// returned stream is an ordinary inter-procedure stream whose value
    /// carries its own key; `.keyed()` on it has no Entity key to stamp (the
    /// router is single-entity), so output identity belongs downstream, where
    /// the key is the consuming Entity's own.
    ///
    /// [`route_latest_named`]: ChannelBuilder::route_latest_named
    pub fn route_latest<'b, D, T, K1, K2, F>(
        &'b self,
        documents: Stream<'b, D, K1>,
        traffic: Stream<'b, T, K2>,
        key_fn: F,
    ) -> Stream<'b, (String, D)>
    where
        D: Clone + Eq + Serialize + DeserializeOwned + Send + Sync + 'static,
        T: Send + Sync + 'static,
        K1: StreamTarget,
        K2: StreamTarget,
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        self.wire_route_latest(documents, traffic, key_fn, None)
    }

    /// [`route_latest`](ChannelBuilder::route_latest) with an explicit node
    /// identity, so one operative may carry several routers (the
    /// `sdk-graph-authoring` node-identity rule — unnamed mentions of one class
    /// are one node).
    pub fn route_latest_named<'b, D, T, K1, K2, F>(
        &'b self,
        documents: Stream<'b, D, K1>,
        traffic: Stream<'b, T, K2>,
        key_fn: F,
        node_name: &'static str,
    ) -> Stream<'b, (String, D)>
    where
        D: Clone + Eq + Serialize + DeserializeOwned + Send + Sync + 'static,
        T: Send + Sync + 'static,
        K1: StreamTarget,
        K2: StreamTarget,
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        self.wire_route_latest(documents, traffic, key_fn, Some(node_name))
    }

    /// The expansion both spellings share: the key function is an edge
    /// transform on the traffic stream (so the traffic payload is replaced by
    /// its key and never reaches the router), and the two streams are one merge
    /// into the packaged procedure.
    fn wire_route_latest<'b, D, T, K1, K2, F>(
        &'b self,
        documents: Stream<'b, D, K1>,
        traffic: Stream<'b, T, K2>,
        key_fn: F,
        node_name: Option<&'static str>,
    ) -> Stream<'b, (String, D)>
    where
        D: Clone + Eq + Serialize + DeserializeOwned + Send + Sync + 'static,
        T: Send + Sync + 'static,
        K1: StreamTarget,
        K2: StreamTarget,
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        let keys = traffic.map_ref("route_latest key", key_fn);
        self.merge::<RouteLatestInput<D>>()
            .from(documents, RouteLatestInput::Document)
            .from(keys, RouteLatestInput::Traffic)
            .wire::<LatestRouter<D>>(node_name)
    }
}

/// The packaged router's merged input: a document off the document stream, or
/// the Entity key one traffic item proved this member owns. The traffic
/// payload itself is dropped by the edge transform that computed the key —
/// the router has no use for it and holding it would put application values
/// in SDK-owned state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum RouteLatestInput<D> {
    Document(D),
    Traffic(String),
}

/// The router's journaled state: the retained document, the version that
/// identifies it, and the version each key has already received.
///
/// `delivered` grows with the keys this member sees, which is the Entity count
/// it already pays for — not a new unbounded-growth class.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RouterState<D> {
    document: Option<D>,
    version: u64,
    delivered: HashMap<String, u64>,
}

/// One journaled mutation of the router's state.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum RouterChange<D> {
    /// The retained document was replaced; the version advances with it, which
    /// is what makes every key owed a delivery again.
    Retain(D),
    /// One key received one version.
    Delivered { key: String, version: u64 },
}

impl<D: Serialize + DeserializeOwned + Send + Sync + 'static> Change for RouterChange<D> {}

/// The single-entity procedure [`ChannelBuilder::route_latest`] expands to.
/// Not public: the block is the whole authoring surface, and the guarantee it
/// carries (deliver only on the key's own traffic) holds because nothing else
/// can wire this node.
pub(crate) struct LatestRouter<D> {
    _phantom: PhantomData<fn() -> D>,
}

impl<D> Default for LatestRouter<D> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<D> Procedure for LatestRouter<D>
where
    D: Clone + Eq + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Input = RouteLatestInput<D>;
    type Output = (String, D);
    type State = RouterState<D>;
    type Change = RouterChange<D>;
    type Clock = WallClock;
    type Config = ();

    fn name() -> &'static str {
        "route_latest"
    }

    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> Self::State {
        RouterState {
            document: None,
            version: 0,
            delivered: HashMap::new(),
        }
    }

    fn apply_change(state: &mut Self::State, change: &Self::Change) {
        match change {
            RouterChange::Retain(document) => {
                state.document = Some(document.clone());
                state.version += 1;
            }
            RouterChange::Delivered { key, version } => {
                state.delivered.insert(key.clone(), *version);
            }
        }
    }

    fn run(
        &mut self,
        input: Self::Input,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        match input {
            RouteLatestInput::Document(document) => {
                // Version 0 means "nothing retained", so the first document
                // lands at 1 and every key's absent entry is already behind it.
                if state.document.as_ref() != Some(&document) {
                    state.apply(RouterChange::Retain(document));
                }
            }
            RouteLatestInput::Traffic(key) => {
                let Some(document) = state.document.as_ref() else {
                    return Ok(());
                };
                let version = state.version;
                if state
                    .delivered
                    .get(&key)
                    .is_some_and(|seen| *seen >= version)
                {
                    return Ok(());
                }
                // Emit first: a failed emit rides the channel's retry ladder,
                // and recording the delivery before it would make the retry
                // silent.
                ctx.emit((key.clone(), document.clone()))?;
                state.apply(RouterChange::Delivered { key, version });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
