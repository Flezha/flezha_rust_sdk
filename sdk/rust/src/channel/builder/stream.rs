//! The stream handles: a `Copy` reference into the builder that wires
//! consumers, chains transforms and carries the producer-side channel config.

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use super::issue::TopologyIssue;
use super::state::{erase, erase_transform, BuilderState, PluginSink};
use super::ChannelBuilder;
use crate::channel::{Channel, EdgeKeyFn, EdgeTransformFn, EdgeYield, OverflowPolicy};
use crate::procedure::Procedure;

/// What a stream handle points at. The two markers are what keep the
/// entry-side and output-side handles distinct types while their shared
/// surface — transforms, keying, per-channel config, `.into` — is written
/// exactly once, against `K: StreamTarget`.
pub trait StreamTarget: Copy + 'static {
    /// Whether the handle's `target` already IS a channel id, or is the
    /// producing procedure whose output channel is created on demand.
    const IS_ENTRY: bool;
}

/// [`EntryStream`]'s marker: `target` already IS the entry channel id.
#[derive(Clone, Copy)]
pub struct EntryTarget;

/// [`Stream`]'s marker: `target` is the producing procedure, whose output
/// channel is created lazily so a leaf node never carries a phantom channel.
#[derive(Clone, Copy)]
pub struct NodeTarget;

impl StreamTarget for EntryTarget {
    const IS_ENTRY: bool = true;
}

impl StreamTarget for NodeTarget {
    const IS_ENTRY: bool = false;
}

/// The entry channel's handle: wires the graph's root consumer via `.into` and
/// carries the entry-side channel config. Same surface as [`Stream`] for
/// everything that is not producer-side (no `codec`, no plugin sink, no
/// `named` — the entry channel's name is given at `input(...)`).
pub type EntryStream<'b, T> = Stream<'b, T, EntryTarget>;

/// A procedure's output stream handle, returned by `.into::<Proc>()`: a
/// lightweight `Copy` reference into the operative's topology builder.
///
/// - `stream.into::<Next>()` wires Next as a consumer and returns Next's own
///   stream, so chains read in flow order.
/// - `map` / `filter` / `flat_map` / `key_by` reshape and key the values on the
///   way to the next `.into`, adding no node to the topology.
/// - Using one handle twice — two `.into` calls on the same variable — is
///   fan-out: every consumer receives every value released on the channel.
///   [`branch`](Stream::branch) is equivalent sugar.
/// - Two different streams wiring the same (unnamed) class fan **in**: the
///   consumer node accumulates both producers' contributions.
/// - Per-channel config (`codec`, `keyed`, `on_full`, `retry_*`, `named`)
///   chains in any order relative to consumer wiring.
///
/// The handle borrows the builder, so it cannot outlive the declaration —
/// using one after the topology freezes at registration is a compile error.
pub struct Stream<'b, T, K = NodeTarget> {
    pub(super) builder: &'b ChannelBuilder,
    /// The entry channel id (`EntryTarget`) or the producing procedure id
    /// (`NodeTarget`) — `K` decides which.
    pub(super) target: u32,
    /// The transform/key chain declared on this handle, `None` for a bare one.
    pub(super) pending: Option<u32>,
    pub(super) _phantom: PhantomData<fn() -> (T, K)>,
}

impl<'b, T, K> Clone for Stream<'b, T, K> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<'b, T, K> Copy for Stream<'b, T, K> {}

impl<'b, T: 'static + Send + Sync, K: StreamTarget> Stream<'b, T, K> {
    pub(super) fn with_channel<R>(self, f: impl FnOnce(&mut BuilderState, u32) -> R) -> R {
        let mut st = self.builder.state.borrow_mut();
        let cid = if K::IS_ENTRY {
            self.target
        } else {
            st.ensure_output_channel(self.target)
        };
        f(&mut st, cid)
    }

    /// This handle with a new payload type and a new pending chain — the one
    /// place a transform's result handle is built.
    fn relink<U>(
        self,
        transform: Option<EdgeTransformFn>,
        key: Option<EdgeKeyFn>,
    ) -> Stream<'b, U, K> {
        let pending = self.pending;
        let id = self.with_channel(|st, cid| st.extend_pending(cid, pending, transform, key));
        Stream {
            builder: self.builder,
            target: self.target,
            pending: Some(id),
            _phantom: PhantomData,
        }
    }

    /// Wire a consumer onto this stream's channel and return **the consumer's**
    /// output stream handle, so a linear pipeline is one chained expression.
    /// Compile-time bound: `S::Input == T`. Calling `.into` on the same handle
    /// repeatedly declares fan-out; wiring the same (unnamed) class from two
    /// streams declares fan-in.
    pub fn into<S: Procedure<Input = T>>(self) -> Stream<'b, S::Output> {
        self.wire::<S>(None)
    }

    /// Like [`into`](Stream::into), but the consumer is a distinct named node:
    /// its identity string is `node_name` instead of `S::name()`, letting one
    /// Procedure class appear at several graph positions with independent
    /// state (`sdk-graph-authoring` node-identity rule).
    pub fn into_named<S: Procedure<Input = T>>(
        self,
        node_name: &'static str,
    ) -> Stream<'b, S::Output> {
        self.wire::<S>(Some(node_name))
    }

    fn wire<S: Procedure<Input = T>>(
        self,
        node_name: Option<&'static str>,
    ) -> Stream<'b, S::Output> {
        let pending = self.pending;
        let node = self.with_channel(|st, cid| st.wire_consumer::<S>(cid, pending, node_name));
        Stream {
            builder: self.builder,
            target: node,
            pending: None,
            _phantom: PhantomData,
        }
    }

    /// Reshape every value on the way to the next consumer. Costs no
    /// Procedure, no Entity, no mailbox and no dispatch hop: the closure is
    /// stored on the consumer edge this chain terminates in and evaluated at
    /// the routing seam, so it runs identically on live delivery and on the
    /// controller's re-forwarded replay slice.
    ///
    /// It MUST therefore be pure and cheap. Pure because a replay that
    /// disagreed with the live run would rebuild a state the live run never
    /// had; cheap because it runs on the routing thread — real work belongs in
    /// a Procedure behind [`key_by`](Stream::key_by), which buys per-Entity
    /// parallelism.
    pub fn map<U, F>(self, f: F) -> Stream<'b, U, K>
    where
        T: Clone,
        U: Send + Sync + 'static,
        F: Fn(T) -> U + Send + Sync + 'static,
    {
        // The owned-value spelling of `map_ref`: the clone is what the author's
        // `Fn(T) -> U` asks for, and nothing else differs.
        self.map_ref("map", move |typed: &T| f(typed.clone()))
    }

    /// The one value-reshaping link: [`map`](Stream::map) is this over an owned
    /// clone, and the packaged building blocks reach it directly to derive a
    /// small value from a payload they then discard (`route_latest`'s key
    /// function) without paying that clone. `what` names the declaration in the
    /// impossible-downcast panic, like every other erasure.
    pub(crate) fn map_ref<U, F>(self, what: &'static str, f: F) -> Stream<'b, U, K>
    where
        U: Send + Sync + 'static,
        F: Fn(&T) -> U + Send + Sync + 'static,
    {
        let link = erase_transform::<T, _>(what, move |typed: &T, _| {
            EdgeYield::One(Arc::new(f(typed)) as Arc<dyn Any + Send + Sync>)
        });
        self.relink(Some(link), None)
    }

    /// Drop the values the predicate rejects. A surviving value is forwarded
    /// by pointer — a filtered stream costs one predicate call per input and
    /// nothing else. Same purity and cost contract as [`map`](Stream::map).
    pub fn filter<F>(self, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
    {
        let link = erase_transform::<T, _>("filter", move |typed: &T, value| {
            if predicate(typed) {
                EdgeYield::One(Arc::clone(value))
            } else {
                EdgeYield::Drop
            }
        });
        self.relink(Some(link), None)
    }

    /// Expand one value into zero or more. Each yielded value is keyed and
    /// delivered independently, so `flat_map` + [`key_by`](Stream::key_by) is
    /// how one input fans out across several Entities. Same purity and cost
    /// contract as [`map`](Stream::map).
    ///
    /// The yielded values are pushed one at a time under the channel's
    /// `on_full` policy, so a `DropNewest` channel may accept some of a
    /// fan-out and drop the rest — the same shape multi-consumer fan-out
    /// already has.
    pub fn flat_map<U, I, F>(self, f: F) -> Stream<'b, U, K>
    where
        T: Clone,
        U: Send + Sync + 'static,
        I: IntoIterator<Item = U>,
        F: Fn(T) -> I + Send + Sync + 'static,
    {
        let link = erase_transform::<T, _>("flat_map", move |typed: &T, _| {
            EdgeYield::Many(
                f(typed.clone())
                    .into_iter()
                    .map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
                    .collect(),
            )
        });
        self.relink(Some(link), None)
    }

    /// Name the Entity each value is routed to, making the consumer a
    /// dynamic-mode Procedure whose Entities are created on first sight of a
    /// key. This is the only way to declare dynamic keying — the consumer
    /// Procedure carries no keying member at all, so a reader of `channels()`
    /// can see where work fans out.
    ///
    /// Total by signature: it names an Entity, it never declines. Discard
    /// inputs with [`filter`](Stream::filter) before it. It is evaluated on the
    /// **post-transform** value, so it must be the last link before `.into`.
    /// Wiring it onto a Procedure declaring `entity_keys()` (static mode) is a
    /// registration error.
    pub fn key_by<F>(self, key: F) -> Self
    where
        F: Fn(&T) -> String + Send + Sync + 'static,
    {
        self.relink(None, Some(erase::<T, _, _>("key_by", key)))
    }

    /// Override the default `OverflowPolicy` (`DropNothing` for every
    /// channel kind). On an entry channel `DropNothing` parks the shared wire
    /// reader on this operative's saturation (see
    /// `docs/sdk/rust_operative_architecture.md` for the co-tenancy cost);
    /// declare `DropNewest` / `DropOldest` when shedding under saturation is
    /// acceptable and ordering or completeness can be sacrificed for liveness.
    pub fn on_full(self, policy: OverflowPolicy) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].overflow_policy = policy);
        self
    }

    /// Override the consumer-side retry budget (total attempts, default 3)
    /// applied before a failing payload is parked to the dead-letter queue.
    pub fn retry_max(self, max_attempts: u32) -> Self {
        self.with_channel(|st, cid| set_retry_max(st, cid, max_attempts));
        self
    }

    /// Override the exponential-backoff base (milliseconds, default 50)
    /// waited out between failed attempts. The wait holds no thread — the
    /// entity becomes eligible again at its due time.
    pub fn retry_backoff_ms(self, base_ms: u64) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].retry.backoff_base_ms = base_ms);
        self
    }
}

impl<'b, T: 'static + Send + Sync> Stream<'b, T, NodeTarget> {
    /// Branching sugar: each arm receives a copy of this handle, so
    /// `s.branch([&|b| { b.into::<A>(); }, &|b| { b.into::<B>(); }])` builds
    /// exactly the topology of two `s.into` statements — nothing more. At
    /// least two arms are required (checked at registration); the two
    /// spellings are freely mixable.
    pub fn branch<const N: usize>(self, arms: [&dyn Fn(Stream<'b, T>); N]) -> Self {
        if N < 2 {
            self.builder
                .state
                .borrow_mut()
                .errors
                .push(TopologyIssue::BranchTooFewArms { arms: N });
        }
        for arm in arms {
            arm(self);
        }
        self
    }

    /// Override the auto-derived channel name (the producer node's identity)
    /// everywhere the channel name appears: wire metadata, journals, errors.
    pub fn named(self, name: &'static str) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].name = name);
        self
    }

    /// Declare the standard inter-proc byte codec (postcard over `T`'s serde
    /// derives) so this channel's handoff batch is journal-able at the
    /// workflow proc-commit barrier and re-seedable on crash-resume. Required
    /// on every inter-proc channel of a durable structured workflow class.
    pub fn codec(self) -> Self
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        self.with_channel(|st, cid| set_codec::<T>(st, cid));
        self
    }

    /// Declare a custom typed→bytes / bytes→typed codec pair for payload
    /// types without the standard serde derives.
    pub fn codec_with<E, D>(self, encode: E, decode: D) -> Self
    where
        E: Fn(&T) -> Result<Vec<u8>, String> + Send + Sync + 'static,
        D: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
    {
        self.with_channel(|st, cid| set_codec_with::<T, E, D>(st, cid, encode, decode));
        self
    }

    /// Opt this channel into output identity: every plugin emit must carry
    /// a natural key via `ctx.emit_keyed(key, value)` and the runtime
    /// stamps `flz.entity` / `flz.seq` / `flz.key` on the wire frame.
    /// Requires a plugin wire (`sink`); `build_topology` rejects a keyed
    /// channel without one.
    pub fn keyed(self) -> Self {
        self.with_channel(|st, cid| st.channels[cid as usize].keyed = true);
        self
    }

    /// Attach this channel to the named plugin sink: every value emitted on it
    /// is serialized once and delivered to the controller's output layer
    /// stamped with the channel name, where the plugins attached to that name
    /// consume it. Several channels may name one sink.
    ///
    /// The handle is returned, so `.into::<S>()` consumer wiring chains after
    /// it and the value then reaches both the consumer mailboxes (typed) and
    /// the sink (as bytes). A channel with no consumers is the plugin-bound
    /// terminal — terminality is the absence of further wiring, not a
    /// different method. [`sealed`](Stream::sealed) states that in the type.
    ///
    /// The serializer runs once per emit regardless of consumer count.
    /// Declaring a second sink on one channel is a registration error
    /// (`TopologyIssue::DuplicatePluginSink`); the first call wins.
    pub fn sink_with<F>(self, plugin_name: &'static str, serializer: F) -> Self
    where
        F: Fn(&T) -> Vec<u8> + Send + Sync + 'static,
    {
        let serializer_erased: crate::channel::PluginSerializeFn =
            erase::<T, _, _>("plugin serializer", serializer);
        self.attach_plugin_sink(plugin_name, serializer_erased)
    }

    /// [`sink_with`](Stream::sink_with) with the default serialization rules:
    /// passthrough for `Vec<u8>`, UTF-8 for `String`, the SDK's default JSON
    /// codec otherwise.
    pub fn sink(self, plugin_name: &'static str) -> Self
    where
        T: serde::Serialize,
    {
        self.attach_plugin_sink(plugin_name, default_plugin_serializer::<T>())
    }

    /// State that nothing follows this channel: the returned [`Channel`]
    /// exposes no builder methods, so a later consumer or transform is a
    /// compile error. Purely type-level — it touches no builder state, so the
    /// topology is identical with or without it.
    pub fn sealed(self) -> Channel<T> {
        Channel::new()
    }

    fn attach_plugin_sink(
        self,
        plugin_name: &'static str,
        serializer: crate::channel::PluginSerializeFn,
    ) -> Self {
        self.with_channel(|st, cid| {
            let entry = &mut st.channels[cid as usize];
            if entry.plugin_sink.is_some() {
                let channel = entry.name;
                st.errors
                    .push(TopologyIssue::DuplicatePluginSink { channel });
                return;
            }
            entry.plugin_sink = Some(PluginSink {
                name: plugin_name,
                serializer,
            });
        });
        self
    }
}

/// Apply a `.retry_max(...)` declaration, rejecting a zero budget at
/// registration time (surfaced by `build_topology` like every other
/// declaration error).
fn set_retry_max(st: &mut BuilderState, channel_id: u32, max_attempts: u32) {
    let entry = &mut st.channels[channel_id as usize];
    if max_attempts == 0 {
        let channel = entry.name;
        st.errors.push(TopologyIssue::RetryMaxZero { channel });
        return;
    }
    entry.retry.max_attempts = max_attempts;
}

/// Install the standard inter-proc byte codec for `T` — postcard over the
/// SDK's standard serde derives, the same wire format the effect records
/// use. Shared by the `.codec()` declarations on both builder stages.
fn set_codec<T>(st: &mut BuilderState, channel_id: u32)
where
    T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    set_codec_with::<T, _, _>(
        st,
        channel_id,
        |typed: &T| {
            postcard::to_allocvec(typed).map_err(|e| format!("inter-proc codec encode failed: {e}"))
        },
        |bytes: Vec<u8>| {
            postcard::from_bytes::<T>(&bytes)
                .map_err(|e| format!("inter-proc codec decode failed: {e}"))
        },
    );
}

/// Install a custom typed→bytes / bytes→typed codec pair, type-erasing both
/// halves onto the channel entry.
fn set_codec_with<T, E, D>(st: &mut BuilderState, channel_id: u32, encode: E, decode: D)
where
    T: Send + Sync + 'static,
    E: Fn(&T) -> Result<Vec<u8>, String> + Send + Sync + 'static,
    D: Fn(Vec<u8>) -> Result<T, String> + Send + Sync + 'static,
{
    let encode_erased: crate::channel::CodecEncodeFn = Arc::new(move |val: &dyn Any| {
        let typed = val
            .downcast_ref::<T>()
            .ok_or_else(|| "inter-proc codec: type mismatch (build-time invariant)".to_string())?;
        encode(typed)
    });
    let decode_erased: crate::channel::EntryWrapFn = Arc::new(move |bytes: Vec<u8>| {
        decode(bytes).map(|v| Arc::new(v) as Arc<dyn Any + Send + Sync>)
    });
    st.channels[channel_id as usize].codec = Some(crate::channel::ChannelCodec {
        encode: encode_erased,
        decode: decode_erased,
    });
}

/// The default plugin serialization for `T`: passthrough for `Vec<u8>`, UTF-8
/// bytes for `String`, the SDK's default JSON codec otherwise — what
/// `sink(...)` applies when the author supplies no serializer.
fn default_plugin_serializer<T: serde::Serialize + Send + Sync + 'static>(
) -> crate::channel::PluginSerializeFn {
    Arc::new(|val: &dyn Any| -> Vec<u8> {
        if let Some(bytes) = val.downcast_ref::<Vec<u8>>() {
            return bytes.clone();
        }
        if let Some(s) = val.downcast_ref::<String>() {
            return s.as_bytes().to_vec();
        }
        let typed = val
            .downcast_ref::<T>()
            .expect("plugin serializer: type mismatch (build-time invariant)");
        serde_json::to_vec(typed).unwrap_or_else(|e| {
            eprintln!("[flezha] default plugin serialization failed: {e}");
            Vec::new()
        })
    })
}
