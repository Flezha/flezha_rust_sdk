//! Per-Shard typed dispatcher.
//!
//! `ShardDispatcherImpl<S>` carries S statically so:
//! - mailbox payloads are downcast to `Arc<S::Input>` without allocation
//! - the partition's state is downcast to `(S, S::State)` and threaded
//!   through `S::run` with a typed signature
//! - the emit sink already knows the cached output channel id
//!
//! The dispatcher trait object stores a single `Arc<dyn ShardDispatcher>`
//! per `PartitionSlot`. The inflight CAS on the mailbox guarantees that
//! `dispatch(...)` is never called concurrently for the same slot, so
//! the per-partition `(S, S::State)` is safely accessible behind a
//! `Mutex` lock that is always uncontended in practice.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

use crate::channel::builder::BuildPartitionArgs;
use crate::channel::{ChannelId, ChannelKind, OverflowPolicy, ShardId};
use crate::executor::mailbox::Mailbox;
use crate::partition::flusher::{
    current_send_frame_fn, encode_partition_identity, FlushableBuffer, PartitionFlusher,
    TypedBuffer,
};
use crate::partition::header::HeaderTemplate;
use crate::partition::{PartitionPayload, PartitionSlot, ShardDispatcher};
use crate::relay::instance::RelayInstance;
use crate::shard::change::NoChange;
use crate::shard::ctx::EmitSink;
use crate::shard::state_handle::StateHandle;
use crate::shard::{Shard, ShardCtx, ShardError};

/// Per-partition holder for a typed Shard instance + its State. Boxed and
/// type-erased into `PartitionSlot.state`.
pub struct PartitionState<S: Shard> {
    pub shard: S,
    pub state: S::State,
}

/// Build the per-Shard serialize/deserialize state fn pointers used by
/// `serialize_relay_state` and `apply_init_values`. The closure captures
/// S statically; `serialize_state` downcasts the type-erased state to
/// `PartitionState<S>` and emits `serde_json::Value` via `S::State:
/// Serialize`. `deserialize_state` parses a `serde_json::Value` into
/// `S::State` and wraps it with a fresh `S::default()` to match what the
/// dispatcher expects when it locks the Mutex.
pub(crate) fn build_state_fns<S: Shard>() -> (
    crate::channel::builder::SerializeStateFn,
    crate::channel::builder::DeserializeStateFn,
) {
    let serialize: crate::channel::builder::SerializeStateFn = Arc::new(
        |state: &Box<dyn Any + Send>| -> Result<serde_json::Value, String> {
            let typed = state
                .downcast_ref::<PartitionState<S>>()
                .ok_or_else(|| "serialize_state: state type mismatch".to_string())?;
            serde_json::to_value(&typed.state).map_err(|e| format!("serialize_state: {}", e))
        },
    );
    let deserialize: crate::channel::builder::DeserializeStateFn = Arc::new(
        |value: &serde_json::Value| -> Result<Box<dyn Any + Send>, String> {
            let state: S::State = serde_json::from_value(value.clone())
                .map_err(|e| format!("deserialize_state: {}", e))?;
            let typed = PartitionState::<S> {
                shard: S::default(),
                state,
            };
            Ok(Box::new(typed) as Box<dyn Any + Send>)
        },
    );
    (serialize, deserialize)
}

/// Typed dispatcher. Caches the producer-edge output channel id at
/// construction so emit pays zero name-lookup cost on the hot path.
pub struct ShardDispatcherImpl<S: Shard> {
    shard_id: ShardId,
    output_channel_id: Option<ChannelId>,
    output_channel_kind: ChannelKind,
    output_channel_name: Option<Arc<str>>,
    _phantom: std::marker::PhantomData<fn() -> S>,
}

impl<S: Shard> ShardDispatcherImpl<S> {
    pub fn new(
        shard_id: ShardId,
        output_channel_id: Option<ChannelId>,
        output_channel_kind: ChannelKind,
        output_channel_name: Option<Arc<str>>,
    ) -> Self {
        Self {
            shard_id,
            output_channel_id,
            output_channel_kind,
            output_channel_name,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<S: Shard> ShardDispatcher for ShardDispatcherImpl<S> {
    fn dispatch(
        &self,
        payload: PartitionPayload,
        state: &mut Box<dyn Any + Send>,
        flush_buffer: &mut Box<dyn FlushableBuffer>,
        instance: &Weak<RelayInstance>,
    ) {
        let PartitionPayload { value, stream } = payload;

        // Downcast value to Arc<S::Input>. Consumers with different
        // Input types in the same Relay downcast independently because
        // the channel wiring guarantees they only ever receive their
        // own type.
        let input_arc = match Arc::downcast::<S::Input>(value) {
            Ok(arc) => arc,
            Err(_) => {
                eprintln!(
                    "[flezha] dispatcher: payload type mismatch for shard {}",
                    S::name()
                );
                return;
            }
        };

        // Zero-copy when the Arc is uniquely held; falls back to a
        // value clone when fan-out kept it shared. The `Clone` bound on
        // `Shard::Input` makes this explicit at the type level.
        let input: S::Input = Arc::unwrap_or_clone(input_arc);

        let typed_state = state
            .downcast_mut::<PartitionState<S>>()
            .expect("dispatcher: state type mismatch for shard");

        let typed_buffer = flush_buffer
            .as_any_mut()
            .downcast_mut::<TypedBuffer<S>>()
            .expect("dispatcher: flush buffer type mismatch for shard");

        let mut sink = PartitionEmitSink {
            output_channel_id: self.output_channel_id,
            output_channel_kind: self.output_channel_kind,
            output_channel_name: self.output_channel_name.as_deref(),
            instance: instance.upgrade(),
            stream: Arc::clone(&stream),
        };

        let mut ctx = ShardCtx::new(&mut sink);
        let handle = StateHandle::<S>::new(&mut typed_state.state, &mut typed_buffer.inner);

        if let Err(err) = typed_state.shard.run(input, handle, &mut ctx) {
            eprintln!("[flezha] shard {} run error: {}", S::name(), err);
        }
    }

    fn shard_id(&self) -> ShardId {
        self.shard_id
    }
}

/// Per-invocation emit sink, constructed fresh inside each `dispatch`
/// call. Holds an upgraded `Arc<RelayInstance>` for the duration of the
/// run; if the instance has been dropped, every emit returns
/// `UnknownChannel`.
struct PartitionEmitSink<'a> {
    output_channel_id: Option<ChannelId>,
    output_channel_kind: ChannelKind,
    output_channel_name: Option<&'a str>,
    instance: Option<Arc<RelayInstance>>,
    stream: Arc<FlezhaStream>,
}

impl<'a> PartitionEmitSink<'a> {
    /// Resolve the channel descriptor + RelayInstance for the cached
    /// output channel. Returns `UnknownChannel` if either has been
    /// dropped or is unset.
    fn resolve_channel(&self) -> Result<(Arc<RelayInstance>, ChannelId), ShardError> {
        let cid = self
            .output_channel_id
            .ok_or_else(|| ShardError::UnknownChannel {
                channel: self
                    .output_channel_name
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "<unknown>".to_string()),
            })?;
        let instance = self
            .instance
            .as_ref()
            .ok_or_else(|| ShardError::UnknownChannel {
                channel: self
                    .output_channel_name
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "<unknown>".to_string()),
            })?
            .clone();
        Ok((instance, cid))
    }

    /// Fan out a typed value to every inter-shard consumer's mailbox
    /// per the channel's `OverflowPolicy`. Shared between hybrid and
    /// pure-`InterShard` paths.
    fn fan_out_to_consumers(
        &self,
        instance: &RelayInstance,
        descriptor: &crate::channel::ChannelDescriptor,
        value: &Arc<dyn Any + Send + Sync>,
    ) -> Result<(), ShardError> {
        let policy = descriptor.overflow_policy;
        for &consumer_shard_id in &descriptor.consumers {
            let consumer_shard = instance.topology.shard(consumer_shard_id).ok_or_else(|| {
                ShardError::UnknownChannel {
                    channel: format!("<shard id {}>", consumer_shard_id),
                }
            })?;

            let partitions = match instance.partitions.get(consumer_shard_id as usize) {
                Some(p) => p,
                None => continue,
            };

            if consumer_shard.partition_keys.is_empty() {
                if let Some(slot) = partitions.first() {
                    enqueue(slot, payload_for(value, &self.stream), policy);
                }
            } else {
                let key_to_idx = instance
                    .partition_key_to_idx
                    .get(consumer_shard_id as usize);
                let keys = (consumer_shard.partition_for_erased)(value.as_ref());
                for key in keys {
                    if let Some(map) = key_to_idx {
                        if let Some(&idx) = map.get(key) {
                            if let Some(slot) = partitions.get(idx) {
                                enqueue(slot, payload_for(value, &self.stream), policy);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Hybrid plugin-sink tail: serialize once inside `catch_unwind`
    /// and forward the bytes through the existing plugin-wire framing
    /// path. A serializer panic surfaces as
    /// `ShardError::PluginSerializerPanic` to the producing Shard;
    /// the dispatcher thread is unaffected.
    fn emit_plugin_sink(
        &self,
        instance: &RelayInstance,
        descriptor: &crate::channel::ChannelDescriptor,
        sink: &crate::channel::PluginSink,
        value: &Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
    ) -> Result<(), ShardError> {
        let serializer = Arc::clone(&sink.serializer);
        let value_for_panic = Arc::clone(value);
        let bytes_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (serializer)(value_for_panic.as_ref())
        }));
        let bytes = match bytes_result {
            Ok(b) => b,
            Err(panic) => {
                return Err(ShardError::PluginSerializerPanic {
                    channel: descriptor.name.to_string(),
                    message: crate::shard::error::format_panic(panic),
                });
            }
        };

        let mut metadata: HashMap<String, String> = match meta {
            Some(mut m) => {
                m.remove("channel");
                m
            }
            None => HashMap::new(),
        };
        metadata.insert("channel".to_string(), descriptor.name.to_string());

        // `sink.name` is informational on the SDK side (the wire frame
        // routes by `channel: String`); kept for parity with
        // `to_plugin(...)` semantics and useful for diagnostics.
        let _plugin_name = &sink.name;
        send_plugin_frame(&self.stream, &instance.instance_id, &bytes, &metadata);
        Ok(())
    }
}

impl<'a> EmitSink for PartitionEmitSink<'a> {
    fn emit_inter_shard(&mut self, value: Arc<dyn Any + Send + Sync>) -> Result<(), ShardError> {
        let (instance, cid) = self.resolve_channel()?;
        let descriptor =
            instance
                .topology
                .channel(cid)
                .ok_or_else(|| ShardError::UnknownChannel {
                    channel: format!("<channel id {}>", cid),
                })?;
        self.fan_out_to_consumers(&instance, descriptor, &value)?;
        if let Some(sink) = descriptor.plugin_sink.as_ref() {
            self.emit_plugin_sink(&instance, descriptor, sink, &value, None)?;
        }
        Ok(())
    }

    fn emit_inter_shard_with_meta(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
    ) -> Result<(), ShardError> {
        let (instance, cid) = self.resolve_channel()?;
        let descriptor =
            instance
                .topology
                .channel(cid)
                .ok_or_else(|| ShardError::UnknownChannel {
                    channel: format!("<channel id {}>", cid),
                })?;
        self.fan_out_to_consumers(&instance, descriptor, &value)?;
        if let Some(sink) = descriptor.plugin_sink.as_ref() {
            self.emit_plugin_sink(&instance, descriptor, sink, &value, meta)?;
        }
        Ok(())
    }

    fn emit_plugin(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
    ) -> Result<(), ShardError> {
        let cid = self
            .output_channel_id
            .ok_or_else(|| ShardError::UnknownChannel {
                channel: self.channel_name().to_string(),
            })?;
        let instance = self
            .instance
            .as_ref()
            .ok_or_else(|| ShardError::UnknownChannel {
                channel: self.channel_name().to_string(),
            })?;
        let descriptor =
            instance
                .topology
                .channel(cid)
                .ok_or_else(|| ShardError::UnknownChannel {
                    channel: format!("<channel id {}>", cid),
                })?;

        let serialize = descriptor.plugin_serializer.as_ref().ok_or_else(|| {
            ShardError::Custom(format!(
                "plugin channel `{}` is missing a serializer (use .to_plugin(name) at \
                 declaration time)",
                descriptor.name
            ))
        })?;

        let bytes = serialize(&*value);

        let mut metadata: HashMap<String, String> = match meta {
            Some(mut m) => {
                // Reserved key: drop any user-supplied "channel" entry
                // per umbrella's "ShardCtx emit semantics" requirement.
                m.remove("channel");
                m
            }
            None => HashMap::new(),
        };
        metadata.insert("channel".to_string(), descriptor.name.to_string());

        send_plugin_frame(&self.stream, &instance.instance_id, &bytes, &metadata);
        Ok(())
    }

    fn channel_name(&self) -> &str {
        self.output_channel_name.unwrap_or("<unset>")
    }

    fn is_plugin_bound(&self) -> bool {
        matches!(self.output_channel_kind, ChannelKind::PluginBound)
    }

    fn has_plugin_sink(&self) -> bool {
        // Resolving the descriptor each time keeps the trait surface
        // small. The cached `output_channel_kind` is used for the
        // pure-plugin fast path; hybrid detection is rare on the hot
        // path (a single `Option::is_some()` after a topology lookup).
        self.instance
            .as_ref()
            .and_then(|inst| {
                self.output_channel_id
                    .and_then(|cid| inst.topology.channel(cid))
            })
            .map(|d| d.plugin_sink.is_some())
            .unwrap_or(false)
    }
}

/// Apply the channel's `OverflowPolicy` when pushing onto a Partition's
/// bounded mailbox. Bumps the appropriate per-Partition counter on every
/// outcome and submits a drain task on success / DropOldest. On Block,
/// parks the calling thread on the slot's `BlockParker` and retries
/// until the push succeeds (Decision 3).
pub(crate) fn enqueue(
    slot: &Arc<PartitionSlot>,
    payload: PartitionPayload,
    policy: OverflowPolicy,
) {
    let mut next = payload;
    loop {
        match slot.mailbox.try_push(next) {
            Ok(()) => {
                slot.mailbox
                    .counters()
                    .pushes_total
                    .fetch_add(1, Ordering::Relaxed);
                if slot.mailbox.try_mark_inflight() {
                    let cloned = Arc::clone(slot);
                    slot.pool.submit(Box::new(move || drain_partition(&cloned)));
                }
                return;
            }
            Err(returned) => match policy {
                OverflowPolicy::Block => {
                    slot.mailbox
                        .counters()
                        .pushes_blocked_total
                        .fetch_add(1, Ordering::Relaxed);
                    next = returned;
                    slot.block_parker.wait_one();
                }
                OverflowPolicy::DropNewest => {
                    slot.mailbox
                        .counters()
                        .drops_newest_total
                        .fetch_add(1, Ordering::Relaxed);
                    drop(returned);
                    return;
                }
                OverflowPolicy::DropOldest => {
                    let _dropped = slot.mailbox.force_push(returned);
                    slot.mailbox
                        .counters()
                        .drops_oldest_total
                        .fetch_add(1, Ordering::Relaxed);
                    if slot.mailbox.try_mark_inflight() {
                        let cloned = Arc::clone(slot);
                        slot.pool.submit(Box::new(move || drain_partition(&cloned)));
                    }
                    return;
                }
            },
        }
    }
}

fn payload_for(value: &Arc<dyn Any + Send + Sync>, stream: &Arc<FlezhaStream>) -> PartitionPayload {
    PartitionPayload {
        value: Arc::clone(value),
        stream: Arc::clone(stream),
    }
}

fn send_plugin_frame(
    stream: &Arc<FlezhaStream>,
    instance_id: &str,
    bytes: &[u8],
    metadata: &HashMap<String, String>,
) {
    let frame = payloader::build_from_binary(instance_id, bytes, metadata.clone());
    if let Err(e) = (&**stream).write_all(&frame) {
        eprintln!(
            "[flezha] plugin emit: failed to write {} bytes for {}: {}",
            bytes.len(),
            instance_id,
            e
        );
    }
}

pub fn drain_partition(slot: &Arc<PartitionSlot>) {
    loop {
        while let Some(payload) = slot.mailbox.pop() {
            // Free one waiter (if any) before running the dispatcher;
            // the popped slot is now available for the unparked producer
            // to re-attempt its push.
            slot.block_parker.unpark_one();
            let mut state_guard = slot.state.lock().unwrap();
            let mut buffer_guard = match slot.flush_buffer.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            let instance_weak = Weak::clone(&slot.instance);
            slot.dispatcher
                .dispatch(payload, &mut state_guard, &mut buffer_guard, &instance_weak);
        }
        slot.mailbox.release_inflight();
        if slot.mailbox.is_empty() {
            return;
        }
        if !slot.mailbox.try_mark_inflight() {
            return;
        }
    }
}

/// Build one `PartitionSlot` for a Shard. Captures S statically; the
/// channel builder collects this closure into `ShardEntry.build_partition`
/// during `Relay::channels` and `build_topology` invokes it once per
/// partition at relay-instance startup.
pub fn build_partition_slot<S: Shard>(args: BuildPartitionArgs<'_>) -> Arc<PartitionSlot> {
    let shard = S::default();
    // v1 cold-start path: `init_payload` is `None`. The controller change
    // wires the warm-restart path through `init_partition_with_payload`
    // and calls a richer constructor.
    let state = S::init_state(args.shard_key, None);
    let typed_state = PartitionState::<S> { shard, state };

    let topology = Arc::clone(&args.topology);
    let shard_desc = topology
        .shard(args.shard_id)
        .expect("build_partition_slot: shard id missing in topology");

    let dispatcher = Arc::new(ShardDispatcherImpl::<S>::new(
        args.shard_id,
        shard_desc.output_channel_id,
        shard_desc
            .output_channel_id
            .and_then(|id| topology.channel(id))
            .map(|c| c.kind)
            .unwrap_or(ChannelKind::InterShard),
        shard_desc
            .output_channel_id
            .and_then(|id| topology.channel(id))
            .map(|c| Arc::clone(&c.name)),
    )) as Arc<dyn ShardDispatcher>;

    let flush_buffer: Arc<Mutex<Box<dyn FlushableBuffer>>> = Arc::new(Mutex::new(Box::new(
        TypedBuffer::<S>::default(),
    )
        as Box<dyn FlushableBuffer>));

    let change_seq = Arc::new(AtomicU64::new(0));
    let partition_identity = Arc::new(encode_partition_identity(
        topology.factory_id,
        shard_desc.name.as_ref(),
        args.shard_key.unwrap_or(""),
    ));

    // Skip flusher registration entirely for `Change == NoChange` Shards
    // per `design.md` Open Question 2 (TypeId comparison at registration
    // time). The PartitionSlot still carries a (always-empty) flush
    // buffer so the dispatcher can construct a `StateHandle<'_, S>`
    // uniformly for all Shards; only the per-process flusher registry
    // is conditional.
    if TypeId::of::<S::Change>() != TypeId::of::<NoChange>() {
        let flusher = PartitionFlusher::instance();
        flusher.register(
            Arc::clone(&partition_identity),
            Arc::clone(&flush_buffer),
            args.persistence_enabled,
            Arc::clone(&change_seq),
            current_send_frame_fn(),
            HeaderTemplate::for_shard::<S>(),
        );
    }

    Arc::new(PartitionSlot {
        state: std::sync::Mutex::new(Box::new(typed_state)),
        mailbox: Arc::new(Mailbox::with_capacity(shard_desc.mailbox_capacity)),
        dispatcher,
        shard_id: args.shard_id,
        shard_key: args.shard_key.map(Arc::from),
        pool: args.pool,
        instance: args.instance,
        topology,
        block_parker: crate::partition::BlockParker::new(),
        flush_buffer,
        persistence_enabled: args.persistence_enabled,
        change_seq,
        partition_identity,
    })
}
