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
use std::cell::RefCell;
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
    encode_partition_identity, FlushableBuffer, PartitionFlusher, TypedBuffer,
};
use crate::partition::header::HeaderTemplate;
use crate::partition::{make_partition_payload, PartitionPayload, PartitionSlot, ShardDispatcher};
use crate::relay::instance::RelayInstance;
use crate::shard::change::NoChange;
use crate::shard::ctx::EmitSink;
use crate::shard::state_handle::StateHandle;
use crate::shard::{Shard, ShardCtx, ShardError};
use crate::util::LockRecover;

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
    /// Flush-before-emit wiring, mirrored from the owning `PartitionSlot`
    /// so the emit sink can drain this partition's just-applied changes to
    /// the durable WAL path before its output frame is written.
    flush: FlushWiring,
    _phantom: std::marker::PhantomData<fn() -> S>,
}

/// Per-partition state the dispatcher needs to drive flush-before-emit.
/// Cloned cheaply (Arc handles + a Copy header) into the emit sink.
#[derive(Clone)]
struct FlushWiring {
    partition_identity: Arc<Vec<u8>>,
    change_seq: Arc<AtomicU64>,
    header: HeaderTemplate,
    persistence_enabled: bool,
}

impl<S: Shard> ShardDispatcherImpl<S> {
    fn new(
        shard_id: ShardId,
        output_channel_id: Option<ChannelId>,
        output_channel_kind: ChannelKind,
        output_channel_name: Option<Arc<str>>,
        flush: FlushWiring,
    ) -> Self {
        Self {
            shard_id,
            output_channel_id,
            output_channel_kind,
            output_channel_name,
            flush,
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

        // Share the change buffer between `StateHandle::apply` and the
        // flush-before-emit hook. Both run on this single dispatch thread
        // inside one `Shard::run`, so the `RefCell` borrows never overlap.
        let buffer_cell = RefCell::new(std::mem::take(&mut typed_buffer.inner));

        // Flush-before-emit: drain this partition's pending changes to the
        // durable WAL path before the sink writes the matching output
        // frame, so change N reaches the controller's WAL before N's
        // output reaches Kafka. Gated on `persistence_enabled` (and the
        // `FLEZHA_FLUSH_BEFORE_EMIT` knob) so `NoChange` / non-persistent
        // relays keep the zero-cost inline emit.
        let flush_before_emit: Option<FlushBeforeEmit> =
            if self.flush.persistence_enabled && flush_before_emit_enabled() {
                let identity = Arc::clone(&self.flush.partition_identity);
                let seq = Arc::clone(&self.flush.change_seq);
                let header = self.flush.header;
                let cell = &buffer_cell;
                Some(Box::new(move || {
                    let mut buf = TypedBuffer::<S> {
                        inner: std::mem::take(&mut *cell.borrow_mut()),
                    };
                    crate::partition::flusher::flush_one(&mut buf, &identity, &seq, &header, true);
                }))
            } else {
                None
            };

        let mut sink = PartitionEmitSink {
            output_channel_id: self.output_channel_id,
            output_channel_kind: self.output_channel_kind,
            output_channel_name: self.output_channel_name.as_deref(),
            instance: instance.upgrade(),
            stream: Arc::clone(&stream),
            flush_before_emit,
        };

        let mut ctx = ShardCtx::new(&mut sink);
        let handle = StateHandle::<S>::new(&mut typed_state.state, &buffer_cell);

        // Contain a shard panic so it can't poison the held `state` lock.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            typed_state.shard.run(input, handle, &mut ctx)
        }));

        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(err)) => eprintln!("[flezha] shard {} run error: {}", S::name(), err),
            Err(_) => {
                eprintln!(
                    "[flezha] shard {} run panicked; partition continues",
                    S::name()
                )
            }
        }

        drop(sink);
        // Return any changes not yet drained by flush-before-emit (or all
        // of them when the hook is disabled) to the buffer the background
        // flusher drains. `change_seq` and the move-out drain guarantee
        // each change is shipped exactly once.
        typed_buffer.inner = buffer_cell.into_inner();
    }

    fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    fn restore_state(
        &self,
        state: &mut Box<dyn Any + Send>,
        snapshot_bytes: &[u8],
        change_log_bytes: &[u8],
        shard_key: Option<&str>,
    ) -> Result<u64, crate::partition::restore::RestoreError> {
        use crate::partition::restore::{
            init_partition_with_payload, iter_change_log_records, RestoreInitPayload,
        };

        let payload = RestoreInitPayload {
            snapshot_bytes,
            change_log_bytes,
        };

        // Rebuild the typed state from snapshot + change-log replay. This runs
        // the same path as cold-start `init_state` + `apply_change`, so a
        // counter that summed to N comes back at N rather than 0.
        let new_state = init_partition_with_payload::<S>(shard_key, Some(&payload))?;

        let typed = state
            .downcast_mut::<PartitionState<S>>()
            .expect("restore_state: state type mismatch for shard");
        typed.state = new_state;

        // Terminal seq = highest WAL seq applied, so the caller can re-arm the
        // partition's change_seq and post-restore appends continue contiguously
        // (no controller-side epoch realign / truncation).
        let mut terminal: u64 = 0;
        for record in iter_change_log_records(change_log_bytes) {
            terminal = terminal.max(record?.seq);
        }
        Ok(terminal)
    }
}

/// Synchronous drain of the producing partition's pending changes onto
/// the durable WAL path, invoked before each output frame is written.
type FlushBeforeEmit<'a> = Box<dyn FnMut() + 'a>;

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
    /// `Some` for persistence-enabled partitions with the flush-before-emit
    /// knob on. Called at the top of every emit so this run's just-applied
    /// changes are durably shipped before the output frame leaves.
    flush_before_emit: Option<FlushBeforeEmit<'a>>,
}

impl<'a> PartitionEmitSink<'a> {
    /// Drain pending changes onto the WAL path before an output frame is
    /// written. No-op for non-persistent / `NoChange` relays.
    fn flush_pending(&mut self) {
        if let Some(flush) = self.flush_before_emit.as_mut() {
            flush();
        }
    }

    /// `UnknownChannel` carrying the cached output channel name (or
    /// `<unknown>` when unset), for the resolve failure paths.
    fn unknown_channel(&self) -> ShardError {
        ShardError::UnknownChannel {
            channel: self
                .output_channel_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| "<unknown>".to_string()),
        }
    }

    /// Resolve the channel descriptor + RelayInstance for the cached
    /// output channel. Returns `UnknownChannel` if either has been
    /// dropped or is unset.
    fn resolve_channel(&self) -> Result<(Arc<RelayInstance>, ChannelId), ShardError> {
        let cid = self
            .output_channel_id
            .ok_or_else(|| self.unknown_channel())?;
        let instance = self
            .instance
            .as_ref()
            .ok_or_else(|| self.unknown_channel())?
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
        route_to_consumers(
            instance,
            &descriptor.consumers,
            descriptor.overflow_policy,
            value,
            &self.stream,
        );
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

        let metadata = channel_metadata(meta, &descriptor.name);

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
        self.emit_inter_shard_with_meta(value, None)
    }

    fn emit_inter_shard_with_meta(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
    ) -> Result<(), ShardError> {
        self.flush_pending();
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
        self.flush_pending();
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

        // Reserved key: any caller-supplied "channel" entry is dropped per
        // ShardCtx emit semantics, then the real channel name is stamped.
        let metadata = channel_metadata(meta, &descriptor.name);

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
/// Bounded park per Block-policy retry; progress is re-checked (and the
/// inline drain re-attempted) on every wakeup.
const BLOCK_PARK_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(50);

/// One throttled warning per blocked episode once it exceeds this — a full
/// mailbox holding a producer this long means a consumer shard is wedged or
/// severely behind, and silence here is how the 2026-06-10 freeze hid.
const BLOCK_WARN_AFTER: std::time::Duration = std::time::Duration::from_secs(5);

pub(crate) fn enqueue(
    slot: &Arc<PartitionSlot>,
    payload: PartitionPayload,
    policy: OverflowPolicy,
) {
    let mut next = payload;
    let mut blocked_since: Option<std::time::Instant> = None;
    let mut block_warned = false;
    loop {
        match slot.mailbox.try_push(next) {
            Ok(()) => {
                slot.mailbox
                    .counters()
                    .pushes_total
                    .fetch_add(1, Ordering::Relaxed);
                // Activity stamp for the dynamic-mode reaper (one atomic
                // store; inert for single/static Partitions that are never
                // reaped).
                slot.last_activity_at
                    .store(crate::partition::dynamic::now_millis(), Ordering::Relaxed);
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
                    if blocked_since.is_none() {
                        blocked_since = Some(std::time::Instant::now());
                        crate::partition::BLOCKED_PARK_EPISODES.fetch_add(1, Ordering::Relaxed);
                    }
                    // Drain the full slot inline rather than parking on it:
                    // the slot's drain may exist only as a queued pool task,
                    // and a parked pool worker can be the lone thread able to
                    // run it (a one-worker pool self-deadlocks; prod
                    // 2026-06-10). The drain_active gate in drain_partition
                    // keeps this race-safe and order-preserving.
                    drain_partition(slot);
                    if slot.mailbox.is_full() {
                        // A live drainer on another thread owns progress;
                        // bounded park so even a lost wake degrades to a
                        // retry loop instead of a permanent freeze.
                        slot.block_parker
                            .wait_one_timeout(&slot.mailbox, BLOCK_PARK_TIMEOUT);
                    }
                    if !block_warned
                        && blocked_since.is_some_and(|t| t.elapsed() >= BLOCK_WARN_AFTER)
                    {
                        block_warned = true;
                        tracing::warn!(
                            target: "flezha::mailbox",
                            shard_id = slot.shard_id,
                            shard_key = ?slot.shard_key,
                            "Block-policy producer stalled >{:?} on a full partition mailbox; consumer shard wedged or far behind",
                            BLOCK_WARN_AFTER
                        );
                    }
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

/// Fan a typed value out to every consumer Shard's partition mailbox per
/// the channel's `OverflowPolicy`. Shared by the inter-shard emit path
/// (`PartitionEmitSink`) and the inbound wire-dispatch path
/// (`RelayDispatchHandle`). Single-mode shards route to partition 0;
/// static-mode shards route by `partition_for_erased` key lookup;
/// dynamic-mode shards derive a key via `shard_key` and lazily create the
/// Partition through the per-Shard `DynamicPartitionRegistry`.
pub(crate) fn route_to_consumers(
    instance: &RelayInstance,
    consumer_ids: &[ShardId],
    policy: OverflowPolicy,
    value: &Arc<dyn Any + Send + Sync>,
    stream: &Arc<FlezhaStream>,
) {
    use crate::relay::topology::PartitioningMode;
    for &consumer_shard_id in consumer_ids {
        let consumer = match instance.topology.shard(consumer_shard_id) {
            Some(s) => s,
            None => continue,
        };
        match consumer.mode {
            PartitioningMode::Dynamic => {
                let registry = match instance
                    .dynamic_registries
                    .get(consumer_shard_id as usize)
                    .and_then(|r| r.as_ref())
                {
                    Some(r) => r,
                    None => continue,
                };
                let key = match consumer
                    .shard_key_erased
                    .as_ref()
                    .and_then(|f| f(value.as_ref()))
                {
                    Some(k) => k,
                    None => {
                        registry
                            .counters
                            .inputs_dropped_no_key_total
                            .fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                };
                if let Ok(slot) = registry.get_or_create(&key) {
                    enqueue(&slot, make_partition_payload(value, stream), policy);
                }
            }
            PartitioningMode::Single => {
                if let Some(slot) = instance
                    .partitions
                    .get(consumer_shard_id as usize)
                    .and_then(|p| p.first())
                {
                    enqueue(slot, make_partition_payload(value, stream), policy);
                }
            }
            PartitioningMode::Static => {
                let partitions = match instance.partitions.get(consumer_shard_id as usize) {
                    Some(p) => p,
                    None => continue,
                };
                let key_to_idx = instance
                    .partition_key_to_idx
                    .get(consumer_shard_id as usize);
                for key in (consumer.partition_for_erased)(value.as_ref()) {
                    if let Some(&idx) = key_to_idx.and_then(|m| m.get(key)) {
                        if let Some(slot) = partitions.get(idx) {
                            enqueue(slot, make_partition_payload(value, stream), policy);
                        }
                    }
                }
            }
        }
    }
}

/// Build the emit wire metadata: drop any caller-supplied reserved
/// `channel` key, then stamp the descriptor's channel name.
fn channel_metadata(
    meta: Option<HashMap<String, String>>,
    channel: &str,
) -> HashMap<String, String> {
    let mut metadata = meta.unwrap_or_default();
    metadata.remove("channel");
    metadata.insert("channel".to_string(), channel.to_string());
    metadata
}

const ENV_FLUSH_BEFORE_EMIT: &str = "FLEZHA_FLUSH_BEFORE_EMIT";

/// Whether flush-before-emit is enabled for persistence-enabled partitions.
/// Defaults to ON (durability over latency); set `FLEZHA_FLUSH_BEFORE_EMIT`
/// to `0`/`false`/`off`/`no` to revert to the inline-emit fast path.
fn flush_before_emit_enabled() -> bool {
    match std::env::var(ENV_FLUSH_BEFORE_EMIT) {
        Ok(v) => !matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        ),
        Err(_) => true,
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
    // Single-ACTIVE-drainer gate. The queued pool task and a Block-policy
    // producer draining inline may both arrive here; the loser leaves and the
    // winner owns the pop loop, so per-slot dispatch order is preserved.
    if slot.drain_active.swap(true, Ordering::AcqRel) {
        return;
    }
    loop {
        while let Some(payload) = slot.mailbox.pop() {
            // Free one waiter (if any) before running the dispatcher;
            // the popped slot is now available for the unparked producer
            // to re-attempt its push.
            slot.block_parker.unpark_one();
            let mut state_guard = slot.state.lock_recover();
            let mut buffer_guard = slot.flush_buffer.lock_recover();
            let instance_weak = Weak::clone(&slot.instance);
            slot.dispatcher
                .dispatch(payload, &mut state_guard, &mut buffer_guard, &instance_weak);
            crate::partition::DISPATCHES_COMPLETED.fetch_add(1, Ordering::Relaxed);
        }
        // Drop the active gate BEFORE releasing inflight so a producer that
        // observes inflight=false can immediately become the next drainer.
        slot.drain_active.store(false, Ordering::Release);
        slot.mailbox.release_inflight();
        if slot.mailbox.is_empty() {
            return;
        }
        if !slot.mailbox.try_mark_inflight() {
            return;
        }
        if slot.drain_active.swap(true, Ordering::AcqRel) {
            // Another thread became the active drainer for the round we just
            // marked; it releases the inflight token when it finishes.
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

    let flush_buffer: Arc<Mutex<Box<dyn FlushableBuffer>>> = Arc::new(Mutex::new(Box::new(
        TypedBuffer::<S>::default(),
    )
        as Box<dyn FlushableBuffer>));

    let change_seq = Arc::new(AtomicU64::new(0));
    // Key the on-disk WAL identity by the relay INSTANCE id, not the factory
    // id: two relays of the same factory must not share one Partition WAL.
    let partition_identity = Arc::new(encode_partition_identity(
        args.instance_id.as_ref(),
        shard_desc.name.as_ref(),
        args.shard_key.unwrap_or(""),
    ));

    let output_channel = shard_desc
        .output_channel_id
        .and_then(|id| topology.channel(id));
    let dispatcher = Arc::new(ShardDispatcherImpl::<S>::new(
        args.shard_id,
        shard_desc.output_channel_id,
        output_channel
            .map(|c| c.kind)
            .unwrap_or(ChannelKind::InterShard),
        output_channel.map(|c| Arc::clone(&c.name)),
        FlushWiring {
            partition_identity: Arc::clone(&partition_identity),
            change_seq: Arc::clone(&change_seq),
            header: HeaderTemplate::for_shard::<S>(),
            persistence_enabled: args.persistence_enabled,
        },
    )) as Arc<dyn ShardDispatcher>;

    // Skip flusher registration entirely for `Change == NoChange` Shards
    // per `design.md` Open Question 2 (TypeId comparison at registration
    // time). The PartitionSlot still carries a (always-empty) flush
    // buffer so the dispatcher can construct a `StateHandle<'_, S>`
    // uniformly for all Shards; only the per-process flusher registry
    // is conditional.
    let flusher_entry_id = if TypeId::of::<S::Change>() != TypeId::of::<NoChange>() {
        let flusher = PartitionFlusher::instance();
        Some(flusher.register(
            Arc::clone(&partition_identity),
            Arc::clone(&flush_buffer),
            args.persistence_enabled,
            Arc::clone(&change_seq),
            HeaderTemplate::for_shard::<S>(),
        ))
    } else {
        None
    };

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
        drain_active: std::sync::atomic::AtomicBool::new(false),
        flush_buffer,
        persistence_enabled: args.persistence_enabled,
        change_seq,
        partition_identity,
        last_activity_at: std::sync::atomic::AtomicI64::new(
            crate::partition::dynamic::now_millis(),
        ),
        flusher_entry_id,
    })
}
