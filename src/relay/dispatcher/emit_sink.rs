//! The output an invocation's outputs leave through, [`CellEmitSink`], on the
//! connection its item rode in on. An emit is routed to its consumers (a
//! steps relay's held on its pass for the stage's commit) and its plugin output
//! written, or, for a Cell that records effects, captured into the
//! invocation's atomic effect record.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use flz_core::host::{end_stamps, ship_plugin_output, ShipAt};
use flz_core::routing::{route_to_consumers, Delivery};
use flz_core::util::wall_now_ms;
use flz_transport::transport::FlezhaStream;

use crate::cell::CellSlot;
use crate::channel::ChannelId;
use crate::relay::instance::RelayInstance;
use crate::tasklet::ctx::EmitSink;
use crate::tasklet::TaskletError;

/// Synchronous drain of the producing cell's pending changes onto
/// the durable WAL path, invoked before each output frame is written.
pub(super) type FlushBeforeEmit<'a> = Box<dyn FnMut() + 'a>;

/// Per-invocation emit output, constructed fresh inside each `dispatch`
/// call. Holds an upgraded `Arc<RelayInstance>` for the duration of the
/// run; if the instance has been dropped, every emit returns
/// `UnknownChannel`.
pub(super) struct CellEmitSink<'a> {
    /// The Cell this invocation belongs to. An emit it cannot deliver is
    /// held against this slot and this slot is handed no further work — the
    /// backpressure a mailbox at capacity owes its producer.
    pub(super) producer: &'a Arc<CellSlot>,
    pub(super) output_channel_id: Option<ChannelId>,
    pub(super) output_channel_name: Option<&'a str>,
    /// `.keyed()` opt-in for the output channel. Non-keyed channels take
    /// the pre-identity emit path byte-for-byte (no stamps, no extra work).
    pub(super) keyed: bool,
    /// `flz.cell` stamp value for keyed emits.
    pub(super) cell_label: &'a str,
    /// Durable change seq, read at stamp time (after flush_pending, so
    /// the just-applied changes are counted) for the `flz.seq` stamp.
    pub(super) change_seq: &'a AtomicU64,
    /// Emit ordinal within this invocation; second half of `flz.seq`.
    pub(super) emit_ordinal: u32,
    pub(super) instance: Option<Arc<RelayInstance>>,
    pub(super) stream: Arc<FlezhaStream>,
    /// `Some` for persistence-enabled cells with the flush-before-emit
    /// knob on. Called at the top of every emit so this run's just-applied
    /// changes are durably shipped before the output frame leaves.
    pub(super) flush_before_emit: Option<FlushBeforeEmit<'a>>,
    /// Input-log offset of the invocation's input, inherited by
    /// inter-tasklet emits so downstream Cells' effect records carry
    /// the origin offset.
    pub(super) consumed_offset: Option<u64>,
    /// The outside call the invocation's input carried (a steps pass's for
    /// every stage), inherited by inter-tasklet emits and stamped on every
    /// output.
    pub(super) reply_to: Option<Arc<str>>,
    /// `Some` for a Cell that records effects: plugin emits are pushed here
    /// (for the invocation's atomic effect record) instead of being written
    /// to the primary stream.
    pub(super) effect_capture: Option<&'a RefCell<super::invocation::Captured>>,
    /// `Some` where the Cell's record carries the offset it took: each
    /// inter-tasklet value is recorded here for that record.
    pub(super) edge_capture: Option<&'a RefCell<super::invocation::Captured>>,
}

impl<'a> CellEmitSink<'a> {
    /// An inter-tasklet emit is never a replay member: the downstream
    /// Tasklet receives the corrected outputs already in order, and a slice
    /// it could never see the end of would hold its drain forever.
    fn delivery(&self) -> Delivery<'_> {
        Delivery {
            stream: &self.stream,
            consumed_offset: self.consumed_offset,
            call: self.reply_to.as_ref(),
            replay: false,
        }
    }

    /// Drain pending changes onto the WAL path before an output frame is
    /// written. No-op for non-persistent / `NoChange` relays.
    fn flush_pending(&mut self) {
        if let Some(flush) = self.flush_before_emit.as_mut() {
            flush();
        }
    }

    /// `UnknownChannel` carrying the cached output channel name (or
    /// `<unknown>` when unset), for the resolve failure paths.
    fn unknown_channel(&self) -> TaskletError {
        TaskletError::UnknownChannel {
            channel: self
                .output_channel_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| "<unknown>".to_string()),
        }
    }

    /// Resolve the channel descriptor + RelayInstance for the cached
    /// output channel. Returns `UnknownChannel` if either has been
    /// dropped or is unset.
    fn resolve_channel(&self) -> Result<(Arc<RelayInstance>, ChannelId), TaskletError> {
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

    /// Append the output-identity stamps for a keyed-channel plugin frame.
    /// Runs after user metadata is merged so stamps can never be
    /// clobbered; no-op (and no cost) for non-keyed channels.
    fn stamp_identity(&mut self, metadata: &mut HashMap<String, String>, key: Option<String>) {
        if self.keyed {
            apply_identity_stamps(
                metadata,
                self.cell_label,
                self.change_seq.load(Ordering::Relaxed),
                &mut self.emit_ordinal,
                key,
            );
        }
    }

    /// A call end's tail: each request the value makes (the chain's transform
    /// and key applied) serialized once and written with the end's stamps, its
    /// deadline read off the wall clock now. The ctx refuses a tasklet's own
    /// `flz.` keys, so the stamps are the only ones.
    fn emit_call(
        &mut self,
        descriptor: &crate::channel::ChannelDescriptor,
        end: &crate::channel::CallEnd,
        sink: &crate::channel::PluginSink,
        value: &Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
    ) -> Result<(), TaskletError> {
        let requests = end
            .requests(value)
            .map_err(|error| TaskletError::non_retryable(error.to_string()))?;
        let now_ms = wall_now_ms();
        for (request, key) in requests {
            let bytes = serialize_contained(&sink.serializer, &descriptor.name, request.as_ref())?;
            self.write_plugin_output(descriptor, bytes, meta.clone(), key, || now_ms)?;
        }
        Ok(())
    }

    /// One plugin output with the core's end stamps: into the invocation's
    /// effect record for a Cell that records effects, else shipped at emit
    /// with, on a keyed channel, the identity stamps.
    fn write_plugin_output(
        &mut self,
        descriptor: &crate::channel::ChannelDescriptor,
        bytes: Vec<u8>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
        now_ms: impl FnOnce() -> u64,
    ) -> Result<(), TaskletError> {
        let mut meta = meta.unwrap_or_default();
        let reply_to = self.reply_to.as_ref();
        end_stamps(descriptor, reply_to, key.as_deref(), now_ms, &mut meta);
        if let Some(captured) = self.effect_capture {
            push_effect_output(captured, &descriptor.name, key, meta, bytes);
            return Ok(());
        }
        self.stamp_identity(&mut meta, key);
        let (slot, channel) = (self.producer, descriptor.name.to_string());
        ship_plugin_output(slot, &self.stream, channel, &bytes, meta, ShipAt::Emit)
            .map_err(|error| TaskletError::non_retryable(error.to_string()))
    }
}

impl<'a> EmitSink for CellEmitSink<'a> {
    /// Fan out to the consumer mailboxes, then to the plugin output if the
    /// channel has one. The plugin-bound terminal is the same path with an
    /// empty consumer list, which is why there is no second arm for it.
    fn emit(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), TaskletError> {
        self.flush_pending();
        // A steps tasklet with no output channel still releases what it
        // emits: a chained loop's leaf carries it into the next pass.
        if self.output_channel_id.is_none() {
            if let Some(instance) = self.instance.as_ref().filter(|i| i.steps.is_some()) {
                flz_core::steps::hold_emit(instance, None, &value, &self.stream, self.producer);
                return Ok(());
            }
        }
        let (instance, cid) = self.resolve_channel()?;
        let descriptor =
            instance
                .topology
                .channel(cid)
                .ok_or_else(|| TaskletError::UnknownChannel {
                    channel: format!("<channel id {}>", cid),
                })?;
        let d = self.delivery();
        route_to_consumers(&instance, descriptor, &value, &d, Some(self.producer));
        if let Some(captured) = self
            .edge_capture
            .filter(|_| !descriptor.consumers.is_empty())
        {
            captured
                .borrow_mut()
                .edges
                .extend(flz_core::routing::edge_records(
                    &instance,
                    descriptor,
                    cid,
                    self.consumed_offset,
                    &value,
                ));
        }
        match (descriptor.call.as_ref(), descriptor.plugin_sink.as_ref()) {
            (Some(end), Some(sink)) => self.emit_call(descriptor, end, sink, &value, meta),
            (None, Some(sink)) => {
                let bytes =
                    serialize_contained(&sink.serializer, &descriptor.name, value.as_ref())?;
                self.write_plugin_output(descriptor, bytes, meta, key, wall_now_ms)
            }
            _ => Ok(()),
        }
    }

    fn channel_name(&self) -> &str {
        self.output_channel_name.unwrap_or("<unset>")
    }

    fn has_plugin_sink(&self) -> bool {
        // Resolving the descriptor each time keeps the trait surface small.
        // `route` only asks when the emit carries metadata, which is rare, so
        // the ordinary emit pays nothing for this lookup.
        self.instance
            .as_ref()
            .and_then(|inst| {
                self.output_channel_id
                    .and_then(|cid| inst.topology.channel(cid))
            })
            .map(|d| d.plugin_sink.is_some())
            .unwrap_or(false)
    }

    fn is_keyed(&self) -> bool {
        self.keyed
    }

    fn complete(&mut self, result: Vec<u8>) -> Result<(), TaskletError> {
        match &self.instance {
            Some(instance) if instance.steps.is_some() => {
                flz_core::steps::complete(self.producer, result)
            }
            _ => flz_core::relay::completion::complete(self.producer, result),
        }
        Ok(())
    }
}

/// Serialize `value` for the plugin end of `channel` inside `catch_unwind`:
/// a serializer panic surfaces as the producing tasklet's
/// `PluginSerializerPanic`, and the dispatch thread is unaffected.
pub(crate) fn serialize_contained(
    serializer: &crate::channel::PluginSerializeFn,
    channel: &str,
    value: &dyn Any,
) -> Result<Vec<u8>, TaskletError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| serializer(value))).map_err(|panic| {
        TaskletError::PluginSerializerPanic {
            channel: channel.to_string(),
            message: crate::tasklet::error::format_panic(panic),
        }
    })
}

/// Output-identity stamps for one keyed-channel plugin frame: the emitting
/// Cell (`flz.cell`), the replay-stable output sequence
/// `<change_seq>.<emit_ordinal>` (`flz.seq`), and the caller's natural key
/// (`flz.key`). Applied after user metadata so stamps win; advances the
/// per-invocation emit ordinal. Shared with the test harness so
/// `replay_emits_equal` compares the same stamp bytes the wire carries.
pub(crate) fn apply_identity_stamps(
    metadata: &mut HashMap<String, String>,
    cell_label: &str,
    change_seq: u64,
    emit_ordinal: &mut u32,
    key: Option<String>,
) {
    metadata.insert("flz.cell".to_string(), cell_label.to_string());
    metadata.insert(
        "flz.seq".to_string(),
        format!("{}.{}", change_seq, emit_ordinal),
    );
    *emit_ordinal += 1;
    if let Some(k) = key {
        metadata.insert("flz.key".to_string(), k);
    }
}

/// Tier-mode capture: push one plugin output into the invocation's atomic
/// effect record instead of any wire path. Key-sorted user metadata keeps
/// the record bytes replay-deterministic (HashMap iteration order is not);
/// the reserved `channel` key is dropped as the shipped frame's is
/// overwritten, and identity stamps are derived controller-side from the
/// durable record.
fn push_effect_output(
    captured: &RefCell<super::invocation::Captured>,
    channel: &str,
    key: Option<String>,
    meta: HashMap<String, String>,
    payload: Vec<u8>,
) {
    let mut metadata: Vec<(String, String)> =
        meta.into_iter().filter(|(k, _)| k != "channel").collect();
    metadata.sort();
    captured
        .borrow_mut()
        .outputs
        .push(crate::cell::flusher::OutputRecord {
            channel: channel.to_string(),
            key: key.unwrap_or_default(),
            metadata,
            payload,
        });
}

const ENV_FLUSH_BEFORE_EMIT: &str = "FLEZHA_FLUSH_BEFORE_EMIT";

/// Whether flush-before-emit is enabled for persistence-enabled cells.
/// Defaults to ON (durability over latency); set `FLEZHA_FLUSH_BEFORE_EMIT`
/// to `0`/`false`/`off`/`no` to revert to the inline-emit fast path.
pub(super) fn flush_before_emit_enabled() -> bool {
    flz_core::util::env_bool(ENV_FLUSH_BEFORE_EMIT, true)
}
