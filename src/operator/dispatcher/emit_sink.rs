//! The sink an invocation's outputs leave through, [`CellEmitSink`], on the
//! connection its item rode in on. An emit is routed to its consumers (a
//! bounded operator's held on its pass for the stage's commit) and its plugin output
//! written, or, for a Cell that records effects, captured into the
//! invocation's atomic effect record.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use flz_core::routing::send_plugin_frame;
use flz_transport::transport::FlezhaStream;

use crate::cell::CellSlot;
use crate::channel::ChannelId;
use crate::operator::instance::OperatorInstance;
use crate::tasklet::ctx::EmitSink;
use crate::tasklet::TaskletError;

/// Synchronous drain of the producing cell's pending changes onto
/// the durable WAL path, invoked before each output frame is written.
pub(super) type FlushBeforeEmit<'a> = Box<dyn FnMut() + 'a>;

/// Per-invocation emit sink, constructed fresh inside each `dispatch`
/// call. Holds an upgraded `Arc<OperatorInstance>` for the duration of the
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
    pub(super) instance: Option<Arc<OperatorInstance>>,
    pub(super) stream: Arc<FlezhaStream>,
    /// `Some` for persistence-enabled cells with the flush-before-emit
    /// knob on. Called at the top of every emit so this run's just-applied
    /// changes are durably shipped before the output frame leaves.
    pub(super) flush_before_emit: Option<FlushBeforeEmit<'a>>,
    /// Input-log offset of the invocation's input, inherited by
    /// inter-tasklet emits so downstream Cells' effect records carry
    /// the origin offset.
    pub(super) consumed_offset: Option<u64>,
    /// `Some` for a Cell that records effects: plugin emits are pushed here
    /// (for the invocation's atomic effect record) instead of being written
    /// to the primary stream.
    pub(super) effect_capture: Option<&'a RefCell<super::invocation::Captured>>,
    /// `Some` where the Cell's record carries the offset it took: each
    /// inter-tasklet value is recorded here for that record.
    pub(super) edge_capture: Option<&'a RefCell<super::invocation::Captured>>,
}

impl<'a> CellEmitSink<'a> {
    /// Drain pending changes onto the WAL path before an output frame is
    /// written. No-op for non-persistent / `NoChange` operators.
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

    /// Resolve the channel descriptor + OperatorInstance for the cached
    /// output channel. Returns `UnknownChannel` if either has been
    /// dropped or is unset.
    fn resolve_channel(&self) -> Result<(Arc<OperatorInstance>, ChannelId), TaskletError> {
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

    /// Plugin-sink tail: serialize once inside `catch_unwind` and forward the
    /// bytes through the plugin-wire framing path. A serializer panic surfaces
    /// as `TaskletError::PluginSerializerPanic` to the producing Tasklet;
    /// the dispatcher thread is unaffected.
    fn emit_plugin_sink(
        &mut self,
        instance: &OperatorInstance,
        descriptor: &crate::channel::ChannelDescriptor,
        sink: &crate::channel::PluginSink,
        value: &Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), TaskletError> {
        let serializer = Arc::clone(&sink.serializer);
        let value_for_panic = Arc::clone(value);
        let bytes_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (serializer)(value_for_panic.as_ref())
        }));
        let bytes = match bytes_result {
            Ok(b) => b,
            Err(panic) => {
                return Err(TaskletError::PluginSerializerPanic {
                    channel: descriptor.name.to_string(),
                    message: crate::tasklet::error::format_panic(panic),
                });
            }
        };

        if let Some(captured) = self.effect_capture {
            push_effect_output(captured, &descriptor.name, key, meta, bytes);
            return Ok(());
        }

        let mut metadata = channel_metadata(meta, &descriptor.name);
        self.stamp_identity(&mut metadata, key);

        send_plugin_frame(&self.stream, &instance.instance_id, &bytes, &metadata)
            .map_err(|error| TaskletError::non_retryable(error.to_string()))
    }
}

impl<'a> EmitSink for CellEmitSink<'a> {
    /// Fan out to the consumer mailboxes, then to the plugin sink if the
    /// channel has one. The plugin-bound terminal is the same path with an
    /// empty consumer list, which is why there is no second arm for it.
    fn emit(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), TaskletError> {
        self.flush_pending();
        // A bounded tasklet with no output channel still releases what it
        // emits: a chained loop's leaf carries it into the next pass.
        if self.output_channel_id.is_none() {
            if let Some(instance) = self.instance.as_ref().filter(|i| i.bounded.is_some()) {
                flz_core::bounded::hold_emit(instance, None, &value, &self.stream, self.producer);
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
        // An inter-tasklet emit is never a replay member: the downstream
        // Tasklet receives the corrected outputs already in order, and a
        // slice it could never see the end of would hold its drain forever.
        flz_core::routing::route_to_consumers(
            &instance,
            descriptor,
            &value,
            &self.stream,
            self.consumed_offset,
            false,
            Some(self.producer),
        );
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
        if let Some(sink) = descriptor.plugin_sink.as_ref() {
            self.emit_plugin_sink(&instance, descriptor, sink, &value, meta, key)?;
        }
        Ok(())
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
        if self.instance.as_ref().is_none_or(|i| i.bounded.is_none()) {
            return Err(TaskletError::non_retryable(
                flz_core::bounded::COMPLETE_OUTSIDE_BOUNDED,
            ));
        }
        flz_core::bounded::complete(self.producer, result);
        Ok(())
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
/// the reserved `channel` key is dropped like `channel_metadata` does, and
/// identity stamps are derived controller-side from the durable record.
fn push_effect_output(
    captured: &RefCell<super::invocation::Captured>,
    channel: &str,
    key: Option<String>,
    meta: Option<HashMap<String, String>>,
    payload: Vec<u8>,
) {
    let mut metadata: Vec<(String, String)> = meta
        .unwrap_or_default()
        .into_iter()
        .filter(|(k, _)| k != "channel")
        .collect();
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
