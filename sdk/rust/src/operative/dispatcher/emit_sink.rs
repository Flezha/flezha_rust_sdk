//! The two sinks an invocation's outputs leave through.
//!
//! [`EntityEmitSink`] serves a stream procedure, on the connection its item
//! rode in on; [`WorkflowCollectAdapter`] serves a structured workflow proc,
//! collecting into the driver instead. Both answer the one `EmitSink` trait,
//! and both route a keyed plugin emit into the invocation's atomic effect
//! record when the effectively-once tier is on.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use flz_core::routing::send_plugin_frame;
use flz_transport::transport::FlezhaStream;

use crate::channel::{ChannelId, ChannelKind};
use crate::entity::EntitySlot;
use crate::operative::instance::StreamInstance;
use crate::operative::topology::OperativeTopology;
use crate::procedure::ctx::EmitSink;
use crate::procedure::ProcedureError;
use crate::workflow::WorkflowCollectSink;

/// Synchronous drain of the producing entity's pending changes onto
/// the durable WAL path, invoked before each output frame is written.
pub(super) type FlushBeforeEmit<'a> = Box<dyn FnMut() + 'a>;

/// Per-invocation emit sink, constructed fresh inside each `dispatch`
/// call. Holds an upgraded `Arc<StreamInstance>` for the duration of the
/// run; if the instance has been dropped, every emit returns
/// `UnknownChannel`.
pub(super) struct EntityEmitSink<'a> {
    /// The Entity this invocation belongs to. An emit it cannot deliver is
    /// held against this slot and this slot is handed no further work — the
    /// backpressure a mailbox at capacity owes its producer.
    pub(super) producer: &'a Arc<EntitySlot>,
    pub(super) output_channel_id: Option<ChannelId>,
    pub(super) output_channel_name: Option<&'a str>,
    /// `.keyed()` opt-in for the output channel. Non-keyed channels take
    /// the pre-identity emit path byte-for-byte (no stamps, no extra work).
    pub(super) keyed: bool,
    /// `flz.entity` stamp value for keyed emits.
    pub(super) entity_label: &'a str,
    /// Durable change seq, read at stamp time (after flush_pending, so
    /// the just-applied changes are counted) for the `flz.seq` stamp.
    pub(super) change_seq: &'a AtomicU64,
    /// Emit ordinal within this invocation; second half of `flz.seq`.
    pub(super) emit_ordinal: u32,
    pub(super) instance: Option<Arc<StreamInstance>>,
    pub(super) stream: Arc<FlezhaStream>,
    /// `Some` for persistence-enabled entities with the flush-before-emit
    /// knob on. Called at the top of every emit so this run's just-applied
    /// changes are durably shipped before the output frame leaves.
    pub(super) flush_before_emit: Option<FlushBeforeEmit<'a>>,
    /// Input-log offset of the invocation's input, inherited by
    /// inter-procedure emits so downstream Entities' effect records carry
    /// the origin offset.
    pub(super) consumed_offset: Option<u64>,
    /// `Some` in effectively-once tier mode: plugin emits are pushed here
    /// (for the invocation's atomic effect record) instead of being written
    /// to the primary stream.
    pub(super) effect_outputs: Option<&'a RefCell<Vec<crate::entity::flusher::OutputRecord>>>,
}

impl<'a> EntityEmitSink<'a> {
    /// Drain pending changes onto the WAL path before an output frame is
    /// written. No-op for non-persistent / `NoChange` operatives.
    fn flush_pending(&mut self) {
        if let Some(flush) = self.flush_before_emit.as_mut() {
            flush();
        }
    }

    /// `UnknownChannel` carrying the cached output channel name (or
    /// `<unknown>` when unset), for the resolve failure paths.
    fn unknown_channel(&self) -> ProcedureError {
        ProcedureError::UnknownChannel {
            channel: self
                .output_channel_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| "<unknown>".to_string()),
        }
    }

    /// Resolve the channel descriptor + StreamInstance for the cached
    /// output channel. Returns `UnknownChannel` if either has been
    /// dropped or is unset.
    fn resolve_channel(&self) -> Result<(Arc<StreamInstance>, ChannelId), ProcedureError> {
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
                self.entity_label,
                self.change_seq.load(Ordering::Relaxed),
                &mut self.emit_ordinal,
                key,
            );
        }
    }

    /// Plugin-sink tail: serialize once inside `catch_unwind` and forward the
    /// bytes through the plugin-wire framing path. A serializer panic surfaces
    /// as `ProcedureError::PluginSerializerPanic` to the producing Procedure;
    /// the dispatcher thread is unaffected.
    fn emit_plugin_sink(
        &mut self,
        instance: &StreamInstance,
        descriptor: &crate::channel::ChannelDescriptor,
        sink: &crate::channel::PluginSink,
        value: &Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), ProcedureError> {
        let serializer = Arc::clone(&sink.serializer);
        let value_for_panic = Arc::clone(value);
        let bytes_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (serializer)(value_for_panic.as_ref())
        }));
        let bytes = match bytes_result {
            Ok(b) => b,
            Err(panic) => {
                return Err(ProcedureError::PluginSerializerPanic {
                    channel: descriptor.name.to_string(),
                    message: crate::procedure::error::format_panic(panic),
                });
            }
        };

        if let Some(cell) = self.effect_outputs {
            push_effect_output(cell, &descriptor.name, key, meta, bytes);
            return Ok(());
        }

        let mut metadata = channel_metadata(meta, &descriptor.name);
        self.stamp_identity(&mut metadata, key);

        send_plugin_frame(&self.stream, &instance.instance_id, &bytes, &metadata);
        Ok(())
    }
}

impl<'a> EmitSink for EntityEmitSink<'a> {
    /// Fan out to the consumer mailboxes, then to the plugin sink if the
    /// channel has one. The plugin-bound terminal is the same path with an
    /// empty consumer list, which is why there is no second arm for it.
    fn emit(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), ProcedureError> {
        self.flush_pending();
        let (instance, cid) = self.resolve_channel()?;
        let descriptor =
            instance
                .topology
                .channel(cid)
                .ok_or_else(|| ProcedureError::UnknownChannel {
                    channel: format!("<channel id {}>", cid),
                })?;
        // An inter-procedure emit is never a replay member: the downstream
        // Procedure receives the corrected outputs already in order, and a
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
}

/// Emit sink for the workflow sequential driver. Inter-procedure output is
/// **collected** for handoff to the next stage at the barrier; plugin-bound
/// output is serialized and routed to the wire via the `WorkflowCollectSink`.
/// Constructed fresh per `run_collecting` call, so it never threads through
/// the stream push-dispatch mailbox path.
pub(super) struct WorkflowCollectAdapter<'a> {
    pub(super) sink: &'a mut dyn WorkflowCollectSink,
    pub(super) output_channel_id: Option<ChannelId>,
    pub(super) output_channel_kind: ChannelKind,
    pub(super) output_channel_name: Option<&'a str>,
    pub(super) topology: &'a OperativeTopology,
    /// `Some` under the effectively-once tier: plugin emits (keyed
    /// included) are captured into the invocation's atomic effect record
    /// instead of the metadata-less workflow plugin wire.
    pub(super) effect_outputs: Option<&'a RefCell<Vec<crate::entity::flusher::OutputRecord>>>,
}

impl<'a> WorkflowCollectAdapter<'a> {
    fn unknown_channel(&self) -> ProcedureError {
        ProcedureError::UnknownChannel {
            channel: self
                .output_channel_name
                .map(|s| s.to_string())
                .unwrap_or_else(|| "<unknown>".to_string()),
        }
    }

    /// Serialize a value with the producer-edge channel's plugin sink and
    /// route the bytes: into the invocation's effect record under the tier,
    /// else to the workflow plugin wire.
    fn route_plugin(
        &mut self,
        value: &Arc<dyn Any + Send + Sync>,
        key: Option<String>,
        meta: Option<HashMap<String, String>>,
    ) -> Result<(), ProcedureError> {
        let cid = self
            .output_channel_id
            .ok_or_else(|| self.unknown_channel())?;
        let descriptor =
            self.topology
                .channel(cid)
                .ok_or_else(|| ProcedureError::UnknownChannel {
                    channel: format!("<channel id {}>", cid),
                })?;
        let serializer = descriptor
            .plugin_sink
            .as_ref()
            .map(|s| &s.serializer)
            .ok_or_else(|| {
                ProcedureError::Custom(format!(
                    "workflow plugin channel `{}` is missing a serializer",
                    descriptor.name
                ))
            })?;
        let bytes = serializer(&**value);
        if let Some(cell) = self.effect_outputs {
            push_effect_output(cell, &descriptor.name, key, meta, bytes);
        } else {
            self.sink.emit_plugin_bytes(&descriptor.name, bytes);
        }
        Ok(())
    }
}

impl<'a> WorkflowCollectAdapter<'a> {
    /// The workflow plugin wire (`emit_plugin_bytes`) carries no metadata,
    /// so `.keyed()` identity stamps cannot ride it. Refuse rather than
    /// silently dropping the identity contract — except under the
    /// effectively-once tier, where emits ride the effect record (which
    /// carries the key durably).
    fn is_plugin_bound(&self) -> bool {
        matches!(self.output_channel_kind, ChannelKind::PluginBound)
    }

    fn refuse_key(&self, key: &Option<String>) -> Result<(), ProcedureError> {
        if self.effect_outputs.is_none() && key.is_some() {
            return Err(ProcedureError::Custom(format!(
                "keyed output identity is not supported on workflow operatives \
                 (channel `{}`): the workflow plugin wire carries no metadata",
                self.channel_name()
            )));
        }
        Ok(())
    }
}

impl<'a> EmitSink for WorkflowCollectAdapter<'a> {
    fn emit(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
        key: Option<String>,
    ) -> Result<(), ProcedureError> {
        self.refuse_key(&key)?;
        if self.has_plugin_sink() {
            self.route_plugin(&value, key, meta)?;
        }
        // The one place a workflow channel's shape still matters: a
        // plugin-bound channel has no consumer to hand a stage batch to, so
        // collecting there would seed the next stage with a terminal output.
        if !self.is_plugin_bound() {
            self.sink.collect(value);
        }
        Ok(())
    }

    fn channel_name(&self) -> &str {
        self.output_channel_name.unwrap_or("<workflow>")
    }

    fn has_plugin_sink(&self) -> bool {
        self.output_channel_id
            .and_then(|cid| self.topology.channel(cid))
            .map(|d| d.plugin_sink.is_some())
            .unwrap_or(false)
    }

    fn is_keyed(&self) -> bool {
        self.output_channel_id
            .and_then(|cid| self.topology.channel(cid))
            .map(|d| d.keyed)
            .unwrap_or(false)
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
/// Entity (`flz.entity`), the replay-stable output sequence
/// `<change_seq>.<emit_ordinal>` (`flz.seq`), and the caller's natural key
/// (`flz.key`). Applied after user metadata so stamps win; advances the
/// per-invocation emit ordinal. Shared with the test harness so
/// `replay_emits_equal` compares the same stamp bytes the wire carries.
pub(crate) fn apply_identity_stamps(
    metadata: &mut HashMap<String, String>,
    entity_label: &str,
    change_seq: u64,
    emit_ordinal: &mut u32,
    key: Option<String>,
) {
    metadata.insert("flz.entity".to_string(), entity_label.to_string());
    metadata.insert(
        "flz.seq".to_string(),
        format!("{}.{}", change_seq, emit_ordinal),
    );
    *emit_ordinal += 1;
    if let Some(k) = key {
        metadata.insert("flz.key".to_string(), k);
    }
}

/// Tier-mode capture (shared by the stream emit sink and the workflow
/// collect adapter): push one plugin output into the invocation's atomic
/// effect record instead of any wire path. Key-sorted user metadata keeps
/// the record bytes replay-deterministic (HashMap iteration order is not);
/// the reserved `channel` key is dropped like `channel_metadata` does, and
/// identity stamps are derived controller-side from the durable record.
fn push_effect_output(
    cell: &RefCell<Vec<crate::entity::flusher::OutputRecord>>,
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
    cell.borrow_mut()
        .push(crate::entity::flusher::OutputRecord {
            channel: channel.to_string(),
            key: key.unwrap_or_default(),
            metadata,
            payload,
        });
}

const ENV_FLUSH_BEFORE_EMIT: &str = "FLEZHA_FLUSH_BEFORE_EMIT";

/// Whether flush-before-emit is enabled for persistence-enabled entities.
/// Defaults to ON (durability over latency); set `FLEZHA_FLUSH_BEFORE_EMIT`
/// to `0`/`false`/`off`/`no` to revert to the inline-emit fast path.
pub(super) fn flush_before_emit_enabled() -> bool {
    flz_core::util::env_bool(ENV_FLUSH_BEFORE_EMIT, true)
}
