use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::shard::error::ShardError;

/// Per-invocation context for a Shard's `run` method. Borrows the runner's
/// per-partition routing tables. Not Send/Sync — lives only across one
/// `Shard::run` call.
///
/// The dispatcher stamps `output_channel_id` (cached at relay-build time)
/// so emit incurs zero channel-name lookups on the hot path.
pub struct ShardCtx<'a> {
    pub(crate) emitter: &'a mut dyn EmitSink,
}

/// Internal trait the runtime implements per-partition. Lets ShardCtx stay
/// non-generic while the underlying dispatcher carries the typed wiring.
pub(crate) trait EmitSink {
    fn emit_inter_shard(&mut self, value: Arc<dyn Any + Send + Sync>) -> Result<(), ShardError>;

    fn emit_plugin(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
    ) -> Result<(), ShardError>;

    /// Hybrid emit: fan out to inter-shard consumers and (if a plugin
    /// sink is configured) serialize the value once and forward bytes
    /// to the plugin wire with the supplied metadata. Default
    /// implementation routes through `emit_inter_shard` only — sinks
    /// without a plugin attachment can ignore `meta`.
    fn emit_inter_shard_with_meta(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        _meta: Option<HashMap<String, String>>,
    ) -> Result<(), ShardError> {
        self.emit_inter_shard(value)
    }

    fn channel_name(&self) -> &str;

    fn is_plugin_bound(&self) -> bool;

    /// True iff the channel is `InterShard` with a `plugin_sink`
    /// attached via `also_to_plugin_with(...)`. Hybrid emits route
    /// through `emit_inter_shard{,_with_meta}` rather than `emit_plugin`.
    fn has_plugin_sink(&self) -> bool {
        false
    }
}

impl<'a> ShardCtx<'a> {
    pub(crate) fn new(emitter: &'a mut dyn EmitSink) -> Self {
        Self { emitter }
    }

    /// Emit a typed value on the producer-edge channel for this Shard.
    /// `C` must match `Shard::Output`; the channel id was cached at
    /// relay-build time from the `c.channel::<T>("name").from::<S>()`
    /// wiring, so this call performs no name lookups.
    pub fn emit<C>(&mut self, value: C) -> Result<(), ShardError>
    where
        C: 'static + Send + Sync,
    {
        let arc: Arc<dyn Any + Send + Sync> = Arc::new(value);
        if self.emitter.is_plugin_bound() {
            self.emitter.emit_plugin(arc, None)
        } else {
            self.emitter.emit_inter_shard(arc)
        }
    }

    /// Emit with user-attached string-keyed metadata. Valid on
    /// plugin-bound channels and on hybrid (`InterShard` +
    /// `also_to_plugin_with`) channels. Pure inter-shard channels with
    /// no plugin sink return `MetadataNotAllowedOnInterShardChannel`.
    pub fn emit_with_meta<C>(
        &mut self,
        value: C,
        meta: HashMap<String, String>,
    ) -> Result<(), ShardError>
    where
        C: 'static + Send + Sync,
    {
        let plugin_bound = self.emitter.is_plugin_bound();
        let has_plugin_sink = self.emitter.has_plugin_sink();
        if !plugin_bound && !has_plugin_sink {
            return Err(ShardError::MetadataNotAllowedOnInterShardChannel {
                channel: self.emitter.channel_name().to_string(),
            });
        }
        let arc: Arc<dyn Any + Send + Sync> = Arc::new(value);
        if plugin_bound {
            self.emitter.emit_plugin(arc, Some(meta))
        } else {
            self.emitter.emit_inter_shard_with_meta(arc, Some(meta))
        }
    }
}
