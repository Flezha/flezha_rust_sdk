use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use crate::operation::error::OperationError;

/// Per-invocation context for a Operation's `run` method. Borrows the runner's
/// per-segment routing tables. Not Send/Sync — lives only across one
/// `Operation::run` call.
///
/// The dispatcher stamps `output_channel_id` (cached at operative-build time)
/// so emit incurs zero channel-name lookups on the hot path.
pub struct OperationCtx<'a> {
    pub(crate) emitter: &'a mut dyn EmitSink,
}

/// Internal trait the runtime implements per-segment. Lets OperationCtx stay
/// non-generic while the underlying dispatcher carries the typed wiring.
pub(crate) trait EmitSink {
    fn emit_inter_operation(&mut self, value: Arc<dyn Any + Send + Sync>) -> Result<(), OperationError>;

    fn emit_plugin(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        meta: Option<HashMap<String, String>>,
    ) -> Result<(), OperationError>;

    /// Hybrid emit: fan out to inter-operation consumers and (if a plugin
    /// sink is configured) serialize the value once and forward bytes
    /// to the plugin wire with the supplied metadata. Default
    /// implementation routes through `emit_inter_operation` only — sinks
    /// without a plugin attachment can ignore `meta`.
    fn emit_inter_operation_with_meta(
        &mut self,
        value: Arc<dyn Any + Send + Sync>,
        _meta: Option<HashMap<String, String>>,
    ) -> Result<(), OperationError> {
        self.emit_inter_operation(value)
    }

    fn channel_name(&self) -> &str;

    fn is_plugin_bound(&self) -> bool;

    /// True iff the channel is `InterOperation` with a `plugin_sink`
    /// attached via `also_to_plugin_with(...)`. Hybrid emits route
    /// through `emit_inter_operation{,_with_meta}` rather than `emit_plugin`.
    fn has_plugin_sink(&self) -> bool {
        false
    }
}

impl<'a> OperationCtx<'a> {
    pub(crate) fn new(emitter: &'a mut dyn EmitSink) -> Self {
        Self { emitter }
    }

    /// Emit a typed value on the producer-edge channel for this Operation.
    /// `C` must match `Operation::Output`; the channel id was cached at
    /// operative-build time from the `c.channel::<T>("name").from::<S>()`
    /// wiring, so this call performs no name lookups.
    pub fn emit<C>(&mut self, value: C) -> Result<(), OperationError>
    where
        C: 'static + Send + Sync,
    {
        let arc: Arc<dyn Any + Send + Sync> = Arc::new(value);
        if self.emitter.is_plugin_bound() {
            self.emitter.emit_plugin(arc, None)
        } else {
            self.emitter.emit_inter_operation(arc)
        }
    }

    /// Emit with user-attached string-keyed metadata. Valid on
    /// plugin-bound channels and on hybrid (`InterOperation` +
    /// `also_to_plugin_with`) channels. Pure inter-operation channels with
    /// no plugin sink return `MetadataNotAllowedOnInterOperationChannel`.
    pub fn emit_with_meta<C>(
        &mut self,
        value: C,
        meta: HashMap<String, String>,
    ) -> Result<(), OperationError>
    where
        C: 'static + Send + Sync,
    {
        let plugin_bound = self.emitter.is_plugin_bound();
        let has_plugin_sink = self.emitter.has_plugin_sink();
        if !plugin_bound && !has_plugin_sink {
            return Err(OperationError::MetadataNotAllowedOnInterOperationChannel {
                channel: self.emitter.channel_name().to_string(),
            });
        }
        let arc: Arc<dyn Any + Send + Sync> = Arc::new(value);
        if plugin_bound {
            self.emitter.emit_plugin(arc, Some(meta))
        } else {
            self.emitter.emit_inter_operation_with_meta(arc, Some(meta))
        }
    }
}
