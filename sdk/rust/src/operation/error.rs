use std::any::Any;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum OperationError {
    #[error("metadata is only valid on plugin-bound channels; channel `{channel}` is inter-operation")]
    MetadataNotAllowedOnInterOperationChannel { channel: String },

    #[error("channel `{channel}` is not declared on this operative")]
    UnknownChannel { channel: String },

    #[error("plugin serializer for channel `{channel}` panicked: {message}")]
    PluginSerializerPanic { channel: String, message: String },

    #[error("operation error: {0}")]
    Custom(String),
}

/// Render a `catch_unwind` panic payload as a human-readable string.
/// Mirrors the formatting used elsewhere in the runtime so error
/// messages stay consistent across Operation panics, dispatcher panics, and
/// hybrid serializer panics.
pub fn format_panic(payload: Box<dyn Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}
