use thiserror::Error;

#[derive(Debug, Error)]
pub enum ShardError {
    #[error("metadata is only valid on plugin-bound channels; channel `{channel}` is inter-shard")]
    MetadataNotAllowedOnInterShardChannel { channel: String },

    #[error("channel `{channel}` is not declared on this relay")]
    UnknownChannel { channel: String },

    #[error("shard error: {0}")]
    Custom(String),
}
