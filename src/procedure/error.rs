use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProcedureError {
    #[error(
        "metadata is only valid on plugin-bound channels; channel `{channel}` is inter-procedure"
    )]
    MetadataNotAllowedOnInterProcedureChannel { channel: String },

    #[error("channel `{channel}` is not declared on this operative")]
    UnknownChannel { channel: String },

    #[error("plugin serializer for channel `{channel}` panicked: {message}")]
    PluginSerializerPanic { channel: String, message: String },

    #[error(
        "channel `{channel}` is declared .keyed() and this procedure is single-entity, so \
         there is no Entity key to emit under; use ctx.emit_keyed(key, value)"
    )]
    KeyRequiredOnKeyedChannel { channel: String },

    #[error(
        "channel `{channel}` is not declared .keyed(); emit_keyed is only valid on keyed channels"
    )]
    KeyNotAllowedOnNonKeyedChannel { channel: String },

    #[error(
        "metadata key `{key}` uses the reserved `flz.` namespace; runtime stamps cannot be \
             set by operative code"
    )]
    ReservedMetadataKey { key: String },

    #[error(
        "procedure `{procedure}` declares clock `{clock}`, which does not derive time from the \
         input; an event-time timer needs `type Clock = EventTimeFrom<_>`"
    )]
    EventTimeTimerWithoutEventTimeClock { procedure: String, clock: String },

    #[error(
        "procedure `{procedure}` already holds its maximum of {max} pending timers for this \
         Entity; cancel one or raise `max_timers_per_entity()`"
    )]
    TimerCapExceeded { procedure: String, max: usize },

    #[error(
        "timers are not available in this context; only a stream operative's `run` / \
         `on_timer` may register them"
    )]
    TimersUnavailable,

    #[error("procedure error: {0}")]
    Custom(String),

    #[error("non-retryable procedure error: {0}")]
    NonRetryable(String),
}

impl ProcedureError {
    /// Classify a domain failure as non-retryable: the dispatcher parks the
    /// payload to the dead-letter queue immediately, skipping the channel's
    /// remaining retry budget. Use for failures that can never succeed on
    /// re-run (malformed payload, permanent semantic violation).
    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self::NonRetryable(message.into())
    }

    /// Whether the retry policy must skip further attempts for this error.
    pub fn is_non_retryable(&self) -> bool {
        matches!(self, Self::NonRetryable(_))
    }
}
pub use flz_core::util::format_panic;

impl From<flz_core::timer::TimerRefusal> for ProcedureError {
    fn from(refusal: flz_core::timer::TimerRefusal) -> Self {
        use flz_core::timer::TimerRefusal;
        match refusal {
            TimerRefusal::EventTimeWithoutEventTimeClock { procedure, clock } => {
                ProcedureError::EventTimeTimerWithoutEventTimeClock { procedure, clock }
            }
            TimerRefusal::CapExceeded { procedure, max } => {
                ProcedureError::TimerCapExceeded { procedure, max }
            }
        }
    }
}
