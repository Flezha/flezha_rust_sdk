/// A tasklet's failure: the author's own (`Custom`, `NonRetryable`) or a
/// refusal of what the tasklet asked the runtime for. `Display` is the
/// catalogue message, and [`ErrorModel`](crate::ErrorModel) gives its code.
#[derive(Debug)]
pub enum TaskletError {
    MetadataNotAllowedOnInterTaskletChannel {
        channel: String,
    },
    UnknownChannel {
        channel: String,
    },
    PluginSerializerPanic {
        channel: String,
        message: String,
    },
    KeyRequiredOnKeyedChannel {
        channel: String,
    },
    KeyNotAllowedOnNonKeyedChannel {
        channel: String,
    },
    ReservedMetadataKey {
        key: String,
    },
    EventTimeTimerWithoutEventTimeClock {
        tasklet: String,
        clock: String,
    },
    TimerCapExceeded {
        tasklet: String,
        max: usize,
    },
    /// Only a tasklet's `run` / `on_timer` may register timers.
    TimersUnavailable,
    /// A part the runtime refused, carrying its catalogue error
    /// (`part.no_output`, `part.outside_invocation`).
    Part(flz_core::FlezhaError),
    /// A `ctx.effect` the runtime refused or could not keep (`effect.*`).
    Effect(flz_core::FlezhaError),
    Custom(String),
    NonRetryable(String),
}

impl crate::ErrorModel for TaskletError {
    fn to_error(&self) -> flz_core::FlezhaError {
        use flz_core::FlezhaError as E;
        let channel =
            |code, channel: &String| E::with(code, [("channel", channel.as_str().into())]);
        match self {
            Self::MetadataNotAllowedOnInterTaskletChannel { channel: c } => {
                channel("runtime.metadata_on_inter_tasklet_channel", c)
            }
            Self::UnknownChannel { channel: c } => channel("runtime.unknown_channel", c),
            Self::PluginSerializerPanic { channel, message } => E::with(
                "runtime.plugin_serializer_panicked",
                [
                    ("channel", channel.as_str().into()),
                    ("reason", message.as_str().into()),
                ],
            ),
            Self::KeyRequiredOnKeyedChannel { channel: c } => {
                channel("runtime.key_required_on_keyed_channel", c)
            }
            Self::KeyNotAllowedOnNonKeyedChannel { channel: c } => {
                channel("runtime.key_not_allowed_on_non_keyed_channel", c)
            }
            Self::ReservedMetadataKey { key } => E::with(
                "runtime.reserved_metadata_key",
                [("key", key.as_str().into())],
            ),
            Self::EventTimeTimerWithoutEventTimeClock { tasklet, clock } => {
                flz_core::timer::TimerRefusal::EventTimeWithoutEventTimeClock {
                    tasklet: tasklet.clone(),
                    clock: clock.clone(),
                }
                .to_error()
            }
            Self::TimerCapExceeded { tasklet, max } => flz_core::timer::TimerRefusal::CapExceeded {
                tasklet: tasklet.clone(),
                max: *max,
            }
            .to_error(),
            Self::TimersUnavailable => E::with("runtime.timers_unavailable", []),
            Self::Part(error) | Self::Effect(error) => error.clone(),
            Self::Custom(error) | Self::NonRetryable(error) => {
                E::with("runtime.tasklet_failed", [("error", error.as_str().into())])
            }
        }
    }
}

flz_transport::model_error!(TaskletError);

impl TaskletError {
    /// Classify a domain failure as non-retryable: the dispatcher parks the
    /// payload to the dead-letter queue immediately, skipping the channel's
    /// remaining retry budget. Use for failures that can never succeed on
    /// re-run (malformed payload, permanent semantic violation).
    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self::NonRetryable(message.into())
    }

    /// Whether the retry policy must skip further attempts for this error.
    pub fn is_non_retryable(&self) -> bool {
        match self {
            Self::NonRetryable(_) => true,
            // Only an unkept result is worth another attempt.
            Self::Effect(error) => error.code != "effect.not_recorded",
            _ => false,
        }
    }
}
pub use flz_core::util::format_panic;

impl From<flz_core::timer::TimerRefusal> for TaskletError {
    fn from(refusal: flz_core::timer::TimerRefusal) -> Self {
        use flz_core::timer::TimerRefusal;
        match refusal {
            TimerRefusal::EventTimeWithoutEventTimeClock { tasklet, clock } => {
                TaskletError::EventTimeTimerWithoutEventTimeClock { tasklet, clock }
            }
            TimerRefusal::CapExceeded { tasklet, max } => {
                TaskletError::TimerCapExceeded { tasklet, max }
            }
        }
    }
}
