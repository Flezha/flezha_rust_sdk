//! The clock a tasklet declares, and how the runtime evaluates it.
//!
//! The runtime, not user code, reads the clock: the dispatcher evaluates the
//! tasklet's [`Tasklet::CLOCK`] once per invocation and `ctx.now()` serves that
//! single value for the whole invocation. Every clock yields a `u64`:
//!
//! - [`ClockKind::Wall`]: milliseconds since the Unix epoch, read at dispatch
//!   entry. Consistent within an invocation, non-deterministic across replays
//!   by declared intent.
//! - [`ClockKind::EventTime`]: what [`Tasklet::event_time`] reads off the input
//!   being dispatched. Fully deterministic; the clock for time-like ordering.
//! - [`ClockKind::Logical`]: the Cell's durable change sequence, which advances
//!   as applied changes are flushed, survives crash-restore, and is `0` until
//!   the first.

pub use flz_core::host::HostClock as ClockKind;

use crate::tasklet::Tasklet;

/// The one value `ctx.now()` serves for an invocation of `S`. `input` is
/// `None` only for `compensate`, where an event-time clock yields `0`. The
/// wall and logical sources are lazy, so a clock that does not read them
/// costs nothing on the dispatch path.
pub(crate) fn evaluate<S: Tasklet>(
    input: Option<&S::Input>,
    wall_ms: impl FnOnce() -> u64,
    logical: impl FnOnce() -> u64,
) -> u64 {
    match S::CLOCK {
        ClockKind::Wall => wall_ms(),
        ClockKind::EventTime => input.map(S::event_time).unwrap_or(0),
        ClockKind::Logical => logical(),
    }
}

pub(crate) use flz_core::util::wall_now_ms;
