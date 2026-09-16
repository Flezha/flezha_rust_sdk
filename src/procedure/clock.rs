//! `Clock` sealed trait and the three runtime-served implementations.
//!
//! The runtime — not user code — reads the clock: the dispatcher evaluates
//! the Procedure's declared `Clock` exactly once per `run` invocation and
//! `ProcedureCtx::now()` serves that single value for the whole invocation.
//! All three clocks yield `u64`:
//!
//! - [`WallClock`] — milliseconds since the Unix epoch, read at dispatch
//!   entry. Consistent within an invocation, non-deterministic across replays
//!   *by declared intent*.
//! - [`EventTimeFrom<F>`] — the value `F::extract` pulls from the input being
//!   dispatched. Fully deterministic; the recommended clock for time-like
//!   ordering.
//! - [`LogicalClock`] — the Entity's durable change sequence: advances as
//!   applied changes are flushed, survives crash-restore, `0` until the first.

mod sealed {
    pub trait Sealed {}
}

/// Time source declared by a Procedure. Sealed: the only impls are
/// `WallClock`, `LogicalClock`, and `EventTimeFrom<F>`. Custom Clocks
/// are refused at compile time.
pub trait Clock: sealed::Sealed + Send + Sync + 'static {
    /// Time value served by `ProcedureCtx::now()`. All shipped clocks use
    /// `u64` (epoch milliseconds, extracted event time, or change seq).
    type Time: Copy + Send + Sync + 'static;

    /// Whether this clock's value is an *event* time — a property of the
    /// input rather than of when it happened to be processed. Only such a
    /// clock gives late-input correction something to order by, so it gates
    /// the correction machinery (lateness detection, the replay sort) at
    /// compile time.
    const IS_EVENT_TIME: bool = false;

    /// Stable name used by build-time clock validation errors.
    #[doc(hidden)]
    fn type_name() -> &'static str;
}

/// Extract an event timestamp from an input. Implement on a unit marker
/// type and declare `type Clock = EventTimeFrom<YourMarker>;` — the
/// dispatcher applies the extractor to each input before `run`, and
/// `ctx.now()` returns the extracted value.
pub trait TimeExtractor<Input>: Send + Sync + 'static {
    fn extract(input: &Input) -> u64;
}

/// Runtime evaluation seam: how the dispatcher computes the one
/// per-invocation value `ctx.now()` serves. The `Input` parameter lets
/// `EventTimeFrom<F>` require `F: TimeExtractor<Input>` — a clock the
/// runtime cannot serve is a compile error at the `Procedure` impl.
pub trait ClockFor<Input>: Clock {
    /// `input` is `None` only for `compensate` (there is no current
    /// input); `EventTimeFrom` yields `0` there. `wall_ms` / `logical`
    /// are lazy so unused sources cost nothing on the dispatch path.
    #[doc(hidden)]
    fn evaluate(
        input: Option<&Input>,
        wall_ms: impl FnOnce() -> u64,
        logical: impl FnOnce() -> u64,
    ) -> u64;
}

/// Wall-clock time source: milliseconds since the Unix epoch, read once
/// at dispatch entry. Suitable for Procedures whose outputs are
/// insensitive to replay-time drift, or that ignore time entirely.
pub struct WallClock;
impl sealed::Sealed for WallClock {}
impl Clock for WallClock {
    type Time = u64;
    fn type_name() -> &'static str {
        "WallClock"
    }
}
impl<Input> ClockFor<Input> for WallClock {
    fn evaluate(
        _input: Option<&Input>,
        wall_ms: impl FnOnce() -> u64,
        _logical: impl FnOnce() -> u64,
    ) -> u64 {
        wall_ms()
    }
}

/// Logical clock: serves the Entity's durable change sequence.
pub struct LogicalClock;
impl sealed::Sealed for LogicalClock {}
impl Clock for LogicalClock {
    type Time = u64;
    fn type_name() -> &'static str {
        "LogicalClock"
    }
}
impl<Input> ClockFor<Input> for LogicalClock {
    fn evaluate(
        _input: Option<&Input>,
        _wall_ms: impl FnOnce() -> u64,
        logical: impl FnOnce() -> u64,
    ) -> u64 {
        logical()
    }
}

/// Event-time clock parametrized by a user-supplied input-to-timestamp
/// extractor marker type.
pub struct EventTimeFrom<F>(std::marker::PhantomData<F>);
impl<F: Send + Sync + 'static> sealed::Sealed for EventTimeFrom<F> {}
impl<F: Send + Sync + 'static> Clock for EventTimeFrom<F> {
    type Time = u64;
    const IS_EVENT_TIME: bool = true;
    fn type_name() -> &'static str {
        "EventTimeFrom"
    }
}
impl<Input, F> ClockFor<Input> for EventTimeFrom<F>
where
    F: TimeExtractor<Input>,
{
    fn evaluate(
        input: Option<&Input>,
        _wall_ms: impl FnOnce() -> u64,
        _logical: impl FnOnce() -> u64,
    ) -> u64 {
        input.map(F::extract).unwrap_or(0)
    }
}
pub(crate) use flz_core::util::wall_now_ms;
