//! `Clock` sealed trait and v1 implementations per `design.md` Decision 9.
//!
//! v1 fully wires only `WallClock`. `LogicalClock` and `EventTimeFrom<F>`
//! ship the trait shape but their `now()` is `unimplemented!()`; the
//! runtime rejects Operations declaring a non-`WallClock` `type Clock` with
//! `OperativeStartError::ClockNotImplementedYet { clock_type }` until the
//! follow-up `operative-clock-impls` change wires them.

use std::sync::atomic::AtomicU64;
use std::time::SystemTime;

mod sealed {
    pub trait Sealed {}
}

/// Time source declared by a Operation. Sealed: the only impls in v1 are
/// `WallClock`, `LogicalClock`, and `EventTimeFrom<F>`. Custom Clocks
/// are rejected at compile time.
pub trait Clock: sealed::Sealed + Send + Sync + 'static {
    /// Time value produced by `now()`. Bounded `Copy + Send + Sync` so
    /// the runtime can store the value in atomics or share across
    /// threads without further wrappers.
    type Time: Copy + Send + Sync + 'static;

    /// Read the clock. Wall-clock implementations call
    /// `SystemTime::now()`; logical clocks return the per-Segment
    /// counter; event-time clocks defer to the user's extractor.
    fn now() -> Self::Time;

    /// Stable name used by the runtime when rejecting non-Wall clocks
    /// pending `operative-clock-impls`.
    #[doc(hidden)]
    fn type_name() -> &'static str;
}

/// Wall-clock time source. Reads `std::time::SystemTime::now()` on each
/// call; suitable for Operations whose state mutations are insensitive to
/// clock skew or for pure event-pipeline Operations that ignore time.
pub struct WallClock;
impl sealed::Sealed for WallClock {}
impl Clock for WallClock {
    type Time = SystemTime;
    fn now() -> SystemTime {
        SystemTime::now()
    }
    fn type_name() -> &'static str {
        "WallClock"
    }
}

/// Logical clock. v1 ships the trait shape; per-Segment state-wiring is
/// reserved for `operative-clock-impls`.
pub struct LogicalClock(#[allow(dead_code)] AtomicU64);
impl sealed::Sealed for LogicalClock {}
impl Clock for LogicalClock {
    type Time = u64;
    fn now() -> u64 {
        unimplemented!(
            "LogicalClock requires per-Segment state wired by `operative-clock-impls`; \
             v1 only fully supports WallClock"
        )
    }
    fn type_name() -> &'static str {
        "LogicalClock"
    }
}

/// Event-time clock parametrized by a user-supplied input-to-timestamp
/// extractor. v1 ships the shape; full wiring is reserved.
pub struct EventTimeFrom<F>(#[allow(dead_code)] std::marker::PhantomData<F>);
impl<F: Send + Sync + 'static> sealed::Sealed for EventTimeFrom<F> {}
impl<F: Send + Sync + 'static> Clock for EventTimeFrom<F> {
    type Time = u64;
    fn now() -> u64 {
        unimplemented!(
            "EventTimeFrom<F> reads timestamps from inputs and is wired by \
             `operative-clock-impls`; v1 only fully supports WallClock"
        )
    }
    fn type_name() -> &'static str {
        "EventTimeFrom"
    }
}
