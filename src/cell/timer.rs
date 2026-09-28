//! Typed wrapper over the core's timer layer.

pub use flz_core::schema::{TimerKind, TimerOp};
pub use flz_core::timer::*;

/// Resolve a Tasklet's declared timer policy.
pub fn timer_policy_for<S: crate::tasklet::Tasklet>() -> TimerPolicy {
    TimerPolicy::new(S::max_timers_per_cell(), S::NAME, S::CLOCK)
}
