//! Typed wrapper over the core's timer layer.

pub use flz_core::schema::{TimerKind, TimerOp};
pub use flz_core::timer::*;

/// Resolve a Procedure's declared timer policy.
pub fn timer_policy_for<S: crate::procedure::Procedure>() -> TimerPolicy {
    TimerPolicy::new(
        S::max_timers_per_entity(),
        S::name(),
        <S::Clock as crate::procedure::Clock>::IS_EVENT_TIME,
        <S::Clock as crate::procedure::Clock>::type_name(),
    )
}
