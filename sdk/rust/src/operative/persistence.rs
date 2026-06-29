//! Persistence-toggle resolution per `operative-state-persistence-rust/design.md`
//! and the umbrella spec scenario "Executor-side runtime override".
//!
//! Three-way precedence:
//!   1. Executor override (`operative_persistence.<OperativeClass>` in operative-start
//!      metadata) wins if present.
//!   2. Otherwise, the ContinuousOperative's class const `ContinuousOperative::PERSISTENCE_ENABLED`.
//!   3. The trait default of the const is `true`.
//!
//! Frozen at instance start: the resolved value is bound on
//! `ContinuousInstance` at construction; subsequent executor config changes
//! only take effect on the next `stop_operative` + `start_operative` cycle.

/// Resolve the persistence flag for a ContinuousOperative instance. The controller's
/// runtime config is parsed by upstream code into a `Option<bool>` keyed
/// by `operative_persistence.<OperativeClass>`; the present value wins
/// regardless of the class const.
pub fn resolve_persistence_enabled(class_const: bool, operator_override: Option<bool>) -> bool {
    operator_override.unwrap_or(class_const)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_override_wins_over_const_true() {
        assert!(!resolve_persistence_enabled(true, Some(false)));
    }

    #[test]
    fn operator_override_wins_over_const_false() {
        assert!(resolve_persistence_enabled(false, Some(true)));
    }

    #[test]
    fn class_const_used_when_no_override() {
        assert!(resolve_persistence_enabled(true, None));
        assert!(!resolve_persistence_enabled(false, None));
    }
}
