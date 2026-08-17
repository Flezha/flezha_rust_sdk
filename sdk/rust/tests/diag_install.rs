//! `init_default_tracing`'s install path, in its own test binary: it
//! claims the process-global subscriber slot, which `tests/diag.rs`
//! needs free for its double-init case.

use flezha_runner::diag::init_default_tracing;

/// fails when: `init_default_tracing` does not install a global
/// subscriber, or the installed filter does not behave like
/// `DEFAULT_DIRECTIVES` (warn passes on an SDK target, debug does not).
#[test]
fn init_installs_a_subscriber_filtered_by_the_default_directives() {
    // The fallback branch is the one under test.
    std::env::remove_var("RUST_LOG");
    assert!(!tracing::dispatcher::has_been_set());

    init_default_tracing();

    assert!(tracing::dispatcher::has_been_set());
    assert!(tracing::event_enabled!(
        target: "flezha::transport",
        tracing::Level::WARN
    ));
    assert!(!tracing::event_enabled!(
        target: "flezha::transport",
        tracing::Level::DEBUG
    ));
}
