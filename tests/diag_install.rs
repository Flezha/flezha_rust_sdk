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

/// Run `test` of this binary in a child with `env` set and `RUST_LOG` unset:
/// `None` inside the child, its standard output and error outside, once it
/// passed.
fn in_child(test: &str, env: &[(&str, &str)]) -> Option<(String, String)> {
    const CHILD: &str = "FLZ_DIAG_CHILD";
    if std::env::var_os(CHILD).is_some() {
        return None;
    }
    let out = std::process::Command::new(std::env::current_exe().expect("the test binary"))
        .args(["--exact", test, "--nocapture"])
        .env(CHILD, "1")
        .env_remove("RUST_LOG")
        .envs(env.iter().copied())
        .output()
        .expect("the child runs");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(out.status.success(), "{stdout}{stderr}");
    Some((stdout, stderr))
}

/// fails when: `FLEZHA_LOG_LEVEL` does not set the level the default
/// directives pass at.
#[test]
fn flezha_log_level_sets_the_default_level() {
    let test = "flezha_log_level_sets_the_default_level";
    if in_child(test, &[("FLEZHA_LOG_LEVEL", "info")]).is_some() {
        return;
    }
    init_default_tracing();
    let enabled = |level| match level {
        tracing::Level::INFO => {
            tracing::event_enabled!(target: "flezha::transport", tracing::Level::INFO)
        }
        _ => tracing::event_enabled!(target: "flezha::transport", tracing::Level::DEBUG),
    };
    assert!(enabled(tracing::Level::INFO));
    assert!(!enabled(tracing::Level::DEBUG));
}

/// fails when: a core notice bypasses the installed subscriber, or the
/// local-mode banner is hidden by the level filter.
#[test]
fn a_core_notice_reaches_the_subscriber_on_target_flezha() {
    use flz_transport::notice::{notice, Level};
    let test = "a_core_notice_reaches_the_subscriber_on_target_flezha";
    let env = [("FLEZHA_LOG_LEVEL", "error"), ("NO_COLOR", "1")];
    let Some((stdout, stderr)) = in_child(test, &env) else {
        init_default_tracing();
        notice(
            Level::Always,
            "flz_core::local",
            flezha_runner::local::NON_DURABILITY_NOTICE,
        );
        notice(Level::Warn, "flz_core::retry", "a retry below the level");
        return;
    };
    assert!(
        !stderr.contains("[flezha]"),
        "the core wrote to stderr: {stderr}"
    );
    let banner = stdout
        .lines()
        .find(|line| line.contains("LOCAL MODE"))
        .unwrap_or_else(|| panic!("the banner never reached the subscriber: {stdout}"));
    assert!(banner.contains("ERROR flezha:"), "{banner}");
    assert!(banner.contains("source=\"flz_core::local\""), "{banner}");
    assert!(!stdout.contains("a retry below the level"), "{stdout}");
}
