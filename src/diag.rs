//! Diagnostics defaults: the SDK's tracing-target inventory as an
//! `EnvFilter` directive list, plus an opt-in subscriber installer.
//!
//! The SDK does not log under one crate name. Explicit diagnostics use
//! `flezha::*` targets (`flezha::transport`, `flezha::credit`,
//! `flezha::mailbox`, ...), while events with no explicit target inherit
//! the module path of whichever crate emits them — `flz_core`,
//! `flz_transport`, or `flezha_runner`. An application filter that names
//! only some of those silently discards the rest, so the full inventory
//! is exported here and guarded by the SDK's directive-coverage tests
//! (`tests/diag.rs`).

use flz_transport::notice::Level;

/// `EnvFilter` directives passing every diagnostic target the SDK emits
/// on, at `warn` and above. Applications that build their own filter
/// start from this list instead of enumerating SDK internals. A new
/// diagnostic target ships with an entry here in the same change
/// (`docs/internal/sdk/naming_conventions.md`, rule 11).
pub const DEFAULT_DIRECTIVES: &str =
    "flezha=warn,flz_core=warn,flz_transport=warn,flezha_runner=warn";

/// Install a `tracing_subscriber::fmt` subscriber filtered by `RUST_LOG`
/// when set (or on an invalid `RUST_LOG`, falling back), and otherwise by
/// [`DEFAULT_DIRECTIVES`] at the level `FLEZHA_LOG_LEVEL` names (`debug`,
/// `info`, `warn` or `error`; `warn` when unset). It also sends the core's
/// notices to `tracing` on target `flezha`, the local-mode banner at `error`
/// so that no level hides it. Opt-in only — the SDK never installs a global
/// subscriber implicitly; call this from the application's `main`. Calling it
/// when a global subscriber is already installed keeps the existing
/// subscriber, emits one warning, and returns — it never panics, so two
/// components defensively initializing tracing cannot take the process down.
pub fn init_default_tracing() {
    let level = flz_core::util::env_raw("FLEZHA_LOG_LEVEL")
        .filter(|level| ["debug", "info", "warn", "error"].contains(&level.as_str()))
        .unwrap_or_else(|| "warn".to_string());
    let directives = DEFAULT_DIRECTIVES.replace("=warn", &format!("={level}"));
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(directives));
    if tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .is_err()
    {
        tracing::warn!(
            target: "flezha",
            "init_default_tracing: a global tracing subscriber is already installed; keeping it"
        );
    }
    flz_transport::notice::set_hook(Box::new(|level, source, text| match level {
        Level::Info => tracing::info!(target: "flezha", source, "{text}"),
        Level::Warn => tracing::warn!(target: "flezha", source, "{text}"),
        Level::Error | Level::Always => tracing::error!(target: "flezha", source, "{text}"),
    }));
}
