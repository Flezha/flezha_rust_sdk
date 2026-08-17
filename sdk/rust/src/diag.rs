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

/// `EnvFilter` directives passing every diagnostic target the SDK emits
/// on, at `warn` and above. Applications that build their own filter
/// start from this list instead of enumerating SDK internals. A new
/// diagnostic target ships with an entry here in the same change
/// (`docs/sdk_naming_conventions.md`, rule 11).
pub const DEFAULT_DIRECTIVES: &str =
    "flezha=warn,flz_core=warn,flz_transport=warn,flezha_runner=warn";

/// Install a `tracing_subscriber::fmt` subscriber filtered by `RUST_LOG`
/// when set (or on an invalid `RUST_LOG`, falling back), and by
/// [`DEFAULT_DIRECTIVES`] otherwise. Opt-in only — the SDK never installs
/// a global subscriber implicitly; call this from the application's
/// `main`. Calling it when a global subscriber is already installed
/// keeps the existing subscriber, emits one warning, and returns — it
/// never panics, so two components defensively initializing tracing
/// cannot take the process down.
pub fn init_default_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(DEFAULT_DIRECTIVES));
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
}
