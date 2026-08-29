//! Directive-coverage guard for `flezha_runner::diag`: every diagnostic
//! target the SDK emits on must pass `DEFAULT_DIRECTIVES`, so a new
//! target added without a directive fails here — in the repo where the
//! drift happens — instead of silently disappearing from application
//! logs, which is what a filter that predates a target does.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use flezha_runner::diag::{init_default_tracing, DEFAULT_DIRECTIVES};
use tracing_subscriber::filter::Targets;
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::Layer;

/// Counts every event reaching the subscriber it is layered onto.
struct Counter(Arc<AtomicUsize>);

impl<S: tracing::Subscriber> Layer<S> for Counter {
    fn on_event(&self, _e: &tracing::Event<'_>, _c: Context<'_, S>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Emits `probe` under a thread-scoped subscriber filtered by
/// `directives` and reports whether the event got through.
fn passes(directives: &str, probe: impl FnOnce()) -> bool {
    let seen = Arc::new(AtomicUsize::new(0));
    let sub = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(directives))
        .with(Counter(Arc::clone(&seen)));
    tracing::subscriber::with_default(sub, probe);
    seen.load(Ordering::SeqCst) > 0
}

/// fails when: an SDK diagnostic target family is silently discarded by
/// `DEFAULT_DIRECTIVES`, so a wedged runner logs nothing at all.
#[test]
fn default_directives_pass_every_sdk_diagnostic_target() {
    // One probe per target family: the explicit `flezha::*` targets and
    // the module paths untargeted events inherit from their emitting
    // crate. `target:` takes a literal only, so each is spelled out.
    macro_rules! probes {
        ($($t:literal => $emit:expr),* $(,)?) => {
            vec![$(($t, passes(DEFAULT_DIRECTIVES, || { $emit; }))),*]
        };
    }
    let results = probes! {
        "flezha" => tracing::warn!(target: "flezha", "control-channel reconnect"),
        "flezha::transport" => tracing::warn!(target: "flezha::transport", "write failed"),
        "flezha::credit" => tracing::warn!(target: "flezha::credit", "sender parked"),
        "flezha::mailbox" => tracing::warn!(target: "flezha::mailbox", "mailbox full"),
        "flz_core" => tracing::warn!(target: "flz_core::state_sink", "sink down"),
        "flz_transport" => tracing::warn!(target: "flz_transport::stream", "stream closed"),
        "flezha_runner" => tracing::warn!(target: "flezha_runner::operative::poll_pool", "pool"),
    };
    let dropped: Vec<&str> = results
        .iter()
        .filter(|(_, passed)| !passed)
        .map(|(target, _)| *target)
        .collect();
    assert!(
        dropped.is_empty(),
        "these targets are silently discarded by DEFAULT_DIRECTIVES: {dropped:?}"
    );
}

/// Recursively scans the `.rs` sources under `dir`: collects every
/// `target: "..."` literal into `targets` and reports whether any tracing
/// emit macro omits `target:` — such events inherit the crate's module
/// path, so the crate's own name must then be a directive too.
fn scan_sources(dir: &Path, targets: &mut BTreeSet<String>) -> bool {
    const EMITS: [&str; 6] = [
        "trace!(", "debug!(", "info!(", "warn!(", "error!(", "event!(",
    ];
    let mut untargeted = false;
    for entry in std::fs::read_dir(dir).expect("source dir readable") {
        let path = entry.expect("dir entry readable").path();
        if path.is_dir() {
            untargeted |= scan_sources(&path, targets);
            continue;
        }
        if path.extension().is_none_or(|e| e != "rs") {
            continue;
        }
        let src = std::fs::read_to_string(&path).expect("source file readable");
        let mut rest = src.as_str();
        while let Some(i) = rest.find("target: \"") {
            let after = &rest[i + "target: \"".len()..];
            let Some(end) = after.find('"') else { break };
            targets.insert(after[..end].to_string());
            rest = &after[end..];
        }
        for emit in EMITS {
            let mut rest = src.as_str();
            while let Some(i) = rest.find(emit) {
                let after = &rest[i + emit.len()..];
                if !after.trim_start().starts_with("target:") {
                    untargeted = true;
                    break;
                }
                rest = after;
            }
        }
    }
    untargeted
}

/// fails when: a diagnostic target exists in SDK source that
/// `DEFAULT_DIRECTIVES` does not enable at `warn` — the mechanical half
/// of the red-build guarantee, immune to a stale probe list above.
#[test]
fn every_source_declared_target_is_enabled_by_the_directives() {
    let sdk_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    // The crates the SDK's diagnostics flow through; a new path
    // dependency that starts emitting tracing events joins this list
    // (and DEFAULT_DIRECTIVES) in the same change.
    let crates = [
        ("flezha_runner", sdk_root.join("src")),
        ("flz_core", sdk_root.join("../../flz_core/src")),
        ("flz_transport", sdk_root.join("../../flz_transport/src")),
    ];

    let mut targets = BTreeSet::new();
    for (crate_name, src_dir) in &crates {
        if scan_sources(src_dir, &mut targets) {
            targets.insert((*crate_name).to_string());
        }
    }
    assert!(!targets.is_empty(), "target sweep found no emit sites");

    let directives: Targets = DEFAULT_DIRECTIVES
        .parse()
        .expect("DEFAULT_DIRECTIVES parses as target=level directives");
    let dropped: Vec<String> = targets
        .into_iter()
        .filter(|t| !directives.would_enable(t, &tracing::Level::WARN))
        .collect();
    assert!(
        dropped.is_empty(),
        "SDK source emits on targets DEFAULT_DIRECTIVES discards at warn: {dropped:?}"
    );
}

/// fails when: `init_default_tracing` panics against an already-installed
/// global subscriber, or does not warn that it kept the existing one.
#[test]
fn init_after_a_global_subscriber_warns_and_returns() {
    let seen = Arc::new(AtomicUsize::new(0));
    let sub = tracing_subscriber::registry().with(Counter(Arc::clone(&seen)));
    tracing::subscriber::set_global_default(sub).expect("test installs the first global");

    init_default_tracing();
    assert_eq!(seen.load(Ordering::SeqCst), 1, "exactly one warning");
    // Every further attempt keeps warning instead of panicking.
    init_default_tracing();
    assert_eq!(seen.load(Ordering::SeqCst), 2);
}
