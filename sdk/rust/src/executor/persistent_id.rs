//! Self-owned, durable **stable identity** for the executor.
//!
//! Created once on first boot and persisted at `FLEZHA_EXECUTOR_ID_PATH`
//! (atomically: temp file + `rename`). The controller trusts this value as the
//! operative-binding key, so it must survive a process restart on the same
//! node. Derived for readability as `"<namespace>-<profile_hash8>-<rand6>"`
//! (profile_hash8 is a stable hash of the declared factory set; the boot-time
//! hostname is deliberately NOT load-bearing — a container hostname churns on
//! recreate, which would break binding continuity).
//!
//! Degradation (no writable path): a fresh per-boot id is generated, cached in
//! memory for the life of the process (so reconnects present the SAME id), and
//! a single `warn` is logged. A restart then presents a NEW id whose old
//! binding ages out via `FLEZHA_EXECUTOR_KEEPALIVE` — i.e. reconnect-as-new.
//! The executor never refuses to start or crashes on missing persistence.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

const ENV_ID_PATH: &str = "FLEZHA_EXECUTOR_ID_PATH";
const DEFAULT_ID_PATH: &str = "/var/lib/flezha-executor/id";

// One stable id per process: derive-or-load runs once and reconnects within the
// same process return the SAME id — critical for the degraded, no-persist case
// where nothing is written to disk to re-load.
static STABLE_ID: OnceLock<String> = OnceLock::new();

pub fn id_path() -> PathBuf {
    std::env::var(ENV_ID_PATH)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_ID_PATH))
}

/// The executor's stable identity for this process. Loads the persisted id if
/// present; otherwise derives one from `namespace` + the declared factory set,
/// persists it best-effort, and caches it. Subsequent calls (including every
/// reconnect) return the same cached value.
pub fn stable_id(namespace: &str, factory_ids: &[String]) -> String {
    STABLE_ID
        .get_or_init(|| derive_or_load(&id_path(), namespace, factory_ids))
        .clone()
}

fn derive_or_load(path: &Path, namespace: &str, factory_ids: &[String]) -> String {
    if let Some(existing) = load_from(path) {
        return existing;
    }

    let id = derive_id(namespace, factory_ids);

    if let Err(e) = persist_to(path, &id) {
        // Degradation per the executor-identity capability: no binding
        // continuity across restart, but the executor runs normally.
        tracing::warn!(
            target: "flezha",
            "executor stable-id persist failed at {:?}: {} — using a fresh per-boot id \
             (no binding continuity across restart)",
            path, e
        );
    }

    id
}

/// `"<namespace>-<profile_hash8>-<rand6>"`. The profile hash is order-independent
/// (factory ids are sorted first) and dependency-free (FNV-1a), so the same
/// executor profile reads the same way across boots; the random suffix keeps two
/// fresh executors of the same profile distinct on first boot.
fn derive_id(namespace: &str, factory_ids: &[String]) -> String {
    let mut ids: Vec<&str> = factory_ids.iter().map(String::as_str).collect();
    ids.sort_unstable();
    format!("{}-{}-{}", namespace, profile_hash8(&ids), rand6())
}

fn profile_hash8(sorted_ids: &[&str]) -> String {
    // FNV-1a over the sorted, NUL-delimited factory ids.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for id in sorted_ids {
        for b in id.bytes().chain(std::iter::once(0u8)) {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("{:08x}", (hash & 0xffff_ffff) as u32)
}

fn rand6() -> String {
    // Dependency-free entropy: clock noise (the shared `nonce_suffix` helper)
    // mixed with the pid is enough to keep two fresh executors distinct —
    // cryptographic randomness is not needed, since the persistence path already
    // scopes uniqueness per executor.
    let nanos = crate::util::nonce_suffix() as u64;
    let mut seed = nanos ^ ((std::process::id() as u64) << 32) ^ nanos.rotate_left(17);
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut out = String::with_capacity(6);
    for _ in 0..6 {
        // xorshift step so the 6 chars spread instead of repeating.
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        out.push(ALPHABET[(seed % ALPHABET.len() as u64) as usize] as char);
    }
    out
}

fn load_from(path: &Path) -> Option<String> {
    let raw = std::fs::read_to_string(path).ok()?;
    let id = raw.trim();
    if id.is_empty() {
        None
    } else {
        Some(id.to_string())
    }
}

fn persist_to(path: &Path, id: &str) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(id.as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("flezha-pid-test-{}-{}", std::process::id(), name));
        p.push("id");
        p
    }

    #[test]
    fn persist_then_load_round_trips() {
        let path = tmp_path("round-trip");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        persist_to(&path, "exec-123").unwrap();
        assert_eq!(load_from(&path), Some("exec-123".to_string()));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn load_missing_file_is_none() {
        let path = tmp_path("missing");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        assert_eq!(load_from(&path), None);
    }

    #[test]
    fn blank_or_whitespace_id_is_none() {
        let path = tmp_path("blank");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        persist_to(&path, "   \n").unwrap();
        assert_eq!(load_from(&path), None);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn derive_id_has_namespace_prefix_profile_hash_and_random_suffix() {
        let factories = vec!["b_factory".to_string(), "a_factory".to_string()];
        let id = derive_id("default", &factories);
        let parts: Vec<&str> = id.splitn(3, '-').collect();
        assert_eq!(parts.len(), 3, "id must be ns-profile-rand, got {id}");
        assert_eq!(parts[0], "default");
        assert_eq!(parts[1].len(), 8, "profile hash must be 8 hex chars");
        assert!(parts[1].chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(parts[2].len(), 6, "random suffix must be 6 chars");
    }

    #[test]
    fn derive_id_profile_segment_is_order_independent_and_set_sensitive() {
        // derive_id sorts the factory ids before hashing, so the profile segment
        // (the middle of ns-profile-rand) is identical regardless of input
        // order...
        let profile = |ids: &[&str]| {
            let owned: Vec<String> = ids.iter().map(|s| s.to_string()).collect();
            derive_id("ns", &owned)
                .split('-')
                .nth(1)
                .unwrap()
                .to_string()
        };
        assert_eq!(profile(&["a", "b", "c"]), profile(&["c", "a", "b"]));
        // ...and differs for a different factory set.
        assert_ne!(profile(&["a", "b", "c"]), profile(&["a", "b"]));
    }

    #[test]
    fn derive_or_load_reloads_a_persisted_id() {
        // First call derives + persists; a second derive_or_load against the
        // same path returns the SAME id (binding continuity across restart).
        let path = tmp_path("reload");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        let first = derive_or_load(&path, "ns", &["f1".to_string()]);
        let second = derive_or_load(&path, "ns", &["f1".to_string()]);
        assert_eq!(first, second);
        assert_eq!(load_from(&path), Some(first));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn unwritable_path_degrades_to_a_fresh_id_without_panicking() {
        // A path under a non-existent, non-creatable parent (a file used as a
        // directory) makes persist fail; derive_or_load must still return a
        // usable id and never panic (degradation contract).
        let mut blocker = std::env::temp_dir();
        blocker.push(format!("flezha-pid-blocker-{}", std::process::id()));
        std::fs::write(&blocker, b"x").unwrap();
        let mut path = blocker.clone();
        path.push("nested"); // parent is a FILE → create_dir_all fails
        let id = derive_or_load(&path, "ns", &["f1".to_string()]);
        assert!(id.starts_with("ns-"), "degraded id is still derived: {id}");
        assert_eq!(load_from(&path), None, "nothing was persisted");
        let _ = std::fs::remove_file(&blocker);
    }
}
