//! Node-local persistence of the controller-assigned executor id.
//!
//! The id is written after every `AssignExecutorId` / `ReassignExecutorId`
//! (atomically: temp file + `rename`) so a fresh process that restarts on
//! the same node can present it as `previous_executor_id` in its
//! `connect_control_channel` handshake. The controller then evicts the
//! stale binding before re-binding instead of leaving a corpse the
//! takeover rebinds onto.
//!
//! Best-effort: a write failure never disrupts the executor — it only
//! forfeits the reconnect optimisation. The path must live on a
//! node-local hostPath (not `emptyDir`, which does not survive a pod
//! restart); ops mounts it and points `FLEZHA_EXECUTOR_ID_PATH` at it.

use std::io::Write;
use std::path::{Path, PathBuf};

const ENV_ID_PATH: &str = "FLEZHA_EXECUTOR_ID_PATH";
const DEFAULT_ID_PATH: &str = "/var/lib/flezha-executor/id";

pub fn id_path() -> PathBuf {
    std::env::var(ENV_ID_PATH)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_ID_PATH))
}

/// Read the persisted id from a prior process, if present and non-empty.
pub fn load() -> Option<String> {
    load_from(&id_path())
}

/// Atomically persist `id`. Best-effort: logs at debug on failure so a
/// read-only / unmounted path never panics the command handler.
pub fn persist(id: &str) {
    if let Err(e) = persist_to(&id_path(), id) {
        tracing::debug!(target: "flezha", "executor id persist failed: {}", e);
    }
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
    fn persist_overwrites_prior_id() {
        let path = tmp_path("overwrite");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        persist_to(&path, "old").unwrap();
        persist_to(&path, "new").unwrap();
        assert_eq!(load_from(&path), Some("new".to_string()));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
