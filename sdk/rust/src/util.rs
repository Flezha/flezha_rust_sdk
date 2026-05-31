//! Small crate-internal helpers shared across modules to avoid
//! re-implementing the same one-liners (env parsing, lock poison
//! recovery, big-endian reads, monotonic-ish nonces).

use std::str::FromStr;
use std::sync::{Mutex, MutexGuard};

/// Parse an env var as `T`, falling back to `default` when unset or
/// unparseable.
pub(crate) fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Like [`env_parse`] for `usize`, additionally rejecting `0` (treated as
/// "unset") so a `0` override falls back to `default`.
pub(crate) fn env_parse_positive(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
}

/// Nanoseconds since the Unix epoch, `0` if the clock is before it. Used
/// only to make reference ids / nonces unique, never for ordering.
pub(crate) fn nonce_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Read a big-endian `u32` at `off`. Caller must ensure `off + 4 <= buf.len()`.
pub(crate) fn read_u32_be(buf: &[u8], off: usize) -> u32 {
    u32::from_be_bytes(buf[off..off + 4].try_into().unwrap())
}

/// Read a big-endian `u64` at `off`. Caller must ensure `off + 8 <= buf.len()`.
pub(crate) fn read_u64_be(buf: &[u8], off: usize) -> u64 {
    u64::from_be_bytes(buf[off..off + 8].try_into().unwrap())
}

/// Lock a `Mutex`, recovering the guard if a previous holder panicked.
/// The data behind these locks is always left consistent on panic, so
/// proceeding with the poisoned guard is correct and avoids a cascade.
pub(crate) trait LockRecover<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T>;
}

impl<T> LockRecover<T> for Mutex<T> {
    fn lock_recover(&self) -> MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
