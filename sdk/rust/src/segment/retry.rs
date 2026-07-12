//! The retry ladder both operative profiles climb before their terminal escape.
//!
//! An invocation that fails retryably is re-run on its [`RetryPolicy`] — the
//! input channel's for an unbounded operation (`platform-dead-letter-queue`),
//! the class's for a bounded segment (`bounded-segment-retry`) — and only an
//! exhausted (or author-classified non-retryable) failure escapes: the unbounded
//! profile parks the payload to the dead-letter queue and advances, the bounded
//! profile fails the operation into AbortAll + saga.
//!
//! What is shared is the **policy**: how long attempt `n` waits, that the wait
//! is counted and logged, and that a caller with a cancellation token may cut it
//! short. What each profile does per attempt — re-clone the input and extract
//! park bytes, or re-run a segment's batch into a fresh sink — stays with the
//! profile.

use std::fmt::Display;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::channel::RetryPolicy;
use crate::segment::park::RETRIES_TOTAL;

/// Record a failed-but-retryable attempt — count it (the `data_heartbeat`
/// retry metric) and log it — and return how long to wait before the next one.
/// `label` names the failing work (operation, or bounded instance/operation/
/// segment).
pub(crate) fn note_failed_attempt(
    label: &str,
    attempt: u32,
    policy: RetryPolicy,
    error: &dyn Display,
) -> Duration {
    let delay = backoff(policy.backoff_base_ms, attempt);
    RETRIES_TOTAL.fetch_add(1, Ordering::Relaxed);
    eprintln!(
        "[flezha] {label} attempt {attempt}/{} failed (retrying in {} ms): {error}",
        policy.max_attempts,
        delay.as_millis()
    );
    delay
}

/// Exponential backoff with ±50% jitter: `base * 2^(attempt-1)`, jitter
/// included, capped at [`crate::channel::RETRY_BACKOFF_MAX_MS`]. The jitter
/// draws from a cheap xorshift over the wall clock — backoff timing is runtime
/// scheduling, outside the operation determinism contract.
fn backoff(base_ms: u64, attempt: u32) -> Duration {
    let exp = base_ms
        .max(1)
        .saturating_mul(1u64 << (attempt.saturating_sub(1)).min(16))
        .min(crate::channel::RETRY_BACKOFF_MAX_MS);
    let mut x = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0x9E37_79B9)
        | 1;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    // Uniform in [exp/2, exp*1.5), then re-capped.
    let jittered = exp / 2 + x % exp;
    Duration::from_millis(jittered.min(crate::channel::RETRY_BACKOFF_MAX_MS))
}
