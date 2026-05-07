//! Per-Partition flusher per `relay-state-persistence-rust/design.md`
//! Decision 2.
//!
//! Single OS thread per process, lazy-initialized via `OnceLock` on the
//! first `register_relay::<R>()` for a Shard with a non-`NoChange`
//! Change. The thread runs an infinite loop: sleep for `linger_ms`,
//! iterate registered Partitions, drain non-empty buffers, build wire
//! frames, hand them to the per-Partition `send_frame` fn pointer.
//!
//! Frame layout (umbrella spec / `rust-sdk-dispatch` delta):
//!
//! ```text
//! [partition_id: u32 BE]
//! [seq: u64 BE]
//! [header: 16B]
//! [change_bytes_len: u32 BE]
//! [change_bytes]
//! [crc32: u32 BE over change_bytes]
//! ```
//!
//! Header is the 16-byte `HeaderTemplate::stamp` output (per Decision 8);
//! the trailing CRC32 is the same value for redundancy on the wire so
//! the agent's WAL reader can validate framing without re-decoding the
//! header. The `change_bytes_len` field bounds the change-bytes range so
//! the WAL reader can step records without re-deserialization.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use dashmap::DashMap;

use crate::partition::header::HeaderTemplate;
use crate::shard::change::{Change, CoalesceKey};

/// Global identifier for a registered Partition entry. Assigned at
/// flusher-registration time; opaque to callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlusherEntryId(u64);

/// Errors returned from the wire stub or its agent-wired replacement.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("transport down: {0}")]
    TransportDown(String),
    #[error("send failure: {0}")]
    Other(String),
}

pub type SendFrameFn = Arc<dyn Fn(Vec<u8>) -> Result<(), SendError> + Send + Sync + 'static>;

/// Per-Partition entry registered with the flusher.
struct FlusherEntry {
    partition_id: u32,
    buffer: Arc<Mutex<Box<dyn FlushableBuffer>>>,
    persistence_enabled: bool,
    change_seq: Arc<AtomicU64>,
    send_frame: SendFrameFn,
    header: HeaderTemplate,
}

/// Type-erased per-Partition flush buffer. The dispatcher downcasts to
/// `TypedBuffer<S>` when constructing a `StateHandle`; the flusher only
/// sees the trait surface.
pub trait FlushableBuffer: Send + Any {
    /// Drain the buffer. Applies coalescing and produces wire frames in
    /// emission order. Returns `(post_coalesce_count, frames)`.
    ///
    /// `change_seq` is fetched-and-added once per produced frame; the
    /// returned frames carry seq values in increasing order.
    fn drain_to_frames(
        &mut self,
        partition_id: u32,
        change_seq: &AtomicU64,
        header: &HeaderTemplate,
    ) -> (usize, Vec<Vec<u8>>);

    /// Whether the underlying buffer is empty. The flusher uses this to
    /// avoid building frames when a Partition has had no `state.apply`
    /// calls in the linger window.
    fn is_empty(&self) -> bool;

    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Concrete per-Shard buffer. The dispatcher downcasts the type-erased
/// `dyn FlushableBuffer` to `TypedBuffer<S>` to get a `&mut Vec<S::Change>`
/// for `StateHandle::new`.
pub struct TypedBuffer<S: crate::shard::Shard> {
    pub inner: Vec<S::Change>,
}

impl<S: crate::shard::Shard> Default for TypedBuffer<S> {
    fn default() -> Self {
        Self { inner: Vec::new() }
    }
}

impl<S: crate::shard::Shard> FlushableBuffer for TypedBuffer<S> {
    fn drain_to_frames(
        &mut self,
        partition_id: u32,
        change_seq: &AtomicU64,
        header: &HeaderTemplate,
    ) -> (usize, Vec<Vec<u8>>) {
        if self.inner.is_empty() {
            return (0, Vec::new());
        }
        let coalesced = coalesce(std::mem::take(&mut self.inner));
        let frames: Vec<Vec<u8>> = coalesced
            .into_iter()
            .map(|c| {
                let seq = change_seq.fetch_add(1, Ordering::Relaxed);
                build_partition_state_change_frame::<S::Change>(partition_id, seq, header, &c)
            })
            .collect();
        let count = frames.len();
        (count, frames)
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Drain-time coalesce: keep the latest record per `coalesce_key`,
/// preserve order of kept records, and retain `coalesce_key() == None`
/// records verbatim.
pub fn coalesce<C: Change>(buf: Vec<C>) -> Vec<C> {
    if buf.is_empty() {
        return buf;
    }
    let mut latest: HashMap<CoalesceKey, usize> = HashMap::with_capacity(buf.len());
    let mut keep_mask = vec![true; buf.len()];
    for (i, c) in buf.iter().enumerate() {
        if let Some(k) = c.coalesce_key() {
            if let Some(&prev_i) = latest.get(&k) {
                keep_mask[prev_i] = false;
            }
            latest.insert(k, i);
        }
    }
    let mut out = Vec::with_capacity(buf.len());
    for (i, c) in buf.into_iter().enumerate() {
        if keep_mask[i] {
            out.push(c);
        }
    }
    out
}

/// Build one wire frame: `[partition_id][seq][header][len][change_bytes][crc32]`.
pub fn build_partition_state_change_frame<C: Change>(
    partition_id: u32,
    seq: u64,
    header: &HeaderTemplate,
    change: &C,
) -> Vec<u8> {
    let change_bytes = postcard::to_allocvec(change)
        .expect("postcard serialization of Change failed; Change must be Serialize");
    let header_bytes = header.stamp(&change_bytes);
    let crc = crc32fast::hash(&change_bytes);
    let mut frame =
        Vec::with_capacity(4 + 8 + 16 + 4 + change_bytes.len() + 4);
    frame.extend_from_slice(&partition_id.to_be_bytes());
    frame.extend_from_slice(&seq.to_be_bytes());
    frame.extend_from_slice(&header_bytes);
    frame.extend_from_slice(&(change_bytes.len() as u32).to_be_bytes());
    frame.extend_from_slice(&change_bytes);
    frame.extend_from_slice(&crc.to_be_bytes());
    frame
}

/// Single per-process flusher. Spawns one OS thread named `flezha-flusher`
/// on first registration of a non-`NoChange` Partition. Programs without
/// any non-`NoChange` Shards never reach `instance()`, so no thread is
/// spawned.
pub struct PartitionFlusher {
    entries: Arc<DashMap<FlusherEntryId, FlusherEntry>>,
    next_entry_id: AtomicU64,
    shutdown: Arc<AtomicBool>,
    linger_ms: u64,
}

static FLUSHER: OnceLock<Arc<PartitionFlusher>> = OnceLock::new();

const ENV_LINGER_MS: &str = "FLEZHA_FLUSH_LINGER_MS";
const DEFAULT_LINGER_MS: u64 = 5;

impl PartitionFlusher {
    /// Lazily-initialized per-process flusher. Spawns the
    /// `flezha-flusher` OS thread on first call.
    pub fn instance() -> Arc<PartitionFlusher> {
        FLUSHER
            .get_or_init(|| {
                let linger_ms = std::env::var(ENV_LINGER_MS)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(DEFAULT_LINGER_MS);
                let flusher = Arc::new(PartitionFlusher {
                    entries: Arc::new(DashMap::new()),
                    next_entry_id: AtomicU64::new(0),
                    shutdown: Arc::new(AtomicBool::new(false)),
                    linger_ms,
                });
                let handle = Arc::clone(&flusher);
                thread::Builder::new()
                    .name("flezha-flusher".to_string())
                    .spawn(move || handle.run_loop())
                    .expect("spawning flezha-flusher thread should not fail");
                flusher
            })
            .clone()
    }

    /// Register a Partition with the flusher. Returns an opaque id used
    /// to deregister at Partition teardown (not yet wired in v1; the
    /// flusher lives for the duration of the process).
    pub fn register(
        &self,
        partition_id: u32,
        buffer: Arc<Mutex<Box<dyn FlushableBuffer>>>,
        persistence_enabled: bool,
        change_seq: Arc<AtomicU64>,
        send_frame: SendFrameFn,
        header: HeaderTemplate,
    ) -> FlusherEntryId {
        let id = FlusherEntryId(self.next_entry_id.fetch_add(1, Ordering::Relaxed));
        self.entries.insert(
            id,
            FlusherEntry {
                partition_id,
                buffer,
                persistence_enabled,
                change_seq,
                send_frame,
                header,
            },
        );
        id
    }

    /// Test-only: synchronously drive one drain pass so unit tests can
    /// observe drained state without sleeping for the linger window.
    #[doc(hidden)]
    pub fn drain_now_for_tests(&self) {
        self.drain_pass();
    }

    fn run_loop(self: Arc<Self>) {
        while !self.shutdown.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(self.linger_ms));
            self.drain_pass();
        }
    }

    fn drain_pass(&self) {
        for entry_ref in self.entries.iter() {
            let entry = entry_ref.value();
            let frames = {
                let mut guard = match entry.buffer.lock() {
                    Ok(g) => g,
                    Err(poisoned) => poisoned.into_inner(),
                };
                if guard.is_empty() {
                    continue;
                }
                let (_count, frames) = guard.drain_to_frames(
                    entry.partition_id,
                    &entry.change_seq,
                    &entry.header,
                );
                frames
            };
            if !entry.persistence_enabled {
                tracing::trace!(
                    target: "flezha::flusher",
                    "drain-and-discard {} frames for partition {} (persistence_enabled = false)",
                    frames.len(),
                    entry.partition_id,
                );
                continue;
            }
            for frame in frames {
                if let Err(e) = (entry.send_frame)(frame) {
                    tracing::warn!(
                        target: "flezha::flusher",
                        "send_frame failed for partition {}: {}",
                        entry.partition_id,
                        e,
                    );
                    // Decrement seq so the next drain re-sends from the
                    // right point. v1 only logs and stops the batch
                    // here; the agent change wires retransmit.
                    entry.change_seq.fetch_sub(1, Ordering::Relaxed);
                    break;
                }
            }
        }
    }
}

// PATCH_POINT_FOR_RELAY_STATE_PERSISTENCE_AGENT
//
// The follow-up `relay-state-persistence-agent` change replaces the
// binding of `stub_send_frame` with the real NIF send-bytes path. Grep
// for this comment in `relay-state-persistence-agent` to find the
// callsite.
pub fn stub_send_frame(bytes: Vec<u8>) -> Result<(), SendError> {
    tracing::trace!(
        target: "flezha::flusher::stub",
        "would send {} bytes",
        bytes.len(),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestChange {
        key: Option<u8>,
        ts: u64,
        value: i64,
    }
    impl Change for TestChange {
        fn coalesce_key(&self) -> Option<CoalesceKey> {
            self.key.map(|k| CoalesceKey::from_hash(&k))
        }
    }

    #[test]
    fn coalesce_collapses_keyed_records_to_latest() {
        let buf: Vec<TestChange> = (0..50)
            .map(|i| TestChange {
                key: Some(7),
                ts: i,
                value: i as i64,
            })
            .collect();
        let out = coalesce(buf);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].ts, 49);
        assert_eq!(out[0].value, 49);
    }

    #[test]
    fn coalesce_preserves_no_key_records_verbatim() {
        let buf: Vec<TestChange> = (0..50)
            .map(|i| TestChange {
                key: None,
                ts: i,
                value: i as i64,
            })
            .collect();
        let out = coalesce(buf);
        assert_eq!(out.len(), 50);
        for (i, c) in out.iter().enumerate() {
            assert_eq!(c.ts, i as u64);
        }
    }

    #[test]
    fn coalesce_mixed_no_key_and_keyed() {
        let buf = vec![
            TestChange { key: None, ts: 1, value: 1 }, // A: kept
            TestChange { key: Some(1), ts: 2, value: 2 }, // B: collapsed
            TestChange { key: Some(1), ts: 3, value: 3 }, // C: kept
            TestChange { key: None, ts: 4, value: 4 }, // D: kept
            TestChange { key: Some(2), ts: 5, value: 5 }, // E: kept
        ];
        let out = coalesce(buf);
        let ts: Vec<u64> = out.iter().map(|c| c.ts).collect();
        assert_eq!(ts, vec![1, 3, 4, 5]);
    }

    #[test]
    fn frame_layout_is_partition_id_seq_header_len_body_crc() {
        let template = HeaderTemplate::new(0x010203, 1);
        let change = TestChange { key: None, ts: 0, value: 42 };
        let frame = build_partition_state_change_frame::<TestChange>(7, 99, &template, &change);
        // 4 + 8 + 16 + 4 + body + 4
        let body_bytes = postcard::to_allocvec(&change).unwrap();
        assert_eq!(frame.len(), 4 + 8 + 16 + 4 + body_bytes.len() + 4);
        assert_eq!(&frame[0..4], &7u32.to_be_bytes());
        assert_eq!(&frame[4..12], &99u64.to_be_bytes());
        // header[0] = sdk_lang Rust = 1
        assert_eq!(frame[12], 1);
        // body length field
        assert_eq!(
            &frame[28..32],
            &(body_bytes.len() as u32).to_be_bytes(),
        );
        let body_end = 32 + body_bytes.len();
        assert_eq!(&frame[32..body_end], &body_bytes[..]);
        assert_eq!(
            &frame[body_end..body_end + 4],
            &crc32fast::hash(&body_bytes).to_be_bytes(),
        );
    }
}
