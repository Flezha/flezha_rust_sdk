//! Per-Segment flusher per `operative-state-persistence-rust/design.md`
//! Decision 2.
//!
//! Single OS thread per process, lazy-initialized via `OnceLock` on the
//! first `register_unbounded::<R>()` for a Operation with a non-`NoChange`
//! Change. The thread runs an infinite loop: sleep for `linger_ms`,
//! iterate registered Segments, drain non-empty buffers, build wire
//! frames, hand them to the per-Segment `send_frame` fn pointer.
//!
//! Frame layout (matches controller `Flezha.State.Wire.decode_frame/1`):
//!
//! ```text
//! [segment_id_len: u16 BE]
//! [segment_id_bytes]   // [rc_len:u8][rc][sh_len:u8][sh][pk_len:u16 BE][pk]
//! [seq: u64 BE]
//! [header: 16B]
//! [body_len: u32 BE]
//! [body]                 // change_bytes (or snapshot_bytes for compactions)
//! [crc32: u32 BE over body]
//! ```
//!
//! The structured `segment_id_bytes` carries `(operative_class, operation_id,
//! segment_key)` so the controller can route the record to the right
//! per-Segment WAL file without a side-table mapping. Header is the
//! 16-byte `HeaderTemplate::stamp` output (per Decision 8); the trailing
//! CRC32 covers the body so the controller can validate framing without
//! re-decoding the header.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use dashmap::DashMap;

use crate::operation::change::{Change, CoalesceKey};
use crate::segment::header::HeaderTemplate;
use crate::util::LockRecover;

/// Global identifier for a registered Segment entry. Assigned at
/// flusher-registration time; opaque to callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FlusherEntryId(u64);

/// Errors returned from the wire stub or its controller-wired replacement.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    #[error("transport down: {0}")]
    TransportDown(String),
    #[error("send failure: {0}")]
    Other(String),
}

/// Ship one already-built wire frame under a given data-stream payload-id. The
/// installed sink wraps the frame in the payloader envelope with that id, so the
/// same reliable sink carries `segment_state_change` and the saga
/// `compensation_intent` / `compensation_done` records.
pub type SendFrameFn = Arc<dyn Fn(&str, &[u8]) -> Result<(), SendError> + Send + Sync + 'static>;

/// Per-Segment entry registered with the flusher.
struct FlusherEntry {
    segment_identity: Arc<Vec<u8>>,
    buffer: Arc<Mutex<Box<dyn FlushableBuffer>>>,
    persistence_enabled: bool,
    change_seq: Arc<AtomicU64>,
    header: HeaderTemplate,
}

/// Encode `(operative_class, operation_id, segment_key)` into the wire-format
/// `segment_id_bytes` consumed by the controller's `decode_segment_id`.
pub fn encode_segment_identity(
    operative_class: &str,
    operation_id: &str,
    segment_key: &str,
) -> Vec<u8> {
    let rc = operative_class.as_bytes();
    let sh = operation_id.as_bytes();
    let pk = segment_key.as_bytes();
    assert!(
        rc.len() <= 255 && sh.len() <= 255,
        "operative_class / operation_id ≤ 255 bytes"
    );
    assert!(pk.len() <= 0xFFFF, "segment_key ≤ 65535 bytes");
    let mut out = Vec::with_capacity(1 + rc.len() + 1 + sh.len() + 2 + pk.len());
    out.push(rc.len() as u8);
    out.extend_from_slice(rc);
    out.push(sh.len() as u8);
    out.extend_from_slice(sh);
    out.extend_from_slice(&(pk.len() as u16).to_be_bytes());
    out.extend_from_slice(pk);
    out
}

/// Type-erased per-Segment flush buffer. The dispatcher downcasts to
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
        segment_identity: &[u8],
        change_seq: &AtomicU64,
        header: &HeaderTemplate,
    ) -> (usize, Vec<Vec<u8>>);

    /// Whether the underlying buffer is empty. The flusher uses this to
    /// avoid building frames when a Segment has had no `state.apply`
    /// calls in the linger window.
    fn is_empty(&self) -> bool;

    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Concrete per-Operation buffer. The dispatcher downcasts the type-erased
/// `dyn FlushableBuffer` to `TypedBuffer<S>` to get a `&mut Vec<S::Change>`
/// for `StateHandle::new`.
pub struct TypedBuffer<S: crate::operation::Operation> {
    pub inner: Vec<S::Change>,
}

impl<S: crate::operation::Operation> Default for TypedBuffer<S> {
    fn default() -> Self {
        Self { inner: Vec::new() }
    }
}

impl<S: crate::operation::Operation> FlushableBuffer for TypedBuffer<S> {
    fn drain_to_frames(
        &mut self,
        segment_identity: &[u8],
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
                build_segment_state_change_frame::<S::Change>(segment_identity, seq, header, &c)
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

/// Build one wire frame matching `Flezha.State.Wire.decode_frame/1`.
///
/// `[segment_id_len: u16 BE][segment_id_bytes][seq: u64 BE][header: 16B]
///  [body_len: u32 BE][body][crc32: u32 BE over body]`
pub fn build_segment_state_change_frame<C: Change>(
    segment_identity: &[u8],
    seq: u64,
    header: &HeaderTemplate,
    change: &C,
) -> Vec<u8> {
    let change_bytes = postcard::to_allocvec(change)
        .expect("postcard serialization of Change failed; Change must be Serialize");
    let header_bytes = header.stamp(&change_bytes);
    build_segment_frame(segment_identity, seq, &header_bytes, &change_bytes)
}

/// Build one wire frame from already-serialized body bytes. Used by the
/// snapshot path (where the body is the postcard-encoded `Self::State`).
pub fn build_segment_frame(
    segment_identity: &[u8],
    seq: u64,
    header: &[u8; 16],
    body: &[u8],
) -> Vec<u8> {
    let crc = crc32fast::hash(body);
    let pid_len = segment_identity.len();
    let mut frame = Vec::with_capacity(2 + pid_len + 8 + 16 + 4 + body.len() + 4);
    frame.extend_from_slice(&(pid_len as u16).to_be_bytes());
    frame.extend_from_slice(segment_identity);
    frame.extend_from_slice(&seq.to_be_bytes());
    frame.extend_from_slice(header);
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(body);
    frame.extend_from_slice(&crc.to_be_bytes());
    frame
}

/// Single per-process flusher. Spawns one OS thread named `flezha-flusher`
/// on first registration of a non-`NoChange` Segment. Programs without
/// any non-`NoChange` Operations never reach `instance()`, so no thread is
/// spawned.
pub struct SegmentFlusher {
    entries: Arc<DashMap<FlusherEntryId, FlusherEntry>>,
    next_entry_id: AtomicU64,
    shutdown: Arc<AtomicBool>,
    linger_ms: u64,
}

static FLUSHER: OnceLock<Arc<SegmentFlusher>> = OnceLock::new();

const ENV_LINGER_MS: &str = "FLEZHA_FLUSH_LINGER_MS";
const DEFAULT_LINGER_MS: u64 = 5;

impl SegmentFlusher {
    /// Lazily-initialized per-process flusher. Spawns the
    /// `flezha-flusher` OS thread on first call.
    pub fn instance() -> Arc<SegmentFlusher> {
        FLUSHER
            .get_or_init(|| {
                let linger_ms = crate::util::env_parse(ENV_LINGER_MS, DEFAULT_LINGER_MS);
                let flusher = Arc::new(SegmentFlusher {
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

    /// Drop a Segment's flusher entry. Called when a dynamic-mode
    /// Segment is evicted by the reaper so its (now-orphaned) flush
    /// buffer stops being drained every linger tick. Idempotent — a
    /// missing id is a no-op.
    pub fn deregister(&self, id: FlusherEntryId) {
        self.entries.remove(&id);
    }

    /// Register a Segment with the flusher. Returns an opaque id used
    /// to deregister at Segment teardown (dynamic-mode eviction calls
    /// [`Self::deregister`]; static Segments live for the process).
    pub fn register(
        &self,
        segment_identity: Arc<Vec<u8>>,
        buffer: Arc<Mutex<Box<dyn FlushableBuffer>>>,
        persistence_enabled: bool,
        change_seq: Arc<AtomicU64>,
        header: HeaderTemplate,
    ) -> FlusherEntryId {
        let id = FlusherEntryId(self.next_entry_id.fetch_add(1, Ordering::Relaxed));
        self.entries.insert(
            id,
            FlusherEntry {
                segment_identity,
                buffer,
                persistence_enabled,
                change_seq,
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
            let mut guard = entry.buffer.lock_recover();
            flush_one(
                &mut **guard,
                &entry.segment_identity,
                &entry.change_seq,
                &entry.header,
                entry.persistence_enabled,
            );
        }
    }
}

/// Drain ONE segment's buffer to frames and ship them through the live
/// global sink. Shared between the background `drain_pass` (one call per
/// registered entry per linger tick) and the synchronous flush-before-emit
/// path on the dispatch thread. The caller holds the buffer lock; this
/// function does not re-lock, so it cannot self-deadlock against the
/// singleton flusher thread.
///
/// The sink resolves the current secondary stream each call (so a
/// reconnect's freshly-wired stream is used without re-registering this
/// segment) and buffers frames in its reconnect ring when the stream is
/// down. Frames carry seq values fetched from the same `change_seq`, so
/// concurrent background + inline drains stay gap-free and never duplicate
/// a record (each change is moved out of the buffer exactly once).
pub fn flush_one(
    buffer: &mut dyn FlushableBuffer,
    segment_identity: &[u8],
    change_seq: &AtomicU64,
    header: &HeaderTemplate,
    persistence_enabled: bool,
) {
    if buffer.is_empty() {
        return;
    }
    let (_count, frames) = buffer.drain_to_frames(segment_identity, change_seq, header);
    if !persistence_enabled {
        tracing::trace!(
            target: "flezha::flusher",
            "drain-and-discard {} frames (persistence_enabled = false)",
            frames.len(),
        );
        return;
    }
    for frame in frames {
        // Credit debit-on-send. Parks here (under the caller's buffer lock,
        // which is what keeps per-Segment seq order across the background and
        // inline drain paths) when the window is exhausted; a producer pushing
        // into this buffer then blocks on the lock — the emit-path park.
        crate::segment::credit::global().acquire(frame.len());
        crate::segment::segment_state_sink::send(
            crate::segment::segment_state_sink::SEGMENT_STATE_CHANGE_PAYLOAD_ID,
            frame,
        );
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

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
            TestChange {
                key: None,
                ts: 1,
                value: 1,
            }, // A: kept
            TestChange {
                key: Some(1),
                ts: 2,
                value: 2,
            }, // B: collapsed
            TestChange {
                key: Some(1),
                ts: 3,
                value: 3,
            }, // C: kept
            TestChange {
                key: None,
                ts: 4,
                value: 4,
            }, // D: kept
            TestChange {
                key: Some(2),
                ts: 5,
                value: 5,
            }, // E: kept
        ];
        let out = coalesce(buf);
        let ts: Vec<u64> = out.iter().map(|c| c.ts).collect();
        assert_eq!(ts, vec![1, 3, 4, 5]);
    }

    #[test]
    fn frame_layout_matches_controller_decode_format() {
        let template = HeaderTemplate::new(0x010203, 1);
        let change = TestChange {
            key: None,
            ts: 0,
            value: 42,
        };
        let identity = encode_segment_identity("MarketOperative", "OhlcOperation", "M1");
        let frame =
            build_segment_state_change_frame::<TestChange>(&identity, 99, &template, &change);

        let body_bytes = postcard::to_allocvec(&change).unwrap();

        // Layout: [pid_len:u16][pid][seq:u64][header:16][body_len:u32][body][crc32:u32]
        let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
        assert_eq!(pid_len, identity.len());
        assert_eq!(&frame[2..2 + pid_len], identity.as_slice());

        let seq_off = 2 + pid_len;
        let header_off = seq_off + 8;
        let body_len_off = header_off + 16;
        let body_off = body_len_off + 4;

        assert_eq!(&frame[seq_off..seq_off + 8], &99u64.to_be_bytes());
        assert_eq!(frame[header_off], 1); // sdk_lang Rust = 1
        assert_eq!(
            &frame[body_len_off..body_len_off + 4],
            &(body_bytes.len() as u32).to_be_bytes(),
        );
        assert_eq!(
            &frame[body_off..body_off + body_bytes.len()],
            body_bytes.as_slice()
        );
        let crc_off = body_off + body_bytes.len();
        assert_eq!(
            &frame[crc_off..crc_off + 4],
            &crc32fast::hash(&body_bytes).to_be_bytes(),
        );
    }

    #[test]
    fn encode_segment_identity_round_trips_three_fields() {
        let id = encode_segment_identity("MarketOperative", "OhlcOperation", "M1");
        // [oc_len:u8 = 15][oc][op_len:u8 = 13][op][sk_len:u16 BE = 2][sk]
        assert_eq!(id[0], 15);
        assert_eq!(&id[1..16], b"MarketOperative");
        assert_eq!(id[16], 13);
        assert_eq!(&id[17..30], b"OhlcOperation");
        assert_eq!(&id[30..32], &2u16.to_be_bytes());
        assert_eq!(&id[32..34], b"M1");
    }
}
