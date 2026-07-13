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
use serde::{Deserialize, Serialize};

use crate::operation::change::{Change, CoalesceKey};
use crate::segment::header::{HeaderTemplate, FORMAT_ID_EFFECT_RECORD};
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

/// One plugin output captured inside an effect record. Field order is the
/// wire contract (postcard is field-order-sensitive); `metadata` carries the
/// caller's user metadata as key-sorted pairs — the controller derives the
/// identity stamps (`flz.segment` / `outbox_seq` / epoch) at flush time, so
/// none ride here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputRecord {
    pub channel: String,
    pub key: String,
    pub metadata: Vec<(String, String)>,
    pub payload: Vec<u8>,
}

/// Dead-letter park carried inside an effect record (effectively-once
/// tier): the poisoned invocation's park entry rides the SAME atomic WAL
/// record as its consumed-offset advance, so a crash can never advance the
/// offset while losing the park. `payload` is `None` when the input type
/// is not byte-representable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DlqEntryBody {
    pub channel: String,
    pub error: String,
    pub attempts: u32,
    pub payload: Option<Vec<u8>>,
}

/// Wire body of a `segment_effect_record` frame (header `format_id = 2`):
/// one invocation's state change(s), plugin emits, consumed input offset,
/// and (for a parked poison payload) its dead-letter entry, persisted by
/// the controller as ONE WAL record under ONE fsync. `changes` entries are
/// the postcard `S::Change` bytes a plain change record would carry, so
/// restore folds them identically.
///
/// `event_time` is the invocation's declared clock value. For an
/// `EventTimeFrom` clock it is the extracted event time, which is what makes
/// the controller's event-time → seq → input-offset lookup a read rather than
/// a guess — the correction baseline rests on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRecordBody {
    pub changes: Vec<Vec<u8>>,
    pub outputs: Vec<OutputRecord>,
    pub consumed_offset: Option<u64>,
    pub dlq: Option<DlqEntryBody>,
    pub event_time: u64,
}

/// One invocation's buffered effect entry (effectively-once tier). Typed on
/// the Operation's Change; serialized into an [`EffectRecordBody`] at drain.
pub struct EffectEntry<S: crate::operation::Operation> {
    pub changes: Vec<S::Change>,
    pub outputs: Vec<OutputRecord>,
    pub consumed_offset: Option<u64>,
    pub dlq: Option<DlqEntryBody>,
    pub event_time: u64,
}

/// One drained wire frame plus the data-stream payload id it ships under
/// (`segment_state_change` for plain changes, `segment_effect_record` for
/// effect records).
pub struct FlushFrame {
    pub payload_id: &'static str,
    pub bytes: Vec<u8>,
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
    ) -> (usize, Vec<FlushFrame>);

    /// Whether the underlying buffer is empty. The flusher uses this to
    /// avoid building frames when a Segment has had no `state.apply`
    /// calls in the linger window. An open checkpoint interval does not
    /// count: its changes are not flushable on their own.
    fn is_empty(&self) -> bool;

    /// Drop an open checkpoint interval's accumulated changes. A restore
    /// re-seats the Segment on a past in which they were never applied, and
    /// the controller re-forwards the inputs that produced them.
    fn discard_staged(&mut self);

    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Concrete per-Operation buffer. The dispatcher downcasts the type-erased
/// `dyn FlushableBuffer` to `TypedBuffer<S>` to get a `&mut Vec<S::Change>`
/// for `StateHandle::new`. Exactly one of `inner` / `effects` is ever
/// populated: plain changes outside the effectively-once tier, per-invocation
/// effect entries inside it.
///
/// `staged` is neither: under checkpoint ack-granularity it holds the changes
/// an open interval has applied, which are not flushable yet — they become
/// durable only inside the effect record the closing `ctx.checkpoint()` ships,
/// together with the offset that acknowledges them. Draining them on their own
/// would put the state ahead of its offset, which is the tear the mode exists
/// to prevent.
pub struct TypedBuffer<S: crate::operation::Operation> {
    pub inner: Vec<S::Change>,
    pub staged: Vec<S::Change>,
    pub effects: Vec<EffectEntry<S>>,
}

impl<S: crate::operation::Operation> Default for TypedBuffer<S> {
    fn default() -> Self {
        Self {
            inner: Vec::new(),
            staged: Vec::new(),
            effects: Vec::new(),
        }
    }
}

impl<S: crate::operation::Operation> FlushableBuffer for TypedBuffer<S> {
    fn drain_to_frames(
        &mut self,
        segment_identity: &[u8],
        change_seq: &AtomicU64,
        header: &HeaderTemplate,
    ) -> (usize, Vec<FlushFrame>) {
        let mut frames: Vec<FlushFrame> = Vec::new();
        if !self.inner.is_empty() {
            let coalesced = coalesce(std::mem::take(&mut self.inner));
            frames.extend(coalesced.into_iter().map(|c| {
                let seq = change_seq.fetch_add(1, Ordering::Relaxed);
                FlushFrame {
                    payload_id: crate::segment::segment_state_sink::SEGMENT_STATE_CHANGE_PAYLOAD_ID,
                    bytes: build_segment_state_change_frame::<S::Change>(
                        segment_identity,
                        seq,
                        header,
                        &c,
                    ),
                }
            }));
        }
        // Effect records are never coalesced or merged: a coalesced-away
        // record would drop its outputs and consumed-offset.
        if !self.effects.is_empty() {
            frames.extend(std::mem::take(&mut self.effects).into_iter().map(|entry| {
                let seq = change_seq.fetch_add(1, Ordering::Relaxed);
                FlushFrame {
                    payload_id:
                        crate::segment::segment_state_sink::SEGMENT_EFFECT_RECORD_PAYLOAD_ID,
                    bytes: build_segment_effect_record_frame::<S>(
                        segment_identity,
                        seq,
                        header,
                        entry,
                    ),
                }
            }));
        }
        let count = frames.len();
        (count, frames)
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty() && self.effects.is_empty()
    }

    fn discard_staged(&mut self) {
        self.staged.clear();
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

/// Build one `segment_effect_record` frame (effectively-once tier): the
/// invocation's changes (each postcard-encoded as a plain change body would
/// be), outputs, and consumed-offset as one postcard `EffectRecordBody`,
/// stamped with `format_id = 2` so mixed WALs stay readable.
pub fn build_segment_effect_record_frame<S: crate::operation::Operation>(
    segment_identity: &[u8],
    seq: u64,
    header: &HeaderTemplate,
    entry: EffectEntry<S>,
) -> Vec<u8> {
    let changes: Vec<Vec<u8>> = entry
        .changes
        .iter()
        .map(|c| {
            postcard::to_allocvec(c)
                .expect("postcard serialization of Change failed; Change must be Serialize")
        })
        .collect();
    let body = EffectRecordBody {
        changes,
        outputs: entry.outputs,
        consumed_offset: entry.consumed_offset,
        dlq: entry.dlq,
        event_time: entry.event_time,
    };
    let body_bytes = postcard::to_allocvec(&body)
        .expect("postcard serialization of EffectRecordBody cannot fail");
    let effect_header = HeaderTemplate {
        format_id: FORMAT_ID_EFFECT_RECORD,
        ..*header
    };
    let header_bytes = effect_header.stamp(&body_bytes);
    build_segment_frame(segment_identity, seq, &header_bytes, &body_bytes)
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
        crate::segment::credit::global().acquire(frame.bytes.len());
        crate::segment::segment_state_sink::send(frame.payload_id, frame.bytes);
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

    #[derive(Default)]
    struct EffOp;
    impl crate::operation::Operation for EffOp {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = TestChange;
        type Clock = crate::operation::WallClock;
        type Config = ();
        fn name() -> &'static str {
            "effect_test_op"
        }
        fn init_state(_: Option<&str>, _: Option<crate::segment::restore::RestoreInitPayload<'_>>) {
        }
        fn run(
            &mut self,
            _: String,
            _: crate::operation::state_handle::StateHandle<'_, Self>,
            _: &mut crate::operation::OperationCtx<'_>,
        ) -> Result<(), crate::operation::OperationError> {
            Ok(())
        }
    }

    #[test]
    fn effect_record_frame_round_trips_and_marks_format_2() {
        let change = TestChange {
            key: None,
            ts: 7,
            value: 42,
        };
        let entry = EffectEntry::<EffOp> {
            event_time: 0,
            changes: vec![change.clone()],
            outputs: vec![OutputRecord {
                channel: "out".into(),
                key: "BTC/1m".into(),
                metadata: vec![("a".into(), "b".into())],
                payload: b"candle".to_vec(),
            }],
            consumed_offset: Some(4711),
            dlq: None,
        };
        let template = HeaderTemplate::new(0x010203, 1);
        let identity = encode_segment_identity("Op", "stage", "k");
        let frame = build_segment_effect_record_frame::<EffOp>(&identity, 9, &template, entry);

        // Same outer layout as a change frame; header format_id = 2.
        let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
        let header_off = 2 + pid_len + 8;
        assert_eq!(frame[header_off + 9], FORMAT_ID_EFFECT_RECORD);

        let body_len_off = header_off + 16;
        let body_len =
            u32::from_be_bytes(frame[body_len_off..body_len_off + 4].try_into().unwrap()) as usize;
        let body = &frame[body_len_off + 4..body_len_off + 4 + body_len];
        let decoded: EffectRecordBody = postcard::from_bytes(body).unwrap();

        assert_eq!(decoded.consumed_offset, Some(4711));
        assert_eq!(decoded.outputs.len(), 1);
        assert_eq!(decoded.outputs[0].channel, "out");
        assert_eq!(decoded.outputs[0].key, "BTC/1m");
        assert_eq!(decoded.outputs[0].payload, b"candle".to_vec());
        assert_eq!(decoded.changes.len(), 1);
        let folded: TestChange = postcard::from_bytes(&decoded.changes[0]).unwrap();
        assert_eq!(folded, change);
    }

    /// Cost-free-default guard (WAL path): a non-tier buffer drains to
    /// frames byte-identical to the pre-tier layout, under the pre-tier
    /// payload id — the tier machinery adds zero bytes and zero work.
    #[test]
    fn non_tier_drain_frames_are_byte_identical_plain_change_frames() {
        let change = TestChange {
            key: None,
            ts: 3,
            value: 9,
        };
        let mut buffer = TypedBuffer::<EffOp> {
            inner: vec![change.clone()],
            ..Default::default()
        };
        let seq = AtomicU64::new(5);
        let template = HeaderTemplate::new(0x010203, 1);
        let identity = encode_segment_identity("Op", "stage", "k");

        let (count, frames) = buffer.drain_to_frames(&identity, &seq, &template);

        assert_eq!(count, 1);
        assert_eq!(frames[0].payload_id, "segment_state_change");
        assert_eq!(
            frames[0].bytes,
            build_segment_state_change_frame::<TestChange>(&identity, 5, &template, &change)
        );
    }

    /// Pins the postcard bytes of `EffectRecordBody` that the controller's
    /// `Flezha.State.Wire.decode_effect_body/1` decodes — the two sides
    /// share this exact fixture (wire_test.exs), so an encoding change
    /// breaks loudly here.
    #[test]
    fn effect_record_body_postcard_bytes_are_the_cross_language_fixture() {
        let body = EffectRecordBody {
            changes: vec![vec![0xAA]],
            outputs: vec![OutputRecord {
                channel: "out".into(),
                key: "k".into(),
                metadata: vec![("a".into(), "b".into())],
                payload: vec![1, 2],
            }],
            consumed_offset: Some(4711),
            dlq: None,
            event_time: 1_700_000_000_123,
        };
        let bytes = postcard::to_allocvec(&body).unwrap();
        assert_eq!(
            bytes,
            vec![
                0x01, 0x01, 0xAA, // changes: 1 × [0xAA]
                0x01, 0x03, b'o', b'u', b't', 0x01,
                b'k', // outputs: 1 × channel "out", key "k"
                0x01, 0x01, b'a', 0x01, b'b', // metadata: 1 × ("a","b")
                0x02, 0x01, 0x02, // payload [1,2]
                0x01, 0xE7, 0x24, // Some(4711)
                0x00, // dlq: None
                0xFB, 0xD0, 0x95, 0xFF, 0xBC, 0x31, // event_time: 1700000000123
            ]
        );
    }

    #[test]
    fn effect_records_are_never_coalesced_at_drain() {
        // Two entries whose changes share a coalesce key AND carry outputs:
        // both must ship — a coalesced-away record would drop its outputs.
        let mk = |ts: u64| EffectEntry::<EffOp> {
            event_time: ts,
            changes: vec![TestChange {
                key: Some(7),
                ts,
                value: ts as i64,
            }],
            outputs: vec![OutputRecord {
                channel: "out".into(),
                key: format!("k{ts}"),
                metadata: vec![],
                payload: vec![1],
            }],
            consumed_offset: Some(ts),
            dlq: None,
        };
        let mut buffer = TypedBuffer::<EffOp> {
            effects: vec![mk(1), mk(2)],
            ..Default::default()
        };

        let seq = AtomicU64::new(0);
        let template = HeaderTemplate::new(0x010203, 1);
        let identity = encode_segment_identity("Op", "stage", "k");
        let (count, frames) = buffer.drain_to_frames(&identity, &seq, &template);

        assert_eq!(count, 2, "output-bearing records must survive the drain");
        assert!(frames.iter().all(|f| f.payload_id
            == crate::segment::segment_state_sink::SEGMENT_EFFECT_RECORD_PAYLOAD_ID));
        assert!(buffer.is_empty());
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
