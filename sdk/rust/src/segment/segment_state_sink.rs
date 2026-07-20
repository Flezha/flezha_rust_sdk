//! Global send path for the per-Segment flusher.
//!
//! At SDK boot — and again after every reconnect, when the controller
//! re-issues `ConnectSocket` — the streams-connect path calls
//! [`install_global_sink`] with a closure wrapping one of the secondary
//! `FlezhaStream`s. The flusher ships every `segment_state_change` /
//! `segment_state_snapshot` frame through [`send`], which always
//! resolves the *live* sink (so a reconnect's freshly-wired stream is
//! picked up without re-registering segments).
//!
//! Two reliability behaviours live here:
//!
//!   * **Reconnect ring** — while no sink is installed (reconnect window)
//!     or a write fails, frames are held in a bounded ring (default
//!     1024, drop-oldest). [`install_global_sink`] replays the ring
//!     through the new sink before returning, so state changes produced
//!     mid-reconnect are not silently lost.
//!   * **Broken-pipe flood detection** — a sustained run of write
//!     failures (default ≥ 10 within 5 s) means the data stream is dead
//!     while the control channel still looks alive; we ask the
//!     coordinator to drive a full control reconnect (which re-runs the
//!     handshake and rebuilds the data streams).

use std::collections::VecDeque;
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use flz_transport::transport::FlezhaStream;

use crate::segment::flusher::SendFrameFn;

/// Data-stream payload-id for a per-Segment state Change record
/// (SDK → controller). The reliable sink stamps this id on every flusher frame.
pub const SEGMENT_STATE_CHANGE_PAYLOAD_ID: &str = "segment_state_change";

/// Data-stream payload-id for an atomic effect record (effectively-once
/// tier, SDK → controller): one invocation's state change(s) + plugin
/// outputs + consumed input offset, same frame layout as
/// `segment_state_change` with a `format_id = 2` header and an
/// `EffectRecordBody` body. Rides the reliable sink so outputs are never
/// on the primary stream in tier mode.
pub const SEGMENT_EFFECT_RECORD_PAYLOAD_ID: &str = "segment_effect_record";

/// Data-stream payload-id for the dynamic-mode hard-delete RPC
/// (SDK → controller). The body is the structured `segment_id` bytes;
/// the controller routes it to the per-Segment file lifecycle.
pub const DELETE_SEGMENT_STATE_PAYLOAD_ID: &str = "delete_segment_state";

/// Data-stream payload-ids for the bounded-operative saga compensation
/// change-log (SDK → controller), same frame layout as `segment_state_change`.
/// `compensation_intent` records an operation's completion; `compensation_done`
/// records a compensation hook's completion. Both ride the reliable sink (with
/// its reconnect ring) so a marker survives a reconnect before a crash.
pub const COMPENSATION_INTENT_PAYLOAD_ID: &str = "compensation_intent";
pub const COMPENSATION_DONE_PAYLOAD_ID: &str = "compensation_done";

/// Data-stream payload-id for a dead-letter park (SDK → controller,
/// tier-off sessions): a poison payload whose retry budget is exhausted,
/// shipped with its error context so the controller can append it to the
/// durable DLQ store. Same outer frame layout as `segment_state_change`
/// with a postcard `ParkBody` body; the controller answers with a
/// `ParkPayloadAck` control command once the entry is fsynced.
pub const PARK_PAYLOAD_PAYLOAD_ID: &str = "park_payload";

/// Data-stream payload-id for a per-Segment state snapshot (compaction,
/// SDK → controller): the postcard-encoded `S::State` captured at a
/// `change_seq` the Segment has reached, same frame layout as
/// `segment_state_change`. The controller writes the snapshot file, truncates
/// the WAL up to that seq, and clears the in-flight compaction. Rides the same
/// reliable sink so a snapshot produced mid-reconnect is not lost.
pub const SEGMENT_STATE_SNAPSHOT_PAYLOAD_ID: &str = "segment_state_snapshot";

const ENV_RING_CAPACITY: &str = "FLEZHA_RECONNECT_RING_CAPACITY";
const DEFAULT_RING_CAPACITY: usize = 1024;
const ENV_FLOOD_THRESHOLD: &str = "FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD";
const DEFAULT_FLOOD_THRESHOLD: usize = 10;
const ENV_FLOOD_WINDOW_MS: &str = "FLEZHA_BROKEN_PIPE_WINDOW_MS";
const DEFAULT_FLOOD_WINDOW_MS: u64 = 5_000;

const RECONNECT_REASON: &str = "broken_pipe_flood";

struct SinkState {
    sink: Option<SendFrameFn>,
    /// `(payload_id, frame)` pairs produced while no sink was installed or a
    /// write failed. Bounded; oldest is dropped on overflow. The payload-id is
    /// retained so a replayed frame is re-wrapped under its own id
    /// (`segment_state_change` vs a `compensation_*` record).
    ring: VecDeque<(String, Vec<u8>)>,
    /// Timestamps of recent write failures, used for flood detection.
    broken_pipe_at: VecDeque<Instant>,
    /// Set once a flood reconnect has been requested; cleared on the next
    /// sink install so we ask for exactly one reconnect per dead stream.
    reconnect_requested: bool,
}

impl SinkState {
    const fn new() -> Self {
        SinkState {
            sink: None,
            ring: VecDeque::new(),
            broken_pipe_at: VecDeque::new(),
            reconnect_requested: false,
        }
    }

    fn push_ring(&mut self, payload_id: &str, frame: Vec<u8>) {
        if self.ring.len() >= ring_capacity() {
            self.ring.pop_front();
        }
        self.ring.push_back((payload_id.to_string(), frame));
    }

    /// Record a write failure; returns `true` when the caller should drive
    /// a control reconnect (flood threshold crossed, not yet requested).
    fn note_broken_pipe(&mut self, now: Instant) -> bool {
        let window = Duration::from_millis(flood_window_ms());
        while let Some(&front) = self.broken_pipe_at.front() {
            if now.duration_since(front) > window {
                self.broken_pipe_at.pop_front();
            } else {
                break;
            }
        }
        self.broken_pipe_at.push_back(now);
        if !self.reconnect_requested && self.broken_pipe_at.len() >= flood_threshold() {
            self.reconnect_requested = true;
            self.broken_pipe_at.clear();
            true
        } else {
            false
        }
    }
}

static STATE: Mutex<SinkState> = Mutex::new(SinkState::new());

/// Live secondary stream, mirrored from the sink install. Used by the
/// reaper's fire-and-forget `delete_segment_state` send — no reconnect
/// ring, no flood detection; a failed write is dropped.
static SECONDARY_STREAM: Mutex<Option<Arc<FlezhaStream>>> = Mutex::new(None);

/// Set/clear the secondary stream used by [`send_oneshot`]. Called from the
/// streams-connect path alongside [`install_global_sink`].
pub fn set_secondary_stream(stream: Option<Arc<FlezhaStream>>) {
    *SECONDARY_STREAM
        .lock()
        .expect("secondary stream lock poisoned") = stream;
}

/// Best-effort one-shot write of a pre-built frame to the current secondary
/// stream. `false` if no stream is installed or the write fails (no retry).
pub fn send_oneshot(frame: &[u8]) -> bool {
    match SECONDARY_STREAM
        .lock()
        .expect("secondary stream lock poisoned")
        .as_ref()
    {
        Some(stream) => (&**stream).write_all(frame).is_ok(),
        None => false,
    }
}

fn ring_capacity() -> usize {
    crate::util::env_parse_positive(ENV_RING_CAPACITY, DEFAULT_RING_CAPACITY)
}

fn flood_threshold() -> usize {
    crate::util::env_parse_positive(ENV_FLOOD_THRESHOLD, DEFAULT_FLOOD_THRESHOLD)
}

fn flood_window_ms() -> u64 {
    crate::util::env_parse(ENV_FLOOD_WINDOW_MS, DEFAULT_FLOOD_WINDOW_MS)
}

/// Whether a global sink has ever been installed. `false` only for
/// transport-less runs (unit tests, standalone benches); the install is
/// never undone, so a reconnect window still reports `true`.
pub fn sink_installed() -> bool {
    STATE
        .lock()
        .expect("segment_state_sink lock poisoned")
        .sink
        .is_some()
}

/// Ship one frame to the controller. Resolves the live sink each call. On
/// a write failure (or while no sink is installed) the frame is buffered
/// in the reconnect ring; a sustained failure run asks the coordinator to
/// reconnect. Never blocks the flusher on a dead stream.
pub fn send(payload_id: &str, frame: Vec<u8>) {
    let should_reconnect = {
        let mut st = STATE.lock().expect("segment_state_sink lock poisoned");
        let outcome = st.sink.as_ref().map(|f| f(payload_id, &frame));
        match outcome {
            Some(Ok(())) => false,
            Some(Err(ref e)) => {
                if st.broken_pipe_at.is_empty() {
                    tracing::warn!(
                        target: "flezha::transport",
                        ring_len = st.ring.len() + 1,
                        "outbound segment-state write failed; buffering into reconnect ring: {}",
                        e
                    );
                }
                st.push_ring(payload_id, frame);
                st.note_broken_pipe(Instant::now())
            }
            None => {
                // Reconnect window: hold the frame, but don't count it as a
                // broken pipe — the stream is intentionally absent.
                st.push_ring(payload_id, frame);
                false
            }
        }
    };
    if should_reconnect {
        tracing::warn!(
            target: "flezha::transport",
            reason = RECONNECT_REASON,
            "outbound write-failure flood crossed threshold; requesting control reconnect"
        );
        crate::runner::request_reconnect(RECONNECT_REASON);
    }
}

/// Install (or replace) the global sink, then replay any frames buffered
/// during the reconnect window through it. Called once after the
/// secondary streams are up — at boot and after every reconnect.
pub fn install_global_sink(send_fn: SendFrameFn) {
    let mut st = STATE.lock().expect("segment_state_sink lock poisoned");
    st.sink = Some(send_fn.clone());
    st.broken_pipe_at.clear();
    st.reconnect_requested = false;

    let pending: Vec<(String, Vec<u8>)> = st.ring.drain(..).collect();
    let mut it = pending.into_iter();
    for (pid, frame) in it.by_ref() {
        if send_fn(&pid, &frame).is_err() {
            // The new stream is already failing — re-buffer this frame and
            // everything after it; a later install replays again.
            st.push_ring(&pid, frame);
            for (rest_pid, rest) in it.by_ref() {
                st.push_ring(&rest_pid, rest);
            }
            break;
        }
    }
}

/// Ship one bounded-operative saga compensation marker (`compensation_intent`
/// or `compensation_done`) for a Segment through the reliable sink. The body is
/// empty — the record's information is the Segment identity plus the payload-id
/// — matching `Flezha.State.Wire.decode_frame/1` (crc32 over an empty body is 0).
pub fn send_compensation_record(payload_id: &str, segment_identity: &[u8]) {
    let frame = crate::segment::flusher::build_segment_frame(segment_identity, 0, &[0u8; 16], &[]);
    send(payload_id, frame);
}

/// Serializes every test that installs the process-global sink — the sink's own
/// tests plus the bounded driver test — so they never clobber each other's sink.
#[cfg(test)]
pub(crate) static SINK_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    use super::*;
    use crate::segment::flusher::SendError;

    fn reset_state() {
        let mut st = STATE.lock().unwrap();
        st.sink = None;
        st.ring.clear();
        st.broken_pipe_at.clear();
        st.reconnect_requested = false;
    }

    /// A sink that records every accepted frame; `fail` flips it to return
    /// `TransportDown` so the failure paths can be driven deterministically.
    fn recording_sink(fail: Arc<AtomicBool>) -> (SendFrameFn, Arc<StdMutex<Vec<Vec<u8>>>>) {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let seen_c = Arc::clone(&seen);
        let f: SendFrameFn = Arc::new(move |_payload_id: &str, frame: &[u8]| {
            if fail.load(Ordering::SeqCst) {
                Err(SendError::TransportDown("test".into()))
            } else {
                seen_c.lock().unwrap().push(frame.to_vec());
                Ok(())
            }
        });
        (f, seen)
    }

    const PID: &str = super::SEGMENT_STATE_CHANGE_PAYLOAD_ID;

    #[test]
    fn frames_with_no_sink_buffer_and_replay_on_install() {
        let _g = super::SINK_TEST_LOCK.lock().unwrap();
        reset_state();

        send(PID, b"a".to_vec());
        send(PID, b"b".to_vec());

        let (sink, seen) = recording_sink(Arc::new(AtomicBool::new(false)));
        install_global_sink(sink);

        assert_eq!(
            *seen.lock().unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()],
            "ring replays in order"
        );
        reset_state();
    }

    #[test]
    fn live_sink_sends_directly_without_buffering() {
        let _g = super::SINK_TEST_LOCK.lock().unwrap();
        reset_state();

        let (sink, seen) = recording_sink(Arc::new(AtomicBool::new(false)));
        install_global_sink(sink);
        send(PID, b"x".to_vec());

        assert_eq!(*seen.lock().unwrap(), vec![b"x".to_vec()]);
        assert!(
            STATE.lock().unwrap().ring.is_empty(),
            "no buffering on success"
        );
        reset_state();
    }

    #[test]
    fn failed_writes_buffer_then_replay_when_stream_recovers() {
        let _g = super::SINK_TEST_LOCK.lock().unwrap();
        reset_state();

        let fail = Arc::new(AtomicBool::new(true));
        let (sink, _seen) = recording_sink(Arc::clone(&fail));
        install_global_sink(sink);

        send(PID, b"1".to_vec());
        send(PID, b"2".to_vec());
        assert_eq!(STATE.lock().unwrap().ring.len(), 2);

        // Stream recovers; a fresh install replays the ring in order.
        let (sink2, seen2) = recording_sink(Arc::new(AtomicBool::new(false)));
        install_global_sink(sink2);

        assert_eq!(*seen2.lock().unwrap(), vec![b"1".to_vec(), b"2".to_vec()]);
        assert!(STATE.lock().unwrap().ring.is_empty());
        reset_state();
    }

    #[test]
    fn ring_drops_oldest_beyond_capacity() {
        let _g = super::SINK_TEST_LOCK.lock().unwrap();
        std::env::set_var(ENV_RING_CAPACITY, "3");
        reset_state();

        for i in 0..5u8 {
            send(PID, vec![i]); // no sink → ring
        }
        let ring: Vec<Vec<u8>> = STATE
            .lock()
            .unwrap()
            .ring
            .iter()
            .map(|(_pid, frame)| frame.clone())
            .collect();
        assert_eq!(
            ring,
            vec![vec![2u8], vec![3u8], vec![4u8]],
            "oldest dropped"
        );

        std::env::remove_var(ENV_RING_CAPACITY);
        reset_state();
    }

    #[test]
    fn broken_pipe_flood_crosses_threshold_once_per_window() {
        let _g = super::SINK_TEST_LOCK.lock().unwrap();
        std::env::set_var(ENV_FLOOD_THRESHOLD, "3");
        std::env::set_var(ENV_FLOOD_WINDOW_MS, "60000");
        reset_state();

        let mut st = STATE.lock().unwrap();
        let now = Instant::now();
        assert!(!st.note_broken_pipe(now));
        assert!(!st.note_broken_pipe(now));
        assert!(st.note_broken_pipe(now), "3rd failure crosses threshold");
        // Already requested: no second trigger until the next install resets.
        assert!(!st.note_broken_pipe(now));
        drop(st);

        std::env::remove_var(ENV_FLOOD_THRESHOLD);
        std::env::remove_var(ENV_FLOOD_WINDOW_MS);
        reset_state();
    }

    #[test]
    fn send_through_failing_sink_drives_flood_branch_without_trigger() {
        // Exercises send()'s flood path end-to-end: a failing sink past the
        // threshold returns `should_reconnect`, and the request_reconnect
        // call is a safe no-op when no coordinator trigger is installed.
        let _g = super::SINK_TEST_LOCK.lock().unwrap();
        std::env::set_var(ENV_FLOOD_THRESHOLD, "3");
        std::env::set_var(ENV_FLOOD_WINDOW_MS, "60000");
        reset_state();

        let (sink, _seen) = recording_sink(Arc::new(AtomicBool::new(true)));
        install_global_sink(sink);
        for i in 0..5u8 {
            send(PID, vec![i]); // every write fails → buffered + counted
        }
        // All five frames buffered (drop-oldest not reached at default cap).
        assert_eq!(STATE.lock().unwrap().ring.len(), 5);

        std::env::remove_var(ENV_FLOOD_THRESHOLD);
        std::env::remove_var(ENV_FLOOD_WINDOW_MS);
        reset_state();
    }

    #[test]
    fn stale_broken_pipes_age_out_of_window() {
        let _g = super::SINK_TEST_LOCK.lock().unwrap();
        std::env::set_var(ENV_FLOOD_THRESHOLD, "2");
        std::env::set_var(ENV_FLOOD_WINDOW_MS, "100");
        reset_state();

        let mut st = STATE.lock().unwrap();
        let t0 = Instant::now();
        assert!(!st.note_broken_pipe(t0));
        // A failure past the window evicts t0, so this counts as the 1st.
        let later = t0 + Duration::from_millis(200);
        assert!(
            !st.note_broken_pipe(later),
            "stale failure aged out, no flood"
        );
        drop(st);

        std::env::remove_var(ENV_FLOOD_THRESHOLD);
        std::env::remove_var(ENV_FLOOD_WINDOW_MS);
        reset_state();
    }
}
