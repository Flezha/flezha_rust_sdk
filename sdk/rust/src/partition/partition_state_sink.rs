//! Global send path for the per-Partition flusher.
//!
//! At SDK boot — and again after every reconnect, when the controller
//! re-issues `ConnectSocket` — the streams-connect path calls
//! [`install_global_sink`] with a closure wrapping one of the secondary
//! `FlezhaStream`s. The flusher ships every `partition_state_change` /
//! `partition_state_snapshot` frame through [`send`], which always
//! resolves the *live* sink (so a reconnect's freshly-wired stream is
//! picked up without re-registering partitions).
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

use crate::partition::flusher::SendFrameFn;

/// Data-stream payload-id for the dynamic-mode hard-delete RPC
/// (SDK → controller). The body is the structured `partition_id` bytes;
/// the controller routes it to the per-Partition file lifecycle.
pub const DELETE_PARTITION_STATE_PAYLOAD_ID: &str = "delete_partition_state";

const ENV_RING_CAPACITY: &str = "FLEZHA_RECONNECT_RING_CAPACITY";
const DEFAULT_RING_CAPACITY: usize = 1024;
const ENV_FLOOD_THRESHOLD: &str = "FLEZHA_BROKEN_PIPE_RECONNECT_THRESHOLD";
const DEFAULT_FLOOD_THRESHOLD: usize = 10;
const ENV_FLOOD_WINDOW_MS: &str = "FLEZHA_BROKEN_PIPE_WINDOW_MS";
const DEFAULT_FLOOD_WINDOW_MS: u64 = 5_000;

const RECONNECT_REASON: &str = "broken_pipe_flood";

struct SinkState {
    sink: Option<SendFrameFn>,
    /// Frames produced while no sink was installed or a write failed.
    /// Bounded; oldest is dropped on overflow.
    ring: VecDeque<Vec<u8>>,
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

    fn push_ring(&mut self, frame: Vec<u8>) {
        if self.ring.len() >= ring_capacity() {
            self.ring.pop_front();
        }
        self.ring.push_back(frame);
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
/// reaper's fire-and-forget `delete_partition_state` send — no reconnect
/// ring, no flood detection; a failed write is dropped.
static SECONDARY_STREAM: Mutex<Option<Arc<FlezhaStream>>> = Mutex::new(None);

/// Set/clear the secondary stream used by [`send_oneshot`]. Called from the
/// streams-connect path alongside [`install_global_sink`].
pub fn set_secondary_stream(stream: Option<Arc<FlezhaStream>>) {
    *SECONDARY_STREAM.lock().expect("secondary stream lock poisoned") = stream;
}

/// Best-effort one-shot write of a pre-built frame to the current secondary
/// stream. `false` if no stream is installed or the write fails (no retry).
pub fn send_oneshot(frame: &[u8]) -> bool {
    match SECONDARY_STREAM.lock().expect("secondary stream lock poisoned").as_ref() {
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

/// Ship one frame to the controller. Resolves the live sink each call. On
/// a write failure (or while no sink is installed) the frame is buffered
/// in the reconnect ring; a sustained failure run asks the coordinator to
/// reconnect. Never blocks the flusher on a dead stream.
pub fn send(frame: Vec<u8>) {
    let should_reconnect = {
        let mut st = STATE.lock().expect("partition_state_sink lock poisoned");
        let outcome = st.sink.as_ref().map(|f| f(&frame));
        match outcome {
            Some(Ok(())) => false,
            Some(Err(ref e)) => {
                if st.broken_pipe_at.is_empty() {
                    tracing::warn!(
                        target: "flezha::transport",
                        ring_len = st.ring.len() + 1,
                        "outbound partition-state write failed; buffering into reconnect ring: {}",
                        e
                    );
                }
                st.push_ring(frame);
                st.note_broken_pipe(Instant::now())
            }
            None => {
                // Reconnect window: hold the frame, but don't count it as a
                // broken pipe — the stream is intentionally absent.
                st.push_ring(frame);
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
        crate::executor::request_reconnect(RECONNECT_REASON);
    }
}

/// Install (or replace) the global sink, then replay any frames buffered
/// during the reconnect window through it. Called once after the
/// secondary streams are up — at boot and after every reconnect.
pub fn install_global_sink(send_fn: SendFrameFn) {
    let mut st = STATE.lock().expect("partition_state_sink lock poisoned");
    st.sink = Some(send_fn.clone());
    st.broken_pipe_at.clear();
    st.reconnect_requested = false;

    let pending: Vec<Vec<u8>> = st.ring.drain(..).collect();
    let mut it = pending.into_iter();
    for frame in it.by_ref() {
        if send_fn(&frame).is_err() {
            // The new stream is already failing — re-buffer this frame and
            // everything after it; a later install replays again.
            st.push_ring(frame);
            for rest in it.by_ref() {
                st.push_ring(rest);
            }
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    use super::*;
    use crate::partition::flusher::SendError;

    // STATE is process-global, so sink tests must not run concurrently.
    static TEST_LOCK: StdMutex<()> = StdMutex::new(());

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
        let f: SendFrameFn = Arc::new(move |frame: &[u8]| {
            if fail.load(Ordering::SeqCst) {
                Err(SendError::TransportDown("test".into()))
            } else {
                seen_c.lock().unwrap().push(frame.to_vec());
                Ok(())
            }
        });
        (f, seen)
    }

    #[test]
    fn frames_with_no_sink_buffer_and_replay_on_install() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_state();

        send(b"a".to_vec());
        send(b"b".to_vec());

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
        let _g = TEST_LOCK.lock().unwrap();
        reset_state();

        let (sink, seen) = recording_sink(Arc::new(AtomicBool::new(false)));
        install_global_sink(sink);
        send(b"x".to_vec());

        assert_eq!(*seen.lock().unwrap(), vec![b"x".to_vec()]);
        assert!(STATE.lock().unwrap().ring.is_empty(), "no buffering on success");
        reset_state();
    }

    #[test]
    fn failed_writes_buffer_then_replay_when_stream_recovers() {
        let _g = TEST_LOCK.lock().unwrap();
        reset_state();

        let fail = Arc::new(AtomicBool::new(true));
        let (sink, _seen) = recording_sink(Arc::clone(&fail));
        install_global_sink(sink);

        send(b"1".to_vec());
        send(b"2".to_vec());
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
        let _g = TEST_LOCK.lock().unwrap();
        std::env::set_var(ENV_RING_CAPACITY, "3");
        reset_state();

        for i in 0..5u8 {
            send(vec![i]); // no sink → ring
        }
        let ring: Vec<Vec<u8>> = STATE.lock().unwrap().ring.iter().cloned().collect();
        assert_eq!(ring, vec![vec![2u8], vec![3u8], vec![4u8]], "oldest dropped");

        std::env::remove_var(ENV_RING_CAPACITY);
        reset_state();
    }

    #[test]
    fn broken_pipe_flood_crosses_threshold_once_per_window() {
        let _g = TEST_LOCK.lock().unwrap();
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
        let _g = TEST_LOCK.lock().unwrap();
        std::env::set_var(ENV_FLOOD_THRESHOLD, "3");
        std::env::set_var(ENV_FLOOD_WINDOW_MS, "60000");
        reset_state();

        let (sink, _seen) = recording_sink(Arc::new(AtomicBool::new(true)));
        install_global_sink(sink);
        for i in 0..5u8 {
            send(vec![i]); // every write fails → buffered + counted
        }
        // All five frames buffered (drop-oldest not reached at default cap).
        assert_eq!(STATE.lock().unwrap().ring.len(), 5);

        std::env::remove_var(ENV_FLOOD_THRESHOLD);
        std::env::remove_var(ENV_FLOOD_WINDOW_MS);
        reset_state();
    }

    #[test]
    fn stale_broken_pipes_age_out_of_window() {
        let _g = TEST_LOCK.lock().unwrap();
        std::env::set_var(ENV_FLOOD_THRESHOLD, "2");
        std::env::set_var(ENV_FLOOD_WINDOW_MS, "100");
        reset_state();

        let mut st = STATE.lock().unwrap();
        let t0 = Instant::now();
        assert!(!st.note_broken_pipe(t0));
        // A failure past the window evicts t0, so this counts as the 1st.
        let later = t0 + Duration::from_millis(200);
        assert!(!st.note_broken_pipe(later), "stale failure aged out, no flood");
        drop(st);

        std::env::remove_var(ENV_FLOOD_THRESHOLD);
        std::env::remove_var(ENV_FLOOD_WINDOW_MS);
        reset_state();
    }
}
