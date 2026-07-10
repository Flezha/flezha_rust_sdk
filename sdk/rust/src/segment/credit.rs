//! Sender-side credit window for the executor → controller state stream.
//!
//! The controller offers a byte window via the `CreditWindowInit` control
//! command; [`arm`] adopts it and the ack rides back as the command's
//! `CommandResponse`. From then on every `segment_state_change` frame
//! [`acquire`]s its wire size before the sink write, and `CreditGrant`
//! frames [`grant`] bytes back as the controller fsyncs. An exhausted
//! window parks the sending thread (the flusher or a dispatch worker mid
//! flush-before-emit) — park, never drop.
//!
//! Guardrails (2026-06-10 BlockParker lesson):
//!  * the park lives in the data send path only; `arm`/`grant` run on the
//!    control coordinator and never block, so heartbeats and grants keep
//!    flowing while senders are parked;
//!  * the park is a bounded `Condvar` wait loop (50 ms recheck), so a lost
//!    grant degrades to a retry loop with warnings, never a silent freeze;
//!  * [`CREDIT_PARKED`] advances on every wait iteration, so a credit-parked
//!    executor classifies as fed-but-parked in the liveness self-report.
//!
//! A frame larger than the whole window is sent once the window is fully
//! replenished (single-frame overshoot); the controller mirrors this by
//! accepting any frame arriving on an empty un-fsynced buffer.
//!
//! Production uses one process-global window (module-level fns); the
//! [`CreditWindow`] type itself is instance-based so tests never touch the
//! global that live send paths flow through.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

const PARK_TIMEOUT: Duration = Duration::from_millis(50);
const WARN_AFTER: Duration = Duration::from_secs(5);

/// Process-global count of credit-park wait iterations. Shipped in every
/// `data_heartbeat` frame's metadata; advancing while `dispatches` is frozen
/// means the executor is parked on credit, not wedged.
pub static CREDIT_PARKED: AtomicU64 = AtomicU64::new(0);

pub fn credit_parked() -> u64 {
    CREDIT_PARKED.load(Ordering::Relaxed)
}

#[derive(Default)]
struct WindowState {
    /// Opaque credit-session id from the last adopted `CreditWindowInit`.
    /// `None` = legacy session (no credit accounting).
    session: Option<String>,
    window_bytes: u64,
    available: i64,
}

pub struct CreditWindow {
    state: Mutex<WindowState>,
    cond: Condvar,
}

impl Default for CreditWindow {
    fn default() -> Self {
        Self::new()
    }
}

impl CreditWindow {
    pub fn new() -> Self {
        CreditWindow {
            state: Mutex::new(WindowState::default()),
            cond: Condvar::new(),
        }
    }

    /// Adopt a controller-offered window, replacing any previous session.
    /// Runs on the control coordinator; never blocks.
    pub fn arm(&self, session: String, window_bytes: u64) {
        let mut st = self.state.lock().expect("credit window mutex poisoned");
        tracing::info!(
            target: "flezha::credit",
            "credit window armed: session={} window_bytes={}",
            session,
            window_bytes,
        );
        st.session = Some(session);
        st.window_bytes = window_bytes;
        st.available = window_bytes as i64;
        drop(st);
        self.cond.notify_all();
    }

    /// Drop the current credit session (reconnect / new `ConnectSocket`).
    /// Parked senders wake and proceed un-metered until the next `arm`.
    pub fn disarm(&self) {
        let mut st = self.state.lock().expect("credit window mutex poisoned");
        if st.session.is_some() {
            tracing::info!(target: "flezha::credit", "credit window disarmed (reconnect)");
        }
        *st = WindowState::default();
        drop(st);
        self.cond.notify_all();
    }

    /// Apply a `CreditGrant`. A grant whose session does not match the
    /// current one is stale (minted for a pre-reconnect session) and is
    /// discarded. Runs on the control coordinator; never blocks.
    pub fn grant(&self, session: &str, bytes: u64) {
        let mut st = self.state.lock().expect("credit window mutex poisoned");
        match st.session.as_deref() {
            Some(current) if current == session => {
                st.available = st
                    .available
                    .saturating_add(bytes as i64)
                    .min(st.window_bytes as i64);
                drop(st);
                self.cond.notify_all();
            }
            _ => {
                tracing::debug!(
                    target: "flezha::credit",
                    "discarding stale credit grant for session {}",
                    session,
                );
            }
        }
    }

    /// Debit `frame_len` bytes from the window, parking the calling thread
    /// until enough credit is available. No-op (immediate return) for a
    /// legacy session. Must never be called from the control coordinator
    /// thread.
    pub fn acquire(&self, frame_len: usize) {
        let need = frame_len as i64;
        let mut st = self.state.lock().expect("credit window mutex poisoned");
        let mut parked_since: Option<Instant> = None;
        let mut warned = false;
        loop {
            match st.session {
                None => return,
                Some(_) => {
                    // Full-window overshoot: a frame larger than the whole
                    // window sends when nothing is outstanding.
                    if st.available >= need || st.available >= st.window_bytes as i64 {
                        st.available -= need;
                        if parked_since.is_some() {
                            tracing::debug!(
                                target: "flezha::credit",
                                "credit park resumed after {:?}",
                                parked_since.map(|t| t.elapsed()).unwrap_or_default(),
                            );
                        }
                        return;
                    }

                    CREDIT_PARKED.fetch_add(1, Ordering::Relaxed);
                    let since = *parked_since.get_or_insert_with(Instant::now);
                    if !warned && since.elapsed() >= WARN_AFTER {
                        warned = true;
                        tracing::warn!(
                            target: "flezha::credit",
                            "sender parked on credit for {:?} (need {} bytes, {} available) — \
                             controller fsync stalled or grant lost",
                            since.elapsed(),
                            need,
                            st.available,
                        );
                    }
                    st = self
                        .cond
                        .wait_timeout(st, PARK_TIMEOUT)
                        .expect("credit window mutex poisoned")
                        .0;
                }
            }
        }
    }

    /// Current credit-session id, if armed. Test/diagnostic surface.
    pub fn current_session(&self) -> Option<String> {
        self.state
            .lock()
            .expect("credit window mutex poisoned")
            .session
            .clone()
    }

    /// Currently available bytes. Test/diagnostic surface.
    pub fn available_bytes(&self) -> i64 {
        self.state
            .lock()
            .expect("credit window mutex poisoned")
            .available
    }
}

// ---- process-global window (production send path) --------------------------

/// The one window the live send path flows through.
pub fn global() -> &'static CreditWindow {
    static WINDOW: OnceLock<CreditWindow> = OnceLock::new();
    WINDOW.get_or_init(CreditWindow::new)
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use super::*;

    // Each test uses its OWN CreditWindow instance — the process-global
    // window stays disarmed, so concurrently-running flusher/sink tests
    // whose send path passes through `credit::acquire` are never metered
    // by these tests' tiny windows.

    #[test]
    fn legacy_session_never_debits_or_parks() {
        let w = CreditWindow::new();
        w.acquire(1_000_000);
        assert_eq!(w.current_session(), None);
    }

    #[test]
    fn armed_window_debits_and_exhaustion_parks_until_grant() {
        let w = Arc::new(CreditWindow::new());
        w.arm("s1".to_string(), 100);
        w.acquire(60);
        assert_eq!(w.available_bytes(), 40);

        let parked_before = credit_parked();
        let (tx, rx) = mpsc::channel();
        let sender = {
            let w = Arc::clone(&w);
            thread::spawn(move || {
                w.acquire(60); // only 40 available -> parks
                tx.send(()).unwrap();
            })
        };

        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "sender must park while credit is insufficient"
        );
        assert!(credit_parked() > parked_before, "park counter must advance");

        w.grant("s1", 60);
        rx.recv_timeout(Duration::from_secs(2))
            .expect("grant must unpark the sender");
        sender.join().unwrap();
        assert_eq!(w.available_bytes(), 40 - 60 + 60);
    }

    #[test]
    fn stale_grant_is_discarded_and_reconnect_resets() {
        let w = CreditWindow::new();
        w.arm("s1".to_string(), 100);
        w.acquire(100);
        w.grant("s-old", 100);
        assert_eq!(w.available_bytes(), 0, "stale-session grant must not apply");

        // Re-attach: a fresh handshake resets to a full window.
        w.arm("s2".to_string(), 100);
        assert_eq!(w.available_bytes(), 100);
        w.grant("s1", 50);
        assert_eq!(
            w.available_bytes(),
            100,
            "grant for the dead session is void"
        );
    }

    #[test]
    fn oversized_frame_sends_on_full_window() {
        let w = Arc::new(CreditWindow::new());
        w.arm("s1".to_string(), 100);
        w.acquire(250); // larger than the window: allowed at full window
        assert_eq!(w.available_bytes(), -150);

        // Next frame parks until the window is fully replenished.
        let (tx, rx) = mpsc::channel();
        let sender = {
            let w = Arc::clone(&w);
            thread::spawn(move || {
                w.acquire(10);
                tx.send(()).unwrap();
            })
        };
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
        w.grant("s1", 250); // caps at window_bytes
        rx.recv_timeout(Duration::from_secs(2))
            .expect("replenished window must unpark");
        sender.join().unwrap();
        assert_eq!(w.available_bytes(), 90);
    }

    #[test]
    fn disarm_wakes_parked_senders() {
        let w = Arc::new(CreditWindow::new());
        w.arm("s1".to_string(), 10);
        w.acquire(10);

        let (tx, rx) = mpsc::channel();
        let sender = {
            let w = Arc::clone(&w);
            thread::spawn(move || {
                w.acquire(10);
                tx.send(()).unwrap();
            })
        };
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
        w.disarm();
        rx.recv_timeout(Duration::from_secs(2))
            .expect("disarm must release parked senders to the legacy path");
        sender.join().unwrap();
    }
}
