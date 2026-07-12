//! Retry-then-park dispatcher semantics (`platform-dead-letter-queue`):
//! - transient failure recovers within the retry budget (no park)
//! - `OperationError::non_retryable` parks immediately
//! - a panicking operation follows the same policy as a returned error
//!   and the segment continues — never a silent drop

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use flezha_executor::channel::ChannelBuilder;
use flezha_executor::operation::OperationError;
use flezha_executor::segment::flusher::{SendError, SendFrameFn};
use flezha_executor::segment::park::{self, ParkBody};
use flezha_executor::segment::segment_state_sink::{install_global_sink, PARK_PAYLOAD_PAYLOAD_ID};
use flezha_executor::UnboundedOperative;

mod common;
use common::{dispatch_input, dummy_stream, setup, wait_until};

/// Serializes the tests that install the process-global sink.
static SINK_LOCK: Mutex<()> = Mutex::new(());

/// Install a recording sink whose park frames are auto-acked from a watcher
/// thread (the controller's fsync-then-ack, minus the controller). Returns
/// the collected `ParkBody`s.
fn install_auto_acking_sink() -> Arc<Mutex<Vec<ParkBody>>> {
    let parked: Arc<Mutex<Vec<ParkBody>>> = Arc::new(Mutex::new(Vec::new()));
    let parked_c = Arc::clone(&parked);
    let sink: SendFrameFn = Arc::new(move |payload_id: &str, frame: &[u8]| {
        if payload_id == PARK_PAYLOAD_PAYLOAD_ID {
            let body = decode_park_body(frame).map_err(SendError::Other)?;
            let park_id = body.park_id.clone();
            let mut seen = parked_c.lock().unwrap();
            if !seen.iter().any(|b| b.park_id == park_id) {
                seen.push(body);
            }
            drop(seen);
            // Ack from another thread: the dispatch thread is blocked
            // inside `park_blocking` while this sink call returns.
            std::thread::spawn(move || park::complete_ack(&park_id));
        }
        Ok(())
    });
    install_global_sink(sink);
    parked
}

/// Parse the outer segment frame and postcard-decode the `ParkBody`.
/// Layout: `[pid_len:u16][pid][seq:u64][header:16][body_len:u32][body][crc:u32]`.
fn decode_park_body(frame: &[u8]) -> Result<ParkBody, String> {
    let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    let body_len_off = 2 + pid_len + 8 + 16;
    let body_len = u32::from_be_bytes(
        frame[body_len_off..body_len_off + 4]
            .try_into()
            .map_err(|_| "short frame".to_string())?,
    ) as usize;
    let body = &frame[body_len_off + 4..body_len_off + 4 + body_len];
    postcard::from_bytes(body).map_err(|e| format!("park body decode: {e}"))
}

// ----- Transient failure recovers within the retry budget -----

static TRANSIENT_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
static TRANSIENT_OK: AtomicUsize = AtomicUsize::new(0);

test_operation!(TransientOperation, "transient_operation", String => String, (), |_input, _state, _ctx| {
    if TRANSIENT_ATTEMPTS.fetch_add(1, Ordering::SeqCst) < 2 {
        Err(OperationError::Custom("transient".into()))
    } else {
        TRANSIENT_OK.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
});

struct TransientOperative;
impl UnboundedOperative for TransientOperative {
    fn factory_id() -> &'static str {
        "transient_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input")
            .retry_backoff_ms(1)
            .into::<TransientOperation>();
    }
}

#[test]
fn transient_failure_recovers_within_retry_budget() {
    // Serializes against the parking tests: PARKED_TOTAL is process-global.
    let _g = SINK_LOCK.lock().unwrap();
    let retries_before = park::RETRIES_TOTAL.load(Ordering::SeqCst);
    let parked_before = park::PARKED_TOTAL.load(Ordering::SeqCst);
    let (handle, _instance) = setup::<TransientOperative>("transient-instance", 2);
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "transient-instance",
        "input",
        b"p".to_vec(),
        &stream,
    );

    assert!(
        wait_until(
            || TRANSIENT_OK.load(Ordering::SeqCst) == 1,
            Duration::from_secs(5)
        ),
        "third attempt should succeed"
    );
    assert_eq!(TRANSIENT_ATTEMPTS.load(Ordering::SeqCst), 3);
    assert_eq!(
        park::RETRIES_TOTAL.load(Ordering::SeqCst) - retries_before,
        2,
        "attempt telemetry records the two retries"
    );
    assert_eq!(
        park::PARKED_TOTAL.load(Ordering::SeqCst),
        parked_before,
        "no payload parked"
    );
}

// ----- Non-retryable error parks immediately -----

static NON_RETRYABLE_RUNS: AtomicUsize = AtomicUsize::new(0);
static NON_RETRYABLE_OK: AtomicUsize = AtomicUsize::new(0);

test_operation!(NonRetryableOperation, "non_retryable_operation", String => String, (), |input, _state, _ctx| {
    NON_RETRYABLE_RUNS.fetch_add(1, Ordering::SeqCst);
    if input == "poison" {
        Err(OperationError::non_retryable("bad record"))
    } else {
        NON_RETRYABLE_OK.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
});

struct NonRetryableOperative;
impl UnboundedOperative for NonRetryableOperative {
    fn factory_id() -> &'static str {
        "non_retryable_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input")
            .retry_backoff_ms(1)
            .into::<NonRetryableOperation>();
    }
}

#[test]
fn non_retryable_error_parks_immediately() {
    let _g = SINK_LOCK.lock().unwrap();
    let parked = install_auto_acking_sink();
    let (handle, _instance) = setup::<NonRetryableOperative>("nonretry-instance", 2);
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "nonretry-instance",
        "input",
        b"poison".to_vec(),
        &stream,
    );
    dispatch_input(
        &handle,
        "nonretry-instance",
        "input",
        b"healthy".to_vec(),
        &stream,
    );

    assert!(
        wait_until(
            || NON_RETRYABLE_OK.load(Ordering::SeqCst) == 1,
            Duration::from_secs(5)
        ),
        "segment must continue past the parked payload"
    );
    let entries = parked.lock().unwrap().clone();
    assert_eq!(entries.len(), 1, "exactly one park");
    assert_eq!(entries[0].entry.attempts, 1, "no further attempts");
    assert_eq!(entries[0].entry.channel, "input");
    assert_eq!(entries[0].entry.payload, Some(b"poison".to_vec()));
    assert!(entries[0].entry.error.contains("bad record"));
    assert_eq!(
        NON_RETRYABLE_RUNS.load(Ordering::SeqCst),
        2,
        "poison ran once, healthy ran once"
    );
}

// ----- Panic follows the same policy as a returned error -----

static PANIC_RUNS: AtomicUsize = AtomicUsize::new(0);
static PANIC_OK: AtomicUsize = AtomicUsize::new(0);

test_operation!(PanickyOperation, "panicky_operation", String => String, (), |input, _state, _ctx| {
    PANIC_RUNS.fetch_add(1, Ordering::SeqCst);
    if input == "poison" {
        panic!("kaboom");
    }
    PANIC_OK.fetch_add(1, Ordering::SeqCst);
    Ok(())
});

struct PanickyOperative;
impl UnboundedOperative for PanickyOperative {
    fn factory_id() -> &'static str {
        "panicky_operative"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("input")
            .retry_backoff_ms(1)
            .into::<PanickyOperation>();
    }
}

#[test]
fn panic_parity_exhausts_budget_then_parks_and_continues() {
    let _g = SINK_LOCK.lock().unwrap();
    let parked = install_auto_acking_sink();
    let (handle, _instance) = setup::<PanickyOperative>("panicky-instance", 2);
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "panicky-instance",
        "input",
        b"poison".to_vec(),
        &stream,
    );
    dispatch_input(
        &handle,
        "panicky-instance",
        "input",
        b"healthy".to_vec(),
        &stream,
    );

    assert!(
        wait_until(
            || PANIC_OK.load(Ordering::SeqCst) == 1,
            Duration::from_secs(5)
        ),
        "segment must continue past the parked payload — never a silent drop"
    );
    let entries = parked.lock().unwrap().clone();
    assert_eq!(
        entries.len(),
        1,
        "parked exactly as a returned error would be"
    );
    assert_eq!(
        entries[0].entry.attempts, 3,
        "panic is retryable by default"
    );
    assert!(entries[0].entry.error.contains("kaboom"));
    assert_eq!(entries[0].entry.payload, Some(b"poison".to_vec()));
    assert_eq!(
        PANIC_RUNS.load(Ordering::SeqCst),
        4,
        "poison ran 3 attempts, healthy ran once"
    );
}
