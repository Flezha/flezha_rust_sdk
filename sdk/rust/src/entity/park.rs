//! Dead-letter park: retry-exhausted payloads ship to the controller's
//! durable DLQ store and the entity advances only once the park is
//! durable (`dead-letter-queue`).
//!
//! Tier-off sessions use this module's blocking wire path: a
//! `park_payload` data-stream frame (reliable sink, reconnect ring) plus a
//! condvar wait for the controller's `ParkPayloadAck` control command,
//! re-sending until acked — delay, never loss. Effectively-once sessions
//! never call it: their park rides the invocation's atomic effect record
//! (`EffectRecordBody::dlq`), whose durability the input-log replay
//! already guarantees.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::entity::flusher::build_entity_frame;
use crate::entity::state_sink::{self, PARK_PAYLOAD_PAYLOAD_ID};

/// Process-global count of retry attempts after a failed invocation
/// (attempt 2..n), shipped in `data_heartbeat` metadata like
/// `DISPATCHES_COMPLETED`.
pub static RETRIES_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Process-global count of payloads parked to the dead-letter queue.
pub static PARKED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Monotonic nonce making tier-off `park_id`s unique within a process.
static PARK_SEQ: AtomicU64 = AtomicU64::new(0);

/// Wire body of a `park_payload` frame (postcard, field order is the
/// contract): the ack correlation id, the tier's input-log offset when
/// present, and the same [`DlqEntryBody`] a tier-on park embeds in its
/// effect record — one park-record shape on every path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParkBody {
    /// SDK-minted correlation id echoed back in `ParkPayloadAck`; also the
    /// controller's dedup handle for ack-lost re-sends.
    pub park_id: String,
    /// Input-log offset when the effectively-once tier provides one.
    pub origin_offset: Option<u64>,
    pub entry: crate::entity::flusher::DlqEntryBody,
}

/// Ack registry: park_ids acked by the controller, plus the condvar the
/// parked dispatch thread waits on. Entries are removed as they are
/// consumed, so the set stays as small as the number of in-flight parks.
static ACKED: Mutex<Option<HashSet<String>>> = Mutex::new(None);
static ACKED_CV: Condvar = Condvar::new();

/// How long to wait for the ack before re-sending the park frame.
const ACK_RESEND_INTERVAL: Duration = Duration::from_secs(2);
/// Warn cadence while a park remains unacked (multiples of the resend
/// interval).
const WARN_EVERY_RESENDS: u64 = 5;

/// Mint a process-unique tier-off park id.
pub fn mint_park_id(entity_label: &str) -> String {
    format!(
        "{}#{}",
        entity_label,
        PARK_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// Record a controller ack for `park_id` and wake the parked dispatcher.
/// Called from the `ParkPayloadAck` control-command handler; unknown ids
/// are retained (the ack can race ahead of the waiter's first check).
pub fn complete_ack(park_id: &str) {
    let mut guard = ACKED.lock().expect("park ack registry poisoned");
    guard
        .get_or_insert_with(HashSet::new)
        .insert(park_id.to_string());
    ACKED_CV.notify_all();
}

/// Consume a pending ack for `park_id`; `true` exactly once per ack.
fn take_ack(park_id: &str) -> bool {
    let mut guard = ACKED.lock().expect("park ack registry poisoned");
    guard
        .as_mut()
        .map(|set| set.remove(park_id))
        .unwrap_or(false)
}

/// Recover the wire bytes of an input for the DLQ entry. Entry-channel
/// payloads are `Vec<u8>`/`String` and round-trip exactly; other input
/// types have no serialization contract and park without bytes.
pub fn extract_payload_bytes<T: std::any::Any>(input: &T) -> Option<Vec<u8>> {
    let any: &dyn std::any::Any = input;
    if let Some(bytes) = any.downcast_ref::<Vec<u8>>() {
        Some(bytes.clone())
    } else {
        any.downcast_ref::<String>().map(|s| s.as_bytes().to_vec())
    }
}

/// Ship a park frame and block until the controller acks it as durable
/// (tier-off path). Re-sends on the resend cadence forever — the entity
/// must not advance past an unparked poison payload. Transport-less runs
/// (no sink ever installed) cannot park; they log loudly and drop,
/// preserving the pre-DLQ standalone behaviour.
pub fn park_blocking(entity_identity: &[u8], body: &ParkBody) {
    if !state_sink::sink_installed() {
        eprintln!(
            "[flezha] DLQ park impossible without a controller transport; dropping poison \
             payload on channel `{}` after {} attempts: {}",
            body.entry.channel, body.entry.attempts, body.entry.error
        );
        return;
    }

    let body_bytes =
        postcard::to_allocvec(body).expect("postcard serialization of ParkBody cannot fail");
    let seq = PARK_SEQ.load(Ordering::Relaxed);
    let frame = build_entity_frame(entity_identity, seq, &[0u8; 16], &body_bytes);

    let mut resends: u64 = 0;
    loop {
        state_sink::send(PARK_PAYLOAD_PAYLOAD_ID, frame.clone());
        let deadline = std::time::Instant::now() + ACK_RESEND_INTERVAL;
        loop {
            if take_ack(&body.park_id) {
                return;
            }
            let guard = ACKED.lock().expect("park ack registry poisoned");
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (_guard, _timeout) = ACKED_CV
                .wait_timeout(guard, remaining)
                .expect("park ack registry poisoned");
        }
        resends += 1;
        if resends.is_multiple_of(WARN_EVERY_RESENDS) {
            tracing::warn!(
                target: "flezha::dlq",
                park_id = %body.park_id,
                channel = %body.entry.channel,
                resends,
                "dead-letter park still unacked; entity held (re-sending)"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_registry_round_trips_and_consumes_once() {
        complete_ack("seg#1");
        assert!(take_ack("seg#1"));
        assert!(!take_ack("seg#1"), "ack is consumed exactly once");
        assert!(!take_ack("seg#never"), "unknown id is not acked");
    }

    #[test]
    fn payload_bytes_extraction_covers_entry_types() {
        assert_eq!(
            extract_payload_bytes(&b"raw".to_vec()),
            Some(b"raw".to_vec())
        );
        assert_eq!(
            extract_payload_bytes(&"text".to_string()),
            Some(b"text".to_vec())
        );
        #[derive(Clone)]
        struct Opaque;
        assert_eq!(extract_payload_bytes(&Opaque), None);
    }

    #[test]
    fn park_body_postcard_bytes_are_the_cross_language_fixture() {
        // Shared fixture with the controller's ParkBody decode
        // (wire_test.exs); an encoding change breaks loudly here.
        let body = ParkBody {
            park_id: "s#0".into(),
            origin_offset: Some(7),
            entry: crate::entity::flusher::DlqEntryBody {
                channel: "in".into(),
                error: "boom".into(),
                attempts: 3,
                payload: Some(vec![1, 2]),
            },
        };
        let bytes = postcard::to_allocvec(&body).unwrap();
        assert_eq!(
            bytes,
            vec![
                0x03, b's', b'#', b'0', // park_id "s#0"
                0x01, 0x07, // Some(7)
                0x02, b'i', b'n', // entry.channel "in"
                0x04, b'b', b'o', b'o', b'm', // entry.error "boom"
                0x03, // entry.attempts 3
                0x01, 0x02, 0x01, 0x02, // entry.payload Some([1,2])
            ]
        );
    }
}
