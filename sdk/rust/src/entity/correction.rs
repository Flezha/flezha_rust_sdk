//! Late-input detection and the correction request (unbounded, tier-only).
//!
//! Only the SDK can decide an input is late: the event time comes from the
//! user's `TimeExtractor`, so the controller — which owns the WAL, the
//! snapshots and the input log — cannot see it. Detection therefore lives
//! here, on the dispatch path, and orchestration lives in the controller. The
//! seam between them is one fire-and-forget frame.
//!
//! The request carries no offsets and no slice. It does not need to: inputs
//! are journaled *before* they are forwarded, so the late input is already in
//! the durable input log at its arrival offset by the time this code runs.
//! The controller re-reads the range and re-forwards it in event-time order.
//! There is nothing to splice — only a slice to re-order.

use serde::{Deserialize, Serialize};

use crate::entity::flusher::build_entity_frame;
use crate::entity::state_sink;

/// Data-stream payload id of a correction request (SDK → controller). Same
/// outer frame shape as `park_payload`: a zero header and a postcard body.
pub const CORRECTION_REQUEST_PAYLOAD_ID: &str = "correction_request";

/// Wire body of a `correction_request` frame. The entity identity rides the
/// frame envelope, so the body is just the target.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CorrectionRequestBody {
    /// Event time of the late input. The controller rewinds to the last state
    /// seq whose event time precedes this, so the replay re-runs every input
    /// that should have come after it.
    pub target_event_time: u64,
}

/// What to do with an input, given its event time and the Entity's high-water.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lateness {
    /// At or ahead of the high-water — the ordinary case. Dispatch it and
    /// advance the high-water.
    OnTime,
    /// Behind the high-water but inside the correction window. Do not dispatch:
    /// the input is durable in the log and comes back inside the replayed
    /// slice, in its rightful place. Request the correction instead.
    Correctable,
    /// Behind the high-water by more than the window. The past it belongs to can
    /// no longer be rebuilt; what happens next is the caller's declared policy,
    /// not this function's business.
    BeyondWindow,
}

/// Classify an input against the Entity's event-time high-water.
///
/// `high_water` is `-1` before the Entity has dispatched anything, which is
/// why it is signed: a fresh Entity's first input is never late.
pub fn classify(event_time: u64, high_water: i64, window_ms: u64) -> Lateness {
    if high_water < 0 || event_time >= high_water as u64 {
        return Lateness::OnTime;
    }
    if high_water as u64 - event_time <= window_ms {
        Lateness::Correctable
    } else {
        Lateness::BeyondWindow
    }
}

/// Ship a correction request. Fire-and-forget: the controller coalesces
/// concurrent requests per Entity into one replay, so a duplicate costs
/// nothing and a lost one is re-raised the moment the input is re-delivered
/// (it is still in the log, still unconsumed).
///
/// A transport-less run has no controller to ask; there is no correction to be
/// had, and the caller falls back to dispatching the input as an arrival.
pub fn request(entity_identity: &[u8], target_event_time: u64) -> bool {
    if !state_sink::sink_installed() {
        return false;
    }
    let body = CorrectionRequestBody { target_event_time };
    let body_bytes = postcard::to_allocvec(&body)
        .expect("postcard serialization of CorrectionRequestBody cannot fail");
    let frame = build_entity_frame(entity_identity, 0, &[0u8; 16], &body_bytes);
    state_sink::send(CORRECTION_REQUEST_PAYLOAD_ID, frame);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    const WINDOW: u64 = 600_000;

    #[test]
    fn a_fresh_entity_has_no_late_inputs() {
        assert_eq!(classify(1_000, -1, WINDOW), Lateness::OnTime);
    }

    #[test]
    fn at_or_ahead_of_the_high_water_is_on_time() {
        assert_eq!(classify(5_000, 5_000, WINDOW), Lateness::OnTime);
        assert_eq!(classify(5_001, 5_000, WINDOW), Lateness::OnTime);
    }

    #[test]
    fn inside_the_window_is_correctable() {
        assert_eq!(classify(4_999, 5_000, WINDOW), Lateness::Correctable);
        // Exactly at the window edge is still correctable.
        assert_eq!(classify(1, 600_001, WINDOW), Lateness::Correctable);
    }

    #[test]
    fn beyond_the_window_is_beyond_the_window() {
        assert_eq!(classify(1, 600_002, WINDOW), Lateness::BeyondWindow);
    }
}
