//! `RestoreInitPayload` + restoration helpers per design.md Decision 5.
//!
//! When an Entity is reconnected with persisted state, the runtime
//! invokes the user's `Procedure::init_state(entity_key, Some(payload))`,
//! then automatically applies the snapshot bytes (replacing the user's
//! freshly-constructed state) and folds the change-log records via
//! `Procedure::apply_change`. The user MAY inspect `payload.snapshot_bytes`
//! and `payload.change_log_bytes` for instrumentation but never has to
//! decode them.

use crate::entity::header::{
    decode_header, validate_header_for_change, validate_header_for_effect_record,
    validate_header_for_timer_ops, FORMAT_ID_EFFECT_RECORD, FORMAT_ID_TIMER_OPS,
};
use crate::entity::timer::{PendingTimers, TimerOp};
use crate::util::{read_u32_be, read_u64_be};

/// Snapshot + change-log bytes handed to `Procedure::init_state` on a warm
/// restore. Borrowed (`&[u8]`) so the runtime owns the underlying buffers
/// for the duration of the call without an allocation.
pub struct RestoreInitPayload<'a> {
    /// Snapshot bytes from the latest snapshot file. Empty if no
    /// snapshot has been written for this Entity. The body is the
    /// postcard-encoded `S::State` exactly as the SDK shipped it via
    /// `entity_state_snapshot`. The controller strips its on-disk header
    /// before shipping, so this slice does NOT include the 16-byte
    /// header — it is the raw State payload.
    pub snapshot_bytes: &'a [u8],
    /// Change-log bytes (concatenated WAL records). Each record is
    /// framed `[seq: u64 BE][len: u32 BE][header: 16B][change_bytes][crc32: u32 BE]`
    /// where `len = 16 + change_bytes.len()` and `crc32` covers
    /// `header + change_bytes`. This matches the controller's on-disk WAL
    /// record format (`Flezha.State.WalWriter.encode_record/3`).
    pub change_log_bytes: &'a [u8],
}

/// Reasons restoration can fail. The runtime surfaces these from the
/// controller-facing operative-start path; the user's `init_state` does not see
/// them directly.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RestoreError {
    #[error("snapshot decode failed: {0}")]
    SnapshotDecode(String),
    #[error("change-log decode failed: {0}")]
    ChangeDecode(String),
    #[error("malformed restoration header (truncated or invalid)")]
    HeaderInvalid,
    #[error("sdk_lang mismatch: expected {expected}, found {found}")]
    LangMismatch { expected: u8, found: u8 },
    #[error("schema_version mismatch: expected {expected}, found {found}")]
    SchemaMismatch { expected: u32, found: u32 },
    #[error("crc32 mismatch on change record (entity={entity}, seq={seq})")]
    CrcMismatch { entity: u32, seq: u64 },
}

/// Runtime-side restoration helper. Not intended for direct user
/// invocation — the user's `Procedure::init_state` is invoked first; this
/// function applies the persisted state on top of whatever
/// `init_state` returned.
///
/// Behavior:
///   1. If `payload.snapshot_bytes` is non-empty, decode + validate the
///      header and replace `state` with `postcard::from_bytes(rest)`.
///   2. Iterate change-log records via the internal `iter_change_log_records`
///      helper; for each record, validate the header, deserialize the change
///      body, and call `S::apply_change(&mut state, &change)`.
///   3. Fold every `TimerOp` the log carries — standalone `format_id = 3`
///      records and the `timers` an effect record encloses — into the returned
///      pending-timer set. Timers are folded state, so they are rebuilt by this
///      same walk rather than by a store of their own.
pub fn init_entity_with_payload<S: crate::procedure::Procedure>(
    entity_key: Option<&str>,
    payload: Option<&RestoreInitPayload<'_>>,
) -> Result<(S::State, PendingTimers), RestoreError>
where
    S::State: serde::de::DeserializeOwned,
{
    let mut timers = PendingTimers::default();
    let mut state = S::init_state(
        entity_key,
        payload.map(|p| RestoreInitPayload {
            snapshot_bytes: p.snapshot_bytes,
            change_log_bytes: p.change_log_bytes,
        }),
    );
    if let Some(p) = payload {
        if !p.snapshot_bytes.is_empty() {
            // The controller strips the snapshot file's header before shipping;
            // the body here is the raw postcard-encoded `S::State`.
            state = postcard::from_bytes(p.snapshot_bytes)
                .map_err(|e| RestoreError::SnapshotDecode(e.to_string()))?;
        }
        for record in iter_change_log_records(p.change_log_bytes) {
            let record = record?;
            let header = decode_header(record.header)?;
            // The trailing crc32 in the WAL covers header + change_bytes.
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(record.header);
            hasher.update(record.change_bytes);
            if hasher.finalize() != record.trailing_crc {
                return Err(RestoreError::CrcMismatch {
                    entity: 0,
                    seq: record.seq,
                });
            }
            match header.format_id {
                FORMAT_ID_EFFECT_RECORD => {
                    // Effectively-once effect record: fold its change portion
                    // exactly as plain change records fold, in order. Outputs
                    // and consumed-offset are controller-side concerns here.
                    validate_header_for_effect_record::<S>(&header)?;
                    let body: crate::entity::flusher::EffectRecordBody =
                        postcard::from_bytes(record.change_bytes)
                            .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
                    for change_bytes in &body.changes {
                        let change: S::Change = postcard::from_bytes(change_bytes)
                            .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
                        S::apply_change(&mut state, &change);
                    }
                    for op in &body.timers {
                        timers.apply(op);
                    }
                }
                FORMAT_ID_TIMER_OPS => {
                    validate_header_for_timer_ops::<S>(&header)?;
                    let ops: Vec<TimerOp> = postcard::from_bytes(record.change_bytes)
                        .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
                    for op in &ops {
                        timers.apply(op);
                    }
                }
                _ => {
                    validate_header_for_change::<S>(&header)?;
                    let change: S::Change = postcard::from_bytes(record.change_bytes)
                        .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
                    S::apply_change(&mut state, &change);
                }
            }
        }
    }
    Ok((state, timers))
}

/// One framed WAL record as produced by the controller's
/// `Flezha.State.WalWriter.encode_record/3`. Field-borrowed against the
/// source byte buffer. `change_bytes` excludes the leading 16-byte
/// header (which is exposed separately as `header`).
#[derive(Debug)]
pub(crate) struct ChangeLogRecord<'a> {
    pub seq: u64,
    pub header: &'a [u8],
    pub change_bytes: &'a [u8],
    pub trailing_crc: u32,
}

/// Iterate concatenated WAL records using the controller on-disk framing
/// `[seq: u64 BE][len: u32 BE][header: 16B][change_bytes][crc32: u32 BE]`
/// where `len = 16 + change_bytes.len()` and `crc32` covers
/// `header + change_bytes`.
pub(crate) fn iter_change_log_records(
    bytes: &[u8],
) -> impl Iterator<Item = Result<ChangeLogRecord<'_>, RestoreError>> {
    ChangeLogIter { remaining: bytes }
}

struct ChangeLogIter<'a> {
    remaining: &'a [u8],
}

impl<'a> Iterator for ChangeLogIter<'a> {
    type Item = Result<ChangeLogRecord<'a>, RestoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining.is_empty() {
            return None;
        }
        // Minimum frame: 8 (seq) + 4 (len) + 16 (header) + 4 (crc) = 32
        if self.remaining.len() < 32 {
            return Some(Err(RestoreError::HeaderInvalid));
        }
        let seq = read_u64_be(self.remaining, 0);
        let len = read_u32_be(self.remaining, 8) as usize;
        if len < 16 {
            return Some(Err(RestoreError::HeaderInvalid));
        }
        let header_start = 12usize;
        let header_end = header_start + 16;
        let change_start = header_end;
        let change_end = header_start + len; // = 12 + 16 + change_bytes.len()
        let frame_end = change_end + 4;
        if self.remaining.len() < frame_end {
            return Some(Err(RestoreError::HeaderInvalid));
        }
        let header = &self.remaining[header_start..header_end];
        let change_bytes = &self.remaining[change_start..change_end];
        let trailing_crc = read_u32_be(self.remaining, change_end);
        let record = ChangeLogRecord {
            seq,
            header,
            change_bytes,
            trailing_crc,
        };
        self.remaining = &self.remaining[frame_end..];
        Some(Ok(record))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::flusher::{EffectRecordBody, OutputRecord};
    use crate::entity::header::{HeaderTemplate, FORMAT_ID_POSTCARD};

    #[test]
    fn empty_change_log_yields_no_records() {
        let mut iter = iter_change_log_records(&[]);
        assert!(iter.next().is_none());
    }

    #[derive(serde::Serialize, serde::Deserialize)]
    struct AddChange(i64);
    impl crate::procedure::change::Change for AddChange {}

    #[derive(Default)]
    struct FoldProc;
    impl crate::procedure::Procedure for FoldProc {
        type Input = String;
        type Output = String;
        type State = i64;
        type Change = AddChange;
        type Clock = crate::procedure::WallClock;
        type Config = ();
        fn name() -> &'static str {
            "restore_fold_test_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
            0
        }
        fn apply_change(state: &mut i64, change: &AddChange) {
            *state += change.0;
        }
        fn run(
            &mut self,
            _: String,
            _: crate::procedure::state_handle::StateHandle<'_, Self>,
            _: &mut crate::procedure::ProcedureCtx<'_>,
        ) -> Result<(), crate::procedure::ProcedureError> {
            Ok(())
        }
    }

    fn wal_record(seq: u64, format_id: u8, body: &[u8]) -> Vec<u8> {
        let template = HeaderTemplate {
            format_id,
            ..HeaderTemplate::for_procedure::<FoldProc>()
        };
        let header = template.stamp(body);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&header);
        hasher.update(body);
        let crc = hasher.finalize();
        let mut out = Vec::new();
        out.extend_from_slice(&seq.to_be_bytes());
        out.extend_from_slice(&((16 + body.len()) as u32).to_be_bytes());
        out.extend_from_slice(&header);
        out.extend_from_slice(body);
        out.extend_from_slice(&crc.to_be_bytes());
        out
    }

    #[test]
    fn mixed_wal_folds_plain_and_effect_records_in_order() {
        // Plain change record (seq 0), then an effect record (seq 1)
        // carrying two changes + an output — tier enabled mid-lifetime.
        let plain = wal_record(
            0,
            FORMAT_ID_POSTCARD,
            &postcard::to_allocvec(&AddChange(5)).unwrap(),
        );
        let effect_body = EffectRecordBody {
            event_time: 0,
            changes: vec![
                postcard::to_allocvec(&AddChange(10)).unwrap(),
                postcard::to_allocvec(&AddChange(100)).unwrap(),
            ],
            outputs: vec![OutputRecord {
                channel: "out".into(),
                key: "k".into(),
                metadata: vec![],
                payload: vec![1, 2],
            }],
            consumed_offset: Some(4711),
            dlq: None,
            timers: Vec::new(),
        };
        let effect = wal_record(
            1,
            FORMAT_ID_EFFECT_RECORD,
            &postcard::to_allocvec(&effect_body).unwrap(),
        );

        let mut change_log = plain;
        change_log.extend_from_slice(&effect);

        let payload = RestoreInitPayload {
            snapshot_bytes: &[],
            change_log_bytes: &change_log,
        };
        let (state, _timers) = init_entity_with_payload::<FoldProc>(None, Some(&payload)).unwrap();
        assert_eq!(state, 115, "5 (plain) + 10 + 100 (effect) folded in order");
    }

    #[test]
    fn truncated_change_log_returns_header_invalid() {
        let mut iter = iter_change_log_records(&[0u8; 10]);
        let err = iter.next().unwrap().unwrap_err();
        assert!(matches!(err, RestoreError::HeaderInvalid));
    }
}
