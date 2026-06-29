//! `RestoreInitPayload` + restoration helpers per design.md Decision 5.
//!
//! When a Segment is reconnected with persisted state, the runtime
//! invokes the user's `Operation::init_state(operation_key, Some(payload))`,
//! then automatically applies the snapshot bytes (replacing the user's
//! freshly-constructed state) and folds the change-log records via
//! `Operation::apply_change`. The user MAY inspect `payload.snapshot_bytes`
//! and `payload.change_log_bytes` for instrumentation but never has to
//! decode them.

use crate::segment::header::{decode_header, validate_header_for_change};
use crate::util::{read_u32_be, read_u64_be};

/// Snapshot + change-log bytes handed to `Operation::init_state` on a warm
/// restore. Borrowed (`&[u8]`) so the runtime owns the underlying buffers
/// for the duration of the call without an allocation.
pub struct RestoreInitPayload<'a> {
    /// Snapshot bytes from the latest snapshot file. Empty if no
    /// snapshot has been written for this Segment. The body is the
    /// postcard-encoded `S::State` exactly as the SDK shipped it via
    /// `segment_state_snapshot`. The controller strips its on-disk header
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
    #[error("crc32 mismatch on change record (segment={segment}, seq={seq})")]
    CrcMismatch { segment: u32, seq: u64 },
}

/// Runtime-side restoration helper. Not intended for direct user
/// invocation — the user's `Operation::init_state` is invoked first; this
/// function applies the persisted state on top of whatever
/// `init_state` returned.
///
/// Behavior:
///   1. If `payload.snapshot_bytes` is non-empty, decode + validate the
///      header and replace `state` with `postcard::from_bytes(rest)`.
///   2. Iterate change-log records via the internal `iter_change_log_records`
///      helper; for each record, validate the header, deserialize the change
///      body, and call `S::apply_change(&mut state, &change)`.
pub fn init_segment_with_payload<S: crate::operation::Operation>(
    operation_key: Option<&str>,
    payload: Option<&RestoreInitPayload<'_>>,
) -> Result<S::State, RestoreError>
where
    S::State: serde::de::DeserializeOwned,
{
    let mut state = S::init_state(
        operation_key,
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
            validate_header_for_change::<S>(&header)?;
            // The trailing crc32 in the WAL covers header + change_bytes.
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(record.header);
            hasher.update(record.change_bytes);
            if hasher.finalize() != record.trailing_crc {
                return Err(RestoreError::CrcMismatch {
                    segment: 0,
                    seq: record.seq,
                });
            }
            let change: S::Change = postcard::from_bytes(record.change_bytes)
                .map_err(|e| RestoreError::ChangeDecode(e.to_string()))?;
            S::apply_change(&mut state, &change);
        }
    }
    Ok(state)
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

    #[test]
    fn empty_change_log_yields_no_records() {
        let mut iter = iter_change_log_records(&[]);
        assert!(iter.next().is_none());
    }

    #[test]
    fn truncated_change_log_returns_header_invalid() {
        let mut iter = iter_change_log_records(&[0u8; 10]);
        let err = iter.next().unwrap().unwrap_err();
        assert!(matches!(err, RestoreError::HeaderInvalid));
    }
}
