//! `RestoreInitPayload` + restoration helpers per design.md Decision 5.
//!
//! When a Partition is reconnected with persisted state, the runtime
//! invokes the user's `Shard::init_state(shard_key, Some(payload))`,
//! then automatically applies the snapshot bytes (replacing the user's
//! freshly-constructed state) and folds the change-log records via
//! `Shard::apply_change`. The user MAY inspect `payload.snapshot_bytes`
//! and `payload.change_log_bytes` for instrumentation but never has to
//! decode them.

use crate::partition::header::{decode_header, validate_header_for_change};

/// Snapshot + change-log bytes handed to `Shard::init_state` on a warm
/// restore. Borrowed (`&[u8]`) so the runtime owns the underlying buffers
/// for the duration of the call without an allocation.
pub struct RestoreInitPayload<'a> {
    /// Snapshot bytes from the latest snapshot file. Empty if no
    /// snapshot has been written for this Partition. The body is the
    /// postcard-encoded `S::State` exactly as the SDK shipped it via
    /// `partition_state_snapshot`. The agent strips its on-disk header
    /// before shipping, so this slice does NOT include the 16-byte
    /// header — it is the raw State payload.
    pub snapshot_bytes: &'a [u8],
    /// Change-log bytes (concatenated WAL records). Each record is
    /// framed `[seq: u64 BE][len: u32 BE][header: 16B][change_bytes][crc32: u32 BE]`
    /// where `len = 16 + change_bytes.len()` and `crc32` covers
    /// `header + change_bytes`. This matches the agent's on-disk WAL
    /// record format (`Flezha.State.WalWriter.encode_record/3`).
    pub change_log_bytes: &'a [u8],
}

/// Reasons restoration can fail. The runtime surfaces these from the
/// agent-facing relay-start path; the user's `init_state` does not see
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
    #[error("crc32 mismatch on change record (partition={partition}, seq={seq})")]
    CrcMismatch { partition: u32, seq: u64 },
}

/// Runtime-side restoration helper. Not intended for direct user
/// invocation — the user's `Shard::init_state` is invoked first; this
/// function applies the persisted state on top of whatever
/// `init_state` returned.
///
/// Behavior:
///   1. If `payload.snapshot_bytes` is non-empty, decode + validate the
///      header and replace `state` with `postcard::from_bytes(rest)`.
///   2. Iterate change-log records via [`iter_change_log_records`]; for
///      each record, validate the header, deserialize the change body,
///      and call `S::apply_change(&mut state, &change)`.
pub fn init_partition_with_payload<S: crate::shard::Shard>(
    shard_key: Option<&str>,
    payload: Option<&RestoreInitPayload<'_>>,
) -> Result<S::State, RestoreError>
where
    S::State: serde::de::DeserializeOwned,
{
    let mut state = S::init_state(shard_key, payload.copied_view());
    if let Some(p) = payload {
        if !p.snapshot_bytes.is_empty() {
            // The agent strips the snapshot file's header before shipping;
            // the body here is the raw postcard-encoded `S::State`.
            state = postcard::from_bytes(p.snapshot_bytes)
                .map_err(|e| RestoreError::SnapshotDecode(e.to_string()))?;
        }
        for record in iter_change_log_records(p.change_log_bytes) {
            let record = record?;
            let header = decode_header(record.header)?;
            validate_header_for_change::<S>(&header)?;
            // The trailing crc32 in the WAL covers header + change_bytes.
            let mut crc_input = Vec::with_capacity(16 + record.change_bytes.len());
            crc_input.extend_from_slice(record.header);
            crc_input.extend_from_slice(record.change_bytes);
            if crc32fast::hash(&crc_input) != record.trailing_crc {
                return Err(RestoreError::CrcMismatch {
                    partition: 0,
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

/// One framed WAL record as produced by the agent's
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

/// Iterate concatenated WAL records using the agent on-disk framing
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
        let seq = u64::from_be_bytes([
            self.remaining[0],
            self.remaining[1],
            self.remaining[2],
            self.remaining[3],
            self.remaining[4],
            self.remaining[5],
            self.remaining[6],
            self.remaining[7],
        ]);
        let len = u32::from_be_bytes([
            self.remaining[8],
            self.remaining[9],
            self.remaining[10],
            self.remaining[11],
        ]) as usize;
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
        let trailing_crc = u32::from_be_bytes([
            self.remaining[change_end],
            self.remaining[change_end + 1],
            self.remaining[change_end + 2],
            self.remaining[change_end + 3],
        ]);
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

/// Helper trait that lets `init_partition_with_payload` hand a fresh
/// borrow of the payload to user `init_state`. Implemented for
/// `Option<&RestoreInitPayload<'_>>` so callers do not have to clone.
trait CopiedView<'a> {
    fn copied_view(&self) -> Option<RestoreInitPayload<'a>>;
}

impl<'a> CopiedView<'a> for Option<&RestoreInitPayload<'a>> {
    fn copied_view(&self) -> Option<RestoreInitPayload<'a>> {
        self.map(|p| RestoreInitPayload {
            snapshot_bytes: p.snapshot_bytes,
            change_log_bytes: p.change_log_bytes,
        })
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
