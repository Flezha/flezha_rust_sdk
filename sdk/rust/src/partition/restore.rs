//! `RestoreInitPayload` + restoration helpers per design.md Decision 5.
//!
//! When a Partition is reconnected with persisted state, the runtime
//! invokes the user's `Shard::init_state(shard_key, Some(payload))`,
//! then automatically applies the snapshot bytes (replacing the user's
//! freshly-constructed state) and folds the change-log records via
//! `Shard::apply_change`. The user MAY inspect `payload.snapshot_bytes`
//! and `payload.change_log_bytes` for instrumentation but never has to
//! decode them.

use crate::partition::header::{
    decode_header, validate_header_for_change, validate_header_for_state,
};

/// Snapshot + change-log bytes handed to `Shard::init_state` on a warm
/// restore. Borrowed (`&[u8]`) so the runtime owns the underlying buffers
/// for the duration of the call without an allocation.
pub struct RestoreInitPayload<'a> {
    /// Snapshot bytes from the latest snapshot file. Empty if no
    /// snapshot has been written for this Partition. The first 16 bytes
    /// are the version header; the remainder is the postcard-encoded
    /// `S::State`.
    pub snapshot_bytes: &'a [u8],
    /// Change-log bytes (concatenated WAL records). Each record is
    /// framed `[partition_id: u32 BE][seq: u64 BE][header: 16B][change_bytes][crc32: u32 BE]`
    /// — the same shape produced by the per-Partition flusher's
    /// `build_partition_state_change_frame`.
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
            let header = decode_header(&p.snapshot_bytes[..16.min(p.snapshot_bytes.len())])?;
            validate_header_for_state::<S>(&header)?;
            // CRC32 over the post-header body should match the header's crc32.
            let body = &p.snapshot_bytes[16..];
            if crc32fast::hash(body) != header.crc32 {
                return Err(RestoreError::CrcMismatch {
                    partition: 0,
                    seq: 0,
                });
            }
            state = postcard::from_bytes(body)
                .map_err(|e| RestoreError::SnapshotDecode(e.to_string()))?;
        }
        for record in iter_change_log_records(p.change_log_bytes) {
            let record = record?;
            let header = decode_header(record.header)?;
            validate_header_for_change::<S>(&header)?;
            if crc32fast::hash(record.change_bytes) != header.crc32 {
                return Err(RestoreError::CrcMismatch {
                    partition: record.partition_id,
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

/// One framed WAL record as produced by the per-Partition flusher.
/// Field-borrowed against the source byte buffer.
#[derive(Debug)]
pub(crate) struct ChangeLogRecord<'a> {
    pub partition_id: u32,
    pub seq: u64,
    pub header: &'a [u8],
    pub change_bytes: &'a [u8],
}

/// Iterate concatenated WAL records using the framing
/// `[partition_id: u32 BE][seq: u64 BE][header: 16B][change_bytes_len: u32 BE][change_bytes][crc32: u32 BE]`.
///
/// The `change_bytes_len` field bounds the trailing change-bytes range
/// so the iterator can step records without re-deserializing the
/// payload to learn its length.
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
        // Minimum frame: 4 (partition_id) + 8 (seq) + 16 (header) + 4 (len) + 4 (crc) = 36
        if self.remaining.len() < 36 {
            return Some(Err(RestoreError::HeaderInvalid));
        }
        let partition_id = u32::from_be_bytes([
            self.remaining[0],
            self.remaining[1],
            self.remaining[2],
            self.remaining[3],
        ]);
        let seq = u64::from_be_bytes([
            self.remaining[4],
            self.remaining[5],
            self.remaining[6],
            self.remaining[7],
            self.remaining[8],
            self.remaining[9],
            self.remaining[10],
            self.remaining[11],
        ]);
        let header = &self.remaining[12..28];
        let len = u32::from_be_bytes([
            self.remaining[28],
            self.remaining[29],
            self.remaining[30],
            self.remaining[31],
        ]) as usize;
        let body_start = 32usize;
        let body_end = body_start + len;
        let frame_end = body_end + 4;
        if self.remaining.len() < frame_end {
            return Some(Err(RestoreError::HeaderInvalid));
        }
        let change_bytes = &self.remaining[body_start..body_end];
        let trailing_crc = u32::from_be_bytes([
            self.remaining[body_end],
            self.remaining[body_end + 1],
            self.remaining[body_end + 2],
            self.remaining[body_end + 3],
        ]);
        if trailing_crc != crc32fast::hash(change_bytes) {
            return Some(Err(RestoreError::CrcMismatch { partition: partition_id, seq }));
        }
        let record = ChangeLogRecord {
            partition_id,
            seq,
            header,
            change_bytes,
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
