//! 16-byte version header per umbrella spec / design.md Decision 8.
//!
//! Layout:
//!  - 0:1   `sdk_lang: u8` = 1 (Rust)
//!  - 1:5   `sdk_version: u32 BE` = (major << 16) | (minor << 8) | patch
//!  - 5:9   `schema_version: u32 BE` = `S::SCHEMA_VERSION`
//!  - 9:10  `format_id: u8` = 1 (postcard) in v1
//!  - 10:12 reserved (zero)
//!  - 12:16 `crc32: u32 BE` = `crc32fast::hash(&change_bytes)`

use crate::partition::restore::RestoreError;

/// Compiled-in language tag stamped at byte 0 of the persistence header.
pub const SDK_LANG_RUST: u8 = 1;

/// Postcard format id stamped at byte 9 of the persistence header.
pub const FORMAT_ID_POSTCARD: u8 = 1;

/// Parsed view of the 16-byte header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodedHeader {
    pub sdk_lang: u8,
    pub sdk_version: u32,
    pub schema_version: u32,
    pub format_id: u8,
    pub crc32: u32,
}

/// Header values frozen at Partition construction so the per-record stamp
/// is a memcpy + crc compute. Reserved bytes are always zero.
#[derive(Debug, Clone, Copy)]
pub struct HeaderTemplate {
    pub sdk_lang: u8,
    pub sdk_version: u32,
    pub schema_version: u32,
    pub format_id: u8,
}

impl HeaderTemplate {
    /// Construct the per-Partition header template. Most callers want
    /// `for_shard::<S>()` which fills in the SDK version and schema
    /// version from compile-time constants.
    pub fn new(sdk_version: u32, schema_version: u32) -> Self {
        Self {
            sdk_lang: SDK_LANG_RUST,
            sdk_version,
            schema_version,
            format_id: FORMAT_ID_POSTCARD,
        }
    }

    /// Build a template bound to a specific Shard's `SCHEMA_VERSION`.
    pub fn for_shard<S: crate::shard::Shard>() -> Self {
        Self::new(parse_pkg_version(), S::SCHEMA_VERSION)
    }

    /// Stamp the header onto fresh bytes. The CRC32 is computed over the
    /// caller-provided change_bytes; the returned 16-byte array is
    /// suitable for the `[header: 16B][change_bytes]` framing produced
    /// by the per-Partition flusher.
    pub fn stamp(&self, change_bytes: &[u8]) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0] = self.sdk_lang;
        out[1..5].copy_from_slice(&self.sdk_version.to_be_bytes());
        out[5..9].copy_from_slice(&self.schema_version.to_be_bytes());
        out[9] = self.format_id;
        // bytes 10..12 are reserved and remain zero.
        let crc = crc32fast::hash(change_bytes);
        out[12..16].copy_from_slice(&crc.to_be_bytes());
        out
    }
}

/// Decode a 16-byte header into its fields. Validation against the
/// running SDK's expectations lives in [`validate_header_for_state`] and
/// [`validate_header_for_change`] so the caller sees a typed error.
pub fn decode_header(bytes: &[u8]) -> Result<DecodedHeader, RestoreError> {
    if bytes.len() < 16 {
        return Err(RestoreError::HeaderInvalid);
    }
    let sdk_version = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    let schema_version = u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
    let crc32 = u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    Ok(DecodedHeader {
        sdk_lang: bytes[0],
        sdk_version,
        schema_version,
        format_id: bytes[9],
        crc32,
    })
}

/// Validate a header about to be applied to a state snapshot. Same
/// language + schema-version checks as for change records.
pub fn validate_header_for_state<S: crate::shard::Shard>(
    header: &DecodedHeader,
) -> Result<(), RestoreError> {
    validate_common::<S>(header)
}

/// Validate a header attached to a change-log record before
/// deserialization.
pub fn validate_header_for_change<S: crate::shard::Shard>(
    header: &DecodedHeader,
) -> Result<(), RestoreError> {
    validate_common::<S>(header)
}

fn validate_common<S: crate::shard::Shard>(header: &DecodedHeader) -> Result<(), RestoreError> {
    if header.sdk_lang != SDK_LANG_RUST {
        return Err(RestoreError::LangMismatch {
            expected: SDK_LANG_RUST,
            found: header.sdk_lang,
        });
    }
    if header.format_id != FORMAT_ID_POSTCARD {
        return Err(RestoreError::HeaderInvalid);
    }
    if header.schema_version != S::SCHEMA_VERSION {
        return Err(RestoreError::SchemaMismatch {
            expected: S::SCHEMA_VERSION,
            found: header.schema_version,
        });
    }
    Ok(())
}

/// Parse `CARGO_PKG_VERSION` into the `(major << 16) | (minor << 8) | patch`
/// form stamped at bytes 1..5 of the header. Components above 255 are
/// clamped (no real-world version reaches this; defensive against
/// "0.1.0-pre.42" suffixes).
pub fn parse_pkg_version() -> u32 {
    let raw = env!("CARGO_PKG_VERSION");
    let mut parts = raw.split(|c: char| !c.is_ascii_digit()).filter(|s| !s.is_empty());
    let major = parts.next().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
    let minor = parts.next().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
    let patch = parts.next().and_then(|s| s.parse::<u32>().ok()).unwrap_or(0);
    (major.min(0xFF) << 16) | (minor.min(0xFF) << 8) | patch.min(0xFF)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamp_layout_matches_umbrella_spec() {
        // Umbrella scenario: SDK at version 0.5.3, SCHEMA_VERSION = 1,
        // change_bytes [0x01, 0x02, 0x03] → header bytes per the spec.
        let template = HeaderTemplate::new((0 << 16) | (5 << 8) | 3, 1);
        let header = template.stamp(&[0x01, 0x02, 0x03]);
        assert_eq!(header[0], 0x01);
        assert_eq!(&header[1..5], &[0x00, 0x00, 0x05, 0x03]);
        assert_eq!(&header[5..9], &[0x00, 0x00, 0x00, 0x01]);
        assert_eq!(header[9], 0x01);
        assert_eq!(&header[10..12], &[0x00, 0x00]);
        let expected_crc = crc32fast::hash(&[0x01, 0x02, 0x03]).to_be_bytes();
        assert_eq!(&header[12..16], &expected_crc);
    }

    #[test]
    fn decode_round_trips_stamp() {
        let template = HeaderTemplate::new(0x010203, 7);
        let bytes = template.stamp(b"hello");
        let decoded = decode_header(&bytes).unwrap();
        assert_eq!(decoded.sdk_lang, SDK_LANG_RUST);
        assert_eq!(decoded.sdk_version, 0x010203);
        assert_eq!(decoded.schema_version, 7);
        assert_eq!(decoded.format_id, FORMAT_ID_POSTCARD);
        assert_eq!(decoded.crc32, crc32fast::hash(b"hello"));
    }

    #[test]
    fn decode_short_buffer_is_header_invalid() {
        let err = decode_header(&[0u8; 8]).unwrap_err();
        assert!(matches!(err, RestoreError::HeaderInvalid));
    }
}
