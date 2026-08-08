//! Typed wrappers over the core's 16-byte record header: every generic here
//! only reads `S::SCHEMA_VERSION`.

pub use flz_core::header::{
    decode_header, parse_pkg_version, DecodedHeader, HeaderTemplate, FORMAT_ID_EFFECT_RECORD,
    FORMAT_ID_POSTCARD, FORMAT_ID_TIMER_OPS, SDK_LANG_RUST,
};

use flz_core::header::RestoreError;

/// Build a template bound to a specific Procedure's `SCHEMA_VERSION`.
pub fn header_template_for<S: crate::procedure::Procedure>() -> HeaderTemplate {
    HeaderTemplate::new(parse_pkg_version(), S::SCHEMA_VERSION)
}

pub fn validate_header_for_state<S: crate::procedure::Procedure>(
    header: &DecodedHeader,
) -> Result<(), RestoreError> {
    flz_core::header::validate_header_for_state(header, S::SCHEMA_VERSION)
}

pub fn validate_header_for_change<S: crate::procedure::Procedure>(
    header: &DecodedHeader,
) -> Result<(), RestoreError> {
    flz_core::header::validate_header_for_change(header, S::SCHEMA_VERSION)
}

pub fn validate_header_for_effect_record<S: crate::procedure::Procedure>(
    header: &DecodedHeader,
) -> Result<(), RestoreError> {
    flz_core::header::validate_header_for_effect_record(header, S::SCHEMA_VERSION)
}

pub fn validate_header_for_timer_ops<S: crate::procedure::Procedure>(
    header: &DecodedHeader,
) -> Result<(), RestoreError> {
    flz_core::header::validate_header_for_timer_ops(header, S::SCHEMA_VERSION)
}
