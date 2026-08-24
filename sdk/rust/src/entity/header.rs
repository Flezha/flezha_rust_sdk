//! The typed view of the core's 16-byte record header: this SDK's `Procedure`
//! consts bound to the core's stamping and resolution.

use flz_core::header::parse_pkg_version;
pub use flz_core::header::{
    decode_header, resolve_record, resolve_state, HeaderTemplate, RecordKind, SchemaDecision,
    FORMAT_ID_EFFECT_RECORD, FORMAT_ID_POSTCARD, FORMAT_ID_TIMER_OPS,
};

/// Build a template bound to a specific Procedure's `SCHEMA_VERSION`. Every
/// record this Procedure writes is stamped from it, and every record it reads
/// is resolved against it.
pub fn header_template_for<S: crate::procedure::Procedure>() -> HeaderTemplate {
    HeaderTemplate::new(parse_pkg_version(), S::SCHEMA_VERSION)
}
