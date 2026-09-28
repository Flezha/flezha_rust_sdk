//! The typed view of the core's 16-byte record header: this SDK's `Tasklet`
//! consts bound to the core's stamping and resolution.

use flz_core::header::pack_version;
pub use flz_core::header::{
    decode_header, resolve_record, resolve_state, HeaderTemplate, RecordKind, SchemaDecision,
    FORMAT_ID_EFFECT_RECORD, FORMAT_ID_POSTCARD, FORMAT_ID_TIMER_OPS,
};

/// Build a template bound to a specific Tasklet's `SCHEMA_VERSION`. Every
/// record this Tasklet writes is stamped from it, and every record it reads
/// is resolved against it.
pub fn header_template_for<S: crate::tasklet::Tasklet>() -> HeaderTemplate {
    HeaderTemplate::new(pack_version(env!("CARGO_PKG_VERSION")), S::SCHEMA_VERSION)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[derive(Default)]
    struct VersionTask;
    #[flezha_runner::tasklet(name = "header_version_test_op")]
    impl crate::tasklet::Tasklet for VersionTask {
        type Input = String;
        type Output = String;
        fn run(
            &mut self,
            _: String,
            _: &mut crate::tasklet::Ctx<'_, Self>,
        ) -> Result<(), crate::tasklet::TaskletError> {
            Ok(())
        }
    }

    /// The stamped version is this SDK crate's own, not whatever version the
    /// core it links against was built at.
    #[test]
    fn the_stamped_version_is_this_sdks_own() {
        assert_eq!(
            header_template_for::<VersionTask>().sdk_version,
            pack_version(env!("CARGO_PKG_VERSION")),
            "the durable header must carry `flezha_runner`'s version"
        );
    }
}
