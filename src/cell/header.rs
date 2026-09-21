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
    use flz_core::restore::RestoreInitPayload;

    use super::*;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct NoChange;
    impl crate::tasklet::change::Change for NoChange {}

    #[derive(Default)]
    struct VersionTask;
    impl crate::tasklet::Tasklet for VersionTask {
        type Input = String;
        type Output = String;
        type State = ();
        type Change = NoChange;
        type Clock = crate::tasklet::WallClock;
        type Config = ();
        fn name() -> &'static str {
            "header_version_test_op"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn apply_change(_: &mut (), _: &NoChange) {}
        fn run(
            &mut self,
            _: String,
            _: crate::tasklet::state_handle::StateHandle<'_, Self>,
            _: &mut crate::tasklet::TaskletCtx<'_>,
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
