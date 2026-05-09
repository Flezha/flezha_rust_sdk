//! Negative compile-time tests for the channel builder. Verifies that
//! type-mismatched wiring fails at the call site with a readable error.
//!
//! Each `.rs` file in `tests/ui/` is compiled in isolation; the matching
//! `.stderr` is the expected error snippet. Run `TRYBUILD=overwrite cargo
//! test ui` to regenerate snapshots after intentional message tweaks.

#[test]
#[cfg_attr(miri, ignore)]
fn ui_compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/wire_wrong_output.rs");
    t.compile_fail("tests/ui/wire_wrong_input.rs");
    t.compile_fail("tests/ui/entry_channel_disallowed_type.rs");
    t.compile_fail("tests/ui/legacy_api_not_importable.rs");
    t.compile_fail("tests/ui/plugin_bound_disallows_on_full.rs");
    t.compile_fail("tests/ui/also_to_plugin_with_after_to_plugin.rs");
}
