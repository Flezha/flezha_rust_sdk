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

    // Authoring-sugar macros (`simplify-sdk-authoring`).
    t.compile_fail("tests/ui/shard_zero_inputs.rs");
    t.compile_fail("tests/ui/shard_many_inputs.rs");
    t.compile_fail("tests/ui/shard_shared_ref_state.rs");
    t.compile_fail("tests/ui/shard_sink_unit_return.rs");
    t.compile_fail("tests/ui/shard_static_and_dynamic.rs");
    t.compile_fail("tests/ui/shard_keys_without_partition_for.rs");
    t.compile_fail("tests/ui/shard_init_without_state.rs");
    t.compile_fail("tests/ui/state_on_enum.rs");
    t.compile_fail("tests/ui/state_on_tuple_struct.rs");
    t.compile_fail("tests/ui/relay_fan_out.rs");

    // Keyed/collection journaling (`expressive-state-journaling`).
    t.compile_fail("tests/ui/state_map_without_journal.rs");
    t.compile_fail("tests/ui/state_journal_type_mismatch.rs");
    t.compile_fail("tests/ui/state_journal_unknown_mode.rs");
    t.pass("tests/ui/shard_option_output.rs");
}
