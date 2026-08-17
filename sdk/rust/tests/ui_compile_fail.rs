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
    t.compile_fail("tests/ui/wire_wrong_input.rs");
    t.compile_fail("tests/ui/entry_channel_disallowed_type.rs");
    t.compile_fail("tests/ui/sealed_disallows_on_full.rs");
    t.compile_fail("tests/ui/sealed_disallows_wiring.rs");
    t.compile_fail("tests/ui/stream_handle_cannot_escape.rs");
    t.compile_fail("tests/ui/entry_stream_has_no_plugin_sink.rs");

    // Authoring-sugar macros (`simplify-sdk-authoring`).
    t.compile_fail("tests/ui/procedure_zero_inputs.rs");
    t.compile_fail("tests/ui/procedure_many_inputs.rs");
    t.compile_fail("tests/ui/procedure_shared_ref_state.rs");
    t.compile_fail("tests/ui/procedure_sink_unit_return.rs");
    t.compile_fail("tests/ui/procedure_entity_key_removed.rs");
    t.compile_fail("tests/ui/procedure_key_unit_return.rs");
    t.compile_fail("tests/ui/entity_keys_without_entities_for.rs");
    t.compile_fail("tests/ui/procedure_init_without_state.rs");
    t.compile_fail("tests/ui/state_on_enum.rs");
    t.compile_fail("tests/ui/state_on_tuple_struct.rs");
    t.compile_fail("tests/ui/operative_fan_out.rs");

    // Keyed/collection journaling (`expressive-state-journaling`).
    t.compile_fail("tests/ui/state_map_without_journal.rs");
    t.compile_fail("tests/ui/state_journal_type_mismatch.rs");
    t.compile_fail("tests/ui/state_journal_unknown_mode.rs");
    t.pass("tests/ui/procedure_option_output.rs");
}
