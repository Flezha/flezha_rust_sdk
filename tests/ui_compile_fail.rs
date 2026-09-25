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
    t.compile_fail("tests/ui/output_without_byte_form.rs");

    // Authoring-sugar macros (`simplify-sdk-authoring`).
    t.compile_fail("tests/ui/tasklet_zero_inputs.rs");
    t.compile_fail("tests/ui/tasklet_many_inputs.rs");
    t.compile_fail("tests/ui/tasklet_shared_ref_state.rs");
    t.compile_fail("tests/ui/tasklet_cell_key_removed.rs");
    t.compile_fail("tests/ui/tasklet_key_unit_return.rs");
    t.compile_fail("tests/ui/tasklet_ctx_with_key.rs");
    t.compile_fail("tests/ui/tasklet_ctx_with_value_return.rs");
    t.compile_fail("tests/ui/tasklet_emits_without_ctx.rs");
    t.compile_fail("tests/ui/simple_tasklet_is_not_also_a_tasklet.rs");
    t.compile_fail("tests/ui/cell_keys_without_cells_for.rs");
    t.compile_fail("tests/ui/tasklet_init_without_state.rs");
    t.compile_fail("tests/ui/state_on_enum.rs");
    t.compile_fail("tests/ui/state_on_tuple_struct.rs");
    t.compile_fail("tests/ui/operator_fan_out.rs");
    t.compile_fail("tests/ui/operator_empty_entry_name.rs");
    t.compile_fail("tests/ui/register_factory_of_both_kinds.rs");
    t.compile_fail("tests/ui/tasklet_operator_keys.rs");

    // The short-form attributes (`#[flezha::unbounded_operator]` /
    // `#[flezha::bounded_operator]`).
    t.compile_fail("tests/ui/operator_sink_without_output.rs");
    t.compile_fail("tests/ui/operator_no_id.rs");
    t.compile_fail("tests/ui/operator_empty_input.rs");
    t.compile_fail("tests/ui/operator_with_hand_written_impl.rs");
    t.compile_fail("tests/ui/operator_shared_ref_state.rs");
    t.compile_fail("tests/ui/operator_unknown_loop_mode.rs");

    // A bounded operator declares its graph in `channels` and its retry ladder on the
    // edge, as an unbounded operator does.
    t.compile_fail("tests/ui/bounded_tasklets_body.rs");
    t.compile_fail("tests/ui/bounded_retry_attempts.rs");

    // Keyed/collection journaling (`expressive-state-journaling`).
    t.compile_fail("tests/ui/state_map_without_journal.rs");
    t.compile_fail("tests/ui/state_journal_type_mismatch.rs");
    t.compile_fail("tests/ui/state_journal_unknown_mode.rs");
    t.pass("tests/ui/tasklet_option_output.rs");
}
