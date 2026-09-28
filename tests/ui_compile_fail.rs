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

    // `#[flezha::tasklet]` on an `impl Tasklet` block, and the typed context.
    t.compile_fail("tests/ui/tasklet_on_function.rs");
    t.compile_fail("tests/ui/tasklet_cell_key_removed.rs");
    t.compile_fail("tests/ui/tasklet_operator_keys.rs");
    t.compile_fail("tests/ui/cell_keys_without_cells_for.rs");
    t.compile_fail("tests/ui/tasklet_two_clocks.rs");
    t.compile_fail("tests/ui/tasklet_item_written_twice.rs");
    t.compile_fail("tests/ui/tasklet_beside_short_form.rs");
    t.compile_fail("tests/ui/state_direct_mutation.rs");
    t.compile_fail("tests/ui/emit_wrong_type.rs");
    t.compile_fail("tests/ui/operator_fan_out.rs");
    t.compile_fail("tests/ui/operator_empty_entry_name.rs");
    t.compile_fail("tests/ui/register_factory_of_both_kinds.rs");

    // The short-form attributes (`#[flezha::unbounded_operator]` /
    // `#[flezha::bounded_operator]`).
    t.compile_fail("tests/ui/operator_sink_without_output.rs");
    t.compile_fail("tests/ui/operator_no_id.rs");
    t.compile_fail("tests/ui/operator_empty_input.rs");
    t.compile_fail("tests/ui/operator_with_hand_written_impl.rs");
    t.compile_fail("tests/ui/operator_shared_ref_state.rs");
    t.compile_fail("tests/ui/operator_unknown_loop_mode.rs");
    t.compile_fail("tests/ui/short_form_state_on_function.rs");
    t.compile_fail("tests/ui/short_form_operator_is_not_a_tasklet.rs");

    // A bounded operator declares its graph in `channels` and its retry ladder on the
    // edge, as an unbounded operator does.
    t.compile_fail("tests/ui/bounded_tasklets_body.rs");
    t.compile_fail("tests/ui/bounded_retry_attempts.rs");
}
