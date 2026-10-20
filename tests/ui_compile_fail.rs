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
    t.compile_fail("tests/ui/stream_handle_cannot_escape.rs");
    t.compile_fail("tests/ui/entry_stream_has_no_plugin_sink.rs");
    t.compile_fail("tests/ui/output_without_byte_form.rs");

    // `#[flezha::tasklet]` on an `impl Tasklet` block, and the typed context.
    t.compile_fail("tests/ui/tasklet_on_function.rs");
    t.compile_fail("tests/ui/tasklet_cell_key_removed.rs");
    t.compile_fail("tests/ui/tasklet_relay_keys.rs");
    t.compile_fail("tests/ui/cell_keys_without_cells_for.rs");
    t.compile_fail("tests/ui/tasklet_two_clocks.rs");
    t.compile_fail("tests/ui/tasklet_item_written_twice.rs");
    t.compile_fail("tests/ui/tasklet_beside_short_form.rs");
    t.compile_fail("tests/ui/state_direct_mutation.rs");
    t.compile_fail("tests/ui/emit_wrong_type.rs");
    t.compile_fail("tests/ui/relay_fan_out.rs");
    t.compile_fail("tests/ui/relay_empty_entry_name.rs");

    // The short-form attribute `#[flezha::relay]`.
    t.compile_fail("tests/ui/relay_output_without_type.rs");
    t.compile_fail("tests/ui/relay_no_factory_id.rs");
    t.compile_fail("tests/ui/relay_empty_input.rs");
    t.compile_fail("tests/ui/relay_with_hand_written_impl.rs");
    t.compile_fail("tests/ui/relay_shared_ref_state.rs");
    t.compile_fail("tests/ui/relay_unknown_each.rs");
    t.compile_fail("tests/ui/relay_misplaced_setting.rs");
    t.compile_fail("tests/ui/short_form_state_on_function.rs");
    t.compile_fail("tests/ui/short_form_relay_is_not_a_tasklet.rs");

    // The settings tree: a setting under a parent that does not carry it is
    // a type error. Each is a row of the reference refusal set.
    t.compile_fail("tests/ui/settings_each_under_flow.rs");
    t.compile_fail("tests/ui/settings_ack_under_at_least_once.rs");
    t.compile_fail("tests/ui/settings_ack_under_steps.rs");
    t.compile_fail("tests/ui/settings_correction_under_steps.rs");
    t.compile_fail("tests/ui/settings_exactly_under_steps.rs");
    t.compile_fail("tests/ui/settings_sleep_after_in_memory.rs");
    t.compile_fail("tests/ui/settings_kind_literal.rs");

    // A steps class declares its graph in `channels` and its retry ladder on
    // the edge, as a flow class does.
    t.compile_fail("tests/ui/steps_tasklets_body.rs");
    t.compile_fail("tests/ui/steps_retry_attempts.rs");
}
