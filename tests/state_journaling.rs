//! Section 3.1: the journaling mutators `#[derive(flezha::State)]` generates
//! for flat collection fields. A per-entity sliding window — the shape every
//! keyed procedure has once its Entity key *is* what a map would have keyed by
//! — must journal one delta per mutation, not a whole-collection set.

use std::collections::VecDeque;

use flezha_runner::state::Change;
use flezha_runner::DerivedState;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, flezha_runner::State)]
struct WindowState {
    ticks: VecDeque<f64>,
    bars: Vec<u32>,
    last: f64,
}

fn replay(changes: &[WindowStateChange]) -> WindowState {
    let mut state = WindowState::default();
    for change in changes {
        <WindowState as DerivedState>::apply_change(&mut state, change);
    }
    state
}

/// fails when: a flat `VecDeque` field loses its per-element deltas and can
/// only be journaled as a whole-collection set — every tick of a long window
/// would then write the entire window to the WAL.
#[test]
fn a_flat_deque_journals_one_delta_per_mutation() {
    let changes = vec![
        WindowStateChange::PushBackTicks(1.0),
        WindowStateChange::PushBackTicks(2.0),
        WindowStateChange::PushBackTicks(3.0),
        WindowStateChange::PopFrontTicks(2),
    ];
    assert_eq!(replay(&changes).ticks, VecDeque::from(vec![3.0]));
}

/// fails when: `pop_front` over-drains a shorter window — replaying a
/// pop recorded against a longer window would panic or empty the state.
#[test]
fn popping_more_than_the_window_holds_is_saturating() {
    let changes = vec![
        WindowStateChange::PushBackTicks(1.0),
        WindowStateChange::PopFrontTicks(9),
    ];
    assert!(replay(&changes).ticks.is_empty());
}

/// fails when: the flat `Vec` push or the scalar set stops composing with the
/// deque deltas in one generated change enum.
#[test]
fn flat_vec_and_scalar_fields_share_the_change_enum() {
    let changes = vec![
        WindowStateChange::PushBars(7),
        WindowStateChange::PushBackTicks(1.5),
        WindowStateChange::SetLast(1.5),
    ];
    let state = replay(&changes);
    assert_eq!(state.bars, vec![7]);
    assert_eq!(state.ticks, VecDeque::from(vec![1.5]));
    assert_eq!(state.last, 1.5);
}

/// fails when: a generated change stops round-tripping through serde — the
/// WAL record could be written but not replayed.
#[test]
fn generated_changes_round_trip_through_serde() {
    for change in [
        WindowStateChange::PushBackTicks(1.0),
        WindowStateChange::PopFrontTicks(3),
        WindowStateChange::SetTicks(VecDeque::from(vec![9.0])),
    ] {
        let json = serde_json::to_string(&change).expect("serialize");
        let back: WindowStateChange = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, change);
    }
}

fn _change_bound_is_satisfied<C: Change>() {}
fn _assert_bounds() {
    _change_bound_is_satisfied::<WindowStateChange>();
}
