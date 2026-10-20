//! The one tasklet form: what `#[flezha::tasklet]` writes on an
//! `impl Tasklet` block, the typed `Ctx`, a change that applies itself, and
//! the clock a block declares by what it writes.

use std::any::TypeId;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::cell::flusher::encode_change;
use crate::cell::header::header_template_for;
use crate::cell::restore::{init_cell_with_payload, RestoreInitPayload};
use crate::cell::timer::{TimerFiring, TimerKind};
use crate::tasklet::clock::evaluate;
use crate::tasklet::{Change, ClockKind, Ctx, NoChange, Tasklet, TaskletError};
use crate::testing::TaskletTestHarness;

#[derive(Default)]
struct Echo;

#[flezha_runner::tasklet(name = "echo")]
impl Tasklet for Echo {
    type Input = String;
    type Output = String;

    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(format!("echo: {input}"))
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
struct Tally {
    total: u64,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Added {
    delta: u64,
}

impl Change<Tally> for Added {
    fn apply_to(&self, state: &mut Tally) {
        state.total += self.delta;
    }
}

#[derive(Default)]
struct Counter;

#[flezha_runner::tasklet(name = "counter")]
impl Tasklet for Counter {
    type Input = u64;
    type Output = u64;
    type State = Tally;
    type Change = Added;

    fn run(&mut self, delta: u64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Added { delta });
        ctx.emit(ctx.state().total)
    }
}

#[test]
fn a_stateless_block_takes_every_default() {
    assert_eq!(Echo::NAME, "echo");
    assert_eq!(Echo::CLOCK, ClockKind::Wall);
    assert_eq!(
        TypeId::of::<<Echo as Tasklet>::Change>(),
        TypeId::of::<NoChange>()
    );
    assert_eq!(TypeId::of::<<Echo as Tasklet>::State>(), TypeId::of::<()>());
    Echo::init_state(None);
    const { assert!(!Echo::COMPENSATE_OVERRIDDEN && !Echo::CELLS_FOR_OVERRIDDEN) };
    let mut harness = TaskletTestHarness::<Echo>::new();
    harness.apply_inputs(["hi".to_string()]);
    harness.expect_emits(&["echo: hi".to_string()]);
}

#[test]
fn a_change_is_applied_and_recorded() {
    let mut harness = TaskletTestHarness::<Counter>::new();
    harness.apply_inputs([5, 3]);
    harness.replay_equals().expect("apply_to is deterministic");
    harness
        .expect_changes(&[Added { delta: 5 }, Added { delta: 3 }])
        .expect_state(&Tally { total: 8 })
        .expect_emits(&[5, 8]);
}

#[derive(Default)]
struct Peek;

#[flezha_runner::tasklet(name = "peek")]
impl Tasklet for Peek {
    type Input = ();
    type Output = u64;
    type State = Tally;
    type Change = Added;

    fn run(&mut self, _: (), ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(ctx.state().total)
    }
}

#[test]
fn reading_the_state_records_nothing() {
    let mut harness = TaskletTestHarness::<Peek>::with_initial_state(Tally { total: 7 });
    harness.apply_inputs([(), ()]);
    assert!(harness.recorded_changes().is_empty());
    harness
        .expect_emits(&[7, 7])
        .expect_state(&Tally { total: 7 });
}

/// The change log a restart reads: each change encoded as the flusher writes
/// it, framed and stamped under `S`'s header.
fn change_log<S: Tasklet>(changes: &[S::Change]) -> Vec<u8> {
    let template = header_template_for::<S>();
    changes
        .iter()
        .enumerate()
        .flat_map(|(seq, change)| {
            let body = encode_change(change);
            flz_core::restore::frame_record(seq as u64, &template.stamp(&body), &body)
        })
        .collect()
}

fn restore<S: Tasklet>(cell_key: Option<&str>, log: &[u8]) -> S::State {
    let payload = RestoreInitPayload {
        snapshot_header: &[],
        snapshot_bytes: &[],
        change_log_bytes: log,
    };
    init_cell_with_payload::<S>(cell_key, Some(&payload))
        .expect("the log restores")
        .0
}

#[test]
fn a_restart_folds_the_log_through_apply_to() {
    let mut harness = TaskletTestHarness::<Counter>::new();
    harness.apply_inputs([4, 5, 6]);
    let log = change_log::<Counter>(harness.recorded_changes());
    assert_eq!(restore::<Counter>(None, &log), *harness.state());
}

/// `Counter` under another type name, as after a rename. Its saved state is
/// keyed by the name it declares, which is unchanged.
#[derive(Default)]
struct RenamedCounter;

#[flezha_runner::tasklet(name = "counter")]
impl Tasklet for RenamedCounter {
    type Input = u64;
    type State = Tally;
    type Change = Added;

    fn run(&mut self, delta: u64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Added { delta });
        Ok(())
    }
}

#[test]
fn saved_state_survives_a_rename_of_the_tasklet_type() {
    let mut harness = TaskletTestHarness::<Counter>::new();
    harness.apply_inputs([2, 40]);
    let log = change_log::<Counter>(harness.recorded_changes());
    assert_eq!(Counter::NAME, RenamedCounter::NAME);
    assert_eq!(restore::<RenamedCounter>(None, &log), Tally { total: 42 });
}

#[derive(Default)]
struct Stamp;

#[flezha_runner::tasklet(name = "stamp")]
impl Tasklet for Stamp {
    type Input = ();
    type Output = u64;

    fn run(&mut self, _: (), ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.emit(ctx.now())
    }
}

#[test]
fn the_default_clock_is_the_wall_clock() {
    assert_eq!(Stamp::CLOCK, ClockKind::Wall);
    let mut harness = TaskletTestHarness::<Stamp>::new().with_wall_clock(1_234);
    harness.apply_inputs([()]);
    harness.expect_emits(&[1_234]);
    assert_eq!(evaluate::<Stamp>(Some(&()), || 9, || 1), 9);
}

#[derive(Clone)]
struct Trade {
    at: u64,
}

/// Emits its clock, and arms an event-time timer ten milliseconds on.
#[derive(Default)]
struct QuietSymbol;

#[flezha_runner::tasklet(name = "quiet_symbol")]
impl Tasklet for QuietSymbol {
    type Input = Trade;
    type Output = u64;

    fn event_time(trade: &Trade) -> u64 {
        trade.at
    }

    fn run(&mut self, _: Trade, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.timer_at_event_time(ctx.now() + 10)?;
        ctx.emit(ctx.now())
    }

    fn on_timer(
        &mut self,
        firing: TimerFiring,
        ctx: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        ctx.emit(firing.scheduled_at)
    }
}

#[test]
fn writing_event_time_makes_the_clock_event_time() {
    assert_eq!(QuietSymbol::CLOCK, ClockKind::EventTime);
    let trade = Trade {
        at: 1_720_000_000_000,
    };
    assert_eq!(evaluate::<QuietSymbol>(Some(&trade), || 9, || 1), trade.at);
    assert_eq!(evaluate::<QuietSymbol>(None, || 9, || 1), 0);
    let mut harness = TaskletTestHarness::<QuietSymbol>::new();
    harness.apply_inputs([trade.clone()]);
    assert_eq!(
        harness.pending_timers(TimerKind::EventTime),
        [trade.at + 10]
    );
    harness
        .replay_emits_equal(std::slice::from_ref(&trade))
        .expect("event time is replay-stable");
    harness.advance_event_time(trade.at + 10);
    harness.expect_emits(&[trade.at, trade.at + 10]);
}

#[derive(Default)]
struct Sequenced;

#[flezha_runner::tasklet(name = "sequenced", logical_clock)]
impl Tasklet for Sequenced {
    type Input = u64;
    type Output = u64;
    type State = Tally;
    type Change = Added;

    fn run(&mut self, delta: u64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Added { delta });
        ctx.emit(ctx.now())
    }
}

#[test]
fn logical_clock_serves_the_change_seq() {
    assert_eq!(Sequenced::CLOCK, ClockKind::Logical);
    let mut harness = TaskletTestHarness::<Sequenced>::new();
    harness.apply_inputs([1, 1, 1]);
    harness.expect_emits(&[0, 1, 2]);
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
struct Window {
    key: String,
}

#[derive(Serialize, Deserialize)]
struct Rekeyed;

impl Change<Window> for Rekeyed {
    fn apply_to(&self, state: &mut Window) {
        state.key.push('!');
    }
}

#[derive(Default)]
struct KeyedStart;

#[flezha_runner::tasklet(name = "keyed_start")]
impl Tasklet for KeyedStart {
    type Input = ();
    type State = Window;
    type Change = Rekeyed;

    fn init_state(cell_key: Option<&str>) -> Window {
        Window {
            key: cell_key.unwrap_or("none").to_string(),
        }
    }

    fn run(&mut self, _: (), _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

#[test]
fn a_new_cell_state_is_the_default() {
    assert_eq!(
        *TaskletTestHarness::<Counter>::new().state(),
        Tally::default()
    );
}

#[test]
fn a_written_init_state_receives_the_cell_key() {
    assert_eq!(restore::<KeyedStart>(Some("1h"), &[]).key, "1h");
    assert_eq!(TaskletTestHarness::<KeyedStart>::new().state().key, "none");
}

static APPLIED: AtomicU64 = AtomicU64::new(0);

/// A change whose fold reads ambient state: every call lands differently.
#[derive(Serialize, Deserialize)]
struct Drifting;

impl Change<Tally> for Drifting {
    fn apply_to(&self, state: &mut Tally) {
        state.total += APPLIED.fetch_add(1, Ordering::Relaxed) + 1;
    }
}

#[derive(Default)]
struct DriftingTasklet;

#[flezha_runner::tasklet(name = "drifting")]
impl Tasklet for DriftingTasklet {
    type Input = ();
    type State = Tally;
    type Change = Drifting;

    fn run(&mut self, _: (), ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Drifting);
        Ok(())
    }
}

#[test]
fn replay_equals_detects_a_non_deterministic_apply_to() {
    let mut harness = TaskletTestHarness::<DriftingTasklet>::new();
    harness.apply_inputs([()]);
    let mismatch = harness.replay_equals().expect_err("the fold drifts");
    assert!(mismatch.to_string().contains("state divergence"));
}

/// A change that loses its field on the way to the WAL.
#[derive(Serialize, Deserialize)]
struct Unsaved {
    #[serde(skip)]
    delta: u64,
}

impl Change<Tally> for Unsaved {
    fn apply_to(&self, state: &mut Tally) {
        state.total += self.delta;
    }
}

#[derive(Default)]
struct UnsavedTasklet;

#[flezha_runner::tasklet(name = "unsaved")]
impl Tasklet for UnsavedTasklet {
    type Input = u64;
    type State = Tally;
    type Change = Unsaved;

    fn run(&mut self, delta: u64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(Unsaved { delta });
        Ok(())
    }
}

#[test]
fn replay_folds_what_the_runtime_would_persist() {
    let mut harness = TaskletTestHarness::<UnsavedTasklet>::new();
    harness.apply_inputs([9]);
    harness
        .replay_equals()
        .expect_err("a change that loses its field restores a different state");
}

#[derive(Default)]
struct Evens;

#[flezha_runner::tasklet(name = "evens")]
impl Tasklet for Evens {
    type Input = u64;
    type Output = u64;

    fn run(&mut self, n: u64, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        if n.is_multiple_of(2) {
            ctx.emit(n)?;
        }
        Ok(())
    }
}

#[test]
fn a_run_that_calls_no_emit_emits_nothing() {
    let mut harness = TaskletTestHarness::<Evens>::new();
    harness.apply_inputs([1, 2, 3]);
    harness.expect_emits(&[2]);
}

#[derive(Default)]
struct Knobs;

#[flezha_runner::tasklet(
    name = "knobs",
    max_cells = 10,
    idle_ttl_seconds = 5,
    mailbox_capacity = 64,
    schema_version = 3,
    min_readable_schema_version = 2,
    compact_after_bytes = 4096,
    max_timers_per_cell = 7
)]
impl Tasklet for Knobs {
    type Input = ();

    fn run(&mut self, _: (), _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

#[test]
fn each_setting_key_takes_effect() {
    assert_eq!(Knobs::max_cells(), 10);
    const { assert!(Knobs::MAX_CELLS_OVERRIDDEN) };
    assert_eq!(Knobs::idle_ttl(), Some(Duration::from_secs(5)));
    assert_eq!(Knobs::mailbox_capacity(), 64);
    const { assert!(Knobs::MAILBOX_CAPACITY_OVERRIDDEN) };
    assert_eq!(Knobs::SCHEMA_VERSION, 3);
    assert_eq!(Knobs::MIN_READABLE_SCHEMA_VERSION, 2);
    assert_eq!(Knobs::compact_after_bytes(), 4096);
    assert_eq!(Knobs::max_timers_per_cell(), 7);
    assert!(Knobs::cell_keys().is_empty());
}

#[derive(Default)]
struct StaticEntry;

#[flezha_runner::tasklet(name = "static_entry_tasklet", cell_keys = ["a", "b", "c", "d"])]
impl Tasklet for StaticEntry {
    type Input = String;

    fn cells_for(input: &String) -> Vec<&'static str> {
        if input.starts_with('a') {
            vec!["a"]
        } else {
            vec!["b", "c"]
        }
    }

    fn run(&mut self, _: String, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }

    fn mailbox_capacity() -> usize {
        16
    }

    fn compensate(
        &mut self,
        _: Vec<std::sync::Arc<()>>,
        _: &mut Ctx<'_, Self>,
    ) -> Result<(), TaskletError> {
        Ok(())
    }
}

#[test]
fn a_written_hook_sets_its_flag() {
    assert_eq!(StaticEntry::cell_keys(), ["a", "b", "c", "d"]);
    const { assert!(StaticEntry::CELLS_FOR_OVERRIDDEN) };
    assert_eq!(StaticEntry::cells_for(&"x".to_string()), ["b", "c"]);
    const { assert!(StaticEntry::MAILBOX_CAPACITY_OVERRIDDEN) };
    const { assert!(StaticEntry::COMPENSATE_OVERRIDDEN) };
}
