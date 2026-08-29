//! What a timer's Entity outliving or outlived: eviction takes its pending
//! set with it, and a firing is excluded while the Entity is busy.

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::entity::timer::TimerKind;
use flezha_runner::operative::StreamOperative;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError, StateHandle};
use flezha_runner::state::{NoChange, RestoreInitPayload};

mod common;
use common::*;

// ------------------------------------------------------------ eviction

/// Spec: "Eviction discards the Entity's timers" — dynamic eviction is
/// destructive by existing decision, so the timers go with the state and the
/// wake-list entry goes with them. A later input cold-starts with nothing
/// pending.
#[test]
fn an_idle_evicted_entity_loses_its_timers() {
    let _guard = timer_test_guard();
    take_firings();
    let armed_before = flezha_runner::entity::timer::timers_armed();

    let (handle, instance) = setup::<EvictOperative>("timers-evict");
    let stream = dummy_stream();
    dispatch_input(&handle, "timers-evict", "in", b"gone".to_vec(), &stream);

    let registry = Arc::clone(instance.dynamic_registries[0].as_ref().expect("dynamic"));
    assert!(
        wait_until(
            || registry.entities.get("gone").is_some_and(|e| !e
                .value()
                .timers
                .lock()
                .unwrap()
                .is_empty()),
            Duration::from_secs(5)
        ),
        "the Entity must arm its timer before the reaper runs"
    );
    assert_eq!(
        flezha_runner::entity::timer::timers_armed(),
        armed_before + 1
    );

    // Sweep directly rather than waiting on the reaper's cadence: the seam
    // under test is what the sweep does, not when it decides to run.
    assert!(
        wait_until(
            || {
                registry.reap_sweep(Duration::from_millis(1));
                registry.entities.get("gone").is_none()
            },
            Duration::from_secs(5)
        ),
        "the Entity is evicted"
    );
    assert_eq!(
        flezha_runner::entity::timer::timers_armed(),
        armed_before,
        "and its armed timer is released with it, not left to fire into nothing"
    );
    assert!(
        firings_snapshot().is_empty(),
        "an evicted Entity's timer never fires"
    );
}

/// Dynamic keying + a TTL: the combination the design calls out as legal but
/// timer-disposable.
#[derive(Default)]
struct EvictProc;
impl Procedure for EvictProc {
    type Input = String;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::state::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "timers_evict_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn entity_idle_ttl() -> Option<Duration> {
        // Long enough that the reaper thread never beats the test to the
        // sweep; the case drives `reap_sweep` itself with a short TTL.
        Some(Duration::from_secs(3_600))
    }
    fn run(
        &mut self,
        _input: String,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        ctx.timer_after(Duration::from_secs(3_600))?;
        Ok(())
    }
    fn on_timer(
        &mut self,
        fired_at: u64,
        kind: TimerKind,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        note_firing("gone", kind, fired_at);
        Ok(())
    }
}

struct EvictOperative;
impl StreamOperative for EvictOperative {
    fn factory_id() -> &'static str {
        "timers_evict_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .key_by(|s: &String| s.clone())
            .into::<EvictProc>();
    }
}

// ------------------------------------------------------ exclusion

/// Spec: "Firing is serialized against run" — the mailbox's single-active-
/// drainer gate is the whole mechanism, so a firing that expires mid-`run`
/// waits for it. The fixture would observe an overlap if one were possible.
#[test]
fn on_timer_never_runs_concurrently_with_run() {
    let _guard = timer_test_guard();
    OVERLAPS.store(0, Ordering::SeqCst);
    INSIDE.store(0, Ordering::SeqCst);
    FIRED.store(0, Ordering::SeqCst);

    let (handle, _instance) = setup::<ExclusionOperative>("timers-exclusion");
    let stream = dummy_stream();
    for i in 0..8u64 {
        dispatch_input(
            &handle,
            "timers-exclusion",
            "in",
            serde_json::to_vec(&i).unwrap(),
            &stream,
        );
    }

    assert!(
        wait_until(
            || FIRED.load(Ordering::SeqCst) >= 4,
            Duration::from_secs(10)
        ),
        "expected several firings to interleave with the runs, saw {}",
        FIRED.load(Ordering::SeqCst)
    );
    assert_eq!(
        OVERLAPS.load(Ordering::SeqCst),
        0,
        "run and on_timer must never be inside the same Entity at once"
    );
}

static INSIDE: AtomicI64 = AtomicI64::new(0);
static OVERLAPS: AtomicUsize = AtomicUsize::new(0);
static FIRED: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct ExclusionProc;
impl ExclusionProc {
    fn enter() {
        if INSIDE.fetch_add(1, Ordering::SeqCst) != 0 {
            OVERLAPS.fetch_add(1, Ordering::SeqCst);
        }
        // Hold the section long enough for a concurrent entry to overlap it.
        let held = Instant::now();
        while held.elapsed() < Duration::from_millis(5) {
            std::hint::spin_loop();
        }
    }
    fn leave() {
        INSIDE.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Procedure for ExclusionProc {
    type Input = u64;
    type Output = ();
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::state::WallClock;
    type Config = ();
    fn name() -> &'static str {
        "timers_exclusion_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        _input: u64,
        _state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Self::enter();
        ctx.timer_after(Duration::from_millis(1))?;
        Self::leave();
        Ok(())
    }
    fn on_timer(
        &mut self,
        _fired_at: u64,
        _kind: TimerKind,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        Self::enter();
        FIRED.fetch_add(1, Ordering::SeqCst);
        Self::leave();
        Ok(())
    }
}

struct ExclusionOperative;
impl StreamOperative for ExclusionOperative {
    fn factory_id() -> &'static str {
        "timers_exclusion_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<u64>("in").into::<ExclusionProc>();
    }
}
