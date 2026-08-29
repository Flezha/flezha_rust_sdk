//! Late-input correction: a replayed slice is re-forwarded in event-time
//! order, not arrival order, and the replay never lowers the high-water.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bi::OperativeDispatchHandle;
use flezha_runner::channel::ChannelBuilder;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{NoChange, RestoreInitPayload, StateHandle};
use flezha_runner::{__dispatch_internals as bi, StreamOperative};
use flz_transport::transport::FlezhaStream;

mod common;
use common::*;

// ----- Late-input correction: the event-time-ordered replay drain -----
//
// The load-bearing behaviour of `stream-late-input-correction`. Re-forwarding
// a replayed slice in ARRIVAL order would put the late trade back after the
// trades that overtook it and recompute exactly the wrong answer the correction
// exists to repair. So the Entity holds the slice until the controller closes
// it, orders it by extracted event time, and only then dispatches.

static REPLAY_OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct TradeTime;
impl flezha_runner::procedure::TimeExtractor<String> for TradeTime {
    fn extract(input: &String) -> u64 {
        input.parse().unwrap_or(0)
    }
}

#[derive(Default)]
struct ReplayProcedure;
impl Procedure for ReplayProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::procedure::EventTimeFrom<TradeTime>;
    type Config = ();
    fn name() -> &'static str {
        "replay_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        REPLAY_OBSERVED.lock().unwrap().push(input);
        Ok(())
    }
}

struct ReplayOperative;
impl StreamOperative for ReplayOperative {
    fn factory_id() -> &'static str {
        "replay_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.correction_window(600_000);
        c.input::<String>("input")
            .into::<ReplayProcedure>()
            .named("out")
            .keyed()
            .sink("kafka");
    }
}

#[test]
fn a_replayed_slice_dispatches_in_event_time_order() {
    REPLAY_OBSERVED.lock().unwrap().clear();
    let (handle, _instance) = setup::<ReplayOperative>("replay-instance");
    let stream = dummy_stream();

    // The controller re-forwards in offset (arrival) order — the late "200"
    // having arrived after "300" is exactly why the correction is running.
    for (offset, event_time) in [(1, "300"), (2, "100"), (3, "200")] {
        dispatch_marked(
            &handle,
            "replay-instance",
            "input",
            event_time.as_bytes().to_vec(),
            &stream,
            &[("flz.replay", "1"), ("flz.offset", &offset.to_string())],
        );
    }

    // Held, not dispatched: the slice is still open, so ordering it would be
    // guessing.
    assert!(stays_quiet(
        || !REPLAY_OBSERVED.lock().unwrap().is_empty(),
        Duration::from_millis(100)
    ));
    assert!(
        REPLAY_OBSERVED.lock().unwrap().is_empty(),
        "an open slice must not dispatch"
    );

    // The controller closes the slice. A broadcast, not a mark on the last
    // input — an input reaches one Entity by key, so a mark would strand every
    // other Entity's buffered slice.
    dispatch_marked(
        &handle,
        "replay-instance",
        "input",
        Vec::new(),
        &stream,
        &[("flz.replay_end", "1")],
    );

    // Post-slice live traffic. It must land AFTER the whole slice even though
    // it arrives while the slice is still buffered: the slice is older, and a
    // live input that jumped it would re-create the mis-ordering the correction
    // just repaired.
    dispatch_marked(
        &handle,
        "replay-instance",
        "input",
        b"600".to_vec(),
        &stream,
        &[("flz.offset", "4")],
    );

    assert!(
        wait_until(
            || REPLAY_OBSERVED.lock().unwrap().len() == 4,
            Duration::from_secs(5)
        ),
        "closed slice did not drain: {:?}",
        REPLAY_OBSERVED.lock().unwrap()
    );

    assert_eq!(
        REPLAY_OBSERVED.lock().unwrap().as_slice(),
        ["100", "200", "300", "600"],
        "the slice must fold in event-time order, with live traffic after it"
    );
}

static HIGHWATER_OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());

#[derive(Default)]
struct HighWaterProcedure;
impl Procedure for HighWaterProcedure {
    type Input = String;
    type Output = String;
    type State = ();
    type Change = NoChange;
    type Clock = flezha_runner::procedure::EventTimeFrom<TradeTime>;
    type Config = ();
    fn name() -> &'static str {
        "high_water_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(
        &mut self,
        input: String,
        _state: StateHandle<'_, Self>,
        _ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        HIGHWATER_OBSERVED.lock().unwrap().push(input);
        Ok(())
    }
}

struct HighWaterOperative;
impl StreamOperative for HighWaterOperative {
    fn factory_id() -> &'static str {
        "high_water_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.correction_window(600_000);
        c.input::<String>("input")
            .into::<HighWaterProcedure>()
            .named("out")
            .keyed()
            .sink("kafka");
    }
}

fn dispatch_hw(
    handle: &OperativeDispatchHandle,
    payload: &str,
    stream: &Arc<FlezhaStream>,
    marks: &[(&str, &str)],
) {
    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    for (k, v) in marks {
        md.insert(k.to_string(), v.to_string());
    }
    handle.dispatch(
        "high-water-instance",
        payload.as_bytes().to_vec(),
        &md,
        Arc::clone(stream),
    );
}

#[test]
fn a_replay_does_not_lower_the_event_time_high_water() {
    // Regression: `dispatch_replay_slice` used to reset the high-water to -1 and
    // rebuild it from the slice alone. Every replay is preceded by a restore that
    // already set the high-water from the state it restored — so on a crash-resume
    // whose unconsumed tail carries a LOWER max event time than the recovered
    // state, that reset silently lowered the high-water, and a genuinely late input
    // afterwards would read as on-time. A missed correction, and a silent one.
    //
    // Asserted on the high-water itself rather than on dispatch behaviour: with no
    // controller transport installed a correctable input falls back to being
    // processed as an arrival, so the observable dispatch would not distinguish the
    // two. The invariant is about the water mark.
    HIGHWATER_OBSERVED.lock().unwrap().clear();
    let (handle, instance) = setup::<HighWaterOperative>("high-water-instance");
    let stream = dummy_stream();

    // Live traffic establishes a high-water of 900.
    dispatch_hw(&handle, "900", &stream, &[("flz.offset", "1")]);
    assert!(wait_until(
        || HIGHWATER_OBSERVED.lock().unwrap().len() == 1,
        Duration::from_secs(5)
    ));

    let slot = Arc::clone(&instance.entities[0][0]);
    assert_eq!(slot.event_time_high_water.load(Ordering::Acquire), 900);

    // A replayed slice whose newest event time (800) is BEHIND the high-water.
    dispatch_hw(
        &handle,
        "800",
        &stream,
        &[("flz.replay", "1"), ("flz.offset", "2")],
    );
    dispatch_hw(&handle, "", &stream, &[("flz.replay_end", "1")]);
    assert!(wait_until(
        || HIGHWATER_OBSERVED.lock().unwrap().len() == 2,
        Duration::from_secs(5)
    ));

    assert_eq!(
        slot.event_time_high_water.load(Ordering::Acquire),
        900,
        "the replay lowered the high-water; a later input at 850 would now read as on-time"
    );
}
