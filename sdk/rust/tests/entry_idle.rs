//! `expose-entry-input-idle-signal`: the last-delivery instant an entry
//! channel carries, and the idle observer registered against it.
//!
//! Drives the real seams — `dispatch` for the stamp, the runner's own tick
//! thread for the episodes — because the value of the feature is that both
//! are wired, not that the state machine is right in isolation (that is
//! covered by the unit cases in `src/entry_idle.rs`).

use flezha_runner::{Flezha, IdleObserver, StreamOperative};

mod common;
use common::prelude::*;
use common::{dispatch_input, dummy_stream, setup, stays_quiet, wait_until};

/// Short enough to keep the suite quick, long enough that the derived
/// 250 ms tick samples an episode several times before it is asserted on.
const THRESHOLD: Duration = Duration::from_millis(300);
/// A wait that always outlasts an episode plus its tick.
const SETTLE: Duration = Duration::from_secs(3);

// ----- Fixtures -----

static OBSERVED: Mutex<Vec<String>> = Mutex::new(Vec::new());

test_procedure!(Sink, "entry_idle_sink", String => (), (), |input, _state, _ctx| {
    OBSERVED.lock().unwrap().push(input);
    Ok(())
});

/// Channel names are the observation key, so each case declares its own
/// operative to keep the process-global cells independent under `cargo test`'s
/// parallel harness.
macro_rules! probe_operative {
    ($op:ident, $factory:literal, $channel:literal) => {
        struct $op;
        impl StreamOperative for $op {
            fn factory_id() -> &'static str {
                $factory
            }
            fn channels(c: &ChannelBuilder) {
                c.input::<String>($channel).into::<Sink>();
            }
        }
    };
}

probe_operative!(StampOperative, "entry_idle_stamp", "stamp_in");
probe_operative!(EpisodeOperative, "entry_idle_episode", "episode_in");
probe_operative!(PanicOperative, "entry_idle_panic", "panic_in");

/// What an observer saw. Shared with the test thread behind an `Arc` because
/// registration takes the observer by value.
#[derive(Default)]
struct Counts {
    idle: AtomicUsize,
    resumed: AtomicUsize,
    gaps: Mutex<Vec<Duration>>,
}

struct Probe(Arc<Counts>);

impl IdleObserver for Probe {
    fn idle(&self, gap: Duration) {
        self.0.gaps.lock().unwrap().push(gap);
        self.0.idle.fetch_add(1, Ordering::SeqCst);
    }
    fn resumed(&self, _gap: Duration) {
        self.0.resumed.fetch_add(1, Ordering::SeqCst);
    }
}

/// An observer that always unwinds, plus the count of how often it was asked.
struct PanickingProbe(Arc<AtomicUsize>);

impl IdleObserver for PanickingProbe {
    fn idle(&self, _gap: Duration) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("entry-idle observer under test");
    }
}

// ----- Cases -----

#[test]
fn a_delivery_stamps_its_channel_and_a_channel_with_no_frame_reads_absent() {
    let (handle, _instance) = setup::<StampOperative>("entry-idle-stamp-1");
    let stream = dummy_stream();
    let runner = Flezha::create_runner("entry-idle-stamp").expect("runner");

    assert_eq!(
        runner.last_entry_delivery("stamp_in"),
        None,
        "absent before the first frame — not a zero or start-time value"
    );
    assert_eq!(
        runner.last_entry_delivery("no_such_channel"),
        None,
        "a channel nothing declares has had nothing arrive either"
    );

    let before = std::time::Instant::now();
    dispatch_input(
        &handle,
        "entry-idle-stamp-1",
        "stamp_in",
        b"one".to_vec(),
        &stream,
    );

    let first = runner.last_entry_delivery("stamp_in").expect("stamped");
    assert!(first >= before, "the stamp is at or after the delivery");

    // The clock has moved past the first stamp before the second delivery.
    assert!(wait_until(
        || std::time::Instant::now() > first,
        Duration::from_secs(1)
    ));
    dispatch_input(
        &handle,
        "entry-idle-stamp-1",
        "stamp_in",
        b"two".to_vec(),
        &stream,
    );
    let second = runner.last_entry_delivery("stamp_in").expect("stamped");
    assert!(second > first, "a later delivery advances the stamp");
}

#[test]
fn an_episode_fires_once_then_a_resume_re_arms_the_next_one() {
    let counts = Arc::new(Counts::default());
    // Registered before the class is even defined: the observation cell is
    // interned by name, so the watch and the later topology share one cell.
    let runner = Flezha::create_runner("entry-idle-episode").expect("runner");
    runner.on_entry_idle("episode_in", THRESHOLD, Probe(Arc::clone(&counts)));

    let (handle, _instance) = setup::<EpisodeOperative>("entry-idle-episode-1");
    let stream = dummy_stream();

    // Cold start: no frame has ever arrived, so however long the tick runs,
    // there is no episode to report.
    assert!(
        stays_quiet(|| counts.idle.load(Ordering::SeqCst) > 0, THRESHOLD * 3),
        "cold start never fires"
    );

    dispatch_input(
        &handle,
        "entry-idle-episode-1",
        "episode_in",
        b"one".to_vec(),
        &stream,
    );
    assert!(
        wait_until(|| counts.idle.load(Ordering::SeqCst) == 1, SETTLE),
        "the gap past the threshold fires the episode"
    );
    let gap = counts.gaps.lock().unwrap()[0];
    assert!(
        gap > THRESHOLD,
        "the reported gap exceeds the threshold: {gap:?}"
    );

    assert!(
        stays_quiet(|| counts.idle.load(Ordering::SeqCst) > 1, THRESHOLD * 3),
        "latched — the episode is reported once, not once per tick"
    );
    assert_eq!(counts.resumed.load(Ordering::SeqCst), 0);

    dispatch_input(
        &handle,
        "entry-idle-episode-1",
        "episode_in",
        b"two".to_vec(),
        &stream,
    );
    assert!(
        wait_until(|| counts.resumed.load(Ordering::SeqCst) == 1, SETTLE),
        "the arriving frame ends the episode"
    );
    assert!(
        wait_until(|| counts.idle.load(Ordering::SeqCst) == 2, SETTLE),
        "the latch re-armed, so the next silence fires again"
    );
}

#[test]
fn a_panicking_observer_is_contained_and_delivery_carries_on() {
    let calls = Arc::new(AtomicUsize::new(0));
    let runner = Flezha::create_runner("entry-idle-panic").expect("runner");
    runner.on_entry_idle("panic_in", THRESHOLD, PanickingProbe(Arc::clone(&calls)));

    let (handle, _instance) = setup::<PanicOperative>("entry-idle-panic-1");
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "entry-idle-panic-1",
        "panic_in",
        b"before".to_vec(),
        &stream,
    );
    assert!(
        wait_until(|| calls.load(Ordering::SeqCst) == 1, SETTLE),
        "the first episode reached the observer"
    );

    dispatch_input(
        &handle,
        "entry-idle-panic-1",
        "panic_in",
        b"after".to_vec(),
        &stream,
    );
    assert!(
        wait_until(
            || {
                let seen = OBSERVED.lock().unwrap();
                seen.iter().any(|v| v == "after")
            },
            SETTLE
        ),
        "delivery is unaffected by the observer that unwound"
    );
    assert!(
        wait_until(|| calls.load(Ordering::SeqCst) == 2, SETTLE),
        "and the next episode still invokes it"
    );
}
