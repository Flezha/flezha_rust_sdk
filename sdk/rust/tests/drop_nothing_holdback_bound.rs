//! The push holdback's bound, and the one park it leaves on an emit path:
//! past the bound a typed emit parks, and the producer's own earlier pushes
//! still go first, so the sequence the holdback exists to preserve survives.
//!
//! Its own test binary because it needs the opposite host shape from
//! `drop_nothing_holdback`: a pool the starvation supervisor may GROW. That
//! is the whole design of the bound — past it a typed emit parks, and the
//! safety valve is the supervisor, not re-entrancy. An FFI host never reaches
//! this case at all: its outputs are materialized as bytes before the core
//! sees them, so its holdback is bounded by one completion's output count.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use flezha_runner::channel::{ChannelBuilder, OverflowPolicy};
use flezha_runner::operative::StreamOperative;

mod common;
use common::*;

const EMITS: usize = 40;
const BOUND: usize = 2;

static SEEN: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

test_procedure!(BoundHead, "holdback_bound_head", String => String, (), |_input, _state, ctx| {
    for i in 0..EMITS {
        ctx.emit(format!("b{i}"))?;
    }
    Ok(())
});

test_procedure!(BoundTail, "holdback_bound_tail", String => (), (), cap 1, |input, _state, _ctx| {
    SEEN.lock().unwrap().push(input);
    Ok(())
});

struct BoundOperative;
impl StreamOperative for BoundOperative {
    fn factory_id() -> &'static str {
        "holdback_bound_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<BoundHead>()
            .named("bound_out")
            .on_full(OverflowPolicy::DropNothing)
            .into::<BoundTail>();
    }
}

static BLOCKED_BEFORE: AtomicUsize = AtomicUsize::new(0);

/// An invocation emitting far past the bound still delivers everything, in
/// order, and does not deadlock: the emit waits for its OWN earlier pushes to
/// land rather than overtaking them, which is what stops the bound from
/// reordering the sequence it exists to bound.
#[test]
fn emitting_past_the_bound_delivers_everything_in_order() {
    flz_core::admission::set_nonparking_for_tests(true);
    flz_core::admission::set_holdback_max_for_tests(BOUND);
    SEEN.lock().unwrap().clear();
    BLOCKED_BEFORE.store(
        flz_core::entity::pushes_blocked_total() as usize,
        Ordering::Release,
    );

    let (handle, instance) = setup::<BoundOperative>("holdback-bound");
    let stream = dummy_stream();
    dispatch_input(&handle, "holdback-bound", "input", b"go".to_vec(), &stream);

    let drained = wait_until(
        || SEEN.lock().unwrap().len() == EMITS,
        Duration::from_secs(30),
    );
    // Restore the bound before asserting so a failure cannot leak it.
    flz_core::admission::set_holdback_max_for_tests(usize::MAX);
    assert!(
        drained,
        "a holdback with a bound lost or wedged work: {} of {EMITS} arrived",
        SEEN.lock().unwrap().len()
    );
    let seen = SEEN.lock().unwrap().clone();
    let expected: Vec<String> = (0..EMITS).map(|i| format!("b{i}")).collect();
    assert_eq!(seen, expected, "the bound's park reordered the sequence");

    // Nothing was discarded, and the backpressure was actually observed —
    // otherwise the test proves only that a fast consumer keeps up.
    let tail = &instance.entities[1][0];
    assert_eq!(
        tail.counters().drops_newest_total.load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        tail.counters().drops_oldest_total.load(Ordering::Relaxed),
        0
    );
    assert!(
        flz_core::entity::pushes_blocked_total() as usize > BLOCKED_BEFORE.load(Ordering::Acquire),
        "no push was ever blocked; the bound was never exercised"
    );
    assert!(instance.entities[0][0]
        .push_holdback
        .lock()
        .unwrap()
        .is_empty());
}

static LOAD_SEEN: AtomicUsize = AtomicUsize::new(0);
/// Feeders racing each other AND the consumer's drain. The gate leak lives
/// between a completing worker publishing its claim and a drain emptying the
/// holdback, so what finds it is concurrency, not volume alone — more feeders
/// means more of those interleavings per second.
const FEEDERS: usize = 4;
const PER_FEEDER: usize = 6_000;
const LOAD_TOTAL: usize = FEEDERS * PER_FEEDER;

// Capacity 1 on the hop: every emit past the one in flight has to be held, so
// the Entity gates and un-gates once per message rather than once per burst.
test_procedure!(LoadHead, "holdback_load_head", String => String, (), cap 1, |input, _state, ctx| {
    ctx.emit(input)?;
    Ok(())
});

test_procedure!(LoadTail, "holdback_load_tail", String => (), (), cap 1, |_input, _state, _ctx| {
    LOAD_SEEN.fetch_add(1, Ordering::AcqRel);
    Ok(())
});

struct LoadOperative;
impl StreamOperative for LoadOperative {
    fn factory_id() -> &'static str {
        "holdback_load_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<LoadHead>()
            .named("load_out")
            .on_full(OverflowPolicy::DropNothing)
            .into::<LoadTail>();
    }
}

/// Concurrent feeders against a saturated inter-procedure hop with a live
/// drain — the shape that finds a LEAKED GATE.
///
/// The completing worker and the consumer's drain both observe the producer's
/// holdback emptying, and exactly one of them may hand the Entity's gates
/// back. Get the claim's publication order wrong and both decide the other is
/// releasing: `drain_active` stays true with no handout outstanding, the
/// holdback is empty, `pending_pushes` reads 0, and that Entity is stopped for
/// the life of the process. Every feeder then parks on its mailbox forever.
///
/// A failure here is a HANG, not a slow pass, and it is invisible to any
/// sequential assertion — the losing interleaving needs a real drain running
/// against a real completion.
#[test]
fn concurrent_feeders_against_a_live_drain_never_leak_a_gate() {
    flz_core::admission::set_nonparking_for_tests(true);
    LOAD_SEEN.store(0, Ordering::Release);

    let (handle, instance) = setup::<LoadOperative>("holdback-load");
    let handle = Arc::new(handle);
    let started = std::time::Instant::now();
    let feeders: Vec<_> = (0..FEEDERS)
        .map(|f| {
            let handle = Arc::clone(&handle);
            std::thread::spawn(move || {
                let stream = dummy_stream();
                for i in 0..PER_FEEDER {
                    dispatch_input(
                        &handle,
                        "holdback-load",
                        "input",
                        format!("f{f}m{i}").into_bytes(),
                        &stream,
                    );
                }
            })
        })
        .collect();

    assert!(
        wait_until(
            || LOAD_SEEN.load(Ordering::Acquire) == LOAD_TOTAL,
            Duration::from_secs(60)
        ),
        "only {} of {LOAD_TOTAL} arrived after {:?} — a producing Entity is holding a round \
         nobody will ever run",
        LOAD_SEEN.load(Ordering::Acquire),
        started.elapsed()
    );
    for f in feeders {
        f.join().expect("feeder finished");
    }

    // Nothing left gated once the last push landed: the gates went back.
    let head = &instance.entities[0][0];
    assert!(!head.push_deferred.load(Ordering::Acquire));
    assert!(!head.push_gated.load(Ordering::Acquire));
    assert!(
        !head.drain_active.load(Ordering::Acquire),
        "the producing Entity is still holding its round after every push landed"
    );
    assert_eq!(flz_core::poll_core::global().pending_push_count(), 0);
}
