//! `DropNothing` withholds work; it never holds a thread. A push into a full
//! consumer is held in the producing Entity's holdback and that Entity is
//! handed no further work, so the invariant is that nothing is discarded AND
//! no worker parks.
//!
//! Its own test binary because the three things it pins are process-wide and
//! read once: the worker pool's size AND ceiling, and the jobs-underway cap.
//! Pinning the ceiling at 1 is the whole point — it is the host shape every
//! C-ABI host has: one thread, with no supervisor rescue available.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use flezha_runner::channel::{ChannelBuilder, OverflowPolicy};
use flezha_runner::operative::StreamOperative;

mod common;
use common::*;

/// The pool is ONE worker and the assertions read process-global engine state
/// (jobs underway, deferrals outstanding), so the cases take turns.
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// The pool must be pinned before the first dispatch starts it, and the
/// non-parking realization enabled before the first push reads it.
fn pin_single_worker_host() -> std::sync::MutexGuard<'static, ()> {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        std::env::set_var("FLEZHA_WORKER_POOL_SIZE", "1");
        std::env::set_var("FLEZHA_WORKER_POOL_MAX", "1");
        std::env::set_var("FLEZHA_MAX_INFLIGHT_ACTIVATIONS", "2");
        flz_core::admission::set_nonparking_for_tests(true);
    });
    SERIAL.lock().unwrap_or_else(|p| p.into_inner())
}

static CHAIN_TAIL: AtomicUsize = AtomicUsize::new(0);
static CHAIN_MID: AtomicUsize = AtomicUsize::new(0);
static DEEPEST_INFLIGHT: AtomicUsize = AtomicUsize::new(0);

const FANOUT: usize = 20;

test_procedure!(ChainHead, "nonparking_chain_head", String => String, (), |_input, _state, ctx| {
    for i in 0..FANOUT {
        ctx.emit(format!("v{i}"))?;
    }
    Ok(())
});

// Capacity 1 on both hops: every emit past the first has nowhere to go, so the
// producer defers on all but one of them.
test_procedure!(ChainMid, "nonparking_chain_mid", String => String, (), cap 1, |input, _state, ctx| {
    CHAIN_MID.fetch_add(1, Ordering::AcqRel);
    note_inflight();
    ctx.emit(input)?;
    Ok(())
});

test_procedure!(ChainTail, "nonparking_chain_tail", String => (), (), cap 1, |_input, _state, _ctx| {
    CHAIN_TAIL.fetch_add(1, Ordering::AcqRel);
    note_inflight();
    Ok(())
});

/// Sample the engine's jobs-underway count from inside user code: the deleted
/// inline serve reached `try_assemble` directly, so an inline drain could put
/// more activations underway than the cap allows. Nothing can now.
fn note_inflight() {
    let now = flz_core::poll_core::global().outstanding_count();
    DEEPEST_INFLIGHT.fetch_max(now, Ordering::AcqRel);
}

struct ChainOperative;
impl StreamOperative for ChainOperative {
    fn factory_id() -> &'static str {
        "nonparking_chain_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<ChainHead>()
            .named("head_out")
            .on_full(OverflowPolicy::DropNothing)
            .into::<ChainMid>()
            .named("mid_out")
            .on_full(OverflowPolicy::DropNothing)
            .into::<ChainTail>();
    }
}

/// A chain of full mailboxes, one worker, no growth available: every value
/// still arrives, in order, with nothing discarded and the jobs-underway cap
/// never breached.
#[test]
fn a_chain_of_full_mailboxes_drains_on_a_single_pinned_worker() {
    let _serial = pin_single_worker_host();
    CHAIN_TAIL.store(0, Ordering::Release);
    CHAIN_MID.store(0, Ordering::Release);
    DEEPEST_INFLIGHT.store(0, Ordering::Release);

    let (handle, instance) = setup::<ChainOperative>("nonparking-chain");
    let stream = dummy_stream();
    dispatch_input(
        &handle,
        "nonparking-chain",
        "input",
        b"go".to_vec(),
        &stream,
    );

    assert!(
        wait_until(
            || CHAIN_TAIL.load(Ordering::Acquire) == FANOUT,
            Duration::from_secs(20)
        ),
        "a DropNothing emit into a full consumer wedged the pinned one-worker host: \
         {} of {FANOUT} reached the middle, {} the tail",
        CHAIN_MID.load(Ordering::Acquire),
        CHAIN_TAIL.load(Ordering::Acquire)
    );

    // Nothing discarded anywhere on the chain: DropNothing means DropNothing
    // whichever mechanism realized it.
    for procedure in instance.entities.iter() {
        for slot in procedure {
            let c = slot.counters();
            assert_eq!(c.drops_newest_total.load(Ordering::Relaxed), 0);
            assert_eq!(c.drops_oldest_total.load(Ordering::Relaxed), 0);
        }
    }

    assert!(
        DEEPEST_INFLIGHT.load(Ordering::Acquire) <= 2,
        "the jobs-underway cap was breached: {} activations were underway at once",
        DEEPEST_INFLIGHT.load(Ordering::Acquire)
    );

    // The holdback is empty and the producer's gates were handed back, or the
    // Entity would never serve another input.
    let head = &instance.entities[0][0];
    assert!(head.push_holdback.lock().unwrap().is_empty());
    assert!(!head.push_deferred.load(Ordering::Acquire));
    assert!(!head.push_gated.load(Ordering::Acquire));
    assert_eq!(flz_core::poll_core::global().pending_push_count(), 0);
}

static ORDERED_SEEN: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

test_procedure!(OrderedHead, "nonparking_ordered_head", String => String, (), |_input, _state, ctx| {
    for i in 0..FANOUT {
        ctx.emit(format!("e{i}"))?;
    }
    Ok(())
});

test_procedure!(OrderedTail, "nonparking_ordered_tail", String => (), (), cap 1, |input, _state, _ctx| {
    ORDERED_SEEN.lock().unwrap().push(input);
    Ok(())
});

struct OrderedOperative;
impl StreamOperative for OrderedOperative {
    fn factory_id() -> &'static str {
        "nonparking_ordered_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<OrderedHead>()
            .named("ordered_out")
            .on_full(OverflowPolicy::DropNothing)
            .into::<OrderedTail>();
    }
}

/// Per-producer emit order survives every deferral: `e1` is held, so `e2..eN`
/// append behind it rather than taking the free slot it is owed.
#[test]
fn per_producer_emit_order_survives_the_holdback() {
    let _serial = pin_single_worker_host();
    ORDERED_SEEN.lock().unwrap().clear();

    let (handle, _instance) = setup::<OrderedOperative>("nonparking-ordered");
    let stream = dummy_stream();
    dispatch_input(
        &handle,
        "nonparking-ordered",
        "input",
        b"go".to_vec(),
        &stream,
    );

    assert!(
        wait_until(
            || ORDERED_SEEN.lock().unwrap().len() == FANOUT,
            Duration::from_secs(20)
        ),
        "only {} of {FANOUT} emits arrived",
        ORDERED_SEEN.lock().unwrap().len()
    );
    let seen = ORDERED_SEEN.lock().unwrap().clone();
    let expected: Vec<String> = (0..FANOUT).map(|i| format!("e{i}")).collect();
    assert_eq!(
        seen, expected,
        "a later emit overtook a deferred earlier one"
    );
}

static GATED_SEEN: AtomicUsize = AtomicUsize::new(0);

test_procedure!(GateHead, "nonparking_gate_head", String => String, (), |_input, _state, ctx| {
    for i in 0..FANOUT {
        ctx.emit(format!("g{i}"))?;
    }
    Ok(())
});

test_procedure!(GateTail, "nonparking_gate_tail", String => (), (), cap 1, |_input, _state, _ctx| {
    GATED_SEEN.fetch_add(1, Ordering::AcqRel);
    Ok(())
});

struct GateOperative;
impl StreamOperative for GateOperative {
    fn factory_id() -> &'static str {
        "nonparking_gate_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<GateHead>()
            .named("gate_out")
            .on_full(OverflowPolicy::DropNothing)
            .into::<GateTail>();
    }
}

/// One erased payload, pushed straight onto a mailbox to saturate it.
fn blob(stream: &Arc<flz_transport::transport::FlezhaStream>) -> flz_core::entity::EntityItem {
    flz_core::entity::EntityItem::Input(flz_core::entity::EntityPayload {
        value: Arc::new(String::from("saturate")) as Arc<dyn std::any::Any + Send + Sync>,
        stream: Arc::clone(stream),
        consumed_offset: None,
        replay: false,
    })
}

/// A producer held on a consumer whose round nobody is running: it keeps its
/// own round claimed — which is what makes the dynamic-mode reaper skip it,
/// with no check of its own — and it is REPORTED, because a gated producer
/// holds no handout and `inflight` cannot see it. Letting the consumer's round
/// run drains everything that was held, in order.
#[test]
fn a_gated_producer_holds_its_round_and_is_reported() {
    let _serial = pin_single_worker_host();
    GATED_SEEN.store(0, Ordering::Release);

    let (handle, instance) = setup::<GateOperative>("nonparking-gate");
    let head = Arc::clone(&instance.entities[0][0]);
    let tail = Arc::clone(&instance.entities[1][0]);
    let stream = dummy_stream();

    // Claim the consumer's round and fill it: no assembly can run, so every
    // emit the producer makes has to be held rather than delivered.
    assert!(tail.mailbox.claim_inflight());
    let mut saturating = 0;
    while tail.mailbox.try_push(blob(&stream)).is_ok() {
        saturating += 1;
    }

    dispatch_input(&handle, "nonparking-gate", "input", b"go".to_vec(), &stream);

    let core = flz_core::poll_core::global();
    assert!(
        wait_until(
            || head.push_gated.load(Ordering::Acquire),
            Duration::from_secs(20)
        ),
        "the producer never gated on its holdback"
    );
    assert_eq!(head.push_holdback.lock().unwrap().len(), FANOUT);
    // The reaper's own skip condition, unchanged: a round already claimed.
    assert!(
        !head.mailbox.claim_inflight(),
        "a gated producer must hold its round so the reaper skips it"
    );
    assert_eq!(
        core.pending_push_count(),
        1,
        "the deferral must be reported"
    );
    assert_eq!(
        head.counters().pushes_total.load(Ordering::Relaxed),
        1,
        "only the operative's own input landed on the producer"
    );

    // Let the consumer run: its first pop re-arms the producer, and the
    // holdback drains behind the item that was already queued.
    tail.mailbox.release_inflight();
    if tail.mailbox.claim_inflight() {
        core.schedule(&tail);
    }
    assert!(
        wait_until(
            || GATED_SEEN.load(Ordering::Acquire) == FANOUT + saturating,
            Duration::from_secs(20)
        ),
        "releasing the consumer must drain the whole holdback: {} of {}",
        GATED_SEEN.load(Ordering::Acquire),
        FANOUT + saturating
    );
    assert!(wait_until(
        || core.pending_push_count() == 0 && !head.push_gated.load(Ordering::Acquire),
        Duration::from_secs(10)
    ));
}
