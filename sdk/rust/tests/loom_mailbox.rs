//! Loom model check for the mailbox CAS dance.
//!
//! The real `Mailbox` in `runner/mailbox.rs` uses `SegQueue` (from
//! crossbeam) and submits drain closures into a work-stealing pool. Neither
//! is loom-friendly. This file mirrors ONLY the CAS+drain logic against
//! loom's primitives so loom can exhaustively enumerate thread interleavings
//! and prove no missed wake / no overlapping drain.
//!
//! Run with:
//!
//!   RUSTFLAGS="--cfg loom" cargo test --test loom_mailbox --release
//!
//! (The `cfg(loom)` gate keeps normal `cargo test` runs unaffected.)

#![cfg(loom)]

use loom::sync::atomic::{AtomicBool, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

struct MiniMailbox {
    queue: Mutex<Vec<usize>>,
    inflight: AtomicBool,
    /// Test probe: counts concurrent drains (must never exceed 1).
    drains_active: AtomicBool,
    /// Test probe: total items drained.
    drained: Mutex<Vec<usize>>,
}

impl MiniMailbox {
    fn new() -> Self {
        MiniMailbox {
            queue: Mutex::new(Vec::new()),
            inflight: AtomicBool::new(false),
            drains_active: AtomicBool::new(false),
            drained: Mutex::new(Vec::new()),
        }
    }
}

/// Mirror of `Mailbox::push_and_maybe_submit`. Instead of submitting to a
/// pool, we directly call `run_drain` in-thread (loom will explore the
/// interleaving). Returns whether this caller "owns" the drain.
fn push_and_maybe_submit(mbox: &Arc<MiniMailbox>, item: usize) -> bool {
    mbox.queue.lock().unwrap().push(item);

    if mbox
        .inflight
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_ok()
    {
        run_drain(Arc::clone(mbox));
        true
    } else {
        false
    }
}

fn run_drain(mbox: Arc<MiniMailbox>) {
    loop {
        // Assert: no other drainer is active right now.
        let was_active = mbox.drains_active.swap(true, Ordering::AcqRel);
        assert!(!was_active, "mailbox allowed overlapping drain!");

        // Hot pop loop.
        loop {
            let item = {
                let mut q = mbox.queue.lock().unwrap();
                q.pop()
            };
            match item {
                Some(i) => mbox.drained.lock().unwrap().push(i),
                None => break,
            }
        }

        mbox.drains_active.store(false, Ordering::Release);

        // Queue looked empty: release ownership.
        mbox.inflight.store(false, Ordering::Release);

        // Re-check for a racing push (missed wake guard).
        let empty = mbox.queue.lock().unwrap().is_empty();
        if empty {
            return;
        }
        if mbox
            .inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            continue;
        }
        return;
    }
}

#[test]
fn two_producers_no_overlap_no_missed_wake() {
    loom::model(|| {
        let mbox = Arc::new(MiniMailbox::new());

        let a = {
            let mbox = Arc::clone(&mbox);
            thread::spawn(move || {
                push_and_maybe_submit(&mbox, 1);
            })
        };
        let b = {
            let mbox = Arc::clone(&mbox);
            thread::spawn(move || {
                push_and_maybe_submit(&mbox, 2);
            })
        };
        a.join().unwrap();
        b.join().unwrap();

        // Both items must have been drained.
        let d = mbox.drained.lock().unwrap().clone();
        assert_eq!(d.len(), 2, "missed item(s): drained = {:?}", d);
        // The queue must be empty and inflight must be false.
        assert!(mbox.queue.lock().unwrap().is_empty());
        assert!(!mbox.inflight.load(Ordering::Acquire));
    });
}

#[test]
fn three_producers_cover_reentrant_drain_path() {
    loom::model(|| {
        let mbox = Arc::new(MiniMailbox::new());

        let a = {
            let mbox = Arc::clone(&mbox);
            thread::spawn(move || {
                push_and_maybe_submit(&mbox, 10);
            })
        };
        let b = {
            let mbox = Arc::clone(&mbox);
            thread::spawn(move || {
                push_and_maybe_submit(&mbox, 20);
            })
        };

        // Main thread is a third producer.
        push_and_maybe_submit(&mbox, 30);

        a.join().unwrap();
        b.join().unwrap();

        let d = mbox.drained.lock().unwrap().clone();
        assert_eq!(d.len(), 3, "drained = {:?}", d);
        assert!(mbox.queue.lock().unwrap().is_empty());
        assert!(!mbox.inflight.load(Ordering::Acquire));
    });
}
