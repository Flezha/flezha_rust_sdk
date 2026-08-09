//! Relocated engine tests: handout exclusivity, ordering, the ladder.

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use flz_transport::transport::FlezhaStream;

    use crate::channel::ChannelBuilder;
    use crate::entity::{EntityItem, EntityPayload, EntitySlot};
    use crate::operative::builder::build_topology_with_persistence;
    use crate::operative::instance::UnboundedInstance;
    use crate::operative::UnboundedOperative;
    use crate::procedure::change::NoChange;
    use crate::procedure::clock::WallClock;
    use crate::procedure::ctx::ProcedureCtx;
    use crate::procedure::error::ProcedureError;
    use crate::procedure::state_handle::StateHandle;
    use crate::procedure::Procedure;
    use flz_core::poll_core::{PollCore, Polled};
    use flz_core::restore::RestoreInitPayload;
    use flz_core::work_stealing_pool::WorkStealingPool;

    #[derive(Default)]
    struct PollProc;
    impl Procedure for PollProc {
        type Input = usize;
        type Output = ();
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "poll_core_exclusivity_proc"
        }
        fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
        fn run(
            &mut self,
            _: usize,
            _: StateHandle<'_, Self>,
            _: &mut ProcedureCtx<'_>,
        ) -> Result<(), ProcedureError> {
            Ok(())
        }
    }

    struct PollOp;
    impl UnboundedOperative for PollOp {
        fn factory_id() -> &'static str {
            "poll_core_exclusivity_op"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<usize>("input").into::<PollProc>();
        }
    }

    fn build_slot(instance_id: &str) -> (Arc<UnboundedInstance>, Arc<EntitySlot>) {
        let cb = ChannelBuilder::new();
        PollOp::channels(&cb);
        let topology =
            build_topology_with_persistence(PollOp::factory_id(), cb, PollOp::PERSISTENCE_ENABLED)
                .expect("topology");
        let pool = Arc::new(WorkStealingPool::with_size(1));
        let instance = UnboundedInstance::new(topology, Arc::from(instance_id), pool);
        let slot = Arc::clone(&instance.entities[0][0]);
        (instance, slot)
    }

    fn payload(ordinal: usize, stream: &Arc<FlezhaStream>) -> EntityItem {
        EntityItem::Input(EntityPayload {
            value: Arc::new(ordinal) as Arc<dyn Any + Send + Sync>,
            stream: Arc::clone(stream),
            consumed_offset: None,
            replay: false,
        })
    }

    /// The spec pin: two workers polling concurrently while two entities each
    /// have 100 queued inputs never both hold the same entity, and each
    /// entity's inputs arrive in enqueue order across successive activations.
    #[test]
    fn same_entity_never_outstanding_twice_and_order_holds() {
        const PER_ENTITY: usize = 100;
        let core = Arc::new(PollCore::new());
        let (_ia, slot_a) = build_slot("poll-excl-a");
        let (_ib, slot_b) = build_slot("poll-excl-b");
        let (sock, _peer) = std::os::unix::net::UnixStream::pair().expect("pair");
        let stream = Arc::new(FlezhaStream::Unix(sock));

        // Per-slot "held by a worker" flags keyed by slot pointer identity.
        let held: Arc<Vec<(usize, AtomicBool)>> = Arc::new(vec![
            (Arc::as_ptr(&slot_a) as usize, AtomicBool::new(false)),
            (Arc::as_ptr(&slot_b) as usize, AtomicBool::new(false)),
        ]);
        let seen: Arc<Vec<(usize, std::sync::Mutex<Vec<usize>>)>> = Arc::new(vec![
            (Arc::as_ptr(&slot_a) as usize, std::sync::Mutex::new(vec![])),
            (Arc::as_ptr(&slot_b) as usize, std::sync::Mutex::new(vec![])),
        ]);
        let total = Arc::new(AtomicUsize::new(0));

        let mut workers = Vec::new();
        for _ in 0..2 {
            let core = Arc::clone(&core);
            let held = Arc::clone(&held);
            let seen = Arc::clone(&seen);
            let total = Arc::clone(&total);
            workers.push(std::thread::spawn(move || loop {
                match core.poll(Duration::from_millis(50)) {
                    Polled::Activation(act) => {
                        let key = Arc::as_ptr(act.slot()) as usize;
                        let flag = &held.iter().find(|(k, _)| *k == key).unwrap().1;
                        assert!(
                            !flag.swap(true, Ordering::AcqRel),
                            "entity handed out while already outstanding"
                        );
                        let log = &seen.iter().find(|(k, _)| *k == key).unwrap().1;
                        for work in act.items() {
                            let EntityItem::Input(p) = &work.item else {
                                panic!("unexpected timer item")
                            };
                            let ordinal = *p.value.downcast_ref::<usize>().unwrap();
                            log.lock().unwrap().push(ordinal);
                            total.fetch_add(1, Ordering::Relaxed);
                        }
                        flag.store(false, Ordering::Release);
                        core.complete(act.token);
                    }
                    Polled::Timeout => {}
                    Polled::ShuttingDown => return,
                }
            }));
        }

        // Interleave pushes across the two entities, scheduling a round on
        // every winning inflight CAS — the producer protocol verbatim.
        for i in 0..PER_ENTITY {
            for slot in [&slot_a, &slot_b] {
                assert!(slot.mailbox.try_push(payload(i, &stream)).is_ok());
                if slot.mailbox.try_mark_inflight() {
                    core.schedule(slot);
                }
            }
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        while total.load(Ordering::Relaxed) < PER_ENTITY * 2 {
            assert!(Instant::now() < deadline, "activations did not all arrive");
            std::thread::sleep(Duration::from_millis(5));
        }
        core.shutdown();
        for w in workers {
            w.join().unwrap();
        }

        let expected: Vec<usize> = (0..PER_ENTITY).collect();
        for (_, log) in seen.iter() {
            assert_eq!(*log.lock().unwrap(), expected, "per-entity order violated");
        }
    }

    /// Completion releases the entity: a batch queued behind an outstanding
    /// activation is handed out only after `complete`.
    #[test]
    fn completion_releases_the_entity() {
        let core = PollCore::new();
        let (_i, slot) = build_slot("poll-release");
        let (sock, _peer) = std::os::unix::net::UnixStream::pair().expect("pair");
        let stream = Arc::new(FlezhaStream::Unix(sock));

        assert!(slot.mailbox.try_push(payload(0, &stream)).is_ok());
        assert!(slot.mailbox.try_mark_inflight());
        core.schedule(&slot);
        let first = match core.poll(Duration::from_millis(100)) {
            Polled::Activation(a) => a,
            _ => panic!("expected an activation"),
        };

        // Arrives mid-flight: the producer's CAS fails (round still held),
        // so nothing is scheduled — and poll must NOT hand the entity out.
        assert!(slot.mailbox.try_push(payload(1, &stream)).is_ok());
        assert!(!slot.mailbox.try_mark_inflight());
        match core.poll(Duration::from_millis(50)) {
            Polled::Timeout => {}
            _ => panic!("entity handed out while outstanding"),
        }

        core.complete(first.token);
        match core.poll(Duration::from_millis(500)) {
            Polled::Activation(second) => {
                assert_eq!(second.items().len(), 1);
                core.complete(second.token);
            }
            _ => panic!("queued input not handed out after completion"),
        }
    }

    /// The engine's ladder: a retryable failure re-hands the same item with
    /// a bumped attempt, the tail queued behind it in order; the attempt
    /// that reaches the budget is marked final.
    #[test]
    fn retry_after_failure_reholds_head_and_tail_in_order() {
        let core = PollCore::new();
        let (_i, slot) = build_slot("poll-retry");
        let (sock, _peer) = std::os::unix::net::UnixStream::pair().expect("pair");
        let stream = Arc::new(FlezhaStream::Unix(sock));

        for i in 0..3 {
            assert!(slot.mailbox.try_push(payload(i, &stream)).is_ok());
        }
        assert!(slot.mailbox.try_mark_inflight());
        core.schedule(&slot);
        let act = match core.poll(Duration::from_millis(100)) {
            Polled::Activation(a) => a,
            _ => panic!("expected an activation"),
        };
        assert_eq!(act.items().len(), 3);
        assert_eq!(act.items()[0].attempt, 1);
        assert!(!act.items()[0].is_final);

        // Item 0 processed fine; item 1 failed retryably: hand back 1 + 2.
        let (token, _slot, items) = act.into_work().expect("work activation");
        let passback: Vec<_> = items.into_iter().skip(1).collect();
        core.retry_after_failure(token, passback, "boom".into());

        let act = match core.poll(Duration::from_millis(500)) {
            Polled::Activation(a) => a,
            _ => panic!("re-hand did not arrive"),
        };
        assert_eq!(act.items().len(), 2);
        let ordinals: Vec<usize> = act
            .items()
            .iter()
            .map(|w| {
                let EntityItem::Input(p) = &w.item else {
                    panic!("unexpected timer")
                };
                *p.value.downcast_ref::<usize>().unwrap()
            })
            .collect();
        assert_eq!(ordinals, vec![1, 2], "head-of-line order violated");
        assert_eq!(act.items()[0].attempt, 2);
        assert_eq!(act.items()[1].attempt, 1, "tail attempts must not bump");
        // Default policy is 3 attempts: one more failure reaches the budget.
        let (token, _slot, items) = act.into_work().expect("work activation");
        let passback: Vec<_> = items.into_iter().collect();
        core.retry_after_failure(token, passback, "boom again".into());
        let act = match core.poll(Duration::from_millis(2_000)) {
            Polled::Activation(a) => a,
            _ => panic!("second re-hand did not arrive"),
        };
        assert_eq!(act.items()[0].attempt, 3);
        assert!(
            act.items()[0].is_final,
            "budget-reaching attempt must be final"
        );
        core.complete(act.token);
    }

    /// A parked head must not take the unprocessed tail with it: the host
    /// stops at the failed item and the core re-hands the rest — same order,
    /// no attempt bump, no backoff (the tail never failed). This is exactly
    /// how a healthy payload queued behind a poison one stays deliverable.
    #[test]
    fn rehand_unprocessed_returns_the_tail_untouched() {
        let core = PollCore::new();
        let (_i, slot) = build_slot("poll-rehand");
        let (sock, _peer) = std::os::unix::net::UnixStream::pair().expect("pair");
        let stream = Arc::new(FlezhaStream::Unix(sock));

        for i in 0..3 {
            assert!(slot.mailbox.try_push(payload(i, &stream)).is_ok());
        }
        assert!(slot.mailbox.try_mark_inflight());
        core.schedule(&slot);
        let act = match core.poll(Duration::from_millis(100)) {
            Polled::Activation(a) => a,
            _ => panic!("expected an activation"),
        };
        assert_eq!(act.items().len(), 3);

        // Item 0 parked at its final attempt; 1 and 2 never ran.
        let started = Instant::now();
        let (token, _slot, items) = act.into_work().expect("work activation");
        let tail: Vec<_> = items.into_iter().skip(1).collect();
        core.rehand_unprocessed(token, tail);

        let act = match core.poll(Duration::from_millis(500)) {
            Polled::Activation(a) => a,
            _ => panic!("unprocessed tail was dropped"),
        };
        assert!(
            started.elapsed() < Duration::from_millis(400),
            "a re-hand is not a retry: no backoff may delay it"
        );
        let ordinals: Vec<usize> = act
            .items()
            .iter()
            .map(|w| {
                let EntityItem::Input(p) = &w.item else {
                    panic!("unexpected timer")
                };
                *p.value.downcast_ref::<usize>().unwrap()
            })
            .collect();
        assert_eq!(ordinals, vec![1, 2], "tail order violated");
        assert_eq!(act.items()[0].attempt, 1, "a re-hand bumps no attempt");
        assert_eq!(act.items()[1].attempt, 1);
        core.complete(act.token);

        // An empty tail is a plain completion: the entity is released.
        assert!(slot.mailbox.try_push(payload(9, &stream)).is_ok());
        assert!(slot.mailbox.try_mark_inflight());
        core.schedule(&slot);
        let act = match core.poll(Duration::from_millis(100)) {
            Polled::Activation(a) => a,
            _ => panic!("expected an activation"),
        };
        core.rehand_unprocessed(act.token, Vec::new());
        assert!(slot.mailbox.try_push(payload(10, &stream)).is_ok());
        assert!(slot.mailbox.try_mark_inflight());
        core.schedule(&slot);
        match core.poll(Duration::from_millis(500)) {
            Polled::Activation(a) => core.complete(a.token),
            _ => panic!("entity not released after an empty re-hand"),
        }
    }
}
