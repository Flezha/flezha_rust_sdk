//! Relocated engine tests: handout exclusivity, ordering, the ladder.

/// Slot scaffolding shared by the engine's own tests and the poll pool's
/// (`operative::poll_pool`) — one definition of how a test builds an Entity
/// and hands it an input.
pub(crate) mod fixtures {
    use std::any::Any;
    use std::sync::Arc;

    use flz_transport::transport::FlezhaStream;

    use crate::channel::ChannelBuilder;
    use crate::entity::{EntityItem, EntityPayload, EntitySlot};
    use crate::operative::builder::build_topology_with_persistence;
    use crate::operative::instance::StreamInstance;
    use crate::operative::StreamOperative;
    use crate::runner::poll_core::PollCore;

    /// One instance of `O` and its first Entity.
    pub(crate) fn build_slot_of<O: StreamOperative>(
        instance_id: &str,
    ) -> (Arc<StreamInstance>, Arc<EntitySlot>) {
        let cb = ChannelBuilder::new();
        O::channels(&cb);
        let topology = build_topology_with_persistence(O::factory_id(), cb, O::PERSISTENCE_ENABLED)
            .expect("topology");
        let instance = StreamInstance::new(topology, Arc::from(instance_id));
        let slot = Arc::clone(&instance.entities[0][0]);
        (instance, slot)
    }

    /// A stream token for an input or firing. The peer is dropped straight
    /// away — the engine only carries the handle, it never writes to it.
    pub(crate) fn unix_stream() -> Arc<FlezhaStream> {
        let (sock, _peer) = std::os::unix::net::UnixStream::pair().expect("pair");
        Arc::new(FlezhaStream::Unix(sock))
    }

    pub(crate) fn payload(ordinal: usize, stream: &Arc<FlezhaStream>) -> EntityItem {
        EntityItem::Input(EntityPayload {
            value: Arc::new(ordinal) as Arc<dyn Any + Send + Sync>,
            stream: Arc::clone(stream),
            consumed_offset: None,
            replay: false,
        })
    }

    /// Drive `done` until it holds, failing the test at a 10 s deadline with
    /// `what` naming the condition. Shared by the engine's own tests and the
    /// poll pool's.
    pub(crate) fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !done() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Queue one input and claim its round — the producer protocol.
    pub(crate) fn push_input(core: &PollCore, slot: &Arc<EntitySlot>, value: usize) {
        assert!(slot
            .mailbox
            .try_push(payload(value, &unix_stream()))
            .is_ok());
        if slot.mailbox.try_mark_inflight() {
            core.schedule(slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use flz_core::poll_core::{Answered, EngineActivation, PollCore, Polled, Starvation};
    use flz_core::restore::RestoreInitPayload;
    use flz_transport::transport::FlezhaStream;

    use super::fixtures::{build_slot_of, payload, unix_stream, wait_until};
    use crate::channel::ChannelBuilder;
    use crate::entity::timer::{TimerKind, TimerOp};
    use crate::entity::{EntityItem, EntitySlot};
    use crate::operative::instance::StreamInstance;
    use crate::operative::StreamOperative;
    use crate::procedure::change::NoChange;
    use crate::procedure::clock::WallClock;
    use crate::procedure::ctx::ProcedureCtx;
    use crate::procedure::error::ProcedureError;
    use crate::procedure::state_handle::StateHandle;
    use crate::procedure::Procedure;

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
    impl StreamOperative for PollOp {
        fn factory_id() -> &'static str {
            "poll_core_exclusivity_op"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<usize>("input").into::<PollProc>();
        }
    }

    /// A one-slot mailbox: the smallest thing that can be full, so a wake
    /// promoted against it always meets the no-room path.
    #[derive(Default)]
    struct TinyProc;
    impl Procedure for TinyProc {
        type Input = usize;
        type Output = ();
        type State = ();
        type Change = NoChange;
        type Clock = WallClock;
        type Config = ();
        fn name() -> &'static str {
            "poll_core_tiny_mailbox_proc"
        }
        fn mailbox_capacity() -> usize {
            1
        }
        const MAILBOX_CAPACITY_OVERRIDDEN: bool = true;
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

    struct TinyOp;
    impl StreamOperative for TinyOp {
        fn factory_id() -> &'static str {
            "poll_core_tiny_mailbox_op"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<usize>("input").into::<TinyProc>();
        }
    }

    /// A slot whose one mailbox slot is taken and whose round is claimed by
    /// nobody: every wake promoted against it meets a full mailbox.
    fn full_slot(instance_id: &str) -> (Arc<StreamInstance>, Arc<EntitySlot>) {
        let (instance, slot) = build_slot_of::<TinyOp>(instance_id);
        assert!(slot.mailbox.try_push(payload(0, &unix_stream())).is_ok());
        assert!(slot.mailbox.is_full());
        // Claim the round so nothing drains it — the caller decides when room
        // appears.
        assert!(slot.mailbox.try_mark_inflight());
        (instance, slot)
    }

    /// A backoff long enough to observe the deferral window from the outside:
    /// base 2 s, so attempt 1 comes due in 1–3 s.
    struct SlowRetryOp;
    impl StreamOperative for SlowRetryOp {
        fn factory_id() -> &'static str {
            "poll_core_slow_retry_op"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<usize>("input")
                .retry_backoff_ms(2_000)
                .into::<PollProc>();
        }
    }

    fn build_slot(instance_id: &str) -> (Arc<StreamInstance>, Arc<EntitySlot>) {
        build_slot_of::<PollOp>(instance_id)
    }

    /// Arm one already-due processing-time timer and keep its firing on this
    /// core: `routing::enqueue` only schedules a round when it wins the
    /// inflight CAS, and the caller has already taken it.
    fn arm_due_timer(core: &PollCore, slot: &Arc<EntitySlot>, at: u64) {
        slot.timers
            .lock()
            .unwrap()
            .apply(&TimerOp::register(TimerKind::ProcessingTime, at));
        assert!(slot.mailbox.try_mark_inflight());
        core.arm_timer(slot, at, unix_stream());
    }

    /// The one timer item a promotion left on the slot's mailbox.
    fn assemble_timer(core: &PollCore, slot: &Arc<EntitySlot>) -> (u64, u64) {
        let act = core.try_assemble(slot).expect("a firing to assemble");
        assert_eq!(act.items().len(), 1);
        assert!(matches!(act.items()[0].item, EntityItem::Timer(_)));
        let now = act.items()[0]
            .now
            .expect("a firing carries its scheduled at");
        (act.token, now)
    }

    /// The producer protocol followed by one handout: queue `count` inputs
    /// (`0..count`), claim the round on the winning inflight CAS, and take the
    /// activation it earns.
    fn queue_and_hand_out(
        core: &PollCore,
        slot: &Arc<EntitySlot>,
        stream: &Arc<FlezhaStream>,
        count: usize,
    ) -> EngineActivation {
        for i in 0..count {
            assert!(slot.mailbox.try_push(payload(i, stream)).is_ok());
        }
        assert!(slot.mailbox.try_mark_inflight());
        core.schedule(slot);
        match core.poll(Duration::from_millis(100)) {
            Polled::Activation(act) => act,
            _ => panic!("expected an activation"),
        }
    }

    /// The ordinals a batch carries, in order.
    fn ordinals(act: &EngineActivation) -> Vec<usize> {
        act.items()
            .iter()
            .map(|w| {
                let EntityItem::Input(p) = &w.item else {
                    panic!("unexpected timer item")
                };
                *p.value.downcast_ref::<usize>().unwrap()
            })
            .collect()
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
        let stream = unix_stream();

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
        let stream = unix_stream();

        let first = queue_and_hand_out(&core, &slot, &stream, 1);

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
        let stream = unix_stream();

        let act = queue_and_hand_out(&core, &slot, &stream, 3);
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
        assert_eq!(ordinals(&act), vec![1, 2], "head-of-line order violated");
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
    fn abandon_returns_the_tail_untouched() {
        let core = PollCore::new();
        let (_i, slot) = build_slot("poll-rehand");
        let stream = unix_stream();

        let act = queue_and_hand_out(&core, &slot, &stream, 3);
        assert_eq!(act.items().len(), 3);

        // Item 0 parked at its final attempt; 1 and 2 never ran.
        let started = Instant::now();
        let (token, _slot, items) = act.into_work().expect("work activation");
        let tail: Vec<_> = items.into_iter().skip(1).collect();
        core.abandon(token, tail);

        let act = match core.poll(Duration::from_millis(500)) {
            Polled::Activation(a) => a,
            _ => panic!("unprocessed tail was dropped"),
        };
        assert!(
            started.elapsed() < Duration::from_millis(400),
            "a re-hand is not a retry: no backoff may delay it"
        );
        assert_eq!(ordinals(&act), vec![1, 2], "tail order violated");
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
        core.abandon(act.token, Vec::new());
        assert!(slot.mailbox.try_push(payload(10, &stream)).is_ok());
        assert!(slot.mailbox.try_mark_inflight());
        core.schedule(&slot);
        match core.poll(Duration::from_millis(500)) {
            Polled::Activation(a) => {
                core.complete(a.token);
            }
            _ => panic!("entity not released after an empty re-hand"),
        }
    }

    /// The third answer, on the path it exists for: a host that took a batch
    /// and could not finish it re-hands EVERYTHING untouched. Per-entity
    /// exclusivity is the handout, so without this the entity would never
    /// appear in another poll result for the life of the process.
    #[test]
    fn abandon_rehands_the_whole_batch_and_releases_the_entity() {
        let core = PollCore::new();
        let (_i, slot) = build_slot("poll-abandon");
        let stream = unix_stream();

        let act = queue_and_hand_out(&core, &slot, &stream, 3);
        let (token, _slot, items) = act.into_work().expect("work activation");
        assert_eq!(core.outstanding_count(), 1);
        assert_eq!(core.abandon(token, items), Answered::Released);
        assert_eq!(core.outstanding_count(), 0);

        let act = match core.poll(Duration::from_millis(500)) {
            Polled::Activation(a) => a,
            _ => panic!("an abandoned entity must be pollable again"),
        };
        assert_eq!(
            ordinals(&act),
            vec![0, 1, 2],
            "items must come back in order"
        );
        for item in act.items() {
            assert_eq!(item.attempt, 1, "an abandon consumes no ladder attempt");
            assert!(!item.is_final);
        }
        core.complete(act.token);
    }

    /// A token the core does not hold is a host defect — a double answer, or
    /// one from a context that never held it. Reporting it is what turns a
    /// silent second release (which would re-arm an entity another worker is
    /// already draining) into a caught bug.
    #[test]
    fn a_second_answer_is_rejected_not_silently_ignored() {
        let core = PollCore::new();
        let (_i, slot) = build_slot("poll-double-answer");
        let stream = unix_stream();

        let act = queue_and_hand_out(&core, &slot, &stream, 1);
        let token = act.token;
        drop(act);
        assert_eq!(core.complete(token), Answered::Released);

        assert_eq!(core.complete(token), Answered::UnknownToken);
        assert_eq!(core.abandon(token, Vec::new()), Answered::UnknownToken);
        assert_eq!(
            core.retry_after_failure(token, Vec::new(), "boom".into()),
            Answered::UnknownToken
        );
        assert_eq!(
            core.abandon(u64::MAX, Vec::new()),
            Answered::UnknownToken,
            "a token never handed out is refused the same way"
        );
    }

    /// Handout liveness, the two values the heartbeat carries: one wedged
    /// Entity is invisible to the runner-wide dispatch counter, and these are
    /// what make it visible. Reporting only — nothing here reclaims.
    #[test]
    fn handout_liveness_reports_the_count_and_the_oldest_age() {
        let core = PollCore::new();
        let (_ia, first) = build_slot("poll-liveness-a");
        let (_ib, second) = build_slot("poll-liveness-b");
        let stream = unix_stream();

        assert_eq!(core.outstanding_count(), 0);
        assert_eq!(
            core.outstanding_oldest_age_ms(),
            0,
            "an idle core reports zero"
        );

        let old = queue_and_hand_out(&core, &first, &stream, 1);
        std::thread::sleep(Duration::from_millis(60));
        let young = queue_and_hand_out(&core, &second, &stream, 1);

        assert_eq!(core.outstanding_count(), 2);
        let age = core.outstanding_oldest_age_ms();
        assert!(
            age >= 60,
            "the OLDEST handout's age is what is reported, got {age}ms"
        );

        core.complete(old.token);
        assert_eq!(core.outstanding_count(), 1);
        assert!(
            core.outstanding_oldest_age_ms() < age,
            "the remaining handout is younger than the one just answered"
        );
        core.complete(young.token);
        assert_eq!(core.outstanding_oldest_age_ms(), 0);
    }

    // ------------------------------------------------ the deferred-wake list

    /// Entering `poll` promotes whatever came due: timers armed in the past
    /// are on the Entity's mailbox by the time that call returns, in ascending
    /// scheduled order however they were armed (what a restore's overdue set
    /// relies on), each carrying its scheduled timestamp as the invocation's
    /// `now`.
    #[test]
    fn poll_entry_promotes_due_timers_in_ascending_order() {
        let core = PollCore::new();
        let (_i, slot) = build_slot("poll-wake-entry");
        let at = flz_core::util::wall_now_ms() - 1;
        arm_due_timer(&core, &slot, at);
        // Armed second, due first: order comes from the list, not arrival.
        slot.timers
            .lock()
            .unwrap()
            .apply(&TimerOp::register(TimerKind::ProcessingTime, at - 1));
        core.arm_timer(&slot, at - 1, unix_stream());
        assert_eq!(core.timers_armed(), 2);

        // Nothing is queued on this core, so the call promotes and times out.
        assert!(matches!(
            core.poll(Duration::from_millis(0)),
            Polled::Timeout
        ));
        assert_eq!(core.timers_armed(), 0, "a fired entry leaves the list");

        let act = core.try_assemble(&slot).expect("both firings");
        let fired: Vec<u64> = act
            .items()
            .iter()
            .map(|w| w.now.expect("a firing carries its scheduled at"))
            .collect();
        assert_eq!(
            fired,
            vec![at - 1, at],
            "ascending scheduled order; fired_at is the scheduled timestamp"
        );
        core.complete(act.token);
    }

    /// The regression this fix exists for. A due firing whose target mailbox
    /// is full used to be pushed under `DropNothing` from inside `poll`, so
    /// the promoter parked there — and a thread inside `poll` is counted by
    /// `pollers_inside`, which makes the starvation wait answer "a thread is
    /// about to take work". A single-poller host (every C-ABI host, which can
    /// install no inline-serve hook) therefore wedged permanently with the
    /// supervisor blind to it: measured at 1 of 3 firings with the pool
    /// ceiling free to grow.
    ///
    /// Now the pass returns, the entry stays on the list, and the firing lands
    /// once room appears — still carrying the timestamp it was ARMED with,
    /// which is what the pending set cancels by.
    #[test]
    fn a_due_firing_into_a_full_mailbox_defers_instead_of_parking() {
        let core = PollCore::new();
        let (_i, slot) = full_slot("poll-wake-full-timer");
        let at = flz_core::util::wall_now_ms() - 1;
        slot.timers
            .lock()
            .unwrap()
            .apply(&TimerOp::register(TimerKind::ProcessingTime, at));
        core.arm_timer(&slot, at, unix_stream());
        assert_eq!(core.timers_armed(), 1);

        // The pass RETURNS. Before the fix this call never came back.
        let started = Instant::now();
        assert!(matches!(
            core.poll(Duration::from_millis(0)),
            Polled::Timeout
        ));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the promoter must not wait for mailbox room"
        );
        assert_eq!(
            core.timers_armed(),
            1,
            "the undeliverable firing stays on the wake list"
        );

        // And the condition is reportable: a due entry no pass could service
        // is starvation with no dwell, which is what buys the host a thread.
        assert!(
            matches!(
                core.wait_starvation(Duration::from_secs(5)),
                Starvation::Observed
            ),
            "a wake nobody can deliver must read as starvation"
        );

        // Room appears; a later pass lands it.
        assert!(slot.mailbox.pop().is_some());
        wait_until(
            "the deferred firing to land once the mailbox has room",
            || {
                let _ = core.poll(Duration::from_millis(10));
                core.timers_armed() == 0
            },
        );
        let (token, fired_at) = assemble_timer(&core, &slot);
        assert_eq!(
            fired_at, at,
            "a deferred firing keeps its ARMED timestamp, not the retry's due time"
        );
        core.complete(token);
    }

    /// The same rule for the other wake a saturated mailbox can meet. A close
    /// silently dropped would hold the interval's outputs until the next
    /// input — exactly what `checkpoint_max_delay_ms` exists to prevent — so
    /// it defers rather than parking or vanishing.
    #[test]
    fn a_due_interval_close_into_a_full_mailbox_defers_instead_of_parking() {
        let core = PollCore::new();
        let (_i, slot) = full_slot("poll-wake-full-close");
        core.arm_interval_close(&slot, flz_core::util::wall_now_ms() - 1);

        let started = Instant::now();
        assert!(matches!(
            core.poll(Duration::from_millis(0)),
            Polled::Timeout
        ));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the promoter must not wait for mailbox room"
        );
        assert_eq!(slot.mailbox.len(), 1, "nothing was admitted");

        assert!(slot.mailbox.pop().is_some());
        wait_until(
            "the deferred close to land once the mailbox has room",
            || {
                let _ = core.poll(Duration::from_millis(10));
                slot.mailbox.len() == 1
            },
        );
        assert!(matches!(
            slot.mailbox.pop(),
            Some(EntityItem::CloseInterval)
        ));
    }

    /// An entry armed while a poller is already blocked shortens that
    /// poller's wait: it is promoted promptly instead of at the
    /// caller-supplied timeout, and no thread was created to notice.
    #[test]
    fn a_wake_armed_during_a_wait_shortens_it() {
        let core = Arc::new(PollCore::new());
        let (_i, slot) = build_slot("poll-wake-during");
        let poller = {
            let core = Arc::clone(&core);
            std::thread::spawn(move || core.poll(Duration::from_secs(30)))
        };
        // Let the poller reach its wait, then arm an overdue deadline.
        std::thread::sleep(Duration::from_millis(50));
        let started = Instant::now();
        arm_due_timer(&core, &slot, flz_core::util::wall_now_ms() - 1);

        let deadline = Instant::now() + Duration::from_secs(5);
        while core.timers_armed() > 0 {
            assert!(
                Instant::now() < deadline,
                "a blocked poller never promoted the entry"
            );
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the wait was not shortened to the nearest due entry"
        );
        assert!(!slot.mailbox.is_empty(), "the firing reached the mailbox");

        core.shutdown();
        assert!(matches!(poller.join().unwrap(), Polled::ShuttingDown));
    }

    /// Two Operative instances arming the same procedure id, entity key and
    /// scheduled time hold two entries, and one instance's cancellation
    /// leaves the other's firing alone — the identity in the list key carries
    /// the instance.
    #[test]
    fn timers_are_scoped_to_the_arming_instance() {
        let core = PollCore::new();
        let (_ia, a) = build_slot("poll-scope-a");
        let (_ib, b) = build_slot("poll-scope-b");
        let at = flz_core::util::wall_now_ms() - 1;
        arm_due_timer(&core, &a, at);
        arm_due_timer(&core, &b, at);
        assert_eq!(
            core.timers_armed(),
            2,
            "same procedure and key, different instances: two entries"
        );

        core.disarm_timer(&a.entity_identity, at);
        assert_eq!(core.timers_armed(), 1);
        assert!(matches!(
            core.poll(Duration::from_millis(0)),
            Polled::Timeout
        ));

        assert!(
            a.mailbox.is_empty(),
            "the disarmed instance's timer must not fire"
        );
        let (token, now) = assemble_timer(&core, &b);
        assert_eq!(now, at);
        core.complete(token);
    }

    /// The list's bookkeeping: arming is an idempotent replace, a disarm takes
    /// exactly one entry, and discarding an Entity takes every entry it owns
    /// and no other's — what the dynamic-mode reaper relies on.
    #[test]
    fn the_wake_list_arms_disarms_and_discards_by_entity() {
        let core = PollCore::new();
        let (_ia, a) = build_slot("poll-armed-a");
        let (_ib, b) = build_slot("poll-armed-b");
        // Far enough out that nothing promotes mid-test.
        let base = flz_core::util::wall_now_ms() + 600_000;

        core.arm_timer(&a, base, unix_stream());
        core.arm_timer(&a, base + 10, unix_stream());
        core.arm_timer(&b, base + 5, unix_stream());
        assert_eq!(core.timers_armed(), 3);

        core.arm_timer(&a, base + 10, unix_stream());
        assert_eq!(core.timers_armed(), 3, "re-arming replaces, it never adds");

        core.disarm_timer(&a.entity_identity, base + 10);
        assert_eq!(core.timers_armed(), 2, "a disarm removes exactly one entry");
        core.disarm_timer(&a.entity_identity, base + 10);
        assert_eq!(core.timers_armed(), 2, "and is idempotent");

        core.discard_entity_timers(&a.entity_identity);
        assert_eq!(
            core.timers_armed(),
            1,
            "discarding an Entity takes every entry it owns and no other's"
        );
        core.discard_entity_timers(&b.entity_identity);
        assert_eq!(core.timers_armed(), 0);
    }

    // ------------------------------------------------ backoff without a sleep

    /// The retry backoff holds no thread: the completing call returns at once,
    /// the entity stays handed out until its due time — a producer enqueueing
    /// meanwhile cannot claim a round — and the re-hand then leads with the
    /// held-back items, ahead of what arrived during the window.
    #[test]
    fn a_backoff_defers_the_re_hand_without_holding_a_thread() {
        let core = PollCore::new();
        let (_i, slot) = build_slot_of::<SlowRetryOp>("poll-defer");
        let stream = unix_stream();

        let act = queue_and_hand_out(&core, &slot, &stream, 2);
        let (token, _slot, items) = act.into_work().expect("work activation");

        let started = Instant::now();
        core.retry_after_failure(token, items, "boom".into());
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "the completing thread was held for the backoff"
        );

        // A producer during the window: it enqueues, but the entity is still
        // handed out, so no round can be claimed and no poller may serve it.
        assert!(slot.mailbox.try_push(payload(9, &stream)).is_ok());
        assert!(!slot.mailbox.try_mark_inflight());
        assert!(matches!(
            core.poll(Duration::from_millis(300)),
            Polled::Timeout
        ));

        let act = match core.poll(Duration::from_secs(6)) {
            Polled::Activation(a) => a,
            _ => panic!("the deferred re-hand never came due"),
        };
        assert_eq!(
            ordinals(&act),
            vec![0, 1, 9],
            "the holdback leads, in order, ahead of the window's arrival"
        );
        assert_eq!(act.items()[0].attempt, 2);
        core.complete(act.token);
    }

    /// Correlated failure — every entity failing at once — leaves pollers free
    /// to serve the entities whose work would succeed. This is the whole point
    /// of not sleeping a worker: with the pool sized to the CPU count, N
    /// deferred entities would otherwise be N workers doing nothing.
    #[test]
    fn correlated_failure_leaves_pollers_serving_healthy_entities() {
        let core = PollCore::new();
        let (_ih, healthy) = build_slot("poll-correlated-healthy");
        let stream = unix_stream();

        let failing: Vec<(Arc<StreamInstance>, Arc<EntitySlot>)> = (0..4)
            .map(|i| build_slot_of::<SlowRetryOp>(&format!("poll-correlated-{i}")))
            .collect();
        for (_instance, slot) in &failing {
            let act = queue_and_hand_out(&core, slot, &stream, 1);
            let (token, _slot, items) = act.into_work().expect("work activation");
            core.retry_after_failure(token, items, "dependency is down".into());
        }

        let act = queue_and_hand_out(&core, &healthy, &stream, 1);
        assert!(
            Arc::ptr_eq(act.slot(), &healthy),
            "a healthy entity was starved by the deferred ones"
        );
        core.complete(act.token);
    }

    // ------------------------------------- the jobs-underway cap and its signal

    /// Hand out one activation per slot, each occupying a job, and return the
    /// tokens still outstanding.
    fn occupy(core: &PollCore, slots: &[Arc<EntitySlot>], stream: &Arc<FlezhaStream>) -> Vec<u64> {
        slots
            .iter()
            .map(|slot| queue_and_hand_out(core, slot, stream, 1).token)
            .collect()
    }

    /// The cap is the core's own limit on jobs underway, independent of how
    /// many threads a host runs: at it, queued entity work stays queued —
    /// pollers wait as if `ready` were empty — and one completion is enough
    /// to release exactly one more.
    #[test]
    fn handout_stops_at_the_cap_and_resumes_on_completion() {
        let core = PollCore::with_max_inflight(2);
        let slots: Vec<Arc<EntitySlot>> = (0..3)
            .map(|i| build_slot(&format!("poll-cap-{i}")).1)
            .collect();
        let stream = unix_stream();

        let held = occupy(&core, &slots[..2], &stream);

        // The third entity's round is queued and runnable; the cap holds it.
        assert!(slots[2].mailbox.try_push(payload(0, &stream)).is_ok());
        assert!(slots[2].mailbox.try_mark_inflight());
        core.schedule(&slots[2]);
        assert!(
            matches!(core.poll(Duration::from_millis(100)), Polled::Timeout),
            "work was handed out past the cap"
        );

        core.complete(held[0]);
        let act = match core.poll(Duration::from_millis(500)) {
            Polled::Activation(a) => a,
            _ => panic!("a completion did not release the queued work"),
        };
        assert!(Arc::ptr_eq(act.slot(), &slots[2]));
        core.complete(act.token);
        core.complete(held[1]);
    }

    /// Command activations are barrier-bounded by the drivers that enqueue
    /// them and answer a caller blocked on a rendezvous — holding one behind
    /// capped entity work would deadlock that caller, so the cap never gates
    /// them.
    #[test]
    fn a_command_activation_is_served_at_the_cap() {
        let core = PollCore::with_max_inflight(1);
        let (_i, slot) = build_slot("poll-cap-command");
        let stream = unix_stream();
        let held = occupy(&core, std::slice::from_ref(&slot), &stream);

        let (_ic, other) = build_slot("poll-cap-command-other");
        assert!(other.mailbox.try_push(payload(0, &stream)).is_ok());
        assert!(other.mailbox.try_mark_inflight());
        core.schedule(&other);

        let (reply, _rx) = crossbeam::channel::bounded(1);
        core.submit_direct(flz_core::poll_core::EnginePayload::Command {
            target: flz_core::host::entity_ref_of(&other),
            change_seq: 0,
            factory: "poll_core_exclusivity_op".into(),
            kind: flz_core::schema::ActivationKind::SnapshotRequest,
            reply,
        });

        match core.poll(Duration::from_millis(500)) {
            Polled::Activation(act) => assert!(
                act.items().is_empty(),
                "the command activation must be served ahead of the capped work"
            ),
            _ => panic!("the cap gated a command activation"),
        }
        core.complete(held[0]);
    }

    /// Starvation is queued work with nobody left to take it: the host is
    /// told, and decides for itself what to do about it (the Rust arm grows
    /// its pool). Nothing is queued here until the waiter is parked, so the
    /// wake is the signal and not a pre-existing condition.
    #[test]
    fn starvation_wakes_when_work_is_queued_with_no_poller() {
        let core = Arc::new(PollCore::new());
        let (_i, slot) = build_slot("poll-starve-queued");
        let stream = unix_stream();

        let supervisor = {
            let core = Arc::clone(&core);
            std::thread::spawn(move || core.wait_starvation(Duration::from_secs(5)))
        };
        std::thread::sleep(Duration::from_millis(50));

        let started = Instant::now();
        assert!(slot.mailbox.try_push(payload(0, &stream)).is_ok());
        assert!(slot.mailbox.try_mark_inflight());
        core.schedule(&slot);

        assert!(
            matches!(supervisor.join().unwrap(), Starvation::Observed),
            "queued work did not read as starvation"
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the waiter timed out instead"
        );
    }

    /// A direct activation queued with nobody to take it is starvation too,
    /// and unlike entity work the cap does not gate it: a workflow driver is
    /// blocked on its step, so "every worker is inside user code" must buy a
    /// thread rather than wedge the stage.
    #[test]
    fn starvation_wakes_at_a_queued_direct_activation() {
        let core = Arc::new(PollCore::with_max_inflight(1));
        let (_i, slot) = build_slot("poll-starve-direct");
        let stream = unix_stream();
        // At the cap: only the direct activation can make this starvation.
        let _held = occupy(&core, std::slice::from_ref(&slot), &stream);

        let supervisor = {
            let core = Arc::clone(&core);
            std::thread::spawn(move || core.wait_starvation(Duration::from_secs(5)))
        };
        std::thread::sleep(Duration::from_millis(50));

        let (reply, _rx) = crossbeam::channel::bounded(1);
        core.submit_direct(flz_core::poll_core::EnginePayload::Command {
            target: flz_core::host::entity_ref_of(&slot),
            change_seq: 0,
            factory: "poll_core_exclusivity_op".into(),
            kind: flz_core::schema::ActivationKind::SnapshotRequest,
            reply,
        });

        assert!(
            matches!(supervisor.join().unwrap(), Starvation::Observed),
            "a queued direct activation did not read as starvation"
        );
    }

    /// A deferred wake coming due with no poller to promote it is starvation
    /// too — otherwise "every worker blocked in user code" would hold a timer
    /// firing until one of them finished.
    #[test]
    fn starvation_wakes_at_a_due_deferred_wake() {
        let core = Arc::new(PollCore::new());
        let (_i, slot) = build_slot("poll-starve-wake");
        slot.timers
            .lock()
            .unwrap()
            .apply(&TimerOp::register(TimerKind::ProcessingTime, 0));
        assert!(slot.mailbox.try_mark_inflight());
        // Due 150 ms out: the waiter must shorten its own wait to it.
        core.arm_timer(&slot, flz_core::util::wall_now_ms() + 150, unix_stream());

        let started = Instant::now();
        assert!(
            matches!(
                core.wait_starvation(Duration::from_secs(5)),
                Starvation::Observed
            ),
            "a due wake with no poller did not read as starvation"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "the waiter woke before the entry was due"
        );
        assert!(started.elapsed() < Duration::from_secs(2));
    }

    /// A poller inside `poll` is about to take the work, so nothing is
    /// starved — that is what keeps a healthy runner from growing threads it
    /// does not need.
    #[test]
    fn no_starvation_while_a_poller_is_waiting() {
        let core = Arc::new(PollCore::new());
        let (_i, slot) = build_slot("poll-starve-served");
        let stream = unix_stream();

        let poller = {
            let core = Arc::clone(&core);
            std::thread::spawn(move || core.poll(Duration::from_secs(2)))
        };
        std::thread::sleep(Duration::from_millis(50));
        assert!(slot.mailbox.try_push(payload(0, &stream)).is_ok());
        assert!(slot.mailbox.try_mark_inflight());
        core.schedule(&slot);

        assert!(
            matches!(
                core.wait_starvation(Duration::from_millis(300)),
                Starvation::Timeout
            ),
            "work a waiting poller can serve must not read as starvation"
        );
        match poller.join().unwrap() {
            Polled::Activation(act) => {
                core.complete(act.token);
            }
            _ => panic!("the waiting poller did not take the queued work"),
        }
    }

    /// A saturated runner is BUSY, not starved: the queue is never empty and
    /// the workers are outside `poll` most of the time, yet they keep coming
    /// back for more. Reporting that as starvation would answer a runner that
    /// is working at capacity with ever more threads — measured as a
    /// throughput collapse, which is why the signal requires a dwell with no
    /// `poll` entry at all rather than a snapshot of "nobody is polling".
    #[test]
    fn a_saturated_runner_does_not_read_as_starved() {
        let core = Arc::new(PollCore::new());
        let slots: Vec<Arc<EntitySlot>> = (0..4)
            .map(|i| build_slot(&format!("poll-saturated-{i}")).1)
            .collect();
        let stop = Arc::new(AtomicBool::new(false));

        // Keeps every entity's queue non-empty for the whole run.
        let feeder = {
            let (core, slots, stop) = (Arc::clone(&core), slots.clone(), Arc::clone(&stop));
            std::thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    for slot in &slots {
                        crate::runner::poll_core_tests::fixtures::push_input(&core, slot, 1);
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
            })
        };
        // One worker spending most of its time in "user code" — so
        // `pollers_inside` reads zero at almost any instant — but re-entering
        // `poll` every few milliseconds.
        let worker = {
            let (core, stop) = (Arc::clone(&core), Arc::clone(&stop));
            std::thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    if let Polled::Activation(act) = core.poll(Duration::from_millis(20)) {
                        std::thread::sleep(Duration::from_millis(5));
                        core.complete(act.token);
                    }
                }
            })
        };

        let verdict = core.wait_starvation(Duration::from_millis(600));
        stop.store(true, Ordering::Release);
        feeder.join().unwrap();
        worker.join().unwrap();
        assert!(
            matches!(verdict, Starvation::Timeout),
            "a runner working through a full queue must not read as starved"
        );
    }

    /// Shutdown is not delayed by a backoff: there is no sleep left to run
    /// out, so a poller observes it immediately.
    #[test]
    fn shutdown_during_a_deferred_window_is_immediate() {
        let core = PollCore::new();
        let (_i, slot) = build_slot_of::<SlowRetryOp>("poll-defer-shutdown");
        let stream = unix_stream();

        let act = queue_and_hand_out(&core, &slot, &stream, 1);
        let (token, _slot, items) = act.into_work().expect("work activation");
        core.retry_after_failure(token, items, "boom".into());

        core.shutdown();
        let started = Instant::now();
        assert!(matches!(
            core.poll(Duration::from_secs(30)),
            Polled::ShuttingDown
        ));
        assert!(started.elapsed() < Duration::from_millis(200));
    }
}
