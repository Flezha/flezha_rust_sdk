//! Relocated engine tests: handout exclusivity, ordering, the ladder.

/// Slot scaffolding shared by the engine's own tests and the poll pool's
/// (`operator::poll_pool`) — one definition of how a test builds a Cell
/// and hands it an input.
pub(crate) mod fixtures {
    use std::any::Any;
    use std::sync::Arc;

    use flz_transport::transport::FlezhaStream;

    use crate::cell::{CellItem, CellPayload, CellSlot};
    use crate::channel::ChannelBuilder;
    use crate::operator::builder::build_topology_with_persistence;
    use crate::operator::instance::OperatorInstance;
    use crate::operator::UnboundedOperator;
    use crate::runner::poll_core::PollCore;

    /// One instance of `O` and its first Cell.
    pub(crate) fn build_slot_of<O: UnboundedOperator>(
        instance_id: &str,
    ) -> (Arc<OperatorInstance>, Arc<CellSlot>) {
        let cb = ChannelBuilder::new();
        O::channels(&cb);
        let topology = build_topology_with_persistence(
            O::factory_id(),
            cb,
            O::PERSISTENCE_ENABLED,
            O::SLEEP_WHEN_IDLE,
        )
        .expect("topology");
        let instance = OperatorInstance::new(topology, Arc::from(instance_id));
        let slot = Arc::clone(&instance.cells[0][0]);
        (instance, slot)
    }

    /// A stream token for an input or firing. The peer is dropped straight
    /// away — the engine only carries the handle, it never writes to it.
    pub(crate) fn unix_stream() -> Arc<FlezhaStream> {
        let (sock, _peer) = std::os::unix::net::UnixStream::pair().expect("pair");
        Arc::new(FlezhaStream::Unix(sock))
    }

    pub(crate) fn payload(ordinal: usize, stream: &Arc<FlezhaStream>) -> CellItem {
        CellItem::Input(CellPayload {
            value: Arc::new(ordinal) as Arc<dyn Any + Send + Sync>,
            stream: Arc::clone(stream),
            consumed_offset: None,
            replay: false,
            forwarded: false,
            hold: None,
        })
    }

    /// Occupy the calling thread for `span` — a stand-in for work — without
    /// yielding it to the scheduler the way a sleep would.
    pub(crate) fn hold_for(span: std::time::Duration) {
        let started = std::time::Instant::now();
        while started.elapsed() < span {
            std::hint::spin_loop();
        }
    }

    /// Drive `done` until it holds, failing the test at a 10 s deadline with
    /// `what` naming the condition. Shared by the engine's own tests and the
    /// poll pool's. The one polling loop in the crate's own tests; its
    /// interval is the one deliberate sleep.
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
    pub(crate) fn push_input(core: &PollCore, slot: &Arc<CellSlot>, value: usize) {
        assert!(slot
            .mailbox
            .try_push(payload(value, &unix_stream()))
            .is_ok());
        if slot.mailbox.claim_inflight() {
            core.schedule(slot);
        }
    }
}

/// The tasklets, operators and slot builders the three test modules
/// below are written against.
mod support {

    use std::sync::Arc;
    use std::time::Duration;

    use flz_core::poll_core::{EngineActivation, PollCore, Polled};
    use flz_transport::transport::FlezhaStream;

    use super::fixtures::{build_slot_of, payload, unix_stream};
    use crate::cell::timer::{TimerKind, TimerOp};
    use crate::cell::{CellItem, CellSlot};
    use crate::channel::ChannelBuilder;
    use crate::operator::instance::OperatorInstance;
    use crate::operator::UnboundedOperator;
    use crate::tasklet::error::TaskletError;
    use crate::tasklet::{Ctx, Tasklet};

    #[derive(Default)]
    pub(super) struct PollTask;
    #[flezha_runner::tasklet(name = "poll_core_exclusivity_task")]
    impl Tasklet for PollTask {
        type Input = usize;
        fn run(&mut self, _: usize, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
            Ok(())
        }
    }

    pub(super) struct PollOp;
    impl UnboundedOperator for PollOp {
        fn factory_id() -> &'static str {
            "poll_core_exclusivity_op"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<usize>("input").into::<PollTask>();
        }
    }

    /// A one-slot mailbox: the smallest thing that can be full, so a wake
    /// promoted against it always meets the no-room path.
    #[derive(Default)]
    pub(super) struct TinyTask;
    #[flezha_runner::tasklet(name = "poll_core_tiny_mailbox_task")]
    impl Tasklet for TinyTask {
        type Input = usize;
        fn mailbox_capacity() -> usize {
            1
        }
        fn run(&mut self, _: usize, _: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
            Ok(())
        }
    }

    pub(super) struct TinyOp;
    impl UnboundedOperator for TinyOp {
        fn factory_id() -> &'static str {
            "poll_core_tiny_mailbox_op"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<usize>("input").into::<TinyTask>();
        }
    }

    /// A slot whose one mailbox slot is taken and whose round is claimed by
    /// nobody: every wake promoted against it meets a full mailbox.
    pub(super) fn full_slot(instance_id: &str) -> (Arc<OperatorInstance>, Arc<CellSlot>) {
        let (instance, slot) = build_slot_of::<TinyOp>(instance_id);
        assert!(slot.mailbox.try_push(payload(0, &unix_stream())).is_ok());
        assert!(slot.mailbox.is_full());
        // Claim the round so nothing drains it — the caller decides when room
        // appears.
        assert!(slot.mailbox.claim_inflight());
        (instance, slot)
    }

    /// A backoff long enough to observe the deferral window from the outside:
    /// base 2 s, so attempt 1 comes due in 1–3 s.
    pub(super) struct SlowRetryOp;
    impl UnboundedOperator for SlowRetryOp {
        fn factory_id() -> &'static str {
            "poll_core_slow_retry_op"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<usize>("input")
                .retry_backoff_ms(2_000)
                .into::<PollTask>();
        }
    }

    pub(super) fn build_slot(instance_id: &str) -> (Arc<OperatorInstance>, Arc<CellSlot>) {
        build_slot_of::<PollOp>(instance_id)
    }

    /// Arm one already-due processing-time timer and keep its firing on this
    /// core: `admission::admit_from_reader` only schedules a round when it wins the
    /// inflight CAS, and the caller has already taken it.
    pub(super) fn arm_due_timer(core: &PollCore, slot: &Arc<CellSlot>, at: u64) {
        slot.timers
            .lock()
            .unwrap()
            .apply(&TimerOp::register(TimerKind::ProcessingTime, at));
        assert!(slot.mailbox.claim_inflight());
        core.arm_timer(slot, at, unix_stream());
    }

    /// The one timer item a promotion left on the slot's mailbox.
    pub(super) fn assemble_timer(core: &PollCore, slot: &Arc<CellSlot>) -> (u64, u64) {
        let act = core.try_assemble(slot).expect("a firing to assemble");
        assert_eq!(act.items().len(), 1);
        assert!(matches!(act.items()[0].item, CellItem::Timer(_)));
        let now = act.items()[0]
            .now
            .expect("a firing carries its scheduled at");
        (act.token, now)
    }

    /// The producer protocol followed by one handout: queue `count` inputs
    /// (`0..count`), claim the round on the winning inflight CAS, and take the
    /// activation it earns.
    pub(super) fn queue_and_hand_out(
        core: &PollCore,
        slot: &Arc<CellSlot>,
        stream: &Arc<FlezhaStream>,
        count: usize,
    ) -> EngineActivation {
        for i in 0..count {
            assert!(slot.mailbox.try_push(payload(i, stream)).is_ok());
        }
        assert!(slot.mailbox.claim_inflight());
        core.schedule(slot);
        match core.poll(Duration::from_millis(100)) {
            Polled::Activation(act) => act,
            _ => panic!("expected an activation"),
        }
    }

    /// The ordinals a batch carries, in order.
    pub(super) fn ordinals(act: &EngineActivation) -> Vec<usize> {
        act.items()
            .iter()
            .map(|w| {
                let CellItem::Input(p) = &w.item else {
                    panic!("unexpected timer item")
                };
                *p.value.downcast_ref::<usize>().unwrap()
            })
            .collect()
    }
}

mod cap_and_starvation;
mod handout;
mod wake_list;
