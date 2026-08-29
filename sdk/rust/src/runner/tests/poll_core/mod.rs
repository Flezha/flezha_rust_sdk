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
    pub(crate) fn push_input(core: &PollCore, slot: &Arc<EntitySlot>, value: usize) {
        assert!(slot
            .mailbox
            .try_push(payload(value, &unix_stream()))
            .is_ok());
        if slot.mailbox.claim_inflight() {
            core.schedule(slot);
        }
    }
}

/// The procedures, operatives and slot builders the three test modules
/// below are written against.
mod support {

    use std::sync::Arc;
    use std::time::Duration;

    use flz_core::poll_core::{EngineActivation, PollCore, Polled};
    use flz_core::restore::RestoreInitPayload;
    use flz_transport::transport::FlezhaStream;

    use super::fixtures::{build_slot_of, payload, unix_stream};
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
    pub(super) struct PollProc;
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

    pub(super) struct PollOp;
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
    pub(super) struct TinyProc;
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

    pub(super) struct TinyOp;
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
    pub(super) fn full_slot(instance_id: &str) -> (Arc<StreamInstance>, Arc<EntitySlot>) {
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

    pub(super) fn build_slot(instance_id: &str) -> (Arc<StreamInstance>, Arc<EntitySlot>) {
        build_slot_of::<PollOp>(instance_id)
    }

    /// Arm one already-due processing-time timer and keep its firing on this
    /// core: `admission::admit_from_reader` only schedules a round when it wins the
    /// inflight CAS, and the caller has already taken it.
    pub(super) fn arm_due_timer(core: &PollCore, slot: &Arc<EntitySlot>, at: u64) {
        slot.timers
            .lock()
            .unwrap()
            .apply(&TimerOp::register(TimerKind::ProcessingTime, at));
        assert!(slot.mailbox.claim_inflight());
        core.arm_timer(slot, at, unix_stream());
    }

    /// The one timer item a promotion left on the slot's mailbox.
    pub(super) fn assemble_timer(core: &PollCore, slot: &Arc<EntitySlot>) -> (u64, u64) {
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
    pub(super) fn queue_and_hand_out(
        core: &PollCore,
        slot: &Arc<EntitySlot>,
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
                let EntityItem::Input(p) = &w.item else {
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
