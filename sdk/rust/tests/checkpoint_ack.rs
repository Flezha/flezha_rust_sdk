//! Checkpoint ack granularity: the consumed offset advances only when an
//! interval closes, mid-interval outputs ship without one, and a restore
//! discards the open interval.

use std::sync::Arc;
use std::time::Duration;

use flezha_runner::channel::ChannelBuilder;
use flezha_runner::procedure::{Procedure, ProcedureCtx, ProcedureError};
use flezha_runner::state::{RestoreInitPayload, StateHandle, WallClock};
use flezha_runner::StreamOperative;

mod common;
use common::*;

// ---------------------------------------------------------------------------
// effectively-once-checkpoint-granularity
// ---------------------------------------------------------------------------

/// Folds every input into state; `commit` closes the interval, `poison` parks.
#[derive(Default)]
struct CheckpointProcedure;
impl Procedure for CheckpointProcedure {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = TickChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "checkpoint_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
        0
    }
    fn apply_change(state: &mut i64, _change: &TickChange) {
        *state += 1;
    }
    fn run(
        &mut self,
        input: String,
        mut state: StateHandle<'_, Self>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        match input.as_str() {
            "poison" => Err(ProcedureError::non_retryable("poison")),
            "commit" => {
                state.apply(TickChange);
                ctx.checkpoint();
                Ok(())
            }
            "emit" => {
                state.apply(TickChange);
                ctx.emit_keyed::<String>("k".to_string(), input)
            }
            // Pure accumulation: nothing durable, nothing emitted.
            _ => {
                state.apply(TickChange);
                Ok(())
            }
        }
    }
}

struct CheckpointOperative;
impl StreamOperative for CheckpointOperative {
    fn factory_id() -> &'static str {
        "checkpoint_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.effectively_once();
        c.checkpoint_ack();
        c.input::<String>("input")
            .into::<CheckpointProcedure>()
            .named("out")
            .keyed()
            .sink("kafka");
    }
}

/// Spec "Consumed-offset advances only at checkpoint boundaries" + "Mid-interval
/// outputs flush when they are produced": an accumulating input makes nothing
/// durable at all; an emitting one ships its output with NO offset (it
/// acknowledges nothing — the state it came from is not durable yet); and the
/// checkpoint ships the whole interval's changes with the closing offset.
#[test]
fn checkpoint_ack_holds_the_offset_until_the_interval_closes() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();

    let (handle, _instance) = setup::<CheckpointOperative>("cp-instance");

    dispatch_at(&handle, "cp-instance", 1, "acc"); // fold only
    dispatch_at(&handle, "cp-instance", 2, "emit"); // fold + emit
    dispatch_at(&handle, "cp-instance", 3, "commit"); // fold + close

    assert!(
        wait_until(
            || captured_effect_bodies()
                .iter()
                .any(|b| b.consumed_offset == Some(3)),
            Duration::from_secs(5)
        ),
        "the checkpoint record never reached the state sink"
    );

    let bodies = captured_effect_bodies();
    assert_eq!(
        bodies.len(),
        2,
        "expected one mid-interval output record and one checkpoint record, got {bodies:?}"
    );

    // The accumulating input produced nothing durable, and the emitting one
    // acknowledged nothing.
    let mid = &bodies[0];
    assert_eq!(
        mid.consumed_offset, None,
        "a mid-interval emit must not ack"
    );
    assert!(
        mid.changes.is_empty(),
        "state must not be durable ahead of its offset"
    );
    assert_eq!(mid.outputs.len(), 1, "the emit still flushes as it happens");

    // The checkpoint carries the WHOLE interval — all three folds — plus the
    // offset of the input that closed it.
    let checkpoint = &bodies[1];
    assert_eq!(checkpoint.consumed_offset, Some(3));
    assert_eq!(
        checkpoint.changes.len(),
        3,
        "the checkpoint must carry every change the interval accumulated"
    );
}

/// Spec "A dead-letter park closes the interval": the poison input is
/// acknowledged together with the interval's accumulated state, so a later
/// replay starts after it instead of re-tripping it.
#[test]
fn a_park_closes_the_checkpoint_interval() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();

    let (handle, _instance) = setup::<CheckpointOperative>("cp-park-instance");

    dispatch_at(&handle, "cp-park-instance", 7, "acc");
    dispatch_at(&handle, "cp-park-instance", 8, "poison");

    assert!(
        wait_until(
            || captured_effect_bodies().iter().any(|b| b.dlq.is_some()),
            Duration::from_secs(5)
        ),
        "the park record never reached the state sink"
    );

    let bodies = captured_effect_bodies();
    let park = bodies.iter().find(|b| b.dlq.is_some()).unwrap();

    assert_eq!(
        park.consumed_offset,
        Some(8),
        "the parked offset must be acknowledged, or the replay re-trips the poison"
    );
    assert_eq!(
        park.changes.len(),
        1,
        "the park must carry the interval's accumulated state, or offset and state tear"
    );
}

/// Spec "Recovery ... SHALL discard any open interval": the changes an
/// abandoned interval applied belong to a past the restore replaced — the
/// controller re-forwards the inputs that produced them.
#[test]
fn a_restore_discards_the_open_interval() {
    use flezha_runner::entity::flusher::{FlushableBuffer, TypedBuffer};

    let mut buf: TypedBuffer<CheckpointProcedure> = TypedBuffer::default();
    buf.staged.changes.push(TickChange);
    buf.staged.changes.push(TickChange);

    // An open interval is not flushable: the background flusher must never
    // ship it on its own (that would put state ahead of its offset).
    assert!(
        buf.is_empty(),
        "an open interval must not look flushable to the flusher"
    );

    buf.discard_staged();
    assert!(
        buf.staged.changes.is_empty() && !buf.staged.is_open(),
        "a restore must drop the open interval"
    );
}

/// Spec "Journaled input may be discarded" / "Un-journaled input is not
/// silently destroyed": the restore-time mailbox clear is conditional on the
/// controller re-forwarding what it drops. It journals inputs before
/// forwarding for the effectively-once tier and for nothing else, so a plain
/// stateful class's queue is the only copy its input has.
#[test]
fn a_restore_discards_queued_input_only_where_the_controller_re_forwards_it() {
    use flezha_runner::entity::{make_entity_payload, restore_discarded_inputs};

    let (_tier_handle, tier) = setup::<TierOperative>("restore-mailbox-tier");
    let (_plain_handle, plain) = setup::<KeyedStampOperative>("restore-mailbox-plain");
    let (stream, _observer) = stream_pair();

    for instance in [&tier, &plain] {
        let value: Arc<dyn std::any::Any + Send + Sync> = Arc::new("queued".to_string());
        assert!(
            instance.entities[0][0]
                .mailbox
                .try_push(flezha_runner::entity::EntityItem::Input(
                    make_entity_payload(&value, &stream, None, false),
                ))
                .is_ok(),
            "mailbox has room"
        );
    }

    let before = restore_discarded_inputs();
    let tier_discarded = tier.entities[0][0].discard_queued_input_on_restore();
    let plain_discarded = plain.entities[0][0].discard_queued_input_on_restore();

    assert_eq!(
        tier_discarded, 1,
        "a tier input is durable in the input log the controller re-forwards; \
         keeping it would double-apply it"
    );
    assert!(
        tier.entities[0][0].mailbox.is_empty(),
        "the tier mailbox must be drained"
    );
    assert_eq!(
        plain_discarded, 0,
        "a journal-less class's queued input exists nowhere else — dropping it is data loss"
    );
    assert!(
        !plain.entities[0][0].mailbox.is_empty(),
        "the journal-less mailbox must survive the restore"
    );
    assert_eq!(
        restore_discarded_inputs() - before,
        1,
        "every discard must be counted, or the loss leaves no trace"
    );
}
