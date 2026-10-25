//! Checkpoint ack granularity: the consumed offset advances only when an
//! interval closes, mid-interval outputs ship without one, a restore
//! discards the open interval, and a compaction or a handover closes it
//! early.

mod common;
use common::prelude::*;
use flezha_runner::{
    AckGranularity, BeyondWindowPolicy, CheckpointOutputs, CheckpointPolicy, Exactly,
    RelaySettings, Run,
};

// ---------------------------------------------------------------------------
// effectively-once-checkpoint-granularity
// ---------------------------------------------------------------------------

/// Folds every input into state; `commit` closes the interval, `poison` parks.
#[derive(Default)]
struct CheckpointTasklet;
#[flezha_runner::tasklet(name = "checkpoint_tasklet")]
impl Tasklet for CheckpointTasklet {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = TickChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        match input.as_str() {
            "poison" => Err(TaskletError::non_retryable("poison")),
            "commit" => {
                ctx.change(TickChange);
                ctx.checkpoint();
                Ok(())
            }
            "emit" => {
                ctx.change(TickChange);
                ctx.emit_keyed("k".to_string(), input)
            }
            // Pure accumulation: nothing durable, nothing emitted.
            _ => {
                ctx.change(TickChange);
                Ok(())
            }
        }
    }
}

struct CheckpointRelay;
impl Relay for CheckpointRelay {
    fn factory_id() -> &'static str {
        "checkpoint_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<CheckpointTasklet>()
            .keyed()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Flow {
                exactly: Exactly::EffectivelyOnce {
                    ack: AckGranularity::Checkpoint(CheckpointPolicy {
                        every: None,
                        outputs: CheckpointOutputs::AsProduced,
                        max_delay_ms: None,
                    }),
                    correction_ms: None,
                    beyond_window: BeyondWindowPolicy::ProcessAsArrival,
                },
            },
            ..RelaySettings::default()
        }
    }
}

/// Spec "Consumed-offset advances only at checkpoint boundaries" + "Mid-interval
/// outputs flush when they are produced": an accumulating input makes nothing
/// durable at all; an emitting one ships its output with NO offset (it
/// acknowledges nothing — the state it came from is not durable yet); and the
/// checkpoint ships the whole interval's changes with the closing offset.
#[test]
fn ack_at_checkpoint_holds_the_offset_until_the_interval_closes() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();

    let (handle, _instance) = setup::<CheckpointRelay>("cp-instance");

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

    let (handle, _instance) = setup::<CheckpointRelay>("cp-park-instance");

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
    use flezha_runner::cell::flusher::{FlushableBuffer, TypedBuffer};

    let mut buf: TypedBuffer<CheckpointTasklet> = TypedBuffer::default();
    buf.staged.changes.push(TickChange);
    buf.staged.changes.push(TickChange);

    // An open interval is not flushable: the background flusher must never
    // ship it on its own (that would put state ahead of its offset).
    assert!(
        buf.is_empty(),
        "an open interval must not look flushable to the flusher"
    );

    buf.discard_unshipped();
    assert!(
        buf.staged.changes.is_empty() && !buf.staged.is_open(),
        "a restore must drop the open interval"
    );
}

/// Spec "Journaled input may be discarded" / "Un-journaled input is not
/// silently destroyed": the restore-time mailbox clear is conditional on the
/// controller re-forwarding what it drops. It journals inputs before
/// forwarding for the effectively-once tier and a class that sleeps, and for
/// nothing else, so a plain stateful class's queue is the only copy its input
/// has.
#[test]
fn a_restore_discards_queued_input_only_where_the_controller_re_forwards_it() {
    use flezha_runner::cell::{make_cell_payload, restore_discarded_inputs};

    let (_tier_handle, tier) = setup::<TierRelay>("restore-mailbox-tier");
    let (_plain_handle, plain) = setup::<KeyedStampRelay>("restore-mailbox-plain");
    let (stream, _observer) = stream_pair();

    for instance in [&tier, &plain] {
        let value: Arc<dyn std::any::Any + Send + Sync> = Arc::new("queued".to_string());
        assert!(
            instance.cells[0][0]
                .mailbox
                .try_push(flezha_runner::cell::CellItem::Input(make_cell_payload(
                    &value, &stream, None, false
                ),))
                .is_ok(),
            "mailbox has room"
        );
    }

    let before = restore_discarded_inputs();
    let tier_discarded = tier.cells[0][0].discard_queued_input_on_restore();
    let plain_discarded = plain.cells[0][0].discard_queued_input_on_restore();

    assert_eq!(
        tier_discarded, 1,
        "a tier input is durable in the input log the controller re-forwards; \
         keeping it would double-apply it"
    );
    assert!(
        tier.cells[0][0].mailbox.is_empty(),
        "the tier mailbox must be drained"
    );
    assert_eq!(
        plain_discarded, 0,
        "a journal-less class's queued input exists nowhere else — dropping it is data loss"
    );
    assert!(
        !plain.cells[0][0].mailbox.is_empty(),
        "the journal-less mailbox must survive the restore"
    );
    assert_eq!(
        restore_discarded_inputs() - before,
        1,
        "every discard must be counted, or the loss leaves no trace"
    );
}

// ---------------------------------------------------------------------------
// A compaction inside an open interval
// ---------------------------------------------------------------------------

struct CheckpointEvery3Relay;
impl Relay for CheckpointEvery3Relay {
    fn factory_id() -> &'static str {
        "checkpoint_every3_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<CheckpointTasklet>()
            .keyed()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Flow {
                exactly: Exactly::EffectivelyOnce {
                    ack: AckGranularity::Checkpoint(CheckpointPolicy {
                        every: Some(3),
                        outputs: CheckpointOutputs::AsProduced,
                        max_delay_ms: None,
                    }),
                    correction_ms: None,
                    beyond_window: BeyondWindowPolicy::ProcessAsArrival,
                },
            },
            ..RelaySettings::default()
        }
    }
}

/// A snapshot as the controller stores it: header, body, and the seq that
/// `truncate_up_to` keeps records strictly above.
struct StoredSnapshot {
    header: Vec<u8>,
    body: Vec<u8>,
    seq: u64,
}

/// `f` over the Cell's flush buffer, under its lock.
fn with_buffer<R>(
    slot: &Arc<flezha_runner::cell::CellSlot>,
    f: impl FnOnce(&mut flezha_runner::cell::flusher::TypedBuffer<CheckpointTasklet>) -> R,
) -> R {
    f(slot
        .flush_buffer
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .as_any_mut()
        .downcast_mut()
        .expect("a typed checkpoint buffer"))
}

fn staged_inputs(slot: &Arc<flezha_runner::cell::CellSlot>) -> u32 {
    with_buffer(slot, |buffer| buffer.staged.inputs)
}

/// One `CompactCell`, as `compact_slot` runs it. An open interval closes
/// early; the snapshot is never refused for it.
fn compact(slot: &Arc<flezha_runner::cell::CellSlot>) -> StoredSnapshot {
    let (frame, seq, _size) = slot
        .dispatcher
        .snapshot_frame(slot)
        .unwrap_or_else(|e| panic!("refused with only an interval open: {e:?}"));
    assert_eq!(
        staged_inputs(slot),
        0,
        "the compaction closes the open interval"
    );
    let f = flz_core::test_support::split_cell_frame(&frame).expect("snapshot frame");
    StoredSnapshot {
        header: f.header.to_vec(),
        body: f.body.to_vec(),
        seq,
    }
}

/// The controller's WAL tail above `snapshot_seq` (all of it with no
/// snapshot), re-framed as it stores it. Only change and effect records reach
/// the WAL; a `processed_prefix` frame goes to the input log.
fn wal_above(snapshot_seq: Option<u64>) -> Vec<u8> {
    use flezha_runner::cell::state_sink::{
        CELL_EFFECT_RECORD_PAYLOAD_ID, CELL_STATE_CHANGE_PAYLOAD_ID,
    };
    TIER_SINK_FRAMES
        .lock()
        .unwrap()
        .iter()
        .filter(|(pid, _)| {
            [CELL_STATE_CHANGE_PAYLOAD_ID, CELL_EFFECT_RECORD_PAYLOAD_ID].contains(&pid.as_str())
        })
        .filter_map(|(_, frame)| flz_core::test_support::split_cell_frame(frame))
        .filter(|f| snapshot_seq.is_none_or(|s| f.seq > s))
        .flat_map(|f| {
            let header: [u8; 16] = f.header.try_into().expect("16-byte header");
            flz_core::restore::frame_record(f.seq, &header, f.body)
        })
        .collect()
}

/// The highest consumed offset any shipped record carries: the floor the
/// controller re-forwards above.
fn floor() -> u64 {
    captured_effect_bodies()
        .iter()
        .filter_map(|b| b.consumed_offset)
        .max()
        .expect("a consumed offset shipped")
}

/// Restore from snapshot + WAL tail; `(count, resume offset)`. The resume
/// offset is the highest consumed offset the tail carries (the compaction
/// re-states it above the snapshot), the one the controller re-forwards after.
fn restore_from(snapshot: Option<&StoredSnapshot>) -> (i64, u64) {
    let tail = wal_above(snapshot.map(|s| s.seq));
    let (count, _timers) =
        restore::<CheckpointTasklet>(snapshot.map(|s| (&s.header[..], &s.body[..])), &tail);
    (count, floor())
}

/// Offsets 1 (checkpoint), 2 and 3 (interval open), then a compaction.
fn open_an_interval_and_compact(instance_id: &str) -> (RelayDispatchHandle, StoredSnapshot) {
    let (handle, instance) = setup::<CheckpointEvery3Relay>(instance_id);
    open_an_interval(&handle, &instance, instance_id);
    let snapshot = compact(&instance.cells[0][0]);
    (handle, snapshot)
}

/// Offsets 1 (checkpoint), 2 and 3: an interval open, two inputs staged.
fn open_an_interval(
    handle: &RelayDispatchHandle,
    instance: &Arc<RelayInstance>,
    instance_id: &str,
) {
    dispatch_at(handle, instance_id, 1, "commit");
    assert!(wait_until(
        || captured_effect_bodies()
            .iter()
            .any(|b| b.consumed_offset == Some(1)),
        Duration::from_secs(5)
    ));
    dispatch_at(handle, instance_id, 2, "acc");
    dispatch_at(handle, instance_id, 3, "acc");
    let slot = Arc::clone(&instance.cells[0][0]);
    assert!(wait_until(
        || staged_inputs(&slot) == 2,
        Duration::from_secs(5)
    ));
}

/// KB-978 (a): the compaction closes the interval early, and the next
/// interval closes after it. That record ships above the snapshot seq, so a
/// restore must not fold the snapshot's inputs twice.
#[test]
fn a_compaction_inside_an_open_interval_does_not_double_count_its_close() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();

    let (handle, snapshot) = open_an_interval_and_compact("cp-compact-close");
    dispatch_at(&handle, "cp-compact-close", 4, "commit");
    assert!(wait_until(
        || captured_effect_bodies()
            .iter()
            .any(|b| b.consumed_offset == Some(4)),
        Duration::from_secs(5)
    ));

    let (count, resume) = restore_from(Some(&snapshot));
    assert_eq!(resume, 4);
    assert_eq!(
        count, 4,
        "four +1 inputs; a snapshot holding 2..3 has the close re-apply them (6)"
    );
}

/// KB-978 (b): the runner dies before the interval closes. A restore resumes
/// from the last shipped offset, so its state must not already hold the inputs
/// the controller re-forwards.
#[test]
fn a_compaction_inside_an_open_interval_does_not_double_count_a_replay() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();

    let (_handle, snapshot) = open_an_interval_and_compact("cp-compact-crash");
    // The runner dies here.
    let (count, resume) = restore_from(Some(&snapshot));
    let replayed = 3 - resume as i64; // offsets resume+1..=3 are re-forwarded, +1 each
    assert_eq!(
        count + replayed,
        3,
        "three +1 inputs; a snapshot holding 3 that resumes from offset 1 counts 5"
    );
}

/// Deadline-only: an interval closes on `max_delay_ms` alone, long enough
/// that no test input sees it close on its own.
struct CheckpointDeadlineRelay;
impl Relay for CheckpointDeadlineRelay {
    fn factory_id() -> &'static str {
        "checkpoint_deadline_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<CheckpointTasklet>()
            .keyed()
            .output_named("out");
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Flow {
                exactly: Exactly::EffectivelyOnce {
                    ack: AckGranularity::Checkpoint(CheckpointPolicy {
                        every: None,
                        outputs: CheckpointOutputs::AsProduced,
                        max_delay_ms: Some(600_000),
                    }),
                    correction_ms: None,
                    beyond_window: BeyondWindowPolicy::ProcessAsArrival,
                },
            },
            ..RelaySettings::default()
        }
    }
}

/// KB-1019: a deadline-only Cell under steady input is mid-interval at every
/// compaction sweep. Each compaction closes the interval early and takes its
/// snapshot, where it used to be refused at every sweep while the WAL grew.
#[test]
fn a_deadline_only_cell_under_steady_input_compacts() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();

    let id = "cp-compact-deadline";
    let (handle, instance) = setup::<CheckpointDeadlineRelay>(id);
    let slot = Arc::clone(&instance.cells[0][0]);
    let mut snapshot = None;
    for sweep in 0..2u64 {
        for offset in sweep * 3 + 1..=sweep * 3 + 3 {
            dispatch_at(&handle, id, offset, "acc");
        }
        assert!(wait_until(
            || staged_inputs(&slot) == 3,
            Duration::from_secs(5)
        ));
        snapshot = Some(compact(&slot));
    }

    let (count, resume) = restore_from(snapshot.as_ref());
    assert_eq!(
        resume, 6,
        "the early close carries the interval's consumed offset"
    );
    assert_eq!(count, 6, "six +1 inputs, each counted once");
}

/// The sum of every number in a serialized relay state.
fn total(state: &[u8]) -> i64 {
    fn walk(v: &serde_json::Value) -> i64 {
        match v {
            serde_json::Value::Number(n) => n.as_i64().unwrap_or(0),
            serde_json::Value::Object(m) => m
                .iter()
                .filter(|(k, _)| k.as_str() != "$cells")
                .map(|(_, v)| walk(v))
                .sum(),
            _ => 0,
        }
    }
    walk(&serde_json::from_slice(state).expect("state json"))
}

/// A destination instance that takes the handover `shipped`, then the
/// re-forward of every input above `floor` up to offset 3; its state's total.
fn arrive(id: &str, shipped: &str, floor: u64) -> i64 {
    use flz_core::command::cell_state::{apply_init_values, serialize_relay_state};
    let (handle, instance) = setup::<CheckpointEvery3Relay>(id);
    apply_init_values(&instance, shipped).unwrap();
    for offset in floor + 1..=3 {
        dispatch_at(&handle, id, offset, "acc");
    }
    let slot = Arc::clone(&instance.cells[0][0]);
    assert!(wait_until(
        || staged_inputs(&slot) == (3 - floor) as u32,
        Duration::from_secs(5)
    ));
    total(&serialize_relay_state(&instance))
}

/// KB-1018: a handover inside an open interval. `MigrationSerialization`
/// ships from a cut that closes the interval early, so the floor the
/// destination re-forwards above already covers every input the shipped
/// state holds, and each input counts once.
#[test]
fn a_handover_inside_an_open_interval_does_not_double_count_the_reforward() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();

    let (handle, src) = setup::<CheckpointEvery3Relay>("cp-move-src");
    open_an_interval(&handle, &src, "cp-move-src");
    let shipped = flz_core::command::cell_state::serialize_moved(&src);
    let floor = floor();
    assert_eq!(total(&shipped), 3);
    assert_eq!(floor, 3, "the handover's cut ships the interval's offset");
    let shipped = String::from_utf8(shipped).unwrap();

    assert_eq!(
        arrive("cp-move-dst", &shipped, floor),
        3,
        "three +1 inputs; the shipped open interval plus the re-forward counts 5"
    );
}

/// KB-1018, a retried attempt: its changes are in the live state, and no
/// record can carry them until the item resolves. The open interval still
/// closes, but neither cut takes the Cell: the handover omits it (the
/// destination restores it from its records and the re-forward re-runs the
/// input) and the compaction refuses as busy until a later sweep.
#[test]
fn a_cut_inside_a_retried_attempt_takes_nothing_of_it() {
    use flz_transport::errors::ErrorModel;
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();

    let (handle, src) = setup::<CheckpointEvery3Relay>("cp-move-retry");
    open_an_interval(&handle, &src, "cp-move-retry");
    let slot = Arc::clone(&src.cells[0][0]);
    // What a failed-but-retryable attempt leaves behind until its next one.
    with_buffer(&slot, |buffer| buffer.pending_attempt.push(TickChange));

    let shipped = flz_core::command::cell_state::serialize_moved(&src);
    assert_eq!(
        total(&shipped),
        0,
        "a Cell mid-retry is omitted from the handover, not shipped with its attempt"
    );
    assert_eq!(floor(), 3, "the open interval closed all the same");

    let refusal = slot
        .dispatcher
        .snapshot_frame(&slot)
        .expect_err("a compaction mid-retry is refused");
    assert_eq!(refusal.code(), "state.cell_busy");
}
