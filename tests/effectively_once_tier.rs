//! The effectively-once tier: one atomic effect record per activation on
//! the state output, the consumed-offset floor a replay skips, and the logical
//! clock a restore re-arms.

use std::io::Read;

use flezha_runner::cell::header::header_template_for;
use flezha_runner::{Exactly, RelaySettings, Run};

mod common;
use common::prelude::*;

/// The worker pool is process-wide and sized from `available_parallelism()`
/// unless it is pinned, which makes it a hidden shared resource across this
/// binary's cases. Pinned once, before the first dispatch builds it, so every
/// case here runs against the same host.
fn pinned_pool() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| std::env::set_var("FLEZHA_WORKER_POOL_SIZE", "4"));
}

// ----- Effectively-once tier: atomic effect record on the state output -----

/// Spec "Emits leave the primary stream in tier mode" + "Offset
/// round-trips": an effectively-once invocation's keyed emit travels inside
/// a `cell_effect_record` on the reliable state output — carrying the
/// change, the output (channel/key/payload), and the `flz.offset` the
/// input arrived with — and no output frame appears on the primary stream.
#[test]
fn tier_emits_ride_the_state_sink_as_one_effect_record() {
    use flezha_runner::cell::flusher::EffectRecordBody;

    pinned_pool();
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();

    let (handle, _instance) = setup::<TierRelay>("tier-instance");
    let (stream, mut observer) = stream_pair();

    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    md.insert("flz.offset".to_string(), "4711".to_string());
    handle.dispatch(
        "tier-instance",
        b"BTC/1m".to_vec(),
        &md,
        Arc::clone(&stream),
    );

    // The background flusher drains the effect entry within the linger window.
    assert!(
        wait_until(
            || {
                TIER_SINK_FRAMES
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(pid, _)| pid == "cell_effect_record")
            },
            Duration::from_secs(5)
        ),
        "no cell_effect_record reached the state sink"
    );

    // Outer frame layout is the state-change layout; header format_id = 2.
    let frames = TIER_SINK_FRAMES.lock().unwrap().clone();
    let (_, frame) = frames
        .iter()
        .find(|(pid, _)| pid == "cell_effect_record")
        .unwrap()
        .clone();
    let pid_len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    assert_eq!(
        frame[2 + pid_len + 8 + 9],
        2,
        "effect record header format_id"
    );

    let bodies = captured_effect_bodies();
    let body: &EffectRecordBody = &bodies[0];

    assert_eq!(body.consumed_offset, Some(4711), "offset must round-trip");
    assert_eq!(body.changes.len(), 1, "the invocation's change rides along");
    assert_eq!(body.outputs.len(), 1);
    assert_eq!(body.outputs[0].channel, "out");
    assert_eq!(body.outputs[0].key, "BTC/1m");
    assert_eq!(body.outputs[0].payload, b"BTC/1m".to_vec());

    // And the primary stream carried nothing: a blocking read with a deadline on it
    // times out rather than yielding a frame.
    observer.set_nonblocking(false).expect("observer blocking");
    observer
        .set_read_timeout(Some(Duration::from_millis(50)))
        .expect("observer read timeout");
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        match observer.read(&mut tmp) {
            Ok(0) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                break
            }
            Err(e) => panic!("observer read failed: {e}"),
        }
    }
    assert!(
        parse_frames(&buf).is_empty(),
        "tier emit leaked onto the primary stream"
    );
}

/// Forwards each input to [`Drain`] and counts it.
#[derive(Default)]
struct Forward;
#[flezha_runner::tasklet(name = "forward")]
impl Tasklet for Forward {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = TickChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(TickChange);
        ctx.emit(input)
    }
}

#[derive(Default)]
struct Drain;
#[flezha_runner::tasklet(name = "drain")]
impl Tasklet for Drain {
    type Input = String;
    type Output = String;
    fn run(&mut self, _input: String, _ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        Ok(())
    }
}

struct TierPassRelay;
impl Relay for TierPassRelay {
    fn factory_id() -> &'static str {
        "tier_pass_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<Forward>().into::<Drain>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Flow {
                exactly: Exactly::effectively_once(),
            },
            ..RelaySettings::default()
        }
    }
}

struct PlainPassRelay;
impl Relay for PlainPassRelay {
    fn factory_id() -> &'static str {
        "plain_pass_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<Forward>().into::<Drain>();
    }
}

/// An inter-tasklet value rides the record that carries its producer's
/// offset, on the tier and off it, so a restore can deliver it again to a
/// consumer that lost it.
#[test]
fn an_edge_value_rides_the_record_that_carries_its_producer_offset() {
    pinned_pool();
    let _serial = WAL_TEST_LOCK.lock().unwrap();

    for (id, tier) in [("edge-tier", true), ("edge-plain", false)] {
        install_capturing_tier_sink();
        let (handle, _instance) = if tier {
            setup::<TierPassRelay>(id)
        } else {
            setup::<PlainPassRelay>(id)
        };
        dispatch_at(&handle, id, 9, "v");

        let kept = || {
            captured_effect_bodies()
                .into_iter()
                .find(|b| b.consumed_offset == Some(9) && !b.edges.is_empty())
        };
        assert!(
            wait_until(|| kept().is_some(), Duration::from_secs(5)),
            "{id}: the pass's record never kept its edge value"
        );
        let body = kept().unwrap();
        assert_eq!(body.edges.len(), 1, "{id}");
        assert_eq!(body.edges[0].origin, Some(9), "{id}");
    }
}

/// Spec "Replay after crash" (SDK half): the restored consumed-floor makes
/// the Cell skip re-forwarded inputs it already durably consumed, and
/// process everything above — never double-applied, never lost.
#[test]
fn tier_replay_skips_inputs_at_or_below_the_consumed_floor() {
    pinned_pool();
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();

    let (handle, instance) = setup::<TierRelay>("tier-replay-instance");

    // Restore left this Cell durably consumed through offset 4711.
    instance.cells[0][0]
        .consumed_floor
        .store(4711, std::sync::atomic::Ordering::Release);

    // Re-forwarded tail: 4710 and 4711 already consumed, 4712 is new.
    dispatch_at(&handle, "tier-replay-instance", 4710, "old-1");
    dispatch_at(&handle, "tier-replay-instance", 4711, "old-2");
    dispatch_at(&handle, "tier-replay-instance", 4712, "new-1");

    assert!(
        wait_until(
            || {
                TIER_SINK_FRAMES
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(pid, _)| pid == "cell_effect_record")
                    .count()
                    >= 1
            },
            Duration::from_secs(5)
        ),
        "the above-floor input must produce an effect record"
    );

    // No late effect record appears for the skipped offsets. Counted by
    // payload id: the output is process-global, and a sibling test's park frame
    // landing in this window says nothing about these offsets.
    let effect_records = || {
        TIER_SINK_FRAMES
            .lock()
            .unwrap()
            .iter()
            .filter(|(pid, _)| pid == "cell_effect_record")
            .count()
    };
    let seen = effect_records();
    assert!(stays_quiet(
        || effect_records() > seen,
        Duration::from_millis(50)
    ));
    let effect_count = TIER_SINK_FRAMES
        .lock()
        .unwrap()
        .iter()
        .filter(|(pid, _)| pid == "cell_effect_record")
        .count();
    assert_eq!(effect_count, 1, "skipped offsets must not re-apply");
}

// ----- Logical clock: ctx.now() == durable change seq, restore re-arm -----

static LOGICAL_OBSERVED: Mutex<Vec<u64>> = Mutex::new(Vec::new());

#[derive(Default)]
struct LogicalNowTasklet;
#[flezha_runner::tasklet(name = "logical_now_tasklet", logical_clock)]
impl Tasklet for LogicalNowTasklet {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = TickChange;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        LOGICAL_OBSERVED.lock().unwrap().push(ctx.now());
        ctx.change(TickChange);
        // Emit so flush-before-emit ships the change deterministically
        // (the background flusher's linger would otherwise race the test).
        ctx.emit(input)?;
        Ok(())
    }
}

test_tasklet!(LogicalNowSinkTasklet, "logical_now_sink_tasklet", String => (), (), |_input, _ctx| {
    Ok(())
});

struct LogicalNowRelay;
impl Relay for LogicalNowRelay {
    fn factory_id() -> &'static str {
        "logical_now_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<LogicalNowTasklet>()
            .named("hop")
            .into::<LogicalNowSinkTasklet>();
    }
}

/// Spec "the logical clock serves the durable change sequence": ctx.now() is
/// the change seq at dispatch entry — advancing with applied changes —
/// and a restored instance continues from the re-armed seq rather than a
/// reset counter.
#[test]
fn logical_clock_serves_change_seq_and_survives_restore_rearm() {
    pinned_pool();
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    LOGICAL_OBSERVED.lock().unwrap().clear();
    let (handle, instance) = setup::<LogicalNowRelay>("logical-now-instance");
    let stream = dummy_stream();

    for payload in [b"a".to_vec(), b"b".to_vec()] {
        dispatch_input(&handle, "logical-now-instance", "input", payload, &stream);
    }
    assert!(wait_until(
        || LOGICAL_OBSERVED.lock().unwrap().len() == 2,
        Duration::from_secs(5)
    ));
    assert_eq!(*LOGICAL_OBSERVED.lock().unwrap(), vec![0, 1]);
    assert_eq!(instance.cells[0][0].change_seq.load(Ordering::Acquire), 2);

    // Crash-restore: fresh instance, restore both changes (seqs 0, 1) and
    // re-arm; the next invocation's logical time continues at 2.
    LOGICAL_OBSERVED.lock().unwrap().clear();
    let (handle_b, instance_b) = setup::<LogicalNowRelay>("logical-now-instance");
    let slot = Arc::clone(&instance_b.cells[0][0]);
    let change_bytes = postcard::to_allocvec(&TickChange).expect("encode change");
    let mut change_log = wal_record(0, &change_bytes, header_template_for::<LogicalNowTasklet>());
    change_log.extend_from_slice(&wal_record(
        1,
        &change_bytes,
        header_template_for::<LogicalNowTasklet>(),
    ));
    let payload = flezha_runner::cell::restore::RestoreInitPayload {
        snapshot_header: &[],
        snapshot_bytes: &[],
        change_log_bytes: &change_log,
    };
    let terminal = slot.dispatcher.restore(&slot, &payload).expect("restore");
    assert_eq!(terminal.terminal_seq, 1);
    slot.change_seq.store(
        terminal.terminal_seq + 1,
        std::sync::atomic::Ordering::Release,
    );

    dispatch_input(
        &handle_b,
        "logical-now-instance",
        "input",
        b"c".to_vec(),
        &stream,
    );
    assert!(wait_until(
        || LOGICAL_OBSERVED.lock().unwrap().len() == 1,
        Duration::from_secs(5)
    ));
    assert_eq!(
        *LOGICAL_OBSERVED.lock().unwrap(),
        vec![2],
        "logical time must continue from the re-armed seq, not reset"
    );
}
