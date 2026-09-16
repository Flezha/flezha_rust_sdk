//! Output identity (contract): the `flz.*` stamps a keyed channel puts on
//! an emit, the byte layout a non-keyed frame keeps, and the identical
//! stamps a crash-restore replay produces.

use flezha_runner::entity::header::header_template_for;
use flz_transport::service::payloader;

mod common;
use common::prelude::*;

// ----- Output identity: keyed-channel stamps, byte-identity, replay -----

/// Spec "Identity stamps on keyed-channel output frames": every keyed
/// plugin emit carries `flz.entity` / `flz.seq` / `flz.key` alongside the
/// `channel` stamp; `flz.seq` reflects the durable change seq after the
/// flush-before-emit drain plus the emit ordinal.
#[test]
fn keyed_channel_emits_carry_identity_stamps() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    let (handle, _instance) = setup::<KeyedStampOperative>("keyed-stamps-instance");
    let (stream, mut observer) = stream_pair();

    dispatch_input(
        &handle,
        "keyed-stamps-instance",
        "input",
        b"x".to_vec(),
        &stream,
    );
    dispatch_input(
        &handle,
        "keyed-stamps-instance",
        "input",
        b"y".to_vec(),
        &stream,
    );

    let frames = read_frames(&mut observer, 2, Duration::from_secs(5));
    let f1 = &frames[0];
    assert_eq!(f1.message, b"x".to_vec());
    assert_eq!(f1.metadata.get("channel").unwrap(), "out");
    assert_eq!(
        f1.metadata.get("flz.entity").unwrap(),
        "keyed-stamps-instance/keyed_stamp_procedure/"
    );
    assert_eq!(f1.metadata.get("flz.seq").unwrap(), "1.0");
    assert_eq!(f1.metadata.get("flz.key").unwrap(), "x");

    let f2 = &frames[1];
    assert_eq!(f2.metadata.get("flz.seq").unwrap(), "2.0");
    assert_eq!(f2.metadata.get("flz.key").unwrap(), "y");
}

/// Spec "Non-keyed channels are unchanged and cost-free": a non-keyed
/// plugin emit produces the exact pre-identity wire frame — byte-identical
/// to `payloader::build_from_binary` with only the `channel` stamp.
#[test]
fn non_keyed_frame_is_byte_identical_to_prechange_layout() {
    // MetaProducerProcedure-free operative: reuse FlushOrderOperative's shape
    // via a minimal pure-plugin operative.
    struct PlainPluginOperative;
    impl StreamOperative for PlainPluginOperative {
        fn factory_id() -> &'static str {
            "plain_plugin_operative"
        }
        fn channels(c: &ChannelBuilder) {
            c.input::<String>("input")
                .into::<PlainPluginProcedure>()
                .sink_named("out");
        }
    }

    let (handle, _instance) = setup::<PlainPluginOperative>("plain-plugin-instance");
    let (stream, mut observer) = stream_pair();

    dispatch_input(
        &handle,
        "plain-plugin-instance",
        "input",
        b"payload".to_vec(),
        &stream,
    );

    let frames = read_frames(&mut observer, 1, Duration::from_secs(5));
    let mut expected_meta = HashMap::new();
    expected_meta.insert("channel".to_string(), "out".to_string());
    let expected =
        payloader::build_from_binary("plain-plugin-instance", b"payload", expected_meta).unwrap();
    assert_eq!(
        frames[0].raw, expected,
        "non-keyed plugin frame must be byte-identical to the pre-change layout"
    );
    assert!(
        !frames[0].metadata.keys().any(|k| k.starts_with("flz.")),
        "non-keyed frames carry no identity stamps"
    );
}

test_procedure!(PlainPluginProcedure, "plain_plugin_procedure", String => String, (), |input, _state, ctx| {
    ctx.emit::<String>(input)?;
    Ok(())
});

/// Spec "Stamps present and replay-stable" (crash-restore e2e): after a
/// simulated crash, a fresh instance restored via the runtime's
/// `restore_state` + change_seq re-arm replays the same input and emits a
/// frame whose `flz.*` stamps (and payload) are identical to the original.
#[test]
fn keyed_stamps_replay_identically_after_crash_restore() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    // Original run: two inputs, two changes, two stamped frames.
    let (handle_a, _instance_a) = setup::<KeyedStampOperative>("restore-stamps-instance");
    let (stream_a, mut observer_a) = stream_pair();
    dispatch_input(
        &handle_a,
        "restore-stamps-instance",
        "input",
        b"x".to_vec(),
        &stream_a,
    );
    dispatch_input(
        &handle_a,
        "restore-stamps-instance",
        "input",
        b"y".to_vec(),
        &stream_a,
    );
    let original = read_frames(&mut observer_a, 2, Duration::from_secs(5));

    // Crash after "x"'s change (seq 0) became durable; "y" is replayed.
    // Fresh instance, same identity: restore state from the WAL and
    // re-arm change_seq exactly like `RestoreInitState` does.
    let (handle_b, instance_b) = setup::<KeyedStampOperative>("restore-stamps-instance");
    let slot = Arc::clone(&instance_b.entities[0][0]);
    let change_bytes = postcard::to_allocvec(&TickChange).expect("encode change");
    let change_log = wal_record(
        0,
        &change_bytes,
        header_template_for::<KeyedStampProcedure>(),
    );
    let terminal = {
        let mut guard = slot.state.lock().unwrap();
        slot.dispatcher
            .restore_state(
                &mut guard,
                &mut flezha_runner::entity::timer::PendingTimers::default(),
                &[],
                &[],
                &change_log,
                None,
            )
            .expect("restore_state")
    };
    assert_eq!(terminal.terminal_seq, 0);
    slot.change_seq.store(
        terminal.terminal_seq + 1,
        std::sync::atomic::Ordering::Release,
    );

    let (stream_b, mut observer_b) = stream_pair();
    dispatch_input(
        &handle_b,
        "restore-stamps-instance",
        "input",
        b"y".to_vec(),
        &stream_b,
    );
    let replayed = read_frames(&mut observer_b, 1, Duration::from_secs(5));

    let orig = &original[1];
    let replay = &replayed[0];
    assert_eq!(replay.message, orig.message);
    for stamp in ["flz.entity", "flz.seq", "flz.key"] {
        assert_eq!(
            replay.metadata.get(stamp),
            orig.metadata.get(stamp),
            "{stamp} must be replay-stable across crash-restore"
        );
    }
    assert_eq!(replay.metadata.get("flz.seq").unwrap(), "2.0");
}
