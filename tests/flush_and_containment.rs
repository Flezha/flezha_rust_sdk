//! Flush-before-emit ordering and failure containment: a durable change
//! reaches the output before the output it justifies, which leaves at emit, a
//! sleeping class's
//! stateless Cells still move its processed prefix, and a panicking tasklet
//! takes nothing else down.

mod common;
use common::prelude::*;

// ----- Flush-before-emit ordering (durability fix) -----
//
// A persistence-enabled Tasklet applies a Change then emits. The change's
// WAL frame must reach the durable output BEFORE the output frame is
// written, so a hard kill can never lose a change whose output already
// shipped; and a typed output leaves at emit, before `run` returns and the
// item settles. Every signal is recorded on this single ordered log; the WAL
// `send`, the plugin serializer and the frame write run on the same dispatch
// thread inside one `run`, so the recorded order is deterministic.

static FLUSH_ORDER_LOG: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

#[derive(serde::Serialize, serde::Deserialize)]
struct AddChange(i64);
impl flezha_runner::state::Change<i64> for AddChange {
    fn apply_to(&self, state: &mut i64) {
        *state += self.0;
    }
}

#[derive(Default)]
struct FlushOrderTasklet;
#[flezha_runner::tasklet(name = "flush_order_tasklet")]
impl Tasklet for FlushOrderTasklet {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = AddChange;
    fn run(&mut self, _: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(AddChange(1));
        let total = *ctx.state();
        ctx.emit(format!("{total}"))?;
        FLUSH_ORDER_LOG.lock().unwrap().push("emit returned");
        Ok(())
    }
}

test_tasklet!(FlushOrderSinkTasklet, "flush_order_sink_tasklet", String => (), (), |_input, _ctx| {
    Ok(())
});

struct FlushOrderRelay;
impl Relay for FlushOrderRelay {
    fn factory_id() -> &'static str {
        "flush_order_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<FlushOrderTasklet>()
            .output_named_with("out", |_v: &String| {
                FLUSH_ORDER_LOG.lock().unwrap().push("serialize");
                Vec::new()
            })
            .into::<FlushOrderSinkTasklet>();
    }
}

#[test]
fn a_change_is_flushed_before_its_output_which_leaves_at_emit() {
    use flezha_runner::cell::flusher::{SendError, SendFrameFn};
    use flezha_runner::cell::state_sink;

    let _serial = WAL_TEST_LOCK.lock().unwrap();
    FLUSH_ORDER_LOG.lock().unwrap().clear();
    // WAL output records every shipped change frame on the shared log.
    let sink: SendFrameFn = Arc::new(
        |_payload_id: &str, _frame: &[u8]| -> Result<(), SendError> {
            FLUSH_ORDER_LOG.lock().unwrap().push("wal");
            Ok(())
        },
    );
    state_sink::install_global_sink(sink);

    let (handle, instance) = setup::<FlushOrderRelay>("flush-order-instance");
    assert!(instance.keeps_state(), "relay must be persistent");
    // An output write that fails would be held in the process-global
    // reconnect ring, which withholds every processed-prefix marker.
    let wire: flz_transport::transport::LocalFrameSink = Arc::new(|_frame: &[u8]| {
        FLUSH_ORDER_LOG.lock().unwrap().push("wire");
        Ok(())
    });
    let stream = Arc::new(flz_transport::transport::FlezhaStream::Local(wire));

    let mut md = HashMap::new();
    md.insert("channel".to_string(), "input".to_string());
    handle.dispatch(
        "flush-order-instance",
        b"go".to_vec(),
        &md,
        Arc::clone(&stream),
    );

    assert!(
        wait_until(
            || FLUSH_ORDER_LOG.lock().unwrap().len() == 4,
            Duration::from_secs(5)
        ),
        "expected four signals, got {:?}",
        FLUSH_ORDER_LOG.lock().unwrap().clone()
    );
    assert_eq!(
        FLUSH_ORDER_LOG.lock().unwrap().clone(),
        vec!["wal", "serialize", "wire", "emit returned"],
        "change must reach the WAL path before the output frame, written at emit"
    );
}

// ----- A sleeping class whose Cells keep no record -----
//
// Its input is journaled, so the controller moves the log's floor only on the
// processed prefix the runner reports. No Cell of a `NoChange` tasklet joins
// the flusher, so the flusher reports the instance's prefix on its own.

test_tasklet!(StatelessSleeperTasklet, "stateless_sleeper_tasklet", String => (), (), |_input, _ctx| {
    Ok(())
});

struct StatelessSleeperRelay;
impl Relay for StatelessSleeperRelay {
    fn factory_id() -> &'static str {
        "stateless_sleeper_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<StatelessSleeperTasklet>();
    }
    fn settings() -> flezha_runner::RelaySettings {
        flezha_runner::RelaySettings {
            durable: flezha_runner::Durable::Local {
                sleep_after: flezha_runner::SleepAfter::After(Duration::from_secs(60)),
            },
            ..flezha_runner::RelaySettings::default()
        }
    }
}

/// The `(cell identity, prefix)` of a captured processed-prefix marker.
fn prefix_marker(frame: &[u8]) -> (Vec<u8>, u64) {
    let len = u16::from_be_bytes([frame[0], frame[1]]) as usize;
    let seq = u64::from_be_bytes(frame[2 + len..2 + len + 8].try_into().unwrap());
    (frame[2..2 + len].to_vec(), seq)
}

#[test]
fn a_sleeping_class_with_stateless_cells_reports_its_processed_prefix() {
    let _serial = WAL_TEST_LOCK.lock().unwrap();
    install_capturing_tier_sink();
    let instance_id = "stateless-sleeper-instance";
    let (handle, _instance) = setup::<StatelessSleeperRelay>(instance_id);

    let identity = flezha_runner::cell::flusher::encode_cell_identity(instance_id, "", "");
    dispatch_at(&handle, instance_id, 7, "one");

    let reported = || {
        TIER_SINK_FRAMES
            .lock()
            .unwrap()
            .iter()
            .filter(|(pid, _)| pid == "processed_prefix")
            .map(|(_, frame)| prefix_marker(frame))
            .any(|marker| marker == (identity.clone(), 7))
    };
    assert!(
        wait_until(reported, Duration::from_secs(5)),
        "offset 7 settled but no processed prefix covering it was reported"
    );
}

static PANIC_AFTER: AtomicUsize = AtomicUsize::new(0);

test_tasklet!(PanicTasklet, "panic_tasklet", String => (), (), |input, _ctx| {
    if input == "boom" {
        panic!("induced tasklet panic");
    }
    PANIC_AFTER.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct PanicRelay;
impl Relay for PanicRelay {
    fn factory_id() -> &'static str {
        "panic_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<PanicTasklet>();
    }
}

#[test]
fn tasklet_panic_does_not_wedge_the_cell() {
    PANIC_AFTER.store(0, Ordering::Release);
    // The exhausted retry budget parks the poison payload. With no global
    // output the park drops immediately (transport-less fallback), but a
    // concurrently-running output test may have installed a non-acking output
    // — then the park (correctly) holds the cell until acked. Ack it
    // out-of-band: park ids are `<cell_label>#<process nonce>`, and
    // this binary parks only here, so a small nonce sweep always hits.
    let _acker = std::thread::spawn(|| {
        for _ in 0..200 {
            for nonce in 0..32 {
                flezha_runner::cell::park::complete_ack(&format!(
                    "panic-instance/panic_tasklet/#{nonce}"
                ));
            }
            if wait_until(
                || PANIC_AFTER.load(Ordering::Acquire) == 1,
                Duration::from_millis(25),
            ) {
                break;
            }
        }
    });
    let (handle, instance) = setup::<PanicRelay>("panic-instance");
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "panic-instance",
        "input",
        b"boom".to_vec(),
        &stream,
    );
    dispatch_input(
        &handle,
        "panic-instance",
        "input",
        b"after".to_vec(),
        &stream,
    );

    assert!(
        wait_until(
            || PANIC_AFTER.load(Ordering::Acquire) == 1,
            Duration::from_secs(5)
        ),
        "cell wedged after a tasklet panic: 'after' was never processed"
    );

    assert!(
        instance.cells[0][0].state.lock().is_ok(),
        "cell state mutex was poisoned by the tasklet panic"
    );
}

// ----- DropNothing producer on a one-worker pool must not self-deadlock -----
//
// The single pool worker, mid-producer-dispatch, emits into a full DropNothing
// consumer mailbox whose drain steps relay sits queued behind the producer's own
// steps relay. Parking the worker there is a permanent self-deadlock; the
// DropNothing arm must drain the full slot inline instead.

static BLOCKED_CONSUMED: AtomicUsize = AtomicUsize::new(0);

test_tasklet!(BlockProducerTasklet, "block_producer_tasklet", String => String, (), |_input, ctx| {
    for i in 0..8 {
        ctx.emit(format!("v{}", i))?;
    }
    Ok(())
});

test_tasklet!(BlockedConsumerTasklet, "blocked_consumer_tasklet", String => String, (), cap 1, |_input, _ctx| {
    BLOCKED_CONSUMED.fetch_add(1, Ordering::AcqRel);
    Ok(())
});

struct BlockDeadlockRelay;
impl Relay for BlockDeadlockRelay {
    fn factory_id() -> &'static str {
        "block_deadlock_relay"
    }
    fn channels(c: &ChannelBuilder) {
        // Inter-tasklet channel: OverflowPolicy defaults to Block.
        c.input::<String>("input")
            .into::<BlockProducerTasklet>()
            .named("blocked_out")
            .into::<BlockedConsumerTasklet>();
    }
}

#[test]
fn block_policy_emit_on_single_worker_pool_does_not_deadlock() {
    BLOCKED_CONSUMED.store(0, Ordering::Release);
    let (handle, _instance) = setup::<BlockDeadlockRelay>("block-deadlock-instance");
    let stream = dummy_stream();

    dispatch_input(
        &handle,
        "block-deadlock-instance",
        "input",
        b"go".to_vec(),
        &stream,
    );

    assert!(
        wait_until(
            || BLOCKED_CONSUMED.load(Ordering::Acquire) == 8,
            Duration::from_secs(10)
        ),
        "DropNothing emit deadlocked the one-worker pool: {} of 8 consumed",
        BLOCKED_CONSUMED.load(Ordering::Acquire)
    );
}
