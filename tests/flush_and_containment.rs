//! Flush-before-emit ordering and failure containment: a durable change
//! reaches the sink before the output it justifies, and a panicking
//! tasklet takes nothing else down.

mod common;
use common::prelude::*;

// ----- Flush-before-emit ordering (durability fix) -----
//
// A persistence-enabled Tasklet applies a Change then emits. The change's
// WAL frame must reach the durable sink BEFORE the output frame is
// written, so a hard kill can never lose a change whose output already
// shipped. Both signals are recorded on this single ordered log; the WAL
// `send` and the plugin serializer run on the same dispatch thread inside
// one `run`, so the recorded order is deterministic.

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
        Ok(())
    }
}

test_tasklet!(FlushOrderSinkTasklet, "flush_order_sink_tasklet", String => (), (), |_input, _ctx| {
    Ok(())
});

struct FlushOrderOperator;
impl Operator for FlushOrderOperator {
    fn factory_id() -> &'static str {
        "flush_order_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<FlushOrderTasklet>()
            .sink_named_with("out", |_v: &String| {
                FLUSH_ORDER_LOG.lock().unwrap().push("emit");
                Vec::new()
            })
            .into::<FlushOrderSinkTasklet>();
    }
}

#[test]
fn change_is_flushed_before_output_is_emitted() {
    use flezha_runner::cell::flusher::{SendError, SendFrameFn};
    use flezha_runner::cell::state_sink;

    let _serial = WAL_TEST_LOCK.lock().unwrap();
    FLUSH_ORDER_LOG.lock().unwrap().clear();
    // WAL sink records every shipped change frame on the shared log.
    let sink: SendFrameFn = Arc::new(
        |_payload_id: &str, _frame: &[u8]| -> Result<(), SendError> {
            FLUSH_ORDER_LOG.lock().unwrap().push("wal");
            Ok(())
        },
    );
    state_sink::install_global_sink(sink);

    let (handle, instance) = setup::<FlushOrderOperator>("flush-order-instance");
    assert!(
        instance.keeps_state(),
        "operator must be persistent"
    );
    let stream = dummy_stream();

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
            || FLUSH_ORDER_LOG.lock().unwrap().len() == 2,
            Duration::from_secs(5)
        ),
        "expected both wal+emit signals, got {:?}",
        FLUSH_ORDER_LOG.lock().unwrap().clone()
    );
    assert_eq!(
        FLUSH_ORDER_LOG.lock().unwrap().clone(),
        vec!["wal", "emit"],
        "change must reach the WAL path before the output frame is written"
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

struct PanicOperator;
impl Operator for PanicOperator {
    fn factory_id() -> &'static str {
        "panic_operator"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<PanicTasklet>();
    }
}

#[test]
fn tasklet_panic_does_not_wedge_the_cell() {
    PANIC_AFTER.store(0, Ordering::Release);
    // The exhausted retry budget parks the poison payload. With no global
    // sink the park drops immediately (transport-less fallback), but a
    // concurrently-running sink test may have installed a non-acking sink
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
    let (handle, instance) = setup::<PanicOperator>("panic-instance");
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
// consumer mailbox whose drain steps operator sits queued behind the producer's own
// steps operator. Parking the worker there is a permanent self-deadlock; the
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

struct BlockDeadlockOperator;
impl Operator for BlockDeadlockOperator {
    fn factory_id() -> &'static str {
        "block_deadlock_operator"
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
    let (handle, _instance) = setup::<BlockDeadlockOperator>("block-deadlock-instance");
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
