//! Flush-before-emit ordering and failure containment: a durable change
//! reaches the sink before the output it justifies, and a panicking
//! procedure takes nothing else down.

mod common;
use common::prelude::*;

// ----- Flush-before-emit ordering (durability fix) -----
//
// A persistence-enabled Procedure applies a Change then emits. The change's
// WAL frame must reach the durable sink BEFORE the output frame is
// written, so a hard kill can never lose a change whose output already
// shipped. Both signals are recorded on this single ordered log; the WAL
// `send` and the plugin serializer run on the same dispatch thread inside
// one `run`, so the recorded order is deterministic.

static FLUSH_ORDER_LOG: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

#[derive(serde::Serialize, serde::Deserialize)]
struct AddChange(i64);
impl flezha_runner::state::Change for AddChange {}

#[derive(Default)]
struct FlushOrderProcedure;
impl Procedure for FlushOrderProcedure {
    type Input = String;
    type Output = String;
    type State = i64;
    type Change = AddChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "flush_order_procedure"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> i64 {
        0
    }
    fn apply_change(state: &mut i64, change: &AddChange) {
        *state += change.0;
    }
    fn run(
        &mut self,
        _: String,
        mut state: StateHandle<'_, FlushOrderProcedure>,
        ctx: &mut ProcedureCtx<'_>,
    ) -> Result<(), ProcedureError> {
        state.apply(AddChange(1));
        let total = *state;
        ctx.emit::<String>(format!("{total}"))?;
        Ok(())
    }
}

test_procedure!(FlushOrderSinkProcedure, "flush_order_sink_procedure", String => (), (), |_input, _state, _ctx| {
    Ok(())
});

struct FlushOrderOperative;
impl StreamOperative for FlushOrderOperative {
    fn factory_id() -> &'static str {
        "flush_order_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input")
            .into::<FlushOrderProcedure>()
            .sink_named_with("out", |_v: &String| {
                FLUSH_ORDER_LOG.lock().unwrap().push("emit");
                Vec::new()
            })
            .into::<FlushOrderSinkProcedure>();
    }
}

#[test]
fn change_is_flushed_before_output_is_emitted() {
    use flezha_runner::entity::flusher::{SendError, SendFrameFn};
    use flezha_runner::entity::state_sink;

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

    let (handle, instance) = setup::<FlushOrderOperative>("flush-order-instance");
    assert!(
        instance.persistence_enabled(),
        "operative must be persistent"
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

test_procedure!(PanicProcedure, "panic_procedure", String => (), (), |input, _state, _ctx| {
    if input == "boom" {
        panic!("induced procedure panic");
    }
    PANIC_AFTER.fetch_add(1, Ordering::Relaxed);
    Ok(())
});

struct PanicOperative;
impl StreamOperative for PanicOperative {
    fn factory_id() -> &'static str {
        "panic_operative"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<PanicProcedure>();
    }
}

#[test]
fn procedure_panic_does_not_wedge_the_entity() {
    PANIC_AFTER.store(0, Ordering::Release);
    // The exhausted retry budget parks the poison payload. With no global
    // sink the park drops immediately (transport-less fallback), but a
    // concurrently-running sink test may have installed a non-acking sink
    // — then the park (correctly) holds the entity until acked. Ack it
    // out-of-band: park ids are `<entity_label>#<process nonce>`, and
    // this binary parks only here, so a small nonce sweep always hits.
    let _acker = std::thread::spawn(|| {
        for _ in 0..200 {
            for nonce in 0..32 {
                flezha_runner::entity::park::complete_ack(&format!(
                    "panic-instance/panic_procedure/#{nonce}"
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
    let (handle, instance) = setup::<PanicOperative>("panic-instance");
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
        "entity wedged after a procedure panic: 'after' was never processed"
    );

    assert!(
        instance.entities[0][0].state.lock().is_ok(),
        "entity state mutex was poisoned by the procedure panic"
    );
}

// ----- DropNothing producer on a one-worker pool must not self-deadlock -----
//
// The single pool worker, mid-producer-dispatch, emits into a full DropNothing
// consumer mailbox whose drain workflow sits queued behind the producer's own
// workflow. Parking the worker there is a permanent self-deadlock; the
// DropNothing arm must drain the full slot inline instead.

static BLOCKED_CONSUMED: AtomicUsize = AtomicUsize::new(0);

test_procedure!(BlockProducerProcedure, "block_producer_procedure", String => String, (), |_input, _state, ctx| {
    for i in 0..8 {
        ctx.emit(format!("v{}", i))?;
    }
    Ok(())
});

test_procedure!(BlockedConsumerProcedure, "blocked_consumer_procedure", String => String, (), cap 1, |_input, _state, _ctx| {
    BLOCKED_CONSUMED.fetch_add(1, Ordering::AcqRel);
    Ok(())
});

struct BlockDeadlockOperative;
impl StreamOperative for BlockDeadlockOperative {
    fn factory_id() -> &'static str {
        "block_deadlock_operative"
    }
    fn channels(c: &ChannelBuilder) {
        // Inter-procedure channel: OverflowPolicy defaults to Block.
        c.input::<String>("input")
            .into::<BlockProducerProcedure>()
            .named("blocked_out")
            .into::<BlockedConsumerProcedure>();
    }
}

#[test]
fn block_policy_emit_on_single_worker_pool_does_not_deadlock() {
    BLOCKED_CONSUMED.store(0, Ordering::Release);
    let (handle, _instance) = setup::<BlockDeadlockOperative>("block-deadlock-instance");
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
