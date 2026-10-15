//! One Cell held up inside an invocation must not stop the background flusher
//! from shipping another Cell's records: the flusher drains every registered
//! Cell's buffer on one thread, so it may not wait on a buffer an invocation
//! holds, whether the invocation is in author code or in a tier-off park
//! waiting on its ack.

mod common;
use common::prelude::*;

use flezha_runner::cell::park;
use flezha_runner::cell::state_sink::PARK_PAYLOAD_PAYLOAD_ID;
use std::sync::PoisonError;

/// The longest either Cell is held: a cap that lets a red run end.
const HOLD_CAP: Duration = Duration::from_secs(3);

static RELEASE_A: Mutex<bool> = Mutex::new(false);
static RELEASE_A_CV: std::sync::Condvar = std::sync::Condvar::new();
static A_ENTERED: AtomicBool = AtomicBool::new(false);
static A_LEFT: AtomicBool = AtomicBool::new(false);

#[derive(Default)]
struct BlockingTasklet;
#[flezha_runner::tasklet(name = "kb544_blocking_tasklet")]
impl Tasklet for BlockingTasklet {
    type Input = String;
    type Output = ();
    type State = i64;
    type Change = TickChange;
    fn run(&mut self, _: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(TickChange);
        A_ENTERED.store(true, Ordering::SeqCst);
        let released = RELEASE_A.lock().unwrap();
        let _ = RELEASE_A_CV
            .wait_timeout_while(released, HOLD_CAP, |r| !*r)
            .unwrap();
        A_LEFT.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct CountingTasklet;
#[flezha_runner::tasklet(name = "kb544_counting_tasklet")]
impl Tasklet for CountingTasklet {
    type Input = String;
    type Output = ();
    type State = i64;
    type Change = TickChange;
    fn run(&mut self, _: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(TickChange);
        Ok(())
    }
}

#[derive(Default)]
struct ParkingTasklet;
#[flezha_runner::tasklet(name = "kb544_parking_tasklet")]
impl Tasklet for ParkingTasklet {
    type Input = String;
    type Output = ();
    type State = i64;
    type Change = TickChange;
    fn run(&mut self, _: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        ctx.change(TickChange);
        Err(TaskletError::non_retryable("kb544 poison"))
    }
}

struct BlockingRelay;
impl Relay for BlockingRelay {
    fn factory_id() -> &'static str {
        "kb544_blocking_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<BlockingTasklet>();
    }
}

struct CountingRelay;
impl Relay for CountingRelay {
    fn factory_id() -> &'static str {
        "kb544_counting_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<CountingTasklet>();
    }
}

struct ParkingRelay;
impl Relay for ParkingRelay {
    fn factory_id() -> &'static str {
        "kb544_parking_relay"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("input").into::<ParkingTasklet>();
    }
}

/// How many of the shipped frames are B's records.
fn b_records(frames: &ShippedFrames) -> usize {
    let needle = b"kb544_counting_tasklet";
    frames
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, frame)| frame.windows(needle.len()).any(|w| w == needle))
        .count()
}

/// Feed B one input once the Cell under test holds its invocation, and answer
/// whether B's record shipped while `still_held` says that Cell is held.
fn b_ships_while_held(
    instance: &str,
    frames: &ShippedFrames,
    still_held: impl Fn() -> bool,
) -> bool {
    let (b, _b_instance) = setup::<CountingRelay>(instance);
    // Let a drain pass reach the held Cell's entry before B is fed.
    std::thread::sleep(Duration::from_millis(50));
    let before = b_records(frames);
    dispatch_input(&b, instance, "input", b"during".to_vec(), &dummy_stream());
    wait_until(
        || b_records(frames) > before,
        HOLD_CAP + Duration::from_secs(1),
    ) && still_held()
}

#[test]
fn a_cell_blocked_in_run_does_not_stall_another_cells_records() {
    let _serial = WAL_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let frames = recording_sink();

    let (a, a_instance) = setup::<BlockingRelay>("kb544-a");
    assert!(a_instance.keeps_state());
    dispatch_input(&a, "kb544-a", "input", b"block".to_vec(), &dummy_stream());
    assert!(wait_until(
        || A_ENTERED.load(Ordering::SeqCst),
        Duration::from_secs(2)
    ));

    let shipped = b_ships_while_held("kb544-b", &frames, || !A_LEFT.load(Ordering::SeqCst));
    *RELEASE_A.lock().unwrap() = true;
    RELEASE_A_CV.notify_all();
    assert!(shipped, "B's record must ship while A is still in run");
}

#[test]
fn a_cell_waiting_on_its_park_ack_does_not_stall_another_cells_records() {
    let _serial = WAL_TEST_LOCK.lock().unwrap_or_else(PoisonError::into_inner);
    let frames = recording_sink();
    let park_id = || {
        frames.lock().unwrap().iter().find_map(|(id, frame)| {
            (id == PARK_PAYLOAD_PAYLOAD_ID).then(|| decode_park_body(frame).unwrap().park_id)
        })
    };

    let (p, p_instance) = setup::<ParkingRelay>("kb544-p");
    assert!(p_instance.keeps_state());
    dispatch_input(&p, "kb544-p", "input", b"poison".to_vec(), &dummy_stream());
    assert!(wait_until(|| park_id().is_some(), Duration::from_secs(2)));

    // Nothing acks the park until the check is done, so it is held throughout.
    let shipped = b_ships_while_held("kb544-b2", &frames, || true);
    park::complete_ack(&park_id().unwrap());
    assert!(
        shipped,
        "B's record must ship while the park waits on its ack"
    );
}
