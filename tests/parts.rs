//! `ctx.part(value)`: a preview of an invocation's output that leaves the
//! runner while `run` still runs, numbered within its attempt, and refused
//! with `part.no_sink` where the output channel ends in no sink.

use std::sync::Condvar;

use flezha_runner::ErrorModel;

mod common;
use common::prelude::*;
use common::{read_frames, setup, stream_pair, WireFrame};

const WAIT: Duration = Duration::from_secs(5);

fn is_part(frame: &WireFrame) -> bool {
    frame.metadata.contains_key("flz.part")
}

/// `(payload, flz.part, flz.part_attempt)` of a part frame.
fn part_of(frame: &WireFrame) -> (String, String, String) {
    (
        String::from_utf8(frame.message.clone()).unwrap(),
        frame.metadata["flz.part"].clone(),
        frame.metadata["flz.part_attempt"].clone(),
    )
}

fn dispatch(
    handle: &OperatorDispatchHandle,
    id: &str,
    input: &str,
) -> std::os::unix::net::UnixStream {
    let (stream, observer) = stream_pair();
    handle.dispatch(id, input.as_bytes().to_vec(), &HashMap::new(), stream);
    observer
}

test_tasklet!(ThreeParts, "parts_three", String => String, (), |input, ctx| {
    for text in ["he", "hel", "hell"] {
        ctx.part(text.to_string())?;
    }
    ctx.emit(input)
});

struct ThreePartsOp;
impl Operator for ThreePartsOp {
    fn factory_id() -> &'static str {
        "parts_three"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<ThreeParts>()
            .sink_named("answer");
    }
}

#[test]
fn parts_are_numbered_within_the_attempt_and_leave_before_the_output() {
    let (handle, _instance) = setup::<ThreePartsOp>("parts-three");
    let mut observer = dispatch(&handle, "parts-three", "hello");
    let frames = read_frames(&mut observer, 4, WAIT);

    let parts: Vec<_> = frames[..3].iter().map(part_of).collect();
    let expected: Vec<_> = [("he", "0"), ("hel", "1"), ("hell", "2")]
        .iter()
        .map(|(text, n)| (text.to_string(), n.to_string(), "1".to_string()))
        .collect();
    assert_eq!(parts, expected);
    for part in &frames[..3] {
        assert_eq!(part.metadata["channel"], "answer");
        assert_eq!(part.metadata["flz.operator"], "parts-three");
        assert!(part.metadata.contains_key("flz.cell"));
    }
    assert!(!is_part(&frames[3]), "the output follows its parts");
    assert_eq!(frames[3].message, b"hello");
}

static FLAKY_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);

test_tasklet!(FlakyParts, "parts_flaky", String => String, (), |input, ctx| {
    ctx.part("a".to_string())?;
    ctx.part("b".to_string())?;
    if FLAKY_ATTEMPTS.fetch_add(1, Ordering::SeqCst) == 0 {
        return Err(TaskletError::Custom("first attempt fails".into()));
    }
    ctx.emit(input)
});

struct FlakyPartsOp;
impl Operator for FlakyPartsOp {
    fn factory_id() -> &'static str {
        "parts_flaky"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .retry_backoff_ms(1)
            .into::<FlakyParts>()
            .sink_named("answer");
    }
}

#[test]
fn a_retry_numbers_its_parts_from_zero_under_the_next_attempt() {
    let (handle, _instance) = setup::<FlakyPartsOp>("parts-flaky");
    let mut observer = dispatch(&handle, "parts-flaky", "done");
    let frames = read_frames(&mut observer, 5, WAIT);

    let parts: Vec<_> = frames[..4]
        .iter()
        .map(|f| {
            let (text, n, attempt) = part_of(f);
            format!("{text}:{n}:{attempt}")
        })
        .collect();
    assert_eq!(parts, ["a:0:1", "b:1:1", "a:0:2", "b:1:2"]);
    assert_eq!(frames[4].message, b"done");
}

/// The codes the no-sink tasklets' refused parts answered, in order.
static REFUSALS: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn refused_part<S: Tasklet<Output = String>>(ctx: &mut Ctx<'_, S>) {
    let refusal = ctx
        .part("preview".to_string())
        .expect_err("a part with no sink to preview is refused");
    REFUSALS.lock().unwrap().push(refusal.code().to_string());
}

test_tasklet!(Feeder, "parts_feeder", String => String, (), |input, ctx| {
    refused_part(ctx);
    ctx.emit(input)
});
test_tasklet!(Relay, "parts_relay", String => String, (), |input, ctx| {
    ctx.emit(input)
});
test_tasklet!(Answerer, "parts_answerer", String => String, (), |input, ctx| {
    refused_part(ctx);
    ctx.emit(input)
});

struct FeederOp;
impl Operator for FeederOp {
    fn factory_id() -> &'static str {
        "parts_feeder"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Feeder>()
            .into::<Relay>()
            .sink_named("answer");
    }
}

struct AnswererOp;
impl Operator for AnswererOp {
    fn factory_id() -> &'static str {
        "parts_answerer"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in").into::<Answerer>().response();
    }
}

#[test]
fn a_channel_without_a_sink_refuses_a_part_and_sends_nothing() {
    let (feeder, _a) = setup::<FeederOp>("parts-feeder");
    let (answerer, _b) = setup::<AnswererOp>("parts-answerer");
    let mut fed = dispatch(&feeder, "parts-feeder", "x");
    let mut answered = dispatch(&answerer, "parts-answerer", "y");

    let fed = read_frames(&mut fed, 1, WAIT);
    let answered = read_frames(&mut answered, 1, WAIT);
    assert!(!is_part(&fed[0]) && fed[0].message == b"x");
    assert!(!is_part(&answered[0]) && answered[0].message == b"y");
    assert_eq!(
        *REFUSALS.lock().unwrap(),
        ["part.no_sink", "part.no_sink"],
        "a channel feeding another tasklet and a response() end preview nothing"
    );
}

/// Opened by the test once it has read the part the blocked `run` sent.
static RELEASE: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());

test_tasklet!(Blocking, "parts_blocking", String => String, (), |input, ctx| {
    ctx.part("early".to_string())?;
    let (open, opened) = &RELEASE;
    let guard = open.lock().unwrap();
    let (_guard, timeout) = opened
        .wait_timeout_while(guard, WAIT, |open| !*open)
        .unwrap();
    if timeout.timed_out() {
        return Err(TaskletError::non_retryable("never released"));
    }
    ctx.emit(input)
});

struct BlockingOp;
impl Operator for BlockingOp {
    fn factory_id() -> &'static str {
        "parts_blocking"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Blocking>()
            .sink_named("answer");
    }
}

#[test]
fn a_part_leaves_while_run_is_still_blocked() {
    let (handle, _instance) = setup::<BlockingOp>("parts-blocking");
    let mut observer = dispatch(&handle, "parts-blocking", "late");

    let before = read_frames(&mut observer, 1, WAIT);
    assert_eq!(
        part_of(&before[0]),
        ("early".into(), "0".into(), "1".into())
    );
    *RELEASE.0.lock().unwrap() = true;
    RELEASE.1.notify_all();

    let after = read_frames(&mut observer, 1, WAIT);
    assert_eq!(after[0].message, b"late");
}
