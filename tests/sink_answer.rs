//! A called input's sink outputs answer the call: every one carries
//! `flz.reply_to` with the token of the input that caused it, a downstream
//! tasklet's included, and an output of an input nobody called carries none.

use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};

use flezha_runner::channel::ChannelBuilder;
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

mod common;
use common::prelude::*;
use common::{setup, wait_until};

type ReceivedFrame = (String, Vec<u8>, HashMap<String, String>);

fn duplex_stream_with_reader() -> (Arc<FlezhaStream>, Arc<Mutex<Vec<ReceivedFrame>>>) {
    let (a, b) = UnixStream::pair().expect("UnixStream::pair");
    let received = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&received);
    std::thread::spawn(move || {
        let mut reader = b;
        while let Ok(frame) = payloader::extract_payload(&mut reader) {
            seen.lock().unwrap().push(frame);
        }
    });
    (Arc::new(FlezhaStream::Unix(a)), received)
}

test_tasklet!(Parse, "sink_answer_parse", String => String, (), |input, ctx| {
    ctx.emit(input)
});
test_tasklet!(Answer, "sink_answer_answer", String => String, (), |input, ctx| {
    ctx.emit(format!("re:{input}"))
});

struct DownstreamAnswer;
impl Relay for DownstreamAnswer {
    fn factory_id() -> &'static str {
        "sink_answer_downstream"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Parse>()
            .into::<Answer>()
            .sink_named("quotes");
    }
}

#[test]
fn the_downstream_answer_carries_the_call_of_the_input_upstream() {
    let (handle, _instance) = setup::<DownstreamAnswer>("sink-answer-downstream");
    let (stream, received) = duplex_stream_with_reader();
    let called = HashMap::from([("flz.reply_to".to_string(), "w:tok".to_string())]);
    for (input, metadata) in [("hello", called), ("plain", HashMap::new())] {
        handle.dispatch(
            "sink-answer-downstream",
            input.as_bytes().to_vec(),
            &metadata,
            Arc::clone(&stream),
        );
        let count = if input == "hello" { 1 } else { 2 };
        assert!(
            wait_until(
                || received.lock().unwrap().len() == count,
                Duration::from_secs(5)
            ),
            "the answer to `{input}` never left"
        );
    }

    let answers: Vec<(Vec<u8>, Option<String>)> = received
        .lock()
        .unwrap()
        .iter()
        .map(|(_, body, meta)| (body.clone(), meta.get("flz.reply_to").cloned()))
        .collect();
    assert_eq!(
        answers,
        vec![
            (b"re:hello".to_vec(), Some("w:tok".to_string())),
            (b"re:plain".to_vec(), None),
        ]
    );
}
