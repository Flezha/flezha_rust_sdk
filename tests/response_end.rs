//! `response()`: a plugin end whose outputs answer the outside call their
//! input carried. Declared, it derives as a sink does (terminal alone, hybrid
//! with a consumer behind it) and never beside a sink; live, every output
//! carries `flz.reply_to`, the call's token on a called input and an empty
//! value otherwise, and a downstream tasklet's output carries the token of the
//! input upstream of it.

use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};

use flezha_runner::channel::{ChannelBuilder, ChannelKind};
use flezha_runner::ErrorModel;
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

test_tasklet!(Parse, "response_parse", String => String, (), |input, ctx| {
    ctx.emit(input)
});
test_tasklet!(Answer, "response_answer", String => String, (), |input, ctx| {
    ctx.emit(format!("re:{input}"))
});
test_tasklet!(Audit, "response_audit", String => (), (), |_input, _ctx| { Ok(()) });

struct DownstreamAnswer;
impl Operator for DownstreamAnswer {
    fn factory_id() -> &'static str {
        "response_downstream"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Parse>()
            .into::<Answer>()
            .response();
    }
}

struct HybridAnswer;
impl Operator for HybridAnswer {
    fn factory_id() -> &'static str {
        "response_hybrid"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Parse>()
            .response_with(|v: &String| v.as_bytes().to_vec())
            .into::<Audit>();
    }
}

struct AnswerBesideSink;
impl Operator for AnswerBesideSink {
    fn factory_id() -> &'static str {
        "response_beside_sink"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Parse>()
            .sink_named("out")
            .response();
    }
}

/// The kind, consumer count and response flag the declaration derives for
/// the output channel of the tasklet named `producer`.
fn end_of<R: Operator>(producer: &str) -> (ChannelKind, usize, bool) {
    let topology = build_topology_for::<R>().expect("topology");
    let descriptor = topology
        .channels
        .iter()
        .find(|c| {
            c.producer
                .and_then(|p| topology.tasklet(p))
                .is_some_and(|t| *t.name == *producer)
        })
        .expect("the producer's channel");
    (
        descriptor.kind,
        descriptor.consumers.len(),
        descriptor.response,
    )
}

#[test]
fn a_response_derives_as_a_sink_does() {
    assert_eq!(
        end_of::<DownstreamAnswer>("response_answer"),
        (ChannelKind::PluginBound, 0, true),
        "alone it is the plugin-bound terminal"
    );
    assert_eq!(
        end_of::<DownstreamAnswer>("response_parse"),
        (ChannelKind::InterTasklet, 1, false),
        "the channel upstream of it carries no end"
    );
    assert_eq!(
        end_of::<HybridAnswer>("response_parse"),
        (ChannelKind::InterTasklet, 1, true),
        "with a consumer behind it the channel stays hybrid"
    );
}

#[test]
fn a_response_beside_a_sink_ends_the_channel_twice() {
    let err = match build_topology_for::<AnswerBesideSink>() {
        Ok(_) => panic!("a channel with two plugin ends registered"),
        Err(e) => e,
    };
    let codes: Vec<String> = err.errors.iter().map(|i| i.code().to_string()).collect();
    assert!(
        codes.contains(&"topology.channel_ends_twice".to_string()),
        "read {codes:?}"
    );
}

#[test]
fn the_downstream_answer_carries_the_call_of_the_input_upstream() {
    let (handle, _instance) = setup::<DownstreamAnswer>("response-downstream");
    let (stream, received) = duplex_stream_with_reader();
    let called = HashMap::from([("flz.reply_to".to_string(), "w:tok".to_string())]);
    for (input, metadata) in [("hello", called), ("plain", HashMap::new())] {
        handle.dispatch(
            "response-downstream",
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
            (b"re:plain".to_vec(), Some(String::new())),
        ]
    );
}
