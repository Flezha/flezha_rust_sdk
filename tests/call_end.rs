//! `call(...)`: a chain's end that makes every value a request to another
//! relay. Declared, it is a plugin end beside no other and answers an entry
//! channel, `<channel>.response`, carrying [`Response`]s; live, each request
//! leaves as a plugin output stamped with the call's facts and the chain's
//! key, a filtered-out value making none; a `Response` fed back reaches the
//! tasklet the Response channel feeds.

use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};

use flezha_runner::channel::{ChannelBuilder, ChannelKind};
use flezha_runner::{CallTarget, Each, ErrorModel, OnFailure, Relay, RelaySettings, Response, Run};
use flz_transport::service::payloader;
use flz_transport::transport::FlezhaStream;

mod common;
use common::contract::contract_file;
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

test_tasklet!(Ask, "call_end_ask", String => String, (), |input, ctx| {
    ctx.emit(input)
});
test_tasklet!(Read, "call_end_read", Response<String, String> => String, (), |input, ctx| {
    let outcome = match &input.result {
        Ok(answer) => format!("ok:{answer}"),
        Err(error) => format!("error:{}", error.code),
    };
    ctx.emit(format!("{}={outcome}@{}/{}", input.request, input.target, input.created))
});

struct Caller;
impl Relay for Caller {
    fn factory_id() -> &'static str {
        "call_end_caller"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Ask>()
            .named("ask")
            .filter(|v: &String| v != "skip")
            .key_by(|v: &String| v.split(':').next().unwrap_or_default().to_string())
            .call::<String>(
                CallTarget::factory("agent")
                    .create(false)
                    .channel("turn")
                    .answer_channel("reply"),
                60_000,
            )
            .into::<Read>()
            .output_named("read");
    }
}

struct CallBesideSink;
impl Relay for CallBesideSink {
    fn factory_id() -> &'static str {
        "call_end_beside_sink"
    }
    fn channels(c: &ChannelBuilder) {
        let ask = c.input::<String>("in").into::<Ask>().output_named("out");
        ask.call::<String>(CallTarget::id("pricing"), 500)
            .into::<Read>();
    }
}

struct OnceCaller;
impl Relay for OnceCaller {
    fn factory_id() -> &'static str {
        "call_end_once"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<Ask>()
            .call::<String>(CallTarget::id("pricing"), 500)
            .into::<Read>();
    }

    fn settings() -> RelaySettings {
        RelaySettings {
            run: Run::Steps {
                each: Each::Once,
                on_failure: OnFailure::UndoAll,
            },
            max_duration: Some(Duration::from_secs(60)),
            ..RelaySettings::default()
        }
    }
}

#[test]
fn a_call_is_a_plugin_end_answering_an_entry_channel() {
    let topology = build_topology_for::<Caller>().expect("topology");
    let ask = topology
        .channels
        .iter()
        .find(|c| &*c.name == "ask")
        .expect("`ask`");
    assert_eq!(ask.kind, ChannelKind::PluginBound);
    let end = ask.call.as_ref().expect("the call end");
    assert_eq!(&*end.target, "factory:agent");
    assert!(!end.create);
    assert_eq!(end.channel.as_deref(), Some("turn"));
    assert_eq!(end.answer_channel.as_deref(), Some("reply"));
    assert_eq!(&*end.response, "ask.response");
    let response = topology
        .channels
        .iter()
        .find(|c| &*c.name == "ask.response")
        .expect("the Response channel");
    assert_eq!(response.kind, ChannelKind::Entry);
    assert_eq!(response.consumers.len(), 1);
}

#[test]
fn a_call_beside_an_output_ends_the_channel_twice() {
    let Err(err) = build_topology_for::<CallBesideSink>() else {
        panic!("a channel with two plugin ends registered");
    };
    let codes: Vec<String> = err.errors.iter().map(|i| i.code().to_string()).collect();
    assert!(
        codes.contains(&"topology.channel_ends_twice".to_string()),
        "read {codes:?}"
    );
}

#[test]
fn a_single_pass_class_that_calls_is_refused() {
    let err = flezha_runner::steps::spec::build_spec::<OnceCaller>()
        .expect_err("a :once class never reads its Response");
    assert_eq!(err.code(), "topology.call_outside_per_input_loop");
}

#[test]
fn each_request_leaves_stamped_and_the_response_reaches_its_reader() {
    let (handle, _instance) = setup::<Caller>("call-end-caller");
    let (stream, received) = duplex_stream_with_reader();
    let before = flz_core::util::wall_now_ms();
    for input in ["skip", "anna-42:hello"] {
        handle.dispatch(
            "call-end-caller",
            input.as_bytes().to_vec(),
            &HashMap::new(),
            Arc::clone(&stream),
        );
    }
    assert!(
        wait_until(
            || received.lock().unwrap().len() == 1,
            Duration::from_secs(5)
        ),
        "the request never left"
    );
    let (_, body, meta) = received.lock().unwrap()[0].clone();
    assert_eq!(
        body,
        b"anna-42:hello".to_vec(),
        "the filtered value made no request"
    );
    assert_eq!(meta["channel"], "ask");
    assert_eq!(meta["flz.call"], "factory:agent");
    assert_eq!(meta["flz.call_create"], "false");
    assert_eq!(meta["flz.call_channel"], "turn");
    assert_eq!(meta["flz.call_answer_channel"], "reply");
    assert_eq!(meta["flz.call_response"], "ask.response");
    assert_eq!(meta["flz.key"], "anna-42");
    let deadline: u64 = meta["flz.deadline_at"].parse().expect("a millisecond");
    assert!(deadline >= before + 60_000 && deadline <= flz_core::util::wall_now_ms() + 60_000);

    let envelope = br#"{"call":"c1","target":"anna-42","created":true,"request":"YW5uYS00MjpoZWxsbw==","value":"aGk="}"#;
    let response = HashMap::from([("channel".to_string(), "ask.response".to_string())]);
    handle.dispatch(
        "call-end-caller",
        envelope.to_vec(),
        &response,
        Arc::clone(&stream),
    );
    assert!(
        wait_until(
            || received.lock().unwrap().len() == 2,
            Duration::from_secs(5)
        ),
        "the Response never reached its reader"
    );
    let (_, body, meta) = received.lock().unwrap()[1].clone();
    assert_eq!(meta["channel"], "read");
    assert_eq!(body, b"anna-42:hello=ok:hi@anna-42/true".to_vec());
}

/// Standard padded base64, to compare decoded bytes with the fixture's.
fn b64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n =
            chunk.iter().fold(0u32, |acc, &b| acc << 8 | u32::from(b)) << (8 * (3 - chunk.len()));
        for i in 0..4 {
            let sextet = (n >> (18 - 6 * i) & 63) as usize;
            out.push(if i <= chunk.len() {
                ALPHABET[sextet] as char
            } else {
                '='
            });
        }
    }
    out
}

/// Every case of the shared fixture decodes to the same fields here as in
/// every other SDK.
#[test]
fn every_envelope_case_decodes() {
    let path = contract_file("test/fixtures/call_response_envelope.json");
    let fixture: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).expect("the shared fixture"))
            .expect("JSON");
    for case in fixture["cases"].as_array().expect("cases") {
        let (name, expect) = (&case["name"], &case["expect"]);
        let response: Response<Vec<u8>, Vec<u8>> =
            Response::decode(case["payload"].as_str().expect("payload").as_bytes())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(b64(&response.request), expect["request"], "{name}");
        assert_eq!(response.target, expect["target"], "{name}");
        assert_eq!(response.created, expect["created"], "{name}");
        match response.result {
            Ok(value) => {
                assert_eq!(expect["ok"], true, "{name}");
                assert_eq!(b64(&value), expect["value"], "{name}");
            }
            Err(error) => {
                assert_eq!(expect["ok"], false, "{name}");
                assert_eq!(error.code, expect["error_code"], "{name}");
                assert_eq!(error.class.as_str(), expect["error_class"], "{name}");
                assert_eq!(error.message, expect["error_message"], "{name}");
            }
        }
    }
}
