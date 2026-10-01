//! A flow and a steps operator under one factory id.
//!
//! Flow and steps operators share one factory-id namespace per runner, so the
//! second registration must be refused whichever kind either one is. This lives
//! in its own test binary because the `plain` id is claimed process-wide, and a
//! binary's registry is shared by every runner in it.

use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;

use flezha_runner::transport::FlezhaStream;
use flezha_runner::{ChannelBuilder, Ctx, ErrorModel, Flezha, FlezhaError, Operator, Tasklet, TaskletError};

/// The steps short form under `plain`: one tasklet, one sink, nothing else.
#[flezha_runner::operator(id = "plain", sink = "plain_sink", run = steps)]
fn plain(input: String) -> String {
    input
}

#[derive(Default)]
struct HandWrittenTask;
#[flezha_runner::tasklet(name = "hand_written")]
impl Tasklet for HandWrittenTask {
    type Input = String;
    type Output = String;
    fn run(&mut self, input: String, ctx: &mut Ctx<'_, Self>) -> Result<(), TaskletError> {
        let _ = ctx.emit(input);
        Ok(())
    }
}

/// A hand-written stream that claims the same factory id the short form does.
struct HandWrittenPlain;
impl Operator for HandWrittenPlain {
    fn factory_id() -> &'static str {
        "plain"
    }
    fn channels(c: &ChannelBuilder) {
        c.input::<String>("in")
            .into::<HandWrittenTask>()
            .sink_named("hand_written_sink");
    }
}

/// Every frame the fake controller captured, as `(payload_id, message)`.
type Frames = Arc<Mutex<Vec<(String, Vec<u8>)>>>;

/// Accepts control connections and captures every frame the SDK writes, as
/// `runner_lifecycle.rs`'s fake controller does: `start()` waits for the local
/// connect and the init write, never for a controller frame.
fn start_fake_controller(dir: &Path) -> Frames {
    let listener =
        UnixListener::bind(dir.join("runner_control_channel.sock")).expect("bind control socket");
    let frames = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&frames);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(s) = stream else { break };
            let sink = Arc::clone(&sink);
            thread::spawn(move || {
                let stream = FlezhaStream::Unix(s);
                while let Ok((payload_id, message, _metadata)) =
                    flezha_runner::service::payloader::extract_payload(&stream)
                {
                    sink.lock().unwrap().push((payload_id, message));
                }
            });
        }
    });
    frames
}

/// The `factory_ids` of the first connect init frame, polled until it arrives.
fn init_frame_factories(frames: &Frames) -> Vec<String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let captured = frames.lock().unwrap();
        if let Some((_, message)) = captured
            .iter()
            .find(|(id, _)| id == "connect_control_channel")
        {
            let json: serde_json::Value =
                serde_json::from_slice(message).expect("init frame is JSON");
            return json["factory_ids"]
                .as_array()
                .expect("factory_ids is an array")
                .iter()
                .map(|v| v.as_str().expect("factory id is a string").to_string())
                .collect();
        }
        drop(captured);
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for the connect init frame"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[test]
fn a_steps_short_form_colliding_with_a_stream_is_refused() {
    let dir = std::env::temp_dir().join(format!("flz-short-dup-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create socket dir");
    std::env::set_var("FLEZHA_SOCKET_FILES_PATH", &dir);
    std::env::set_var("FLEZHA_RUNNER_ID_PATH", dir.join("runner.id"));
    std::env::set_var("FLEZHA_TRANSPORT", "uds");
    std::env::set_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS", "300000");

    let frames = start_fake_controller(&dir);

    let runner = Flezha::create_runner("ns_short_dup").expect("create");
    runner
        .register_factory(HandWrittenPlain)
        .expect("the hand-written stream takes the id first");

    let err = runner.register_factory(Plain).err().expect(
        "a steps operator under a flow operator's factory id must be refused, not skipped",
    );
    assert!(
        matches!(err, FlezhaError::DuplicateFactoryId { ref factory_id } if factory_id == "plain"),
        "expected DuplicateFactoryId naming `plain`, got: {err}"
    );
    assert_eq!(err.code(), "topology.duplicate_factory_id", "{err}");
    assert_eq!(err.class(), flezha_runner::errors::Class::Invalid);

    // The refused registration left the declared set exactly as it was.
    runner.start().expect("start");
    assert_eq!(init_frame_factories(&frames), vec!["plain".to_string()]);
}
