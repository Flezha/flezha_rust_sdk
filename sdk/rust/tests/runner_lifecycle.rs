//! Runner lifecycle + process-wide singleton guard + on-demand define/remove
//! (`sdk-runner-entrypoint-redesign`).
//!
//! Every phase below shares the ONE process-wide runner slot, the global
//! class registries, and the process env (socket path), so the phases run
//! sequentially inside a single `#[test]` rather than as parallel test fns.
//! The "controller" is a bare `UnixListener` that accepts control
//! connections and captures + decodes every frame the SDK writes (the init
//! JSON and SDK-initiated `CommandRequest`s such as `UpdateFactories`) —
//! `start()` only waits for the local connect + init write, never for a
//! controller frame.

use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use flezha_runner::state::{NoChange, RestoreInitPayload, WallClock};
use flezha_runner::transport::FlezhaStream;
use flezha_runner::{
    ChannelBuilder, Flezha, FlezhaError, Operation, OperationCtx, OperationError, StateHandle,
    UnboundedOperative,
};
use prost::Message;

#[derive(serde::Serialize, serde::Deserialize, Default)]
struct EmptyState;

#[derive(Default)]
struct PassOp;
impl Operation for PassOp {
    type Input = String;
    type Output = String;
    type State = EmptyState;
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str {
        "lifecycle_pass"
    }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) -> EmptyState {
        EmptyState
    }
    fn run(
        &mut self,
        _input: String,
        _state: StateHandle<'_, Self>,
        _ctx: &mut OperationCtx<'_>,
    ) -> Result<(), OperationError> {
        Ok(())
    }
}

struct ProbeOperativeA;
impl UnboundedOperative for ProbeOperativeA {
    fn factory_id() -> &'static str {
        "lifecycle_probe_a"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("in").into::<PassOp>();
    }
}

struct ProbeOperativeB;
impl UnboundedOperative for ProbeOperativeB {
    fn factory_id() -> &'static str {
        "lifecycle_probe_b"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("in").into::<PassOp>();
    }
}

struct ProbeOperativeC;
impl UnboundedOperative for ProbeOperativeC {
    fn factory_id() -> &'static str {
        "lifecycle_probe_c"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.input::<String>("in").into::<PassOp>();
    }
}

/// Declares a channel and never wires it — `define_unbounded` must report
/// this as `Err(FlezhaError::InvalidTopology)`, not panic.
struct UnwiredOperative;
impl UnboundedOperative for UnwiredOperative {
    fn factory_id() -> &'static str {
        "lifecycle_unwired"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.channel::<String>("dangling");
    }
}

/// A frame captured off a control connection: the payloader payload id plus
/// the raw message bytes.
type CapturedFrame = (String, Vec<u8>);

/// Accepts control connections at the path `control_socket_path()` resolves
/// to and DECODES every payloader frame the SDK writes into `frames`.
/// Counts accepts so the guard phases can assert a rejected `start()` never
/// dialed.
fn start_fake_controller(dir: &Path) -> (Arc<AtomicUsize>, Arc<Mutex<Vec<CapturedFrame>>>) {
    let listener =
        UnixListener::bind(dir.join("runner_control_channel.sock")).expect("bind control socket");
    let accepted = Arc::new(AtomicUsize::new(0));
    let frames = Arc::new(Mutex::new(Vec::new()));
    let counter = Arc::clone(&accepted);
    let sink = Arc::clone(&frames);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(s) = stream else { break };
            counter.fetch_add(1, Ordering::SeqCst);
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
    (accepted, frames)
}

/// Poll the captured frames until `probe` yields a value; panics after 5 s.
fn await_captured<T>(
    frames: &Arc<Mutex<Vec<CapturedFrame>>>,
    what: &str,
    probe: impl Fn(&[CapturedFrame]) -> Option<T>,
) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(found) = probe(&frames.lock().unwrap()) {
            return found;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(20));
    }
}

/// The `factory_ids` list of the `index`-th connect init frame (0-based),
/// waiting until that many init frames exist.
fn init_frame_factories(frames: &Arc<Mutex<Vec<CapturedFrame>>>, index: usize) -> Vec<String> {
    await_captured(frames, &format!("init frame #{index}"), |captured| {
        let (_, message) = captured
            .iter()
            .filter(|(id, _)| id == "connect_control_channel")
            .nth(index)?;
        let json: serde_json::Value = serde_json::from_slice(message).expect("init frame is JSON");
        Some(
            json["factory_ids"]
                .as_array()
                .expect("factory_ids is an array")
                .iter()
                .map(|v| v.as_str().expect("factory id is a string").to_string())
                .collect(),
        )
    })
}

/// The sorted factory-id set of the NEWEST `UpdateFactories` frame once at
/// least `min_count` of them were captured.
fn update_factories_set(frames: &Arc<Mutex<Vec<CapturedFrame>>>, min_count: usize) -> Vec<String> {
    await_captured(
        frames,
        &format!("UpdateFactories frame #{min_count}"),
        |captured| {
            let updates: Vec<Vec<String>> = captured
                .iter()
                .filter(|(id, _)| id == "control_command")
                .filter_map(|(_, message)| {
                    let request =
                        flezha_runner::pb::CommandRequest::decode(message.as_slice()).ok()?;
                    match request.command {
                        Some(flezha_runner::pb::command_request::Command::UpdateFactories(
                            update,
                        )) => Some(update.factories.into_iter().map(|f| f.id).collect()),
                        _ => None,
                    }
                })
                .collect();
            if updates.len() < min_count {
                return None;
            }
            let mut newest = updates.last().expect("at least one update").clone();
            newest.sort();
            Some(newest)
        },
    )
}

#[test]
fn runner_lifecycle_and_singleton_guard() {
    let dir = std::env::temp_dir().join(format!("flz-lifecycle-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create socket dir");
    std::env::set_var("FLEZHA_SOCKET_FILES_PATH", &dir);
    std::env::set_var("FLEZHA_RUNNER_ID_PATH", dir.join("runner.id"));
    std::env::set_var("FLEZHA_TRANSPORT", "uds");
    // Keep the reader's ack deadline far above the test's runtime so the
    // frame-less fake controller never triggers reconnect churn mid-test.
    std::env::set_var("FLEZHA_CONTROL_ACK_TIMEOUT_MS", "300000");

    let (accepted, frames) = start_fake_controller(&dir);

    // -- before anything starts, the accessor is empty and never throws --
    assert!(Flezha::current_runner().is_none());

    // -- an empty namespace is a typed error --
    assert!(matches!(
        Flezha::create_runner(""),
        Err(FlezhaError::InvalidNamespace)
    ));

    // -- an invalid topology is an Err at the define call site --
    let broken = Flezha::create_runner("ns_broken").expect("create");
    let err = broken
        .define_unbounded::<UnwiredOperative>()
        .err()
        .expect("unwired channel must fail declaration");
    assert!(
        matches!(err, FlezhaError::InvalidTopology(_)),
        "expected InvalidTopology, got: {err}"
    );
    assert!(
        err.to_string().contains("dangling"),
        "error should name the unwired channel, got: {err}"
    );
    // ...and the failed define left nothing declared: removing it is NotDefined.
    assert!(matches!(
        broken.remove_unbounded::<UnwiredOperative>(),
        Err(FlezhaError::NotDefined { .. })
    ));
    drop(broken); // never started ⇒ Drop is a no-op

    // -- a duplicate factory id on one runner is refused --
    let dup = Flezha::create_runner("ns_dup").expect("create");
    dup.define_unbounded::<ProbeOperativeA>().expect("define A");
    match dup.define_unbounded::<ProbeOperativeA>() {
        Err(FlezhaError::DuplicateFactoryId { factory_id }) => {
            assert_eq!(factory_id, "lifecycle_probe_a");
        }
        other => panic!("expected DuplicateFactoryId, got: {:?}", other.err()),
    }
    drop(dup);

    // -- removing an undefined factory is NotDefined --
    let fresh = Flezha::create_runner("ns_fresh").expect("create");
    match fresh.remove_unbounded::<ProbeOperativeB>() {
        Err(FlezhaError::NotDefined { factory_id }) => {
            assert_eq!(factory_id, "lifecycle_probe_b");
        }
        other => panic!("expected NotDefined, got: {:?}", other.err()),
    }
    drop(fresh);

    // -- creating (and mutating) runners never claims the slot --
    assert!(Flezha::current_runner().is_none());

    // -- two threads race start(); exactly one wins deterministically --
    let barrier = Arc::new(Barrier::new(2));
    let spawn_start = |ns: &'static str, define_b: bool| {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            let runner = Flezha::create_runner(ns).unwrap();
            if define_b {
                runner.define_unbounded::<ProbeOperativeB>().unwrap();
            } else {
                runner.define_unbounded::<ProbeOperativeA>().unwrap();
            }
            barrier.wait();
            match runner.start().map(|_| ()) {
                Ok(()) => Ok(runner),
                Err(e) => Err(e),
            }
        })
    };
    let t1 = spawn_start("ns_one", false);
    let t2 = spawn_start("ns_two", true);
    let (r1, r2) = (t1.join().unwrap(), t2.join().unwrap());

    let (runner, loser_err, winner_ns) = match (r1, r2) {
        (Ok(r), Err(e)) => (r, e, "ns_one"),
        (Err(e), Ok(r)) => (r, e, "ns_two"),
        (Ok(_), Ok(_)) => panic!("both concurrent start() calls succeeded"),
        (Err(a), Err(b)) => panic!("both concurrent start() calls failed: {a} / {b}"),
    };
    match loser_err {
        FlezhaError::AlreadyRunning { namespace } => {
            assert_eq!(
                namespace, winner_ns,
                "loser must be told the winner's namespace"
            );
        }
        other => panic!("expected AlreadyRunning, got: {other}"),
    }
    assert_eq!(runner.namespace(), winner_ns);

    // -- the init frame carried exactly the winner's declared factory --
    let winner_factory = if winner_ns == "ns_one" {
        "lifecycle_probe_a"
    } else {
        "lifecycle_probe_b"
    };
    assert_eq!(init_frame_factories(&frames, 0), vec![winner_factory]);

    // -- double-start on one runner is an error --
    match runner.start() {
        Err(FlezhaError::AlreadyRunning { namespace }) => assert_eq!(namespace, winner_ns),
        Ok(_) => panic!("second start() on the same runner must fail"),
        Err(other) => panic!("expected AlreadyRunning, got: {other}"),
    }

    // -- the accessor reflects the active runner --
    let current = Flezha::current_runner().expect("a runner is active");
    assert_eq!(current.namespace(), winner_ns);
    drop(current);

    // -- a second runner's start() is rejected without dialing the controller --
    let accepts_before = accepted.load(Ordering::SeqCst);
    let second = Flezha::create_runner("ns_second").unwrap();
    match second.start() {
        Err(FlezhaError::AlreadyRunning { namespace }) => assert_eq!(namespace, winner_ns),
        Ok(_) => panic!("expected AlreadyRunning, got a started runner"),
        Err(other) => panic!("expected AlreadyRunning, got: {other}"),
    }
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        accepts_before,
        "a rejected start() must not attempt a connection"
    );
    drop(second);

    // -- post-start define ships the FULL factory set as UpdateFactories --
    runner
        .define_unbounded::<ProbeOperativeC>()
        .expect("post-start define is legal");
    let mut expected = vec![winner_factory.to_string(), "lifecycle_probe_c".to_string()];
    expected.sort();
    assert_eq!(update_factories_set(&frames, 1), expected);

    // -- post-start remove (no live instances) ships the remaining set --
    runner
        .remove_unbounded::<ProbeOperativeC>()
        .expect("post-start remove of an instance-less type is legal");
    assert_eq!(
        update_factories_set(&frames, 2),
        vec![winner_factory.to_string()]
    );
    assert!(
        flezha_runner::operative::registry::get_class("lifecycle_probe_c").is_none(),
        "the removed class must be unregistered"
    );

    // -- pre-start awaits/operatives error typed; started runner serves them --
    let never_started = Flezha::create_runner("ns_cold").unwrap();
    assert!(matches!(
        never_started.await_termination(),
        Err(FlezhaError::NotStarted)
    ));
    assert!(matches!(
        never_started.operatives(),
        Err(FlezhaError::NotStarted)
    ));
    drop(never_started);
    assert!(
        runner.operatives().expect("started runner").is_empty(),
        "no AddOperative was issued, so the instance registry is empty"
    );

    // -- await_termination() unblocks when the owning handle drops --
    let observer = Flezha::current_runner().expect("active");
    let waiter = thread::spawn(move || observer.await_termination());
    // Give the waiter time to actually block before we drop the owner.
    thread::sleep(Duration::from_millis(100));
    assert!(!waiter.is_finished(), "await_termination returned early");
    drop(runner);
    waiter
        .join()
        .expect("await_termination must return after the owner drops")
        .expect("await_termination on a started runner returns Ok");

    // -- the accessor is empty again after shutdown --
    assert!(Flezha::current_runner().is_none());

    // -- restart: generation 2's declared set starts EMPTY (no leak) --
    let restarted = Flezha::create_runner("ns_restart").unwrap();
    restarted
        .start()
        .expect("restart after close must succeed — zero-op start is legal");
    assert_eq!(restarted.namespace(), "ns_restart");
    assert_eq!(
        Flezha::current_runner().expect("active again").namespace(),
        "ns_restart"
    );
    // Generation 1 declared probe factories; generation 2 declared nothing,
    // so its init frame must carry an EMPTY factory list (zero-op start) —
    // pinning the empty-per-generation contract the old cross-generation
    // registry leak violated.
    assert_eq!(
        init_frame_factories(&frames, 1),
        Vec::<String>::new(),
        "generation 2 must re-declare: the init frame starts empty"
    );

    // -- pre-start define+remove shaped the NEXT generation's init frame --
    drop(restarted);
    assert!(Flezha::current_runner().is_none());

    let shaped = Flezha::create_runner("ns_shaped").unwrap();
    shaped
        .define_unbounded::<ProbeOperativeA>()
        .expect("define A")
        .define_unbounded::<ProbeOperativeB>()
        .expect("define B");
    shaped
        .remove_unbounded::<ProbeOperativeB>()
        .expect("pre-start remove is legal");
    shaped.start().expect("start");
    assert_eq!(
        init_frame_factories(&frames, 2),
        vec!["lifecycle_probe_a".to_string()],
        "the init frame must reflect the pre-start remove"
    );
    drop(shaped);
    assert!(Flezha::current_runner().is_none());
}
