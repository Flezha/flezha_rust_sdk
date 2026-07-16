//! Runner lifecycle + process-wide singleton guard
//! (`runner-lifecycle-singleton-api`).
//!
//! Every phase below shares the ONE process-wide runner slot, the global
//! class registry, and the process env (socket path), so the phases run
//! sequentially inside a single `#[test]` rather than as parallel test fns.
//! The "controller" is a bare `UnixListener` that accepts and drains control
//! connections — `start()` only waits for the local connect + init write,
//! never for a controller frame.

use std::os::unix::net::UnixListener;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use flezha_runner::state::{NoChange, RestoreInitPayload, WallClock};
use flezha_runner::{
    ChannelBuilder, Flezha, Operation, OperationCtx, OperationError, RunnerStartError, StateHandle,
    UnboundedOperative,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default)]
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

/// Declares a channel and never wires it — `define_unbounded` must report
/// this as `Err(TopologyBuildError)`, not panic.
struct UnwiredOperative;
impl UnboundedOperative for UnwiredOperative {
    fn factory_id() -> &'static str {
        "lifecycle_unwired"
    }
    fn channels(c: &mut ChannelBuilder) {
        c.channel::<String>("dangling");
    }
}

/// Accepts control connections at the path `control_socket_path()` resolves
/// to and drains whatever the SDK writes. Counts accepts so the guard tests
/// can assert a rejected `start()` never dialed.
fn start_fake_controller(dir: &Path) -> Arc<AtomicUsize> {
    let listener =
        UnixListener::bind(dir.join("runner_control_channel.sock")).expect("bind control socket");
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&accepted);
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut s) = stream else { break };
            counter.fetch_add(1, Ordering::SeqCst);
            thread::spawn(move || {
                use std::io::Read;
                let mut buf = [0u8; 4096];
                while matches!(s.read(&mut buf), Ok(n) if n > 0) {}
            });
        }
    });
    accepted
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

    let accepted = start_fake_controller(&dir);

    // -- before anything starts, the accessor is empty and never throws --
    assert!(Flezha::current_runner().is_none());

    // -- an empty namespace is a typed error --
    assert!(matches!(
        Flezha::new_runner(""),
        Err(RunnerStartError::InvalidNamespace)
    ));

    // -- 5.6: an invalid topology is an Err at the define call site --
    let err = Flezha::new_runner("ns_broken")
        .expect("builder")
        .define_unbounded::<UnwiredOperative>()
        .err()
        .expect("unwired channel must fail declaration");
    assert!(
        err.to_string().contains("dangling"),
        "error should name the unwired channel, got: {err}"
    );

    // -- 5.5: two threads race start(); exactly one wins deterministically --
    let barrier = Arc::new(Barrier::new(2));
    let spawn_start = |ns: &'static str, define_b: bool| {
        let barrier = Arc::clone(&barrier);
        thread::spawn(move || {
            let builder = if define_b {
                Flezha::new_runner(ns)
                    .unwrap()
                    .define_unbounded::<ProbeOperativeB>()
                    .unwrap()
            } else {
                Flezha::new_runner(ns)
                    .unwrap()
                    .define_unbounded::<ProbeOperativeA>()
                    .unwrap()
            };
            barrier.wait();
            builder.start()
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
        RunnerStartError::AlreadyRunning { namespace } => {
            assert_eq!(
                namespace, winner_ns,
                "loser must be told the winner's namespace"
            );
        }
        other => panic!("expected AlreadyRunning, got: {other}"),
    }
    assert_eq!(runner.id(), winner_ns);

    // -- 5.4: the accessor reflects the active runner --
    let current = Flezha::current_runner().expect("a runner is active");
    assert_eq!(current.id(), winner_ns);
    drop(current);

    // -- 5.1: a second start() is rejected without dialing the controller --
    let accepts_before = accepted.load(Ordering::SeqCst);
    let second = Flezha::new_runner("ns_second").unwrap().start();
    match second {
        Err(RunnerStartError::AlreadyRunning { namespace }) => assert_eq!(namespace, winner_ns),
        Ok(_) => panic!("expected AlreadyRunning, got a started runner"),
        Err(other) => panic!("expected AlreadyRunning, got: {other}"),
    }
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        accepts_before,
        "a rejected start() must not attempt a connection"
    );

    // -- 5.3: await_termination() unblocks when the owning handle drops --
    let observer = Flezha::current_runner().expect("active");
    let waiter = thread::spawn(move || observer.await_termination());
    // Give the waiter time to actually block before we drop the owner.
    thread::sleep(Duration::from_millis(100));
    assert!(!waiter.is_finished(), "await_termination returned early");
    drop(runner);
    waiter
        .join()
        .expect("await_termination must return after the owner drops");

    // -- 5.4: the accessor is empty again after shutdown --
    assert!(Flezha::current_runner().is_none());

    // -- 5.2: a runner may be started again after the first one dropped --
    let restarted = Flezha::new_runner("ns_restart")
        .unwrap()
        .start()
        .expect("restart after close must succeed");
    assert_eq!(restarted.id(), "ns_restart");
    assert_eq!(
        Flezha::current_runner().expect("active again").id(),
        "ns_restart"
    );
    drop(restarted);
    assert!(Flezha::current_runner().is_none());
}
