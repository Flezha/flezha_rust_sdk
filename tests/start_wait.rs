//! A start no controller answers fails after `FLEZHA_RUNNER_START_WAIT_MS`
//! with `runtime.connect_unconfirmed`, rather than waiting for ever, and leaves
//! the process's runner slot free. The start runs in a child process of this
//! test binary, whose standard error is what the test reads.

use std::process::Command;

use flezha_runner::{ErrorModel, Flezha, FlezhaError};

const CHILD: &str = "FLZ_START_WAIT_CHILD";

#[test]
fn an_unanswered_start_fails_after_the_start_wait() {
    if std::env::var_os(CHILD).is_some() {
        return unanswered_start();
    }
    let dir = std::env::temp_dir().join(format!("flz-start-wait-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create socket dir");
    let child = Command::new(std::env::current_exe().expect("the test binary"))
        .args(["--exact", "an_unanswered_start_fails_after_the_start_wait"])
        .args(["--nocapture", "--test-threads=1"])
        .env(CHILD, "1")
        .env("FLEZHA_SOCKET_FILES_PATH", &dir)
        .env("FLEZHA_RUNNER_ID_PATH", dir.join("runner.id"))
        .env("FLEZHA_TRANSPORT", "uds")
        .env("FLEZHA_RUNNER_START_WAIT_MS", "300")
        .output()
        .expect("run the start in a child");
    let stderr = String::from_utf8_lossy(&child.stderr);
    assert!(child.status.success(), "the child failed:\n{stderr}");

    let dialling = [
        "[flezha] warning: control socket absent at ",
        "[flezha] warning: Connection attempt ",
        "[flezha] Control channel attempt ",
    ];
    assert!(stderr.contains("[flezha] warning: Connection attempt 1 failed: "));
    let stray: Vec<_> = stderr
        .lines()
        .filter(|line| line.starts_with("[flezha]"))
        .filter(|line| !dialling.iter().any(|shape| line.starts_with(shape)))
        .collect();
    assert!(stray.is_empty(), "lines the dial does not write: {stray:?}");
}

fn unanswered_start() {
    let runner = Flezha::create_runner("start_wait_ns").expect("create");
    match runner.start() {
        Err(error @ FlezhaError::ConnectUnconfirmed { wait_ms: 300 }) => {
            assert_eq!(error.code(), "runtime.connect_unconfirmed");
        }
        Ok(_) => panic!("a start no controller answered succeeded"),
        Err(other) => panic!("expected ConnectUnconfirmed, got: {other:?}"),
    }
    assert!(
        Flezha::current_runner().is_none(),
        "the slot stayed claimed"
    );
}
