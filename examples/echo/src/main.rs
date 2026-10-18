//! Starts a runner: a process that connects to the controller and serves your
//! relays.
//!
//! Declare every relay you want served before `start()`. One runner can
//! serve several, and the same call takes a steps relay.

mod relay;
mod tasklet;

use flezha_runner::Flezha;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "default".to_string());

    // `run()` is `start()` followed by `await_termination()`. Splitting them
    // leaves room for the calls below, and still blocks until shutdown.
    // The runner is bound to a name because a chained temporary would be
    // dropped, and shut down, at the end of the statement.
    let runner = Flezha::create_runner(namespace)?;
    // connects to the controller and serves these relays
    runner.register_factory(relay::EchoRelay)?.start()?;

    // Everything `flz relay` and `flz plugin` do is also a method of
    // `control()`, and `send` hands a message to a relay. Uncomment to
    // drive this namespace from the application instead of from a terminal:
    //
    // let control = runner.control();
    // control.start_relay(&flezha_runner::RelaySpec::new("echo").id("my_echo"))?;
    // control.enable_plugin(
    //     "writer",
    //     &flezha_runner::PluginSpec::new("relay_file_output")
    //         .opts(serde_json::json!({ "path": "echo-out.txt" })),
    // )?;
    // control.bind_plugin("writer", "my_echo", serde_json::json!([["on_output", {}]]), "")?;
    // let opts = flezha_runner::SendOpts::new().msg_id("hello-1");
    // runner.send("my_echo", "hello world", "in", &opts)?;
    //
    // docs/public/topics/core/sending_and_calling.md covers sending. An
    // enforced controller refuses all of it until
    // `flz role bind runner@default app default`.

    runner.await_termination()?;
    Ok(())
}
