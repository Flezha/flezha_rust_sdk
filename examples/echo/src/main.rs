//! Starts a runner: a process that connects to the controller and serves your
//! operatives.
//!
//! Declare every operative you want served before `start()`. One runner can
//! serve several, and `define_workflow` is the same call for the workflow kind.

mod operative;
mod procedure;

use flezha_runner::Flezha;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "default".to_string());

    // `run()` is `start()` followed by `await_termination()`. Splitting them
    // leaves room for the admin calls below, and still blocks until shutdown.
    // The runner is bound to a name because a chained temporary would be
    // dropped, and shut down, at the end of the statement.
    let runner = Flezha::create_runner(namespace)?;
    runner
        .define_stream::<operative::EchoOperative>()?
        .start()?;

    // Everything `flz operative` and `flz plugin` do is also a method here.
    // Uncomment to drive this namespace from the application instead of from a
    // terminal:
    //
    // let admin = runner.admin();
    // admin.start_operative(&flezha_runner::OperativeSpec::new("my_echo", "echo"))?;
    // admin.enable_plugin(
    //     "writer",
    //     serde_json::json!({
    //         "id": "writer",
    //         "provider": "operative_file_sink",
    //         "operative_id": "my_echo",
    //         "opts": { "path": "echo-out.txt" },
    //         "hooks": [["on_output", {}]],
    //     }),
    // )?;
    // admin.send("my_echo", "hello world", "in")?;
    //
    // docs/sdk/admin.md has the whole surface. An enforced controller refuses
    // all of it until `flz role bind runner@default app default`.

    runner.await_termination()?;
    Ok(())
}
