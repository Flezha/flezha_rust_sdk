//! Starts a runner: a process that connects to the controller and serves your
//! operators.
//!
//! Declare every operator you want served before `start()`. One runner can
//! serve several, and the same call takes a steps operator.

mod operator;
mod tasklet;

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
    // connects to the controller and serves these operators
    runner.register_factory(operator::EchoOperator)?.start()?;

    // Everything `flz operator` and `flz plugin` do is also a method here.
    // Uncomment to drive this namespace from the application instead of from a
    // terminal:
    //
    // let admin = runner.admin();
    // admin.start_operator(&flezha_runner::OperatorSpec::new("echo").id("my_echo"))?;
    // admin.enable_plugin(
    //     "writer",
    //     &flezha_runner::PluginSpec::new("operator_file_sink")
    //         .opts(serde_json::json!({ "path": "echo-out.txt" })),
    // )?;
    // admin.bind_plugin("writer", "my_echo", serde_json::json!([["on_output", {}]]), "")?;
    // admin.send("my_echo", "hello world", "in")?;
    //
    // docs/sdk/admin.md has the whole surface. An enforced controller refuses
    // all of it until `flz role bind runner@default app default`.

    runner.await_termination()?;
    Ok(())
}
