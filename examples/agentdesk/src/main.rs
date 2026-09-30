//! Starts the desk's runner: the process that connects to the controller and
//! serves agentdesk's operators, in the `agents` namespace.

mod admit;
mod agent;
mod budget;
mod deliver;
mod demo_model;
mod gateway;
mod http;
mod reply;
mod request;
mod watch;

use flezha_runner::Flezha;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runner = Flezha::create_runner("agents")?;
    runner
        .register_factory(agent::AgentOperator)?
        .register_factory(gateway::GatewayOperator)?
        .start()?;

    runner.await_termination()?;
    Ok(())
}
