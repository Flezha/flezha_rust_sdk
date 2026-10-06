//! The runner: connects to the controller and serves the starter relay until
//! shutdown.

mod count;
mod relay;

use flezha_runner::Flezha;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "default".to_string());
    Flezha::create_runner(namespace)?
        .register_factory(relay::StarterRelay)?
        .run()?;
    Ok(())
}
