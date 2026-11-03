//! The starter example, built and tested against this checkout: its own
//! `Cargo.toml` names the SDK by its released version on crates.io, so its
//! sources are compiled here instead. Its tasklet's tests run here as they run there.

#[path = "../examples/starter/src/count.rs"]
mod count;
#[path = "../examples/starter/src/relay.rs"]
mod relay;

#[test]
fn the_starter_relay_declares_a_graph_the_runner_accepts() {
    flezha_runner::relay::builder::build_topology_for::<relay::StarterRelay>()
        .expect("the starter's graph");
}
