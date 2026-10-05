//! Starts the desk's runner: the process that connects to the controller and
//! serves tradedesk's relays.
//!
//! The namespace is the first argument and defaults to `trading`. A runner
//! serves one namespace, and at most one runner may run per process. Under an
//! enforced controller it proves its namespace with that namespace's key, read
//! from `FLEZHA_NAMESPACE_SECRET`.
//!
//! The guides write the desk's relays one at a time, and each is registered
//! here before `start()`. Registering a factory starts nothing: it offers the
//! controller relays of that type, which are started from it later.
//! `register_factory` is the same call for both kinds, the stream that tracks
//! prices and the steps relay that places one order.

mod buy_above_average;
mod confirm_fill;
mod order_executor;
mod place_order;
mod record_order;
mod reserve_funds;
mod track_prices;
mod trade_watcher;

use flezha_runner::Flezha;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let namespace = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "trading".to_string());

    let runner = Flezha::create_runner(namespace)?;
    runner
        .register_factory(trade_watcher::TradeWatcherRelay)?
        .register_factory(order_executor::OrderExecutorRelay)?
        .start()?;

    runner.await_termination()?;
    Ok(())
}
