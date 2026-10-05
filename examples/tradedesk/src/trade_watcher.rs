use flezha_runner::{ChannelBuilder, Relay};
use serde::Deserialize;

use crate::buy_above_average::BuyAboveAverage;
use crate::track_prices::{TrackPrices, Trade};

/// One message from the exchange: `channel` says what it is, and a trade
/// message carries one or more trades in `data`. The exchange's other messages,
/// such as a subscription's confirmation, have neither field, so both default
/// to empty rather than failing to decode. Fields the struct does not name are
/// ignored.
#[derive(Clone, Deserialize)]
pub struct TradeMessage {
    #[serde(default)]
    channel: String,
    #[serde(default)]
    data: Vec<Trade>,
}

/// The desk's stream: trades in, each price compared with its symbol's recent
/// average, and an order started when a price jumps far enough above it.
///
/// A flow relay runs for as long as the desk does. The exchange sends
/// other messages beside its trades and delivers several trades in one message,
/// so the wiring keeps the trade messages, splits each into its trades before
/// anything else sees them, drops what the desk does not follow, and routes the
/// rest to a cell per symbol.
pub struct TradeWatcherRelay;

impl Relay for TradeWatcherRelay {
    fn factory_id() -> &'static str {
        "trade_watcher"
    }

    /// Four transforms on the wiring, then two tasklets, and one place output
    /// leaves the desk.
    ///
    /// The transforms run at the routing seam rather than as tasklets of their
    /// own: no cell, no mailbox, no dispatch hop, which is why they must stay
    /// cheap and free of side effects. `key_by` names the cell each trade
    /// belongs to, and has to be the last link before `into`, because it is
    /// evaluated on whatever the transforms above it produced. Only the first
    /// stage is keyed: a plain `into` leaves its consumer single-cell, which is
    /// right for a stage that keeps nothing per symbol.
    fn channels(c: &ChannelBuilder) {
        c.input::<TradeMessage>("trades") // JSON from the exchange
            .filter(|message| message.channel == "trade") // trade messages only
            .flat_map(|message| message.data) // a message into its trades
            .filter(|trade| trade.ord_type == "market") // market orders only
            .key_by(|trade| trade.symbol.clone()) // one cell per symbol
            .into::<TrackPrices>()
            .into::<BuyAboveAverage>() // fed by the tasklet above
            .output_named("signals"); // out to the file plugin
    }
}
