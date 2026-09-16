// `c.input::<RawTrade>("...")` where RawTrade implements neither
// DeserializeOwned nor the passthrough types (`Vec<u8>` / `String`).
// Should fail with the `DeserializeOwned` trait-bound error — a typed
// entry must be decodable at the entry edge.

use flezha_runner::channel::ChannelBuilder;

struct RawTrade;

fn declare(c: &ChannelBuilder) {
    c.input::<RawTrade>("trade.raw");
}

fn main() {}
