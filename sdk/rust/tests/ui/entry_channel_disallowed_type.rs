// `c.input::<RawTrade>("...")` where RawTrade is not Vec<u8> or String.
// Should fail with the sealed `EntryChannelType` trait-bound error.

use flezha_runner::channel::ChannelBuilder;

struct RawTrade;

fn declare(c: &mut ChannelBuilder) {
    c.input::<RawTrade>("trade.raw");
}

fn main() {}
