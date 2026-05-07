// `c.channel::<TradeMsg>("...").from::<S>()` where S::Output = Candle.
// Should fail with a clear "expected TradeMsg, found Candle" trait-bound
// error pointing at the `.from::<ProducesCandle>()` call.

use flezha_compute_engine::channel::ChannelBuilder;
use flezha_compute_engine::shard::{Shard, ShardCtx, ShardError};
use flezha_compute_engine::state::{NoChange, RestoreInitPayload, StateHandle, WallClock};

struct TradeMsg;
struct Candle;

#[derive(Default)]
struct ProducesCandle;
impl Shard for ProducesCandle {
    type Input = ();
    type Output = Candle;
    type State = ();
    type Change = NoChange;
    type Clock = WallClock;
    type Config = ();
    fn name() -> &'static str { "produces_candle" }
    fn init_state(_: Option<&str>, _: Option<RestoreInitPayload<'_>>) {}
    fn run(&mut self, _: (), _: StateHandle<'_, Self>, _: &mut ShardCtx<'_>) -> Result<(), ShardError> {
        Ok(())
    }
}

fn declare(c: &mut ChannelBuilder) {
    c.channel::<TradeMsg>("trade.parsed")
        .from::<ProducesCandle>();
}

fn main() {}
