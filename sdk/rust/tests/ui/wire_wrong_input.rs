// `c.channel::<TradeMsg>("...").from::<S>().into::<C>()` where C::Input =
// Candle. Should fail with a clear "expected TradeMsg, found Candle"
// trait-bound error pointing at the `.into::<ConsumesCandle>()` call.

use flezha_compute_engine::channel::ChannelBuilder;
use flezha_compute_engine::shard::{Shard, ShardCtx, ShardError};

#[derive(Clone)]
struct TradeMsg;
#[derive(Clone)]
struct Candle;

#[derive(Default)]
struct ProducesTrade;
impl Shard for ProducesTrade {
    type Input = ();
    type Output = TradeMsg;
    type State = ();
    fn name() -> &'static str { "produces_trade" }
    fn init_state(_: Option<&str>) {}
    fn run(&mut self, _: (), _: &mut (), _: &mut ShardCtx<'_>) -> Result<(), ShardError> {
        Ok(())
    }
}

#[derive(Default)]
struct ConsumesCandle;
impl Shard for ConsumesCandle {
    type Input = Candle;
    type Output = ();
    type State = ();
    fn name() -> &'static str { "consumes_candle" }
    fn init_state(_: Option<&str>) {}
    fn run(&mut self, _: Candle, _: &mut (), _: &mut ShardCtx<'_>) -> Result<(), ShardError> {
        Ok(())
    }
}

fn declare(c: &mut ChannelBuilder) {
    c.channel::<TradeMsg>("trade.parsed")
        .from::<ProducesTrade>()
        .into::<ConsumesCandle>();
}

fn main() {}
