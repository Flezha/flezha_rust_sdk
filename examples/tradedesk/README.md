# Tradedesk: the Rust project of the tradedesk tutorial

The toy trader the [tradedesk tutorial](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/tradedesk.md) builds, as a
standalone project. Nothing here depends on the Flezha source tree:
`Cargo.toml` names one released version of `flezha_runner`, the way your own
project would.

It holds the desk's two relays and the runner that serves them. The tutorial
writes them one tasklet at a time.

| File | What is in it |
|---|---|
| `src/main.rs` | the runner: this process joins the `trading` namespace and registers both factories |
| `src/trade_watcher.rs` | the ETL stream, a flow relay: the exchange's trade messages in, a cell per symbol, buy signals out |
| `src/track_prices.rs` | keeps each symbol's last ten prices, and compares a new price with their average |
| `src/buy_above_average.rs` | starts an order when a price is 2 % or more above its average |
| `src/order_executor.rs` | the workflow, a steps relay: two steps in order, then two side by side. The order an instance is started with is the `OrderTicket` struct here |
| `src/reserve_funds.rs` | step one: puts the money aside, and releases it again if the order is undone |
| `src/place_order.rs` | step two: places the paper order, and cancels it if the order is undone |
| `src/confirm_fill.rs` | waits ten seconds for the exchange, confirms the fill and ends the order with its result, and refuses an order above the exchange's limit |
| `src/record_order.rs` | books the placed order, and has nothing to undo |

Every tasklet is your own type, with `#[tasklet(name = "...")]` on its
`impl Tasklet` block. What a tasklet remembers, the changes that record it, and
the values it takes and hands on sit in its own file, beside it.

## Run it

You need a controller running, the `trading` namespace created and a key for it
([Open the desk](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/tradedesk/open_the_desk.md) does both).

```sh
FLEZHA_NAMESPACE_SECRET=<trading key> cargo run --release
```

It blocks until shutdown, which is a SIGTERM or Ctrl-C. The namespace is the
first argument and defaults to `trading`: `cargo run --release -- my_desk`.

From another terminal, the controller lists it:

```sh
flz runner list --namespace trading
```

## Start an order by hand

One instance of `order_executor` is one order. `--text` carries the order on the
`order` channel:

```sh
flz relay start order_executor --id order_1 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 10}' --channel order
```

The runner's terminal shows the order being booked. The ids come from
`ctx.stable_id()`, so yours differ:

```
[book] EUR/USD 10 as ord-6b2e13f521b6781afd9f710152cd2f25
```

About ten seconds later, once the exchange has answered, the instance ends on
its own, and `flz relay info` shows how:

```sh
flz relay info order_1 --namespace trading
```

```
...
Relay: order_1
  Run: steps
  Phase: finished
  Status: ok
  Finished At (unix s): 1790583840
```

## Watch it undo itself

The paper exchange fills at most 100 at a time, so `confirm_fill` refuses a
larger order. Start one the same way, with a quantity above the limit:

```sh
flz relay start order_executor --id order_2 --namespace trading \
  --text '{"symbol": "EUR/USD", "quantity": 500}' --channel order
```

`confirm_fill` is tried three times, ten seconds each, and then the order ends
in failure. Every step that had finished is undone, in reverse: the order is
cancelled, then the money is released. `record_order` has no undo, so the
booking stays:

```
[book] EUR/USD 500 as ord-0e7ee9553cfe5eac606fd232fd7ab33c
[flezha] tasklet confirm_fill attempt 1/3 failed (retrying in 30 ms): the exchange fills at most 100 a time, and this is 500
[flezha] tasklet confirm_fill attempt 2/3 failed (retrying in 77 ms): the exchange fills at most 100 a time, and this is 500
[undo] cancelled ord-0e7ee9553cfe5eac606fd232fd7ab33c
[undo] released res-54e4641a002cc27f8d030507d8d5aea5
```

```sh
flz relay info order_2 --namespace trading
```

```
...
Relay: order_2
  Run: steps
  Phase: finished
  Status: error
  Error: tasklet confirm_fill failed: the exchange fills at most 100 a time, and this is 500
  Finished At (unix s): 1790583874
```

## Building from a checkout

To run the project against a Flezha checkout instead of the published crates,
point Cargo at the checkout once, from this folder:

```sh
mkdir -p .cargo
cat > .cargo/config.toml <<'EOF'
[patch.crates-io]
flezha_runner = { path = "../.." }
flz_core = { path = "../../../../crates/flz_core" }
flz_transport = { path = "../../../../crates/flz_transport" }
EOF
```

Then build with `cargo run --release` as before. A copy of this folder outside the
checkout names the checkout's `sdk/rust`, `flz_core` and `flz_transport` instead of
the relative paths. Git ignores the `.cargo` folder; delete it to use the published
crates again.

The empty `[workspace]` table in `Cargo.toml` keeps the repository's own
workspace from claiming this folder, and does nothing in a copy of its own.

## Where to go next

The echo in [`../echo`](../echo/README.md) is the project of the getting-started pages, and
[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what a tasklet, a relay and a runner each are.
