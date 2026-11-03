# Starter: a relay that counts visits per page

A small, complete Flezha project in Rust, to grow into your own: one relay, one
tasklet that keeps state, and a test. `Cargo.toml` names the SDK by its
released version, the way your own project does.

| File | What it is |
|---|---|
| `src/count.rs` | the tasklet `count`: keeps a page's total in state, changes it only through the `Counted` change, and answers the new count; its test, four hits with no controller and the replay check, is at the bottom |
| `src/relay.rs` | the relay `starter`: the entry channel `hit`, described and keyed by page, into `count`, out on `counts` |
| `src/main.rs` | the runner: serves the relay until shutdown |

## Run it

You need a controller running and reachable, as
[Start the controller](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md#start-the-controller)
starts one. From this folder, run the test and start the runner:

```sh
cargo test
cargo run --release
```

Then, from another terminal, start the relay and call it:

```sh
flz relay start starter --id starter
flz relay call starter --channel hit --arg page=home
```

```
{"page":"home","total":1}
```

Each call for the same page answers the next total. `flz` is the release's
`bin/flz`. The namespace is the runner's first argument and defaults to
`default`: `cargo run --release -- my_app`.

## Make it yours

Nothing inside the files is renamed for you. Change the package name in
`Cargo.toml`. `starter` is the name `flz relay start` takes. `count` is the name
the tasklet's state is saved under, so choose it before the first run.

## Building from a checkout

To run the project against a Flezha checkout instead of the published crates,
point Cargo at the checkout once, from this folder, with the checkout's absolute
path:

```sh
mkdir -p .cargo
cat > .cargo/config.toml <<'TOML'
[patch.crates-io]
flezha_runner = { path = "/path/to/flezha/sdk/rust" }
flz_core = { path = "/path/to/flezha/crates/flz_core" }
flz_transport = { path = "/path/to/flezha/crates/flz_transport" }
TOML
```

Git ignores the `.cargo` folder; delete it to use the published crates again.
