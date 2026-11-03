# Echo: the Rust hello world

The Rust hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `Cargo.toml` names one released version of `flezha_runner`,
the way your own project would.

Three files, one for each of the [building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md):

| File | Part | What it does |
|---|---|---|
| `src/tasklet.rs` | the tasklet | the processing stage: prefixes each message |
| `src/relay.rs` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `src/main.rs` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([Up and running](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md)).

```sh
cargo run --release
```

Then, from another terminal:

```sh
flz relay start echo --id my_echo
flz relay send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It blocks until shutdown, which is a SIGTERM or Ctrl-C. The namespace is the
first argument and defaults to `default`: `cargo run --release -- my_app`.

## Building from a checkout

To run the example against a Flezha checkout instead of the published crates,
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
checkout names the checkout's `sdk/rust`, `flz_core` and `flz_transport` instead of the
relative paths. Git ignores the `.cargo` folder; delete it to use the published crates
again.

The empty `[workspace]` table in `Cargo.toml` keeps the repository's own
workspace from claiming this folder, and does nothing in a copy of its own.

## Where to go next

[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what each of the three parts is and why the split is the way it
is. The other Rust examples, covering state, keys, timers, steps relays and the
rest, are the SDK crate's own examples in `sdk/rust/examples/`.
