# Echo: the Rust hello world

The Rust hello world, as a standalone project. Nothing here depends on the
Flezha source tree: `Cargo.toml` names one released version of `flezha_runner`,
the way your own project would.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `src/procedure.rs` | the procedure | the processing stage: prefixes each message |
| `src/operative.rs` | the operative | the graph: `in` feeds the procedure, output lands on `out` |
| `src/main.rs` | the runner | this process: offers the operative and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).

```sh
cargo run --release
```

Then, from another terminal:

```sh
flz operative start my_echo echo
flz operative send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It blocks until shutdown, which is a SIGTERM or Ctrl-C. The namespace is the
first argument and defaults to `default`: `cargo run --release -- my_app`.

## Building from a checkout

`flezha_runner` is not on crates.io yet, so Cargo cannot resolve it. From this
folder, point Cargo at the checkout once:

```sh
mkdir -p .cargo
printf '[patch.crates-io]\nflezha_runner = { path = "../.." }\n' > .cargo/config.toml
```

A copy of this folder outside the checkout names the checkout's `sdk/rust`
instead of `../..`. Git ignores the `.cargo` folder, and once the crate is
published the step goes away and `cargo run --release` is all you need.

The empty `[workspace]` table in `Cargo.toml` keeps the repository's own
workspace from claiming this folder, and does nothing in a copy of its own.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is. The other Rust examples, covering state, keys, timers, workflows and the
rest, are the SDK crate's own examples in `sdk/rust/examples/`.
