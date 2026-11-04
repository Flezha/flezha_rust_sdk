# Agentdesk: the Rust project of the agentdesk tutorial

The chat sessions the [agentdesk tutorial](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/agentdesk.md)
builds, as a standalone project. Nothing here depends on the Flezha source tree:
`Cargo.toml` names one released version of `flezha_runner`, the way your own
project would.

Every conversation is a session of its own: an instance of the steps relay `agent`,
which runs one turn per message and keeps the conversation as its state. A turn is
answered by a demo model that lives in this project. It is a stand-in, not a model:
it answers every message with the same sentence, one token at a time, and reports how
many tokens it used. `src/demo_model.rs` is where a real model's client would be called.

In front of the sessions is `gateway`, a flow relay that carries the HTTP
plugin: a POST is a message for a user's session, answered 202 at once, a GET streams
the session's answer as it is written, and each user has a daily token budget.

| File | What is in it |
|---|---|
| `src/main.rs` | the runner: this process joins the `agents` namespace and registers the `agent` and `gateway` factories |
| `src/agent.rs` | a session, a steps relay: one turn per message, and a failed turn fails alone. The `Message` the gateway sends is here |
| `src/reply.rs` | answers a message with the demo model, sends the answer as it grows, and keeps the conversation |
| `src/demo_model.rs` | the demo model |
| `src/gateway.rs` | the HTTP side, a flow relay: it carries the HTTP plugin, answers every request, and calls the session a message is for |
| `src/request.rs` | one HTTP request, as the plugin hands it to the gateway, and the two routes the gateway knows |
| `src/http.rs` | the gateway's answer to a request: a status and a body, or the session to stream |
| `src/watch.rs` | answers a request for a session's events with its stream, and any other request with 404 |
| `src/budget.rs` | keeps the tokens each user's answers used today, and admits a message while they are under the day's budget |
| `src/admit.rs` | answers a message 202 when it goes on to the session, and 429 when its user is over the budget |
| `Dockerfile` | the runner's image: the binary on a Debian 12 runtime |
| `.dockerignore` | keeps everything but the binary out of the runner's image build |
| `controller/Dockerfile` | the controller's image: Flezha's own, with the plugin packages in `controller/plugins/` |
| `compose.yaml` | one controller and one runner, in Docker Compose, with the gateway's HTTP port published on this machine only |
| `cluster.yaml` | a second controller with its runner, and a PostgreSQL database both controllers copy their state to; read on top of `compose.yaml` |
| `k8s/agentdesk.yaml` | the same images on Kubernetes: one pod with the controller and the runner, a volume, and a service |

## Run it

You need Rust with Cargo, and Docker with Docker Compose.

```sh
cargo build --release --target-dir target
docker compose up -d --build --wait
alias flz='docker compose exec controller bin/flz'
```

The runner's image copies the binary Cargo built on this machine; one built against a C
library newer than Debian 12's may not start in it. `--target-dir target` keeps the
binary where the image looks for it, also inside a Flezha checkout, whose Cargo settings
put build output elsewhere. The alias runs `flz` inside the
controller's container. The runner joins `agents` on its own; the controller lists it:

```sh
flz runner list --namespace agents
```

## Talk to a session

Start a session, then send it a turn and wait for its answer:

```sh
flz relay start agent --id anna --namespace agents
flz relay call anna --namespace agents --text 'Hello'
```

```
{"text":"Message 1: you said \"Hello\". I am a demo model.","tokens":10}
```

## Talk to it over HTTP

With the HTTP plugin's package in `controller/plugins/` ([Add your own plugin](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/add_your_own_plugin.md) builds it), start
the gateway, enable the plugin on it, and send a message from this machine:

```sh
flz relay start gateway --id gateway --namespace agents
flz plugin enable web http_server --relay_id=gateway --namespace agents --hook=on_terminate --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http
curl -s -d 'Hello from the web' localhost:8080/sessions/anna/messages
curl -sN localhost:8080/sessions/anna/events
```

The example runs with authorization off, and the plugin authenticates nobody: it is a
demonstration, not a setup to expose.

`docker compose down -v` stops both containers and removes the sessions with their volumes.

## Run it as a cluster, or on Kubernetes

With `RELEASE_COOKIE` and `DURABILITY_PASSWORD` set in the shell,
`COMPOSE_FILE=compose.yaml:cluster.yaml docker compose up -d --build --wait` adds a
second controller; [Run a cluster](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/run_a_cluster.md)
walks it. [Run on Kubernetes](https://github.com/flezha/flezha/blob/main/docs/public/tutorials/agentdesk/run_on_kubernetes.md)
builds the images and deploys `k8s/agentdesk.yaml` on k3s.

## Building from a checkout

Docker pulls the controller's image `flezha/flezha:0.1.1` from Docker Hub; the
commands below build it from the checkout instead.
To run the project
against a Flezha checkout instead of the published crates, also point Cargo at the
checkout once, from this folder:

```sh
mkdir -p .cargo
cat > .cargo/config.toml <<'EOF'
[patch.crates-io]
flezha_runner = { path = "../.." }
flz_core = { path = "../../../../crates/flz_core" }
flz_transport = { path = "../../../../crates/flz_transport" }
EOF
```

Then build with `cargo build --release --target-dir target` as before. Build the
controller's image from the repository root:

```sh
docker build -f deploy/docker/prod/Dockerfile.controller -t flezha/flezha:0.1.1 .
```

Git ignores the `.cargo` folder; delete it to use the published crates again. The empty
`[workspace]` table in `Cargo.toml` keeps the repository's own workspace from
claiming this folder, and does nothing in a copy of its own.

## Where to go next

The tradedesk in [`../tradedesk`](../tradedesk/README.md) is the tradedesk tutorial's project, and
the [calls contract](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/calls.md) is what `flz relay call`
follows.
