# The runner's image: the binary `cargo build --release` builds, on a Debian 12 runtime.
FROM debian:bookworm-slim
COPY target/release/agentdesk /agentdesk/agentdesk
USER 1000
ENTRYPOINT ["/agentdesk/agentdesk"]
