#!/usr/bin/env bash
# The three tiers this repository runs, and no controller anywhere.
#
#   unit        the three crates' own suites: the macros, the SDK (with the
#               features its integration tests need) and the reference runner
#   local mode  the declared graph run in process, part of the SDK's suite
#   smoke       the fake controller on a socket directory, the reference runner
#               launched against it, and the binary's exit code
#
# The SDK compiles the core itself, at the git tag its manifest names. The fake
# controller and the contract set come from the core release at the same
# version, checked against that release's manifest.
#
#   scripts/ci.sh --core <dir|url> [--version 0.1.0]
#
# What this cannot see is the fifteen-capability vocabulary the controller
# owns: persistence, effectively-once, correction, partitioning, workflows,
# liveness, restarts. Those are end-to-end scenarios and they run in the core's
# own tree, which boots this repository's reference runner in every suite.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

CORE=""
VERSION="0.1.0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --core)    CORE="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done
[[ -n "${CORE}" ]] || { echo "--core <dir|url> is required" >&2; exit 1; }

step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

# The crates share no workspace, so they would otherwise build the core three
# times over.
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-${PWD}/target}"

step "fetching the core ${VERSION}"
scripts/fetch-core.sh --from "${CORE}" --version "${VERSION}"

step "unit and local mode"
cargo test --quiet --manifest-path macros/Cargo.toml
# `testing` is not optional: six integration tests import
# `flezha_runner::testing`, which the feature gates.
cargo test --quiet --manifest-path Cargo.toml --features bench-internals,testing --lib --tests
cargo test --quiet --manifest-path test_app/Cargo.toml

step "building the reference runner"
cargo build --quiet --manifest-path test_app/Cargo.toml

step "smoke"
# The runner reads the same socket directory the fake controller binds in, and
# the connect fixture is what pins the variable's name.
SOCKETS="$(mktemp -d)"
INPUTS="${SOCKETS}/inputs.txt"
printf 'alpha\nbeta\n' > "${INPUTS}"
export FLEZHA_SOCKET_FILES_PATH="${SOCKETS}"
export FLEZHA_TRANSPORT=uds

# The runner ends at end of stream on its standard input, so it is held open
# for as long as the tier runs and closed when it is over.
mkfifo "${SOCKETS}/stdin"
sleep 120 > "${SOCKETS}/stdin" &
HOLD=$!
"${CARGO_TARGET_DIR}/debug/rust_runner" smoke > "${SOCKETS}/runner.log" 2>&1 < "${SOCKETS}/stdin" &
RUNNER=$!

set +e
.core/flz-fake-controller \
  --socket-dir "${SOCKETS}" \
  --factory do_nothing --operative echo-1 --channel input \
  --inputs "${INPUTS}" --expect-outputs 2 --timeout-ms 60000
CODE=$?
set -e

kill "${HOLD}" "${RUNNER}" 2>/dev/null || true
if (( CODE != 0 )); then
  echo "the runner's own log:"
  sed 's/^/  /' "${SOCKETS}/runner.log"
fi
rm -rf "${SOCKETS}"

# The tier's verdict is the binary's, never the runner's: a terminal
# disconnect ends the runner non-zero on purpose, so the orchestrator that owns
# it restarts it.
exit "${CODE}"
