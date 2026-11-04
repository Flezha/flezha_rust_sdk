#!/usr/bin/env bash
# Fetch the two things this SDK takes from a core release rather than from the
# core's git tag: the fake controller and the contract set.
#
# The SDK compiles the core itself, as a git dependency at the tag its manifest
# names. What it cannot compile is the fake controller its smoke tier runs
# against and the shared fixtures its tests read, so those come from the
# release at the same version, each checked against the release's SHA-256
# manifest before it is used.
#
#   scripts/fetch-core.sh --from <dir|url> [--version 0.1.1]
#
# `--from` is a staged release directory (what `scripts/build-dist.sh
# --stage-release` writes in the core's tree) or the URL a GitHub release
# serves its assets from. Everything it installs is build output and is ignored
# by git: the fake controller at `.core/`, and the contract set at `contract/`.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

FROM=""
VERSION="0.1.1"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --from)    FROM="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

refuse() {
  echo "fetch-core: $*" >&2
  exit 1
}

[[ -n "${FROM}" ]] || refuse "--from <dir|url> is required"
command -v sha256sum >/dev/null 2>&1 || refuse "sha256sum is required"

# The platform a core release names its per-platform assets by, the directory
# column of the core's `scripts/native_triples.sh`.
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64)               platform=linux-x64 ;;
  Linux-aarch64|Linux-arm64)  platform=linux-arm64 ;;
  Darwin-x86_64)              platform=macos-x64 ;;
  Darwin-arm64)               platform=macos-arm64 ;;
  *) refuse "no core release asset is published for $(uname -s) $(uname -m)" ;;
esac

scratch=.core/download
rm -rf "${scratch}"
mkdir -p "${scratch}"

# One asset's bytes, from a directory or a release URL.
asset() {
  local name="$1" into="$2"
  if [[ "${FROM}" =~ ^https?:// ]]; then
    curl -fsSL "${FROM%/}/${name}" -o "${into}" || refuse "${name}: not downloadable from ${FROM}"
  else
    [[ -f "${FROM}/${name}" ]] || refuse "${FROM}/${name} is not there"
    cp "${FROM}/${name}" "${into}"
  fi
}

# One asset, checked against the manifest and moved to `into` only if it matches.
verified() {
  local name="$1" into="$2" expected actual
  expected="$(awk -v name="${name}" '$2 == name || $2 == "*" name { print $1 }' "${scratch}/SHA256SUMS.txt")"
  [[ -n "${expected}" ]] || refuse "${name} is not in SHA256SUMS.txt; it was not part of this release"
  asset "${name}" "${scratch}/${name}"
  actual="$(sha256sum "${scratch}/${name}" | awk '{ print $1 }')"
  [[ "${actual}" == "${expected}" ]] || refuse "${name} does not match its manifest digest"
  mkdir -p "$(dirname "${into}")"
  mv "${scratch}/${name}" "${into}"
  echo "  verified ${name} -> ${into}"
}

echo "fetch-core: taking the ${VERSION} core from ${FROM}"
asset SHA256SUMS.txt "${scratch}/SHA256SUMS.txt"

verified "flz-fake-controller-${platform}" .core/flz-fake-controller
chmod +x .core/flz-fake-controller

verified "flezha-fixtures-${VERSION}.tar.gz" "${scratch}/fixtures.tar.gz"
rm -rf contract
mkdir -p contract
tar -xzf "${scratch}/fixtures.tar.gz" -C contract
echo "  unpacked the contract set -> contract/"

rm -rf "${scratch}"
echo "fetch-core: done"
