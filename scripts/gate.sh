#!/usr/bin/env bash
# Run the CI gates locally: format, clippy, tests, rustdoc, all features.
#
# Usage: scripts/gate.sh [extra cargo test args]
# Deps: nix (the flake dev shell provides cargo and protoc).
set -euo pipefail
cd "$(dirname "$0")/.."
nix develop --command bash -c '
  set -euo pipefail
  cargo fmt --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  log=target/gate-test.log
  status=0
  cargo test --workspace --all-features "$@" >"$log" 2>&1 || status=$?
  rg "^test .* FAILED|^thread .* panicked" "$log" || true
  awk '"'"'/^test result/ {p+=$4; f+=$6; i+=$8} END {print "tests: " p " passed, " f " failed, " i " ignored"}'"'"' "$log"
  if [ "$status" -ne 0 ]; then echo "tests failed, see $log"; exit "$status"; fi
  RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace --all-features -q
  echo "gate: ok"
' gate "$@"
