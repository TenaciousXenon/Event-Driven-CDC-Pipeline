#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
suite=${1:-unit}
if [[ $# -gt 0 ]]; then shift; fi
case "$suite" in
  unit)
    python3 -m unittest discover -s tests/chaos -p 'test_*.py'
    (cd worker && go vet ./... && go test -race -count=1 ./...)
    ;;
  fuzz)
    (cd worker && go test ./processing -run '^$' -fuzz '^FuzzDecodeNeverPanics$' -fuzztime="${FUZZ_TIME:-20s}" -parallel=2 "$@")
    ;;
  integration)
    bash scripts/bootstrap.sh
    bash scripts/smoke.sh
    ;;
  chaos)
    python3 tests/chaos/run.py run "$@"
    ;;
  *) echo 'Usage: scripts/test.sh unit|fuzz|integration|chaos [arguments]' >&2; exit 2 ;;
esac
