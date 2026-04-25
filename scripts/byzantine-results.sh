#!/usr/bin/env bash
# Pretty wrapper for Byzantine / MAC tests and benchmarks (6.5840).
# Usage:
#   ./scripts/byzantine-results.sh           # verbose tests grouped by theme
#   ./scripts/byzantine-results.sh --bench   # also run benchmarks
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/src"

# Ensure real Go module cache is used (Cursor sandbox may override)
export GOMODCACHE="${HOME}/go/pkg/mod"

RUN="TestCompare|TestBaseline|TestByzantine|TestMAC|TestSummary|TestFlip"

echo "=== go test -v ./byzantine (-run '${RUN}') ==="
go test -v -count=1 ./byzantine -run "${RUN}" 2>&1 | sed 's/^/  /'

if [[ "${1:-}" == "--bench" ]]; then
  echo ""
  echo "=== go test -bench -benchmem -run '^$' ./byzantine ==="
  go test -bench='Benchmark' -benchmem -benchtime=500ms -count=1 -run '^$' ./byzantine 2>&1 | sed 's/^/  /'
fi
