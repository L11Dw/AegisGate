#!/usr/bin/env bash
# Breaker open/recovery benchmark scenario.
# Uses a mock backend that returns 500 to trigger breaker.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
echo "=== Breaker Benchmark ==="
"$BENCHMARK_DIR/run.sh" breaker "${1:-1}" "${2:-5}" "${3:-10}" "${4:-3}"
