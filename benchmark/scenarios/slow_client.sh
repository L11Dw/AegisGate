#!/usr/bin/env bash
# Slow client backpressure benchmark scenario.
# Uses a mock backend with delay to simulate slow responses.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
echo "=== Slow Client Benchmark ==="
"$BENCHMARK_DIR/run.sh" slow_client "${1:-1}" "${2:-5}" "${3:-10}" "${4:-3}"
