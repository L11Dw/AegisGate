#!/usr/bin/env bash
# Reload during traffic benchmark scenario.
# Sends SIGHUP to trigger reload while traffic is flowing.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
echo "=== Reload During Traffic Benchmark ==="
"$BENCHMARK_DIR/run.sh" reload "${1:-1}" "${2:-5}" "${3:-10}" "${4:-3}"
