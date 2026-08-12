#!/usr/bin/env bash
# Normal keep-alive benchmark scenario.
# Runs the default benchmark with 1, 2, and 4 workers.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Normal Keep-Alive Benchmark ==="
echo ""

for workers in 1 2 4; do
  echo "--- Workers: $workers ---"
  "$BENCHMARK_DIR/run.sh" normal "$workers" 5 10 3
  echo ""
done

echo "=== Complete ==="
echo "Results in: $BENCHMARK_DIR/results/"
