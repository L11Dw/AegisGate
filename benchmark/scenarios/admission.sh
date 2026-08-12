#!/usr/bin/env bash
# Global admission rate limiting benchmark scenario.
# Uses low rate_limit to trigger 429 rejections.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCHMARK_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
echo "=== Admission Rate Limiting Benchmark ==="
"$BENCHMARK_DIR/run.sh" admission "${1:-1}" "${2:-5}" "${3:-10}" "${4:-3}"
