#!/usr/bin/env bash
# Contract test for the local benchmark-suite command planner.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SUITE="$ROOT_DIR/benchmark/run-local-suite.sh"
REPORTER="$ROOT_DIR/benchmark/render-local-report.sh"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

[[ -x "$SUITE" ]] || fail "missing executable $SUITE"
[[ -x "$REPORTER" ]] || fail "missing executable $REPORTER"
grep -Fq 'REPORT.md' "$SUITE" ||
  fail "suite must produce a readable REPORT.md beside raw evidence"
grep -Fq '"$REPORTER" "$OUTPUT_DIR"' "$SUITE" ||
  fail "suite must render a report from its copied JSON evidence"
grep -Fq 'rate_limit: 1000000' "$ROOT_DIR/benchmark/run.sh" ||
  fail "normal benchmark baseline must not be rate limited"
grep -Fq 'burst: 1000000' "$ROOT_DIR/benchmark/run.sh" ||
  fail "normal benchmark baseline must lease capacity across workers"
grep -Fq "s/rate_limit: 1000000/rate_limit: 10/" "$ROOT_DIR/benchmark/run.sh" ||
  fail "admission scenario must override the normal benchmark rate limit"
grep -Fq "s/burst: 1000000/burst: 5/" "$ROOT_DIR/benchmark/run.sh" ||
  fail "admission scenario must override the normal benchmark burst"
grep -Fq 'kBackpressureAssertionBytes=$((5 * 1024 * 1024))' "$ROOT_DIR/benchmark/run.sh" ||
  fail "small responses must not be treated as deterministic backpressure probes"
grep -Fq 'git_dirty' "$ROOT_DIR/benchmark/run.sh" || fail "result must record dirty-worktree provenance"
grep -Fq 'non2xx_responses' "$ROOT_DIR/benchmark/run.sh" || fail "result must classify HTTP errors"
grep -Fq 'socket_errors' "$ROOT_DIR/benchmark/run.sh" || fail "result must classify socket errors"
grep -Fq 'git_diff_sha256' "$ROOT_DIR/benchmark/run.sh" || fail "result must record the complete diff fingerprint"
grep -Fq 'p50_us_median' "$ROOT_DIR/benchmark/run.sh" || fail "result must summarize percentile min/median/max"
grep -Fq 'status_429' "$ROOT_DIR/benchmark/run.sh" || fail "result must classify 429 responses"
grep -Fq 'status_5xx' "$ROOT_DIR/benchmark/run.sh" || fail "result must classify 5xx responses"
before_dirs=$(find "$ROOT_DIR/benchmark/results" -maxdepth 1 -type d -name 'local-suite-*' 2>/dev/null | wc -l)
output=$("$SUITE" --dry-run --quick)
grep -Fq 'AEGISGATE_BENCH_CONNECTIONS=16 ./benchmark/run.sh normal 1 1 3 1' <<<"$output" ||
  fail "missing quick worker-1 baseline"
grep -Fq 'AEGISGATE_BENCH_CONNECTIONS=16 ./benchmark/run.sh normal 2 1 3 1' <<<"$output" ||
  fail "missing quick worker-2 baseline"
grep -Fq 'AEGISGATE_BENCH_CONNECTIONS=16 ./benchmark/run.sh normal 4 1 3 1' <<<"$output" ||
  fail "missing quick worker-4 baseline"
grep -Fq 'AEGISGATE_BENCH_CONNECTIONS=16 ./benchmark/run.sh normal 8 1 3 1' <<<"$output" ||
  fail "missing quick worker-8 baseline"
grep -Fq 'AEGISGATE_BENCH_CONNECTIONS=1 ./benchmark/run.sh normal 2 1 3 1' <<<"$output" ||
  fail "missing quick concurrency-1 ladder point"
grep -Fq 'AEGISGATE_BENCH_CONNECTIONS=64 ./benchmark/run.sh normal 2 1 3 1' <<<"$output" ||
  fail "missing quick concurrency-64 ladder point"
grep -Fq 'AEGISGATE_BENCH_BODY_BYTES=524288 ./benchmark/run.sh slow_client 2 1 1 0' <<<"$output" ||
  fail "missing 512KiB backpressure case"
grep -Fq './benchmark/run.sh admission 2 1 1 0' <<<"$output" || fail "missing admission scenario"
grep -Fq './benchmark/run.sh breaker 2 1 1 0' <<<"$output" || fail "missing breaker scenario"
grep -Fq './benchmark/run.sh slow_client 2 1 1 0' <<<"$output" || fail "missing slow-client scenario"
grep -Fq './benchmark/run.sh reload 2 1 1 0' <<<"$output" || fail "missing reload scenario"

full_output=$("$SUITE" --dry-run --full)
grep -Fq 'AEGISGATE_BENCH_CONNECTIONS=512 ./benchmark/run.sh normal 2 3 10 3' <<<"$full_output" ||
  fail "missing full concurrency-512 ladder point"
grep -Fq 'AEGISGATE_BENCH_BODY_BYTES=5242880 ./benchmark/run.sh slow_client 2 1 1 0' <<<"$full_output" ||
  fail "missing 5MiB backpressure case"
grep -Fq 'AEGISGATE_BENCH_BODY_BYTES=16777216 ./benchmark/run.sh slow_client 2 1 1 0' <<<"$full_output" ||
  fail "missing 16MiB backpressure case"

after_dirs=$(find "$ROOT_DIR/benchmark/results" -maxdepth 1 -type d -name 'local-suite-*' 2>/dev/null | wc -l)
[[ "$before_dirs" = "$after_dirs" ]] || fail "dry-run created a local-suite directory"

echo "PASS: local benchmark-suite command contract"
