#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RUNNER="$ROOT_DIR/benchmark/run.sh"
REPORTER="$ROOT_DIR/benchmark/render-local-report.sh"

usage() {
  cat <<'EOF'
Usage: ./benchmark/run-local-suite.sh [--quick|--full] [--dry-run]

  --quick    Short worker-scaling and concurrency-ladder check plus semantic
             scenarios. Intended to finish in roughly one minute.
  --full     Release matrix (default): 1/2/4 worker scaling at 64 connections,
             then a 1/4/16/64/128/256/512 connection ladder at two workers.
  --dry-run  Print commands only; do not build or start local services.
EOF
}

MODE="full"
DRY_RUN=false
while (($#)); do
  case "$1" in
    --quick) MODE="quick" ;;
    --full) MODE="full" ;;
    --dry-run) DRY_RUN=true ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; exit 2 ;;
  esac
  shift
done

[[ -x "$RUNNER" ]] || { echo "missing benchmark runner: $RUNNER" >&2; exit 1; }
[[ -x "$REPORTER" ]] || { echo "missing report renderer: $REPORTER" >&2; exit 1; }
if ! $DRY_RUN && ! command -v wrk >/dev/null; then
  echo "wrk is required; install it with: sudo apt install wrk" >&2
  exit 1
fi

declare -a LABELS=() CONNECTIONS=() SCENARIOS=() WORKERS=() RUNS=() DURATIONS=() WARMUPS=() BODY_BYTES=()

add_case() {
  LABELS+=("$1")
  CONNECTIONS+=("$2")
  SCENARIOS+=("$3")
  WORKERS+=("$4")
  RUNS+=("$5")
  DURATIONS+=("$6")
  WARMUPS+=("$7")
  BODY_BYTES+=("${8:-0}")
}

add_semantic_cases() {
  add_case "admission_w2" 1 admission 2 1 1 0
  add_case "breaker_w2" 1 breaker 2 1 1 0
  add_case "reload_w2" 1 reload 2 1 1 0
}

add_backpressure_cases() {
  add_case "slow_client_w2_b524288" 1 slow_client 2 1 1 0 524288
  if [[ "$MODE" == "full" ]]; then
    add_case "slow_client_w2_b5242880" 1 slow_client 2 1 1 0 5242880
    add_case "slow_client_w2_b16777216" 1 slow_client 2 1 1 0 16777216
  fi
}

if [[ "$MODE" == "quick" ]]; then
  for workers in 1 2 4 8; do
    add_case "scale_w${workers}_c16" 16 normal "$workers" 1 3 1
  done
  add_case "ladder_w2_c1" 1 normal 2 1 3 1
  add_case "ladder_w2_c64" 64 normal 2 1 3 1
else
  for workers in 1 2 4 8; do
    add_case "scale_w${workers}_c64" 64 normal "$workers" 5 10 3
  done
  for connections in 1 4 16 64 128 256 512; do
    add_case "ladder_w2_c${connections}" "$connections" normal 2 3 10 3
  done
fi
add_semantic_cases
add_backpressure_cases

print_command() {
  local index="$1"
  printf 'AEGISGATE_BENCH_CONNECTIONS=%s ' "${CONNECTIONS[index]}"
  if (( BODY_BYTES[index] > 0 )); then
    printf 'AEGISGATE_BENCH_BODY_BYTES=%s ' "${BODY_BYTES[index]}"
  fi
  printf './benchmark/run.sh %s %s %s %s %s\n' "${SCENARIOS[index]}" "${WORKERS[index]}" "${RUNS[index]}" "${DURATIONS[index]}" "${WARMUPS[index]}"
}

if $DRY_RUN; then
  for index in "${!LABELS[@]}"; do
    printf '%s\t' "${LABELS[index]}"
    print_command "$index"
  done
  exit 0
fi

TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
OUTPUT_DIR="$ROOT_DIR/benchmark/results/local-suite-$TIMESTAMP"
mkdir -p "$OUTPUT_DIR/logs" "$OUTPUT_DIR/results"
{
  echo "timestamp=$(date -Iseconds)"
  echo "git_sha=$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
  if git -C "$ROOT_DIR" diff --quiet && git -C "$ROOT_DIR" diff --cached --quiet; then
  echo "git_dirty=false"
  else
    echo "git_dirty=true"
  fi
  echo "git_diff_sha256=$(git -C "$ROOT_DIR" diff HEAD --binary 2>/dev/null | sha256sum | awk '{print $1}')"
  echo "kernel=$(uname -r)"
  echo "cpu_count=$(nproc)"
  echo "cpu_model=$(grep 'model name' /proc/cpuinfo 2>/dev/null | head -1 | sed 's/.*: //' || echo unknown)"
  echo "wrk=$(wrk --version | head -1)"
  echo "mode=$MODE"
} > "$OUTPUT_DIR/environment.txt"
printf 'label\tconnections\tbody_bytes\tscenario\tworkers\truns\tduration_s\twarmup_s\tresult_json\tlog\n' > "$OUTPUT_DIR/manifest.tsv"
LADDER_CEILING_REACHED=false

run_case() {
  local index="$1" label="${LABELS[index]}" log_file result_path status
  log_file="$OUTPUT_DIR/logs/${label}.log"
  echo "[suite] ${label}: $(print_command "$index")"
  set +e
  AEGISGATE_BENCH_CONNECTIONS="${CONNECTIONS[index]}" AEGISGATE_BENCH_BODY_BYTES="${BODY_BYTES[index]}" "$RUNNER" "${SCENARIOS[index]}" "${WORKERS[index]}" "${RUNS[index]}" "${DURATIONS[index]}" "${WARMUPS[index]}" 2>&1 | tee "$log_file"
  status=${PIPESTATUS[0]}
  set -e
  if ((status != 0)); then
    echo "[suite] failed: ${label}; evidence retained in $OUTPUT_DIR" >&2
    exit "$status"
  fi
  result_path="$(sed -n 's/^\[bench\] result: //p' "$log_file" | tail -1)"
  [[ -n "$result_path" && -f "$result_path" ]] || {
    echo "[suite] no JSON result for ${label}; evidence retained in $OUTPUT_DIR" >&2
    exit 1
  }
  cp "$result_path" "$OUTPUT_DIR/results/${label}.json"
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$label" "${CONNECTIONS[index]}" "${BODY_BYTES[index]}" "${SCENARIOS[index]}" "${WORKERS[index]}" "${RUNS[index]}" "${DURATIONS[index]}" "${WARMUPS[index]}" "results/${label}.json" "logs/${label}.log" >> "$OUTPUT_DIR/manifest.tsv"
  if [[ "$label" == ladder_* && "${SCENARIOS[index]}" == normal ]]; then
    local errors
    errors="$(sed -n 's/^[[:space:]]*"errors":[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$OUTPUT_DIR/results/${label}.json" | head -1)"
    if [[ -n "$errors" && "$errors" != 0 ]]; then
      LADDER_CEILING_REACHED=true
      printf 'stopped_after=%s\nerrors=%s\n' "$label" "$errors" > "$OUTPUT_DIR/load-ceiling.txt"
      echo "[suite] concurrency ladder stopped at $label after $errors errors; semantic cases continue."
    fi
  fi
}

for index in "${!LABELS[@]}"; do
  if $LADDER_CEILING_REACHED && [[ "${LABELS[index]}" == ladder_* && "${SCENARIOS[index]}" == normal ]]; then
    echo "[suite] skipped ${LABELS[index]} after recorded concurrency ceiling"
    continue
  fi
  run_case "$index"
done

"$REPORTER" "$OUTPUT_DIR" >/dev/null
echo "[suite] PASS: local evidence saved in $OUTPUT_DIR"
echo "[suite] report: $OUTPUT_DIR/REPORT.md"
