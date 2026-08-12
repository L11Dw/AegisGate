#!/usr/bin/env bash
# M4-B Benchmark Runner
#
# Usage: ./benchmark/run.sh [scenario] [workers] [runs] [duration] [warmup]
#
# Scenarios:
#   normal       - Normal keep-alive requests
#   slow_client  - Slow client backpressure
#   breaker      - Breaker open/recovery
#   admission    - Global admission rate limiting
#   reload       - Reload during traffic
#
# Requirements:
#   - curl for HTTP benchmarking
#   - Built aegisgate_server and aegisgate_mock_backend (Release)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
GIT_SHA=$(git -C "$PROJECT_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")
CPU_INFO=$(grep "model name" /proc/cpuinfo 2>/dev/null | head -1 | sed 's/.*: //' || echo "unknown")
KERNEL=$(uname -r)

SCENARIO="${1:-normal}"
WORKERS="${2:-1}"
RUNS="${3:-5}"
DURATION="${4:-10}"
WARMUP="${5:-3}"

GATEWAY_PORT=0
GATEWAY_PID=
BACKEND_PID=
CONFIG_FILE=

mkdir -p "$RESULTS_DIR"
RESULT_FILE="$RESULTS_DIR/${SCENARIO}_w${WORKERS}_${TIMESTAMP}.json"

log() { echo "[bench] $*"; }

# Build release if needed.
build_release() {
  if [ ! -f "$PROJECT_DIR/build/release/aegisgate_server" ]; then
    log "Building release..."
    cmake -S "$PROJECT_DIR" -B "$PROJECT_DIR/build/release" \
      -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF 2>&1 | tail -3
    cmake --build "$PROJECT_DIR/build/release" -j"$(nproc)" 2>&1 | tail -3
  fi
}

# Generate YAML config.
generate_config() {
  local workers=$1
  local scenario=$2
  local rate_limit=100000
  local burst=1000
  local max_inflight=10000
  local retry_budget=0
  local backend_port=9100

  case "$scenario" in
    admission)
      rate_limit=50
      burst=20
      max_inflight=10
      ;;
    breaker)
      # breaker opens from 500 failures, then recovers.
      ;;
    slow_client)
      # slow client is simulated by the mock backend delay.
      ;;
    reload)
      # reload is triggered by SIGHUP during the benchmark.
      ;;
  esac

  cat <<EOF
workers: $workers
routes:
  - name: bench
    host: bench.local
    path_prefix: /
    endpoints:
      - host: 127.0.0.1
        port: $backend_port
        weight: 1
    rate_limit: $rate_limit
    burst: $burst
    max_inflight: $max_inflight
    retry_budget: $retry_budget
EOF
}

# Start mock backend.
start_backend() {
  local port=9100
  local status="${1:-200}"
  local delay="${2:-0}"
  "$PROJECT_DIR/build/release/aegisgate_mock_backend" "$port" \
    --status "$status" --delay-ms "$delay" &
  BACKEND_PID=$!
  sleep 0.2
  if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
    log "ERROR: mock backend failed to start"
    return 1
  fi
}

# Start gateway on a fixed port.
start_gateway() {
  local config_file=$1
  local log_path="${2:-}"
  local port=8080
  local log_arg=""
  if [ -n "$log_path" ]; then
    log_arg="--log-path $log_path"
  fi
  # Try ports 8080-8090.
  for p in $(seq 8080 8090); do
    # shellcheck disable=SC2086
    "$PROJECT_DIR/build/release/aegisgate_server" "$config_file" "$p" $log_arg &
    GATEWAY_PID=$!
    sleep 0.3
    if kill -0 "$GATEWAY_PID" 2>/dev/null; then
      GATEWAY_PORT=$p
      log "Gateway started on port $GATEWAY_PORT (PID: $GATEWAY_PID)"
      return 0
    fi
  done
  log "ERROR: gateway failed to start on any port"
  return 1
}

# Cleanup on exit.
cleanup() {
  [ -n "${GATEWAY_PID:-}" ] && kill "$GATEWAY_PID" 2>/dev/null || true
  [ -n "${BACKEND_PID:-}" ] && kill "$BACKEND_PID" 2>/dev/null || true
  [ -n "${CONFIG_FILE:-}" ] && rm -f "$CONFIG_FILE"
}
trap cleanup EXIT

# Measure latency with curl (returns percentile estimates).
measure_latency_curl() {
  local count=$1
  local latencies=()
  for _ in $(seq 1 "$count"); do
    local time_ms
    time_ms=$(curl -s --noproxy '*' -o /dev/null -w "%{time_total}" \
      -H "Host: bench.local" \
      "http://127.0.0.1:$GATEWAY_PORT/" 2>/dev/null || echo "1.0")
    local us
    us=$(echo "$time_ms * 1000000" | bc 2>/dev/null | cut -d. -f1 || echo "1000000")
    latencies+=("$us")
  done
  # Sort and compute percentiles.
  IFS=$'\n' sorted=($(sort -n <<<"${latencies[*]}")); unset IFS
  local n=${#sorted[@]}
  local p50_idx=$(( n * 50 / 100 ))
  local p95_idx=$(( n * 95 / 100 ))
  local p99_idx=$(( n * 99 / 100 ))
  local sum=0
  for v in "${sorted[@]}"; do sum=$((sum + v)); done
  local mean=$((sum / n))
  echo "${sorted[$p50_idx]} ${sorted[$p95_idx]} ${sorted[$p99_idx]} $mean"
}

# Run a single benchmark iteration.
run_benchmark() {
  local duration=$1
  local scenario=$2
  local start_time end_time count errors latency_result
  start_time=$(date +%s%N)
  end_time=$((start_time + duration * 1000000000))
  count=0
  errors=0

  while [ "$(date +%s%N)" -lt "$end_time" ]; do
    local code
    code=$(curl -s --noproxy '*' -o /dev/null -w "%{http_code}" \
      -H "Host: bench.local" \
      "http://127.0.0.1:$GATEWAY_PORT/" 2>/dev/null || echo "000")
    if [ "$code" = "200" ]; then
      count=$((count + 1))
    else
      errors=$((errors + 1))
    fi
  done

  local actual_duration_ms=$(( ($(date +%s%N) - start_time) / 1000000 ))
  local rps=0
  if [ "$actual_duration_ms" -gt 0 ]; then
    rps=$(( count * 1000 / actual_duration_ms ))
  fi

  # Measure latency for a sample.
  latency_result=$(measure_latency_curl 50)

  echo "$count $errors $actual_duration_ms $rps $latency_result"
}

# Collect system metrics from /proc.
collect_system_metrics() {
  local pid=$1
  local rss=0 cpu=0 ctx_switches=0
  if [ -d "/proc/$pid" ]; then
    rss=$(grep VmRSS "/proc/$pid/status" 2>/dev/null | awk '{print $2}' || echo "0")
    cpu=$(ps -p "$pid" -o %cpu= 2>/dev/null | tr -d ' ' || echo "0")
    ctx_switches=$(grep -oP 'voluntary_ctxt_switches:\s+\K\d+' "/proc/$pid/status" 2>/dev/null || echo "0")
  fi
  echo "$rss $cpu $ctx_switches"
}

# Main.
log "=== AegisGate Benchmark ==="
log "Scenario: $SCENARIO"
log "Workers: $WORKERS"
log "Runs: $RUNS"
log "Duration: ${DURATION}s per run"
log "Warmup: ${WARMUP}s"
log "Git SHA: $GIT_SHA"
log "CPU: $CPU_INFO"
log "Kernel: $KERNEL"

build_release

CONFIG_FILE=$(mktemp /tmp/aegisgate_bench_XXXXXX.yaml)
generate_config "$WORKERS" "$SCENARIO" > "$CONFIG_FILE"

BENCH_LOG_PATH="$RESULTS_DIR/${SCENARIO}_w${WORKERS}_${TIMESTAMP}.log.jsonl"

# Start scenario-specific backend.
case "$SCENARIO" in
  slow_client)
    log "Starting slow backend (200ms delay)..."
    start_backend 200 200
    ;;
  breaker)
    log "Starting failing backend (500 status)..."
    start_backend 500 0
    ;;
  *)
    start_backend 200 0
    ;;
esac

start_gateway "$CONFIG_FILE" "$BENCH_LOG_PATH"

log "Warming up for ${WARMUP}s..."
run_benchmark "$WARMUP" "$SCENARIO" > /dev/null

log "Running $RUNS iterations of ${DURATION}s each..."
RUNS_JSON=""
RELOAD_COUNT=0
for i in $(seq 1 "$RUNS"); do
  log "  Run $i/$RUNS..."

  # Scenario-specific pre-run actions.
  case "$SCENARIO" in
    reload)
      # Send SIGHUP to trigger reload mid-traffic.
      if [ -n "$GATEWAY_PID" ] && kill -0 "$GATEWAY_PID" 2>/dev/null; then
        kill -HUP "$GATEWAY_PID" 2>/dev/null || true
        RELOAD_COUNT=$((RELOAD_COUNT + 1))
      fi
      ;;
  esac

  RESULT=$(run_benchmark "$DURATION" "$SCENARIO")
  read -r requests errors duration_ms rps p50 p95 p99 mean <<< "$RESULT"
  SYS=$(collect_system_metrics "$GATEWAY_PID")
  read -r rss cpu ctx <<< "$SYS"

  RUN_JSON="{\"requests\":$requests,\"errors\":$errors,\"duration_ms\":$duration_ms,\"rps\":$rps,\"p50_us\":$p50,\"p95_us\":$p95,\"p99_us\":$p99,\"mean_us\":$mean,\"rss_kb\":$rss,\"cpu_percent\":$cpu,\"context_switches\":$ctx}"
  if [ -n "$RUNS_JSON" ]; then
    RUNS_JSON="$RUNS_JSON,$RUN_JSON"
  else
    RUNS_JSON="$RUN_JSON"
  fi
done

# Compute summary statistics.
compute_summary() {
  local field=$1
  shift
  local values=("$@")
  local n=${#values[@]}
  local sum=0
  for v in "${values[@]}"; do sum=$((sum + v)); done
  local mean=$((sum / n))
  local variance=0
  for v in "${values[@]}"; do
    local diff=$((v - mean))
    variance=$((variance + diff * diff))
  done
  local stddev
  stddev=$(echo "sqrt($variance / $n)" | bc 2>/dev/null || echo "0")
  echo "$mean $stddev"
}

# Extract per-run values for summary.
RPS_VALUES=()
P50_VALUES=()
P99_VALUES=()
for i in $(seq 1 "$RUNS"); do
  # Re-parse from the JSON (simplified).
  RPS_VALUES+=("$(echo "$RUNS_JSON" | grep -oP '"rps":\K\d+' | sed -n "${i}p")")
  P50_VALUES+=("$(echo "$RUNS_JSON" | grep -oP '"p50_us":\K\d+' | sed -n "${i}p")")
  P99_VALUES+=("$(echo "$RUNS_JSON" | grep -oP '"p99_us":\K\d+' | sed -n "${i}p")")
done

RPS_SUMMARY=$(compute_summary rps "${RPS_VALUES[@]}")
P50_SUMMARY=$(compute_summary p50 "${P50_VALUES[@]}")
P99_SUMMARY=$(compute_summary p99 "${P99_VALUES[@]}")
read -r rps_mean rps_stddev <<< "$RPS_SUMMARY"
read -r p50_mean p50_stddev <<< "$P50_SUMMARY"
read -r p99_mean p99_stddev <<< "$P99_SUMMARY"

# Read logger stats from the log file line count.
LOG_LINES=0
if [ -f "$BENCH_LOG_PATH" ]; then
  LOG_LINES=$(wc -l < "$BENCH_LOG_PATH" 2>/dev/null || echo "0")
fi
DROPPED_COUNT=0

cat > "$RESULT_FILE" <<EOF
{
  "git_sha": "$GIT_SHA",
  "cpu": "$CPU_INFO",
  "kernel": "$KERNEL",
  "workers": $WORKERS,
  "scenario": "$SCENARIO",
  "duration_seconds": $DURATION,
  "runs": [$RUNS_JSON],
  "summary": {
    "rps_mean": $rps_mean,
    "rps_stddev": $rps_stddev,
    "p50_us_mean": $p50_mean,
    "p50_us_stddev": $p50_stddev,
    "p99_us_mean": $p99_mean,
    "p99_us_stddev": $p99_stddev
  },
  "log_lines_written": $LOG_LINES,
  "log_dropped_count": $DROPPED_COUNT,
  "scenario_details": {
    "reload_count": $RELOAD_COUNT
  },
  "timestamp": "$(date -Iseconds)"
}
EOF

log ""
log "=== Results ==="
log "Result file: $RESULT_FILE"
cat "$RESULT_FILE"
