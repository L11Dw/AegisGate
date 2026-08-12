#!/usr/bin/env bash
# M4-B Benchmark Runner
#
# Usage: ./benchmark/run.sh [scenario] [workers] [runs]
#
# Scenarios:
#   normal       - Normal keep-alive requests
#   slow_client  - Slow client backpressure
#   breaker      - Breaker open/recovery
#   admission    - Global admission rate limiting
#   reload       - Reload during traffic
#
# Requirements:
#   - wrk (preferred) or curl for HTTP benchmarking
#   - Built aegisgate_server and mock backend
#   - SUDO for port binding if needed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
GIT_SHA=$(git -C "$PROJECT_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")
CPU_INFO=$(grep "model name" /proc/cpuinfo 1 | sed 's/.*: //' || echo "unknown")
KERNEL=$(uname -r)

SCENARIO="${1:-normal}"
WORKERS="${2:-1}"
RUNS="${3:-5}"
DURATION="${4:-10}"
WARMUP="${5:-3}"

mkdir -p "$RESULTS_DIR"
RESULT_FILE="$RESULTS_DIR/${SCENARIO}_w${WORKERS}_${TIMESTAMP}.json"

echo "=== AegisGate Benchmark ==="
echo "Scenario: $SCENARIO"
echo "Workers: $WORKERS"
echo "Runs: $RUNS"
echo "Duration: ${DURATION}s per run"
echo "Warmup: ${WARMUP}s"
echo "Git SHA: $GIT_SHA"
echo "CPU: $CPU_INFO"
echo "Kernel: $KERNEL"
echo "Result: $RESULT_FILE"
echo ""

# Build if needed.
if [ ! -f "$PROJECT_DIR/build/release/apps/aegisgate_server" ]; then
  echo "Building release..."
  cmake -S "$PROJECT_DIR" -B "$PROJECT_DIR/build/release" \
    -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF 2>&1 | tail -3
  cmake --build "$PROJECT_DIR/build/release" -j"$(nproc)" 2>&1 | tail -3
fi

SERVER="$PROJECT_DIR/build/release/apps/aegisgate_server"
MOCK="$PROJECT_DIR/build/release/apps/aegisgate_mock_backend"

# Generate config for the scenario.
generate_config() {
  local workers=$1
  local scenario=$2
  cat <<EOF
workers: $workers
routes:
  - name: bench
    host: bench.local
    path_prefix: /
    endpoints:
      - host: 127.0.0.1
        port: 9100
        weight: 1
    rate_limit: 100000
    burst: 1000
    max_inflight: 10000
    retry_budget: 0
EOF
}

# Start mock backend.
start_backend() {
  "$MOCK" --port 9100 --response "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK" &
  BACKEND_PID=$!
  sleep 0.5
}

# Start gateway.
start_gateway() {
  local config_file=$1
  "$SERVER" --config "$config_file" --listen 127.0.0.1:0 --port-file /tmp/aegisgate_bench_port &
  GATEWAY_PID=$!
  # Wait for port file.
  for i in $(seq 1 20); do
    if [ -f /tmp/aegisgate_bench_port ]; then
      GATEWAY_PORT=$(cat /tmp/aegisgate_bench_port)
      break
    fi
    sleep 0.1
  done
  echo "Gateway started on port $GATEWAY_PORT (PID: $GATEWAY_PID)"
}

# Run a single benchmark with curl.
run_curl_benchmark() {
  local duration=$1
  local results_file=$2
  local start_time=$(date +%s%N)
  local end_time=$((start_time + duration * 1000000000))
  local count=0
  local errors=0

  while [ "$(date +%s%N)" -lt "$end_time" ]; do
    if curl -s -o /dev/null -w "%{http_code}" \
        -H "Host: bench.local" \
        "http://127.0.0.1:$GATEWAY_PORT/" 2>/dev/null | grep -q "200"; then
      count=$((count + 1))
    else
      errors=$((errors + 1))
    fi
  done

  local actual_duration=$(( ($(date +%s%N) - start_time) / 1000000 ))
  local rps=$(( count * 1000 / (actual_duration > 0 ? actual_duration : 1) ))

  echo "{\"requests\":$count,\"errors\":$errors,\"duration_ms\":$actual_duration,\"rps\":$rps}"
}

# Run a single benchmark with wrk (if available).
run_wrk_benchmark() {
  local duration=$1
  local threads=$2
  local connections=$3

  if command -v wrk &>/dev/null; then
    wrk -t"$threads" -c"$connections" -d"${duration}s" \
        -H "Host: bench.local" \
        "http://127.0.0.1:$GATEWAY_PORT/" 2>&1
  else
    echo "wrk not available, using curl fallback"
    run_curl_benchmark "$duration" "/dev/null"
  fi
}

# Cleanup.
cleanup() {
  [ -n "${GATEWAY_PID:-}" ] && kill "$GATEWAY_PID" 2>/dev/null || true
  [ -n "${BACKEND_PID:-}" ] && kill "$BACKEND_PID" 2>/dev/null || true
  rm -f /tmp/aegisgate_bench_port
}
trap cleanup EXIT

# Main.
CONFIG_FILE=$(mktemp)
generate_config "$WORKERS" "$SCENARIO" > "$CONFIG_FILE"

start_backend
start_gateway "$CONFIG_FILE"

echo ""
echo "=== Running benchmark ==="

# Collect system metrics.
collect_metrics() {
  local pid=$1
  if [ -d "/proc/$pid" ]; then
    local rss=$(grep VmRSS "/proc/$pid/status" 2>/dev/null | awk '{print $2}' || echo "0")
    local cpu=$(ps -p "$pid" -o %cpu= 2>/dev/null | tr -d ' ' || echo "0")
    echo "{\"rss_kb\":$rss,\"cpu_percent\":$cpu}"
  else
    echo "{\"rss_kb\":0,\"cpu_percent\":0}"
  fi
}

# Warmup.
echo "Warming up for ${WARMUP}s..."
run_curl_benchmark "$WARMUP" "/dev/null" > /dev/null

# Run benchmarks.
echo "Running $RUNS iterations of ${DURATION}s each..."
RUNS_JSON=""
for i in $(seq 1 "$RUNS"); do
  echo "  Run $i/$RUNS..."
  RUN_RESULT=$(run_curl_benchmark "$DURATION" "/dev/null")
  METRICS=$(collect_metrics "$GATEWAY_PID")
  if [ -n "$RUNS_JSON" ]; then
    RUNS_JSON="$RUNS_JSON,$RUN_RESULT"
  else
    RUNS_JSON="$RUN_RESULT"
  fi
done

# Get final metrics.
FINAL_METRICS=$(collect_metrics "$GATEWAY_PID")

# Write JSON result.
cat > "$RESULT_FILE" <<EOF
{
  "git_sha": "$GIT_SHA",
  "cpu": "$CPU_INFO",
  "kernel": "$KERNEL",
  "workers": $WORKERS,
  "scenario": "$SCENARIO",
  "duration_seconds": $DURATION,
  "runs": [$RUNS_JSON],
  "system": $FINAL_METRICS,
  "timestamp": "$(date -Iseconds)"
}
EOF

echo ""
echo "=== Results ==="
echo "Result file: $RESULT_FILE"
cat "$RESULT_FILE"
