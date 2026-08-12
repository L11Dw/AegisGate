#!/usr/bin/env bash
# M4-B reproducible benchmark and scenario verification harness.
# Usage: ./benchmark/run.sh [normal|admission|breaker|slow_client|reload] [workers] [runs] [duration] [warmup]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"
SCENARIO="${1:-normal}"
WORKERS="${2:-1}"
RUNS="${3:-5}"
DURATION="${4:-10}"
WARMUP="${5:-3}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
GIT_SHA="$(git -C "$PROJECT_DIR" rev-parse --short HEAD 2>/dev/null || echo unknown)"
CPU_INFO="$(grep "model name" /proc/cpuinfo 2>/dev/null | head -1 | sed 's/.*: //' || echo unknown)"
KERNEL="$(uname -r)"
BACKEND_PORT=9100
RELOAD_BACKEND_PORT=9101
GATEWAY_PORT=0
GATEWAY_PID=""
BACKEND_PID=""
BACKEND2_PID=""
CONFIG_FILE=""
BENCH_LOG_PATH=""
RESULT_FILE=""

case "$SCENARIO" in normal|admission|breaker|slow_client|reload) ;; *)
  echo "unknown scenario: $SCENARIO" >&2; exit 2 ;; esac

mkdir -p "$RESULTS_DIR"
RESULT_FILE="$RESULTS_DIR/${SCENARIO}_w${WORKERS}_${TIMESTAMP}.json"

log() { echo "[bench] $*"; }
die() { log "ERROR: $*"; exit 1; }
now_ms() { echo $(( $(date +%s%N) / 1000000 )); }

stop_and_wait() {
  local pid="$1"
  [ -n "$pid" ] || return 0
  if kill -0 "$pid" 2>/dev/null; then kill -TERM "$pid" 2>/dev/null || true; fi
  wait "$pid" 2>/dev/null || true
}

cleanup() {
  stop_and_wait "$GATEWAY_PID"
  stop_and_wait "$BACKEND_PID"
  stop_and_wait "$BACKEND2_PID"
  [ -n "$CONFIG_FILE" ] && rm -f "$CONFIG_FILE" "${CONFIG_FILE}.next" || true
}
trap cleanup EXIT

build_release() {
  # Always invoke the incremental build.  Skipping it when the executable
  # exists can silently benchmark an older binary after a source change.
  cmake -S "$PROJECT_DIR" -B "$PROJECT_DIR/build/release" -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
  cmake --build "$PROJECT_DIR/build/release" -j"$(nproc)"
}

write_config() {
  local path="$1" workers="$2" port="$3" mode="$4"
  cat > "$path" <<EOF
workers: $workers
routes:
  - name: bench
    host: bench.local
    path_prefix: /
    endpoints:
      - host: 127.0.0.1
        port: $port
        weight: 1
    rate_limit: 100000
    burst: 1000
    max_inflight: 10000
    retry_budget: 0
EOF
  case "$mode" in
    admission)
      sed -i 's/rate_limit: 100000/rate_limit: 10/; s/burst: 1000/burst: 5/; s/max_inflight: 10000/max_inflight: 32/' "$path"
      ;;
    breaker)
      cat >> "$path" <<'EOF'
    circuit_breaker:
      window_seconds: 10
      min_requests: 2
      failure_threshold: 0.5
      open_seconds: 1
      half_open_probes: 1
EOF
      ;;
  esac
}

request_code() {
  curl --noproxy '*' --connect-timeout 1 --max-time 5 -sS -o /dev/null -w '%{http_code}' \
    -H 'Host: bench.local' "http://127.0.0.1:${GATEWAY_PORT}/" 2>/dev/null || printf '000'
}

metrics_text() {
  curl --noproxy '*' --connect-timeout 1 --max-time 2 -sS \
    "http://127.0.0.1:${GATEWAY_PORT}/metrics" 2>/dev/null || true
}

wait_until() {
  local timeout_ms="$1" description="$2" command="$3"
  local deadline=$(( $(now_ms) + timeout_ms ))
  while [ "$(now_ms)" -lt "$deadline" ]; do
    if eval "$command"; then return 0; fi
    sleep 0.05
  done
  die "timed out waiting for $description"
}

wait_gateway_ready() {
  # Health-check the gateway's local metrics endpoint.  Sending a routed
  # request here would mutate admission and breaker state before a semantic
  # scenario begins (notably consuming one of the deliberate 500s).
  wait_until 5000 "gateway readiness" '[ -n "$(metrics_text)" ]'
}

start_backend() {
  local target_var="$1" port="$2" status="$3" body_bytes="${4:-0}"
  local -a args=("$PROJECT_DIR/build/release/aegisgate_mock_backend" "$port" --status "$status")
  if [ "$body_bytes" -gt 0 ]; then args+=(--body-bytes "$body_bytes"); fi
  "${args[@]}" &
  local pid=$!
  wait_until 3000 "mock backend on $port" \
    "backend_code=\$(curl --noproxy '*' --connect-timeout 1 --max-time 1 -sS -o /dev/null -w '%{http_code}' http://127.0.0.1:$port/ 2>/dev/null || printf 000); [ \"\$backend_code\" != 000 ]"
  printf -v "$target_var" '%s' "$pid"
}

start_gateway() {
  local -a args=("$PROJECT_DIR/build/release/aegisgate_server" "$CONFIG_FILE")
  BENCH_LOG_PATH="$RESULTS_DIR/${SCENARIO}_w${WORKERS}_${TIMESTAMP}.log.jsonl"
  for port in $(seq 8080 8090); do
    "${args[@]}" "$port" --log-path "$BENCH_LOG_PATH" &
    GATEWAY_PID=$!
    GATEWAY_PORT="$port"
    if wait_until 1500 "gateway process on $port" "kill -0 $GATEWAY_PID 2>/dev/null" &&
       wait_gateway_ready; then
      log "gateway ready on $GATEWAY_PORT (pid $GATEWAY_PID)"
      return
    fi
    stop_and_wait "$GATEWAY_PID"
    GATEWAY_PID=""
  done
  die "gateway did not become ready on ports 8080-8090"
}

count_metric() {
  local name="$1"
  metrics_text | awk -v key="$name" '$1 == key { print $2; exit }'
}

breaker_is() {
  local state="$1"
  metrics_text | grep -q "aegisgate_circuit_state{route=\"bench\".*state=\"${state}\"} 1"
}

measure_latency_us() {
  local samples=30 total=0 value
  local -a values=()
  for _ in $(seq 1 "$samples"); do
    value=$(curl --noproxy '*' --connect-timeout 1 --max-time 5 -sS -o /dev/null -w '%{time_total}' \
      -H 'Host: bench.local' "http://127.0.0.1:${GATEWAY_PORT}/" 2>/dev/null || printf '1')
    value=$(awk -v seconds="$value" 'BEGIN { printf "%d", seconds * 1000000 }')
    values+=("$value")
  done
  IFS=$'\n' values=($(sort -n <<<"${values[*]}")); unset IFS
  for value in "${values[@]}"; do total=$((total + value)); done
  local n=${#values[@]}
  printf '%s %s %s %s' "${values[$((n / 2))]}" "${values[$((n * 95 / 100))]}" \
    "${values[$((n * 99 / 100))]}" "$((total / n))"
}

run_sequential_load() {
  local seconds="$1" expected="$2"
  local deadline=$(( $(now_ms) + seconds * 1000 ))
  local accepted=0 rejected=0 failures=0 code
  while [ "$(now_ms)" -lt "$deadline" ]; do
    code=$(request_code)
    if [ "$code" = "$expected" ]; then accepted=$((accepted + 1));
    elif [ "$code" = 429 ]; then rejected=$((rejected + 1));
    else failures=$((failures + 1)); fi
  done
  printf '%s %s %s' "$accepted" "$rejected" "$failures"
}

run_parallel_burst() {
  local requests="$1" dir
  dir=$(mktemp -d /tmp/aegisgate_admission_XXXXXX)
  local -a pids=()
  for index in $(seq 1 "$requests"); do
    (request_code > "$dir/$index") & pids+=("$!")
  done
  for pid in "${pids[@]}"; do wait "$pid"; done
  local accepted rejected failures
  accepted=$(awk '$0 == "200" { ++count } END { print count + 0 }' "$dir"/*)
  rejected=$(awk '$0 == "429" { ++count } END { print count + 0 }' "$dir"/*)
  failures=$((requests - accepted - rejected))
  rm -rf "$dir"
  printf '%s %s %s' "$accepted" "$rejected" "$failures"
}

run_normal() {
  run_sequential_load "$WARMUP" 200 >/dev/null
  local requests=0 errors=0 duration_ms=0 rps=0 p50=0 p95=0 p99=0 mean=0 start end result
  for _ in $(seq 1 "$RUNS"); do
    start=$(now_ms); result=$(run_sequential_load "$DURATION" 200); end=$(now_ms)
    read -r accepted rejected failures <<< "$result"
    requests=$((requests + accepted)); errors=$((errors + rejected + failures))
    duration_ms=$((duration_ms + end - start))
  done
  [ "$requests" -gt 0 ] || die "normal scenario observed no successful response"
  read -r p50 p95 p99 mean <<< "$(measure_latency_us)"
  rps=$(( requests * 1000 / duration_ms ))
  SCENARIO_DETAILS="\"accepted\":$requests,\"errors\":$errors"
  print_result "$requests" "$errors" "$duration_ms" "$rps" "$p50" "$p95" "$p99" "$mean"
}

run_admission() {
  # Give the coordinator's first refill tick a bounded chance to populate the
  # small global budget; this is benchmark setup, not request timing.
  sleep 1
  read -r accepted rejected failures <<< "$(run_parallel_burst 64)"
  [ "$accepted" -gt 0 ] || die "admission scenario observed no admitted request"
  [ "$rejected" -gt 0 ] || die "admission scenario observed no 429 rejection"
  local p50 p95 p99 mean
  read -r p50 p95 p99 mean <<< "$(measure_latency_us)"
  SCENARIO_DETAILS="\"accepted\":$accepted,\"rate_limited\":$rejected,\"other_failures\":$failures,\"configured_rate_limit\":10,\"configured_burst\":5"
  print_result "$accepted" "$((rejected + failures))" 0 0 "$p50" "$p95" "$p99" "$mean"
}

run_breaker() {
  local failures=0 code
  for _ in 1 2 3 4; do code=$(request_code); [ "$code" = 500 ] && failures=$((failures + 1)); done
  [ "$failures" -ge 2 ] || die "breaker setup did not receive two backend failures"
  wait_until 3000 "breaker open" 'breaker_is open'
  stop_and_wait "$BACKEND_PID"; BACKEND_PID=""
  start_backend BACKEND_PID "$BACKEND_PORT" 200
  wait_until 5000 "breaker recovery" 'code=$(request_code); [ "$code" = 200 ] && breaker_is closed'
  SCENARIO_DETAILS="\"failure_responses_before_open\":$failures,\"final_breaker_state\":\"closed\""
  print_result "$failures" 0 0 0 0 0 0 0
}

slow_reader() {
  local body_bytes="$1" ready_file="$2" result_file="$3"
  exec 3<>"/dev/tcp/127.0.0.1/${GATEWAY_PORT}"
  printf 'GET / HTTP/1.1\r\nHost: bench.local\r\nConnection: close\r\n\r\n' >&3
  local line content_length=0
  while IFS=$'\r' read -r line <&3; do
    [ -z "$line" ] && break
    case "$line" in Content-Length:*) content_length="${line#Content-Length: }";; esac
  done
  [ "$content_length" = "$body_bytes" ] || exit 1
  dd bs=1 count=1024 iflag=fullblock <&3 > "${result_file}.prefix" 2>/dev/null
  : > "$ready_file"
  sleep 1
  local remaining blocks tail
  remaining=$((body_bytes - 1024))
  blocks=$((remaining / 4096))
  tail=$((remaining % 4096))
  dd bs=4096 count="$blocks" iflag=fullblock <&3 > "${result_file}.body" 2>/dev/null
  if [ "$tail" -gt 0 ]; then dd bs=1 count="$tail" iflag=fullblock <&3 >> "${result_file}.body" 2>/dev/null; fi
  cat "${result_file}.prefix" "${result_file}.body" | wc -c > "$result_file"
  exec 3>&-; exec 3<&-
}

run_slow_client() {
  local body_bytes=524288 temp ready result slow_pid parallel_status pauses resumes final_bytes
  temp=$(mktemp -d /tmp/aegisgate_slow_XXXXXX); ready="$temp/ready"; result="$temp/result"
  slow_reader "$body_bytes" "$ready" "$result" & slow_pid=$!
  wait_until 5000 "slow reader prefix" "test -f '$ready'"
  parallel_status=$(request_code)
  [ "$parallel_status" = 200 ] || { kill "$slow_pid" 2>/dev/null || true; die "parallel request failed while slow client was paused"; }
  wait "$slow_pid" || die "slow reader failed"
  final_bytes=$(cat "$result")
  pauses=$(count_metric aegisgate_upstream_read_pauses_total); resumes=$(count_metric aegisgate_upstream_read_resumes_total)
  rm -rf "$temp"
  [ "$final_bytes" = "$body_bytes" ] || die "slow reader received $final_bytes bytes, expected $body_bytes"
  [ "${pauses:-0}" -gt 0 ] || die "slow-client scenario did not observe an upstream-read pause"
  [ "${resumes:-0}" -gt 0 ] || die "slow-client scenario did not observe an upstream-read resume"
  SCENARIO_DETAILS="\"body_bytes\":$body_bytes,\"slow_reader_bytes\":$final_bytes,\"parallel_request_status\":$parallel_status,\"upstream_read_pauses\":$pauses,\"upstream_read_resumes\":$resumes"
  print_result 2 0 0 0 0 0 0 0
}

run_reload() {
  local before after traffic_pid reload_events
  before=$(request_code); [ "$before" = 200 ] || die "reload setup request to E1 failed"
  (run_sequential_load 2 200 >/dev/null) & traffic_pid=$!
  write_config "${CONFIG_FILE}.next" "$WORKERS" "$RELOAD_BACKEND_PORT" normal
  mv "${CONFIG_FILE}.next" "$CONFIG_FILE"
  kill -HUP "$GATEWAY_PID"
  wait_until 5000 "reload_succeeded log event" "grep -q '\"event\":\"reload_succeeded\"' '$BENCH_LOG_PATH'"
  after=$(request_code); [ "$after" = 200 ] || die "post-reload request to E2 failed"
  wait "$traffic_pid"
  wait_until 3000 "E2 request metric" "metrics_text | grep -q 'upstream=\"127.0.0.1:${RELOAD_BACKEND_PORT}\"'"
  reload_events=$(grep -c '"event":"reload_succeeded"' "$BENCH_LOG_PATH" || true)
  SCENARIO_DETAILS="\"pre_reload_status\":$before,\"post_reload_status\":$after,\"reload_succeeded_events\":$reload_events,\"post_reload_upstream\":\"127.0.0.1:${RELOAD_BACKEND_PORT}\""
  print_result 2 0 0 0 0 0 0 0
}

print_result() {
  local requests="$1" errors="$2" duration_ms="$3" rps="$4" p50="$5" p95="$6" p99="$7" mean="$8"
  local dropped io_dropped critical log_lines
  dropped=$(count_metric aegisgate_log_dropped_total); io_dropped=$(count_metric aegisgate_log_io_dropped_total)
  critical=$(count_metric aegisgate_log_critical_overflow_total); log_lines=$(wc -l < "$BENCH_LOG_PATH" 2>/dev/null || echo 0)
  cat > "$RESULT_FILE" <<EOF
{
  "git_sha": "$GIT_SHA",
  "cpu": "$CPU_INFO",
  "kernel": "$KERNEL",
  "workers": $WORKERS,
  "scenario": "$SCENARIO",
  "duration_seconds": $DURATION,
  "requests": $requests,
  "errors": $errors,
  "rps": $rps,
  "p50_us": $p50,
  "p95_us": $p95,
  "p99_us": $p99,
  "mean_us": $mean,
  "duration_ms": $duration_ms,
  "log_lines_written": ${log_lines:-0},
  "log_dropped_total": ${dropped:-0},
  "log_io_dropped_total": ${io_dropped:-0},
  "log_critical_overflow_total": ${critical:-0},
  "scenario_details": { $SCENARIO_DETAILS },
  "timestamp": "$(date -Iseconds)"
}
EOF
  log "result: $RESULT_FILE"
  cat "$RESULT_FILE"
}

log "scenario=$SCENARIO workers=$WORKERS runs=$RUNS duration=${DURATION}s warmup=${WARMUP}s sha=$GIT_SHA"
build_release
CONFIG_FILE=$(mktemp /tmp/aegisgate_bench_XXXXXX.yaml)

case "$SCENARIO" in
  normal) write_config "$CONFIG_FILE" "$WORKERS" "$BACKEND_PORT" normal; start_backend BACKEND_PID "$BACKEND_PORT" 200;;
  admission) write_config "$CONFIG_FILE" "$WORKERS" "$BACKEND_PORT" admission; start_backend BACKEND_PID "$BACKEND_PORT" 200;;
  breaker) write_config "$CONFIG_FILE" "$WORKERS" "$BACKEND_PORT" breaker; start_backend BACKEND_PID "$BACKEND_PORT" 500;;
  slow_client) write_config "$CONFIG_FILE" "$WORKERS" "$BACKEND_PORT" normal; start_backend BACKEND_PID "$BACKEND_PORT" 200 524288;;
  reload) write_config "$CONFIG_FILE" "$WORKERS" "$BACKEND_PORT" normal; start_backend BACKEND_PID "$BACKEND_PORT" 200; start_backend BACKEND2_PID "$RELOAD_BACKEND_PORT" 200;;
esac
start_gateway

case "$SCENARIO" in
  normal) run_normal;; admission) run_admission;; breaker) run_breaker;; slow_client) run_slow_client;; reload) run_reload;;
esac
