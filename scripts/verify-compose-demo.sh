#!/usr/bin/env bash
# External Compose contract: forwarding, metrics, JSONL, SIGHUP reload, cleanup.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deploy/docker-compose.yml"
RUN_DIR="$(mktemp -d /tmp/aegisgate-compose-demo.XXXXXX)"
CONFIG_DIR="$RUN_DIR/config"
CONFIG_FILE="$CONFIG_DIR/demo.yaml"
LOG_DIR="$RUN_DIR/logs"
PROJECT_NAME="aegisgate-verify-$RANDOM"
mkdir -p "$LOG_DIR" "$CONFIG_DIR"
cp "$ROOT_DIR/configs/demo.yaml" "$CONFIG_FILE"

compose() {
  AEGISGATE_CONFIG_DIR="$CONFIG_DIR" AEGISGATE_LOG_DIR="$LOG_DIR" \
    docker compose --project-name "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$RUN_DIR"
}
trap cleanup EXIT

die() {
  echo "[compose-demo] ERROR: $*" >&2
  compose logs --no-color >&2 || true
  exit 1
}

wait_until() {
  local timeout_seconds="$1" label="$2" command="$3"
  local deadline=$((SECONDS + timeout_seconds))
  while (( SECONDS < deadline )); do
    if eval "$command"; then return 0; fi
    sleep 0.1
  done
  die "timed out waiting for $label"
}

http_status() {
  local host="$1"
  curl --noproxy '*' --connect-timeout 1 --max-time 3 -sS -o /dev/null -w '%{http_code}' \
    -H "Host: $host" http://127.0.0.1:8080/ 2>/dev/null || printf '000'
}

log_has_event() {
  local event="$1"
  grep -Fq "\"event\":\"${event}\"" "$LOG_DIR/aegisgate.jsonl" 2>/dev/null
}

command -v docker >/dev/null || { echo "[compose-demo] SKIP: docker is not installed"; exit 0; }
docker compose version >/dev/null 2>&1 || { echo "[compose-demo] SKIP: docker compose is unavailable"; exit 0; }
docker info >/dev/null 2>&1 || { echo "[compose-demo] SKIP: docker daemon is unavailable or inaccessible"; exit 0; }

echo "[compose-demo] building and starting $PROJECT_NAME"
compose up --build -d
wait_until 90 "gateway metrics" '[ -n "$(curl --noproxy "*" --connect-timeout 1 --max-time 2 -sS http://127.0.0.1:8080/metrics 2>/dev/null)" ]'

[ "$(http_status normal.demo.local)" = "200" ] || die "normal route did not return 200"
wait_until 10 "structured request log" 'log_has_event request_complete'
log_has_event gateway_start || die "gateway_start was not logged"

# Switch normal.demo.local from the 200 backend to the deterministic 503 backend.
# The file is a private copy mounted as a directory, so rename is observable
# inside the container and never edits the repository configuration.
# The target mock is intentionally a separate endpoint: update both fields
# atomically, otherwise the new address is paired with the old port and the
# verifier would exercise an unintended connect failure (502) instead of its
# deterministic 503 response.
sed -e 's/172\.29\.0\.10/172.29.0.11/' -e 's/port: 18080/port: 18081/' \
  "$CONFIG_FILE" > "$CONFIG_FILE.next"
mv "$CONFIG_FILE.next" "$CONFIG_FILE"
compose kill -s HUP gateway >/dev/null
wait_until 15 "reload_succeeded log" 'log_has_event reload_succeeded'
wait_until 15 "traffic through reloaded endpoint" '[ "$(http_status normal.demo.local)" = "503" ]'

echo "[compose-demo] PASS: forwarding, metrics, JSONL logging, and SIGHUP reload verified"
