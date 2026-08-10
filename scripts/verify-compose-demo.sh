#!/usr/bin/env bash
set -euo pipefail

project_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
compose=(sudo docker compose -f "$project_root/deploy/docker-compose.yml")

cleanup() {
  "${compose[@]}" logs --no-color || true
  "${compose[@]}" down -v || true
}
trap cleanup EXIT

"${compose[@]}" config
"${compose[@]}" up --build -d --wait
"${compose[@]}" ps

curl --noproxy '*' --fail --silent --show-error --http1.1 \
  -H 'Host: normal.demo.local' http://127.0.0.1:8080/ >/dev/null

fault_status="$(curl --noproxy '*' --silent --show-error --output /dev/null --write-out '%{http_code}' \
  --http1.1 -H 'Host: fault.demo.local' http://127.0.0.1:8080/)"
test "$fault_status" = 503

curl --noproxy '*' --fail --silent --show-error --http1.1 http://127.0.0.1:8080/metrics \
  | grep -q '^aegisgate_requests_total'

echo 'AegisGate Compose demo verified.'
