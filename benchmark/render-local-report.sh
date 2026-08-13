#!/usr/bin/env bash
# Render a concise Markdown report from a run-local-suite evidence directory.
set -euo pipefail

if (($# != 1)); then
  echo "Usage: $0 <benchmark/results/local-suite-*>" >&2
  exit 2
fi

OUTPUT_DIR="$1"
MANIFEST="$OUTPUT_DIR/manifest.tsv"
[[ -f "$MANIFEST" ]] || { echo "missing manifest: $MANIFEST" >&2; exit 1; }

value() {
  local key="$1" file="$2"
  sed -n "s/^${key}=//p" "$file" | head -1
}

json_value() {
  local key="$1" file="$2"
  sed -n "s/^[[:space:]]*\"${key}\":[[:space:]]*\([^,}]*\).*/\1/p" "$file" | head -1 | tr -d '"'
}

report="$OUTPUT_DIR/REPORT.md"
{
  echo "# AegisGate local benchmark report"
  echo
  echo "Generated: $(date -Iseconds)"
  echo
  echo "## Environment"
  echo
  echo "- Git revision: \`$(value git_sha "$OUTPUT_DIR/environment.txt")\`"
  echo "- Working tree dirty at capture: $(value git_dirty "$OUTPUT_DIR/environment.txt")"
  echo "- Git diff SHA-256: \`$(value git_diff_sha256 "$OUTPUT_DIR/environment.txt")\`"
  echo "- Mode: $(value mode "$OUTPUT_DIR/environment.txt")"
  echo "- CPU: $(value cpu_model "$OUTPUT_DIR/environment.txt") ($(value cpu_count "$OUTPUT_DIR/environment.txt") logical CPUs)"
  echo "- Kernel: $(value kernel "$OUTPUT_DIR/environment.txt")"
  echo "- Load generator: $(value wrk "$OUTPUT_DIR/environment.txt")"
  echo
  echo "## Results"
  echo
  echo "| Case | Workers | Connections | Body | RPS | p50 (µs) | p99 (µs) | Errors | HTTP non-2xx | 429 | 5xx | Socket errors | Evidence |"
  echo "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |"
  tail -n +2 "$MANIFEST" | while IFS=$'\t' read -r label connections body scenario workers runs duration warmup result_json log_file; do
    json="$OUTPUT_DIR/$result_json"
    rps=$(json_value rps "$json")
    p50=$(json_value p50_us "$json")
    p99=$(json_value p99_us "$json")
    errors=$(json_value errors "$json")
    http_errors=$(sed -n 's/.*"non2xx_responses"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$json" | head -1)
    socket_errors=$(sed -n 's/.*"socket_errors"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$json" | head -1)
    status_429=$(sed -n 's/.*"status_429"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$json" | head -1)
    status_5xx=$(sed -n 's/.*"status_5xx"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$json" | head -1)
    [[ -n "$status_429" ]] || status_429=$(sed -n 's/.*"rate_limited"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$json" | head -1)
    [[ -n "$status_5xx" ]] || status_5xx=$(sed -n 's/.*"failure_responses_before_open"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' "$json" | head -1)
    http_errors=${http_errors:--}; status_429=${status_429:--}; status_5xx=${status_5xx:--}; socket_errors=${socket_errors:-0}
    body_text="-"
    [[ "$body" != 0 ]] && body_text="$body B"
    printf '| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | [%s](%s) |\n' \
      "$label" "$workers" "$connections" "$body_text" "$rps" "$p50" "$p99" "$errors" "$http_errors" "$status_429" "$status_5xx" "$socket_errors" "log" "$log_file"
  done

  echo
  echo "Resource snapshots are stored in each JSON result as gateway CPU%, RSS KiB, and voluntary/nonvoluntary context switches."

  echo
  echo "## Interpretation"
  echo
  echo "- This is a single-host loopback result: gateway and mock backend share the same machine and CPU resources."
  echo "- The worker matrix is diagnostic, not a portable QPS claim; compare worker counts only on the same host and topology."
  echo "- HTTP errors and socket errors are reported separately. A nonzero socket count is a load-generator/test-host boundary until gateway logs or metrics prove otherwise."
  echo "- Independent-backend or remote-backend runs require an operator-supplied backend address; they are not inferred from this local report."

  if [[ -f "$OUTPUT_DIR/load-ceiling.txt" ]]; then
    echo
    echo "## Observed concurrency boundary"
    echo
    echo '```text'
    cat "$OUTPUT_DIR/load-ceiling.txt"
    echo '```'
    echo
    echo "The suite retained the evidence and continued with semantic scenarios. This is an observed boundary, not a diagnosis of the gateway."
  fi

  echo
  echo "## Reproduce"
  echo
  echo "\`./benchmark/run-local-suite.sh --$(value mode "$OUTPUT_DIR/environment.txt")\`"
  echo
  echo "Raw command output is under [logs/](logs/); machine-readable measurements are under [results/](results/)."
} > "$report"

echo "$report"
