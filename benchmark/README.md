# AegisGate Benchmark

[English](README.md) | [简体中文](../README.zh-CN.md) | [Method and evidence](../docs/en/benchmark-report.md)

## Quick Start

```bash
# Build release
cmake -S . -B build/release -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
cmake --build build/release -j$(nproc)

# Run default benchmark (normal scenario, 1 worker, 5 runs)
./benchmark/run.sh

# Short end-to-end local suite: worker scaling, connection ladder and
# protection/backpressure scenarios (requires wrk)
./benchmark/run-local-suite.sh --quick

# Full local matrix: roughly 6-7 minutes on a quiet development machine
./benchmark/run-local-suite.sh --full

# Run with specific parameters
./benchmark/run.sh normal 2 10 30 5
#                  ^      ^ ^  ^  ^
#                  |      | |  |  warmup seconds
#                  |      | |  duration per run
#                  |      | number of runs
#                  |      workers
#                  scenario
```

## Scenarios

| Scenario | Description |
|----------|-------------|
| `normal` | HTTP forwarding baseline. With `AEGISGATE_BENCH_CONNECTIONS=N`, uses `wrk` and persistent connections; the full suite measures 1/2/4 worker scaling at 64 connections plus a 1→512 connection ladder at 2 workers. |
| `slow_client` | A single downstream client reads 1 KiB of a Content-Length response, pauses, then drains it. The suite verifies every byte and an independent parallel 200 response at 512KiB, 5MiB and 16MiB; 5MiB and 16MiB additionally require upstream pause/resume, while 512KiB is too small to force that transition on every kernel/socket-buffer configuration. |
| `breaker` | A 500 backend opens the circuit; the backend is restarted as 200 on the same port and the script requires HalfOpen recovery back to Closed. |
| `admission` | 64 concurrent requests under a 10 rps / burst 5 budget; requires both admitted 200s and rejected 429s. |
| `reload` | Traffic begins on endpoint E1, config is atomically replaced with E2 and SIGHUP is sent; the script requires `reload_succeeded` plus post-reload E2 traffic. |

## Output Format

Results are written to `benchmark/results/` as JSON:

```json
{
  "git_sha": "abc1234",
  "git_dirty": false,
  "git_diff_sha256": "e3b0c44298fc1c149afbf4c8996fb924...",
  "cpu": "Intel i7-12700K",
  "kernel": "6.5.0",
  "workers": 2,
  "scenario": "normal",
  "duration_seconds": 10,
  "requests": 12345,
  "errors": 0,
  "duration_ms": 10000,
  "rps": 1234,
  "p50_us": 420,
  "p95_us": 610,
  "p99_us": 840,
  "resource_snapshot": {
    "gateway_cpu_percent": 12.5,
    "gateway_rss_kib": 18432,
    "gateway_voluntary_ctxt_switches": 1234,
    "gateway_nonvoluntary_ctxt_switches": 7
  },
  "scenario_details": {
    "accepted": 12345,
    "errors": 0,
    "non2xx_responses": 0,
    "socket_errors": 0,
    "percentiles_per_run": [
      {"p50_us": 420, "p75_us": 510, "p90_us": 570, "p99_us": 840}
    ]
  },
  "timestamp": "2026-08-13T10:30:00+08:00"
}
```

## Measurement Discipline

- `normal` performs warmup + N runs × duration; semantic scenarios are one deterministic exercise each.
- Each `normal` run keeps its own percentile sample in `scenario_details.percentiles_per_run`; the top-level
  percentile fields are arithmetic means for quick comparison and must not be treated as a merged histogram.
- `non2xx_responses` and `socket_errors` are recorded separately so a load-generator failure is not confused with
  an HTTP failure from the gateway.
- `git_dirty` records whether uncommitted changes were present when the benchmark started; do not compare a dirty
  result with a release baseline without recording the diff.
- `wrk` reports p50/p75/p90/p99. It does not report p95, so a wrk JSON result deliberately uses `null` for `p95_us` rather than inventing a percentile.
- The full connection ladder stops after its first non-zero-error point and writes `load-ceiling.txt`; this records the observed boundary instead of treating overload as success.
- The 16MiB response ceiling is an explicit upstream Content-Length limit. It is safe for the streaming path used here; 512MiB responses are intentionally unsupported until the mock and response-limit architecture are redesigned.
- Results include latency values, log drop counters and scenario-specific assertions.
- The gateway exposes `aegisgate_worker_*` gauges for active connections, in-flight requests, and
  backpressure transitions so scaling runs can detect worker imbalance.
- Do not modify production code based on single-run results
- P-1～P-5 optimizations require ≥3% throughput gain, ≤5% p99 regression
- Keep `benchmark/results/` local. Every JSON sample records its own machine,
  kernel, and Git SHA; it is evidence for that environment, not a portable QPS claim.

## Requirements

- `wrk` for the local suite (`sudo apt install wrk` on Ubuntu/Debian)
- `curl` for deterministic semantic scenarios
- `jq` for JSON processing (optional)
- Linux with epoll support
