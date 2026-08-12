# AegisGate Benchmark

[English](README.md) | [简体中文](../README.zh-CN.md) | [Method and evidence](../docs/en/benchmark-report.md)

## Quick Start

```bash
# Build release
cmake -S . -B build/release -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
cmake --build build/release -j$(nproc)

# Run default benchmark (normal scenario, 1 worker, 5 runs)
./benchmark/run.sh

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
| `normal` | HTTP forwarding baseline; measures sequential request throughput and latency at 1/2/4 workers. |
| `slow_client` | A 512 KiB Content-Length body is read only 1 KiB at first, then drained. It must preserve every byte and observe upstream read pause/resume counters while a parallel request still succeeds. |
| `breaker` | A 500 backend opens the circuit; the backend is restarted as 200 on the same port and the script requires HalfOpen recovery back to Closed. |
| `admission` | 64 concurrent requests under a 10 rps / burst 5 budget; requires both admitted 200s and rejected 429s. |
| `reload` | Traffic begins on endpoint E1, config is atomically replaced with E2 and SIGHUP is sent; the script requires `reload_succeeded` plus post-reload E2 traffic. |

## Output Format

Results are written to `benchmark/results/` as JSON:

```json
{
  "git_sha": "abc1234",
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
  "scenario_details": {"accepted": 12345, "errors": 0},
  "timestamp": "2026-08-13T10:30:00+08:00"
}
```

## Measurement Discipline

- `normal` performs warmup + N runs × duration; semantic scenarios are one deterministic exercise each.
- Results include mean/p50/p95/p99, log drop counters and scenario-specific assertions.
- Do not modify production code based on single-run results
- P-1～P-5 optimizations require ≥3% throughput gain, ≤5% p99 regression
- Keep `benchmark/results/` local. Every JSON sample records its own machine,
  kernel, and Git SHA; it is evidence for that environment, not a portable QPS claim.

## Requirements

- `wrk` (preferred) or `curl` for HTTP benchmarking
- `jq` for JSON processing (optional)
- Linux with epoll support
