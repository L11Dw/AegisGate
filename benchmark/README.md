# AegisGate Benchmark

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
| `normal` | Normal keep-alive requests (1/2/4 workers) |
| `slow_client` | Slow client backpressure |
| `breaker` | Breaker open/recovery |
| `admission` | Global admission rate limiting |
| `reload` | Reload during traffic |

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
  "runs": [
    {"requests": 12345, "errors": 0, "duration_ms": 10000, "rps": 1234}
  ],
  "system": {"rss_kb": 12345, "cpu_percent": 45.2},
  "timestamp": "2026-08-13T10:30:00+08:00"
}
```

## Measurement Discipline

- Each scenario: warmup + N runs × duration
- Report mean, stddev, p50/p95/p99
- Do not modify production code based on single-run results
- P-1～P-5 optimizations require ≥3% throughput gain, ≤5% p99 regression

## Requirements

- `wrk` (preferred) or `curl` for HTTP benchmarking
- `jq` for JSON processing (optional)
- Linux with epoll support
