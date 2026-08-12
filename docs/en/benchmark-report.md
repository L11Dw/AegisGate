# Benchmark method and evidence

[English](benchmark-report.md) | [简体中文](../zh-CN/benchmark-report.md)

## Purpose

The benchmark harness is a reproducible behavior and measurement tool. It is not a source of universal performance claims: results vary with CPU topology, kernel, compiler, socket buffers, Docker, and the upstream implementation.

## Run matrix

Run normal forwarding at one, two, and four workers after a warm-up. Keep the generated JSON files outside Git:

```bash
./benchmark/run.sh normal 1 5 10 3
./benchmark/run.sh normal 2 5 10 3
./benchmark/run.sh normal 4 5 10 3
```

The harness emits git SHA, CPU model, kernel, worker count, request count, error count, RPS, latency percentiles, log drop counters, and scenario details.

## Semantic scenarios

| Scenario | Required observation |
|---|---|
| `admission` | Both admitted 200 responses and rejected 429 responses under a constrained global budget. |
| `breaker` | Backend failures open the circuit; a healthy backend on the same port restores Closed state. |
| `slow_client` | A 512 KiB body is received intact while upstream pause and resume counters both increase. |
| `reload` | Atomic configuration replacement plus SIGHUP produces `reload_succeeded` and later traffic reaches E2. |

## Optimization policy

P-1 through P-5 are not adopted without evidence. Retain an optimization only when repeated measurements show at least a 3% throughput improvement, no more than 5% P99 regression, and all semantic and sanitizer gates remain green. At this release point, no benchmark-driven production optimization is claimed.
