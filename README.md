# AegisGate

> A high-performance HTTP API gateway built with C++20 on Linux.

## Status

**In design and implementation planning.** AegisGate is being built incrementally; every implemented capability will be accompanied by tests and reproducible evidence. This repository does not claim production readiness or a target QPS before measurements exist.

## Problem

Services commonly need a single entry point that can route requests to multiple backend instances while protecting both the gateway and its upstreams under overload or partial failure. AegisGate focuses on four practical concerns:

- route HTTP requests by `Host` and path prefix;
- distribute traffic across healthy backend instances;
- bound work with rate limits, in-flight limits, timeouts, and safe retries;
- make latency, failures, and protection decisions observable.

## Scope

The first deliverable is a single-event-loop HTTP/1.1 reverse proxy with upstream Keep-Alive reuse, weighted round-robin, route-level admission control, timeout handling, limited idempotent retries, metrics, fault-injectable mock backends, and reproducible tests and benchmarks.

Subsequent deliverables add multi-worker I/O, active health checks, circuit breaking, backpressure, configuration reload, structured logging, and performance analysis. Adaptive overload control is an optional experiment only; it will be retained only if controlled measurements demonstrate a benefit.

Deliberate non-goals include HTTP/2, HTTP/3, gRPC, WebSocket, TLS termination, service discovery, distributed control planes, and HTTP chunked transfer encoding.

## Architecture

```text
Client
  │ HTTP/1.1
  ▼
AegisGate
  ├── route matching and admission control
  ├── load balancing and upstream connection reuse
  ├── timeout / retry / fault isolation
  └── metrics and structured logs
  ▼
Mock or application backends
```

See [the design document](AegisGate-%E8%AE%BE%E8%AE%A1%E6%96%87%E6%A1%A3.md) for protocol boundaries, state ownership, failure handling, and verification criteria.

## Run the demo

The local Compose demo builds the existing CMake targets, starts the gateway and two deterministic mock backends, and publishes the gateway at `127.0.0.1:8080`.

```bash
docker compose -f deploy/docker-compose.yml up --build
curl -i -H 'Host: normal.demo.local' http://127.0.0.1:8080/
curl -i -H 'Host: fault.demo.local' http://127.0.0.1:8080/
curl -s http://127.0.0.1:8080/metrics
```

The demo uses fixed private Docker-network addresses because this MVP deliberately accepts only literal IPv4 upstream endpoints; it does not perform DNS resolution. See [benchmark/README.md](benchmark/README.md) for a reproducible measurement procedure and result-record template.

## Planned verification

- unit and integration tests, including TCP segmentation, malformed HTTP, connection reuse, timeouts, and fault injection;
- ASan/UBSan coverage for core integration scenarios;
- reproducible load tests reporting QPS, P50/P99 latency, error rate, CPU, memory, and optimization deltas;
- a Docker Compose demo that reproduces backend degradation and traffic protection.

## Technology

- C++20 on Linux
- epoll, eventfd, timerfd, non-blocking sockets
- CMake and Ninja
- GoogleTest, yaml-cpp, Prometheus text exposition
- perf, Sanitizers, wrk/wrk2, FlameGraph

## License

The license will be selected before the first public code release.
