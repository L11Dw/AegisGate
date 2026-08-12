# AegisGate

> **An event-driven HTTP gateway for predictable operations.**

[English](README.md) | [简体中文](README.zh-CN.md)

![AegisGate request flow](docs/assets/aegisgate-flow-light.png)

AegisGate is an HTTP/1.1 reverse proxy for Linux services. It routes requests to upstream services while making ownership, overload protection, failure handling, and configuration changes explicit and observable.

It provides a runnable demo and reproducible verification. It does **not** claim production readiness or a universal throughput figure.

## Highlights

- Event-driven Linux I/O with `epoll`, `eventfd`, `timerfd`, and non-blocking sockets.
- Host/path routing, weighted and least-active selection, worker-local connection pools.
- Global admission, timeouts, bounded retries, health checks, and circuit breaking.
- Multi-worker I/O, response backpressure, atomic configuration reload, Prometheus metrics, and JSON Lines logs.

## Quick start

```bash
docker compose -f deploy/docker-compose.yml up --build
curl -i -H 'Host: normal.demo.local' http://127.0.0.1:8080/
curl -s http://127.0.0.1:8080/metrics
docker compose -f deploy/docker-compose.yml down -v --remove-orphans
```

For a scripted demo that verifies forwarding, logs, and SIGHUP reload:

```bash
./scripts/verify-compose-demo.sh
```

To build locally:

```bash
cmake -S . -B build/release -G Ninja -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF
cmake --build build/release
./build/release/aegisgate_server configs/demo.yaml 8080 --log-path ./aegisgate.jsonl
```

## Documentation

| Topic | English | 简体中文 |
|---|---|---|
| Architecture and ownership | [Read](docs/en/architecture.md) | [阅读](docs/zh-CN/architecture.md) |
| Operations and reload | [Read](docs/en/operations.md) | [阅读](docs/zh-CN/operations.md) |
| Metrics and logs | [Read](docs/en/observability.md) | [阅读](docs/zh-CN/observability.md) |
| Protocol boundaries | [Read](docs/en/limitations.md) | [阅读](docs/zh-CN/limitations.md) |
| Benchmark evidence | [Read](docs/en/benchmark-report.md) | [阅读](docs/zh-CN/benchmark-report.md) |
| Release validation | [Read](docs/en/release-validation.md) | [阅读](docs/zh-CN/release-validation.md) |

## Architecture

```text
Clients → Acceptor → I/O workers → upstream services
                       │
                       ├─ ClientConnection / ProxyTransaction
                       ├─ worker-local selection, timers, connection pools
                       └─ streaming backpressure

Control plane: immutable configuration generations, global admission,
health/circuit coordination, reload, metrics, and structured logging.
```

An event-loop-owned object is accessed only by its owning thread. Cross-thread work uses bounded queues and wake descriptors; a socket descriptor is transferred only through `FdOwner`.

## Verification

The repository maintains Debug ASan/UBSan, plain, and ThreadSanitizer gates. The benchmark harness exercises normal forwarding, admission limiting, circuit recovery, a 512 KiB slow-reader backpressure flow, and atomic reload under traffic.

```bash
./benchmark/run.sh normal 1 5 10 3
```

Read the [benchmark guide](benchmark/README.md) before interpreting a result.

## Scope and limitations

This release supports HTTP/1.1 and literal IPv4 upstream endpoints. TLS termination, DNS/service discovery, HTTP chunked transfer encoding, HTTP/2, HTTP/3, gRPC, WebSocket, dynamic worker resizing, and cross-worker connection migration are deliberately out of scope. See [protocol boundaries](docs/en/limitations.md).

## Contributing

Issues and pull requests are welcome. Keep changes focused, preserve the event-loop ownership model, add deterministic tests for behavior changes, and run relevant sanitizer gates before review.

## License

Distributed under the [MIT License](LICENSE).
