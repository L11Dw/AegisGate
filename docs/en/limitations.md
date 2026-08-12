# Protocol boundaries and limitations

[English](limitations.md) | [简体中文](../zh-CN/limitations.md)

## Supported scope

- Linux only; C++20; non-blocking TCP with `epoll`.
- HTTP/1.1 request routing and Content-Length responses.
- Literal IPv4 upstream addresses.
- Fixed worker count for the lifetime of a process.
- Worker-local upstream reuse; no cross-worker pool sharing.

## Not supported in this release

- TLS termination, certificate management, DNS resolution, service discovery, or a distributed control plane.
- HTTP chunked transfer encoding, HTTP/2, HTTP/3, gRPC, WebSocket, and arbitrary protocol tunnelling.
- Request-body streaming; body handling follows the current bounded HTTP parser behavior.
- Dynamic worker resizing, connection migration, or a globally shared idle connection pool.
- A claim of production readiness, an SLA, or a portable QPS number.

## Operational implications

Deploy TLS and service discovery in a component in front of or beside AegisGate if they are required. Configure stable literal upstream addresses for this version. A configuration reload can change routes and policy, but it cannot change the process worker count.

These boundaries are intentional: they keep ownership, failure, and shutdown semantics independently testable. New protocol features must preserve the same event-loop and committed-response contracts.
