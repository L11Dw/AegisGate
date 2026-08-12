# Architecture

[English](architecture.md) | [简体中文](../zh-CN/architecture.md)

## Design intent

AegisGate keeps the request path event-driven and makes the ownership boundary of every loop-attached object explicit. Its control plane coordinates shared decisions; its data plane remains worker-local.

## Data plane

The acceptor assigns each accepted client descriptor to an I/O worker in round-robin order. The destination worker owns the descriptor and constructs its `ClientConnection`, parser, buffers, timers, `ProxyTransaction`, upstream pool, and selection state. A connection, buffer, channel, timer, or pool is never migrated or shared between workers.

Each request is handled by a `ProxyTransaction`. It selects an upstream, reserves admission and outcome capacity where required, forwards the request, and maps an upstream result to the downstream response. A retry is allowed only before a downstream response header has been committed and only for an eligible idempotent request.

## Control plane

`RuntimeGeneration` groups the immutable configuration snapshot with its coordinator, admission state, outcome channels, and per-worker selection states. A new configuration is prepared on every worker and published atomically. Requests already in flight retain their original generation; they do not consult the new coordinator during a retry.

The health and circuit coordinator has a single state-transition owner. Workers read published decisions and submit outcomes; they do not mutate shared breaker state directly. Global admission and global in-flight accounting remain distinct from worker-local least-active counters.

## Cross-thread rules

- Work crosses threads through bounded queues and wake descriptors only.
- `eventfd` wakes a loop; it never carries request data.
- File-descriptor transfer uses `FdOwner`, which gives exactly one party responsibility for close.
- Owner-thread APIs for `EventLoop`, `Channel`, `ClientConnection`, `UpstreamConnection`, `TimerQueue`, and `UpstreamPool` are not called from foreign threads.

## Backpressure and shutdown

Streaming response flow control uses high/low watermarks. When the downstream buffer crosses the high watermark, upstream reading pauses; when it drains below the low watermark, reading resumes. A response whose header is already committed is truncated on a later upstream failure rather than replaced with a synthetic error response.

Shutdown stops acceptance before loop-owned objects are destroyed. Destruction tasks are posted to the owning worker, and a retiring generation stops checkers, waits for request leases, drains outcomes, stops its coordinator, and only then releases its runtime state.
