# Observability

[English](observability.md) | [简体中文](../zh-CN/observability.md)

## Metrics

`GET /metrics` returns Prometheus text exposition. It includes request outcomes, route and upstream decisions, admission and breaker state, active connections, logging drop counters, and streaming backpressure counters.

The following counters are useful when validating an overloaded path:

- `aegisgate_log_dropped_total`, `aegisgate_log_io_dropped_total`, and `aegisgate_log_critical_overflow_total` show logger degradation.
- `aegisgate_upstream_read_pauses_total` and `aegisgate_upstream_read_resumes_total` show downstream backpressure propagation.
- circuit-state and upstream labels show whether a route has opened or recovered.

Metric collection must not be used as a synchronization mechanism for request processing. It is an observation surface, not a control API.

## Structured logs

`--log-path` writes JSON Lines through `AsyncLogger`. The request path submits a small value record to a bounded queue; a dedicated writer thread owns file output. Queue pressure and I/O failures increase counters rather than blocking request workers.

Events include gateway start/stop, reload outcomes, and `request_complete`. Request terminal records may include generation, status, reason, latency, retries, route, and upstream. Bodies, `Authorization`, `Cookie`, and raw configuration content are not logged.

Treat logs as potentially sensitive operational metadata. Restrict file access, rotate files externally, and retain them according to your own policy.
