# 可观测性

[English](../en/observability.md) | [简体中文](observability.md)

## 指标

`GET /metrics` 返回 Prometheus 文本格式。它包括请求结果、route 和 upstream 决策、admission 与 breaker 状态、活动连接、日志丢弃计数和流式背压计数。

以下计数器适合验证过载路径：

- `aegisgate_log_dropped_total`、`aegisgate_log_io_dropped_total`、`aegisgate_log_critical_overflow_total` 表示日志降级。
- `aegisgate_upstream_read_pauses_total`、`aegisgate_upstream_read_resumes_total` 表示下游背压是否传递到上游读取。
- circuit-state 与 upstream label 可以显示 route 是否打开或恢复。

指标只能用于观测，不能作为请求处理的同步机制或控制 API。

## 结构化日志

`--log-path` 通过 `AsyncLogger` 写 JSON Lines。请求路径只向有界队列提交小型值对象，专属 writer 线程拥有文件 I/O。队列压力和写入失败只增加计数器，不会阻塞请求 worker。

日志事件包括 gateway start/stop、reload 结果和 `request_complete`。请求终局记录可包含 generation、status、reason、latency、retries、route 与 upstream；不会记录 body、`Authorization`、`Cookie` 或原始配置内容。

日志仍属于可能敏感的运维元数据；应限制文件访问权限、在外部轮转，并按自身策略保留。
