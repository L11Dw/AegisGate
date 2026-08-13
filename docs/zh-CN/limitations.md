# 协议边界与限制

[English](../en/limitations.md) | [简体中文](limitations.md)

## 已支持范围

- 仅 Linux、C++20、基于 `epoll` 的非阻塞 TCP。
- HTTP/1.1 请求路由和 Content-Length 响应；上游响应最大 16 MiB。
- 字面量 IPv4 上游地址。
- 进程生命周期内固定的 worker 数。
- worker 本地上游复用，不共享跨 worker 连接池。

## 本版本不支持

- TLS 终止、证书管理、DNS 解析、服务发现和分布式控制面。
- HTTP chunked、HTTP/2、HTTP/3、gRPC、WebSocket 和任意协议隧道。
- 请求 body 流式化；请求 body 仍限制为 1 MiB。chunked 响应和超过 16 MiB 的上游 Content-Length 响应会被拒绝。
- 动态 worker 调整、连接迁移和全局共享 idle 连接池。
- “生产就绪”、SLA 或可泛化 QPS 的宣称。

## 运维含义

如需 TLS 和服务发现，应在 AegisGate 前方或旁边部署相应组件。本版本应配置稳定的字面量上游地址。reload 可以修改 route 和策略，但不能改变进程 worker 数。

这些边界是有意保留的：它们使所有权、故障处理和关闭语义可独立验证。未来新增协议能力时，必须保持同样的 event loop 与已提交响应契约。
