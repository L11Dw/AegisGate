# 架构

[English](../en/architecture.md) | [简体中文](architecture.md)

## 设计目标

AegisGate 让请求路径保持事件驱动，并明确每个归属 event loop 的对象的所有权边界。控制面协调共享决策；数据面始终保持 worker 本地化。

## 数据面

Acceptor 以 round-robin 将已接受的客户端描述符分配给 I/O worker。目标 worker 接管描述符，并构造自己的 `ClientConnection`、解析器、Buffer、Timer、`ProxyTransaction`、上游连接池和选择状态。连接、Buffer、Channel、Timer 和连接池不会迁移或跨 worker 共享。

每个请求由 `ProxyTransaction` 处理：选择上游，在需要时预留准入和结果容量，转发请求，并将上游结果映射为下游响应。只有在下游响应头尚未提交、且请求满足幂等条件时，才允许重试。

## 控制面

`RuntimeGeneration` 将不可变配置快照与该代的 coordinator、准入状态、OutcomeChannel 和每 worker SelectionState 组织在一起。新配置先在所有 worker 上 prepare，再原子发布。已在飞的请求始终持有原 generation，retry 时不会转而查询新 coordinator。

健康检查与熔断 coordinator 只有一个状态转换 owner。worker 读取已发布决策并提交结果，不直接修改共享 breaker 状态。全局 admission / in-flight 统计与 worker 本地 least-active 计数严格分离。

## 跨线程规则

- 工作只能通过有界队列和 wake fd 跨线程传递。
- `eventfd` 只用于唤醒 event loop，绝不承载请求数据。
- 描述符必须使用 `FdOwner` 转移，确保恰好一个关闭责任方。
- 不得从外部线程调用 `EventLoop`、`Channel`、`ClientConnection`、`UpstreamConnection`、`TimerQueue` 或 `UpstreamPool` 的 owner API。

## 背压与关闭

流式响应以高低水位控制背压：下游 Buffer 越过高水位时暂停读取上游，低于低水位后恢复。下游响应头一旦提交，后续上游失败只会截断流，不会补发合成错误响应。

关闭会先停止 accept，再销毁 loop 所属对象。销毁任务投递到 owner worker；退役 generation 依次停止 checker、等待请求 lease、drain outcome、停止 coordinator，最后才释放运行时状态。
