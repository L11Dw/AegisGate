# AegisGate：高性能 HTTP API 网关

## 1. 项目定位

**AegisGate 是一个运行在 Linux 上、面向多个 HTTP 后端实例的高性能反向代理网关。**

它解决的不是“把请求从 A 转发到 B”这么简单的问题，而是在线服务常见的三个矛盾：

1. 流量突增时，网关不能无限把请求压给下游；
2. 某个实例变慢或故障时，客户端不能持续撞向坏实例；
3. 为了排障和容量评估，必须知道请求在哪个阶段变慢、被限流或被熔断。

项目的交付形态是一套可运行的 C++ 服务端程序、可控的 Mock 下游服务和一套可复现压测脚本。它不是 WebServer 教程，也不试图复刻 Envoy、Nginx 或 Service Mesh。

> **项目状态**：当前仓库处于设计与实现准备阶段。本文定义目标架构和验收标准，不代表其中全部能力已经实现。每项能力只有在代码、测试与可复现实验一并提交后，才会在 README 中标记为已完成。

## 2. 真实使用场景

一个中小型在线业务通常会有多个无状态服务实例：用户服务、订单服务、文件服务等。客户端若直接访问后端，无法统一实施限流、超时、灰度路由和故障隔离。

```text
                 +---------------------+
Client ---HTTP-->|     AegisGate       |
                 | route / balance     |
                 | limit / retry       |
                 | health / metrics    |
                 +----+---------+------+ 
                      |         |
          +-----------+--+   +--+------------+
          | order-1      |   | order-2       |
          | slow/fault   |   | normal        |
          +--------------+   +---------------+
```

MVP 自带两个可控的 Mock 后端：一个正常实例和一个可注入延迟、5xx、连接中断或并发上限的故障实例。完整交付版的 Docker Demo 再增加 `order`、`inventory`、`file` 三类后端路由，用于展示多路由场景。这样可以真实验证：AegisGate 是否在故障发生后切走流量、是否阻止重试风暴、是否保持尾延迟稳定。

## 3. 成功定义

完成项目不以“代码量”或预设 QPS 为标准，而必须同时满足：

- 多个客户端可经网关稳定访问多个 HTTP/1.1 后端；
- 一个后端被注入高延迟或持续 5xx 后，流量能够被健康检查/熔断逻辑隔离；
- 单条路由可配置限流、超时、重试和负载均衡策略；
- 网关可观测：请求数、失败数、限流数、各后端延迟、熔断状态均可查询；
- 进程在 ASan/UBSan 下通过核心集成测试；
- 给出固定机器、固定负载下的压测方法与结果，至少包含 QPS、P50/P99、错误率、CPU、内存和优化前后对比；
- README 中可用一条命令启动 Demo，并复现“后端变慢后自动保护”的演示。

## 4. 范围控制

项目按三个验收层交付。MVP 与完整交付版均有独立、可验证的成品定义；可选实验只在前两层稳定后开展，且不会阻塞主项目交付。

### 4.1 MVP：先证明代理路径正确

- 单 I/O EventLoop 的 HTTP/1.1 反向代理；
- 请求行、常用 Header、`Content-Length` Body 与客户端 Keep-Alive；仅支持带 `Content-Length` 的请求和响应；
- 上游 Keep-Alive 复用：单 EventLoop 内为每个上游保留少量空闲连接；不支持 HTTP pipelining，一条上游连接同一时刻仅服务一个请求；
- 路由：按 `Host + Path 前缀` 转发至指定上游集群；
- 加权轮询；
- 路由级令牌桶限流与最大在途请求数；
- 连接超时、首字节超时、总请求超时；仅对 `GET`/`HEAD` 的单次重试；
- Prometheus 文本指标端点：请求数、请求延迟、限流数和在途请求数；
- 两个可注入延迟、5xx、连接中断的 Mock 后端；
- 固定配置、单元测试、集成测试和压测基线。

### 4.2 完整交付版：证明能保护下游

- 熔断器：`Closed -> Open -> HalfOpen -> Closed`，并结合被动失败统计暂时摘除故障实例；
- 主动健康检查：周期性 `GET /healthz`；
- 最少活跃请求负载均衡；
- 多 I/O worker；连接、请求状态与上游空闲连接仍保持线程内归属；
- 写高水位线、慢客户端/慢上游背压；
- YAML 配置校验、`SIGHUP` 异步热加载；
- 结构化异步日志、Docker Compose Demo、Sanitizer 与完整故障注入测试；
- `perf` 定位并解决一个可复现热点，输出优化前后基准报告。

### 4.3 可选对照实验：自适应过载保护

仅在完整交付版稳定后进行。将固定最大在途请求数作为基线，实现一个带最小/最大边界和缓慢调节速率的简单 AIMD/阈值控制器；根据窗口内 P99、错误率和上游在途请求数调整并发上限。必须在突发流量和慢后端场景中，与静态策略对比吞吐、P99、错误率和恢复时间。没有实测优势时，不将“自适应”写入简历。

### 4.4 明确不做

- HTTP/2、HTTP/3、gRPC、WebSocket、TLS 终止；
- `Transfer-Encoding: chunked` 与 HTTP pipelining；
- 服务发现中心、xDS、Kubernetes 集成、Service Mesh；
- 多机集群、Raft、高可用控制面；
- 全量鉴权体系、用户/角色管理后台；
- 缓存代理、复杂 WAF、插件市场；
- 对 Envoy 或 Nginx 的协议/API 兼容承诺。

这些不是“未来规划未完成项”，而是本项目有意识地不承担的产品目标。完成 MVP 与完整交付版，即是一个边界完整的成品。

### 4.5 协议边界与不支持行为

HTTP 报文可在 TCP 层以任意大小分段到达，解析器必须正确处理半包、粘包与跨 `epoll` 事件的增量解析；这与 HTTP 的 `Transfer-Encoding: chunked` 完全不同。

- 收到客户端 `Transfer-Encoding: chunked`、请求 HTTP pipelining 或超出 Header/Body 上限的请求时，网关返回明确的 `400` 或 `501`，并关闭该客户端连接；
- 收到上游 chunked 响应时，网关记录协议不支持错误、关闭对应上游连接，并向客户端返回 `502`；
- 一条 Keep-Alive 上游连接同一时刻只服务一个请求。MVP 在一个请求完成前不读取该客户端连接上的下一条请求，避免隐式支持 pipelining。

## 5. 核心架构

### 5.1 线程模型

```text
MVP
  └─ 一个 EventLoop 同时接收客户端与驱动上游连接；异步日志、主动健康检查和多 worker 均不在 MVP 内。

完整交付版
acceptor thread
  └─ 接收客户端 fd，并分配给 N 个 I/O worker

I/O worker (每核一个 EventLoop)
  ├─ client connections
  ├─ upstream connections / connection pool
  ├─ HTTP parser and request state machine
  ├─ route, rate limit, balance, timeout, circuit state
  └─ eventfd task queue（热配置切换等跨线程任务）

background workers
  ├─ async log writer
  ├─ health-check scheduler
  └─ metrics snapshot / config parser
```

MVP 先用单线程消除并发状态的干扰；进入完整交付版后，连接、请求上下文和连接池严格归属某个 I/O worker，正常转发路径不加全局互斥锁。慢日志与配置解析不能阻塞 I/O worker。

路由级限流在 MVP 中由单 EventLoop 的令牌桶精确执行。完整交付版中，路由的配置限额仍是全局语义：全局原子令牌预算按时间窗口补充，worker 以小批量领取令牌并在线程内消费，避免每个 worker 各自持有完整额度而把总 RPS 放大为 worker 数的倍数。最大在途请求数使用路由级原子计数，成功准入后递增、请求最终完成时递减。

### 5.2 数据路径

1. Acceptor 接收连接，设置 `O_NONBLOCK`，交给一个 I/O worker。
2. Worker 用 `epoll` 监听读写事件，增量解析 HTTP 请求。
3. 根据路由匹配结果检查令牌桶、并发上限和熔断状态。
4. 负载均衡器选择健康上游，从该 EventLoop/worker 的空闲 Keep-Alive 连接复用或新建连接。
5. 将请求写入上游；响应以流式方式回传客户端，避免无必要的全量 Body 拷贝。
6. 请求结束时记录结果、延迟和字节数；若符合条件更新熔断器与健康状态。

### 5.3 背压与资源上限

每个连接设置读/写高水位线。上游写缓冲超过高水位时，暂停读取客户端 fd；客户端持续慢读并导致响应缓存超标时，记录原因并断开连接。每条路由同时限制：

- 每秒令牌数与突发容量；
- 最大在途请求数；
- 单请求 Header/Body 上限；
- 上游连接数与空闲连接数。

这四类上限优先于“尽可能多接请求”。网关的职责是保护下游和自身，而不是在过载时耗尽内存。

## 6. 关键设计

### 6.1 路由与配置

MVP 只在启动时加载并校验一份 YAML 配置，配置错误即拒绝启动。完整交付版中，配置文件在后台线程解析与校验，成功后生成不可变 `RouteTable`，再以原子共享指针切换。已有请求继续使用旧快照，新请求读取新快照，因此热更新不需要暂停流量。

MVP 配置示例：

```yaml
routes:
  - name: order-api
    match: { host: api.demo.local, path_prefix: /orders }
    upstream: order_cluster
    connect_timeout_ms: 30
    first_byte_timeout_ms: 80
    request_timeout_ms: 150
    retries: 1
    rate_limit: { rps: 1200, burst: 200 }
    max_inflight: 500
    balance: weighted_round_robin

upstreams:
  - name: order_cluster
    endpoints:
      - { address: 127.0.0.1:9001, weight: 2 }
      - { address: 127.0.0.1:9002, weight: 1 }
```

### 6.2 超时、重试与熔断

- 连接建立超时从发起非阻塞 `connect` 起计，到连接完成或失败为止；上游首字节超时从完整请求写入上游起计，到收到响应首字节为止；端到端请求超时从客户端请求被完整解析起计，到向客户端完成响应或失败为止；
- 仅 `GET`、`HEAD` 或显式标记为幂等的请求允许自动重试；
- 只在连接建立失败、未收到响应头的连接错误或明确可重试的 5xx 时重试；
- 仅在尚未向客户端发送任何上游响应字节时，才允许丢弃当前上游尝试、切换上游并重试；
- 路由有独立重试预算，避免下游已故障时放大流量；
- 熔断器在完整交付版实现，状态范围为 `route × endpoint`，由各 worker 共享。统计采用固定大小时间桶、每桶原子成功/失败计数和单个状态原子变量；worker 只更新计数，请求结束路径或定时检查统一通过 CAS 执行状态切换。达到最小请求数后按窗口错误率触发 Open；Open 状态仅放少量探测请求进入 HalfOpen；
- 超时属于网关端到端预算的一部分，重试会消耗剩余预算，不会无限延长客户端等待时间。

### 6.3 负载均衡与健康状态

先实现加权轮询，作为稳定、易验证的基线；再实现最少活跃请求，用于请求耗时差异大的服务。健康检查与熔断状态分离：前者是所有 worker 共享的 endpoint 可用性状态，后者是 `route × endpoint` 的运行时保护状态。主动健康检查只更新前者；请求失败统计只更新后者，二者任一拒绝流量时均不选择该实例。

### 6.4 可观测性

每个请求生成或透传 `X-Request-ID`。完整交付版的日志使用 JSON Lines，字段至少含路由、上游、状态码、延迟、重试次数、限流/熔断原因与收发字节数。

`/metrics` 输出：

- `aegisgate_requests_total`（route/status/upstream）；
- `aegisgate_request_duration_seconds`（直方图）；
- `aegisgate_rate_limited_total`；
- `aegisgate_active_connections` / `inflight_requests`；

完整交付版额外输出：

- `aegisgate_retries_total`；
- `aegisgate_circuit_state`；
- `aegisgate_upstream_health`。

## 7. 技术选型

| 层 | 选择 | 原因 |
|---|---|---|
| 语言 | C++20 | `std::span`、`std::jthread`、原子与 RAII 工具足够成熟 |
| 平台 | Linux x86_64 | 直接使用 epoll、eventfd、timerfd、perf、Sanitizer |
| 构建 | CMake + Ninja | 与 C++ 工程和 CI 常用工作流一致 |
| 网络 | 自研精简 Reactor | 项目重点是连接管理、事件循环和代理数据路径，不将完整网络库作为另一个项目重做 |
| 配置 | yaml-cpp | YAML 易读；MVP 启动时加载，热更新仅在完整交付版进入控制面 |
| 测试 | GoogleTest + Python 黑盒压测 | 单元、集成、故障注入各有合适工具 |
| 性能诊断 | wrk/wrk2、perf、FlameGraph、ASan/UBSan | 结果可复现、可解释 |

第三方库只承担非核心能力；HTTP 解析、连接状态机、限流、熔断、负载均衡、连接池和背压均由项目实现。

## 8. 目录设计

```text
AegisGate/
├── apps/
│   ├── aegisgate/                 # 网关可执行程序
│   └── mock-backend/              # 可注入延迟和错误的 Demo 服务
├── include/aegisgate/
├── src/
│   ├── net/                       # epoll、EventLoop、Buffer、Connection
│   ├── http/                      # parser、request/response、header
│   ├── proxy/                     # downstream/upstream、连接池、转发状态机
│   ├── routing/                   # RouteTable、路由匹配、热更新
│   ├── resilience/                # token bucket、concurrency limit、retry、circuit
│   ├── health/                    # health checker、endpoint state
│   ├── observability/             # log、metrics、request id
│   └── config/
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fault_injection/
├── benchmark/
├── deploy/docker-compose.yml
├── configs/demo.yaml
└── docs/
```

## 9. 里程碑与验收

### M1：可代理（MVP）

- EventLoop、Connection、Buffer、HTTP/1.1 请求解析；
- 单 Route、单上游连接、客户端与上游 Keep-Alive；
- `curl` 与基础集成测试通过。

### M2：MVP 验收

- 路由表、多上游实例、加权轮询；
- 令牌桶、最大在途请求、超时和 `GET`/`HEAD` 单次重试；
- Mock 后端故障注入与端到端压测基线建立。

### M3：可保护（完整交付版）

- 熔断、主动健康检查、最少活跃请求和幂等重试预算；
- 后端延迟/错误注入测试；
- 多 I/O worker、高水位线、慢客户端与慢上游背压验证。

### M4：可证明（完整交付版）

- Metrics、异步 JSON 日志、请求关联；
- `perf` 找出一个真实热点并完成优化；
- ASan/UBSan、压力回归、Docker Demo、架构与基准报告。

M4 全部完成后，项目即完成。若时间充足且基准稳定，再做第 4.3 节的自适应控制对照实验；它不会阻塞项目交付。

## 10. 测试与压测方案

### 正确性

- TCP 半包、粘包、任意分段到达、非法 Header、超大 Body；
- 明确拒绝 `Transfer-Encoding: chunked` 与 HTTP pipelining；
- 多路由匹配、配置热加载前后请求一致性；
- 上游连接重用、上游主动断开、客户端中途取消；
- 限流边界、重试预算耗尽、熔断状态转换；
- 后端连续 5xx、超时、恢复后的 HalfOpen 探测；
- 进程重启与资源泄漏检查。

### 性能

固定压测环境，记录 CPU 型号、核心数、内存、Linux 内核、编译器和构建参数。分别测试：

1. 纯代理基线：小 JSON 响应、健康后端；
2. 多连接 Keep-Alive：不同并发连接数和请求体大小；
3. 慢上游：观察 P99、队列长度和拒绝比例；
4. 故障上游：观察错误隔离与重试放大是否受控；
5. 优化前后：例如普通拷贝写 vs `writev`/缓冲区复用。

报告只陈述本机实测数字，不预先宣称“十万 QPS”或“生产级”。

## 11. 关键技术取舍

- 为什么请求状态限定在线程内，而不是使用全局锁保护连接池；
- 为什么重试仅限幂等请求，且需要重试预算和端到端超时预算；
- 限流、最大在途请求与背压分别保护什么资源；
- 健康检查与熔断为何不能合并为一个状态；
- 如何避免配置热加载与正在执行请求互相干扰；
- 为什么网关瓶颈不一定在 epoll，而可能在 HTTP 解析、内存拷贝、上游连接重建或日志 I/O；
- 如何用基准数据证明优化有效，而不是只展示代码技巧。

## 12. 设计参考

- Envoy：高性能边缘/中间/服务代理的产品定位与工程组织：<https://github.com/envoyproxy/envoy>
- Dragonfly：shared-nothing 分片和低尾延迟数据路径的设计启发：<https://github.com/dragonflydb/dragonfly>
- bRPC：高性能 C++ 服务中的异步处理、监控与压测能力参考：<https://github.com/apache/brpc>
