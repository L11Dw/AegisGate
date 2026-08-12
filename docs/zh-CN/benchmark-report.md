# 基准方法与证据

[English](../en/benchmark-report.md) | [简体中文](benchmark-report.md)

## 目的

benchmark 是可复现的行为与测量工具，不是泛化性能承诺。结果会随 CPU 拓扑、内核、编译器、socket buffer、Docker 和上游实现而变化。

## 运行矩阵

normal 转发以 1、2、4 worker 运行，先预热；生成的 JSON 保持在 Git 之外：

```bash
./benchmark/run.sh normal 1 5 10 3
./benchmark/run.sh normal 2 5 10 3
./benchmark/run.sh normal 4 5 10 3
```

脚本输出 git SHA、CPU、kernel、worker 数、请求数、错误数、RPS、延迟分位、日志丢弃计数和场景细节。

## 语义场景

| 场景 | 必须观察到的结果 |
|---|---|
| `admission` | 受限全局预算下，同时存在 200 和 429。 |
| `breaker` | 后端失败打开熔断；同端口健康后端使状态恢复 Closed。 |
| `slow_client` | 512 KiB body 完整接收，且上游 pause / resume 计数均递增。 |
| `reload` | 原子替换配置并发送 SIGHUP 后产生 `reload_succeeded`，后续流量到 E2。 |

## 优化政策

P-1 到 P-5 不会在缺乏证据时采用。只有重复测量显示吞吐至少提升 3%、P99 不恶化超过 5%、并且所有语义与 sanitizer 门禁仍通过时，才保留优化。本次发布不宣称任何 benchmark 驱动的生产优化。
