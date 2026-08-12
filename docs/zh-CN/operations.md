# 运维与重载

[English](../en/operations.md) | [简体中文](operations.md)

## 启动

服务端接收 YAML 配置路径和监听端口。`--log-path` 会启用 JSON Lines 输出，但不会把文件 I/O 放到请求 worker 上。

```bash
./build/release/aegisgate_server ./configs/demo.yaml 8080 --log-path ./aegisgate.jsonl
```

本版本的上游端点使用字面量 IPv4。配置中的 `workers` 在启动期固定；reload 时若改变该值，候选配置会被拒绝，旧配置继续服务。

## 安全重载

写入完整的新文件，原子 rename 覆盖原配置，再发送 SIGHUP：

```bash
mv ./configs/next.yaml ./configs/demo.yaml
kill -HUP <gateway-pid>
```

watcher 监听配置所在目录，因此支持原子 rename。重载请求会 debounce 和 coalesce；解析不在控制 loop 中执行；候选 generation 必须在所有 worker prepare 成功后才发布。无效 YAML、worker 数不兼容或 prepare 失败都不会影响已发布 generation。

发布后新请求绑定新 generation；已在飞的请求、retry、流、admission lease 和 outcome 记账仍持有旧 generation，直至结束。

## 关闭

发送 SIGTERM 关闭服务。Gateway 会停止 accept、拒绝新的控制任务、drain owner-thread 销毁任务，并且不通过跨线程析构来清理 active / retiring generation。普通关闭不要使用 kill -9，否则无法有序 drain 日志和连接。

## 部署建议

建议由服务管理器管理进程，配置可写日志目录和足够的文件描述符上限；配置部署使用“完整文件 + rename”。通过 `/metrics` 和 JSONL 日志观察运行状态，不要抓取或解析请求 body。
