# 发布验证

[English](../en/release-validation.md) | [简体中文](release-validation.md)

## 必需命令

在已配置且工作树干净的 checkout 中运行：

```bash
cmake --build build/debug --target aegisgate_tests
ASAN_OPTIONS=detect_leaks=0 ctest --test-dir build/debug --output-on-failure --timeout 30

cmake --build build/plain --target aegisgate_tests
ctest --test-dir build/plain --output-on-failure --timeout 30

./scripts/tsan-verify.sh
git diff --check
```

Docker 可用时运行公开部署验证：

```bash
./scripts/verify-compose-demo.sh
```

该脚本会构建并启动 Compose demo，验证转发和 `/metrics`、JSONL 日志，替换配置副本、发送 SIGHUP、检查 reload 成功和新上游流量，并始终清理 demo 容器与 volume。

## 证据标准

所有命令都必须在 release candidate 上成功完成。依赖环境的 demo 若无法运行，必须作为带原因的 skip 报告，不能伪装为通过。benchmark JSON 会记录生成它的机器与 revision，且不进入仓库。
