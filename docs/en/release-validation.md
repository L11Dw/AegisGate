# Release validation

[English](release-validation.md) | [简体中文](../zh-CN/release-validation.md)

## Required commands

Run the following from a clean, configured checkout:

```bash
cmake --build build/debug --target aegisgate_tests
ASAN_OPTIONS=detect_leaks=0 ctest --test-dir build/debug --output-on-failure --timeout 30

cmake --build build/plain --target aegisgate_tests
ctest --test-dir build/plain --output-on-failure --timeout 30

./scripts/tsan-verify.sh
git diff --check
```

Run the public deployment check when Docker is available:

```bash
./scripts/verify-compose-demo.sh
```

The script builds and starts the Compose demo, verifies a routed request and `/metrics`, verifies JSONL logs, replaces a copied configuration, sends SIGHUP, checks reload success and new-upstream traffic, and always removes demo containers and volumes.

## Evidence standard

All commands must complete successfully on the release candidate. A skipped environment-dependent demo must be reported as a skip with its reason; it must not be represented as a pass. Benchmark JSON files record the machine and revision that produced them and are not checked into the repository.
