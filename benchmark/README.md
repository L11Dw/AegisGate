# Reproducible benchmark procedure

This directory deliberately contains no claimed throughput or latency figures.
Record a result only with the exact command, host details, gateway revision, configuration, and raw tool output.

## Start the fixed demo

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Normal traffic uses `Host: normal.demo.local`; the fault route uses `Host: fault.demo.local` and its mock backend returns `503`.

## Capture a baseline

Use a separate terminal and retain the raw `wrk` output:

```bash
wrk -t 2 -c 32 -d 30s -H 'Host: normal.demo.local' http://127.0.0.1:8080/
curl -s -H 'Host: ignored.local' http://127.0.0.1:8080/metrics
```

For tail latency, use `wrk2` with an explicitly chosen fixed rate:

```bash
wrk2 -t 2 -c 32 -R 500 -d 60s -H 'Host: normal.demo.local' http://127.0.0.1:8080/
```

## Result record template

```text
revision:
build type / compiler:
CPU / RAM / kernel:
container runtime version:
command:
gateway configuration checksum:
duration / connections / threads / target rate:
QPS:
P50 / P99 latency:
HTTP status distribution:
CPU / RSS:
raw-output file:
comparison baseline and observed delta:
```

Do not compare results collected on different hosts, commands, connection counts, or configurations as an optimization result.
