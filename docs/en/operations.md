# Operations

[English](operations.md) | [简体中文](../zh-CN/operations.md)

## Start

The server accepts a YAML configuration path and listen port. `--log-path` enables JSON Lines output without placing file I/O on request workers.

```bash
./build/release/aegisgate_server ./configs/demo.yaml 8080 --log-path ./aegisgate.jsonl
```

Use literal IPv4 addresses for upstream endpoints in this release. A configuration declares a fixed `workers` value; changing that value during reload is rejected and the old configuration remains active.

## Reload safely

Write a complete replacement file and atomically rename it over the configured path, then send SIGHUP:

```bash
mv ./configs/next.yaml ./configs/demo.yaml
kill -HUP <gateway-pid>
```

The watcher observes the configuration directory so that atomic rename works. Reload requests are debounced and coalesced. Parsing happens away from the control loop; a candidate is prepared on all workers before publication. Invalid YAML, an incompatible worker count, or a failed prepare leaves the published generation unchanged.

New requests bind the new generation after publication. Existing requests, retries, streams, admission leases, and outcome accounting retain their original generation until completion.

## Shutdown

Send SIGTERM to stop the server. The gateway stops acceptance, rejects new control work, drains owner-thread destruction tasks, and tears down active and retiring generations without cross-thread object destruction. Do not kill -9 a live process as an ordinary shutdown mechanism; it prevents orderly log drain and connection closure.

## Production notes

Run the process under a service manager with a writable log directory, a file-descriptor limit appropriate to expected concurrency, and a configuration deployment procedure based on complete files plus rename. Use `/metrics` and JSONL logs for operational observation; do not scrape or parse request bodies.
