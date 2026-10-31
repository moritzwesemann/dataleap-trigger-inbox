# Load test: Trigger Inbox

2026-10-30 20:09 · darwin/arm64 · 12 CPUs · Postgres 18.6 (Homebrew), wal_sync_method=fsync_writethrough · go1.27.2

How fast the inbox accepts webhooks while 200 workers keep running simulated agents. The runs are sped up: they take 50–200 ms instead of ~30 seconds.

## burst

10000 webinar registrations for one agent arrive at the same moment. 200 senders push them in parallel, like a load balancer in front of the inbox would: a single machine cannot open 10000 new connections in the same instant.

| events | all answered after | answered 200 | errors | p50 | p95 | p99 | stored | **200 but not stored** |
|---|---|---|---|---|---|---|---|---|
| 10000 | 0.8 s | 10000 | 0 | 16.2 ms | 23.8 ms | 26.9 ms | 10000 | **0** |

## ingest

How fast the inbox accepts webhooks for 1,000 agents while the workers keep running. Each rate runs for 10s.

| target rate | achieved | sent | answered 200 | errors | p50 | p95 | p99 | stored | **200 but not stored** | still waiting at the end |
|---|---|---|---|---|---|---|---|---|---|---|
| 1000/s | 1000/s | 10000 | 10000 | 0 | 6.4 ms | 8.5 ms | 11.2 ms | 10000 | **0** | 10 |
| 5000/s | 4993/s | 50000 | 50000 | 0 | 9.0 ms | 13.8 ms | 17.0 ms | 50000 | **0** | 36002 |
| 10000/s | 9984/s | 100000 | 100000 | 0 | 10.0 ms | 16.4 ms | 22.7 ms | 100000 | **0** | 85556 |
| 20000/s | 19933/s | 200000 | 200000 | 0 | 11.8 ms | 23.2 ms | 38.8 ms | 200000 | **0** | 185581 |
