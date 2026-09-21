# Load test: PostgreSQL load with and without the Valkey queue

This is the measurement behind the library's claim: moving the hot queue path to Valkey removes the
idle worker poll from PostgreSQL. It reports what was actually measured, including the parts that did
not improve.

The harness is in [`benchmarks/`](../benchmarks/ITB.HangfirePostgreSql.ValkeyQueue.Benchmarks) and is
runnable — every number below can be reproduced with the command in [Running it](#running-it).

## What is measured

PostgreSQL-side load is read from `pg_stat_statements`, which counts the statements the **server**
executed, rather than what the client believes it sent. Two phases:

| Phase | Setup | Question it answers |
| --- | --- | --- |
| **Idle** | A Hangfire server with an empty queue, observed for 120 s | What does an idle worker cost PostgreSQL? |
| **Throughput** | 2 000 jobs enqueued, then drained by 20 workers | What does each job cost PostgreSQL? |

Each arm runs in its own Hangfire schema (`bench_baseline`, `bench_valkey`), dropped and recreated per
run, so the two cannot see each other's rows.

Counted statements are those naming the arm's schema. Hangfire quotes identifiers
(`"bench_valkey"."job"`), so bare `BEGIN` / `COMMIT` / `DISCARD ALL` name no table and fall outside
that filter — they are real round trips, so they are reported separately as *transaction-control
calls* rather than dropped.

`QueuePollInterval` is left at the Hangfire default of 15 s. Lowering it would inflate the baseline's
idle load and flatter the Valkey arm.

## Results

Two consecutive runs on the environment below. Query **counts** were identical across both runs;
only wall-clock timings moved, so those are given as a range.

| Metric | PostgreSQL only | With Valkey | Change |
| --- | ---: | ---: | ---: |
| Idle Hangfire queries (120 s) | 164 | 4 | **−97.6 %** |
| Idle Hangfire queries / second | 1.37 | 0.03 | **−97.6 %** |
| Idle transaction-control calls | 486 | 6 | **−98.8 %** |
| Queries during drain (2 000 jobs) | 64 000 | 56 000 | −12.5 % |
| Transaction-control calls during drain | 58 002 | 44 002 | −24.1 % |
| Queries per job | 32.00 | 28.00 | −12.5 % |
| PostgreSQL exec time during drain | 1 576–1 598 ms | 1 318–1 362 ms | −15 % |
| Drain duration | 6.9 s | 6.5–6.8 s | −2 to −6 % |
| Throughput | 290–292 jobs/s | 296–310 jobs/s | +2 to +6 % |

Both arms completed 2 000/2 000 jobs.

### Reading these numbers

**The idle result is the real one.** An idle Hangfire server drops from ~1.37 queries/s against
PostgreSQL to ~0.03 — a 97.6 % reduction, and the reason the library exists. The remaining 4 queries
are the maintenance pass (reconciler + orphan sweep), not polling. In a deployment where workers are
idle most of the time — the common case for report/notification queues — this is close to the entire
PostgreSQL load from Hangfire.

**The per-job result is modest and should not be oversold.** 32 → 28 queries per job is −12.5 %,
because PostgreSQL remains the durable store: the job row, its parameters, and every state transition
are still written there. Only the queue hop moves. A library that claimed a large throughput win here
would be measuring something other than Hangfire.

**Throughput is essentially unchanged.** The +2–6 % is within the run-to-run noise of this
environment and should be read as "no regression", not as a speedup. Both arms are bounded by
PostgreSQL state writes, not by the queue.

This matches the README's framing: on Hangfire.PostgreSql 1.21.1 the baseline already wakes idle
workers via `LISTEN`/`NOTIFY`, so the pickup-latency win is marginal. The win is idle database load.

### What this test does not cover

- **A single machine.** Both databases are local containers; there is no network latency, which in a
  real deployment (ElastiCache + Aurora across AZs) affects both arms but not equally.
- **Empty jobs.** `BenchmarkJob` does nothing, so queue overhead is the whole cost. Real jobs that do
  work will show a smaller relative difference.
- **A single node.** No Valkey cluster, no PostgreSQL replica, no failover during the run.
- **Short duration.** 120 s idle and ~7 s of drain. It does not speak to connection-pool behaviour,
  memory growth, or Valkey eviction over days.

Treat these as directional evidence from a controlled environment, not as a projection of production
numbers. Run it against your own workload before relying on it.

## Environment

| | |
| --- | --- |
| Host | Apple M2 Pro, 10 cores, 16 GB RAM, macOS 26.6.2 |
| .NET | 10.0.102 |
| PostgreSQL | 17.10 (`postgres:17-alpine`, Docker) |
| Valkey | 8.1.10 (`valkey/valkey:8-alpine`, Docker) |
| Hangfire.Core | 1.8.23 |
| Hangfire.PostgreSql | 1.21.1 |
| Parameters | 2 000 jobs, 20 workers, 120 s idle, `QueuePollInterval` 15 s |

This is a developer machine, not production hardware. The absolute throughput figures are a property
of this laptop; the **relative** query-count differences are the transferable result, and those were
stable across runs.

## Running it

`pg_stat_statements` must be preloaded — without it the harness fails loudly rather than reporting
zero load for both arms.

```bash
docker run -d --name hfvq-pg \
  -e POSTGRES_PASSWORD='Asd123!1' -e POSTGRES_DB=hangfire_bench \
  -p 55432:5432 postgres:17-alpine \
  -c shared_preload_libraries=pg_stat_statements \
  -c pg_stat_statements.track=all

docker run -d --name hfvq-valkey -p 56379:6379 valkey/valkey:8-alpine

dotnet run --project benchmarks/ITB.HangfirePostgreSql.ValkeyQueue.Benchmarks -c Release -- \
  --jobs 2000 --workers 20 --idle-seconds 120
```

The run takes roughly five minutes, most of it the two idle phases. It exits non-zero if either arm
fails to drain every job.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--jobs` | 2000 | Jobs enqueued in the throughput phase |
| `--workers` | 20 | Hangfire worker threads |
| `--idle-seconds` | 60 | Length of the idle observation |
| `--postgres` | `Host=localhost;Port=55432;…` | PostgreSQL connection string |
| `--valkey` | `localhost:56379` | Valkey connection string |

Cleanup: `docker rm -f hfvq-pg hfvq-valkey`.
