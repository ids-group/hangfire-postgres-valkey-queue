# ITB.HangfirePostgreSql.ValkeyQueue

A Valkey/Redis-backed queue provider for **Hangfire.PostgreSql**. PostgreSQL stays the durable source
of truth (job data, state history, scheduled/recurring jobs, dashboard); only the hot queue path (job
IDs) moves to Valkey — `LPUSH` to enqueue, blocking `BLMOVE` to dequeue — so idle worker polling load
on PostgreSQL drops toward zero.

## Measured effect

PostgreSQL-side load counted from `pg_stat_statements` — the statements the server actually executed.
Two arms in separate schemas: stock Hangfire.PostgreSql, and the same setup with this provider.

| Metric | PostgreSQL only | With Valkey | Change |
| --- | ---: | ---: | ---: |
| Idle Hangfire queries (120 s) | 164 | 4 | **−97.6 %** |
| Idle queries / second | 1.37 | 0.03 | **−97.6 %** |
| Idle transaction-control calls | 486 | 6 | **−98.8 %** |
| Queries per job (2 000 jobs) | 32.00 | 28.00 | −12.5 % |
| PostgreSQL exec time during drain | 1 576–1 598 ms | 1 318–1 362 ms | −15 % |
| Throughput | 290–292 jobs/s | 296–310 jobs/s | +2 to +6 % |

**The idle row is the result that matters.** An idle Hangfire server drops from ~1.37 queries/s to
~0.03; the remaining 4 queries are the maintenance pass, not polling. Per-job cost falls only 12.5 %
because PostgreSQL still stores the job row, its parameters and every state transition — only the
queue hop moves. Throughput is unchanged: the +2–6 % is run-to-run noise, and should be read as "no
regression" rather than a speedup.

Measured on an Apple M2 Pro with PostgreSQL 17 and Valkey 8 in local containers, 20 workers,
`QueuePollInterval` at the 15 s default. Absolute figures are a property of that machine; the
relative query counts were identical across runs. Methodology, the reproducible harness and what the
test does *not* cover:
[docs/LOAD-TEST.md](https://github.com/ids-group/hangfire-postgres-valkey-queue/blob/main/docs/LOAD-TEST.md).

> **Where the win is.** On Hangfire.PostgreSql 1.21.1 the baseline already wakes idle workers via
> PostgreSQL `LISTEN`/`NOTIFY`, so the pickup-latency win is marginal and the reduction under
> *sustained* load is modest. The win is idle database load — measure against your own workload
> before adopting it, and keep pure-PostgreSQL as the fallback.

## Installation

```bash
dotnet add package ITB.HangfirePostgreSql.ValkeyQueue
```

Targets .NET 10. Requires Hangfire.PostgreSql 1.21.x and a Valkey (or Redis) 7+ server.

## Usage

```csharp
using Hangfire;
using Hangfire.PostgreSql;
using Hangfire.PostgreSql.Factories;
using Hangfire.PostgreSql.ValkeyQueue;
using Hangfire.Server;
using StackExchange.Redis;

var storageOptions = new PostgreSqlStorageOptions { UseSlidingInvisibilityTimeout = true };
var storage = new PostgreSqlStorage(
    new NpgsqlConnectionFactory(pgConnectionString, storageOptions), storageOptions);

var redisOptions = ConfigurationOptions.Parse(valkeyConnectionString);
var mux = ConnectionMultiplexer.Connect(redisOptions);

var valkey = new ValkeyQueueOptions
{
    InvisibilityTimeout = TimeSpan.FromMinutes(30),   // MUST exceed your longest job
    BlockingConnectionConfig = redisOptions,          // required on managed Valkey (TLS + AUTH)
};

storage.UseValkeyQueues(mux, valkey);                 // point the "default" queue at Valkey

services.AddHangfire(c => c.UseStorage(storage));
services.AddHangfireServer();

// Safety nets — recover from Valkey data loss / worker death. Register as a singleton
// IBackgroundProcess so AddHangfireServer picks it up.
services.AddSingleton<IBackgroundProcess>(
    new ValkeyQueueMaintenance(mux, valkey, pgConnectionString));
```

A complete, runnable worked example — including the `ValkeyQueueMaintenance` registration that is
easy to forget — is in
[docs/EXAMPLE.md](https://github.com/ids-group/hangfire-postgres-valkey-queue/blob/main/docs/EXAMPLE.md).

## Configuration

`ValkeyQueueOptions`:

| Option | Default | Notes |
| --- | --- | --- |
| `KeyPrefix` | `hangfire:` | Prefix for every Valkey key this provider owns. |
| `BlockTimeout` | 2 s | How long a single `BLMOVE` blocks before the worker loops. |
| `InvisibilityTimeout` | 30 min | A job in a processing list longer than this is treated as orphaned. **Must exceed your longest job.** |
| `MaintenanceInterval` | 15 s | How often the reconciler and orphan sweep run. |
| `ReconcileGrace` | 30 s | The reconciler ignores jobs enqueued more recently than this, so it never races an in-flight `LPUSH`. |
| `Schema` | `hangfire` | The schema Hangfire.PostgreSql created its tables in. |
| `Queues` | `["default"]` | Queues served by Valkey. Anything not listed stays on PostgreSQL. |
| `BlockingConnectionConfig` | `null` | `ConfigurationOptions` for the dedicated blocking connections. **Set this whenever AUTH/TLS is in play** — see below. |

## Correctness

The enqueue is no longer transactional with job creation (it pushes to Valkey, not PostgreSQL), so two
safety nets close every failure window:

- **Reconciler** re-enqueues jobs PostgreSQL reports as `Enqueued` but missing from Valkey (Valkey data
  loss / failover).
- **Orphan sweep** requeues jobs stuck in a processing list past `InvisibilityTimeout` (worker died
  mid-job).

Both live in `ValkeyQueueMaintenance`. **Without it registered, those failures are silent job loss** —
this is the one registration step you cannot skip.

Duplicates are execution-safe: only one worker wins the `Enqueued → Processing` state transition in
PostgreSQL; the loser is discarded.

## Notes

- One dedicated blocking connection per worker thread (`BLMOVE` monopolises its connection) — fine for
  tens of workers, revisit for hundreds.
- On managed Valkey (ElastiCache with TLS + AUTH) set `BlockingConnectionConfig`; otherwise the
  blocking connections are derived from the shared multiplexer's connection string, which masks the
  password and every blocking connection fails to authenticate.
- `InvisibilityTimeout` must exceed your longest-running job or the sweep will re-flag it.
- Keys are wrapped in a `{hash tag}` so all three keys for a queue land in the same cluster hash slot;
  without it, cluster mode rejects the `BLMOVE` with `CROSSSLOT`.

## Links

- [Source and issues](https://github.com/ids-group/hangfire-postgres-valkey-queue)
- [Worked example](https://github.com/ids-group/hangfire-postgres-valkey-queue/blob/main/docs/EXAMPLE.md)
- [Load test results](https://github.com/ids-group/hangfire-postgres-valkey-queue/blob/main/docs/LOAD-TEST.md)

## License

MIT — see
[LICENSE](https://github.com/ids-group/hangfire-postgres-valkey-queue/blob/main/LICENSE).
