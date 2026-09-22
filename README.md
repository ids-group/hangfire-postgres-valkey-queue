# ITB.HangfirePostgreSql.ValkeyQueue

[![NuGet](https://img.shields.io/nuget/v/ITB.HangfirePostgreSql.ValkeyQueue.svg)](https://www.nuget.org/packages/ITB.HangfirePostgreSql.ValkeyQueue/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

A Valkey/Redis-backed queue provider for **Hangfire.PostgreSql**. PostgreSQL stays the durable source
of truth (job data, state history, scheduled/recurring jobs, dashboard); only the hot queue path (job
IDs) moves to Valkey — `LPUSH` to enqueue, blocking `BLMOVE` to dequeue — so idle worker polling load
on PostgreSQL drops toward zero.

In a measured run, an idle Hangfire server's PostgreSQL load fell by **97.6 %** (1.37 → 0.03
queries/s). Per-job cost fell 12.5 %, and throughput was unchanged. Full methodology, environment and
caveats: [docs/LOAD-TEST.md](docs/LOAD-TEST.md).

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
easy to forget — is in [docs/EXAMPLE.md](docs/EXAMPLE.md).

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

The enqueue is no longer transactional with job creation (it pushes to Valkey, not PostgreSQL). Two
things keep that safe.

**Ordering.** The `LPUSH` is deferred to the enqueueing transaction's `TransactionCompleted`, so an id
becomes visible to workers only after PostgreSQL has committed `statename = 'Enqueued'`, and a
rolled-back transaction publishes nothing. Pushing inline would race: a worker parked in `BLMOVE` wakes
in microseconds, reads a job that is not `Enqueued` yet, and Hangfire's `Worker.Execute` discards it
from the queue — recoverable, but only by the reconciler, one `ReconcileGrace` +
`MaintenanceInterval` later.

**Recovery.** Two safety nets close what is left:

- **Reconciler** re-enqueues jobs PostgreSQL reports as `Enqueued` but missing from Valkey (Valkey data
  loss / failover, or a push that failed after the commit).
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

## Development

```bash
# Postgres + Valkey for the tests (pg_stat_statements is only needed for benchmarks)
docker run -d --name hfvq-pg -e POSTGRES_PASSWORD='test_password' \
  -e POSTGRES_DB=hangfire_bench -p 55432:5432 postgres:17-alpine
docker run -d --name hfvq-valkey -p 56379:6379 valkey/valkey:8-alpine

dotnet test
```

The suite covers the queue hand-off, both recovery paths, and the cluster hash-slot invariant.
Endpoints are overridable via `POSTGRES_TEST_CONNECTION`, `REDIS_TEST_HOST` and `REDIS_TEST_PORT`.

## Package README

The text shown on nuget.org is
[`src/ITB.HangfirePostgreSql.ValkeyQueue/PACKAGE.md`](src/ITB.HangfirePostgreSql.ValkeyQueue/PACKAGE.md),
not this file — nuget.org renders it standalone, so its links have to be absolute GitHub URLs. Keep
the two in step when changing usage or configuration.

## License

MIT — see [LICENSE](LICENSE).
