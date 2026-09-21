# Worked example: configuring the Valkey queue

A complete ASP.NET Core worker that runs Hangfire on PostgreSQL with the queue on Valkey. Every line
that matters is here — this is not a fragment.

The code below is compiled as part of CI
([`tests/.../ConfigurationExampleTests.cs`](../tests/ITB.HangfirePostgreSql.ValkeyQueue.Tests/ConfigurationExampleTests.cs)),
so it cannot drift out of date silently.

## 1. Local infrastructure

```bash
docker run -d --name hangfire-pg \
  -e POSTGRES_PASSWORD='test_password' -e POSTGRES_DB=hangfire \
  -p 5432:5432 postgres:17-alpine

docker run -d --name hangfire-valkey -p 6379:6379 valkey/valkey:8-alpine
```

## 2. Packages

```bash
dotnet add package ITB.HangfirePostgreSql.ValkeyQueue
dotnet add package Hangfire.AspNetCore
```

`Hangfire.PostgreSql` and `StackExchange.Redis` arrive transitively.

## 3. `appsettings.json`

```json
{
  "ConnectionStrings": {
    "Postgres": "Host=localhost;Port=5432;Database=hangfire;Username=postgres;Password=test_password",
    "Valkey": "localhost:6379"
  }
}
```

On AWS ElastiCache the Valkey string carries TLS and AUTH, and both matter for step 5:

```json
"Valkey": "my-cache.abc123.use1.cache.amazonaws.com:6379,ssl=True,password=<secret>,abortConnect=False"
```

## 4. `Program.cs`

```csharp
using Hangfire;
using Hangfire.PostgreSql;
using Hangfire.PostgreSql.Factories;
using Hangfire.PostgreSql.ValkeyQueue;
using Hangfire.Server;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var pgConnectionString = builder.Configuration.GetConnectionString("Postgres")!;
var valkeyConnectionString = builder.Configuration.GetConnectionString("Valkey")!;

// --- PostgreSQL: still the durable store -------------------------------------------------
var storageOptions = new PostgreSqlStorageOptions
{
    // Recommended: lets Hangfire extend a running job's invisibility instead of letting a
    // long job look abandoned.
    UseSlidingInvisibilityTimeout = true,
    PrepareSchemaIfNecessary = true,
};

var storage = new PostgreSqlStorage(
    new NpgsqlConnectionFactory(pgConnectionString, storageOptions), storageOptions);

// --- Valkey: the hot queue path ----------------------------------------------------------
// Parse ONCE and keep the result: the same ConfigurationOptions object is handed to the
// multiplexer and to BlockingConnectionConfig below.
var redisOptions = ConfigurationOptions.Parse(valkeyConnectionString);
var mux = ConnectionMultiplexer.Connect(redisOptions);

var valkeyOptions = new ValkeyQueueOptions
{
    Queues = ["default"],

    // MUST exceed your longest-running job, or the orphan sweep will requeue a job that is
    // still being worked and you will run it twice.
    InvisibilityTimeout = TimeSpan.FromMinutes(30),

    // Required whenever AUTH or TLS is in play (i.e. any managed Valkey). Without it the
    // dedicated blocking connections are rebuilt from mux.Configuration, which MASKS the
    // password — they would fail to authenticate and no job would ever be dequeued.
    BlockingConnectionConfig = redisOptions,

    // Must match the schema Hangfire.PostgreSql uses; the reconciler reads it directly.
    Schema = "hangfire",
};

storage.UseValkeyQueues(mux, valkeyOptions);

// --- Wiring ------------------------------------------------------------------------------
builder.Services.AddSingleton<IConnectionMultiplexer>(mux);
builder.Services.AddHangfire(config => config
    .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
    .UseSimpleAssemblyNameTypeSerializer()
    .UseRecommendedSerializerSettings()
    .UseStorage(storage));

builder.Services.AddHangfireServer(options =>
{
    options.WorkerCount = 20;
    options.Queues = ["default"];
});

// --- The safety nets: DO NOT SKIP THIS ---------------------------------------------------
// The enqueue is no longer transactional with job creation, so these two recovery paths are
// what keep the system correct:
//   * the reconciler re-enqueues jobs Postgres reports as Enqueued but Valkey has lost;
//   * the orphan sweep requeues jobs whose worker died mid-execution.
// Registered as a singleton IBackgroundProcess, AddHangfireServer picks it up automatically.
// Without it, Valkey data loss or a worker crash means silent job loss.
builder.Services.AddSingleton<IBackgroundProcess>(
    new ValkeyQueueMaintenance(mux, valkeyOptions, pgConnectionString));

var app = builder.Build();

app.UseHangfireDashboard("/hangfire");

// A job to prove it works.
app.MapPost("/enqueue", (IBackgroundJobClient jobs) =>
{
    var id = jobs.Enqueue(() => Console.WriteLine("processed via the Valkey queue"));

    return Results.Ok(new { jobId = id });
});

app.Run();
```

## 5. Verifying it works

Start the app and enqueue a job:

```bash
curl -X POST http://localhost:5000/enqueue
```

**The queue is on Valkey.** While a job waits, its ID sits in a Valkey list:

```bash
docker exec hangfire-valkey valkey-cli KEYS 'hangfire:*'
# hangfire:{default}:queue        <- waiting job IDs
# hangfire:{default}:processing   <- handed to a worker, not yet acked
# hangfire:{default}:fetched      <- jobId -> fetched-at, used by the orphan sweep
```

The `{default}` braces are a cluster hash tag, so all three keys land in one slot and the
queue→processing `BLMOVE` is legal in cluster mode.

**PostgreSQL is still the source of truth.** The job, its arguments and its state history are there,
and the dashboard at `/hangfire` renders normally:

```bash
docker exec hangfire-pg psql -U postgres -d hangfire \
  -c 'SELECT id, statename FROM hangfire.job ORDER BY id DESC LIMIT 5;'
```

**The polling is gone.** With the app idle, PostgreSQL should be near-silent. Compare against a run
without `UseValkeyQueues` — that is exactly what [LOAD-TEST.md](LOAD-TEST.md) measures.

## Common mistakes

| Symptom | Cause |
| --- | --- |
| Jobs enqueue but nothing is ever processed on managed Valkey | `BlockingConnectionConfig` not set — blocking connections lost the password. |
| A long job runs twice | `InvisibilityTimeout` is shorter than the job; the orphan sweep reclaimed it. |
| Jobs vanish after a Valkey failover | `ValkeyQueueMaintenance` not registered, so the reconciler never runs. |
| `CROSSSLOT` errors on a Valkey cluster | A custom `KeyPrefix` that breaks the `{queue}` hash tag. Keep the prefix free of braces. |
| The reconciler logs data-loss warnings constantly | `Schema` does not match Hangfire's actual schema, so it reads the wrong tables. |

## Migrating an existing Hangfire installation

1. Deploy with `UseValkeyQueues` **and** `ValkeyQueueMaintenance` registered.
2. Jobs already sitting in the PostgreSQL queue are not migrated automatically — the reconciler picks
   them up on its first pass, because PostgreSQL still reports them as `Enqueued`.
3. To roll back, remove the `UseValkeyQueues` call. PostgreSQL retained every job, so the
   PostgreSQL-backed queue resumes; IDs still in Valkey lists are re-enqueued by Hangfire's own
   invisibility timeout.
