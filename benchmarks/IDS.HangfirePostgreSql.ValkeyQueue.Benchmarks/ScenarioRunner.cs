using System.Diagnostics;
using Hangfire;
using Hangfire.PostgreSql;
using Hangfire.PostgreSql.Factories;
using Hangfire.PostgreSql.ValkeyQueue;
using Npgsql;
using StackExchange.Redis;

namespace IDS.HangfirePostgreSql.ValkeyQueue.Benchmarks;

/// <summary>The measured result of one arm of the comparison.</summary>
internal sealed record ScenarioResult(
    string Name,
    PostgresLoad IdleLoad,
    long IdleTransactions,
    TimeSpan IdleDuration,
    PostgresLoad ThroughputLoad,
    long ThroughputTransactions,
    TimeSpan DrainDuration,
    int JobsCompleted,
    IReadOnlyList<(string Query, long Calls)> TopStatements)
{
    /// <summary>Hangfire queries per second against an EMPTY queue — the polling cost this library
    /// exists to remove.</summary>
    public double IdleQueriesPerSecond => IdleLoad.Calls / IdleDuration.TotalSeconds;

    /// <summary>Postgres queries executed per job processed.</summary>
    public double QueriesPerJob => JobsCompleted == 0 ? 0 : (double)ThroughputLoad.Calls / JobsCompleted;

    public double JobsPerSecond => JobsCompleted / DrainDuration.TotalSeconds;
}

/// <summary>
/// Runs one arm (with or without Valkey) in two phases:
/// <list type="number">
/// <item><b>Idle</b> — a server with an empty queue, to measure the polling floor.</item>
/// <item><b>Throughput</b> — N jobs enqueued then drained, to measure per-job cost.</item>
/// </list>
/// Each arm gets its own Hangfire schema so the two runs cannot see each other's rows.
/// </summary>
internal sealed class ScenarioRunner(BenchmarkOptions options)
{
    public ScenarioResult Run(string name, string schema, bool useValkey)
    {
        Console.WriteLine($"\n=== {name} ===");
        ResetSchema(schema);

        var probe = new PostgresLoadProbe(options.PostgresConnectionString, schema);
        probe.EnsureAvailable();

        var storageOptions = new PostgreSqlStorageOptions
        {
            SchemaName = schema,
            UseSlidingInvisibilityTimeout = true,
            PrepareSchemaIfNecessary = true,
            // Left at the library default. Lowering it would flatter the Valkey arm by making the
            // Postgres baseline poll harder than a real deployment would.
            QueuePollInterval = TimeSpan.FromSeconds(15),
        };

        var storage = new PostgreSqlStorage(
            new NpgsqlConnectionFactory(options.PostgresConnectionString, storageOptions), storageOptions);

        ConnectionMultiplexer? mux = null;
        if (useValkey)
        {
            var redisConfig = ConfigurationOptions.Parse(options.ValkeyConnectionString);
            redisConfig.AbortOnConnectFail = false;
            mux = ConnectionMultiplexer.Connect(redisConfig);
            ClearValkey(mux);

            storage.UseValkeyQueues(mux, new ValkeyQueueOptions
            {
                Queues = ["default"],
                Schema = schema,
                BlockingConnectionConfig = redisConfig,
            });
        }

        try
        {
            var client = new BackgroundJobClient(storage);
            using var server = new BackgroundJobServer(
                new BackgroundJobServerOptions
                {
                    WorkerCount = options.WorkerCount,
                    Queues = ["default"],
                    // The benchmark measures the queue, not Hangfire's periodic housekeeping.
                    // Pushed out so a schedule-poller tick cannot land inside the idle window.
                    SchedulePollingInterval = TimeSpan.FromMinutes(10),
                },
                storage);

            // Let the server finish starting and installing its schema before any measurement —
            // schema DDL would otherwise be counted as queue load.
            Thread.Sleep(TimeSpan.FromSeconds(5));

            var (idleLoad, idleTx, idleDuration) = MeasureIdle(probe);
            var (throughputLoad, throughputTx, drainDuration, completed) = MeasureThroughput(probe, client);
            var top = probe.TopStatements(5);

            return new ScenarioResult(
                name, idleLoad, idleTx, idleDuration,
                throughputLoad, throughputTx, drainDuration, completed, top);
        }
        finally
        {
            // PostgreSqlStorage is not IDisposable in 1.21.x; its connections go with the process.
            mux?.Dispose();
        }
    }

    /// <summary>Phase 1: nothing to do. Every query Postgres sees here is pure polling overhead.</summary>
    private (PostgresLoad Load, long Transactions, TimeSpan Duration) MeasureIdle(PostgresLoadProbe probe)
    {
        Console.WriteLine($"  idle phase: {options.IdleDuration.TotalSeconds:N0}s with an empty queue...");
        probe.Reset();

        var sw = Stopwatch.StartNew();
        Thread.Sleep(options.IdleDuration);
        sw.Stop();

        var load = probe.Snapshot();
        var transactions = probe.TransactionCalls();
        Console.WriteLine($"    {load.Calls} Hangfire queries (+{transactions} tx control) " +
                          $"in {sw.Elapsed.TotalSeconds:N1}s ({load.Calls / sw.Elapsed.TotalSeconds:N2}/s)");

        return (load, transactions, sw.Elapsed);
    }

    /// <summary>Phase 2: enqueue N jobs, then time the drain.</summary>
    private (PostgresLoad Load, long Transactions, TimeSpan Duration, int Completed) MeasureThroughput(
        PostgresLoadProbe probe, BackgroundJobClient client)
    {
        Console.WriteLine($"  throughput phase: {options.JobCount} jobs / {options.WorkerCount} workers...");
        BenchmarkJob.Reset();
        probe.Reset();

        var sw = Stopwatch.StartNew();
        for (var i = 0; i < options.JobCount; i++)
        {
            client.Enqueue(() => BenchmarkJob.Execute(i));
        }

        var deadline = sw.Elapsed + options.DrainTimeout;
        while (BenchmarkJob.Completed < options.JobCount && sw.Elapsed < deadline)
        {
            Thread.Sleep(50);
        }

        sw.Stop();
        var load = probe.Snapshot();
        var transactions = probe.TransactionCalls();
        var completed = BenchmarkJob.Completed;

        if (completed < options.JobCount)
        {
            Console.WriteLine($"    WARNING: drained only {completed}/{options.JobCount} before timeout.");
        }

        Console.WriteLine($"    {completed} jobs in {sw.Elapsed.TotalSeconds:N1}s " +
                          $"({completed / sw.Elapsed.TotalSeconds:N0} jobs/s), " +
                          $"{load.Calls} Hangfire queries (+{transactions} tx control)");

        return (load, transactions, sw.Elapsed, completed);
    }

    /// <summary>Drops the arm's schema so each run starts from nothing.</summary>
    private void ResetSchema(string schema)
    {
        using var conn = new NpgsqlConnection(options.PostgresConnectionString);
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DROP SCHEMA IF EXISTS {schema} CASCADE";
        cmd.ExecuteNonQuery();
    }

    private static void ClearValkey(IConnectionMultiplexer mux)
    {
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        var db = mux.GetDatabase();
        foreach (var key in server.Keys(db.Database, pattern: "hangfire:*"))
        {
            db.KeyDelete(key);
        }
    }
}
