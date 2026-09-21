using System.Globalization;
using System.Text.Json;
using FluentAssertions;
using Hangfire;
using Hangfire.Common;
using Hangfire.PostgreSql;
using Hangfire.PostgreSql.ValkeyQueue;
using Hangfire.Storage;
using Npgsql;
using StackExchange.Redis;
using Xunit;

namespace IDS.HangfirePostgreSql.ValkeyQueue.Tests;

/// <summary>
/// The Valkey hybrid queue. Postgres stays the durable store; only the hot job-id queue moves to
/// Valkey, which means the enqueue is no longer transactional with job creation — so correctness
/// rests entirely on two recovery paths that nothing else exercises: the orphan sweep (a worker died
/// holding a job) and the reconciler (Valkey lost the id Postgres still reports as Enqueued). A gap
/// in either loses a job silently.
///
/// <para>Runs against a real Valkey through the library's public provider — the queue types
/// themselves are internal, and the point is the behaviour Hangfire sees, not their internals.</para>
/// </summary>
[Collection(ValkeyQueueCollection.Name)]
public class ValkeyJobQueueTests(ValkeyQueueFixture fixture)
{
    /// <summary>A queue no live Hangfire server listens to.</summary>
    private const string Queue = "valkey-test";

    /// <summary>
    /// The reconciler reads <c>{Schema}.job</c> / <c>{Schema}.state</c>, and the schema name is an
    /// option — so these tests point it at their own schema instead of Hangfire's. Creating tables in
    /// the real <c>hangfire</c> schema would race Hangfire's OWN installer, whose DDL is not
    /// idempotent. Sharing nothing removes the question.
    /// </summary>
    private const string Schema = "valkey_queue_tests";

    private readonly ValkeyQueueFixture _fixture = Reset(fixture);

    /// <summary>
    /// xUnit builds the class once per test, so this runs before each one. Both stores have to go:
    /// the tests assert on absolute counts, and the reconciler re-pushes anything an earlier test
    /// left Enqueued in Postgres.
    /// </summary>
    private static ValkeyQueueFixture Reset(ValkeyQueueFixture fixture)
    {
        fixture.FlushRedis();
        fixture.DropTestSchema(Schema);

        return fixture;
    }

    [Fact]
    public void Enqueue_then_dequeue_hands_the_job_over_and_tracks_it_as_fetched()
    {
        using var mux = Connect();
        var (queue, monitoring) = Build(mux);

        queue.Enqueue(null!, Queue, "4242");

        Count(monitoring).Should().Be(new QueueCounts(1, 0));

        using var fetched = queue.Dequeue([Queue], CancellationToken.None);

        fetched.JobId.Should().Be("4242");
        Count(monitoring).Should().Be(new QueueCounts(0, 1),
            "the id moves to the processing list so a dead worker's job is recoverable");
    }

    [Fact]
    public void Removing_a_finished_job_clears_it_from_both_lists()
    {
        using var mux = Connect();
        var (queue, monitoring) = Build(mux);
        queue.Enqueue(null!, Queue, "1");

        var fetched = queue.Dequeue([Queue], CancellationToken.None);
        fetched.RemoveFromQueue();

        Count(monitoring).Should().Be(new QueueCounts(0, 0));
    }

    [Fact]
    public void Requeue_puts_the_job_back_for_the_next_worker()
    {
        // The shutdown path: Hangfire calls Requeue so another instance picks the job up.
        using var mux = Connect();
        var (queue, monitoring) = Build(mux);
        queue.Enqueue(null!, Queue, "7");

        var fetched = queue.Dequeue([Queue], CancellationToken.None);
        fetched.Requeue();

        Count(monitoring).Should().Be(new QueueCounts(1, 0));
        monitoring.GetEnqueuedJobIds(Queue, 0, 10).Should().Contain(7);
    }

    [Fact]
    public void Disposing_a_fetched_job_does_not_pre_empt_the_orphan_sweep()
    {
        // Dispose must NOT quietly requeue or drop: a worker that dies without acking is meant to be
        // recovered by the sweep after the invisibility timeout, and Dispose running first would
        // either lose the job or duplicate it.
        using var mux = Connect();
        var (queue, monitoring) = Build(mux);
        queue.Enqueue(null!, Queue, "9");

        queue.Dequeue([Queue], CancellationToken.None).Dispose();

        Count(monitoring).Should().Be(new QueueCounts(0, 1));
    }

    [Fact]
    public void Every_key_for_a_queue_lands_in_one_cluster_hash_slot()
    {
        // On ElastiCache in cluster mode the queue->processing LMOVE spans two keys; without the
        // {queue} hash tag they hash to different slots and the server rejects it with CROSSSLOT.
        // That failure appears only on a clustered deployment, so it is worth pinning here.
        using var mux = Connect();
        var (queue, _) = Build(mux);
        queue.Enqueue(null!, Queue, "11");
        using var fetched = queue.Dequeue([Queue], CancellationToken.None);

        var keys = mux.GetServer(mux.GetEndPoints()[0])
            .Keys(_fixture.RedisDatabase, pattern: "hangfire:*")
            .Select(k => (string)k!)
            .ToList();

        keys.Should().NotBeEmpty();
        keys.Should().OnlyContain(k => k.Contains($"{{{Queue}}}", StringComparison.Ordinal),
            "all three per-queue keys must carry the same hash tag");
    }

    [Fact]
    public void The_sweep_requeues_a_job_whose_worker_never_came_back()
    {
        using var mux = Connect();
        var (queue, monitoring) = Build(mux);
        queue.Enqueue(null!, Queue, "31");
        using var fetched = queue.Dequeue([Queue], CancellationToken.None);

        // Zero invisibility timeout == "anything in processing is already overdue".
        RunMaintenance(mux, Options(invisibility: TimeSpan.Zero));

        // Asserted per-id, not on the totals: the reconciler half of the same pass may legitimately
        // re-push unrelated jobs other tests left Enqueued in the shared Postgres schema.
        monitoring.GetEnqueuedJobIds(Queue, 0, 100).Should().Contain(31);
        monitoring.GetFetchedJobIds(Queue, 0, 100).Should().NotContain(31);
    }

    [Fact]
    public void The_sweep_leaves_a_job_that_is_still_being_worked()
    {
        // The other half — a sweep that reclaimed in-flight jobs would run every long job twice.
        using var mux = Connect();
        var (queue, monitoring) = Build(mux);
        queue.Enqueue(null!, Queue, "32");
        using var fetched = queue.Dequeue([Queue], CancellationToken.None);

        RunMaintenance(mux, Options(invisibility: TimeSpan.FromMinutes(30)));

        monitoring.GetFetchedJobIds(Queue, 0, 100).Should().Contain(32);
        monitoring.GetEnqueuedJobIds(Queue, 0, 100).Should().NotContain(32);
    }

    [Fact]
    public void The_reconciler_re_enqueues_a_job_that_postgres_still_reports_as_enqueued()
    {
        // Valkey data loss / failover: the id is gone from the list but Postgres — the durable store —
        // still says Enqueued. Without this the job is simply never run.
        using var mux = Connect();
        var (_, monitoring) = Build(mux);
        var jobId = CreateEnqueuedPostgresJob();

        RunMaintenance(mux, Options(reconcileGrace: TimeSpan.Zero));

        monitoring.GetEnqueuedJobIds(Queue, 0, 100).Should().Contain(jobId);
    }

    [Fact]
    public void The_reconciler_ignores_a_job_enqueued_moments_ago()
    {
        // The grace window: without it the reconciler races the LPUSH of a job being created right
        // now and pushes a duplicate every pass.
        using var mux = Connect();
        var (_, monitoring) = Build(mux);
        var jobId = CreateEnqueuedPostgresJob();

        RunMaintenance(mux, Options(reconcileGrace: TimeSpan.FromMinutes(5)));

        monitoring.GetEnqueuedJobIds(Queue, 0, 100).Should().NotContain(jobId);
    }

    [Fact]
    public void The_reconciler_does_not_push_the_same_job_twice()
    {
        // Two passes in a row must be idempotent — the first push makes the job "live" in Valkey,
        // which the second pass has to notice.
        using var mux = Connect();
        var (_, monitoring) = Build(mux);
        var jobId = CreateEnqueuedPostgresJob();
        var options = Options(reconcileGrace: TimeSpan.Zero);

        RunMaintenance(mux, options);
        RunMaintenance(mux, options);

        monitoring.GetEnqueuedJobIds(Queue, 0, 100)
            .Count(id => id == jobId).Should().Be(1);
    }

    /// <summary>
    /// A record, not a tuple: <c>Should().Be()</c> on a ValueTuple binds to the comparable-type
    /// assertion, which erases the element names — so every call site warned (CS8123) and failures
    /// printed "Item1 = 7" instead of "Enqueued = 7".
    /// </summary>
    private sealed record QueueCounts(long Enqueued, long Fetched);

    private static QueueCounts Count(IPersistentJobQueueMonitoringApi monitoring)
    {
        var dto = monitoring.GetEnqueuedAndFetchedCount(Queue);

        return new QueueCounts(dto.EnqueuedCount, dto.FetchedCount);
    }

    /// <summary>
    /// Writes the two rows the reconciler reads — an <c>Enqueued</c> job and its state — directly.
    ///
    /// <para>Deliberately NOT via <c>BackgroundJobClient</c>: constructing a <c>PostgreSqlStorage</c>
    /// installs the Hangfire schema under a <c>pg_advisory_xact_lock</c>, and a test must not depend
    /// on winning that race.</para>
    ///
    /// <para>The one thing worth keeping from the real client is the timestamp FORMAT — Hangfire
    /// writes <c>EnqueuedAt</c> as unix milliseconds from CompatibilityLevel 170 on and as ISO-8601
    /// below it, and reading it with a plain <c>DateTimeOffset.TryParse</c> is the bug these tests
    /// caught. So the value comes from Hangfire's own serializer rather than a literal.</para>
    /// </summary>
    /// <returns>The job id, as the reconciler will report it.</returns>
    private long CreateEnqueuedPostgresJob()
    {
        var stateData = JsonSerializer.Serialize(new Dictionary<string, string>
        {
            ["Queue"] = Queue,
            ["EnqueuedAt"] = JobHelper.SerializeDateTime(DateTime.UtcNow),
        });

        using var conn = OpenHangfire();
        using var tx = conn.BeginTransaction();

        long jobId;
        using (var insertJob = conn.CreateCommand())
        {
            insertJob.CommandText =
                $"INSERT INTO {Schema}.job (invocationdata, arguments, createdat, statename) " +
                "VALUES ('{}', '[]', now(), 'Enqueued') RETURNING id";
            jobId = Convert.ToInt64(insertJob.ExecuteScalar(), CultureInfo.InvariantCulture);
        }

        using (var insertState = conn.CreateCommand())
        {
            insertState.CommandText =
                $"INSERT INTO {Schema}.state (jobid, name, createdat, data) " +
                $"VALUES (@jobid, 'Enqueued', now(), @data{DataCast(conn)}) RETURNING id";
            insertState.Parameters.AddWithValue("jobid", jobId);
            insertState.Parameters.AddWithValue("data", stateData);
            var stateId = Convert.ToInt64(insertState.ExecuteScalar(), CultureInfo.InvariantCulture);

            using var link = conn.CreateCommand();
            link.CommandText = $"UPDATE {Schema}.job SET stateid = @stateid WHERE id = @jobid";
            link.Parameters.AddWithValue("stateid", stateId);
            link.Parameters.AddWithValue("jobid", jobId);
            link.ExecuteNonQuery();
        }

        tx.Commit();

        return jobId;
    }

    /// <summary>
    /// Opens a connection with the two tables the reconciler queries guaranteed to exist, in this
    /// test's own schema. Column names and types mirror Hangfire.PostgreSql's.
    /// </summary>
    private NpgsqlConnection OpenHangfire()
    {
        var conn = _fixture.OpenPostgres();

        using var ddl = conn.CreateCommand();
        ddl.CommandText = $"""
            CREATE SCHEMA IF NOT EXISTS {Schema};
            CREATE TABLE IF NOT EXISTS {Schema}.job (
                id BIGSERIAL PRIMARY KEY,
                stateid BIGINT NULL,
                statename TEXT NULL,
                invocationdata TEXT NOT NULL,
                arguments TEXT NOT NULL,
                createdat TIMESTAMP NOT NULL,
                expireat TIMESTAMP NULL);
            CREATE TABLE IF NOT EXISTS {Schema}.state (
                id BIGSERIAL PRIMARY KEY,
                jobid BIGINT NOT NULL,
                name TEXT NOT NULL,
                reason TEXT NULL,
                createdat TIMESTAMP NOT NULL,
                data JSONB NULL);
            """;
        ddl.ExecuteNonQuery();

        return conn;
    }

    /// <summary>
    /// The state payload column is <c>jsonb</c>, which a text parameter only reaches through an
    /// explicit cast — and must not have one if it is ever plain <c>text</c>. Read the type, don't assume.
    /// </summary>
    private static string DataCast(NpgsqlConnection conn)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT data_type FROM information_schema.columns " +
            $"WHERE table_schema = '{Schema}' AND table_name = 'state' AND column_name = 'data'";

        return (cmd.ExecuteScalar() as string) == "jsonb" ? "::jsonb" : string.Empty;
    }

    private ValkeyQueueOptions Options(TimeSpan? invisibility = null, TimeSpan? reconcileGrace = null) => new ()
    {
        Queues = [Queue],
        Schema = Schema,
        InvisibilityTimeout = invisibility ?? TimeSpan.FromMinutes(30),
        ReconcileGrace = reconcileGrace ?? TimeSpan.FromSeconds(30),
        MaintenanceInterval = TimeSpan.Zero,
        BlockingConnectionConfig = _fixture.RedisConfig(),
    };

    private (IPersistentJobQueue Queue, IPersistentJobQueueMonitoringApi Monitoring) Build(IConnectionMultiplexer mux)
    {
        var provider = new ValkeyJobQueueProvider(mux, Options());

        return (provider.GetJobQueue(), provider.GetJobQueueMonitoringApi());
    }

    /// <summary>Runs exactly one maintenance pass (sweep + reconcile) and returns.</summary>
    private void RunMaintenance(IConnectionMultiplexer mux, ValkeyQueueOptions options)
    {
        // The reconciler queries {Schema}.job unconditionally, so make sure it is there even in tests
        // that never create a job.
        OpenHangfire().Dispose();

        var maintenance = new ValkeyQueueMaintenance(mux, options, _fixture.PostgresConnectionString);

        // Execute ends with context.Wait(MaintenanceInterval). A cancelled token would make that
        // throw (Hangfire's Wait is WaitOrThrow), so the interval is zeroed in the options instead —
        // the pass returns the moment the sweep and the reconcile are done.
        maintenance.Execute(new Hangfire.Server.BackgroundProcessContext(
            "valkey-test-server",
            new NoopJobStorage(),
            new Dictionary<string, object>(),
            CancellationToken.None));
    }

    private ConnectionMultiplexer Connect() => _fixture.Connect();

    /// <summary>
    /// <c>ValkeyQueueMaintenance.Execute</c> never touches <c>context.Storage</c>; the context just
    /// requires one.
    /// </summary>
    private sealed class NoopJobStorage : JobStorage
    {
        public override IMonitoringApi GetMonitoringApi() => throw new NotSupportedException();

        public override IStorageConnection GetConnection() => throw new NotSupportedException();
    }
}
