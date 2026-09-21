using Npgsql;
using StackExchange.Redis;
using Xunit;

namespace ITB.HangfirePostgreSql.ValkeyQueue.Tests;

/// <summary>
/// The Postgres and Valkey endpoints the suite runs against, read from environment variables so the
/// same tests work locally (docker run) and in CI (service containers) without a Testcontainers
/// dependency. Defaults match <c>docs/EXAMPLE.md</c> and the compose file in the README.
/// </summary>
public sealed class ValkeyQueueFixture
{
    public string PostgresConnectionString { get; } =
        Environment.GetEnvironmentVariable("POSTGRES_TEST_CONNECTION")
        ?? "Host=localhost;Port=55432;Database=hangfire_bench;Username=postgres;Password=Asd123!1";

    public string RedisHost { get; } = Environment.GetEnvironmentVariable("REDIS_TEST_HOST") ?? "localhost";

    public int RedisPort { get; } =
        int.TryParse(Environment.GetEnvironmentVariable("REDIS_TEST_PORT"), out var port) ? port : 56379;

    /// <summary>A database no other suite uses, so a stray key cannot cross-contaminate.</summary>
    public int RedisDatabase => 3;

    public ConfigurationOptions RedisConfig()
    {
        var config = new ConfigurationOptions
        {
            AbortOnConnectFail = false,
            DefaultDatabase = RedisDatabase,
        };
        config.EndPoints.Add(RedisHost, RedisPort);

        return config;
    }

    public ConnectionMultiplexer Connect() => ConnectionMultiplexer.Connect(RedisConfig());

    /// <summary>
    /// Deletes the keys this library owns. The suite asserts on absolute list lengths ("0 enqueued,
    /// 1 fetched"), so ids another test left behind read as the queue's own content and fail it.
    ///
    /// <para>Scans and deletes rather than FLUSHDB: the latter needs <c>allowAdmin</c>, and a
    /// prefix-scoped delete cannot wipe a co-tenant's data if this ever points at a shared Valkey.</para>
    /// </summary>
    public void FlushRedis()
    {
        using var mux = ConnectionMultiplexer.Connect(RedisConfig());
        var server = mux.GetServer(mux.GetEndPoints()[0]);
        var db = mux.GetDatabase();

        foreach (var key in server.Keys(RedisDatabase, pattern: "hangfire:*"))
        {
            db.KeyDelete(key);
        }
    }

    /// <summary>
    /// Drops the schema the reconciler reads. Rows left Enqueued by an earlier test would otherwise
    /// be re-pushed by the next test's maintenance pass, inflating its queue.
    /// </summary>
    public void DropTestSchema(string schema)
    {
        using var conn = OpenPostgres();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = $"DROP SCHEMA IF EXISTS {schema} CASCADE";
        cmd.ExecuteNonQuery();
    }

    public NpgsqlConnection OpenPostgres()
    {
        var conn = new NpgsqlConnection(PostgresConnectionString);
        conn.Open();

        return conn;
    }
}

/// <summary>
/// One fixture for the whole assembly. The tests share a Valkey database and a Postgres schema, and
/// xUnit runs collections in parallel by default — which would let one test's FLUSH or maintenance
/// pass wipe another's state mid-assertion.
/// </summary>
[CollectionDefinition(Name)]
public sealed class ValkeyQueueCollection : ICollectionFixture<ValkeyQueueFixture>
{
    public const string Name = "valkey-queue";
}
