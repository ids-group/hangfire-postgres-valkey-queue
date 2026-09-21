using StackExchange.Redis;

namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>Registration seam.</summary>
public static class PostgreSqlStorageValkeyExtensions
{
    /// <summary>Point the given queues at Valkey instead of Postgres. Postgres stays the
    /// durable store (job data, state history, dashboard); only the hot job-id queue moves.
    /// Call before handing the storage to <c>UseStorage(...)</c>.</summary>
    public static PostgreSqlStorage UseValkeyQueues(
        this PostgreSqlStorage storage,
        IConnectionMultiplexer mux,
        ValkeyQueueOptions options)
    {
        var provider = new ValkeyJobQueueProvider(mux, options);
        storage.QueueProviders.Add(provider, options.Queues);
        return storage;
    }
}
