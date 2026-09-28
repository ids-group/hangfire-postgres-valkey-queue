using StackExchange.Redis;

namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>Registration seam.</summary>
public static class PostgreSqlStorageValkeyExtensions
{
    /// <summary>Point the given queues at Valkey instead of Postgres. Postgres stays the
    /// durable store (job data, state history, dashboard); only the hot job-id queue moves.
    /// Call before handing the storage to <c>UseStorage(...)</c>.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A heartbeat cannot keep a job alive under the
    /// configured invisibility timeout, so live jobs would be requeued while still running.</exception>
    public static PostgreSqlStorage UseValkeyQueues(
        this PostgreSqlStorage storage,
        IConnectionMultiplexer mux,
        ValkeyQueueOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.HeartbeatInterval <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.HeartbeatInterval,
                $"{nameof(ValkeyQueueOptions.HeartbeatInterval)} must be positive.");
        }

        if (options.InvisibilityTimeout < options.HeartbeatInterval * 3)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.InvisibilityTimeout,
                $"{nameof(ValkeyQueueOptions.InvisibilityTimeout)} must be at least 3x " +
                $"{nameof(ValkeyQueueOptions.HeartbeatInterval)} ({options.HeartbeatInterval * 3}), " +
                "otherwise a single missed heartbeat requeues a job that is still running.");
        }

        var provider = new ValkeyJobQueueProvider(mux, options);
        storage.QueueProviders.Add(provider, options.Queues);
        return storage;
    }
}
