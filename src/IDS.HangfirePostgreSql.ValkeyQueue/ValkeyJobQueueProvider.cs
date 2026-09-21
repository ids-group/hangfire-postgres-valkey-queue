using StackExchange.Redis;

namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>Ties the queue and its monitoring API together for registration via
/// <c>PostgreSqlStorage.QueueProviders.Add(provider, queues)</c>.</summary>
public sealed class ValkeyJobQueueProvider : IPersistentJobQueueProvider
{
    private readonly ValkeyJobQueue _queue;
    private readonly ValkeyJobQueueMonitoringApi _monitoring;

    /// <summary>Creates the provider over the given Valkey multiplexer and options.</summary>
    public ValkeyJobQueueProvider(IConnectionMultiplexer mux, ValkeyQueueOptions options)
    {
        _queue = new ValkeyJobQueue(mux, options);
        _monitoring = new ValkeyJobQueueMonitoringApi(mux, options);
    }

    /// <inheritdoc/>
    public IPersistentJobQueue GetJobQueue() => _queue;

    /// <inheritdoc/>
    public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi() => _monitoring;
}
