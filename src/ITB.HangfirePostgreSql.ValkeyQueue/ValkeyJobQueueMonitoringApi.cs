using StackExchange.Redis;

namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>Feeds the Hangfire dashboard's Queues / Enqueued views straight from Valkey lists.</summary>
internal sealed class ValkeyJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
{
    private readonly IConnectionMultiplexer _mux;
    private readonly ValkeyKeys _keys;
    private readonly string[] _queues;

    public ValkeyJobQueueMonitoringApi(IConnectionMultiplexer mux, ValkeyQueueOptions options)
    {
        _mux = mux;
        _keys = new ValkeyKeys(options.KeyPrefix);
        _queues = options.Queues;
    }

    // Empty Valkey lists don't exist as keys, so we can't discover queues by SCAN —
    // return the configured set so the dashboard lists them even when empty.
    public IEnumerable<string> GetQueues() => _queues;

    public IEnumerable<long> GetEnqueuedJobIds(string queue, int from, int perPage) =>
        Range(_keys.Queue(queue), from, perPage);

    public IEnumerable<long> GetFetchedJobIds(string queue, int from, int perPage) =>
        Range(_keys.Processing(queue), from, perPage);

    public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
    {
        var db = _mux.GetDatabase();
        return new EnqueuedAndFetchedCountDto
        {
            EnqueuedCount = db.ListLength(_keys.Queue(queue)),
            FetchedCount = db.ListLength(_keys.Processing(queue)),
        };
    }

    private IEnumerable<long> Range(string key, int from, int perPage)
    {
        var values = _mux.GetDatabase().ListRange(key, from, from + perPage - 1);
        var ids = new List<long>(values.Length);
        foreach (var v in values)
        {
            if (long.TryParse((string?)v, out var id))
            {
                ids.Add(id);
            }
        }

        return ids;
    }
}
