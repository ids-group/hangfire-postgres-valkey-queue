using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>A job a worker is holding. Success -> RemoveFromQueue; retry/shutdown -> Requeue.
/// A crash before either is the orphan sweep's job.</summary>
internal sealed class ValkeyFetchedJob : IFetchedJob
{
    private readonly IConnectionMultiplexer _mux;
    private readonly ValkeyKeys _keys;
    private readonly string _queue;

    public ValkeyFetchedJob(IConnectionMultiplexer mux, ValkeyQueueOptions options, string queue, string jobId)
    {
        _mux = mux;
        _keys = new ValkeyKeys(options.KeyPrefix);
        _queue = queue;
        JobId = jobId;
    }

    public string JobId { get; }

    public void RemoveFromQueue()
    {
        var db = _mux.GetDatabase();
        var batch = db.CreateBatch();
        _ = batch.ListRemoveAsync(_keys.Processing(_queue), JobId, count: 1);
        _ = batch.HashDeleteAsync(_keys.Fetched(_queue), JobId);
        batch.Execute();
    }

    public void Requeue()
    {
        var db = _mux.GetDatabase();
        var batch = db.CreateBatch();
        _ = batch.ListRemoveAsync(_keys.Processing(_queue), JobId, count: 1);
        _ = batch.HashDeleteAsync(_keys.Fetched(_queue), JobId);
        // Tail push: a requeued job is picked up next, not last.
        _ = batch.ListRightPushAsync(_keys.Queue(_queue), JobId);
        batch.Execute();
    }

    // No-op: if the worker crashes without calling Remove/Requeue, the entry stays
    // in the processing list and the orphan sweep requeues it after the invisibility
    // timeout. That is the intended recovery path, so Dispose must not pre-empt it.
    public void Dispose()
    {
    }
}
