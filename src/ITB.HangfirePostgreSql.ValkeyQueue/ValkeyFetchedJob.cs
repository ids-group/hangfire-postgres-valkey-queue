using Hangfire.Logging;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>A job a worker is holding. Success -> RemoveFromQueue; retry/shutdown -> Requeue.
/// A crash before either is the orphan sweep's job.
///
/// <para>While held, a timer refreshes the job's fetched timestamp every
/// <see cref="ValkeyQueueOptions.HeartbeatInterval"/>. That is what lets the invisibility timeout
/// mean "this worker has gone quiet" rather than "this job is taking too long", so a long-running
/// job is never requeued underneath the worker still running it.</para></summary>
internal sealed class ValkeyFetchedJob : IFetchedJob
{
    private const string HeartbeatScript = @"
        if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
            return redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
        end
        return 0";

    private const string RemoveScript = @"
        redis.call('LREM', KEYS[1], 1, ARGV[1])
        redis.call('HDEL', KEYS[2], ARGV[1])
        return 1";

    private const string RequeueScript = @"
        redis.call('LREM', KEYS[1], 1, ARGV[1])
        redis.call('HDEL', KEYS[2], ARGV[1])
        redis.call('RPUSH', KEYS[3], ARGV[1])
        return 1";

    private static readonly ILog Logger = LogProvider.For<ValkeyFetchedJob>();

    private readonly IConnectionMultiplexer _mux;
    private readonly ValkeyKeys _keys;
    private readonly string _queue;
    private readonly Timer? _heartbeat;
    private readonly Lock _gate = new();
    private bool _finished;

    public ValkeyFetchedJob(IConnectionMultiplexer mux, ValkeyQueueOptions options, string queue, string jobId)
    {
        _mux = mux;
        _keys = new ValkeyKeys(options.KeyPrefix);
        _queue = queue;
        JobId = jobId;

        if (options.HeartbeatInterval > TimeSpan.Zero)
        {
            _heartbeat = new Timer(_ => Beat(), null, options.HeartbeatInterval, options.HeartbeatInterval);
        }
    }

    public string JobId { get; }

    public void RemoveFromQueue()
    {
        StopHeartbeat();
        _mux.GetDatabase().ScriptEvaluate(
            RemoveScript,
            [_keys.Processing(_queue), _keys.Fetched(_queue)],
            [JobId]);
    }

    public void Requeue()
    {
        StopHeartbeat();
        _mux.GetDatabase().ScriptEvaluate(
            RequeueScript,
            // Tail push: a requeued job is picked up next, not last.
            [_keys.Processing(_queue), _keys.Fetched(_queue), _keys.Queue(_queue)],
            [JobId]);
    }

    public void Dispose()
    {
        StopHeartbeat();
    }

    private void StopHeartbeat()
    {
        lock (_gate)
        {
            _finished = true;
            _heartbeat?.Dispose();
        }
    }

    private void Beat()
    {
        lock (_gate)
        {
            if (_finished)
            {
                return;
            }

            try
            {
                _mux.GetDatabase().ScriptEvaluate(
                    HeartbeatScript,
                    [_keys.Fetched(_queue)],
                    [JobId, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()]);
            }
            // ObjectDisposedException belongs here with the connection faults: the multiplexer can
            // be torn down during shutdown while a tick is already in flight, and a timer callback
            // that throws takes the whole process down with it.
            catch (Exception ex) when (ex is RedisException or TimeoutException or ObjectDisposedException)
            {
                Logger.WarnException(
                    $"Valkey heartbeat failed for job {JobId} on '{_queue}'; retrying next tick.",
                    ex);
            }
        }
    }
}
