using System.Data;
using Hangfire.Logging;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>The hot path. Enqueue = LPUSH; Dequeue = blocking BLMOVE into a
/// per-queue processing list (so a dead worker's job can be recovered).</summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage(
    "Microsoft.Design",
    "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable",
    Justification =
        "The ThreadLocal holds one blocking connection per Hangfire worker thread, and those " +
        "threads live until the process exits. Hangfire never disposes an IPersistentJobQueue, " +
        "so implementing IDisposable here would add a method nothing calls; the connections are " +
        "reclaimed with the process.")]
internal sealed class ValkeyJobQueue : IPersistentJobQueue
{
    private static readonly ILog Logger = LogProvider.For<ValkeyJobQueue>();

    private readonly IConnectionMultiplexer _mux;
    private readonly ValkeyQueueOptions _options;
    private readonly ValkeyKeys _keys;

    // One dedicated blocking connection PER WORKER THREAD. BLMOVE monopolises a
    // connection until it returns, so running it on the shared multiplexer would
    // stall every other command. Hangfire worker threads are long-lived, so a
    // ThreadLocal yields exactly WorkerCount blocking connections — bounded.
    // ponytail: these connections live until process exit; fine for a fixed pool.
    private readonly ThreadLocal<IDatabase> _blockingDb;

    public ValkeyJobQueue(IConnectionMultiplexer mux, ValkeyQueueOptions options)
    {
        _mux = mux;
        _options = options;
        _keys = new ValkeyKeys(options.KeyPrefix);
        _blockingDb = new ThreadLocal<IDatabase>(
            () => ConnectionMultiplexer.Connect(BlockingConfig(mux, options)).GetDatabase());
    }

    public void Enqueue(IDbConnection connection, string queue, string jobId)
    {
        // The Postgres `connection` is deliberately ignored — moving the queue off
        // Postgres is the whole point. The job row + its Enqueued state are still
        // written to Postgres transactionally by the caller, so if this LPUSH is
        // lost (Valkey down), the job stays Enqueued in Postgres and the reconciler
        // re-pushes it. Hence we swallow connection failures rather than failing the
        // whole job-creation transaction.
        try
        {
            _mux.GetDatabase().ListLeftPush(_keys.Queue(queue), jobId);
        }
        catch (RedisConnectionException ex)
        {
            Logger.WarnException(
                $"Valkey LPUSH failed for job {jobId} on '{queue}'; it stays Enqueued in Postgres and the reconciler will recover it.",
                ex);
        }
    }

    public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
    {
        var db = _blockingDb.Value!;
        var blockSeconds = (int)Math.Max(1, _options.BlockTimeout.TotalSeconds);

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Fast path: non-blocking, honouring queue priority order.
            foreach (var queue in queues)
            {
                var moved = db.ListMove(_keys.Queue(queue), _keys.Processing(queue), ListSide.Right, ListSide.Left);
                if (!moved.IsNullOrEmpty)
                {
                    return Fetch(db, queue, moved!);
                }
            }

            // Slow path: block on the first queue so we wake the instant a job
            // arrives (sub-second pickup) instead of polling Postgres on a timer.
            // ponytail: only queues[0] gets the blocking wait; the others are
            //   re-scanned each loop, so their worst-case idle latency == BlockTimeout.
            //   Hangfire installs are usually single-queue ("default"), so fine.
            var primary = queues[0];
            var blocked = db.Execute(
                "BLMOVE", _keys.Queue(primary), _keys.Processing(primary), "RIGHT", "LEFT", blockSeconds);
            if (!blocked.IsNull)
            {
                return Fetch(db, primary, (string)blocked!);
            }
        }
    }

    private ValkeyFetchedJob Fetch(IDatabase db, string queue, string jobId)
    {
        // Record fetch time for the orphan sweep. Not atomic with the move above; a
        // crash in between leaves a processing entry with no timestamp, which the
        // sweep treats as fetched long ago (so it still gets requeued).
        db.HashSet(_keys.Fetched(queue), jobId, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        return new ValkeyFetchedJob(_mux, _options, queue, jobId);
    }

    private static ConfigurationOptions BlockingConfig(IConnectionMultiplexer mux, ValkeyQueueOptions options)
    {
        // Blocking BLMOVE needs its own connection per worker thread. Prefer the explicit
        // ConfigurationOptions when provided: on managed Valkey mux.Configuration masks the
        // password, so re-parsing it would drop AUTH and every blocking connection would fail.
        // Falls back to the shared multiplexer's connection string for AUTH-less (local) setups.
        var config = (options.BlockingConnectionConfig ?? ConfigurationOptions.Parse(mux.Configuration)).Clone();
        // SyncTimeout must exceed the BLMOVE server-side block or Execute() throws.
        config.SyncTimeout = Math.Max(config.SyncTimeout, 30_000);
        return config;
    }
}
