using System.Text.Json;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Server;
using Npgsql;
using StackExchange.Redis;

namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>Two safety nets run on a timer inside the Hangfire server:
/// <list type="bullet">
/// <item><b>Orphan sweep</b> — requeues jobs stuck in a processing list past the
/// invisibility timeout (worker died mid-job).</item>
/// <item><b>Reconciler</b> — re-enqueues jobs Postgres reports as Enqueued but that
/// are missing from Valkey (Valkey data loss / failover).</item>
/// </list>
/// Registered as an <see cref="IBackgroundProcess"/>, so <c>AddHangfireServer</c> picks it up.</summary>
public sealed class ValkeyQueueMaintenance : IBackgroundProcess
{
    private static readonly ILog Logger = LogProvider.For<ValkeyQueueMaintenance>();

    private readonly IConnectionMultiplexer _mux;
    private readonly ValkeyQueueOptions _options;
    private readonly string _connectionString;
    private readonly ValkeyKeys _keys;

    /// <summary>Creates the maintenance process. <paramref name="postgresConnectionString"/> is
    /// used by the reconciler to read jobs Postgres still reports as Enqueued.</summary>
    public ValkeyQueueMaintenance(IConnectionMultiplexer mux, ValkeyQueueOptions options, string postgresConnectionString)
    {
        _mux = mux;
        _options = options;
        _connectionString = postgresConnectionString;
        _keys = new ValkeyKeys(options.KeyPrefix);
    }

    /// <inheritdoc/>
    public void Execute(BackgroundProcessContext context)
    {
        try
        {
            SweepOrphans();
            Reconcile();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Logger.ErrorException("Valkey queue maintenance pass failed; retrying next tick.", ex);
        }

        context.Wait(_options.MaintenanceInterval);
    }

    private void SweepOrphans()
    {
        var db = _mux.GetDatabase();
        var cutoff = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - (long)_options.InvisibilityTimeout.TotalMilliseconds;

        foreach (var queue in _options.Queues)
        {
            var processing = db.ListRange(_keys.Processing(queue));
            if (processing.Length == 0)
            {
                continue;
            }

            var fetched = db.HashGetAll(_keys.Fetched(queue))
                .ToDictionary(e => (string)e.Name!, e => (long)e.Value);

            var requeued = 0;
            foreach (var entry in processing)
            {
                var jobId = (string)entry!;
                // Missing timestamp (crash between move and HSET) counts as long ago.
                var fetchedAt = fetched.TryGetValue(jobId, out var ts) ? ts : 0L;
                if (fetchedAt > cutoff)
                {
                    continue;
                }

                var batch = db.CreateBatch();
                _ = batch.ListRemoveAsync(_keys.Processing(queue), jobId, count: 1);
                _ = batch.HashDeleteAsync(_keys.Fetched(queue), jobId);
                _ = batch.ListRightPushAsync(_keys.Queue(queue), jobId);
                batch.Execute();
                requeued++;
            }

            if (requeued > 0)
            {
                Logger.Warn($"Orphan sweep requeued {requeued} job(s) from processing:{queue} (worker death / timeout).");
            }
        }
    }

    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Security",
        "CA2100:Review SQL queries for security vulnerabilities",
        Justification =
            "The only interpolated value is ValkeyQueueOptions.Schema, which the application sets " +
            "in its own startup code to match the schema Hangfire.PostgreSql installed. An " +
            "identifier cannot be supplied as a parameter, so this is the available form.")]
    private void Reconcile()
    {
        var graceCutoff = DateTimeOffset.UtcNow - _options.ReconcileGrace;
        var db = _mux.GetDatabase();

        // Jobs Postgres currently reports as Enqueued. statename is denormalised onto
        // the job row by Hangfire.PostgreSql; the Enqueued state's Data JSON carries
        // the queue name and EnqueuedAt.
        var pgEnqueued = new List<(long Id, string Queue, DateTimeOffset EnqueuedAt)>();
        using (var conn = new NpgsqlConnection(_connectionString))
        {
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText =
                $"SELECT j.id, s.data FROM {_options.Schema}.job j " +
                $"JOIN {_options.Schema}.state s ON s.id = j.stateid " +
                "WHERE j.statename = 'Enqueued'";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var (queue, enqueuedAt) = ParseState(reader.GetString(1));
                pgEnqueued.Add((reader.GetInt64(0), queue, enqueuedAt));
            }
        }

        if (pgEnqueued.Count == 0)
        {
            return;
        }

        // Build live Valkey membership per queue once (queue + processing + in-flight).
        // ponytail: O(n) LRANGE scan per list; fine for POC queue sizes. Mirror
        //   membership in a Redis SET if queues ever get very large.
        var live = new Dictionary<string, HashSet<string>>();
        foreach (var queue in pgEnqueued.Select(x => x.Queue).Distinct())
        {
            var set = new HashSet<string>();
            foreach (var v in db.ListRange(_keys.Queue(queue)))
            {
                set.Add((string)v!);
            }

            foreach (var v in db.ListRange(_keys.Processing(queue)))
            {
                set.Add((string)v!);
            }

            foreach (var v in db.HashKeys(_keys.Fetched(queue)))
            {
                set.Add((string)v!);
            }

            live[queue] = set;
        }

        var reAdded = 0;
        foreach (var (id, queue, enqueuedAt) in pgEnqueued)
        {
            if (enqueuedAt > graceCutoff)
            {
                continue; // too fresh — an LPUSH may still be in flight
            }

            if (live[queue].Contains(id.ToString(System.Globalization.CultureInfo.InvariantCulture)))
            {
                continue;
            }

            db.ListLeftPush(_keys.Queue(queue), id.ToString(System.Globalization.CultureInfo.InvariantCulture));
            reAdded++;
        }

        if (reAdded > 0)
        {
            Logger.Warn($"Reconciler re-enqueued {reAdded} job(s) missing from Valkey (data-loss / failover recovery).");
        }
    }

    private static (string Queue, DateTimeOffset EnqueuedAt) ParseState(string data)
    {
        using var doc = JsonDocument.Parse(data);
        var root = doc.RootElement;
        var queue = root.TryGetProperty("Queue", out var q) ? q.GetString() ?? "default" : "default";
        var enqueuedAt = root.TryGetProperty("EnqueuedAt", out var e) && e.ValueKind == JsonValueKind.String
            ? DeserializeEnqueuedAt(e.GetString())
            // No timestamp at all -> treat as old so the job still recovers. A duplicate LPUSH is
            // execution-safe: only one worker wins the Enqueued->Processing transition in Postgres.
            : DateTimeOffset.MinValue;

        return (queue, enqueuedAt);
    }

    /// <summary>
    /// Reads the timestamp with Hangfire's OWN deserializer, which is the only thing guaranteed to
    /// match whatever <c>EnqueuedState.SerializeData</c> wrote.
    ///
    /// <para>The format depends on the configured <c>CompatibilityLevel</c>: up to Version_110 it is
    /// round-trip ISO-8601, from Version_170 on (which is what this app runs) it is unix
    /// milliseconds. A plain <c>DateTimeOffset.TryParse</c> silently failed on the numeric form and
    /// fell back to <see cref="DateTimeOffset.MinValue"/> — so every Enqueued job looked ancient, the
    /// ReconcileGrace window never applied, and the reconciler re-pushed a duplicate of every job
    /// being created while also logging its data-loss warning on nearly every pass.</para>
    /// </summary>
    /// <param name="value">The stored timestamp.</param>
    /// <returns>The timestamp as UTC, or <see cref="DateTimeOffset.MinValue"/> when unreadable.</returns>
    private static DateTimeOffset DeserializeEnqueuedAt(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return DateTimeOffset.MinValue;
        }

        try
        {
            return new DateTimeOffset(DateTime.SpecifyKind(JobHelper.DeserializeDateTime(value), DateTimeKind.Utc));
        }
        catch (Exception ex) when (ex is FormatException or ArgumentException or OverflowException)
        {
            // Genuinely unreadable -> recover the job rather than strand it.
            return DateTimeOffset.MinValue;
        }
    }
}
