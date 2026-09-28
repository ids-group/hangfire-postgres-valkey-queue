using StackExchange.Redis;

namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>Tunables for the Valkey-backed Hangfire queue.</summary>
public sealed class ValkeyQueueOptions
{
    /// <summary>Prefix for every Valkey key this provider owns.</summary>
    public string KeyPrefix { get; set; } = "hangfire:";

    /// <summary>How long a single BLMOVE blocks waiting for a job before the worker loops.</summary>
    public TimeSpan BlockTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>How long a job may go without a heartbeat before the orphan sweep treats it as
    /// abandoned and requeues it.
    ///
    /// <para>This is a liveness window, not a runtime budget: a worker holding a job refreshes its
    /// timestamp every <see cref="HeartbeatInterval"/>, so a job running for hours is safe as long as
    /// its process is alive. Size this against how long a dead worker may go unnoticed, and keep it at
    /// least 3x <see cref="HeartbeatInterval"/> so a single missed tick never requeues a live job.</para>
    ///
    /// <para>The 30 minute default is deliberately conservative: it is safe for a fleet still running
    /// a version of this package that does not send heartbeats. Once every host is on 1.1.0 or later,
    /// lowering it to a few minutes makes recovery from worker death much faster.</para></summary>
    public TimeSpan InvisibilityTimeout { get; set; } = TimeSpan.FromMinutes(30);

    /// <summary>How often a worker holding a job refreshes its fetched timestamp, proving the job is
    /// still alive. The refresh only rewrites a timestamp that is still there, so it can never
    /// resurrect a job the sweep has already requeued.</summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>How often the reconciler + orphan sweep run.</summary>
    public TimeSpan MaintenanceInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>The reconciler ignores jobs enqueued more recently than this, so it never
    /// races an LPUSH that is still in flight.</summary>
    public TimeSpan ReconcileGrace { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Postgres schema Hangfire.PostgreSql created its tables in.</summary>
    public string Schema { get; set; } = "hangfire";

    /// <summary>Queues served by Valkey. Anything not listed stays on Postgres.</summary>
    public string[] Queues { get; set; } = ["default"];

    /// <summary>ConfigurationOptions for the dedicated blocking (BLMOVE) connections. When null,
    /// they are derived from the shared multiplexer's connection string — which masks the password
    /// on managed Valkey (TLS+AUTH), so the cloned connection would fail to authenticate. Set this
    /// to the same options the multiplexer was built from whenever AUTH/TLS is in play.</summary>
    public ConfigurationOptions? BlockingConnectionConfig { get; set; }
}
