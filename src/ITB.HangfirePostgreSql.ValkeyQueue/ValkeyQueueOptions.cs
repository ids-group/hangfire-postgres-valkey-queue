using StackExchange.Redis;

namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>Tunables for the Valkey-backed Hangfire queue.</summary>
public sealed class ValkeyQueueOptions
{
    /// <summary>Prefix for every Valkey key this provider owns.</summary>
    public string KeyPrefix { get; set; } = "hangfire:";

    /// <summary>How long a single BLMOVE blocks waiting for a job before the worker loops.</summary>
    public TimeSpan BlockTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>A job sitting in a processing list longer than this is treated as orphaned
    /// (its worker died) and requeued. Must exceed your longest expected job runtime.</summary>
    public TimeSpan InvisibilityTimeout { get; set; } = TimeSpan.FromMinutes(30);

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
