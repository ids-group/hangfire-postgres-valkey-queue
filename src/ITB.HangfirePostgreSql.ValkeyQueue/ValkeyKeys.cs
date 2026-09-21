namespace Hangfire.PostgreSql.ValkeyQueue;

/// <summary>Central place for the three Valkey keys per queue, so the queue,
/// monitoring API and maintenance job can't drift apart.</summary>
internal sealed class ValkeyKeys(string prefix)
{
    // The queue name is wrapped in a {hash tag} so all three keys for a queue land in the same
    // Valkey/Redis Cluster hash slot. Without this, the LMOVE/BLMOVE in Dequeue (queue -> processing)
    // spans two slots and cluster-mode rejects it with CROSSSLOT. Single-node ignores hash tags,
    // so this is safe everywhere.

    /// <summary>List of job ids waiting to be processed.</summary>
    public string Queue(string queue) => $"{prefix}{{{queue}}}:queue";

    /// <summary>List of job ids handed to a worker but not yet acked (reliable-queue pattern).</summary>
    public string Processing(string queue) => $"{prefix}{{{queue}}}:processing";

    /// <summary>Hash of jobId -> fetched-at unix ms, used by the orphan sweep.</summary>
    public string Fetched(string queue) => $"{prefix}{{{queue}}}:fetched";
}
