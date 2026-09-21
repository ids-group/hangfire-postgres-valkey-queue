namespace ITB.HangfirePostgreSql.ValkeyQueue.Benchmarks;

/// <summary>Command-line knobs for a run. Defaults match the numbers published in docs/LOAD-TEST.md.</summary>
internal sealed class BenchmarkOptions
{
    public string PostgresConnectionString { get; private set; } =
        "Host=localhost;Port=55432;Database=hangfire_bench;Username=postgres;Password=Asd123!1";

    public string ValkeyConnectionString { get; private set; } = "localhost:56379";

    /// <summary>Jobs enqueued in the throughput phase.</summary>
    public int JobCount { get; private set; } = 2000;

    /// <summary>Hangfire worker threads draining the queue.</summary>
    public int WorkerCount { get; private set; } = 20;

    /// <summary>How long the idle phase observes a server with an empty queue.</summary>
    public TimeSpan IdleDuration { get; private set; } = TimeSpan.FromSeconds(60);

    /// <summary>Ceiling on the drain phase so a broken run fails instead of hanging.</summary>
    public TimeSpan DrainTimeout { get; private set; } = TimeSpan.FromMinutes(5);

    public static BenchmarkOptions Parse(string[] args)
    {
        var options = new BenchmarkOptions();

        for (var i = 0; i + 1 < args.Length; i += 2)
        {
            var value = args[i + 1];
            switch (args[i])
            {
                case "--postgres": options.PostgresConnectionString = value; break;
                case "--valkey": options.ValkeyConnectionString = value; break;
                case "--jobs": options.JobCount = int.Parse(value); break;
                case "--workers": options.WorkerCount = int.Parse(value); break;
                case "--idle-seconds": options.IdleDuration = TimeSpan.FromSeconds(double.Parse(value)); break;
                default: throw new ArgumentException($"Unknown argument '{args[i]}'.");
            }
        }

        return options;
    }
}
