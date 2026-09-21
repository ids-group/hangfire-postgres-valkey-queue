namespace ITB.HangfirePostgreSql.ValkeyQueue.Benchmarks;

/// <summary>
/// The unit of work. Deliberately empty: the benchmark measures the cost of moving a job THROUGH
/// Hangfire, and any real payload would dominate the numbers and hide the difference between the two
/// queue backends.
/// </summary>
public static class BenchmarkJob
{
    private static int _completed;

    /// <summary>Jobs finished since the last <see cref="Reset"/>.</summary>
    public static int Completed => Volatile.Read(ref _completed);

    public static void Reset() => Volatile.Write(ref _completed, 0);

    /// <summary>Hangfire invokes this per job.</summary>
    public static void Execute(int index) => Interlocked.Increment(ref _completed);
}
