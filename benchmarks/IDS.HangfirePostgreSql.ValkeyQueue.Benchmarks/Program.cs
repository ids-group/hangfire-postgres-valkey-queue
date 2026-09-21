using System.Globalization;
using IDS.HangfirePostgreSql.ValkeyQueue.Benchmarks;

var options = BenchmarkOptions.Parse(args);

Console.WriteLine("Hangfire PostgreSQL load: with Valkey vs without");
Console.WriteLine($"  jobs={options.JobCount} workers={options.WorkerCount} " +
                  $"idle={options.IdleDuration.TotalSeconds:N0}s");

var runner = new ScenarioRunner(options);

// Baseline first: if the environment is broken, it fails before the Valkey arm spends time on it.
var baseline = runner.Run("Postgres only (baseline)", "bench_baseline", useValkey: false);
var valkey = runner.Run("Postgres + Valkey queue", "bench_valkey", useValkey: true);

Console.WriteLine();
Console.WriteLine("=== Results ===");
Console.WriteLine();
Console.WriteLine($"| {"Metric",-42} | {"Postgres only",15} | {"With Valkey",15} | {"Change",12} |");
Console.WriteLine($"|{new string('-', 44)}|{new string('-', 17)}|{new string('-', 17)}|{new string('-', 14)}|");

Row("Idle Hangfire queries (total)", baseline.IdleLoad.Calls, valkey.IdleLoad.Calls, lowerIsBetter: true);
Row("Idle Hangfire queries / second", baseline.IdleQueriesPerSecond, valkey.IdleQueriesPerSecond, true, "N2");
Row("Idle transaction-control calls", baseline.IdleTransactions, valkey.IdleTransactions, true);
Row("Queries during drain (total)", baseline.ThroughputLoad.Calls, valkey.ThroughputLoad.Calls, true);
Row("Transaction-control calls during drain", baseline.ThroughputTransactions, valkey.ThroughputTransactions, true);
Row("Queries per job", baseline.QueriesPerJob, valkey.QueriesPerJob, true, "N2");
Row("Postgres exec time during drain (ms)", baseline.ThroughputLoad.TotalExecMs, valkey.ThroughputLoad.TotalExecMs, true, "N0");
Row("Drain duration (s)", baseline.DrainDuration.TotalSeconds, valkey.DrainDuration.TotalSeconds, true, "N1");
Row("Throughput (jobs / second)", baseline.JobsPerSecond, valkey.JobsPerSecond, lowerIsBetter: false, "N0");

Console.WriteLine();
Console.WriteLine($"Jobs completed: baseline {baseline.JobsCompleted}/{options.JobCount}, " +
                  $"valkey {valkey.JobsCompleted}/{options.JobCount}");

foreach (var scenario in new[] { baseline, valkey })
{
    Console.WriteLine();
    Console.WriteLine($"Top Hangfire statements — {scenario.Name}:");
    foreach (var (query, calls) in scenario.TopStatements)
    {
        Console.WriteLine($"  {calls,8}  {query}");
    }
}

return baseline.JobsCompleted == options.JobCount && valkey.JobsCompleted == options.JobCount ? 0 : 1;

void Row(string label, double before, double after, bool lowerIsBetter, string format = "N0")
{
    var change = before == 0
        ? "n/a"
        : $"{(after - before) / before * 100:+0.0;-0.0;0.0}%";

    // A metric that improved is worth spotting at a glance in a wall of numbers.
    var better = lowerIsBetter ? after < before : after > before;
    var marker = before == 0 ? " " : better ? "*" : " ";

    Console.WriteLine(
        $"| {label,-42} | {before.ToString(format, CultureInfo.InvariantCulture),15} " +
        $"| {after.ToString(format, CultureInfo.InvariantCulture),15} | {change + marker,12} |");
}
