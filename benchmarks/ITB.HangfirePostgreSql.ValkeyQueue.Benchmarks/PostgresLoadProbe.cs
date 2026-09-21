using System.Globalization;
using Npgsql;

namespace ITB.HangfirePostgreSql.ValkeyQueue.Benchmarks;

/// <summary>A pg_stat_statements snapshot: how much work Postgres actually did.</summary>
internal sealed record PostgresLoad(long Calls, double TotalExecMs, long RowsRead)
{
    public static PostgresLoad operator -(PostgresLoad a, PostgresLoad b) =>
        new (a.Calls - b.Calls, a.TotalExecMs - b.TotalExecMs, a.RowsRead - b.RowsRead);
}

/// <summary>
/// Measures Postgres-side load via <c>pg_stat_statements</c>, which counts the queries the server
/// executed rather than what the client thinks it sent — the whole claim of this library is that the
/// idle worker poll disappears from Postgres, and this is where that shows up.
///
/// <para>Only Hangfire's own statements are counted (the ones touching the Hangfire schema), so the
/// benchmark's own bookkeeping queries can't inflate either side.</para>
/// </summary>
internal sealed class PostgresLoadProbe(string connectionString, string schema)
{
    /// <summary>
    /// Hangfire quotes identifiers, so its normalised SQL reads <c>"bench_valkey"."job"</c> — the
    /// quote characters sit between schema and table, and a naive <c>%schema.%</c> pattern matches
    /// nothing at all (which reads as "zero load" for both arms rather than as a broken filter).
    ///
    /// <para>This counts statements attributable to the Hangfire schema. Each arm's bare
    /// <c>BEGIN</c>/<c>COMMIT</c>/<c>DISCARD ALL</c> name no table and so fall outside it; they are
    /// reported separately by <see cref="TransactionCalls"/>.</para>
    /// </summary>
    private string SchemaPattern => $"%\"{schema}\".%";

    /// <summary>Verifies the extension is installed, and fails loudly if not — a run without it
    /// would silently report zero load for both arms and "prove" nothing.</summary>
    public void EnsureAvailable()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "CREATE EXTENSION IF NOT EXISTS pg_stat_statements";

        try
        {
            cmd.ExecuteNonQuery();
        }
        catch (PostgresException ex)
        {
            throw new InvalidOperationException(
                "pg_stat_statements is unavailable. Start Postgres with " +
                "`-c shared_preload_libraries=pg_stat_statements` (see docs/LOAD-TEST.md).", ex);
        }
    }

    public void Reset()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT pg_stat_statements_reset()";
        cmd.ExecuteNonQuery();
    }

    public PostgresLoad Snapshot()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();

        // Hangfire's statements are the ones naming its schema. LIKE over the normalised query text
        // is crude but sufficient here: the alternative (filtering by dbid alone) would sweep in the
        // probe's own SELECTs and the harness's enqueue helpers.
        cmd.CommandText =
            "SELECT COALESCE(SUM(calls), 0)::bigint, " +
            "       COALESCE(SUM(total_exec_time), 0)::double precision, " +
            "       COALESCE(SUM(rows), 0)::bigint " +
            "FROM pg_stat_statements " +
            "WHERE query ILIKE @pattern";
        cmd.Parameters.AddWithValue("pattern", SchemaPattern);

        using var reader = cmd.ExecuteReader();
        reader.Read();

        return new PostgresLoad(reader.GetInt64(0), reader.GetDouble(1), reader.GetInt64(2));
    }

    /// <summary>
    /// Bare transaction-control statements (<c>BEGIN</c> / <c>COMMIT</c> / <c>DISCARD ALL</c>). They
    /// name no table, so the schema filter cannot see them, but they are round trips Postgres served
    /// and a connection-pool cost — worth reporting rather than quietly dropping.
    /// </summary>
    public long TransactionCalls()
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT COALESCE(SUM(calls), 0)::bigint FROM pg_stat_statements " +
            "WHERE query IN ('BEGIN', 'COMMIT', 'ROLLBACK', 'DISCARD ALL') " +
            "   OR query ILIKE 'BEGIN TRANSACTION%'";

        return (long)cmd.ExecuteScalar()!;
    }

    /// <summary>The top Hangfire statements by call count — what the load actually consisted of.</summary>
    public IReadOnlyList<(string Query, long Calls)> TopStatements(int limit)
    {
        using var conn = Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText =
            "SELECT query, calls FROM pg_stat_statements " +
            "WHERE query ILIKE @pattern ORDER BY calls DESC LIMIT @limit";
        cmd.Parameters.AddWithValue("pattern", SchemaPattern);
        cmd.Parameters.AddWithValue("limit", limit);

        var rows = new List<(string, long)>();
        using var reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            rows.Add((Normalise(reader.GetString(0)), reader.GetInt64(1)));
        }

        return rows;
    }

    private static string Normalise(string query)
    {
        var collapsed = string.Join(' ', query.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

        return collapsed.Length <= 110
            ? collapsed
            : string.Concat(collapsed.AsSpan(0, 107), "...");
    }

    private NpgsqlConnection Open()
    {
        var conn = new NpgsqlConnection(connectionString);
        conn.Open();

        return conn;
    }

    public static string Format(double value) => value.ToString("N1", CultureInfo.InvariantCulture);
}
