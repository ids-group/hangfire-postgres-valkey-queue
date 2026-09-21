using FluentAssertions;
using Hangfire;
using Hangfire.PostgreSql;
using Hangfire.PostgreSql.Factories;
using Hangfire.PostgreSql.ValkeyQueue;
using Hangfire.Server;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Xunit;

namespace IDS.HangfirePostgreSql.ValkeyQueue.Tests;

/// <summary>
/// The configuration shown in <c>docs/EXAMPLE.md</c>, compiled and executed.
///
/// <para>Documentation that only lives in a fenced code block rots the first time an option is
/// renamed, and the reader is the one who finds out. Running the same wiring here means a breaking
/// change to the public surface fails the build instead.</para>
/// </summary>
[Collection(ValkeyQueueCollection.Name)]
public class ConfigurationExampleTests(ValkeyQueueFixture fixture)
{
    private readonly ValkeyQueueFixture _fixture = fixture;

    [Fact]
    public void The_documented_setup_wires_up_and_points_the_queue_at_valkey()
    {
        var pgConnectionString = _fixture.PostgresConnectionString;

        // --- exactly the shape of docs/EXAMPLE.md, step 4 ------------------------------------
        var storageOptions = new PostgreSqlStorageOptions
        {
            UseSlidingInvisibilityTimeout = true,
            // The example enables this; here it stays off so the test never races Hangfire's
            // schema installer, which takes an advisory lock.
            PrepareSchemaIfNecessary = false,
            SchemaName = ExampleSchema,
        };

        var storage = new PostgreSqlStorage(
            new NpgsqlConnectionFactory(pgConnectionString, storageOptions), storageOptions);

        var redisOptions = _fixture.RedisConfig();
        using var mux = ConnectionMultiplexer.Connect(redisOptions);

        var valkeyOptions = new ValkeyQueueOptions
        {
            Queues = ["default"],
            InvisibilityTimeout = TimeSpan.FromMinutes(30),
            BlockingConnectionConfig = redisOptions,
            Schema = ExampleSchema,
        };

        storage.UseValkeyQueues(mux, valkeyOptions);

        var services = new ServiceCollection();
        services.AddSingleton<IConnectionMultiplexer>(mux);
        services.AddHangfire(config => config
            .SetDataCompatibilityLevel(CompatibilityLevel.Version_180)
            .UseSimpleAssemblyNameTypeSerializer()
            .UseRecommendedSerializerSettings()
            .UseStorage(storage));

        services.AddSingleton<IBackgroundProcess>(
            new ValkeyQueueMaintenance(mux, valkeyOptions, pgConnectionString));
        // --- end of the documented setup -----------------------------------------------------

        using var provider = services.BuildServiceProvider();

        provider.GetServices<IBackgroundProcess>().Should().ContainSingle(
            p => p is ValkeyQueueMaintenance,
            "the example tells the reader this registration is what prevents silent job loss");

        // The point of the whole setup: "default" must be served by Valkey, not Postgres.
        var queueProvider = storage.QueueProviders.GetProvider("default");

        queueProvider.Should().BeOfType<ValkeyJobQueueProvider>();
    }

    /// <summary>A schema of its own, so this never collides with the queue tests' fixtures.</summary>
    private const string ExampleSchema = "valkey_example_tests";
}
