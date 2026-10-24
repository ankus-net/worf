using Npgsql;

namespace Worf.Tests;

/// <summary>
/// Stops the provider and the subscriber with an immediate shutdown while changes are streaming, and checks that after
/// restart no rows are lost or applied twice.
/// </summary>
[TestClass]
public sealed class RestartRecoveryTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";
    private const int RowsPerTransaction = 10;
    private const int TotalRows = 30_000;

    private static readonly string[] s_configuration =
    [
        "wal_level = logical",
        "max_wal_senders = 10",
        "max_replication_slots = 10",
        "track_commit_timestamp = on",
        "worf.synchronous_commit = true",
    ];

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Crashes both nodes, then the subscriber alone, while a writer streams inserts. The table has no key, so a row
    /// applied twice would show as a duplicate, and one never applied as a missing id.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ImmediateShutdownWhileStreaming()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode provider = await TestNode.StartAsync("provider", s_configuration, token);
        await using TestNode subscriber = await TestNode.StartAsync("subscriber", s_configuration, token);

        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER", token);
            _ = await node.SafePsqlAsync(Database, "CREATE EXTENSION worf", token);
            _ = await node.SafePsqlAsync(Database,
                $"SELECT * FROM worf.create_node(node_name := '{node.Name}', dsn := '{node.Dsn(Database, SuperUser)}')", token);
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE stream_rows(id bigint, payload text)", token);
            _ = await node.SafePsqlAsync(Database, "CREATE INDEX ON stream_rows(id)", token);
        }

        _ = await provider.SafePsqlAsync(Database,
            "SELECT * FROM worf.replication_set_add_table('default_insert_only', 'stream_rows')", token);
        _ = await subscriber.SafePsqlAsync(Database, $$"""
            SELECT * FROM worf.create_subscription(
                subscription_name := 'test_subscription',
                provider_dsn := '{{provider.Dsn(Database, SuperUser)}}',
                replication_sets := '{default_insert_only}',
                synchronize_structure := false,
                synchronize_data := false)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.show_subscription_status() WHERE status = 'replicating')", token),
            "subscription failed to reach \"replicating\" state");

        using CancellationTokenSource writerStop = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task writer = WriteRowsAsync(provider, writerStop.Token);

        // Both nodes crash while changes stream.
        await WaitForRowsAsync(subscriber, 2_000, token);
        await Task.WhenAll(provider.StopAsync("immediate", token), subscriber.StopAsync("immediate", token));
        await subscriber.StartServerAsync(token);
        await provider.StartServerAsync(token);

        // The subscriber crashes alone while changes stream.
        await WaitForRowsAsync(subscriber, 12_000, token);
        await subscriber.StopAsync("immediate", token);
        await subscriber.StartServerAsync(token);

        await writer;

        _ = await provider.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);

        string expected = $"{TotalRows}|{TotalRows}|{(long)TotalRows * (TotalRows + 1) / 2}";
        string providerRows = await provider.SafePsqlAsync(Database,
            "SELECT count(*), count(DISTINCT id), sum(id) FROM stream_rows", token);
        string subscriberRows = await subscriber.SafePsqlAsync(Database,
            "SELECT count(*), count(DISTINCT id), sum(id) FROM stream_rows", token);
        Assert.AreEqual(expected, providerRows, "provider rows");
        Assert.AreEqual(expected, subscriberRows, "subscriber rows");
    }

    /// <summary>
    /// Inserts the rows in small transactions, retrying a transaction whose outcome a crash hides.
    /// </summary>
    private static async Task WriteRowsAsync(TestNode provider, CancellationToken cancellationToken)
    {
        long next = 1;
        while (next <= TotalRows)
        {
            try
            {
                await using NpgsqlConnection connection = await provider.OpenAsync(Database, cancellationToken);
                while (next <= TotalRows)
                {
                    // The statement inserts only ids not yet committed, so retrying after a crash can't duplicate a
                    // row whose commit the crash hid.
                    await using NpgsqlCommand command = new($"""
                        INSERT INTO stream_rows
                        SELECT i, repeat('x', 100) FROM generate_series({next}, {next + RowsPerTransaction - 1}) AS i
                        WHERE NOT EXISTS (SELECT 1 FROM stream_rows WHERE id = i)
                        """, connection);
                    _ = await command.ExecuteNonQueryAsync(cancellationToken);
                    next += RowsPerTransaction;

                    // Spread the writes out, so the crashes land while changes stream.
                    await Task.Delay(TimeSpan.FromMilliseconds(2), cancellationToken);
                }
            }
            catch (Exception error) when (error is NpgsqlException or InvalidOperationException && !cancellationToken.IsCancellationRequested)
            {
                // The provider is down; wait for it to come back.
                await Task.Delay(TimeSpan.FromMilliseconds(200), cancellationToken);
            }
        }
    }

    private static async Task WaitForRowsAsync(TestNode subscriber, int rows, CancellationToken cancellationToken)
        => Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            $"SELECT count(*) >= {rows} FROM stream_rows", cancellationToken), $"subscriber didn't reach {rows} rows");
}
