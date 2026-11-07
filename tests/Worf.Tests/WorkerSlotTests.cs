using System.Diagnostics;
using Npgsql;

namespace Worf.Tests;

/// <summary>
/// worf's workers take slots that outlive their processes while their database's manager may still read them.
/// </summary>
[TestClass]
public sealed class WorkerSlotTests
{
    private const string Database = "postgres";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Databases dropped with their workers running, as DROP DATABASE ... WITH (FORCE) drops them, more times than
    /// there are slots, leave no slots behind: a database created after them still gets its manager.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DroppedDatabasesFreeTheirWorkerSlots()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await TestNode.StartAsync("node", ["max_worker_processes = 8"], token);
        for (int i = 0; i < 12; i++)
        {
            string database = $"dropped{i}";
            _ = await node.SafePsqlAsync(Database, $"CREATE DATABASE {database}", token);
            _ = await node.SafePsqlAsync(database, $"""
                CREATE EXTENSION worf;
                SELECT * FROM worf.create_node(node_name := 'node{i}', dsn := '{node.Dsn(database)}');
                """, token);
            if (!await node.PollQueryUntilAsync(Database,
                $"SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = '{database}' AND backend_type LIKE 'worf manager%')", token))
            {
                // The sessions and processes, as the supervisor's wait shows among them, say why no manager started.
                Assert.Fail($"{database}'s manager\n{await node.DescribeReplicationAsync(Database, token)}");
            }

            _ = await node.SafePsqlAsync(Database, $"DROP DATABASE {database} WITH (FORCE)", token);
        }

        _ = await node.SafePsqlAsync(Database, "CREATE DATABASE kept", token);
        _ = await node.SafePsqlAsync("kept", $"""
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'kept', dsn := '{node.Dsn("kept")}');
            """, token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = 'kept' AND backend_type LIKE 'worf manager%')", token),
            "the manager of the database created after them");
        Assert.DoesNotContain("all background worker slots are already used", node.ReadLog());
    }

    /// <summary>
    /// CREATE EXTENSION worf in a prepared transaction gives the database its manager once COMMIT PREPARED commits it,
    /// though the manager that PREPARE TRANSACTION woke found nothing to manage then.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task PreparedExtensionsGetTheirManager()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await TestNode.StartAsync("node", ["max_prepared_transactions = 2"], token);
        _ = await node.SafePsqlAsync(Database, "CREATE DATABASE prepared", token);
        const string Manager = "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE datname = 'prepared' AND backend_type LIKE 'worf manager%')";
        _ = await node.SafePsqlAsync("prepared", $"""
            BEGIN;
            CREATE EXTENSION worf;
            SELECT * FROM worf.create_node(node_name := 'prepared', dsn := '{node.Dsn("prepared")}');
            PREPARE TRANSACTION 'extension';
            """, token);

        // The manager that the prepared transaction woke finds nothing to manage yet, and exits.
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"SELECT NOT ({Manager})", token), "no manager while the extension is prepared");

        _ = await node.SafePsqlAsync("prepared", "COMMIT PREPARED 'extension'", token);
        Stopwatch starting = Stopwatch.StartNew();
        while (await node.SafePsqlAsync(Database, Manager, token) != "t")
        {
            Assert.IsLessThan(TimeSpan.FromSeconds(30), starting.Elapsed, "the manager once COMMIT PREPARED committed the extension");
            await Task.Delay(TimeSpan.FromMilliseconds(200), token);
        }
    }

    /// <summary>
    /// COMMIT PREPARED of a transaction that changed neither worf nor a database starts no manager, in its own database
    /// or any other, though the managers that found nothing to manage there exited.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task UnrelatedPreparedCommitsStartNoManager()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await TestNode.StartAsync("node", ["max_prepared_transactions = 2", "log_min_messages = debug1"], token);
        for (int i = 0; i < 3; i++)
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE DATABASE unrelated{i}", token);
        }

        _ = await node.SafePsqlAsync("unrelated0", "CREATE TABLE t (a int)", token);
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        int before = Starts();

        // Nor does it read the catalogs, as one that scans pg_db_role_setting would.
        await using NpgsqlConnection committing = await node.OpenAsync("unrelated0", token);
        long scansBefore = await ScansAsync(committing, token);
        for (int i = 0; i < 20; i++)
        {
            await using NpgsqlCommand prepared = new($"BEGIN; INSERT INTO t VALUES ({i}); PREPARE TRANSACTION 'unrelated'; COMMIT PREPARED 'unrelated'", committing);
            _ = await prepared.ExecuteNonQueryAsync(token);
        }

        Assert.AreEqual(scansBefore, await ScansAsync(committing, token), "scans of pg_db_role_setting by 20 unrelated COMMIT PREPARED");
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.AreEqual(before, Starts(), "managers started by 20 unrelated COMMIT PREPARED");

        int Starts() => node.ReadLog().Split('\n').Count(line => line.Contains("starting background worker process \"worf manager", StringComparison.Ordinal));
    }

    /// <summary>
    /// A subscription created in a prepared transaction gets its apply worker once COMMIT PREPARED commits it, as one
    /// committed at once does, rather than at the manager's next look.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task PreparedSubscriptionsStartTheirWorker()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await TestNode.StartAsync("provider", ["wal_level = logical", "max_wal_senders = 10", "max_replication_slots = 10"], token);
        await using TestNode subscriber = await TestNode.StartAsync("subscriber", ["max_prepared_transactions = 2"], token);
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, $"""
                CREATE EXTENSION worf;
                SELECT * FROM worf.create_node(node_name := '{node.Name}', dsn := '{node.Dsn(Database, "postgres")}');
                """, token);
        }

        _ = await subscriber.SafePsqlAsync(Database, $"""
            BEGIN;
            SELECT worf.create_subscription(subscription_name := 'ps', provider_dsn := '{provider.Dsn(Database, "postgres")}',
                synchronize_structure := false, synchronize_data := false);
            PREPARE TRANSACTION 'ps';
            """, token);
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        _ = await subscriber.SafePsqlAsync(Database, "COMMIT PREPARED 'ps'", token);
        Stopwatch starting = Stopwatch.StartNew();
        while (await subscriber.SafePsqlAsync(Database, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE backend_type LIKE 'worf apply%')", token) != "t")
        {
            Assert.IsLessThan(TimeSpan.FromSeconds(3), starting.Elapsed, "the apply worker once COMMIT PREPARED committed the subscription");
            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }
    }

    // How often pg_db_role_setting has been scanned whole, as the statistics show once the session's own are flushed.
    private static async Task<long> ScansAsync(NpgsqlConnection connection, CancellationToken token)
    {
        await using (NpgsqlCommand flush = new("SELECT pg_stat_force_next_flush(); SELECT pg_stat_clear_snapshot()", connection))
        {
            _ = await flush.ExecuteNonQueryAsync(token);
        }

        await using NpgsqlCommand scans = new("SELECT seq_scan FROM pg_stat_sys_tables WHERE relname = 'pg_db_role_setting'", connection);
        return (long)(await scans.ExecuteScalarAsync(token))!;
    }
}
