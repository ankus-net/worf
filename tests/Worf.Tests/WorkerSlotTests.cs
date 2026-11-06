using System.Diagnostics;

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
}
