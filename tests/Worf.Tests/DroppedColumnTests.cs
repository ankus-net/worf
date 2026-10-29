namespace Worf.Tests;

/// <summary>
/// Apply ignores a column the subscriber dropped in rows from a provider that still has it.
/// </summary>
[TestClass]
public sealed class DroppedColumnTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Rows keep replicating after the subscriber drops a column the provider still has, and a column the subscriber
    /// never had still stops apply.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RowsWithAColumnDroppedHereStillApply()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, a text, b text);
            SELECT * FROM worf.replication_set_add_table('default', 't');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE t (id integer PRIMARY KEY, a text, b text)", token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := false)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'sub' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");

        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (1, 'a1', 'b1')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 1)", token),
            "the row before the drop");

        _ = await subscriber.SafePsqlAsync(Database, "ALTER TABLE t DROP COLUMN b", token);
        _ = await provider.SafePsqlAsync(Database, """
            INSERT INTO t VALUES (2, 'a2', 'b2');
            UPDATE t SET a = 'a1 again', b = 'b1 again' WHERE id = 1;
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 2)", token),
            "the rows after the drop");
        Assert.AreEqual("1|a1 again\n2|a2", await subscriber.SafePsqlAsync(Database, "SELECT * FROM t ORDER BY id", token));

        int logpos = subscriber.ReadLog().Length;
        _ = await provider.SafePsqlAsync(Database, """
            ALTER TABLE t ADD COLUMN c text;
            INSERT INTO t VALUES (3, 'a3', 'b3', 'c3');
            """, token);
        Assert.IsTrue(await PollLogAsync(subscriber, logpos, "unknown column name c", token), "the column the subscriber never had");
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
        ], token);
        try
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER", token);
            _ = await node.SafePsqlAsync(Database, "CREATE EXTENSION worf", token);
            _ = await node.SafePsqlAsync(Database,
                $"SELECT * FROM worf.create_node(node_name := '{name}', dsn := '{node.Dsn(Database, SuperUser)}')", token);
        }
        catch
        {
            await node.DisposeAsync();
            throw;
        }

        return node;
    }

    private static async Task<bool> PollLogAsync(TestNode node, int logpos, string text, CancellationToken token)
    {
        using CancellationTokenSource timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TestNode.PollTimeout);
        while (!node.ReadLog()[logpos..].Contains(text, StringComparison.Ordinal))
        {
            if (timeout.IsCancellationRequested)
            {
                return false;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }

        return true;
    }
}
