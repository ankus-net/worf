namespace Worf.Tests;

/// <summary>
/// Apply converts a column's values from the provider's type when the subscriber's column has another, as when a node
/// that hasn't yet received a column's change of type keeps writing.
/// </summary>
[TestClass]
public sealed class ColumnTypeTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Values sent in their type's internal or binary format are decoded with the provider's type, PostgreSQL's own by
    /// OID and others by name, and converted to the subscriber's through text, so a value that fits applies and one
    /// that doesn't fails with PostgreSQL's own error, which exception_behaviour then handles.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ValuesConvertToTheSubscribersColumnTypes()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token, "worf.exception_behaviour = discard");
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TYPE mood AS ENUM ('sad', 'happy');
                CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
                """, token);
        }

        // int4 to int8 and back, int4 to float4, text to int4 and back, a domain to int8, an enum to text and text to
        // an enum.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, a integer, b bigint, c integer, d text, e integer, f mood, g posint, h text);
            SELECT * FROM worf.replication_set_add_table('default', 't');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database,
            "CREATE TABLE t (id integer PRIMARY KEY, a bigint, b integer, c real, d integer, e text, f text, g bigint, h mood)", token);
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

        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (1, 1, 2, 3, '-4', 5, 'happy', 6, 'sad')", token);
        _ = await provider.SafePsqlAsync(Database, "UPDATE t SET a = 2147483647, d = '40' WHERE id = 1", token);

        // A value too big for the subscriber's column is discarded with its error.
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (2, 1, 3000000000, 3, '4', 5, 'sad', 6, 'happy')", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (3, 1, 2, 3, '4', 5, 'sad', 6, 'happy')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 3)", token),
            "the rows after the one that doesn't fit");
        Assert.AreEqual("1|2147483647|2|3|40|5|happy|6|sad\n3|1|2|3|4|5|sad|6|happy",
            await subscriber.SafePsqlAsync(Database, "SELECT concat_ws('|', id, a, b, c, d, e, f, g, h) FROM t ORDER BY id", token));
        Assert.AreEqual("INSERT|22003|3000000000", await subscriber.SafePsqlAsync(Database,
            "SELECT concat_ws('|', operation, error_sqlstate, remote_new_tuple->>'b') FROM worf.exception_log", token));
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token, params string[] settings)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            .. settings,
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
}
