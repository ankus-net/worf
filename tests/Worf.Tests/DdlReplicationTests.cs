namespace Worf.Tests;

/// <summary>
/// <c>worf.enable_ddl_replication</c> replicates DDL as it runs, and <c>worf.include_ddl_repset</c> replicates the
/// tables it creates.
/// </summary>
[TestClass]
public sealed class DdlReplicationTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// DDL on either of two nodes that replicate both ways reaches the other, with its search path, and the tables it
    /// creates replicate both ways. Temporary objects stay on their node.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DdlAndTheTablesItCreatesReplicate()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);

        // A table created on one node replicates both ways.
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SCHEMA app;
            SET search_path = app;
            CREATE TABLE t (id integer PRIMARY KEY, v text);
            INSERT INTO t VALUES (1, 'from a');
            """, token);
        await WaitAsync(nodeA, token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO app.t VALUES (2, 'from b')", token);
        await WaitAsync(nodeB, token);
        Assert.AreEqual("1|from a\n2|from b", await nodeA.SafePsqlAsync(Database, "SELECT * FROM app.t ORDER BY id", token));
        Assert.AreEqual("1|from a\n2|from b", await nodeB.SafePsqlAsync(Database, "SELECT * FROM app.t ORDER BY id", token));

        // DDL from the other node, including an index built concurrently, and temporary tables that stay put.
        _ = await nodeB.SafePsqlAsync(Database, """
            CREATE TEMP TABLE scratch (id integer);
            ALTER TABLE scratch ADD COLUMN w text;
            DROP TABLE scratch;
            ALTER TABLE app.t ADD COLUMN w text DEFAULT 'w';
            """, token);
        _ = await nodeB.SafePsqlAsync(Database, "CREATE INDEX CONCURRENTLY t_v ON app.t (v)", token);
        await WaitAsync(nodeB, token);
        Assert.AreEqual("w|t_v", await nodeA.SafePsqlAsync(Database, """
            SELECT (SELECT string_agg(attname, ',') FROM pg_attribute WHERE attrelid = 'app.t'::regclass AND attname = 'w'),
                (SELECT string_agg(indexname, ',') FROM pg_indexes WHERE tablename = 't' AND indexname = 't_v')
            """, token));

        // A table without a primary key replicates inserts until it gets one.
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE TABLE app.nopk (id integer NOT NULL, v text);
            INSERT INTO app.nopk VALUES (1, 'one');
            """, token);
        await WaitAsync(nodeA, token);
        Assert.AreEqual("default_insert_only", await SetsAsync(nodeB, "app.nopk", token));
        _ = await nodeA.SafePsqlAsync(Database, """
            ALTER TABLE app.nopk ADD PRIMARY KEY (id);
            UPDATE app.nopk SET v = 'uno';
            """, token);
        await WaitAsync(nodeA, token);
        Assert.AreEqual("default", await SetsAsync(nodeA, "app.nopk", token));
        Assert.AreEqual("default", await SetsAsync(nodeB, "app.nopk", token));
        Assert.AreEqual("1|uno", await nodeB.SafePsqlAsync(Database, "SELECT * FROM app.nopk", token));

        _ = await nodeA.SafePsqlAsync(Database, "DROP TABLE app.nopk", token);
        await WaitAsync(nodeA, token);
        Assert.AreEqual("", await nodeB.SafePsqlAsync(Database, "SELECT to_regclass('app.nopk')", token));

        // worf's own tables stay out of replication sets, and each node drops worf itself.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.conflict_history_ensure_partition(CURRENT_DATE + 60)", token);
        Assert.AreEqual("0", await nodeA.SafePsqlAsync(Database, """
            SELECT count(*) FROM worf.replication_set_table r JOIN pg_class c ON c.oid = r.set_reloid
            WHERE c.relnamespace = 'worf'::regnamespace
            """, token));
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.drop_subscription('b_from_a')", token);
        _ = await nodeB.SafePsqlAsync(Database, "DROP EXTENSION worf CASCADE", token);
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
            "worf.conflict_resolution = last_update_wins",
            "worf.enable_ddl_replication = on",
            "worf.include_ddl_repset = on",
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

    private static async Task SubscribeAsync(TestNode subscriber, string name, TestNode provider, CancellationToken token)
    {
        _ = await subscriber.SafePsqlAsync(Database, $$"""
            SELECT worf.create_subscription(
                subscription_name := '{{name}}',
                provider_dsn := '{{provider.Dsn(Database, SuperUser)}}',
                synchronize_structure := false,
                synchronize_data := false,
                forward_origins := '{}')
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = '{name}' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");
    }

    /// <summary>
    /// Waits for the provider's subscriber to apply everything the provider has written.
    /// </summary>
    private static async Task WaitAsync(TestNode provider, CancellationToken token)
        => _ = await provider.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);

    private static Task<string> SetsAsync(TestNode node, string table, CancellationToken token)
        => node.SafePsqlAsync(Database,
            $"SELECT string_agg(set_name, ',' ORDER BY set_name) FROM worf.tables WHERE relid = '{table}'::regclass", token);
}
