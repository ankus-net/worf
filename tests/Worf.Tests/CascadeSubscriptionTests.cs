namespace Worf.Tests;

/// <summary>
/// Port of pglogical's <c>t/011_pglogical_cascade_sub.pl</c>: truncate on cascaded nodes with different replication
/// sets (RT87453).
/// </summary>
[TestClass]
public sealed class CascadeSubscriptionTests
{
    private const string Database = "pgltest";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// A TRUNCATE on the first node reaches the second, which subscribes to its replication set, but not the third,
    /// which subscribes to another set of the second.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task TruncateFollowsReplicationSets()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode nodeA = await TestNode.StartAsync("node_a",
        [
            "wal_level = 'logical'",
            "max_replication_slots = 12",
            "max_wal_senders = 12",
            "max_connections = 200",
            "log_line_prefix = '%t %p '",
            "track_commit_timestamp = on",
            "worf.synchronous_commit = true",
        ], token);
        _ = await nodeA.SafePsqlAsync("postgres", $"CREATE DATABASE {Database}", token);

        await using TestNode nodeB = await TestNode.StartAsync("node_b",
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
            "fsync=off",
            "worf.synchronous_commit = true",
        ], token);
        _ = await nodeB.SafePsqlAsync("postgres", $"CREATE DATABASE {Database}", token);

        await using TestNode nodeC = await TestNode.StartAsync("node_c",
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
            "fsync=off",
            "worf.synchronous_commit = true",
            "log_line_prefix = '%t %p '",
        ], token);
        _ = await nodeC.SafePsqlAsync("postgres", $"CREATE DATABASE {Database}", token);

        // Create worf node on all nodes
        foreach (TestNode n in new[] { nodeA, nodeB, nodeC })
        {
            _ = await n.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER;", token);
            _ = await n.SafePsqlAsync(Database, "CREATE EXTENSION IF NOT EXISTS worf;", token);
            _ = await n.SafePsqlAsync(Database, $"""
                SELECT * FROM worf.create_node(node_name := '{n.Name}',
                    dsn := '{n.Dsn(Database, SuperUser)}');
                """, token);
        }

        // create the different replication sets
        _ = await nodeA.SafePsqlAsync(Database, "SELECT * FROM worf.create_replication_set('set_a');", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT * FROM worf.create_replication_set('set_b');", token);

        // Create subscriptions and wait for them to be replicating
        _ = await nodeB.SafePsqlAsync(Database, $$"""
            SELECT worf.create_subscription(
                subscription_name := 'sub_a_b',
                replication_sets := '{set_a}',
                provider_dsn := '{{nodeA.Dsn(Database, SuperUser)}}'
            );
            """, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'sub_a_b' AND status = 'replicating');
            """, token), "subscription failed to reach \"replicating\" state");

        _ = await nodeC.SafePsqlAsync(Database, $$"""
            SELECT worf.create_subscription(
                subscription_name := 'sub_b_c',
                replication_sets := '{set_b}',
                provider_dsn := '{{nodeB.Dsn(Database, SuperUser)}}'
            );
            """, token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'sub_b_c' AND status = 'replicating');
            """, token), "subscription failed to reach \"replicating\" state");

        // Create the tables and add them to the correct repset
        _ = await nodeA.SafePsqlAsync(Database, "create table a2b( x int primary key);", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('set_a', 'a2b', false);", token);

        _ = await nodeB.SafePsqlAsync(Database, "create table a2b( x int primary key)", token);
        _ = await nodeB.SafePsqlAsync(Database, "create table b2c( x int primary key)", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('set_b', 'b2c', false);", token);

        _ = await nodeC.SafePsqlAsync(Database, "create table a2b( x int primary key)", token);
        _ = await nodeC.SafePsqlAsync(Database, "create table b2c( x int)", token);

        // Add an INVALID index, then add the real PRIMARY KEY.
        _ = await nodeC.SafePsqlAsync(Database, "insert into b2c values (1),(1)", token);
        _ = await nodeC.PsqlAsync(Database, "create unique index concurrently on b2c(x)", token);
        _ = await nodeC.SafePsqlAsync(Database, "delete from b2c", token);
        _ = await nodeC.SafePsqlAsync(Database, "alter table b2c add primary key (x)", token);

        // insert some rows in the a2b table to check that it actually worked
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO a2b VALUES (1)", token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO b2c VALUES (1)", token);
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO a2b VALUES (1)", token);

        await Task.Delay(TimeSpan.FromSeconds(2), token);

        Assert.AreEqual("1", await nodeA.SafePsqlAsync(Database, "TABLE a2b;", token), "row present on node_a");
        Assert.AreEqual("1", await nodeB.SafePsqlAsync(Database, "TABLE a2b;", token), "row present on node_b");
        Assert.AreEqual("1", await nodeC.SafePsqlAsync(Database, "TABLE a2b;", token), "row present on node_c");

        _ = await nodeA.SafePsqlAsync(Database, "TRUNCATE TABLE a2b", token);

        await Task.Delay(TimeSpan.FromSeconds(2), token);

        // check on node_a and node_b that the truncate actually deleted the rows in a2b but not on node_c or any
        // other table.
        Assert.AreEqual("", await nodeA.SafePsqlAsync(Database, "TABLE a2b;", token), "no row present on node_a");
        Assert.AreEqual("", await nodeB.SafePsqlAsync(Database, "TABLE a2b;", token), "no row present on node_b");
        Assert.AreEqual("1", await nodeC.SafePsqlAsync(Database, "TABLE a2b;", token), "row present on node_c");
        Assert.AreEqual("1", await nodeC.SafePsqlAsync(Database, "TABLE b2c;", token), "row present on node_c");

        // check the subscriptions again
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'sub_a_b' AND status = 'replicating');
            """, token), "subscription failed to reach \"replicating\" state");
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'sub_b_c' AND status = 'replicating');
            """, token), "subscription failed to reach \"replicating\" state");
    }
}
