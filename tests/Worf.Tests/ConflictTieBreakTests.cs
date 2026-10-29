using System.Globalization;

namespace Worf.Tests;

/// <summary>
/// Conflicts between changes with equal commit timestamps resolve alike on every node.
/// </summary>
[TestClass]
public sealed class ConflictTieBreakTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Under last_update_wins, a local update and a remote one committed at the same time keep the change from the node
    /// with the higher id, whichever node applies the remote change.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task EqualCommitTimesKeepTheSameNodesChange()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        uint idA = uint.Parse(await nodeA.SafePsqlAsync(Database, "SELECT node_id FROM worf.node", token), CultureInfo.InvariantCulture);
        uint idB = uint.Parse(await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.node", token), CultureInfo.InvariantCulture);
        bool aWins = idA > idB;

        // node_a applies node_b's change. The provider forwards every origin, so the change node_b commits under a
        // replication origin, which sets its commit timestamp, reaches node_a.
        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        Assert.AreEqual(aWins ? "a1" : "b1", await UpdateAtSameTimeAsync(nodeA, "a1", nodeB, "b1", token));

        // node_b applies node_a's change.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.drop_subscription('a_from_b')", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        Assert.AreEqual(aWins ? "a2" : "b2", await UpdateAtSameTimeAsync(nodeB, "b2", nodeA, "a2", token));
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
            "worf.synchronous_commit = true",
        ], token);
        try
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER", token);
            _ = await node.SafePsqlAsync(Database, "CREATE EXTENSION worf", token);
            _ = await node.SafePsqlAsync(Database,
                $"SELECT * FROM worf.create_node(node_name := '{name}', dsn := '{node.Dsn(Database, SuperUser)}')", token);
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE tie (id integer PRIMARY KEY, v text);
                INSERT INTO tie VALUES (1, 'initial');
                SELECT * FROM worf.replication_set_add_table('default', 'tie');
                """, token);
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
                forward_origins := '{all}')
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = '{name}' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");
    }

    /// <summary>
    /// Updates the row on the subscriber, then on the provider with the subscriber's commit timestamp, and returns
    /// the value the subscriber keeps.
    /// </summary>
    private static async Task<string> UpdateAtSameTimeAsync(TestNode subscriber, string localValue, TestNode provider,
        string remoteValue, CancellationToken token)
    {
        _ = await subscriber.SafePsqlAsync(Database, $"UPDATE tie SET v = '{localValue}' WHERE id = 1", token);
        string committed = await subscriber.SafePsqlAsync(Database,
            "SELECT pg_xact_commit_timestamp(xmin)::text FROM tie WHERE id = 1", token);

        _ = await provider.SafePsqlAsync(Database, "SELECT pg_replication_origin_create('tie_break')", token);
        _ = await provider.SafePsqlAsync(Database, $"""
            SELECT pg_replication_origin_session_setup('tie_break');
            SELECT pg_replication_origin_xact_setup('0/1', '{committed}');
            UPDATE tie SET v = '{remoteValue}' WHERE id = 1;
            """, token);
        _ = await provider.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);

        Assert.AreEqual("t", await provider.SafePsqlAsync(Database,
            $"SELECT pg_xact_commit_timestamp(xmin) = '{committed}' FROM tie WHERE id = 1", token), "the provider's commit time");
        return await subscriber.SafePsqlAsync(Database, "SELECT v FROM tie WHERE id = 1", token);
    }
}
