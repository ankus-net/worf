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
        bool aWins = await NodeIdAsync(nodeA, token) > await NodeIdAsync(nodeB, token);

        // node_a applies node_b's change. The provider forwards every origin, so the change node_b commits under a
        // replication origin, which sets its commit timestamp, reaches node_a.
        await SubscribeAsync(nodeA, "a_from_b", nodeB, true, token);
        string committed = await UpdateAsync(nodeA, "a1", token);
        await UpdateAtAsync(nodeB, "b1", committed, token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual(aWins ? "a1" : "b1", await ValueAsync(nodeA, token));

        // node_b applies node_a's change.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.drop_subscription('a_from_b')", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, true, token);
        committed = await UpdateAsync(nodeB, "b2", token);
        await UpdateAtAsync(nodeA, "a2", committed, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual(aWins ? "a2" : "b2", await ValueAsync(nodeB, token));
    }

    /// <summary>
    /// A tie between changes from two nodes keeps the same change on a node that relays one of them to the other, as
    /// on that other node, which compares the node that wrote the relayed change rather than the relay.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RelayedTiesKeepTheWritingNodesChange()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode node1 = await StartNodeAsync("node_1", token);
        await using TestNode node2 = await StartNodeAsync("node_2", token);
        await using TestNode node3 = await StartNodeAsync("node_3", token);

        // The relay has the lowest id and the leaf the highest, so the leaf's change wins, though the relay's id would
        // lose to the other writer.
        List<(uint Id, TestNode Node)> byId = [];
        foreach (TestNode node in new[] { node1, node2, node3 })
        {
            byId.Add((await NodeIdAsync(node, token), node));
        }

        byId.Sort((x, y) => x.Id.CompareTo(y.Id));
        (TestNode relay, TestNode writer, TestNode leaf) = (byId[0].Node, byId[1].Node, byId[2].Node);

        // The writer and the relay replicate both ways, and the relay relays the leaf's changes to the writer. The relay
        // takes only the writer's own changes, so nothing comes back to the writer but its own.
        await SubscribeAsync(relay, "relay_from_writer", writer, false, token);
        await SubscribeAsync(relay, "relay_from_leaf", leaf, true, token);
        await SubscribeAsync(writer, "writer_from_relay", relay, true, token);

        string committed = await UpdateAsync(writer, "writer", token);
        await UpdateAtAsync(leaf, "leaf", committed, token);
        _ = await leaf.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await writer.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await relay.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);

        Assert.AreEqual("leaf", await ValueAsync(relay, token), "the relay's row");
        Assert.AreEqual("leaf", await ValueAsync(writer, token), "the writer's row");
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

    private static async Task<uint> NodeIdAsync(TestNode node, CancellationToken token)
        => uint.Parse(await node.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token), CultureInfo.InvariantCulture);

    private static async Task SubscribeAsync(TestNode subscriber, string name, TestNode provider, bool forwardAll,
        CancellationToken token)
    {
        _ = await subscriber.SafePsqlAsync(Database, $$"""
            SELECT worf.create_subscription(
                subscription_name := '{{name}}',
                provider_dsn := '{{provider.Dsn(Database, SuperUser)}}',
                synchronize_structure := false,
                synchronize_data := false,
                forward_origins := '{{(forwardAll ? "{all}" : "{}")}}')
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = '{name}' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");
    }

    /// <summary>
    /// Updates the row and returns its commit timestamp.
    /// </summary>
    private static async Task<string> UpdateAsync(TestNode node, string value, CancellationToken token)
    {
        _ = await node.SafePsqlAsync(Database, $"UPDATE tie SET v = '{value}' WHERE id = 1", token);
        return await node.SafePsqlAsync(Database, "SELECT pg_xact_commit_timestamp(xmin)::text FROM tie WHERE id = 1", token);
    }

    /// <summary>
    /// Updates the row with the given commit timestamp, under a replication origin, which a provider sends only to
    /// subscribers that forward every origin.
    /// </summary>
    private static async Task UpdateAtAsync(TestNode node, string value, string committed, CancellationToken token)
    {
        _ = await node.SafePsqlAsync(Database, "SELECT pg_replication_origin_create('tie_break')", token);
        _ = await node.SafePsqlAsync(Database, $"""
            SELECT pg_replication_origin_session_setup('tie_break');
            SELECT pg_replication_origin_xact_setup('0/1', '{committed}');
            UPDATE tie SET v = '{value}' WHERE id = 1;
            """, token);
        Assert.AreEqual("t", await node.SafePsqlAsync(Database,
            $"SELECT pg_xact_commit_timestamp(xmin) = '{committed}' FROM tie WHERE id = 1", token), "the commit time");
    }

    private static Task<string> ValueAsync(TestNode node, CancellationToken token)
        => node.SafePsqlAsync(Database, "SELECT v FROM tie WHERE id = 1", token);
}
