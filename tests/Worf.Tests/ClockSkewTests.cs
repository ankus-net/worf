using System.Diagnostics;

namespace Worf.Tests;

/// <summary>
/// Apply waits for the local clock to pass a remote transaction's commit time, up to <c>worf.max_clock_skew</c>.
/// </summary>
[TestClass]
public sealed class ClockSkewTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Between nodes that replicate both ways under last_update_wins, a local change made after a remote one from a
    /// clock half a second ahead commits after it, and a remote change from a clock further ahead than
    /// <c>worf.max_clock_skew</c> applies at once, with a warning. One way, or under apply_remote, apply doesn't wait.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task LocalChangesCommitAfterRemoteChangesFromAheadOfTheClock()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);

        // The provider forwards every origin, so changes it commits under a replication origin, which sets their
        // commit timestamps, reach the subscriber.
        _ = await subscriber.SafePsqlAsync(Database, $$"""
            SELECT worf.create_subscription(
                subscription_name := 'skew',
                provider_dsn := '{{provider.Dsn(Database, SuperUser)}}',
                synchronize_structure := false,
                synchronize_data := false,
                forward_origins := '{all}')
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'skew' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");
        _ = await provider.SafePsqlAsync(Database, "SELECT pg_replication_origin_create('skew')", token);

        // One way, a change from a clock six seconds ahead applies without waiting.
        await AppliesAtOnceAsync(provider, subscriber, "one way", token);

        // Once the provider subscribes back, under last_update_wins on both nodes, which a session's setting doesn't
        // change, the subscriber's restarted worker waits.
        _ = await provider.SafePsqlAsync(Database, $$"""
            SET worf.conflict_resolution = error;
            SELECT worf.create_subscription(
                subscription_name := 'back',
                provider_dsn := '{{subscriber.Dsn(Database, SuperUser)}}',
                synchronize_structure := false,
                synchronize_data := false,
                forward_origins := '{}')
            """, token);
        Assert.DoesNotContain("replicate both ways", provider.ReadLog());
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('skew', true)", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('skew', true)", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'skew' AND status = 'replicating')
            """, token), "the restarted subscription");

        string ahead = await UpdateAheadAsync(provider, "remote", "500 milliseconds", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT v = 'remote' FROM skew", token), "the remote change");
        _ = await subscriber.SafePsqlAsync(Database, "UPDATE skew SET v = 'local'", token);
        Assert.AreEqual("t", await subscriber.SafePsqlAsync(Database,
            $"SELECT pg_xact_commit_timestamp(xmin) > '{ahead}' FROM skew", token), "the local change's commit time");

        _ = await UpdateAheadAsync(provider, "far", "1 hour", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT v = 'far' FROM skew", token), "the far change");
        Assert.Contains("more than worf.max_clock_skew", subscriber.ReadLog());

        // Under apply_remote, apply doesn't wait.
        _ = await subscriber.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.conflict_resolution = apply_remote", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        await AppliesAtOnceAsync(provider, subscriber, "apply_remote", token);
    }

    /// <summary>
    /// Checks that a change from a clock six seconds ahead applies well before the clock catches up.
    /// </summary>
    private static async Task AppliesAtOnceAsync(TestNode provider, TestNode subscriber, string value, CancellationToken token)
    {
        Stopwatch applying = Stopwatch.StartNew();
        _ = await UpdateAheadAsync(provider, value, "6 seconds", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"SELECT v = '{value}' FROM skew", token), $"the change {value}");
        Assert.IsLessThan(4500, applying.ElapsedMilliseconds, $"apply waited out the skew {value}");
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
            "worf.max_clock_skew = '10s'",
        ], token);
        try
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER", token);
            _ = await node.SafePsqlAsync(Database, "CREATE EXTENSION worf", token);
            _ = await node.SafePsqlAsync(Database,
                $"SELECT * FROM worf.create_node(node_name := '{name}', dsn := '{node.Dsn(Database, SuperUser)}')", token);
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE skew (id integer PRIMARY KEY, v text);
                INSERT INTO skew VALUES (1, 'initial');
                SELECT * FROM worf.replication_set_add_table('default', 'skew');
                """, token);
        }
        catch
        {
            await node.DisposeAsync();
            throw;
        }

        return node;
    }

    /// <summary>
    /// Updates the row with a commit timestamp ahead of the clock, and returns it.
    /// </summary>
    private static async Task<string> UpdateAheadAsync(TestNode node, string value, string interval, CancellationToken token)
    {
        _ = await node.SafePsqlAsync(Database, $"""
            SELECT pg_replication_origin_session_setup('skew');
            SELECT pg_replication_origin_xact_setup('0/1', clock_timestamp() + interval '{interval}');
            UPDATE skew SET v = '{value}' WHERE id = 1;
            """, token);
        return await node.SafePsqlAsync(Database, "SELECT pg_xact_commit_timestamp(xmin)::text FROM skew", token);
    }
}
