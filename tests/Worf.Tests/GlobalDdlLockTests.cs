using Npgsql;

namespace Worf.Tests;

/// <summary>
/// <c>worf.global_ddl_lock</c> holds back writes on the other nodes for replicated DDL that can't apply in any order.
/// </summary>
[TestClass]
public sealed class GlobalDdlLockTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// A column type change replicates under the lock and lets the other node's writes through once it applies. A fence
    /// holds writes back until the session that asked for it ends, or, once kept, until it's released. DDL fails when a
    /// node's write under way outlasts worf.ddl_lock_timeout.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DdlThatCantApplyInAnyOrderHoldsBackWrites()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id integer PRIMARY KEY, v integer);
                SELECT * FROM worf.replication_set_add_table('default', 't');
                """, token);
        }

        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);

        _ = await nodeA.SafePsqlAsync(Database, "ALTER TABLE t ALTER COLUMN v TYPE bigint", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("bigint", await nodeB.SafePsqlAsync(Database, "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'v'", token));
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (1, 10000000000)", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("10000000000", await nodeA.SafePsqlAsync(Database, "SELECT v FROM t WHERE id = 1", token));

        // A fence holds back writes while the session that asked for it lasts.
        await using (NpgsqlConnection fencing = await nodeB.OpenAsync(Database, token))
        {
            await using NpgsqlCommand fence = new("SELECT worf.fence_writes(42, ARRAY['public.t'], 1000)", fencing);
            _ = await fence.ExecuteScalarAsync(token);
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(
                () => nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (2, 2)", token));
            Assert.AreEqual("55P03", error.SqlState);
        }

        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (2, 2)", token);

        // Once kept, until it's released.
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.fence_writes(43, ARRAY['public.t'], 1000); SELECT worf.keep_fence(43)", token);
        Assert.IsFalse(await nodeB.PsqlAsync(Database, "INSERT INTO t VALUES (3, 3)", token), "a write through a kept fence");
        Assert.AreEqual("1", await nodeB.SafePsqlAsync(Database, "SELECT worf.release_fence(43)", token));
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (3, 3)", token);

        // A write under way on the other node that outlasts the timeout fails the DDL and drops its fence.
        await using (NpgsqlConnection writing = await nodeB.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await writing.BeginTransactionAsync(token);
            await using (NpgsqlCommand insert = new("INSERT INTO t VALUES (4, 4)", writing, transaction))
            {
                _ = await insert.ExecuteNonQueryAsync(token);
            }

            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeA.SafePsqlAsync(Database,
                "SET worf.ddl_lock_timeout = '1s'; ALTER TABLE t ALTER COLUMN v TYPE numeric", token));
            Assert.AreEqual("55P03", error.SqlState);
            await transaction.CommitAsync(token);
        }

        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (5, 5)", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("1,2,3,4,5|bigint", await nodeA.SafePsqlAsync(Database,
            "SELECT string_agg(id::text, ',' ORDER BY id), pg_typeof(min(v))::text FROM t", token));
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
}
