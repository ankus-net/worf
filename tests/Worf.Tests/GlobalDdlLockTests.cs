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
            await using NpgsqlCommand fence = new(
                "SELECT worf.fence_writes(42, (SELECT node_id FROM worf.node WHERE node_name = 'node_a'), ARRAY['public.t'], 1000)", fencing);
            _ = await fence.ExecuteScalarAsync(token);
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(
                () => nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (2, 2)", token));
            Assert.AreEqual("55P03", error.SqlState);
        }

        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (2, 2)", token);

        // Once kept, until it's released.
        _ = await nodeB.SafePsqlAsync(Database, """
            SELECT worf.fence_writes(43, (SELECT node_id FROM worf.node WHERE node_name = 'node_a'), ARRAY['public.t'], 1000);
            SELECT worf.keep_fence(43, (SELECT node_id FROM worf.node WHERE node_name = 'node_a'), 0);
            """, token);
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

    /// <summary>
    /// The DDL waits until this node has applied the other node's earlier writes, and the other node's writes fail
    /// while it's in flight. DDL in a transaction block holds the lock from its first statement, a savepoint rolled
    /// back lets its fences go, a unique index built concurrently can't take the lock, and a fence holds back the
    /// other node's DDL but not EXPLAIN, across a restart, until released. A table owner who isn't a superuser takes
    /// the lock too.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task TheLockOrdersWritesAroundDdl()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE ROLE owner;
                CREATE TABLE t (id integer PRIMARY KEY, v integer);
                ALTER TABLE t OWNER TO owner;
                SELECT * FROM worf.replication_set_add_table('default', 't');
                """, token);
        }

        await SubscribeAsync(nodeA, "a_from_b", nodeB, token, "3 seconds");
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);

        // node_a applies node_b's changes three seconds late, so its DDL waits for node_b's earlier write, which it
        // has applied once the DDL returns, and node_b's writes fail meanwhile.
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (1, 1)", token);
        Task<string> ddl = nodeA.SafePsqlAsync(Database, "SET ROLE owner; ALTER TABLE t ALTER COLUMN v TYPE bigint", token);
        Assert.IsTrue(await PollFailsAsync(nodeB, "UPDATE t SET v = v WHERE id = 1", token), "node_b's writes held back");
        _ = await ddl;
        Assert.AreEqual("1", await nodeA.SafePsqlAsync(Database, "SELECT count(*) FROM t WHERE id = 1", token), "node_b's earlier write");
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (2, 20000000000)", token);
        Assert.AreEqual("1|bigint", await nodeA.SafePsqlAsync(Database, """
            SELECT count(*), format_type(atttypid, atttypmod) FROM t, pg_attribute
            WHERE id = 1 AND attrelid = 't'::regclass AND attname = 'v' GROUP BY 2
            """, token));

        // In a transaction block, the first DDL takes the lock, so later DDL there needn't wait for node_b's writes.
        await using (NpgsqlConnection migrating = await nodeA.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await migrating.BeginTransactionAsync(token);
            await using (NpgsqlCommand add = new("ALTER TABLE t ADD COLUMN c integer", migrating, transaction))
            {
                _ = await add.ExecuteNonQueryAsync(token);
            }

            PostgresException held = await Assert.ThrowsExactlyAsync<PostgresException>(
                () => nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (3, 3)", token));
            Assert.AreEqual("55P03", held.SqlState);
            await using (NpgsqlCommand change = new("SET worf.ddl_lock_timeout = '10s'; ALTER TABLE t ALTER COLUMN v TYPE numeric", migrating, transaction))
            {
                _ = await change.ExecuteNonQueryAsync(token);
            }

            await transaction.CommitAsync(token);
        }

        // A savepoint rolled back lets its fences go, and one released keeps them.
        _ = await nodeA.SafePsqlAsync(Database, """
            BEGIN;
            SAVEPOINT s;
            ALTER TABLE t ALTER COLUMN v TYPE text;
            ROLLBACK TO SAVEPOINT s;
            COMMIT;
            """, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT count(*) = 0 FROM pg_stat_activity WHERE application_name LIKE 'worf_ddl_lock%'", token), "the fence's session ended");
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (4, 4)", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            BEGIN;
            SAVEPOINT s;
            ALTER TABLE t ALTER COLUMN c TYPE bigint;
            RELEASE SAVEPOINT s;
            COMMIT;
            """, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("numeric|bigint", await nodeB.SafePsqlAsync(Database, """
            SELECT string_agg(format_type(atttypid, atttypmod), '|' ORDER BY attnum) FROM pg_attribute
            WHERE attrelid = 't'::regclass AND attname IN ('v', 'c')
            """, token));

        PostgresException concurrently = await Assert.ThrowsExactlyAsync<PostgresException>(
            () => nodeA.SafePsqlAsync(Database, "CREATE UNIQUE INDEX CONCURRENTLY t_v ON t (v)", token));
        Assert.AreEqual("55P03", concurrently.SqlState);

        // A kept fence holds back node_b's DDL as well as its writes, but not EXPLAIN, and lasts across a restart.
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await nodeB.SafePsqlAsync(Database, """
            SELECT worf.fence_writes(44, (SELECT node_id FROM worf.node WHERE node_name = 'node_a'), ARRAY['public.t'], 1000);
            SELECT worf.keep_fence(44, (SELECT node_id FROM worf.node WHERE node_name = 'node_a'), 0);
            """, token);
        PostgresException ddlHeld = await Assert.ThrowsExactlyAsync<PostgresException>(
            () => nodeB.SafePsqlAsync(Database, "ALTER TABLE t ALTER COLUMN v TYPE text", token));
        Assert.AreEqual("55P03", ddlHeld.SqlState);
        _ = await nodeB.SafePsqlAsync(Database, "EXPLAIN INSERT INTO t VALUES (5, 5)", token);
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        Assert.IsFalse(await nodeB.PsqlAsync(Database, "INSERT INTO t VALUES (5, 5)", token), "a write through a kept fence after a restart");
        Assert.AreEqual("1", await nodeB.SafePsqlAsync(Database, "SELECT worf.release_fence(44)", token));
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (5, 5)", token);
    }

    /// <summary>
    /// A node that doesn't receive the DDL holds nothing back and isn't waited for, and the DDL's message carries the
    /// position of each node that does.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task OnlyNodesTheDdlReachesAreLocked()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id integer PRIMARY KEY, v integer);
                SELECT * FROM worf.replication_set_add_table('default', 't');
                """, token);
        }

        // node_a and node_b replicate both ways; node_c only sends to node_a.
        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeA, "a_from_c", nodeC, token);

        _ = await nodeA.SafePsqlAsync(Database, "ALTER TABLE t ALTER COLUMN v TYPE bigint", token);
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO t VALUES (1, 1)", token);
        Assert.AreEqual("1", await nodeA.SafePsqlAsync(Database, """
            SELECT count(*) FROM worf.queue, json_object_keys(message->'positions')
            WHERE message_type = 'D' AND message->>'command' LIKE 'ALTER TABLE t ALTER COLUMN v%'
            """, token), "a position for node_b alone");
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (2, 20000000000)", token);
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO t VALUES (3, 3)", token);
    }

    /// <summary>
    /// Polls until a statement fails, as when another node's fence arrives.
    /// </summary>
    private static async Task<bool> PollFailsAsync(TestNode node, string sql, CancellationToken token)
    {
        for (int i = 0; i < 300; i++)
        {
            if (!await node.PsqlAsync(Database, sql, token))
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }

        return false;
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

    private static async Task SubscribeAsync(TestNode subscriber, string name, TestNode provider, CancellationToken token,
        string applyDelay = "0")
    {
        _ = await subscriber.SafePsqlAsync(Database, $$"""
            SELECT worf.create_subscription(
                subscription_name := '{{name}}',
                provider_dsn := '{{provider.Dsn(Database, SuperUser)}}',
                synchronize_structure := false,
                synchronize_data := false,
                forward_origins := '{}',
                apply_delay := '{{applyDelay}}')
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = '{name}' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");
    }
}
