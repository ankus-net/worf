using System.Globalization;
using Npgsql;

namespace Worf.Tests;

/// <summary>
/// Changes a provider relays from other nodes keep the node that wrote them, and the subscription that applied them.
/// </summary>
[TestClass]
public sealed class RelayOriginTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Around a ring of three nodes that forward every origin, each node's changes reach the others through the next
    /// node, which applies them under a relay origin that the following node's walsender can name. A relayed
    /// transaction that fails leaves no progress for the next origin created, and under last_update_wins a node waits
    /// for its clock to pass a change from a clock ahead, as its own changes reach the node that wrote it.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RelayedChangesKeepTheirWriterAroundARing()
    {
        CancellationToken token = TestContext.CancellationToken;

        TestNode[] nodes = await Task.WhenAll(
            StartNodeAsync("node_a", token, "worf.conflict_resolution = last_update_wins", "worf.max_clock_skew = '10s'"),
            StartNodeAsync("node_b", token, "worf.conflict_resolution = last_update_wins", "worf.max_clock_skew = '10s'"),
            StartNodeAsync("node_c", token, "worf.conflict_resolution = last_update_wins", "worf.max_clock_skew = '10s'"));
        await using TestNode nodeA = nodes[0];
        await using TestNode nodeB = nodes[1];
        await using TestNode nodeC = nodes[2];

        // Only node_c rejects the bad row.
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, $"""
                CREATE TABLE ring (id integer PRIMARY KEY, v text{(node == nodeC ? " CONSTRAINT not_bad CHECK (v <> 'bad')" : "")});
                SELECT * FROM worf.replication_set_add_table('default', 'ring');
                """, token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, "{all}", token);
        await SubscribeAsync(nodeC, "c_from_b", nodeB, "{all}", token);

        // node_c's first change from node_a fails, and its relay origin with it until the constraint goes. An origin
        // created meanwhile starts with no progress.
        int logpos = nodeC.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO ring VALUES (1, 'bad')", token);
        Assert.IsTrue(await nodeC.PollLogAsync(logpos, "violates check constraint", token), "the relayed change failing");
        _ = await nodeC.SafePsqlAsync(Database, "SELECT pg_replication_origin_create('probe')", token);
        Assert.AreEqual("", await nodeC.SafePsqlAsync(Database, "SELECT pg_replication_origin_progress('probe', false)", token),
            "a new origin's progress");
        _ = await nodeC.SafePsqlAsync(Database, "SELECT pg_replication_origin_drop('probe'); ALTER TABLE ring DROP CONSTRAINT not_bad", token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM ring WHERE id = 1)", token), "node_a's row on node_c");

        // Once the ring closes, node_b and node_a apply changes that come round it under new relay origins, which the
        // walsenders sending those transactions on can name.
        await SubscribeAsync(nodeA, "a_from_c", nodeC, "{all}", token);
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO ring VALUES (2, 'c')", token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO ring VALUES (3, 'b')", token);
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT count(*) = 3 FROM ring", token), $"every row on {node.Name}");
        }

        Assert.AreEqual("2", await nodeA.SafePsqlAsync(Database, """
            SELECT count(DISTINCT w.writer_node) FROM ring r, worf.origin_writer w
            WHERE r.id IN (2, 3) AND w.roident = (pg_xact_commit_timestamp_origin(r.xmin)).roident
            """, token), "node_a's rows from node_c and node_b, under an origin for each");

        // node_b's provider doesn't subscribe to it, but node_b's changes reach node_a through node_c, so node_b waits
        // for its clock to pass node_a's change from two seconds ahead, and its later change wins everywhere.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_replication_origin_create('skew')", token);
        string ahead = await UpdateAheadAsync(nodeA, "ahead", "2 seconds", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT v = 'ahead' FROM ring WHERE id = 1", token), "the change from ahead");
        _ = await nodeB.SafePsqlAsync(Database, "UPDATE ring SET v = 'local' WHERE id = 1", token);
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database,
            $"SELECT pg_xact_commit_timestamp(xmin) > '{ahead}' FROM ring WHERE id = 1", token), "the local change's commit time");
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT v = 'local' FROM ring WHERE id = 1", token), $"the local change on {node.Name}");
        }

        foreach (TestNode node in nodes)
        {
            Assert.DoesNotContain("no data left in message", node.ReadLog(), node.Name);
        }
    }

    /// <summary>
    /// A row keeps the subscription that applied it, though PostgreSQL reuses its origin's id: a table sync's rows are
    /// the subscription's, and another subscription's aren't, under keep_local. Relay origins give their slots up to
    /// those that need them rather than applying a node's changes as another's. A failed table sync restarts after the
    /// delay, and the two-way warning compares the settings the workers run with.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task OriginsKeepTheirSubscriptionAndFreeTheirSlots()
    {
        CancellationToken token = TestContext.CancellationToken;

        TestNode[] nodes = await Task.WhenAll(
            StartNodeAsync("provider", token),
            StartNodeAsync("subscriber", token, "worf.conflict_resolution = keep_local"));
        await using TestNode provider = nodes[0];
        await using TestNode subscriber = nodes[1];

        // Three replication origins can be tracked on the subscriber.
        string limit = int.Parse(await subscriber.SafePsqlAsync(Database, "SHOW server_version_num", token), CultureInfo.InvariantCulture) >= 180000
            ? "max_active_replication_origins" : "max_replication_slots";
        _ = await subscriber.SafePsqlAsync(Database, $"ALTER SYSTEM SET {limit} = 3", token);
        await subscriber.StopAsync("fast", token);
        await subscriber.StartServerAsync(token);

        // A slot name leaves room for a table sync's suffix in 63 bytes.
        Assert.AreEqual("54", await subscriber.SafePsqlAsync(Database, """
            SELECT max(length(worf.worf_gen_slot_name(repeat('d', d), repeat('n', n), repeat('s', s))))
            FROM generate_series(1, 20) d, generate_series(1, 20) n, generate_series(1, 20) s
            """, token), "the longest slot name");

        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, $"""
                CREATE TABLE c (id integer PRIMARY KEY, v text);
                CREATE TABLE e (id integer PRIMARY KEY, v text);
                CREATE TABLE d (id integer PRIMARY KEY, v text);
                CREATE TABLE d2 (id integer PRIMARY KEY);
                """, token);
        }

        _ = await provider.SafePsqlAsync(Database, """
            SELECT worf.create_replication_set('cset');
            SELECT * FROM worf.replication_set_add_table('cset', 'c');
            SELECT * FROM worf.replication_set_add_table('default', 'd');
            CREATE TABLE d3 (id integer PRIMARY KEY);
            INSERT INTO e VALUES (1, 'copied');
            INSERT INTO d2 VALUES (1);
            INSERT INTO d3 VALUES (1);
            """, token);
        await SubscribeAsync(subscriber, "sub", provider, "{all}", token);
        await SubscribeAsync(subscriber, "temp", provider, "{all}", token, "cset");

        // The workers on both nodes run with their configuration files' settings, keep_local and apply_remote, rather
        // than the last_update_wins that the connections creating the subscription set.
        int logpos = provider.ReadLog().Length;
        await using (NpgsqlConnection connection = await OpenWithLastUpdateWinsAsync(provider, token))
        await using (NpgsqlCommand command = new($"""
            SELECT worf.create_subscription(subscription_name := 'back',
                provider_dsn := '{subscriber.Dsn(Database, SuperUser)} options=''-cworf.conflict_resolution=last_update_wins''',
                synchronize_structure := false, synchronize_data := false, forward_origins := '{"{}"}')
            """, connection))
        {
            _ = await command.ExecuteNonQueryAsync(token);
        }

        Assert.Contains("replicate both ways", provider.ReadLog()[logpos..], "the two-way warning");
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM pg_stat_replication WHERE application_name = 'back')", token), "the subscription back");

        // The provider commits as other nodes under origins named like relay origins of its subscription back.
        string back = await provider.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription WHERE sub_name = 'back'", token);
        foreach (int writer in new[] { 9001, 9002, 9003, 9004, 9005 })
        {
            _ = await provider.SafePsqlAsync(Database, $"SELECT pg_replication_origin_create('{back}@{writer}')", token);
        }

        // A row that another subscription applied under the id that a relay origin of this subscription takes after
        // it's dropped is in conflict with this subscription's change, and stays.
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO c VALUES (1, 'old')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM c WHERE id = 1)", token), "the other subscription's row");
        string before = await provider.SafePsqlAsync(Database, "SELECT clock_timestamp()::text", token);
        _ = await subscriber.SafePsqlAsync(Database, """
            SELECT worf.drop_subscription('temp');
            SELECT worf.alter_subscription_add_replication_set('sub', 'cset');
            """, token);
        Assert.IsTrue(await provider.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (SELECT 1 FROM pg_stat_replication WHERE application_name = 'sub' AND backend_start > '{before}')
            """, token), "the subscription streaming the other's set");
        await WriteAsAsync(provider, $"{back}@9005", "UPDATE c SET v = 'new' WHERE id = 1; INSERT INTO c VALUES (2, 'after')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM c WHERE id = 2)", token), "the change after the update");
        Assert.AreEqual("old", await subscriber.SafePsqlAsync(Database, "SELECT v FROM c WHERE id = 1", token), "the other subscription's row");

        // A table sync's rows are the subscription's, and its later changes apply.
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'e', synchronize_data := true)", token);
        await WaitForSyncAsync(subscriber, "e", token);
        _ = await provider.SafePsqlAsync(Database, "UPDATE e SET v = 'updated' WHERE id = 1", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT v = 'updated' FROM e WHERE id = 1", token), "the synchronized row's update");

        // An origin that a rolled back transaction used leaves its progress in shared memory, which the relay origin
        // created next with its id doesn't start from.
        _ = await subscriber.SafePsqlAsync(Database, """
            BEGIN;
            SELECT pg_replication_origin_create('leftover');
            SELECT pg_replication_origin_session_setup('leftover');
            SELECT pg_replication_origin_xact_setup('FF/0', now());
            INSERT INTO d VALUES (0, 'leftover');
            ROLLBACK;
            """, token);
        await WriteAsAsync(provider, $"{back}@9001", "INSERT INTO d VALUES (1, '9001')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM d WHERE id = 1)", token), "the first writer's row");
        Assert.AreEqual("t", await subscriber.SafePsqlAsync(Database,
            "SELECT pg_replication_origin_progress(roname, false) < 'FF/0' FROM pg_replication_origin WHERE roname LIKE '%@9001'", token),
            "the relay origin's progress");

        // Four writers take turns with two slots for relay origins, and each row keeps its writer.
        for (int id = 2; id <= 8; id++)
        {
            int writer = 9000 + ((id - 1) % 4) + 1;
            await WriteAsAsync(provider, $"{back}@{writer}", $"INSERT INTO d VALUES ({id}, '{writer}')", token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT count(*) = 8 FROM d", token), "the writers' rows");
        Assert.AreEqual("", await subscriber.SafePsqlAsync(Database, """
            SELECT string_agg(d.id || ': ' || d.v || ' as ' || coalesce(w.writer_node::text, 'none'), ', ' ORDER BY d.id) FROM d
                LEFT JOIN LATERAL (SELECT writer_node FROM worf.origin_writer
                    WHERE roident = (pg_xact_commit_timestamp_origin(d.xmin)).roident AND since_xid::text::bigint <= d.xmin::text::bigint
                    ORDER BY since_xid DESC LIMIT 1) w ON true
            WHERE w.writer_node IS DISTINCT FROM d.v::oid
            """, token), "rows applied as another node's");

        // They give their slots up to a table sync.
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'd2', synchronize_data := true)", token);
        await WaitForSyncAsync(subscriber, "d2", token);

        // A sync that fails, here for want of its table, restarts after the delay an apply worker does.
        logpos = subscriber.ReadLog().Length;
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'd3', synchronize_data := true)", token);
        Assert.IsTrue(await subscriber.PollLogAsync(logpos, "starting sync of table public.d3", token), "the sync");
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        int starts = subscriber.ReadLog()[logpos..].Split("starting sync of table public.d3").Length - 1;
        Assert.IsLessThanOrEqualTo(OperatingSystem.IsWindows() ? 5 : 2, starts, "sync worker starts in three seconds");
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE d3 (id integer PRIMARY KEY)", token);
        await WaitForSyncAsync(subscriber, "d3", token);
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token, params string[] settings)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
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

    private static async Task SubscribeAsync(TestNode subscriber, string name, TestNode provider, string forwardOrigins,
        CancellationToken token, string replicationSets = "default,default_insert_only,ddl_sql")
    {
        _ = await subscriber.SafePsqlAsync(Database, $$"""
            SELECT worf.create_subscription(
                subscription_name := '{{name}}',
                provider_dsn := '{{provider.Dsn(Database, SuperUser)}}',
                replication_sets := '{{{replicationSets}}}',
                synchronize_structure := false,
                synchronize_data := false,
                forward_origins := '{{forwardOrigins}}')
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = '{name}' AND status = 'replicating')
            """, token), $"subscription {name} failed to reach \"replicating\" state");
        Assert.IsTrue(await provider.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (SELECT 1 FROM pg_stat_replication WHERE application_name = '{name}')
            """, token), $"subscription {name}'s walsender");
    }

    /// <summary>
    /// Opens a connection whose session sets last_update_wins, which the node's workers don't run with.
    /// </summary>
    private static async Task<NpgsqlConnection> OpenWithLastUpdateWinsAsync(TestNode node, CancellationToken token)
    {
        NpgsqlConnectionStringBuilder builder = new()
        {
            Host = "127.0.0.1",
            Port = node.Cluster.Port,
            Database = Database,
            Username = SuperUser,
            Options = "-c worf.conflict_resolution=last_update_wins",
            Pooling = false,
            Timeout = 60,
        };
        NpgsqlConnection connection = new(builder.ConnectionString);
        await connection.OpenAsync(token);
        return connection;
    }

    /// <summary>
    /// Commits a transaction under a replication origin, as apply does for the node it names.
    /// </summary>
    private static Task<string> WriteAsAsync(TestNode node, string origin, string sql, CancellationToken token)
        => node.SafePsqlAsync(Database, $"SELECT pg_replication_origin_session_setup('{origin}'); {sql}", token);

    private static async Task WaitForSyncAsync(TestNode subscriber, string table, CancellationToken token)
        => Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            $"SELECT EXISTS (SELECT 1 FROM worf.local_sync_status WHERE sync_relname = '{table}' AND sync_status = 'r')", token),
            $"the sync of {table}");

    /// <summary>
    /// Updates the row with a commit timestamp ahead of the clock, and returns it.
    /// </summary>
    private static async Task<string> UpdateAheadAsync(TestNode node, string value, string interval, CancellationToken token)
    {
        _ = await node.SafePsqlAsync(Database, $"""
            SELECT pg_replication_origin_session_setup('skew');
            SELECT pg_replication_origin_xact_setup('0/1', clock_timestamp() + interval '{interval}');
            UPDATE ring SET v = '{value}' WHERE id = 1;
            """, token);
        return await node.SafePsqlAsync(Database, "SELECT pg_xact_commit_timestamp(xmin)::text FROM ring WHERE id = 1", token);
    }
}
