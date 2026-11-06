using System.Diagnostics;
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
    /// the subscription's, and another subscription's or another origin's aren't, under keep_local. Relay origins give
    /// their slots up to those that need them, PostgreSQL subscriptions whose workers can start too, and only then,
    /// rather than applying a node's changes as another's, and worf forgets what it recorded of them once it's
    /// superseded for vacuum_freeze_min_age transactions. A frozen row has no commit origin. A failed table sync
    /// restarts after the delay, and the two-way warning compares the settings the workers run with, from the command
    /// line too.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task OriginsKeepTheirSubscriptionAndFreeTheirSlots()
    {
        CancellationToken token = TestContext.CancellationToken;

        TestNode[] nodes = await Task.WhenAll(
            StartNodeAsync("provider", token),
            StartNodeAsync("subscriber", token));
        await using TestNode provider = nodes[0];
        await using TestNode subscriber = nodes[1];

        // Three replication origins can be tracked on the subscriber, which resolves conflicts as its command line says.
        string limit = int.Parse(await subscriber.SafePsqlAsync(Database, "SHOW server_version_num", token), CultureInfo.InvariantCulture) >= 180000
            ? "max_active_replication_origins" : "max_replication_slots";
        _ = await subscriber.SafePsqlAsync(Database, $"ALTER SYSTEM SET {limit} = 3", token);
        _ = await subscriber.SafePsqlAsync(Database, "ALTER SYSTEM SET autovacuum = off", token);
        await subscriber.StopAsync("fast", token);
        await subscriber.StartServerAsync(token, "-c worf.conflict_resolution=keep_local");

        // A slot name leaves room for a table sync's suffix in 63 bytes.
        Assert.AreEqual("54", await subscriber.SafePsqlAsync(Database, """
            SELECT max(length(worf.worf_gen_slot_name(repeat('d', d), repeat('n', n), repeat('s', s))))
            FROM generate_series(1, 20) d, generate_series(1, 20) n, generate_series(1, 20) s
            """, token), "the longest slot name");

        // No subscription's slot name ends as a table sync's does, so none is taken for another subscription's.
        Assert.AreEqual("worf_postgres_prov_sub_a3f85cc_", await subscriber.SafePsqlAsync(Database,
            "SELECT worf.worf_gen_slot_name('postgres', 'prov', 'sub_a3f85ccf')", token));

        const string CreateE = "CREATE SEQUENCE e_n_seq; CREATE TABLE e (id integer PRIMARY KEY, v text, n bigint DEFAULT nextval('e_n_seq'))";
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, $"""
                CREATE TABLE c (id integer PRIMARY KEY, v text);
                CREATE TABLE d (id integer PRIMARY KEY, v text);
                CREATE TABLE d2 (id integer PRIMARY KEY);
                """, token);
        }

        _ = await provider.SafePsqlAsync(Database, CreateE, token);
        _ = await provider.SafePsqlAsync(Database, """
            SELECT worf.create_replication_set('cset');
            SELECT worf.create_replication_set('tset');
            SELECT * FROM worf.replication_set_add_table('cset', 'c');
            SELECT * FROM worf.replication_set_add_table('default', 'd');
            CREATE TABLE d3 (id integer PRIMARY KEY);
            SELECT worf.enable_sequence_range('e_n_seq', participants := '{}', range_size := 100);
            INSERT INTO e VALUES (1, 'copied');
            INSERT INTO d2 VALUES (1);
            INSERT INTO d3 VALUES (1);
            """, token);
        await SubscribeAsync(subscriber, "sub", provider, "{all}", token);
        await SubscribeAsync(subscriber, "temp", provider, "{all}", token, "cset");

        // The workers on both nodes run with their configuration files' settings, keep_local and apply_remote, rather
        // than the last_update_wins of the role that creates the subscription and connects back.
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, $"ALTER ROLE {SuperUser} SET worf.conflict_resolution = last_update_wins", token);
        }

        int logpos = provider.ReadLog().Length;
        await using (NpgsqlConnection connection = await OpenAsSuperUserAsync(provider, token))
        await using (NpgsqlCommand command = new($"""
            SELECT worf.create_subscription(subscription_name := 'back',
                provider_dsn := '{subscriber.Dsn(Database, SuperUser)}',
                synchronize_structure := false, synchronize_data := false, forward_origins := '{"{}"}')
            """, connection))
        {
            _ = await command.ExecuteNonQueryAsync(token);
        }

        Assert.Contains("replicate both ways", provider.ReadLog()[logpos..], "the two-way warning");
        Assert.Contains("is apply_remote on node \"provider\" and keep_local on node \"subscriber\"", provider.ReadLog()[logpos..],
            "the two-way warning's settings");
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM pg_stat_replication WHERE application_name = 'back')", token), "the subscription back");

        // The provider commits as other nodes under origins named like relay origins of its subscription back.
        string back = await provider.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription WHERE sub_name = 'back'", token);
        foreach (int writer in new[] { 9001, 9002, 9003, 9004, 9005 })
        {
            _ = await provider.SafePsqlAsync(Database, $"SELECT pg_replication_origin_create('{back}@{writer}')", token);
        }

        // A row that another subscription applied under an id is that subscription's after the id is reused.
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO c VALUES (1, 'old')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM c WHERE id = 1)", token), "the other subscription's row");

        // A table sync's rows are the subscription's, and its later changes apply, though the sync joins a managed
        // sequence before it creates its origin, and that waits for the other subscription's drop, whose origin's id
        // it takes. The subscriber's sequence is new, and the subscription back, disabled, makes no exchange that would
        // join it first.
        _ = await provider.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('back', true)", token);
        _ = await subscriber.SafePsqlAsync(Database, CreateE, token);
        await using (NpgsqlConnection dropper = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction dropping = await dropper.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE pg_replication_origin IN EXCLUSIVE MODE", dropper, dropping))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'e', synchronize_data := true)", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
                SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                    WHERE l.relation = 'pg_replication_origin'::regclass AND NOT l.granted AND a.backend_type LIKE 'worf sync%')
                """, token), "the sync waiting to create its origin");
            await using (NpgsqlCommand command = new("SELECT worf.drop_subscription('temp')", dropper, dropping))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            await dropping.CommitAsync(token);
        }

        await WaitForSyncAsync(subscriber, "e", token);
        _ = await provider.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('back', true)", token);
        Assert.AreEqual("1", await subscriber.SafePsqlAsync(Database,
            "SELECT count(*) FROM worf.sequence_range WHERE seqoid = 'e_n_seq'::regclass", token), "the joined sequence");
        _ = await provider.SafePsqlAsync(Database, "UPDATE e SET v = 'updated' WHERE id = 1", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT v = 'updated' FROM e WHERE id = 1", token), "the synchronized row's update");

        // An origin worf didn't create takes the id the table sync's had, and a row written under it is in conflict
        // with the subscription's change.
        _ = await subscriber.SafePsqlAsync(Database, "SELECT pg_replication_origin_create('native')", token);
        Assert.AreEqual("t", await subscriber.SafePsqlAsync(Database, """
            SELECT EXISTS (SELECT 1 FROM worf.origin_writer w JOIN pg_replication_origin o USING (roident)
                WHERE o.roname = 'native' AND w.writer_node <> 0)
            """, token), "a recorded id for the other origin");
        await WriteAsAsync(subscriber, "native", "UPDATE e SET v = 'native' WHERE id = 1", token);
        _ = await provider.SafePsqlAsync(Database, "UPDATE e SET v = 'remote' WHERE id = 1; INSERT INTO e VALUES (2, 'after')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM e WHERE id = 2)", token), "the change after the update");
        Assert.AreEqual("native", await subscriber.SafePsqlAsync(Database, "SELECT v FROM e WHERE id = 1", token), "the other origin's row");
        _ = await subscriber.SafePsqlAsync(Database, "SELECT pg_replication_origin_drop('native')", token);

        // The provider's walsender can't name an origin that the transaction committed under it created, and sends
        // the transaction as the provider's.
        await WriteAsAsync(provider, "fresh", "INSERT INTO e VALUES (3, 'fresh')", token, create: true);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM e WHERE id = 3)", token),
            "the change under a new origin");

        // The other subscription's row is in conflict with this subscription's change, under the relay origin that
        // takes the id next, and stays.
        string before = await provider.SafePsqlAsync(Database, "SELECT clock_timestamp()::text", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_add_replication_set('sub', 'cset')", token);
        Assert.IsTrue(await provider.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (SELECT 1 FROM pg_stat_replication WHERE application_name = 'sub' AND backend_start > '{before}')
            """, token), "the subscription streaming the other's set");
        await WriteAsAsync(provider, $"{back}@9005", "UPDATE c SET v = 'new' WHERE id = 1; INSERT INTO c VALUES (2, 'after')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM c WHERE id = 2)", token), "the change after the update");
        Assert.AreEqual("old", await subscriber.SafePsqlAsync(Database, "SELECT v FROM c WHERE id = 1", token), "the other subscription's row");
        Assert.AreEqual("t", await subscriber.SafePsqlAsync(Database, """
            SELECT (pg_xact_commit_timestamp_origin(c.xmin)).roident = o.roident FROM c, pg_replication_origin o
            WHERE c.id = 1 AND o.roname LIKE '%@9005'
            """, token), "the relay origin with the other subscription's id");

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

        // Writers take turns with two slots for relay origins, here those of 9001 and 9005, and each row keeps its
        // writer. A relay origin that set up the session within the last minute keeps its slot, so a third writer's
        // transaction stops apply with 53400, as PostgreSQL's apply worker stops when no slot is free, rather than drop
        // and create an origin for each transaction; once the worker starts again, an origin that hasn't applied since
        // gives its slot up before the next is created, which takes its id, so a turn records nothing of the drop.
        await WriteAsAsync(provider, $"{back}@9005", "INSERT INTO c VALUES (3, '9005')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM c WHERE id = 3)", token), "the fifth writer's row");
        string turns = await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())", token);
        logpos = subscriber.ReadLog().Length;
        for (int id = 2; id <= 4; id++)
        {
            int writer = 9000 + id;
            await WriteAsAsync(provider, $"{back}@{writer}", $"INSERT INTO d VALUES ({id}, '{writer}')", token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT count(*) = 4 FROM d", token), "the writers' rows");
        string turnLog = subscriber.ReadLog()[logpos..];
        string[] waitLines = [.. turnLog.Split('\n').Where(line => line.Contains("no replication origin slot is free for replication origin", StringComparison.Ordinal))];
        IEnumerable<int> waits = Enumerable.Range(9002, 3).Where(writer => waitLines.Any(line => line.Contains($"@{writer}\"", StringComparison.Ordinal)));
        Assert.AreEqual("9002,9004", string.Join(",", waits), "the writers that waited, but not 9003, which took the slot of 9005, unused since the worker started again");
        Assert.Contains("which it keeps for a minute after its changes last applied", turnLog, "the reason the slots were kept");
        Assert.AreEqual("t", await subscriber.SafePsqlAsync(Database,
            "SELECT max(n) > 1 FROM (SELECT count(*) n FROM worf.origin_writer GROUP BY roident) c", token), "an id recorded more than once");
        Assert.AreEqual("3|0", await subscriber.SafePsqlAsync(Database, $"""
            SELECT count(*) FILTER (WHERE writer_node <> 0), count(*) FILTER (WHERE writer_node = 0)
            FROM worf.origin_writer WHERE since_xid >= '{turns}'
            """, token), "relay origins and drops recorded in the turns");
        const string AppliedAsAnother = """
            SELECT string_agg(d.id || ': ' || d.v || ' as ' || coalesce(w.writer_node::text, 'none'), ', ' ORDER BY d.id) FROM d
                LEFT JOIN LATERAL (SELECT writer_node FROM worf.origin_writer
                    WHERE roident = (pg_xact_commit_timestamp_origin(d.xmin)).roident AND since_xid::text::bigint <= d.xmin::text::bigint
                    ORDER BY since_xid DESC LIMIT 1) w ON true
            WHERE d.v ~ '^9' AND pg_xact_commit_timestamp(d.xmin) IS NOT NULL AND w.writer_node IS DISTINCT FROM d.v::oid
                AND age(d.xmin) < age((SELECT relfrozenxid FROM pg_class WHERE oid = 'd'::regclass))
            """;
        Assert.AreEqual("", await subscriber.SafePsqlAsync(Database, AppliedAsAnother, token), "rows applied as another node's");

        // They keep their slots while nothing wants one, as when the apply worker restarts with every slot taken, though
        // an enabled PostgreSQL subscription whose worker never starts, as its owner can't log in, and an origin a user
        // created have none, and give one up to another subscription that finds none free.
        const string Relays = "SELECT count(*) || ': ' || string_agg(roname, ',' ORDER BY roname) FROM pg_replication_origin WHERE roname LIKE '%@90__'";
        string relays = await subscriber.SafePsqlAsync(Database, Relays, token);
        Assert.StartsWith("2: ", relays, "the relay origins");
        _ = await subscriber.SafePsqlAsync(Database, $"""
            CREATE ROLE ghost SUPERUSER NOLOGIN;
            CREATE SUBSCRIPTION pgsub CONNECTION '{provider.Dsn(Database, SuperUser)}' PUBLICATION p WITH (connect = false);
            ALTER SUBSCRIPTION pgsub OWNER TO ghost;
            ALTER SUBSCRIPTION pgsub ENABLE;
            SELECT pg_replication_origin_create('other');
            """, token);
        string records = await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM worf.origin_writer", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('sub', true)", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('sub', true)", token);
        await WriteAsAsync(provider, $"{back}@9004", "INSERT INTO d VALUES (9, '9004')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM d WHERE id = 9)", token), "the row after the restart");
        await Task.Delay(TimeSpan.FromMilliseconds(500), token);
        Assert.AreEqual(relays, await subscriber.SafePsqlAsync(Database, Relays, token), "the relay origins after the restart");
        Assert.AreEqual(records, await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM worf.origin_writer", token),
            "origin records after the restart");
        logpos = subscriber.ReadLog().Length;
        await SubscribeAsync(subscriber, "temp2", provider, "{all}", token, "tset");
        Assert.AreEqual("t", await subscriber.SafePsqlAsync(Database,
            "SELECT count(*) < 2 FROM pg_replication_origin WHERE roname LIKE '%@90__'", token), "the relay origins left");
        Assert.DoesNotContain("could not find free replication state slot", subscriber.ReadLog()[logpos..], "errors for want of a slot");
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.drop_subscription('temp2')", token);

        // A record superseded vacuum_freeze_min_age transactions ago, here none, is forgotten as ids are recorded again,
        // though nothing vacuums.
        _ = await subscriber.SafePsqlAsync(Database, "ALTER SYSTEM SET vacuum_freeze_min_age = 0", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        string horizon = await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())", token);
        string recorded = await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM worf.origin_writer", token);
        for (int id = 10; id <= 13; id++)
        {
            int writer = id % 2 == 0 ? 9002 : 9004;
            await WriteAsAsync(provider, $"{back}@{writer}", $"INSERT INTO d VALUES ({id}, '{writer}')", token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT count(*) = 9 FROM d", token), "the rows after the horizon");
        Assert.AreEqual("0", await subscriber.SafePsqlAsync(Database, $"""
            SELECT count(*) FROM (
                SELECT roident FROM worf.origin_writer
                GROUP BY roident
                HAVING count(*) FILTER (WHERE since_xid::text::bigint < {horizon}) > 1) r
            """, token), "ids that keep superseded records from before the horizon");
        Assert.IsLessThan(int.Parse(recorded, CultureInfo.InvariantCulture), int.Parse(await subscriber.SafePsqlAsync(Database,
            $"SELECT count(*) FROM worf.origin_writer WHERE since_xid::text::bigint < {horizon}", token), CultureInfo.InvariantCulture),
            "records from before the horizon");

        // A row whose relay origin's id has been reused since, and whose record is forgotten, is the subscription's that
        // holds the id now, as pglogical takes it, so the provider's change to it applies under keep_local.
        _ = await provider.SafePsqlAsync(Database, "UPDATE d SET v = 'updated' WHERE id = 2", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT v = 'updated' FROM d WHERE id = 2", token),
            "the update of a row whose record is forgotten");

        // A row that COPY FREEZE froze has no commit timestamp or origin, as in pglogical, though its transaction isn't
        // older than its table's relfrozenxid, so the provider's change to it applies under keep_local too.
        _ = await provider.SafePsqlAsync(Database, "CREATE TABLE f (id integer PRIMARY KEY, v text); INSERT INTO f VALUES (1, 'provider')", token);
        await using (NpgsqlConnection copier = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction copying = await copier.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("CREATE TABLE f (id integer PRIMARY KEY, v text)", copier, copying))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            await using (TextWriter rows = await copier.BeginTextImportAsync("COPY f FROM STDIN (FREEZE)", token))
            {
                await rows.WriteAsync("1\tlocal\n".AsMemory(), token);
            }

            await copying.CommitAsync(token);
        }

        Assert.AreEqual("t", await subscriber.SafePsqlAsync(Database,
            "SELECT age(f.xmin) <= age(c.relfrozenxid) FROM f, pg_class c WHERE c.oid = 'f'::regclass", token), "the frozen row's transaction");
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'f')", token);
        _ = await provider.SafePsqlAsync(Database, "UPDATE f SET v = 'updated' WHERE id = 1", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT v = 'updated' FROM f WHERE id = 1", token),
            "the update of a frozen row");

        // They give their slots up to a table sync.
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'd2', synchronize_data := true)", token);
        await WaitForSyncAsync(subscriber, "d2", token);

        // A sync that fails, here for want of its table, restarts after the delay an apply worker does, by the server's
        // clock.
        logpos = subscriber.ReadLog().Length;
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'd3', synchronize_data := true)", token);
        Assert.IsTrue(await subscriber.PollLogAsync(logpos, "starting sync of table public.d3", token), "the sync");
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        List<DateTime> starts = subscriber.LogTimes(logpos, "starting sync of table public.d3");
        Assert.IsLessThanOrEqualTo(2, starts.Count(at => at <= starts[0].AddSeconds(3)), "sync worker starts in three seconds");
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE d3 (id integer PRIMARY KEY)", token);
        await WaitForSyncAsync(subscriber, "d3", token);

        // They give one up to the PostgreSQL subscription once its owner can log in, so its worker can start, though its
        // workers don't say they want one, and not to the origin the user created.
        await WriteAsAsync(provider, $"{back}@9004", "INSERT INTO d VALUES (14, '9004')", token);
        await WriteAsAsync(provider, $"{back}@9002", "INSERT INTO d VALUES (15, '9002')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT count(*) = 11 FROM d", token), "the rows before another user");
        Assert.StartsWith("2: ", await subscriber.SafePsqlAsync(Database, Relays, token), "the relay origins before another user");
        _ = await subscriber.SafePsqlAsync(Database, "ALTER ROLE ghost LOGIN", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
            SELECT EXISTS (SELECT 1 FROM pg_replication_origin_status s JOIN pg_subscription p ON s.external_id = 'pg_' || p.oid)
            """, token), "the PostgreSQL subscription's origin set up");
        Assert.AreEqual("1|f", await subscriber.SafePsqlAsync(Database, """
            SELECT count(*), EXISTS (SELECT 1 FROM pg_replication_origin_status WHERE external_id = 'other')
            FROM pg_replication_origin WHERE roname LIKE '%@90__'
            """, token), "the relay origins left, and the user's origin");

        // With every slot taken by origins that aren't relay origins, here another subscription's, a forwarded node's
        // transactions wait for one, rather than committing under the subscription's own origin, which would send their
        // rows back to the node that wrote them as the provider's, and record nothing meanwhile, neither the origin they
        // would create for a node writing here for the first time nor a claim on it for each. The relay origin left gives its slot up once the
        // provider's own row, applied under the subscription's origin, moves the worker's restart past it. The node's
        // transaction comes once relay origins no longer leave slots given up for PostgreSQL subscriptions to them, ten
        // seconds after the last that wants one, here the one disabled, which keeps its slot, so that it looks for a slot
        // of its own.
        _ = await subscriber.SafePsqlAsync(Database, "ALTER SUBSCRIPTION pgsub DISABLE", token);
        Stopwatch sinceWanted = Stopwatch.StartNew();
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO d VALUES (16, 'provider')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM d WHERE id = 16)", token), "the provider's row");
        await SubscribeAsync(subscriber, "temp3", provider, "{all}", token, "tset");
        Assert.AreEqual("0", await subscriber.SafePsqlAsync(Database, """
            SELECT count(*) FROM pg_replication_origin o JOIN pg_replication_origin_status s ON s.local_id = o.roident WHERE o.roname LIKE '%@90__'
            """, token), "relay origins with slots");
        _ = await provider.SafePsqlAsync(Database, $"SELECT pg_replication_origin_create('{back}@9006')", token);
        records = await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM worf.origin_writer", token);
        if (TimeSpan.FromSeconds(11) - sinceWanted.Elapsed is TimeSpan left && left > TimeSpan.Zero)
        {
            await Task.Delay(left, token);
        }

        // Two transactions under the node's origin, on one session, as a session that ended may still hold the origin.
        await using (NpgsqlConnection writing = await provider.OpenAsync(Database, token))
        {
            foreach (string sql in new[] { $"SELECT pg_replication_origin_session_setup('{back}@9006')", "INSERT INTO d VALUES (17, '9006')", "INSERT INTO d VALUES (18, '9006')" })
            {
                await using NpgsqlCommand command = new(sql, writing);
                _ = await command.ExecuteNonQueryAsync(token);
            }
        }

        await Task.Delay(TimeSpan.FromSeconds(2), token);
        Assert.AreEqual("0|" + records, await subscriber.SafePsqlAsync(Database,
            "SELECT (SELECT count(*) FROM d WHERE id IN (17, 18)), (SELECT count(*) FROM worf.origin_writer)", token),
            "the rows and records while every slot is taken");
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.drop_subscription('temp3')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT count(*) = 2 FROM d WHERE id IN (17, 18)", token), "the rows once a slot is free");
        Assert.AreEqual("2", await subscriber.SafePsqlAsync(Database, """
            SELECT count(*) FROM d, pg_replication_origin o
            WHERE d.id IN (17, 18) AND o.roident = (pg_xact_commit_timestamp_origin(d.xmin)).roident AND o.roname LIKE '%@9006'
            """, token), "the rows under the node's relay origin");

        // The subscriptions dropped while their sequence range exchanges went on, and those that started, logged no
        // failure for want of their slots, on either node.
        foreach (TestNode node in nodes)
        {
            Assert.DoesNotContain("carries this sequence range exchange", node.ReadLog(), node.Name);
        }
    }

    /// <summary>
    /// Names that differ give slots that differ, even when a slot's characters can't hold them whole, as databases'
    /// names, so that subscriptions of the same name in a cluster's databases keep slots and replication origins of their
    /// own, and dropping one of two subscriptions that share a slot, as made before names were told apart, keeps the
    /// slot and replication origin for the other, which goes on applying once enabled. A drop waits for a process that
    /// still holds the subscription's origin to let it go.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SubscriptionsKeepTheirOwnSlots()
    {
        CancellationToken token = TestContext.CancellationToken;

        TestNode[] nodes = await Task.WhenAll(
            StartNodeAsync("provider", token),
            StartNodeAsync("subscriber", token));
        await using TestNode provider = nodes[0];
        await using TestNode subscriber = nodes[1];
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id integer PRIMARY KEY)", token);
        }

        _ = await provider.SafePsqlAsync(Database, """
            SELECT worf.replication_set_add_table('default', 't');
            SELECT worf.create_replication_set('other');
            """, token);
        await SubscribeAsync(subscriber, "s_1", provider, "{}", token, "default");

        // A subscription's name holds only what a slot's name does, so a name like s_1's but for that is refused.
        PostgresException refused = await Assert.ThrowsExactlyAsync<PostgresException>(() => subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(subscription_name := 's.1', provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                replication_sets := '{"{other}"}', synchronize_structure := false, synchronize_data := false)
            """, token), "a subscription named like s_1");
        Assert.AreEqual("42602", refused.SqlState);

        // s_2 comes to share s_1's slot, as two subscriptions made before names were told apart could, and is dropped
        // while s_1 is disabled.
        await SubscribeAsync(subscriber, "s_2", provider, "{}", token, "other");
        string slot = await subscriber.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription WHERE sub_name = 's_1'", token);
        string ownSlot = await subscriber.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription WHERE sub_name = 's_2'", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('s_1', true)", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('s_2', true)", token);
        _ = await subscriber.SafePsqlAsync(Database, $"UPDATE worf.subscription SET sub_slot_name = '{slot}' WHERE sub_name = 's_2'", token);
        int logpos = subscriber.ReadLog().Length;
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.drop_subscription('s_2')", token);
        Assert.Contains($"subscription \"s_2\" shares slot \"{slot}\" with another subscription", subscriber.ReadLog()[logpos..]);
        Assert.AreEqual("1", await provider.SafePsqlAsync(Database, $"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}'", token),
            "s_1's slot");
        Assert.AreEqual("1", await subscriber.SafePsqlAsync(Database, $"SELECT count(*) FROM pg_replication_origin WHERE roname = '{slot}'", token),
            "s_1's origin");
        _ = await provider.SafePsqlAsync(Database, $"SELECT pg_drop_replication_slot('{ownSlot}')", token);
        _ = await subscriber.SafePsqlAsync(Database, $"SELECT pg_replication_origin_drop('{ownSlot}')", token);

        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (1)", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('s_1', true)", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT FROM t WHERE id = 1)", token), "s_1's row");

        // Subscriptions of the same name in databases whose names a slot's characters both write as a_b get slots and
        // replication origins of their own, so dropping one leaves the other replicating.
        foreach (string dbname in new[] { "a-b", "a_b" })
        {
            _ = await subscriber.SafePsqlAsync(Database, $"CREATE DATABASE \"{dbname}\"", token);
            _ = await subscriber.SafePsqlAsync(dbname, $$"""
                CREATE EXTENSION worf;
                CREATE TABLE t (id integer PRIMARY KEY);
                SELECT * FROM worf.create_node(node_name := 'sub_{{dbname.Replace('-', '_')}}', dsn := '{{subscriber.Dsn(dbname, SuperUser)}}');
                SELECT worf.create_subscription(subscription_name := 's', provider_dsn := '{{provider.Dsn(Database, SuperUser)}}',
                    replication_sets := '{default}', synchronize_structure := false, synchronize_data := false);
                """, token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(dbname,
                "SELECT EXISTS (SELECT FROM worf.show_subscription_status() WHERE status = 'replicating')", token), $"{dbname}'s subscription");
        }

        string[] slots = [await subscriber.SafePsqlAsync("a-b", "SELECT sub_slot_name FROM worf.subscription", token),
            await subscriber.SafePsqlAsync("a_b", "SELECT sub_slot_name FROM worf.subscription", token)];
        Assert.AreNotEqual(slots[0], slots[1], "the two databases' slots");

        // A process that still holds the replication origin of a subscription being dropped, as a worker that has left its
        // slot holds it until its process exits, has the drop wait for it to let the origin go.
        _ = await subscriber.SafePsqlAsync("a_b", "SELECT worf.alter_subscription_disable('s', true)", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync("a_b",
            "SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE backend_type LIKE 'worf apply%' AND datname = 'a_b')", token), "a_b's apply worker gone");
        await using (NpgsqlConnection holding = await subscriber.OpenAsync("a_b", token))
        {
            await using (NpgsqlCommand hold = new($"SELECT pg_replication_origin_session_setup('{slots[1]}')", holding))
            {
                _ = await hold.ExecuteNonQueryAsync(token);
            }

            Task<string> dropping = subscriber.SafePsqlAsync("a_b", "SELECT worf.drop_subscription('s')", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync("a_b",
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event = 'ReplicationOriginDrop')", token), "the drop waiting for the origin");
            Assert.IsFalse(dropping.IsCompleted, "the drop, while the origin is held");
            await using (NpgsqlCommand reset = new("SELECT pg_replication_origin_session_reset()", holding))
            {
                _ = await reset.ExecuteNonQueryAsync(token);
            }

            _ = await dropping;
        }

        Assert.AreEqual("1|1", await subscriber.SafePsqlAsync(Database, $"""
            SELECT (SELECT count(*) FROM pg_replication_origin WHERE roname = '{slots[0]}'),
                (SELECT count(*) FROM pg_replication_origin WHERE roname = '{slots[1]}') + 1
            """, token), "a-b's origin, and none left for a_b");
        Assert.AreEqual("1", await provider.SafePsqlAsync(Database, $"SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slots[0]}'", token),
            "a-b's slot");
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (2)", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync("a-b", "SELECT EXISTS (SELECT FROM t WHERE id = 2)", token), "a-b's row");
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token, params string[] settings)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "max_worker_processes = 16",
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
    /// Opens a connection as the superuser the test creates, whose role settings the node's workers don't run with.
    /// </summary>
    private static async Task<NpgsqlConnection> OpenAsSuperUserAsync(TestNode node, CancellationToken token)
    {
        NpgsqlConnectionStringBuilder builder = new()
        {
            Host = "127.0.0.1",
            Port = node.Cluster.Port,
            Database = Database,
            Username = SuperUser,
            Pooling = false,
            Timeout = 60,
        };
        NpgsqlConnection connection = new(builder.ConnectionString);
        await connection.OpenAsync(token);
        return connection;
    }

    /// <summary>
    /// Commits a transaction under a replication origin, as apply does for the node it names, and that creates the
    /// origin when asked.
    /// </summary>
    private static Task<string> WriteAsAsync(TestNode node, string origin, string sql, CancellationToken token, bool create = false)
        => node.SafePsqlAsync(Database,
            $"{(create ? $"SELECT pg_replication_origin_create('{origin}'); " : "")}SELECT pg_replication_origin_session_setup('{origin}'); {sql}", token);

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
