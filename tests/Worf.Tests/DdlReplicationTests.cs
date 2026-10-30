using Npgsql;

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

        // A user who logs in and isn't a superuser creates and changes tables, which join and move between the sets on
        // both nodes, but can't mark a session as restoring a subscription's structure.
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE ROLE developer LOGIN", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "GRANT USAGE, CREATE ON SCHEMA app TO developer", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE TABLE app.owned (id integer PRIMARY KEY, v text);
            INSERT INTO app.owned VALUES (1, 'one');
            ALTER TABLE app.owned DROP CONSTRAINT owned_pkey;
            """, token, "developer");
        await WaitAsync(nodeA, token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.AreEqual("default_insert_only", await SetsAsync(node, "app.owned", token), $"{node.Name}: a table that lost its primary key");
        }

        Assert.AreEqual("1|one", await nodeB.SafePsqlAsync(Database, "SELECT * FROM app.owned", token));
        PostgresException restoring = await Assert.ThrowsExactlyAsync<PostgresException>(
            () => nodeA.SafePsqlAsync(Database, "SET worf.subscription_schema_restore = on", token, "developer"));
        Assert.AreEqual("42501", restoring.SqlState);

        // worf's own tables stay out of replication sets and the queue, even created from functions, and each node
        // drops worf itself.
        _ = await nodeA.SafePsqlAsync(Database, """
            SET worf.allow_ddl_from_functions = on;
            SELECT worf.conflict_history_ensure_partition(CURRENT_DATE + 60);
            """, token);
        Assert.AreEqual("0|0", await nodeA.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM worf.replication_set_table r JOIN pg_class c ON c.oid = r.set_reloid
                    WHERE c.relnamespace = 'worf'::regnamespace),
                (SELECT count(*) FROM worf.queue WHERE message::text LIKE '%conflict_history%')
            """, token));
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.drop_subscription('b_from_a')", token);
        PostgresException together = await Assert.ThrowsExactlyAsync<PostgresException>(
            () => nodeB.SafePsqlAsync(Database, "DROP EXTENSION worf, plpgsql CASCADE", token));
        Assert.AreEqual("55000", together.SqlState);
        _ = await nodeB.SafePsqlAsync(Database, "DROP EXTENSION worf CASCADE", token);
        _ = await nodeA.SafePsqlAsync(Database, "CREATE TABLE app.after (id integer PRIMARY KEY)", token);
        Assert.AreEqual("1|replicating", await nodeA.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM pg_extension WHERE extname = 'worf'),
                (SELECT status FROM worf.show_subscription_status() WHERE subscription_name = 'a_from_b')
            """, token), "node_a keeps worf");
        Assert.AreEqual("", await nodeB.SafePsqlAsync(Database, "SELECT extname FROM pg_extension WHERE extname = 'worf'", token));
    }

    /// <summary>
    /// Statements that use the session's own objects stay on their node, DDL from functions is queued once, a DROP of
    /// local and other tables replicates the others, the settings a statement ran with apply with it, tables follow
    /// their replica identity between the default sets, and a subscriber's structure sync doesn't queue the schema.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task OnlyDdlOtherNodesCanRunReplicates()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);

        _ = await nodeA.SafePsqlAsync(Database, """
            SET worf.allow_ddl_from_functions = on;
            CREATE FUNCTION make_s() RETURNS void LANGUAGE plpgsql AS $$ BEGIN CREATE TABLE s (id serial PRIMARY KEY, v text); END $$;
            SELECT make_s();
            CREATE TABLE orders (id integer PRIMARY KEY);
            CREATE TABLE pk (id integer PRIMARY KEY, v text);
            CREATE TABLE parted (id integer NOT NULL, v text) PARTITION BY RANGE (id);
            CREATE TABLE parted_1 PARTITION OF parted FOR VALUES FROM (0) TO (100);
            CREATE TABLE ul (id integer PRIMARY KEY);
            SET DateStyle = 'ISO, DMY';
            CREATE TABLE d (id integer PRIMARY KEY, day date DEFAULT '01/02/2026');
            """, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE TEMP TABLE tmp (id integer, v text);
            ALTER TABLE tmp RENAME TO tmp2;
            COMMENT ON TABLE tmp2 IS 'scratch';
            CREATE VIEW tmp_view AS SELECT * FROM tmp2;
            CREATE TABLE from_temp AS SELECT * FROM tmp2;
            CREATE TABLE like_temp (LIKE tmp2);
            PREPARE p AS SELECT 1 AS x;
            CREATE TABLE from_prepared AS EXECUTE p;
            GRANT SELECT ON tmp2 TO PUBLIC;
            CREATE FUNCTION nothing() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
            CREATE TRIGGER tmp_trigger BEFORE INSERT ON tmp2 FOR EACH ROW EXECUTE FUNCTION nothing();
            DROP TRIGGER tmp_trigger ON tmp2;
            CREATE POLICY tmp_policy ON tmp2 USING (true);
            DROP TABLE tmp2, orders CASCADE;
            ALTER TABLE pk DROP CONSTRAINT pk_pkey;
            ALTER TABLE parted ADD PRIMARY KEY (id);
            ALTER TABLE ul SET UNLOGGED;
            CREATE FUNCTION pg_temp.tmp_conv(integer) RETURNS bigint LANGUAGE sql AS 'SELECT $1::bigint';
            COMMENT ON FUNCTION pg_temp.tmp_conv(integer) IS 'scratch';
            ALTER FUNCTION pg_temp.tmp_conv(integer) STABLE;
            GRANT EXECUTE ON FUNCTION pg_temp.tmp_conv(integer) TO PUBLIC;
            CREATE TYPE pg_temp.tmp_type AS (a integer);
            COMMENT ON TYPE pg_temp.tmp_type IS 'scratch';
            CREATE TABLE conv (id integer PRIMARY KEY, v integer);
            ALTER TABLE conv ALTER COLUMN v TYPE bigint USING pg_temp.tmp_conv(v);
            ALTER TABLE conv ADD COLUMN w bigint DEFAULT pg_temp.tmp_conv(1);
            ALTER TABLE conv ADD COLUMN x pg_temp.tmp_type;
            DROP TYPE pg_temp.tmp_type CASCADE;
            DROP FUNCTION pg_temp.tmp_conv(integer) CASCADE;
            """, token);
        PostgresException together = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeA.SafePsqlAsync(Database, """
            CREATE FUNCTION pg_temp.tmp_f() RETURNS integer LANGUAGE sql AS 'SELECT 1';
            CREATE FUNCTION kept_f() RETURNS integer LANGUAGE sql AS 'SELECT 1';
            DROP FUNCTION pg_temp.tmp_f(), kept_f();
            """, token));
        Assert.AreEqual("55000", together.SqlState);

        // Statements about roles, databases, and PostgreSQL's own publications and subscriptions stay on their node.
        _ = await nodeA.SafePsqlAsync(Database, "CREATE DATABASE other STRATEGY FILE_COPY", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE ROLE r1;
            ALTER ROLE r1 RENAME TO r2;
            COMMENT ON ROLE r2 IS 'here';
            ALTER DATABASE other RENAME TO other2;
            ALTER DATABASE other2 OWNER TO r2;
            COMMENT ON DATABASE other2 IS 'here';
            GRANT CONNECT ON DATABASE other2 TO r2;
            CREATE PUBLICATION pub;
            ALTER PUBLICATION pub RENAME TO pub2;
            ALTER PUBLICATION pub2 OWNER TO r2;
            COMMENT ON PUBLICATION pub2 IS 'here';
            CREATE SUBSCRIPTION sub CONNECTION 'host=127.0.0.1 dbname=nowhere' PUBLICATION pub2 WITH (connect = false);
            ALTER SUBSCRIPTION sub RENAME TO sub2;
            COMMENT ON SUBSCRIPTION sub2 IS 'here';
            ALTER SUBSCRIPTION sub2 SET (slot_name = NONE);
            DROP SUBSCRIPTION sub2;
            DROP PUBLICATION pub2;
            """, token);
        await WaitAsync(nodeA, token);

        Assert.AreEqual("1|0|0|0", await nodeA.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM worf.queue WHERE message->>'command' LIKE 'CREATE TABLE s %'),
                (SELECT count(*) FROM worf.queue WHERE message::text LIKE '%tmp%' OR message::text LIKE '%EXECUTE p%'),
                (SELECT count(*) FROM worf.replication_set_table WHERE set_reloid IN ('from_temp'::regclass, 'like_temp'::regclass, 'from_prepared'::regclass)),
                (SELECT count(*) FROM worf.queue WHERE message->>'command' ~ '(ROLE|DATABASE|PUBLICATION|SUBSCRIPTION)')
            """, token), "DDL from a function queued once, and nothing that used the session's objects or was the cluster's");
        Assert.AreEqual("integer|", await nodeB.SafePsqlAsync(Database, """
            SELECT format_type(atttypid, atttypmod), (SELECT attname FROM pg_attribute WHERE attrelid = 'conv'::regclass AND attname = 'w')
            FROM pg_attribute WHERE attrelid = 'conv'::regclass AND attname = 'v'
            """, token), "what used temporary functions and types stayed on node_a");
        Assert.AreEqual("s||||'2026-02-01'::date", await nodeB.SafePsqlAsync(Database, """
            SELECT to_regclass('s')::text, to_regclass('orders')::text, to_regclass('from_temp')::text, to_regclass('from_prepared')::text,
                pg_get_expr(adbin, adrelid)
            FROM pg_attrdef WHERE adrelid = 'd'::regclass
            """, token), "what reached node_b");
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.AreEqual("default_insert_only", await SetsAsync(node, "pk", token), $"{node.Name}: a table that lost its primary key");
            Assert.AreEqual("default", await SetsAsync(node, "parted_1", token), $"{node.Name}: a partition whose parent gained one");
            Assert.AreEqual("", await SetsAsync(node, "ul", token), $"{node.Name}: an unlogged table");
        }

        // DDL a table's owner runs applies as that owner, and later changes apply as before.
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE ROLE owner", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, """
            ALTER TABLE pk OWNER TO owner;
            SET ROLE owner;
            ALTER TABLE pk ADD COLUMN w text;
            RESET ROLE;
            INSERT INTO d VALUES (1);
            """, token);
        await WaitAsync(nodeA, token);
        Assert.AreEqual("owner|1", await nodeB.SafePsqlAsync(Database,
            "SELECT pg_get_userbyid(relowner), (SELECT count(*) FROM d) FROM pg_class WHERE oid = 'pk'::regclass", token));

        // A subscriber whose structure comes from node_a queues none of it.
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        _ = await nodeC.SafePsqlAsync(Database, "CREATE ROLE owner", token);
        _ = await nodeC.SafePsqlAsync(Database, $$"""
            SELECT worf.create_subscription(
                subscription_name := 'c_from_a',
                provider_dsn := '{{nodeA.Dsn(Database, SuperUser)}}',
                synchronize_structure := true,
                synchronize_data := true,
                forward_origins := '{}')
            """, token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, """
            SELECT EXISTS (SELECT 1 FROM worf.show_subscription_status() WHERE subscription_name = 'c_from_a' AND status = 'replicating')
            """, token), "node_c's structure sync");
        Assert.AreEqual("0", await nodeC.SafePsqlAsync(Database, "SELECT count(*) FROM worf.queue WHERE message_type = 'D'", token));
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
