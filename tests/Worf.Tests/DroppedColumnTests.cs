namespace Worf.Tests;

/// <summary>
/// Apply ignores a column the subscriber dropped in rows from a provider that still has it.
/// </summary>
[TestClass]
public sealed class DroppedColumnTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Rows keep replicating after the subscriber drops a column the provider still has, also while apply is in a
    /// transaction that writes to the table later, and a column the subscriber never had still stops apply.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RowsWithAColumnDroppedHereStillApply()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, a text, b text);
            CREATE TABLE u (id integer PRIMARY KEY, a text, b text);
            CREATE TABLE w (id integer PRIMARY KEY, v text);
            SELECT * FROM worf.replication_set_add_table('default', 't');
            SELECT * FROM worf.replication_set_add_table('default', 'u');
            SELECT * FROM worf.replication_set_add_table('default', 'w');
            """, token);

        // Apply takes a while over a row of t's with a slow trigger.
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, a text, b text);
            CREATE TABLE u (id integer PRIMARY KEY, a text, b text);
            CREATE FUNCTION slow() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.a = 'slow' THEN
                    PERFORM pg_sleep(2);
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER slow BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION slow();
            ALTER TABLE t ENABLE ALWAYS TRIGGER slow;
            """, token);

        // Apply's triggers run under the search path the database sets, as a local session's do, so a function of the
        // database's own that one calls without a schema is the one a local insert calls.
        _ = await subscriber.SafePsqlAsync(Database, $"""
            CREATE TABLE w (id integer PRIMARY KEY, v text);
            CREATE FUNCTION public.upper(text) RETURNS text LANGUAGE sql AS $$SELECT 'custom:' || $1$$;
            CREATE FUNCTION shout() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.v := upper(NEW.v); RETURN NEW; END $$;
            CREATE TRIGGER shout BEFORE INSERT ON w FOR EACH ROW EXECUTE FUNCTION shout();
            ALTER TABLE w ENABLE ALWAYS TRIGGER shout;
            ALTER DATABASE {Database} SET search_path = public, pg_catalog;
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := false)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'sub' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");

        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (1, 'a1', 'b1'); INSERT INTO u VALUES (1, 'a1', 'b1'); INSERT INTO w VALUES (1, 'abc')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM u WHERE id = 1)", token),
            "the rows before the drop");
        _ = await subscriber.SafePsqlAsync(Database, "INSERT INTO w VALUES (2, 'abc')", token);
        Assert.AreEqual("1|custom:abc\n2|custom:abc", await subscriber.SafePsqlAsync(Database, "SELECT * FROM w ORDER BY id", token),
            "the rows apply and a local session inserted");

        _ = await subscriber.SafePsqlAsync(Database, "ALTER TABLE t DROP COLUMN b", token);
        _ = await provider.SafePsqlAsync(Database, """
            INSERT INTO t VALUES (2, 'a2', 'b2');
            UPDATE t SET a = 'a1 again', b = 'b1 again' WHERE id = 1;
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 2)", token),
            "the rows after the drop");
        Assert.AreEqual("1|a1 again\n2|a2", await subscriber.SafePsqlAsync(Database, "SELECT * FROM t ORDER BY id", token));

        // The drop of u's column commits while apply is in the trigger, and apply takes it in as it opens u, which it
        // then looks up again rather than losing its OID.
        int droppos = subscriber.ReadLog().Length;
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (3, 'slow'); INSERT INTO u VALUES (2, 'a2', 'b2')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND backend_type <> 'client backend')", token),
            "apply in the trigger");
        _ = await subscriber.SafePsqlAsync(Database, "ALTER TABLE u DROP COLUMN b", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM u WHERE id = 2)", token),
            "the row after the drop");
        Assert.AreEqual("1|a1\n2|a2", await subscriber.SafePsqlAsync(Database, "SELECT * FROM u ORDER BY id", token));
        Assert.DoesNotContain("exiting with error", subscriber.ReadLog()[droppos..], "apply kept going");

        int logpos = subscriber.ReadLog().Length;
        _ = await provider.SafePsqlAsync(Database, """
            ALTER TABLE t ADD COLUMN c text;
            INSERT INTO t VALUES (4, 'a4', 'b4', 'c4');
            """, token);
        Assert.IsTrue(await subscriber.PollLogAsync(logpos, "unknown column name c", token), "the column the subscriber never had");
    }

    /// <summary>
    /// A table owner who isn't a superuser drops columns, tables and other relations. Columns are recorded, a dropped
    /// table's columns are forgotten, and temporary tables are left out. A drop that apply runs is recorded too, and a
    /// delete whose old row still carries a dropped column finds its row.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DropsByOwnersAndApplyAreRecorded()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE f (id integer PRIMARY KEY, a text, b text);
                CREATE TABLE r (id integer PRIMARY KEY, a text, b text);
                """, token);
        }

        _ = await provider.SafePsqlAsync(Database, """
            SELECT * FROM worf.replication_set_add_table('default', 'f');
            SELECT * FROM worf.replication_set_add_table('default', 'r');
            ALTER TABLE f REPLICA IDENTITY FULL;
            INSERT INTO f VALUES (1, 'a1', 'b1'), (2, 'a2', 'b2');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := true)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'sub' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT count(*) = 2 FROM f", token), "the copied rows");

        // The owner drops a column, other relations, and a table and a temporary table with dropped columns.
        string scratch = await subscriber.SafePsqlAsync(Database, """
            CREATE ROLE owner;
            GRANT CREATE ON SCHEMA public TO owner;
            ALTER TABLE f OWNER TO owner;
            SET ROLE owner;
            CREATE TABLE scratch (id integer, z text);
            SELECT 'scratch'::regclass::oid;
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            SET ROLE owner;
            ALTER TABLE f DROP COLUMN b;
            ALTER TABLE scratch DROP COLUMN z;
            CREATE INDEX scratch_id ON scratch (id);
            CREATE VIEW scratch_view AS SELECT id FROM scratch;
            CREATE SEQUENCE scratch_seq;
            DROP VIEW scratch_view;
            DROP INDEX scratch_id;
            DROP SEQUENCE scratch_seq;
            DROP TABLE scratch;
            CREATE TEMPORARY TABLE scratch_temp (id integer, y text);
            ALTER TABLE scratch_temp DROP COLUMN y;
            DROP TABLE scratch_temp;
            """, token);
        Assert.AreEqual("f|b", await subscriber.SafePsqlAsync(Database,
            "SELECT relid::regclass::text, attname FROM worf.dropped_column", token), "only the owner's table's dropped column");

        // A delete whose old row carries the dropped column.
        _ = await provider.SafePsqlAsync(Database, "DELETE FROM f WHERE id = 1", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT NOT EXISTS (SELECT 1 FROM f WHERE id = 1)", token),
            "the deleted row");

        // A drop that apply runs.
        _ = await provider.SafePsqlAsync(Database, "SELECT worf.replicate_ddl_command('ALTER TABLE public.r DROP COLUMN b')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.dropped_column WHERE relid = 'r'::regclass AND attname = 'b')", token),
            "the drop that apply ran");
        Assert.AreEqual("0", await subscriber.SafePsqlAsync(Database,
            $"SELECT count(*) FROM worf.dropped_column WHERE relid = {scratch}", token));
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
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
}
