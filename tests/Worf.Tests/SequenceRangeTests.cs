using System.Diagnostics;
using System.Globalization;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.Internal;

namespace Worf.Tests;

/// <summary>
/// Sequence range management.
/// </summary>
[TestClass]
public sealed class SequenceRangeTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// A sequence waits in quarantine until activated, then draws from the owner's blocks, above the values the table
    /// already holds, and switches to its next block as the active one runs low. The guard admits values the sequence
    /// has passed and rows that already exist, and rejects the rest.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task OwnerDrawsFromItsBlocksAndGuardsHandWrittenValues()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node", token);
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE t (id bigserial PRIMARY KEY, v text);
            INSERT INTO t (id, v) VALUES (500, 'legacy');
            SELECT worf.enable_sequence_range('t_id_seq', range_size := 10);
            """, token);

        // Quarantined until activated.
        PostgresException quarantined = await Assert.ThrowsExactlyAsync<PostgresException>(
            () => node.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('x')", token));
        Assert.AreEqual("2200H", quarantined.SqlState);
        Assert.AreEqual("collecting", await node.SafePsqlAsync(Database, "SELECT state FROM worf.sequence_ranges", token));

        // The root starts above the legacy row.
        _ = await node.SafePsqlAsync(Database, "SELECT worf.activate_sequence_range('t_id_seq')", token);
        Assert.AreEqual("active|501|510|511|520", await node.SafePsqlAsync(Database,
            "SELECT state, active_lo, active_hi, next_lo, next_hi FROM worf.sequence_ranges", token));
        Assert.AreEqual("501", await node.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('first') RETURNING id", token));

        // The range worker switches as the block runs low.
        for (int i = 0; i < 25; i++)
        {
            _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('more')", token);
            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }

        Assert.AreEqual("27|t", await node.SafePsqlAsync(Database,
            "SELECT count(*), count(DISTINCT id) = count(*) FROM t", token));
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT active_lo > 510 AND next_lo IS NOT NULL FROM worf.sequence_ranges", token), "a switch to a later block");

        // Values behind the sequence in its block pass, values ahead of it, below the root or past it fail, and a
        // value a row already holds reaches that row.
        string last = await node.SafePsqlAsync(Database, "SELECT max(id) FROM t", token);
        _ = await node.SafePsqlAsync(Database, "DELETE FROM t WHERE id = 503", token);
        _ = await node.SafePsqlAsync(Database, "INSERT INTO t (id, v) VALUES (503, 'by hand, behind')", token);
        string granted = await node.SafePsqlAsync(Database, "SELECT watermark FROM worf.sequence_range", token);
        foreach (string value in new[] { "400", Next(last), Next(granted) })
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(
                () => node.SafePsqlAsync(Database, $"INSERT INTO t (id, v) VALUES ({value}, 'by hand')", token), value);
            Assert.AreEqual("23514", error.SqlState, value);
        }

        _ = await node.SafePsqlAsync(Database,
            "INSERT INTO t (id, v) VALUES (500, 'upserted') ON CONFLICT (id) DO UPDATE SET v = excluded.v", token);
        Assert.AreEqual("upserted", await node.SafePsqlAsync(Database, "SELECT v FROM t WHERE id = 500", token));

        // A role that isn't a superuser numbers rows from the block, and the guard checks its own values too.
        _ = await node.SafePsqlAsync(Database, """
            CREATE ROLE app;
            GRANT INSERT, SELECT ON t TO app;
            GRANT USAGE ON SEQUENCE t_id_seq TO app;
            """, token);
        Assert.AreEqual("t", await node.SafePsqlAsync(Database,
            "SET ROLE app; INSERT INTO t (v) VALUES ('app') RETURNING id > 500", token));
        PostgresException appError = await Assert.ThrowsExactlyAsync<PostgresException>(
            () => node.SafePsqlAsync(Database, "SET ROLE app; INSERT INTO t (id, v) VALUES (1, 'app by hand')", token));
        Assert.AreEqual("23514", appError.SqlState);
    }

    /// <summary>
    /// Range management rejects the DDL and settings that would let values escape it, and its guard checks every row
    /// of a statement as the row was finally written, but stays silent under session_replication_role replica. Rows
    /// that DDL brings under the guard are checked too, a partitioned table's guard covers its partitions' own columns,
    /// and a row an update moves to another partition keeps its value. Ordinary roles keep running DDL, and only a
    /// sequence's owner reserves its values.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ValuesCantEscapeRangeManagement()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node", token);
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE t (id bigserial PRIMARY KEY, v text);
            GRANT UPDATE ON SEQUENCE t_id_seq TO PUBLIC;
            CREATE TEMP SEQUENCE temp_seq;
            CREATE SEQUENCE cycling CYCLE;
            CREATE SEQUENCE replicated;
            SELECT worf.replication_set_add_sequence('default', 'replicated');
            CREATE SEQUENCE textual_seq;
            CREATE TABLE textual (code text PRIMARY KEY DEFAULT 'INV-' || nextval('textual_seq'));
            CREATE TABLE parent (v text);
            CREATE TABLE child (id bigserial PRIMARY KEY) INHERITS (parent);
            CREATE SCHEMA "a.b";
            CREATE SEQUENCE "a.b".dotted;
            CREATE SCHEMA other;
            CREATE SEQUENCE nseq;
            CREATE TABLE n (id numeric PRIMARY KEY DEFAULT nextval('nseq'), v text);
            CREATE TABLE o (id bigserial, region text, PRIMARY KEY (id, region)) PARTITION BY LIST (region);
            CREATE TABLE o_us PARTITION OF o FOR VALUES IN ('us');
            CREATE TABLE o_eu PARTITION OF o FOR VALUES IN ('eu');
            INSERT INTO o VALUES (5, 'us');
            """, token);
        foreach (string sequence in new[] { "temp_seq", "cycling", "replicated", "textual_seq" })
        {
            Assert.IsFalse(await node.PsqlAsync(Database, $"SELECT worf.enable_sequence_range('{sequence}', participants := '{{}}')", token),
                sequence);
        }

        _ = await node.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{}', range_size := 100);
            SELECT worf.enable_sequence_range('child_id_seq', participants := '{}', range_size := 100);
            SELECT worf.enable_sequence_range('"a.b".dotted', participants := '{}', range_size := 100);
            SELECT worf.enable_sequence_range('nseq', participants := '{}', range_size := 100);
            SELECT worf.enable_sequence_range('o_id_seq', participants := '{}', range_size := 100);
            """, token);
        Assert.AreEqual("0", await node.SafePsqlAsync(Database, """
            SELECT count(*) FROM pg_class c, aclexplode(c.relacl) a WHERE c.oid = 't_id_seq'::regclass AND a.privilege_type = 'UPDATE'
            """, token), "UPDATE revoked");

        foreach (string ddl in new[]
        {
            "ALTER SEQUENCE t_id_seq RESTART",
            "ALTER SEQUENCE t_id_seq SET UNLOGGED",
            "ALTER TABLE t SET UNLOGGED",
            "ALTER SEQUENCE t_id_seq RENAME TO renamed",
            "ALTER SEQUENCE t_id_seq SET SCHEMA other",
            "ALTER TABLE t SET SCHEMA other",
            "ALTER SCHEMA \"a.b\" RENAME TO ab",
            "GRANT UPDATE ON SEQUENCE t_id_seq TO PUBLIC",
            "GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO PUBLIC",
            "GRANT ALL ON ALL SEQUENCES IN SCHEMA \"a.b\" TO PUBLIC",
            "ALTER TABLE t ALTER COLUMN id SET DEFAULT 0",
            "ALTER TABLE t ALTER COLUMN id DROP DEFAULT",
            "DROP TRIGGER worf_sequence_range_guard ON t",
            "DROP TRIGGER worf_sequence_range_guard_update ON t",
            "CREATE OR REPLACE TRIGGER worf_sequence_range_guard AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()",
            "ALTER TABLE t DISABLE TRIGGER worf_sequence_range_guard",
            "ALTER TABLE t ENABLE REPLICA TRIGGER worf_sequence_range_guard",
            "ALTER TABLE t DISABLE TRIGGER ALL",
            "ALTER TRIGGER worf_sequence_range_guard ON t RENAME TO other",
            "ALTER TRIGGER worf_sequence_range_guard_update ON t DEPENDS ON EXTENSION worf",
            "TRUNCATE t RESTART IDENTITY",
            "TRUNCATE parent RESTART IDENTITY",
        })
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database, ddl, token), ddl);
            Assert.AreEqual("55000", error.SqlState, ddl);
        }

        foreach (string call in new[]
        {
            "SELECT worf.replication_set_add_sequence('default', 't_id_seq')",
            "SELECT worf.synchronize_sequence('t_id_seq')",
            "SELECT worf.alter_sequence_range('t_id_seq', 1, 1)",
            "SELECT worf.alter_sequence_range('worf.snowflake_node', 2)",
        })
        {
            Assert.IsFalse(await node.PsqlAsync(Database, call, token), call);
        }

        // A dumped guard is skipped, ENABLE ALWAYS and TRUNCATE ONLY leave values alone, and adding a schema's
        // sequences leaves out the managed ones.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TRIGGER worf_sequence_range_guard AFTER INSERT ON t FOR EACH ROW
                WHEN (worf.sequence_range_check('t_id_seq'::regclass, new.id)) EXECUTE FUNCTION worf.sequence_range_guard();
            CREATE TABLE dumped (id bigserial PRIMARY KEY);
            CREATE TRIGGER worf_sequence_range_guard AFTER INSERT ON dumped FOR EACH ROW
                WHEN (worf.sequence_range_check('dumped_id_seq'::regclass, new.id)) EXECUTE FUNCTION worf.sequence_range_guard('1:1');
            ALTER TABLE t ENABLE ALWAYS TRIGGER worf_sequence_range_guard;
            ALTER TABLE t ENABLE TRIGGER worf_sequence_range_guard;
            TRUNCATE ONLY parent RESTART IDENTITY;
            SELECT worf.replication_set_add_all_sequences('default', '{public}');
            """, token);
        Assert.AreEqual("0|0|t", await node.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM pg_trigger WHERE tgrelid = 'dumped'::regclass),
                (SELECT count(*) FROM worf.replication_set_seq s JOIN worf.sequence_range r ON r.seqoid = s.set_seqoid),
                EXISTS (SELECT 1 FROM worf.replication_set_seq WHERE set_seqoid = 'cycling'::regclass)
            """, token));

        _ = await node.SafePsqlAsync(Database, "GRANT USAGE ON SEQUENCE t_id_seq TO PUBLIC; TRUNCATE t; GRANT ALL ON ALL TABLES IN SCHEMA public TO PUBLIC", token);

        // Every row of a statement is checked, as written: a value the sequence passed, which no row holds, passes
        // alone, and fails beside one ahead of the sequence, which fails the statement.
        string first = await node.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('a'), ('b') RETURNING id", token);
        string behind = first.Split('\n')[1];
        string ahead = Next(Next(behind));
        _ = await node.SafePsqlAsync(Database, $"DELETE FROM t WHERE id = {behind}", token);
        await AssertOutsideAsync(node, $"INSERT INTO t (id, v) VALUES ({behind}, 'behind'), ({ahead}, 'ahead')");
        Assert.AreEqual("0", await node.SafePsqlAsync(Database, $"SELECT count(*) FROM t WHERE id = {behind}", token));
        _ = await node.SafePsqlAsync(Database, $"INSERT INTO t (id, v) VALUES ({behind}, 'behind')", token);
        _ = await node.SafePsqlAsync(Database, $"SET session_replication_role = replica; INSERT INTO t (id, v) VALUES ({ahead}, 'replica')", token);

        // A BEFORE trigger that sorts after the guard can't change the key past it, and a row with a value another
        // row holds is still a new row, without a unique index on the value alone.
        _ = await node.SafePsqlAsync(Database, """
            CREATE FUNCTION bump() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN NEW.id := NEW.id + 1000000; RETURN NEW; END$$;
            CREATE TRIGGER zzz BEFORE INSERT OR UPDATE ON t FOR EACH ROW EXECUTE FUNCTION bump();
            CREATE TABLE tenants (id bigint NOT NULL DEFAULT nextval('t_id_seq'), tenant int, PRIMARY KEY (id, tenant));
            SET session_replication_role = replica;
            INSERT INTO tenants VALUES (1200, 1);
            """, token);
        await AssertOutsideAsync(node, "INSERT INTO t (v) VALUES ('bumped')");
        await AssertOutsideAsync(node, "UPDATE t SET v = 'bumped' WHERE id = " + behind);
        _ = await node.SafePsqlAsync(Database, "DROP TRIGGER zzz ON t; UPDATE t SET v = 'updated' WHERE id = " + behind, token);
        await AssertOutsideAsync(node, "INSERT INTO tenants VALUES (1200, 2)");
        _ = await node.SafePsqlAsync(Database, "INSERT INTO tenants (tenant) VALUES (2)", token);

        // Renaming the key keeps it checked, and a numeric key is checked as its whole values.
        _ = await node.SafePsqlAsync(Database, "ALTER TABLE t RENAME COLUMN id TO key; INSERT INTO t (v) VALUES ('renamed')", token);
        await AssertOutsideAsync(node, "INSERT INTO t (key, v) VALUES (" + Next(await node.SafePsqlAsync(Database, "SELECT max(key) FROM t", token)) + ", 'x')");
        _ = await node.SafePsqlAsync(Database, "INSERT INTO n (v) VALUES ('default')", token);
        await AssertOutsideAsync(node, "INSERT INTO n (id, v) VALUES (5.5, 'fraction')");
        await AssertOutsideAsync(node, "INSERT INTO n (id, v) VALUES (" + Next(Next(await node.SafePsqlAsync(Database, "SELECT max(id) FROM n", token))) + ", 'ahead')");

        // An update moves a row to another partition with its value, even one from before range management, but not to
        // a value of its choosing, and a MERGE that only updates does too.
        _ = await node.SafePsqlAsync(Database, """
            UPDATE o SET region = 'eu' WHERE id = 5;
            MERGE INTO o USING (VALUES (5)) s (k) ON o.id = s.k WHEN MATCHED THEN UPDATE SET region = 'us';
            """, token);
        await AssertOutsideAsync(node, "UPDATE o SET region = 'eu', id = 1000000 WHERE id = 5");
        await AssertOutsideAsync(node, "MERGE INTO o USING (VALUES (5)) s (k) ON o.id = s.k WHEN MATCHED THEN UPDATE SET region = 'eu' WHEN NOT MATCHED THEN INSERT VALUES (s.k, 'eu')");

        // Rows that DDL brings under the guard can't hold values the sequence may still issue, though values from before
        // range management can stay. A partitioned table's guard checks a column that only a partition's own default
        // draws from a managed sequence.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE legacy (id bigint PRIMARY KEY, v text);
            INSERT INTO legacy VALUES (424242, 'inside the root');
            CREATE TABLE pp (id bigint PRIMARY KEY DEFAULT nextval('t_id_seq'), ref bigint) PARTITION BY RANGE (id);
            CREATE TABLE pp_low PARTITION OF pp (ref DEFAULT nextval('nseq')) FOR VALUES FROM (MINVALUE) TO (1000000);
            CREATE TABLE pp_high (LIKE pp);
            INSERT INTO pp_high VALUES (1555555, NULL);
            """, token);
        await AssertOutsideAsync(node, "ALTER TABLE legacy ALTER COLUMN id SET DEFAULT nextval('t_id_seq')");
        await AssertOutsideAsync(node, "ALTER TABLE pp ATTACH PARTITION pp_high FOR VALUES FROM (1000000) TO (MAXVALUE)");
        await AssertOutsideAsync(node, "INSERT INTO pp_low (ref) VALUES (515151)");
        _ = await node.SafePsqlAsync(Database, """
            UPDATE legacy SET id = -1;
            ALTER TABLE legacy ALTER COLUMN id SET DEFAULT nextval('t_id_seq');
            INSERT INTO pp_low DEFAULT VALUES;
            """, token);

        // A role that isn't a superuser runs its own DDL, with the tables it creates joining the default replication
        // sets, but can't claim to restore a subscription's structure, and only the sequence's owner reserves its values.
        _ = await node.SafePsqlAsync(Database, """
            CREATE ROLE app;
            GRANT USAGE ON SEQUENCE t_id_seq TO app;
            GRANT USAGE ON SCHEMA worf TO app;
            CREATE SCHEMA app AUTHORIZATION app;
            SET worf.include_ddl_repset = on;
            SET ROLE app;
            CREATE TABLE app.app_rows (id bigint PRIMARY KEY DEFAULT nextval('public.t_id_seq'), v text);
            CREATE TABLE app.own_rows (id bigserial PRIMARY KEY);
            ALTER TABLE app.app_rows ADD COLUMN w int;
            GRANT ALL ON ALL TABLES IN SCHEMA app TO PUBLIC;
            INSERT INTO app.app_rows (v) VALUES ('app');
            """, token);
        Assert.AreEqual("2|2", await node.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM pg_trigger WHERE tgrelid = 'app.app_rows'::regclass AND tgname LIKE 'worf_sequence_range_guard%'),
                (SELECT count(*) FROM worf.replication_set_table WHERE set_reloid IN ('app.app_rows'::regclass, 'app.own_rows'::regclass))
            """, token));
        PostgresException restore = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
            "SET ROLE app; SET worf.subscription_schema_restore = on; DROP TRIGGER worf_sequence_range_guard ON app.app_rows", token));
        Assert.AreEqual("42501", restore.SqlState);
        PostgresException reserve = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
            "SET ROLE app; SELECT worf.reserve_sequence_values('t_id_seq', 1, 2)", token));
        Assert.AreEqual("42501", reserve.SqlState);
        Assert.Contains("must be owner", reserve.MessageText);

        async Task AssertOutsideAsync(TestNode target, string sql)
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => target.SafePsqlAsync(Database, sql, token), sql);
            Assert.AreEqual("23514", error.SqlState, sql);
        }
    }

    /// <summary>
    /// A session reserves a block right after the database's first sequence is enabled, as the range worker starts. The
    /// range worker switches blocks, and a load's reserved block, while sessions read the ranges' view or have
    /// inserted, but not while a transaction that called nextval stays open. It leaves a block when the sequence moves
    /// backwards in it, behind where it was at the last check or behind rows, warns once a root has granted nine tenths
    /// of its values, and partitions of a guarded table keep the guard as they come and go.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SwitchesAndReservationsWorkAroundSessions()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node", token);
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE t (id bigserial PRIMARY KEY, v text);
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{}', range_size := 10);
            """, token);
        _ = await node.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_range('t_id_seq', 50, timeout := '20 seconds')", token);
        _ = await node.SafePsqlAsync(Database, """
            CREATE SEQUENCE small AS smallint MAXVALUE 1000;
            SELECT worf.enable_sequence_range('small', participants := '{}', range_size := 10);
            CREATE TABLE pt (id bigserial PRIMARY KEY, v text) PARTITION BY RANGE (id);
            CREATE TABLE pt_low PARTITION OF pt FOR VALUES FROM (MINVALUE) TO (1000000000);
            SELECT worf.enable_sequence_range('pt_id_seq', participants := '{}', range_size := 100);
            CREATE TABLE pt_high PARTITION OF pt FOR VALUES FROM (1000000000) TO (MAXVALUE);
            """, token);

        // A transaction that reads the view doesn't hold off the switches that sessions inserting need.
        await using (NpgsqlConnection reader = await node.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand read = new("BEGIN; SELECT values_left FROM worf.sequence_ranges", reader))
            {
                _ = await read.ExecuteScalarAsync(token);
            }

            await using NpgsqlConnection writer = await node.OpenAsync(Database, token);
            for (int i = 0; i < 40; i++)
            {
                await using NpgsqlCommand insert = new("INSERT INTO t (v) VALUES ('while read')", writer);
                _ = await insert.ExecuteNonQueryAsync(token);
                await Task.Delay(TimeSpan.FromMilliseconds(50), token);
            }

            // A session that has inserted reserves a block and loads it, its guard following the switch.
            await using (NpgsqlCommand reserve = new("SELECT worf.reserve_sequence_range('t_id_seq', 500)", writer))
            {
                _ = await reserve.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand load = new("INSERT INTO t (v) SELECT 'bulk' FROM generate_series(1, 500)", writer);
            _ = await load.ExecuteNonQueryAsync(token);
        }

        // An open transaction that called nextval holds off every switch until it ends.
        string heldBlock;
        await using (NpgsqlConnection holder = await node.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand hold = new("BEGIN; SELECT nextval('t_id_seq')", holder))
            {
                _ = await hold.ExecuteScalarAsync(token);
            }

            heldBlock = await node.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token);
            await using NpgsqlConnection writer = await node.OpenAsync(Database, token);
            bool exhausted = false;
            for (int i = 0; i < 5000 && !exhausted; i++)
            {
                try
                {
                    await using NpgsqlCommand insert = new("INSERT INTO t (v) SELECT 'held' FROM generate_series(1, 10)", writer);
                    _ = await insert.ExecuteNonQueryAsync(token);
                }
                catch (PostgresException error) when (error.SqlState == "2200H")
                {
                    exhausted = true;
                }
            }

            Assert.IsTrue(exhausted, "the sequence ran out while a switch waited");
        }

        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            $"SELECT active_lo <> {heldBlock} FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token), "a switch once the transaction ended");
        _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('after')", token);

        // A session can't reserve a block it would have to switch to while its own transaction holds the sequence.
        PostgresException held = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
            "BEGIN; INSERT INTO t (v) VALUES ('first'); SELECT worf.reserve_sequence_range('t_id_seq', 100000)", token));
        Assert.AreEqual("55P03", held.SqlState);

        // The worker switches to the block that was reserved once the transaction has ended.
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT NOT EXISTS (SELECT 1 FROM worf.sequence_range_block WHERE seqoid = 't_id_seq'::regclass AND state = 'next' AND reserved)", token),
            "the reserved block switched to");

        // A setval that moves the sequence back in its block makes the worker leave the block.
        int logpos = node.ReadLog().Length;
        string before = await node.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token);
        _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('x'), ('y'), ('z')", token);
        await Task.Delay(TimeSpan.FromMilliseconds(500), token);
        _ = await node.SafePsqlAsync(Database, $"SELECT setval('t_id_seq', {before})", token);
        Assert.IsTrue(await node.PollLogAsync(logpos, "last value moved back", token), "the backwards move noticed");
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            $"SELECT state = 'active' AND active_lo <> {before} FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token),
            "a switch away from the block");

        // So does one back to a value past where the sequence was at the last check, though behind rows in the block.
        logpos = node.ReadLog().Length;
        before = await node.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token);
        _ = await node.SafePsqlAsync(Database, """
            WITH added AS (INSERT INTO t (v) SELECT 'w' FROM generate_series(1, 5) RETURNING id)
            SELECT setval('t_id_seq', (SELECT max(id) FROM added) - 2)
            """, token);
        Assert.IsTrue(await node.PollLogAsync(logpos, "behind the value", token), "the backwards move behind the rows noticed");
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            $"SELECT state = 'active' AND active_lo <> {before} FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token),
            "a switch away from the block behind the rows");
        _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'after' FROM generate_series(1, 5)", token);

        // A reservation that takes most of a root warns.
        logpos = node.ReadLog().Length;
        _ = await node.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_range('small', 850)", token);
        Assert.IsTrue(await node.PollLogAsync(logpos, "root has granted nine tenths of its values", token), "the root's warning");

        // Partitions take the parent's guard, and one detached keeps a guard of its own.
        _ = await node.SafePsqlAsync(Database, "INSERT INTO pt (v) VALUES ('a'), ('b')", token);
        string ptAhead = Next(Next(await node.SafePsqlAsync(Database, "SELECT max(id) FROM pt", token)));
        foreach (string sql in new[] { $"INSERT INTO pt (id, v) VALUES ({ptAhead}, 'ahead')", "INSERT INTO pt (id, v) VALUES (2000000000, 'high')" })
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database, sql, token), sql);
            Assert.AreEqual("23514", error.SqlState, sql);
        }

        _ = await node.SafePsqlAsync(Database, "ALTER TABLE pt DETACH PARTITION pt_high", token);
        PostgresException detached = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
            "INSERT INTO pt_high (id, v) VALUES (2000000000, 'detached')", token));
        Assert.AreEqual("23514", detached.SqlState);

        // Attaching a partition doesn't hold up writers to the rest of the tree.
        await using (NpgsqlConnection attacher = await node.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand attach = new("BEGIN; ALTER TABLE pt ATTACH PARTITION pt_high FOR VALUES FROM (1000000000) TO (MAXVALUE)", attacher))
            {
                _ = await attach.ExecuteNonQueryAsync(token);
            }

            _ = await node.SafePsqlAsync(Database, "SET lock_timeout = '2s'; INSERT INTO pt (v) VALUES ('while attaching')", token);
            await using NpgsqlCommand commit = new("COMMIT", attacher);
            _ = await commit.ExecuteNonQueryAsync(token);
        }

        Assert.AreEqual("6", await node.SafePsqlAsync(Database,
            "SELECT count(*) FROM pg_trigger WHERE tgrelid IN ('pt'::regclass, 'pt_low'::regclass, 'pt_high'::regclass) AND tgname LIKE 'worf_sequence_range_guard%'",
            token), "a guard pair on each table");
    }

    /// <summary>
    /// A descending sequence with an increment of -5 and a cache, filling a column narrower than itself, draws from
    /// blocks below the values its table holds, within the column's range, with its cache forced to 1. A sequence that
    /// already issued values draws from blocks beyond them.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DescendingSequencesFillNarrowerColumns()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node", token);
        _ = await node.SafePsqlAsync(Database, """
            CREATE SEQUENCE down INCREMENT BY -5 CACHE 20;
            CREATE TABLE d (id integer PRIMARY KEY DEFAULT nextval('down'), v text);
            INSERT INTO d VALUES (-100, 'legacy');
            SELECT worf.enable_sequence_range('down', participants := '{}', range_size := 10);
            """, token);
        Assert.AreEqual("1|-146|-101|-192|-147", await node.SafePsqlAsync(Database, """
            SELECT s.seqcache, r.active_lo, r.active_hi, r.next_lo, r.next_hi
            FROM worf.sequence_ranges r JOIN pg_sequence s ON s.seqrelid = 'down'::regclass
            """, token));
        Assert.AreEqual("-101\n-106", await node.SafePsqlAsync(Database,
            "INSERT INTO d (v) VALUES ('a'), ('b') RETURNING id", token));
        Assert.AreEqual(int.MinValue.ToString(CultureInfo.InvariantCulture), await node.SafePsqlAsync(Database,
            "SELECT root_end FROM worf.sequence_range WHERE seqoid = 'down'::regclass", token));

        _ = await node.SafePsqlAsync(Database, """
            CREATE SEQUENCE q3;
            SELECT nextval('q3') FROM generate_series(1, 3);
            SELECT worf.enable_sequence_range('q3', participants := '{}', range_size := 10);
            """, token);
        Assert.AreEqual("4", await node.SafePsqlAsync(Database, "SELECT nextval('q3')", token));
    }

    /// <summary>
    /// Two nodes that replicate both ways insert into the same table at once with keys from the same sequence, which the
    /// first node manages: the second learns of it through the exchange, gets blocks from the first, and no key is
    /// written on both nodes. A table on the second node that comes to use the sequence may hold the first's keys.
    /// Idle, with or without managed sequences and Snowflake IDs, and with a sequence whose root can't start, neither
    /// node uses a transaction ID, though they go on checking and exchanging.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task BidirectionalNodesNeverShareAKey()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id bigserial PRIMARY KEY, v text);
                SELECT * FROM worf.replication_set_add_table('default', 't');
                CREATE TABLE narrow (id smallserial PRIMARY KEY, v text);
                """, token);
        }

        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO narrow VALUES (32767, 'legacy')", token);
        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await AssertIdleAsync([nodeA, nodeB], false, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 1000);
            SELECT worf.enable_snowflake('{node_b}');
            """, token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database,
                "SELECT count(*) = 2 AND bool_and(state = 'active') AND worf.snowflake_node() IS NOT NULL FROM worf.sequence_ranges", token),
                $"{node.Name} drawing from blocks, with a Snowflake number");
        }

        await Task.WhenAll(InsertAsync(nodeA, "a"), InsertAsync(nodeB, "b"));
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.AreEqual("600|600|300|300", await node.SafePsqlAsync(Database,
                "SELECT count(*), count(DISTINCT id), count(*) FILTER (WHERE v = 'a'), count(*) FILTER (WHERE v = 'b') FROM t", token),
                node.Name);
        }

        // A root with no values left above node_b's can't start. The owner notes why and warns once, and takes in
        // node_b's report and request, which stay as they are, without writing, while node_b asks less often.
        int refused = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('narrow_id_seq', participants := '{node_b}')", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, """
            SELECT detail = 'sequence public.narrow_id_seq has no values left above 32767 for a root' FROM worf.sequence_ranges
            WHERE sequence = 'narrow_id_seq'::regclass
            """, token), "narrow_id_seq's root can't start");
        await AssertIdleAsync([nodeA, nodeB], true, token, TimeSpan.FromSeconds(6));
        Assert.AreEqual(1, nodeA.ReadLog()[refused..].Split("has no values left above 32767 for a root").Length - 1, "node_a warned once");

        // A table that comes to use the sequence may hold the other node's keys, which the other node has issued.
        _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE copied AS SELECT id, v FROM t WHERE v = 'a'", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT watermark >= (SELECT max(id) FROM copied) FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token), "node_a's grants heard of");
        _ = await nodeB.SafePsqlAsync(Database, "ALTER TABLE copied ALTER COLUMN id SET DEFAULT nextval('t_id_seq')", token);

        async Task InsertAsync(TestNode node, string value)
        {
            await using NpgsqlConnection connection = await node.OpenAsync(Database, token);
            for (int i = 0; i < 300; i++)
            {
                await using NpgsqlCommand insert = new($"INSERT INTO t (v) VALUES ('{value}')", connection);
                _ = await insert.ExecuteNonQueryAsync(token);
            }
        }
    }

    /// <summary>
    /// A node that gathers from two providers grants their blocks in its own calls. A grant reaches a provider only
    /// once the node committed it, so an exchange that fails after a call, as an injected error here does, loses no
    /// grant another node holds. A call that names a slot it doesn't stream, or another node's, or another role's that
    /// nothing streams, or that the role streams while it speaks for another node, is refused, and one that relays
    /// reports of more nodes than the provider keeps leaves the rest out. Once the providers subscribe back, every node
    /// exchanges both ways at once, with no deadlock or lock timeout. A provider whose host never answers holds up
    /// nothing else.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task GatherAndMeshExchangesNeverShareAKey()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        TestNode[] nodes = [nodeA, nodeB, nodeC];
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 50)", token);
        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        await SubscribeAsync(nodeA, "a_from_c", nodeC, token);
        await WaitActiveAsync(nodes, token);

        // A call is tied to the slot it names, which the calling role must stream, and to the node that first used it.
        string slot = await nodeB.SafePsqlAsync(Database, "SELECT slot_name FROM pg_replication_slots", token);
        string message = $$"""{"node_id": 1, "node_name": "intruder", "slot": "{{slot}}", "sysid": "1", "timeline": 1, "wal_lsn": "0/0", "sequences": []}""";
        foreach (string caller in new[] { "", $"SET SESSION AUTHORIZATION {SuperUser};" })
        {
            PostgresException refused = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
                $"{caller} SELECT worf.sequence_range_exchange('{message}')", token), caller);
            Assert.AreEqual("42501", refused.SqlState, caller);
        }

        // While nothing streams the slot, it stays bound to the role that streamed it, so that another subscriber's role,
        // with the grants an exchange needs, can't speak for node_a through it.
        string nodeAId = await nodeA.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        _ = await nodeB.SafePsqlAsync(Database,
            "CREATE ROLE c_user; GRANT USAGE ON SCHEMA worf TO c_user; GRANT EXECUTE ON FUNCTION worf.sequence_range_exchange(text) TO c_user", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('a_from_b')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT NOT active FROM pg_replication_slots", token), "a_from_b's slot idle");
        string forged = $$"""{"node_id": {{nodeAId}}, "node_name": "node_a", "slot": "{{slot}}", "sysid": "1", "timeline": 1, "wal_lsn": "0/0", "sequences": []}""";
        PostgresException impersonated = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
            $"SET SESSION AUTHORIZATION c_user; SELECT worf.sequence_range_exchange('{forged}')", token));
        Assert.AreEqual("42501", impersonated.SqlState);
        Assert.Contains("is bound to another role", impersonated.MessageText);

        // Nor can it by streaming the slot itself, as a role with REPLICATION can, while it speaks for node_c.
        string nodeCId = await nodeC.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        _ = await nodeB.SafePsqlAsync(Database, "ALTER ROLE c_user LOGIN REPLICATION", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_create_logical_replication_slot('c_link', 'worf')", token);
        _ = await nodeB.SafePsqlAsync(Database, $"INSERT INTO worf.sequence_range_link VALUES ('c_link', {nodeCId}, 'c_user'::regrole)", token);
        NpgsqlConnectionStringBuilder asCUser = new() { Host = "127.0.0.1", Port = nodeB.Cluster.Port, Database = Database, Username = "c_user" };
        await using (LogicalReplicationConnection replication = new(asCUser.ConnectionString))
        {
            await replication.Open(token);
            using CancellationTokenSource streaming = CancellationTokenSource.CreateLinkedTokenSource(token);
            await using IAsyncEnumerator<XLogDataMessage> messages = replication.StartLogicalReplication(
                new WorfReplicationSlot(slot), streaming.Token,
                options: [new("min_proto_version", "1"), new("max_proto_version", "1"), new("startup_params_format", "1")])
                .GetAsyncEnumerator(streaming.Token);
            Assert.IsTrue(await messages.MoveNextAsync(), "c_user streaming a_from_b's slot");
            PostgresException takeover = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
                $"SET SESSION AUTHORIZATION c_user; SELECT worf.sequence_range_exchange('{forged}')", token));
            Assert.AreEqual("42501", takeover.SqlState);
            Assert.Contains("already speaks for node", takeover.MessageText);
            await streaming.CancelAsync();
        }

        // A call relays reports of no more nodes than node_b keeps, and node_b goes on counting node_a and node_c as heard
        // from however many it is told of.
        string[] range = (await nodeB.SafePsqlAsync(Database,
            "SELECT epoch, range_size, max_range_size, watermark, root_start, root_end FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass",
            token)).Split('|');
        string madeUpAt = (DateTimeOffset.UtcNow.AddDays(1).ToUnixTimeMilliseconds() / 1000.0).ToString(CultureInfo.InvariantCulture);
        string madeUp = string.Join(", ", Enumerable.Range(1, 1100).Select(i =>
            $$"""{"node_id": {{1000000 + i}}, "node_name": "made_up_{{i}}", "epoch": {{range[0]}}, "state": "active", "reported_at": {{madeUpAt}}}"""));
        string flood = $$"""
            {"node_id": {{nodeAId}}, "node_name": "node_a", "slot": "{{slot}}", "sysid": "1", "timeline": 1, "wal_lsn": "0/0", "sequences": [
                {"name": "public.t_id_seq", "owner_id": {{nodeAId}}, "epoch": {{range[0]}}, "range_size": {{range[1]}}, "max_range_size": {{range[2]}},
                    "distance": 0, "high_water": null, "watermark": {{range[3]}}, "state": "active", "root_start": {{range[4]}}, "root_end": {{range[5]}},
                    "reset": null, "reset_done": null, "held_complete": false, "held": [], "requests": [], "reports": [{{madeUp}}], "grants": [], "acks": []}]}
            """;
        int flooded = nodeB.ReadLog().Length;
        Assert.AreEqual("t|t", await nodeB.SafePsqlAsync(Database, $"""
            SET SESSION AUTHORIZATION {SuperUser};
            SELECT worf.sequence_range_heard({nodeAId}, true, '-infinity', false) > now() - interval '1 minute',
                worf.sequence_range_heard({nodeCId}, false, '-infinity', false) > now() - interval '1 minute'
            FROM (SELECT worf.sequence_range_exchange('{flood}')) e
            """, token), "node_a and node_c heard from");
        Assert.AreEqual("1024", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass", token),
            "the nodes node_b keeps");
        Assert.IsTrue(await nodeB.PollLogAsync(flooded, "beyond the 1024 this node keeps; they are ignored", token), "the ignored reports");
        _ = await nodeB.SafePsqlAsync(Database, """
            DELETE FROM worf.sequence_range_node WHERE node_name LIKE 'made_up_%';
            SELECT pg_drop_replication_slot('c_link');
            """, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('a_from_b')", token);

        // node_a's calls fail once they have taken in the reply, while node_b uses up its blocks and asks for more.
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE FUNCTION fail_exchange() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected exchange fault'; END$$;
            CREATE TRIGGER fail_exchange AFTER UPDATE OF exchanged_at ON worf.sequence_range FOR EACH ROW EXECUTE FUNCTION fail_exchange();
            """, token);
        int logpos = nodeA.ReadLog().Length;
        Task inserting = InsertAsync(nodeB, 120, token);
        Assert.IsTrue(await nodeA.PollLogAsync(logpos, "injected exchange fault", token), "the injected fault");
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        _ = await nodeA.SafePsqlAsync(Database, "DROP TRIGGER fail_exchange ON worf.sequence_range", token);
        await inserting;

        // The providers subscribe back and to each other, and every node writes at once.
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeC, "c_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        await SubscribeAsync(nodeC, "c_from_b", nodeB, token);
        int[] logpositions = [.. nodes.Select(node => node.ReadLog().Length)];
        await Task.WhenAll(nodes.Select(node => InsertAsync(node, 400, token)));
        for (int i = 0; i < nodes.Length; i++)
        {
            string log = nodes[i].ReadLog()[logpositions[i]..];
            Assert.DoesNotContain("deadlock detected", log, nodes[i].Name);
            Assert.DoesNotContain("statement timeout", log, nodes[i].Name);
        }

        await AssertNoSharedValuesAsync(nodes, token);

        // A database that only subscribes asks its providers whether they manage any sequence from its exchange worker,
        // so a provider whose host never answers holds up neither its manager, which restarts the other subscription's
        // apply worker as usual, nor the other exchanges, which give up on it after worf.sequence_range_timeout.
        _ = await nodeA.SafePsqlAsync(Database, "CREATE DATABASE other", token);
        _ = await nodeA.SafePsqlAsync("other",
            $"CREATE EXTENSION worf; SELECT * FROM worf.create_node(node_name := 'node_x', dsn := '{nodeA.Dsn("other", SuperUser)}')", token);
        await SubscribeAsync(nodeA, "x_from_b", nodeB, token, "other");
        await SubscribeAsync(nodeA, "x_from_c", nodeC, token, "other");
        int unreachable = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync("other", """
            SELECT worf.alter_node_add_interface('node_c', 'unreachable', 'host=192.0.2.1 port=5432 dbname=postgres');
            SELECT worf.alter_subscription_interface('x_from_c', 'unreachable');
            """, token);
        Assert.IsTrue(await nodeA.PollLogAsync(unreachable, "exchange with node node_c failed: could not reach node node_c within 5000 ms", token),
            "the exchange gave up on the unreachable host");
        Stopwatch restarted = Stopwatch.StartNew();
        _ = await nodeA.SafePsqlAsync("other",
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'other' AND backend_type LIKE 'worf apply%'", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync("other",
            "SELECT status = 'replicating' FROM worf.show_subscription_status() WHERE subscription_name = 'x_from_b'", token), "x_from_b restarted");
        Assert.IsLessThan(30.0, restarted.Elapsed.TotalSeconds, "the manager restarted x_from_b without waiting on the unreachable host");
    }

    /// <summary>
    /// In a cascade, a node two links from the owner gets its blocks from its parent's pool block, and a node whose
    /// link to the owner is dropped finds the owner through another neighbor, which relays its reports without writing. A late joiner whose values reach into the
    /// root is refused by the pool holder it asks, one sequence's failure leaves the others' exchanges alone, and a
    /// pool holder's standby, promoted without the grants its primary made last, fences its pool until its children
    /// report, and takes the grants it lost back into its ledger.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task CascadedNodesDrawFromPoolBlocks()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        TestNode[] nodes = [nodeA, nodeB, nodeC];
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id bigserial PRIMARY KEY, v text);
                CREATE TABLE late (id bigserial PRIMARY KEY, v text);
                CREATE TABLE narrow (id smallserial PRIMARY KEY, v text);
                """, token);
        }

        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO narrow VALUES (32767, 'legacy')", token);
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO late (id, v) SELECT g, 'legacy' FROM generate_series(1, 5000) g", token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 100, max_range_size := 100)", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeC, "c_from_a", nodeA, token);
        await SubscribeAsync(nodeC, "c_from_b", nodeB, token);
        await WaitActiveAsync(nodes, token);
        string nodeBId = await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);

        // A root with no values left above node_b's fails to start, and the other sequence's exchanges go on.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('narrow_id_seq', participants := '{node_b}')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_ranges WHERE sequence = 'narrow_id_seq'::regclass)", token), "node_b joined");
        await InsertAsync(nodeB, 300, token);

        // Dropping node_c's link to the owner makes node_b its parent, whose pool block serves it.
        _ = await nodeC.SafePsqlAsync(Database, "SELECT worf.drop_subscription('c_from_a')", token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database,
            $"SELECT parent_node = {nodeBId} AND distance = 2 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token), "node_c's new parent");
        await InsertAsync(nodeC, 1500, token);
        Assert.AreNotEqual("0", await nodeB.SafePsqlAsync(Database,
            "SELECT count(*) FROM worf.sequence_range_block WHERE kind = 'p' AND seqoid = 't_id_seq'::regclass", token), "node_b's pool block");

        // node_b relays each end's reports to the other, once node_a no longer hears from node_c, and none of them writes
        // anything while nothing changes.
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database,
            "SELECT bool_and(via_node <> node_id) FROM worf.sequence_range_node WHERE node_name = 'node_c'", token), "node_c's reports relayed");
        await AssertIdleAsync(nodes, true, token);

        // node_b asks for another pool block once its pool blocks can't serve two more of node_c's last requests.
        string last = await nodeB.SafePsqlAsync(Database, """
            SELECT hi - lo + 1 FROM worf.sequence_range_grant
            WHERE seqoid = 't_id_seq'::regclass AND grantee_name = 'node_c' ORDER BY granted_at DESC LIMIT 1
            """, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"""
            SELECT (SELECT coalesce(sum(floor((f.hi::numeric - f.lo + 1) / {last})), 0)
                    FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
                    WHERE b.seqoid = 't_id_seq'::regclass AND b.kind = 'p' AND b.state = 'open') >= 2
                OR EXISTS (SELECT 1 FROM worf.sequence_range_request WHERE seqoid = 't_id_seq'::regclass AND kind = 'p' AND state = 'open')
            """, token), "node_b's pool ahead of node_c's requests");

        // node_c joins a sequence after its root started, with values that reach into it, and node_b refuses it. The root
        // starts at once, with no participants to wait for, so that node_c's report, relayed by node_b, can't reach the
        // owner first and have the root start above node_c's values.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('late_id_seq', participants := '{}', range_size := 100)", token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database,
            "SELECT state = 'refused' FROM worf.sequence_ranges WHERE sequence = 'late_id_seq'::regclass", token), "node_c refused");
        PostgresException quarantined = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeC.SafePsqlAsync(Database,
            "INSERT INTO late (v) VALUES ('new')", token));
        Assert.AreEqual("2200H", quarantined.SqlState);

        // node_b's standby stops at a copy, node_b grants node_c more from its pool and fails, and the standby is
        // promoted without those grants: it abandons its node blocks and fences its pool until node_c reports what it
        // holds, which brings the lost grants back into its ledger.
        using TemporaryDirectory copies = new($"worf-sequence-range-standby-{Guid.NewGuid():N}");
        string standby = Path.Join(copies.FullName, "node_b");
        await CopyDataAsync(nodeB, standby, token);
        const string GrantedToC = "SELECT max(hi) FROM worf.sequence_range_grant WHERE seqoid = 't_id_seq'::regclass AND grantee_name = 'node_c'";
        string copied = await nodeB.SafePsqlAsync(Database, GrantedToC, token);
        bool grantedAfterCopy = false;
        for (int i = 0; i < 30 && !grantedAfterCopy; i++)
        {
            await InsertAsync(nodeC, 100, token);
            grantedAfterCopy = long.Parse(await nodeB.SafePsqlAsync(Database, GrantedToC, token), CultureInfo.InvariantCulture) >
                long.Parse(copied, CultureInfo.InvariantCulture);
        }

        Assert.IsTrue(grantedAfterCopy, "node_b granting node_c a block after the copy");
        int logpos = nodeB.ReadLog().Length;
        await nodeB.StopAsync("immediate", token);
        await RestoreDataAsync(nodeB, standby, token, running: false, standby: true);

        // Until it is promoted, the standby refuses node_c's calls whole and reservations at once, and has no Snowflake
        // number of its own, while its view still reads.
        Assert.IsTrue(await nodeB.PollLogAsync(logpos, "cannot exchange sequence ranges during recovery", token), "the standby refused node_c");
        Assert.AreEqual("25006", (await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
            "SELECT worf.reserve_sequence_range('t_id_seq', 10, '5s')", token))).SqlState);
        Assert.AreEqual("", await nodeB.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token));
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database, "SELECT count(values_left) > 0 FROM worf.sequence_ranges", token));
        (int promoted, string output) = await nodeB.RunProgramAsync("pg_ctl", ["promote", "-w", "-D", nodeB.Cluster.DataDirectory], token);
        Assert.AreEqual(0, promoted, output);
        // node_b fences its pool when its range worker sees the new timeline, or first when node_c's report shows a
        // grant node_b has no record of.
        Assert.IsTrue(await nodeB.PollLogAsync(logpos, "so it grants nothing until every node it granted to reports", token), "node_b fenced its pool");
        Assert.IsTrue(await nodeB.PollLogAsync(logpos, "which this allocator had no record of; it is back in the ledger", token), "node_b's lost grant");
        Assert.IsTrue(await nodeB.PollLogAsync(logpos, "the allocator's fence lifted", token), "node_b's fence lifted");
        await WaitActiveAsync([nodeB], token);
        foreach (TestNode node in nodes)
        {
            await InsertAsync(node, 150, token);
        }

        await AssertNoSharedValuesAsync(nodes, token);
    }

    /// <summary>
    /// Restoring a node from a copy taken while it was stopped rolls it back on the same timeline. Its neighbor sees
    /// its WAL position move back and tells it to reset: a child drops the request it made before the copy, whose
    /// answer it already used, and asks again, and an owner that only answers calls fences its root until the child
    /// reports its blocks. The child knows how far it applied the owner's changes even after a restart, so the owner
    /// never issues again the values of rows the child copied from it. Recovering the owner to a point in time puts it on
    /// a new timeline, and it fences its root likewise. It drew from blocks it granted itself after that point only once
    /// the child had heard of them, so it never issues those values again either. Meanwhile a node keeps drawing from
    /// its blocks while the owner's host accepts connections and never answers them.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestoredNodesResetAndNeverShareAKey()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        TestNode[] nodes = [nodeA, nodeB];
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text); CREATE TABLE padding (id bigint PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT * FROM worf.replication_set_add_table('default', 't');
            SELECT * FROM worf.replication_set_add_table('default', 'padding');
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100);
            """, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await WaitActiveAsync(nodes, token);

        // With the owner's host taking connections and never answering, node_b's exchanges give up after
        // worf.sequence_range_timeout, and it draws from its blocks until its next block is active and its request waits.
        // Windows has no way to stop a process, so there the owner is shut down.
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT next_lo IS NOT NULL FROM worf.sequence_ranges", token), "node_b's next block");
        int hung = nodeB.ReadLog().Length;
        using TemporaryDirectory copies = new($"worf-sequence-range-copies-{Guid.NewGuid():N}");
        string copyB = Path.Join(copies.FullName, "node_b");
        try
        {
            if (OperatingSystem.IsWindows())
            {
                await nodeA.StopAsync("fast", token);
            }
            else
            {
                await SuspendAsync(nodeA, true, token);
            }

            await InsertAsync(nodeB, 150, token);
            Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_request WHERE state = 'open')", token),
                "node_b's request waiting");
            if (!OperatingSystem.IsWindows())
            {
                Assert.IsTrue(await nodeB.PollLogAsync(hung, "node node_a didn't answer within 5000 ms", token), "node_b's exchange gave up");
            }

            // node_b is copied, gets its request answered and uses the block, then goes back to the copy.
            await CopyDataAsync(nodeB, copyB, token);
        }
        finally
        {
            if (OperatingSystem.IsWindows())
            {
                await nodeA.StartServerAsync(token);
            }
            else
            {
                await SuspendAsync(nodeA, false, CancellationToken.None);
            }
        }

        // node_b writes a few megabytes first, so that its calls show a WAL position the restored copy, which writes as it
        // starts, stays below.
        _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE written AS SELECT g, repeat('x', 100) AS v FROM generate_series(1, 20000) g", token);
        await InsertAsync(nodeB, 150, token);
        string granted = await nodeA.SafePsqlAsync(Database, "SELECT max(hi) FROM worf.sequence_range_grant WHERE grantee_name = 'node_b'", token);
        await RestoreDataAsync(nodeB, copyB, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            $"SELECT state = 'active' AND active_lo > {granted} FROM worf.sequence_ranges", token), "node_b drawing from a block granted after the rollback");
        await InsertAsync(nodeB, 50, token);

        // node_a is copied and draws values from its block that node_b copies, with changes that take its WAL position
        // past where its copy gets to as it starts, though node_b's tables, which a restart of node_b leaves it alone with,
        // recorded it before the copy. node_a goes back to the copy, and node_b, which applied node_a's changes to beyond
        // node_a's WAL position, tells it to reset, so node_a never issues those values again.
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT next_lo IS NOT NULL AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_request) FROM worf.sequence_ranges", token), "node_b settled");
        string copyBehind = Path.Join(copies.FullName, "node_a_behind");
        await CopyDataAsync(nodeA, copyBehind, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            INSERT INTO t (v) SELECT 'node_a' FROM generate_series(1, 10);
            INSERT INTO padding SELECT g, repeat(md5(g::text), 4) FROM generate_series(1, 2000) g;
            """, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        HashSet<string> drawnBehind = [.. (await nodeB.SafePsqlAsync(Database, "SELECT id FROM t WHERE v = 'node_a'", token)).Split('\n')];
        await nodeB.StopAsync("fast", token);
        int behind = nodeA.ReadLog().Length;
        await RestoreDataAsync(nodeA, copyBehind, token);
        HashSet<string> keptBehind = [.. (await nodeA.SafePsqlAsync(Database, "SELECT id FROM t", token)).Split('\n')];
        await nodeB.StartServerAsync(token);
        Assert.IsTrue(await nodeA.PollLogAsync(behind, "saw this node's WAL position move backwards", token), "node_a reset");
        await WaitActiveAsync(nodes, token);
        await InsertAsync(nodeA, 20, token);
        foreach (string id in (await nodeA.SafePsqlAsync(Database, "SELECT id FROM t", token)).Split('\n').Where(id => !keptBehind.Contains(id)))
        {
            Assert.DoesNotContain(id, drawnBehind, $"node_a issued {id} again, which node_b copied from it before the restore");
        }

        // node_a writes past where node_b applied its changes to, so that node_b's subscription streams them again.
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO padding SELECT g, repeat(md5(g::text), 4) FROM generate_series(10001, 14000) g", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);

        // node_a, which only answers node_b's calls, is copied, grants node_b more, and is recovered from the copy to a
        // point after that. node_b may draw hundreds of values from the blocks it already holds, so it inserts until
        // node_a grants it one.
        _ = await nodeA.SafePsqlAsync(Database, "ALTER SYSTEM SET wal_keep_size = '1GB'; SELECT pg_reload_conf()", token);
        string copyA = Path.Join(copies.FullName, "node_a");
        await CopyDataAsync(nodeA, copyA, token);
        string copied = await nodeA.SafePsqlAsync(Database, "SELECT max(hi) FROM worf.sequence_range_grant WHERE grantee_name = 'node_b'", token);
        bool grantedAfterCopy = false;
        for (int i = 0; i < 30 && !grantedAfterCopy; i++)
        {
            await InsertAsync(nodeB, 100, token);
            grantedAfterCopy = await nodeA.SafePsqlAsync(Database,
                $"SELECT max(hi) > {copied} FROM worf.sequence_range_grant WHERE grantee_name = 'node_b'", token) == "t";
        }

        Assert.IsTrue(grantedAfterCopy, "node_a granting node_b a block after the copy");
        string target = await nodeA.SafePsqlAsync(Database, "SELECT pg_current_wal_lsn()", token);

        // node_a also draws from blocks it granted itself after that point, whose rows node_b copies, but only once node_b
        // has heard of them: while node_b, still streaming node_a's rows, can't exchange with it, they wait, even after
        // node_b's last exchange has gone stale. Then node_a goes down at once.
        const string Unheard = "SELECT count(*) FROM t WHERE v = 'node_a' AND id > (SELECT watermark FROM worf.sequence_range)";
        _ = await nodeA.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange(text) RENAME TO sequence_range_exchange_paused", token);
        Task inserting = InsertAsync(nodeA, 600, token);
        await Task.Delay(TimeSpan.FromSeconds(8), token);
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, Unheard, token), "node_a issued values node_b hadn't heard of");
        _ = await nodeA.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange_paused(text) RENAME TO sequence_range_exchange", token);
        await inserting;
        await nodeA.StopAsync("immediate", token);
        HashSet<string> copiedFromA = [.. (await nodeB.SafePsqlAsync(Database, "SELECT id FROM t WHERE v = 'node_a'", token)).Split('\n')];
        int logpos = nodeA.ReadLog().Length;
        await RestoreDataAsync(nodeA, copyA, token, running: false, target: target);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT NOT pg_is_in_recovery()", token), "node_a recovered");
        Assert.AreEqual("00000002", await nodeA.SafePsqlAsync(Database, "SELECT substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8)", token),
            "node_a's new timeline");
        HashSet<string> restored = [.. (await nodeA.SafePsqlAsync(Database, "SELECT id FROM t", token)).Split('\n')];

        // node_a fences its root when its range worker sees the new timeline, or first when node_b, which applied its
        // changes beyond its WAL position, tells it to reset.
        Assert.IsTrue(await nodeA.PollLogAsync(logpos, "so it grants nothing until every node it granted to reports", token), "node_a fenced its root");
        Assert.IsTrue(await nodeA.PollLogAsync(logpos, "the allocator's fence lifted", token), "node_a's fence lifted");
        await WaitActiveAsync(nodes, token);
        foreach (TestNode node in nodes)
        {
            await InsertAsync(node, 300, token);
        }

        await AssertNoSharedValuesAsync(nodes, token);
        foreach (string id in (await nodeA.SafePsqlAsync(Database, "SELECT id FROM t", token)).Split('\n').Where(id => !restored.Contains(id)))
        {
            Assert.DoesNotContain(id, copiedFromA, $"node_a issued {id} again, which node_b copied from it before the recovery");
        }
    }

    /// <summary>
    /// Moving the owner starts a new epoch, whose root starts above every block of the old one, while each node keeps
    /// drawing from the blocks it holds until the new epoch's arrive.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task MovingTheOwnerStartsAnEpochAboveTheOldBlocks()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        TestNode[] nodes = [nodeA, nodeB];
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges", token),
                $"{node.Name} drawing from a block");
            _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'old' FROM generate_series(1, 5)", token);
        }

        // node_b can't take over while node_a's epoch is live.
        PostgresException live = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_a}', range_size := 100)", token));
        Assert.AreEqual("55006", live.SqlState);

        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.disable_sequence_range('t_id_seq')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'disabled' FROM worf.sequence_ranges", token), "the disable reached node_b");
        Assert.AreEqual("6", await nodeB.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('disabled') RETURNING (SELECT count(*) FROM t) + 1", token));

        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_a}', range_size := 100)", token);
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT state = 'active' AND epoch = 2 FROM worf.sequence_ranges", token),
                $"{node.Name} drawing from the new epoch");

            Assert.AreEqual("node_b", await node.SafePsqlAsync(Database, "SELECT owner FROM worf.sequence_ranges", token));
        }

        long oldEnd = long.MinValue;
        HashSet<long> keys = [];
        foreach (TestNode node in nodes)
        {
            oldEnd = Math.Max(oldEnd, long.Parse(await node.SafePsqlAsync(Database, """
                SELECT greatest(
                    (SELECT max(hi) FROM worf.sequence_range_block WHERE epoch = 1 AND kind = 'n'),
                    (SELECT max(g.hi) FROM worf.sequence_range_grant g JOIN worf.sequence_range_block b ON b.block_id = g.from_block WHERE b.epoch = 1))
                """, token), CultureInfo.InvariantCulture));
            _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'new' FROM generate_series(1, 5)", token);
            foreach (string id in (await node.SafePsqlAsync(Database, "SELECT id FROM t", token)).Split('\n'))
            {
                Assert.IsTrue(keys.Add(long.Parse(id, CultureInfo.InvariantCulture)), $"{node.Name}'s key {id} is another node's too");
            }
        }

        Assert.IsGreaterThan(oldEnd, long.Parse(await nodeB.SafePsqlAsync(Database,
            "SELECT lo FROM worf.sequence_range_block WHERE kind = 'r' AND epoch = 2", token), CultureInfo.InvariantCulture),
            "the new root starts above the old epoch's blocks");
    }

    /// <summary>
    /// A bulk load reserves a block that holds all its rows, and rows that already carry keys reserve exactly those keys,
    /// on the owner and on a node that asks the owner, which refuses keys that may be in use. An owner that hears of
    /// grants beyond its watermark resets, and a node waiting on its fence soon hears of it lifting.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ReservationsServeBulkLoads()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        TestNode[] nodes = [nodeA, nodeB];
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges", token),
                $"{node.Name} drawing from a block");

            // A load larger than a block fails at the block's end, and succeeds once reserved.
            PostgresException shortBlock = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
                "INSERT INTO t (v) SELECT 'bulk' FROM generate_series(1, 1000)", token));
            Assert.AreEqual("2200H", shortBlock.SqlState);
            _ = await node.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_range('t_id_seq', 1000)", token);

            _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'bulk' FROM generate_series(1, 1000)", token);
        }

        // Keys that rows carry: free ones are granted exactly, and ones another node holds are refused.
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_values('t_id_seq', 1000000, 1000099)", token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t (id, v) SELECT g, 'imported' FROM generate_series(1000000, 1000099) g", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_values('t_id_seq', 2000000, 2000009)", token);
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t (id, v) SELECT g, 'imported' FROM generate_series(2000000, 2000009) g", token);
        string held = await nodeA.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges", token);
        PostgresException refused = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
            $"SELECT worf.reserve_sequence_values('t_id_seq', {held}, {held})", token));
        Assert.AreEqual("22023", refused.SqlState);

        HashSet<long> keys = [];
        foreach (TestNode node in nodes)
        {
            foreach (string id in (await node.SafePsqlAsync(Database, "SELECT id FROM t", token)).Split('\n'))
            {
                Assert.IsTrue(keys.Add(long.Parse(id, CultureInfo.InvariantCulture)), $"{node.Name}'s key {id} is another node's too");
            }
        }

        Assert.HasCount(2110, keys);

        // node_b hears of grants beyond node_a's watermark, as when node_a, the owner, went back to before them by less
        // than its WAL position shows. node_a resets, and its fence holds, here on a participant that never reports, while
        // node_b asks for a block. With an exchange interval of ten minutes, node_b still asks node_a every ten checks,
        // and draws from a block beyond that watermark soon after the fence lifts.
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_exchange_interval = '10min'; SELECT pg_reload_conf()", token);
        _ = await nodeA.SafePsqlAsync(Database, "UPDATE worf.sequence_range SET participants = '{node_b,ghost}'", token);
        string beyond = await nodeB.SafePsqlAsync(Database, "UPDATE worf.sequence_range SET watermark = watermark + 1000000 RETURNING watermark", token);
        int logpos = nodeA.ReadLog().Length;
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('t_id_seq')", token);
        Assert.IsTrue(await nodeA.PollLogAsync(logpos, $"has heard of grants up to {beyond}, beyond this owner's watermark", token), "node_a reset");
        await Task.Delay(TimeSpan.FromSeconds(4), token);

        // The fence lifts just after one of node_b's calls.
        await using NpgsqlConnection sessions = await nodeA.OpenAsync(Database, token);
        await using (NpgsqlCommand fresh = new("SET stats_fetch_consistency = none", sessions))
        {
            _ = await fresh.ExecuteNonQueryAsync(token);
        }

        await using NpgsqlCommand count = new("SELECT sessions FROM pg_stat_database WHERE datname = current_database()", sessions);
        long called = (long)(await count.ExecuteScalarAsync(token))!;
        Stopwatch asked = Stopwatch.StartNew();
        while ((long)(await count.ExecuteScalarAsync(token))! == called && asked.Elapsed < TestNode.PollTimeout)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(20), token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "UPDATE worf.sequence_range SET participants = '{node_b}'", token);
        Stopwatch lifted = Stopwatch.StartNew();
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"SELECT state = 'active' AND active_lo > {beyond} FROM worf.sequence_ranges", token),
            "node_b drawing from a block beyond the watermark it heard of");
        Assert.IsLessThan(3.0, lifted.Elapsed.TotalSeconds, "node_b's block soon after node_a's fence lifted");
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT state = 'active' AND active_lo > {beyond} FROM worf.sequence_ranges", token),
            "node_a drawing from a block beyond the watermark");
    }

    /// <summary>
    /// A subscriber whose structure comes from the provider gets the provider's managed sequences quarantined as they
    /// are created and joins them before rows are copied, so the copied rows don't count against it, and then draws from
    /// blocks of its own.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task StructureSyncJoinsBeforeCopying()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE TABLE t (id bigserial PRIMARY KEY, v text);
            CREATE TABLE u (id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v text);
            SELECT * FROM worf.replication_set_add_table('default', 't');
            SELECT * FROM worf.replication_set_add_table('default', 'u');
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{}', range_size := 100);
            SELECT worf.enable_sequence_range('u_id_seq', participants := '{}', range_size := 100);
            INSERT INTO t (v) SELECT 'a' FROM generate_series(1, 20);
            INSERT INTO u (v) SELECT 'a' FROM generate_series(1, 20);
            """, token);

        _ = await nodeB.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'b_from_a',
                provider_dsn := '{nodeA.Dsn(Database, SuperUser)}',
                synchronize_structure := true,
                synchronize_data := true,
                forward_origins := '{"{}"}')
            """, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, """
            SELECT count(*) = 2 AND bool_and(state = 'active') FROM worf.sequence_ranges
            """, token), "node_b drawing from blocks of both sequences");


        Assert.AreEqual("20|20|2|0", await nodeB.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM t), (SELECT count(*) FROM u),
                (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgname = 'worf_sequence_range_guard'),
                (SELECT count(high_water) FROM worf.sequence_range)
            """, token), "rows copied, one guard per table, and no high-water mark from the copied rows");

        HashSet<long> keys = [];
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, $"INSERT INTO t (v) SELECT '{node.Name}' FROM generate_series(1, 10)", token);
            _ = await node.SafePsqlAsync(Database, $"INSERT INTO u (v) SELECT '{node.Name}' FROM generate_series(1, 10)", token);
        }

        foreach (string table in new[] { "t", "u" })
        {
            keys.Clear();
            foreach (TestNode node in new[] { nodeA, nodeB })
            {
                foreach (string id in (await node.SafePsqlAsync(Database, $"SELECT id FROM {table} WHERE v = '{node.Name}'", token)).Split('\n'))
                {
                    Assert.IsTrue(keys.Add(long.Parse(id, CultureInfo.InvariantCulture)), $"{node.Name}'s key {id} in {table} is another node's too");
                }
            }
        }
    }

    /// <summary>
    /// Snowflake IDs carry each node's own number, from the owner of the numbers, so they never collide, and each
    /// node's IDs only grow, across a restart too, which an idle node makes with its number while the owner is down. A
    /// reset of every sequence leaves the node its number. With only three
    /// numbers, the owner moves back and forth without running out: the numbers below a new root's start that no node
    /// holds come back, and a node that gives up its number takes one of them. A number two nodes hold is given up.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SnowflakeIdsAreUniqueAndOnlyGrow()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        TestNode[] nodes = [nodeA, nodeB];
        _ = await nodeA.SafePsqlAsync(Database, "ALTER SEQUENCE worf.snowflake_node MAXVALUE 2; SELECT worf.enable_snowflake('{node_b}')", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);

        HashSet<long> ids = [];
        Dictionary<TestNode, string> numbers = [];
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT worf.snowflake_node() IS NOT NULL", token), $"{node.Name}'s node number");
            numbers[node] = await node.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token);
        }

        // node_b keeps when it last heard from node_a, the owner of the numbers, recorded within an hour, though it issues
        // no IDs, so that it keeps its number and issues IDs after a restart while node_a is down. node_a's report stays
        // as it is meanwhile, since the time its clock is reserved to moves only while it issues IDs.
        string ownerId = await nodeA.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        string ownerRow = $"seqoid = 'worf.snowflake_node'::regclass AND node_id = {ownerId}";
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"""
            SELECT n.watermark = r.watermark AND n.holds = ARRAY[{numbers[nodeA]}]::bigint[]
            FROM worf.sequence_range_node n JOIN worf.sequence_range r ON r.seqoid = n.seqoid WHERE n.{ownerRow}
            """, token), "node_b heard node_a's last report");
        _ = await nodeB.SafePsqlAsync(Database, $"UPDATE worf.sequence_range_node SET reported_at = now() - interval '13 hours' WHERE {ownerRow}", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"SELECT reported_at > now() - interval '1 hour' FROM worf.sequence_range_node WHERE {ownerRow}", token),
            "node_b recorded hearing from node_a");
        await nodeA.StopAsync("fast", token);
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        Assert.AreEqual($"{numbers[nodeB]}|t", await nodeB.SafePsqlAsync(Database, "SELECT worf.snowflake_node(), worf.snowflake_id() IS NOT NULL", token),
            "node_b's node number and an ID after a restart");
        await nodeA.StartServerAsync(token);

        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE s (n serial PRIMARY KEY, id bigint NOT NULL DEFAULT worf.snowflake_id());
                CREATE ROLE app;
                GRANT INSERT ON s TO app;
                GRANT USAGE ON SEQUENCE s_n_seq TO app;
                SET ROLE app;
                INSERT INTO s DEFAULT VALUES;
                RESET ROLE;
                INSERT INTO s (n) SELECT g FROM generate_series(2, 5000) g;
                """, token);
        }

        Assert.AreNotEqual(numbers[nodeA], numbers[nodeB]);

        // A reset without a sequence named keeps the node's number, whose IDs carry their times.
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range()", token);
        Assert.AreEqual(numbers[nodeB], await nodeB.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token));

        // A restart keeps the clock where it was.
        await nodeA.StopAsync("fast", token);
        await nodeA.StartServerAsync(token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT worf.snowflake_node() IS NOT NULL", token), "node_a's node number after a restart");
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO s (n) SELECT g FROM generate_series(5001, 5100) g", token);

        foreach (TestNode node in nodes)
        {
            Assert.AreEqual($"t|t|{numbers[node]}", await node.SafePsqlAsync(Database, """
                SELECT bool_and(id > prev),
                    bool_and(timestamptz '2025-01-01 00:00:00+00' + (id >> 22) * interval '1 millisecond' BETWEEN now() - interval '5 minutes' AND now() + interval '1 minute'),
                    min((id >> 12) & 1023)
                FROM (SELECT id, lag(id) OVER (ORDER BY n) AS prev FROM s) ids WHERE prev IS NOT NULL
                """, token), $"{node.Name}'s IDs grow, carry the time and its number");
            foreach (string id in (await node.SafePsqlAsync(Database, "SELECT id FROM s", token)).Split('\n'))
            {
                Assert.IsTrue(ids.Add(long.Parse(id, CultureInfo.InvariantCulture)), $"{node.Name}'s ID {id} is another node's too");
            }
        }

        // The owner moves three times. Each new root starts above every number held, and soon none is left above.
        (TestNode owner, TestNode other) = (nodeA, nodeB);
        for (int epoch = 2; epoch <= 4; epoch++)
        {
            _ = await owner.SafePsqlAsync(Database, "SELECT worf.disable_sequence_range('worf.snowflake_node')", token);
            Assert.IsTrue(await other.PollQueryUntilAsync(Database,
                "SELECT state = 'disabled' FROM worf.sequence_ranges WHERE sequence = 'worf.snowflake_node'::regclass", token), $"epoch {epoch - 1} disabled");
            _ = await other.SafePsqlAsync(Database, $"SELECT worf.enable_snowflake('{{{owner.Name}}}')", token);
            (owner, other) = (other, owner);
            foreach (TestNode node in nodes)
            {
                Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"""
                    SELECT state = 'active' AND epoch = {epoch} AND worf.snowflake_node() IS NOT NULL
                    FROM worf.sequence_ranges WHERE sequence = 'worf.snowflake_node'::regclass
                    """, token), $"{node.Name} in epoch {epoch}");
            }

            Assert.AreNotEqual(await owner.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token),
                await other.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token), $"the numbers in epoch {epoch}");
        }

        // The number the first move retired comes back below the root's start, and a node that gives up its own takes it.
        Assert.IsTrue(await owner.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
                    JOIN worf.sequence_range r ON r.seqoid = b.seqoid AND r.epoch = b.epoch
                WHERE b.seqoid = 'worf.snowflake_node'::regclass AND b.kind = 'r' AND f.lo < r.root_start)
            """, token), "a number back in the root");
        string given = await other.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token);
        _ = await other.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('worf.snowflake_node')", token);
        Assert.IsTrue(await other.PollQueryUntilAsync(Database,
            $"SELECT worf.snowflake_node() IS NOT NULL AND worf.snowflake_node() <> {given}", token), $"{other.Name}'s new number");
        Assert.AreNotEqual(await owner.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token),
            await other.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token), "the numbers after the reset");
        foreach (TestNode node in nodes)
        {
            Assert.AreEqual("t", await node.SafePsqlAsync(Database, "SELECT worf.snowflake_node() BETWEEN 0 AND 2", token), $"{node.Name}'s number");
            _ = await node.SafePsqlAsync(Database, "INSERT INTO s (n) SELECT g FROM generate_series(10001, 10100) g", token);
            foreach (string id in (await node.SafePsqlAsync(Database, "SELECT id FROM s WHERE n > 10000", token)).Split('\n'))
            {
                Assert.IsTrue(ids.Add(long.Parse(id, CultureInfo.InvariantCulture)), $"{node.Name}'s ID {id} is another node's too");
            }
        }

        // A number that two nodes hold, as one given out again too soon would be, is given up.
        int logpos = owner.ReadLog().Length;
        string shared = await owner.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token);
        _ = await other.SafePsqlAsync(Database, $"""
            UPDATE worf.sequence_range_block SET lo = {shared}, hi = {shared}
            WHERE seqoid = 'worf.snowflake_node'::regclass AND kind = 'n' AND state = 'active'
            """, token);
        Assert.IsTrue(await owner.PollLogAsync(logpos, $"Snowflake node number {shared}: node {other.Name} reports holding it too", token), "the number given up");
    }

    private static async Task SubscribeAsync(TestNode subscriber, string name, TestNode provider, CancellationToken token,
        string database = Database)
    {
        _ = await subscriber.SafePsqlAsync(database, $$"""
            SELECT worf.create_subscription(
                subscription_name := '{{name}}',
                provider_dsn := '{{provider.Dsn(Database, SuperUser)}}',
                synchronize_structure := false,
                synchronize_data := false,
                forward_origins := '{}')
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(database, $"""
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = '{name}' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");
    }

    /// <summary>
    /// Waits for the nodes to settle, all at once, and then checks that none uses a transaction ID for a while, two seconds unless
    /// given, which span twenty checks and four exchanges, nor takes more than one call an exchange interval through each
    /// slot on it. A node has settled once the first call through each slot on it has bound the slot, and, with
    /// sequences managed, once nothing is requested or granted for a sequence whose root started, each such sequence
    /// but the Snowflake node numbers has a next block ready, and every node it has heard of reports the watermark it
    /// has.
    /// </summary>
    private static async Task AssertIdleAsync(TestNode[] nodes, bool managed, CancellationToken token, TimeSpan? span = null)
    {
        // A node that settles may unsettle one that had, as a request it answers leads to one of the neighbor's, so the
        // nodes are checked until every one of them has settled at once.
        string settled = $"""
            SELECT NOT EXISTS (
                    SELECT 1 FROM pg_replication_slots s
                    WHERE s.plugin = 'worf' AND s.database = current_database()
                        AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_link l WHERE l.slot_name = s.slot_name))
                AND (NOT {managed} OR (
                    WITH started AS (
                        SELECT r.seqoid, r.watermark FROM worf.sequence_range r
                        WHERE r.state <> 'collecting' AND NOT EXISTS (
                            SELECT 1 FROM worf.sequence_range_node o CROSS JOIN worf.local_node l
                            WHERE o.seqoid = r.seqoid AND o.node_id = r.owner_node AND o.node_id <> l.node_id AND o.state = 'collecting'))
                    SELECT NOT EXISTS (SELECT 1 FROM worf.sequence_range_request q JOIN started r ON r.seqoid = q.seqoid)
                        AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_grant WHERE state = 'sent')
                        AND coalesce((SELECT bool_and(s.next_lo IS NOT NULL) FROM worf.sequence_ranges s JOIN started r ON r.seqoid = s.sequence
                            WHERE s.sequence <> 'worf.snowflake_node'::regclass), true)
                        AND NOT EXISTS (
                            SELECT 1 FROM started r JOIN worf.sequence_range_node n ON n.seqoid = r.seqoid CROSS JOIN worf.local_node l
                            WHERE n.node_id <> l.node_id AND n.watermark IS DISTINCT FROM r.watermark)))
            """;
        bool waited = true;
        while (waited)
        {
            string[] states = await Task.WhenAll(nodes.Select(node => node.SafePsqlAsync(Database, settled, token)));
            waited = false;
            foreach (TestNode node in nodes.Where((_, i) => states[i] != "t"))
            {
                waited = true;
                Assert.IsTrue(await node.PollQueryUntilAsync(Database, settled, token), $"{node.Name} settled");
            }
        }

        // Each exchange call is a session of its own. A slot takes no more than a call an exchange interval, and one where
        // each end of the while cuts an interval, and these queries take two sessions.
        TimeSpan idle = span ?? TimeSpan.FromSeconds(2);
        long calls = (long)(idle / TimeSpan.FromMilliseconds(500)) + 1;
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        const string Next = """
            SELECT pg_snapshot_xmax(pg_current_snapshot()), (SELECT sessions FROM pg_stat_database WHERE datname = current_database()),
                (SELECT count(*) FROM pg_replication_slots WHERE plugin = 'worf' AND database = current_database())
            """;
        string[][] before = await Task.WhenAll(nodes.Select(async node => (await node.SafePsqlAsync(Database, Next, token)).Split('|')));
        await Task.Delay(idle, token);
        string[][] after = await Task.WhenAll(nodes.Select(async node => (await node.SafePsqlAsync(Database, Next, token)).Split('|')));
        for (int i = 0; i < nodes.Length; i++)
        {
            Assert.AreEqual(before[i][0], after[i][0], $"{nodes[i].Name} used transaction IDs while idle");
            Assert.IsLessThanOrEqualTo((calls * long.Parse(after[i][2], CultureInfo.InvariantCulture)) + 2,
                long.Parse(after[i][1], CultureInfo.InvariantCulture) - long.Parse(before[i][1], CultureInfo.InvariantCulture), $"{nodes[i].Name}'s exchange calls");
        }
    }

    /// <summary>
    /// Stops or resumes every process of a node's server, as a host that accepts connections and never answers them.
    /// </summary>
    private static async Task SuspendAsync(TestNode node, bool suspend, CancellationToken token)
    {
        // The postmaster stops first, so that it starts no process the others miss, and resumes last. A child may exit
        // before its signal.
        string postmaster = File.ReadLines(Path.Join(node.Cluster.DataDirectory, "postmaster.pid")).First().Trim();
        if (suspend)
        {
            await SignalAsync("-STOP", [postmaster], true);
        }

        (_, string children, _) = await ProcessRunner.RunAsync("pgrep", ["-P", postmaster], token);
        await SignalAsync(suspend ? "-STOP" : "-CONT", children.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries), false);
        if (!suspend)
        {
            await SignalAsync("-CONT", [postmaster], true);
        }

        async Task SignalAsync(string signal, string[] pids, bool required)
        {
            foreach (string pid in pids)
            {
                (int exitCode, _, string error) = await ProcessRunner.RunAsync("kill", [signal, pid], token);
                Assert.IsTrue(exitCode == 0 || !required, error);
            }
        }
    }

    private static async Task WaitActiveAsync(IEnumerable<TestNode> nodes, CancellationToken token)
    {
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database,
                "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token), $"{node.Name} drawing from a block");
        }
    }

    /// <summary>
    /// Inserts rows into <c>t</c> ten to a statement, each marked with the node's name, waiting out the moments a node
    /// has used its block and has no next one yet.
    /// </summary>
    private static async Task InsertAsync(TestNode node, int count, CancellationToken token)
    {
        await using NpgsqlConnection connection = await node.OpenAsync(Database, token);
        Stopwatch waited = Stopwatch.StartNew();
        for (int i = 0; i < count;)
        {
            try
            {
                int rows = Math.Min(10, count - i);
                await using NpgsqlCommand insert = new($"INSERT INTO t (v) SELECT '{node.Name}' FROM generate_series(1, {rows})", connection);
                _ = await insert.ExecuteNonQueryAsync(token);
                i += rows;
                waited.Restart();
            }
            catch (PostgresException error) when (error.SqlState == "2200H" && waited.Elapsed < TestNode.PollTimeout)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(50), token);
            }
        }
    }

    /// <summary>
    /// Checks that no two blocks of <c>t</c>'s sequence that any of the nodes holds, or held, share a value, and that
    /// no two nodes wrote the same key.
    /// </summary>
    private static async Task AssertNoSharedValuesAsync(TestNode[] nodes, CancellationToken token)
    {
        List<(string Node, long Lo, long Hi)> blocks = [];
        HashSet<long> keys = [];
        foreach (TestNode node in nodes)
        {
            string held = await node.SafePsqlAsync(Database,
                "SELECT lo, hi FROM worf.sequence_range_block WHERE seqoid = 't_id_seq'::regclass AND kind IN ('n', 'i')", token);
            foreach (string[] bounds in held.Split('\n').Select(block => block.Split('|')))
            {
                blocks.Add((node.Name, long.Parse(bounds[0], CultureInfo.InvariantCulture), long.Parse(bounds[1], CultureInfo.InvariantCulture)));
            }

            string ids = await node.SafePsqlAsync(Database, $"SELECT id FROM t WHERE v = '{node.Name}'", token);
            foreach (string id in ids.Split('\n', StringSplitOptions.RemoveEmptyEntries))
            {
                Assert.IsTrue(keys.Add(long.Parse(id, CultureInfo.InvariantCulture)), $"{node.Name}'s key {id} is another node's too");
            }
        }

        for (int i = 0; i < blocks.Count; i++)
        {
            for (int j = i + 1; j < blocks.Count; j++)
            {
                Assert.IsFalse(blocks[i].Lo <= blocks[j].Hi && blocks[j].Lo <= blocks[i].Hi,
                    $"{blocks[i].Node}'s block [{blocks[i].Lo}, {blocks[i].Hi}] and {blocks[j].Node}'s [{blocks[j].Lo}, {blocks[j].Hi}] overlap");
            }
        }
    }

    /// <summary>
    /// Copies a node's data directory while the node is stopped, as a cold backup or snapshot is taken.
    /// </summary>
    private static async Task CopyDataAsync(TestNode node, string target, CancellationToken token)
    {
        await node.StopAsync("fast", token);
        CopyDirectory(node.Cluster.DataDirectory, target);
        await node.StartServerAsync(token);
    }

    /// <summary>
    /// Puts a copy of a node's data directory back in place, as a restored backup or snapshot does, or recovers the copy
    /// to a point in the WAL the node wrote since, as point-in-time recovery from a base backup and its archived WAL does,
    /// which promotes it onto a new timeline.
    /// </summary>
    private static async Task RestoreDataAsync(TestNode node, string source, CancellationToken token, bool running = true, bool standby = false,
        string? target = null)
    {
        if (running)
        {
            await node.StopAsync("fast", token);
        }

        DirectoryInfo data = new(node.Cluster.DataDirectory);
        string wal = source + "_wal";
        if (target is not null)
        {
            CopyDirectory(Path.Join(data.FullName, "pg_wal"), wal);
        }

        foreach (FileSystemInfo entry in data.EnumerateFileSystemInfos())
        {
            if (entry is DirectoryInfo directory)
            {
                directory.Delete(recursive: true);
            }
            else
            {
                entry.Delete();
            }
        }

        CopyDirectory(source, data.FullName);
        if (standby)
        {
            await File.WriteAllTextAsync(Path.Join(data.FullName, "standby.signal"), "", token);
        }

        if (target is not null)
        {
            foreach (string segment in Directory.EnumerateFiles(wal))
            {
                File.Copy(segment, Path.Join(data.FullName, "pg_wal", Path.GetFileName(segment)), overwrite: true);
            }

            await File.WriteAllTextAsync(Path.Join(data.FullName, "recovery.signal"), "", token);
            await File.AppendAllTextAsync(Path.Join(data.FullName, "postgresql.auto.conf"),
                $"restore_command = 'exit 1'\nrecovery_target_lsn = '{target}'\nrecovery_target_action = 'promote'\n", token);
        }

        await node.StartServerAsync(token);
    }

    private static void CopyDirectory(string source, string target)
    {
        _ = Directory.CreateDirectory(target);
        foreach (string file in Directory.EnumerateFiles(source))
        {
            File.Copy(file, Path.Join(target, Path.GetFileName(file)));
        }

        foreach (string directory in Directory.EnumerateDirectories(source))
        {
            CopyDirectory(directory, Path.Join(target, Path.GetFileName(directory)));
        }
    }

    private static string Next(string value)
        => (long.Parse(value, CultureInfo.InvariantCulture) + 1).ToString(CultureInfo.InvariantCulture);

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "worf.sequence_range_check_interval = 100",
            "worf.sequence_range_exchange_interval = 500",
            "worf.sequence_range_timeout = 5000",
            "max_worker_processes = 16",
            "autovacuum = off",
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
