using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net.Sockets;
using System.Text;
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

        // The range worker, once the manager has started it, switches as the block runs low.
        Assert.IsTrue(await node.PollLogAsync(0, "sequence range worker managing sequences", token), "the range worker started");
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

        // A session that reaches the last twentieth of the active block wakes the range worker, which switches then
        // rather than at its next check.
        _ = await node.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_check_interval = '10min'", token);
        _ = await node.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        await Task.Delay(500, token);
        long[] block = [.. (await node.SafePsqlAsync(Database, "SELECT active_lo, active_hi, values_left FROM worf.sequence_ranges", token))
            .Split('|').Select(value => long.Parse(value, CultureInfo.InvariantCulture))];
        _ = await node.SafePsqlAsync(Database, $"INSERT INTO t (v) SELECT 'tail' FROM generate_series(1, {block[2] - ((block[1] - block[0]) / 20)})", token);
        Stopwatch switching = Stopwatch.StartNew();
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"SELECT active_lo <> {block[0]} FROM worf.sequence_ranges", token), "a switch");
        Assert.IsLessThan(5000, switching.ElapsedMilliseconds, "a switch as the block ran low");
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
            """, token);
        await using (NpgsqlConnection connection = await node.OpenAsync(Database, token))
        {
            List<string> notices = [];
            connection.Notice += (_, e) => notices.Add(e.Notice.MessageText);
            await using NpgsqlCommand add = new("SELECT worf.replication_set_add_all_sequences('default', '{public}')", connection);
            _ = await add.ExecuteScalarAsync(token);
            Assert.IsTrue(notices.Exists(notice => notice.Contains("public.t_id_seq is left out")), "a notice of the managed sequence left out");
        }

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

        // Without a row trigger before an update, which could change the key, the update guard fires only on an update
        // that sets the key.
        Assert.Contains(" AFTER UPDATE OF id ON ", await node.SafePsqlAsync(Database,
            "SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgrelid = 't'::regclass AND tgname = 'worf_sequence_range_guard_update'", token));
        await AssertOutsideAsync(node, "INSERT INTO tenants VALUES (1200, 2)");
        _ = await node.SafePsqlAsync(Database, "INSERT INTO tenants (tenant) VALUES (2)", token);

        // Renaming the key keeps it checked, and a numeric key is checked as its whole values.
        _ = await node.SafePsqlAsync(Database, "ALTER TABLE t RENAME COLUMN id TO key; INSERT INTO t (v) VALUES ('renamed')", token);
        await AssertOutsideAsync(node, "INSERT INTO t (key, v) VALUES (" + Next(await node.SafePsqlAsync(Database, "SELECT max(key) FROM t", token)) + ", 'x')");
        _ = await node.SafePsqlAsync(Database, "INSERT INTO n (v) VALUES ('default')", token);
        await AssertOutsideAsync(node, "INSERT INTO n (id, v) VALUES (5.5, 'fraction')");
        await AssertOutsideAsync(node, "INSERT INTO n (id, v) VALUES (" + Next(Next(await node.SafePsqlAsync(Database, "SELECT max(id) FROM n", token))) + ", 'ahead')");

        // A session that restores rows writes values from before range management, though not values the sequence may
        // still issue: ahead of it in its block, or past every value granted.
        _ = await node.SafePsqlAsync(Database, "SET worf.sequence_range_restore = on; INSERT INTO t (key, v) VALUES (0, 'restored'), (-5, 'restored')", token);
        await AssertOutsideAsync(node, "INSERT INTO t (key, v) VALUES (-6, 'not restored')");
        string restoredAhead = Next(Next(await node.SafePsqlAsync(Database, "SELECT max(key) FROM t", token)));
        string pastGrants = Next(await node.SafePsqlAsync(Database, "SELECT watermark FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token));
        foreach (string restored in new[] { restoredAhead, pastGrants })
        {
            await AssertOutsideAsync(node, $"SET worf.sequence_range_restore = on; INSERT INTO t (key, v) VALUES ({restored}, 'restored')");
        }

        // An update moves a row to another partition with its value, even one from before range management, but not to
        // a value of its choosing, and a MERGE that only updates does too.
        _ = await node.SafePsqlAsync(Database, """
            UPDATE o SET region = 'eu' WHERE id = 5;
            MERGE INTO o USING (VALUES (5)) s (k) ON o.id = s.k WHEN MATCHED THEN UPDATE SET region = 'us';
            """, token);
        await AssertOutsideAsync(node, "UPDATE o SET region = 'eu', id = 1000000 WHERE id = 5");
        await AssertOutsideAsync(node, "MERGE INTO o USING (VALUES (5)) s (k) ON o.id = s.k WHEN MATCHED THEN UPDATE SET region = 'eu' WHEN NOT MATCHED THEN INSERT VALUES (s.k, 'eu')");

        // An update ends before a statement that runs after it with the same command ID, as an extended protocol
        // portal does after another's partial execution, so that statement's rows aren't taken for rows it moved.
        Assert.AreEqual("23514", await PartialExecuteAsync("UPDATE t SET v = v RETURNING key", "INSERT INTO t (key, v) VALUES (333333, 'forged')"));

        // An update that an error ends inside an EXCEPTION block leaves with its subtransaction, so the savepoint of a
        // later transaction in the same simple query, which starts at the same time, isn't taken for it.
        (int forgedExit, string forged) = await node.RunProgramAsync("psql", ["-X", "-d", Database, "-c", """
            BEGIN; DO $$BEGIN BEGIN UPDATE t SET v = (1 / (length(v) - length(v)))::text; EXCEPTION WHEN division_by_zero THEN NULL; END; END$$; COMMIT;
            BEGIN; SAVEPOINT s; INSERT INTO t (key, v) VALUES (777778, 'forged'); COMMIT;
            """], token);
        Assert.AreNotEqual(0, forgedExit, forged);
        Assert.Contains("is not in a sequence range this node holds", forged);

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

        // A numeric value that is no bigint, as NaN, passes the checks of existing rows. They find a value that fails
        // through an index on the column, and skip a partition whose bound, which spares PostgreSQL's own check, rules
        // the failing values out, without reading every row.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE odd (id numeric);
            INSERT INTO odd VALUES ('NaN'), (1e30), (5.5), ('Infinity'), ('-Infinity');
            ALTER TABLE odd ALTER COLUMN id SET DEFAULT nextval('nseq');
            CREATE TABLE indexed (id bigint PRIMARY KEY);
            INSERT INTO indexed SELECT -g FROM generate_series(1, 20000) g;
            CREATE TABLE ip (id bigint DEFAULT nextval('t_id_seq'), v text) PARTITION BY RANGE (id);
            CREATE TABLE ip_old (id bigint NOT NULL CHECK (id < 1), v text);
            INSERT INTO ip_old SELECT -g, 'old' FROM generate_series(1, 20000) g;
            ANALYZE indexed, ip_old;
            """, token);
        const string Scans = "SELECT sum(seq_scan) FROM pg_stat_user_tables WHERE relid IN ('indexed'::regclass, 'ip_old'::regclass)";
        string scans = await node.SafePsqlAsync(Database, Scans, token);
        _ = await node.SafePsqlAsync(Database, """
            ALTER TABLE indexed ALTER COLUMN id SET DEFAULT nextval('t_id_seq');
            ALTER TABLE ip ATTACH PARTITION ip_old FOR VALUES FROM (MINVALUE) TO (1);
            SELECT pg_stat_force_next_flush();
            """, token);
        Assert.AreEqual(scans, await node.SafePsqlAsync(Database, Scans, token), "no sequential scan");

        // The rows of a table whose owner has no rights on its schema are checked all the same, and the sequence its
        // rows use is enabled.
        _ = await node.SafePsqlAsync(Database, """
            CREATE ROLE hider;
            CREATE SCHEMA hidden;
            CREATE TABLE hidden.h (id bigint PRIMARY KEY);
            INSERT INTO hidden.h VALUES (-7);
            CREATE SEQUENCE hidden.hs;
            CREATE TABLE hidden.h2 (id bigint DEFAULT nextval('hidden.hs'));
            INSERT INTO hidden.h2 VALUES (500);
            ALTER TABLE hidden.h OWNER TO hider;
            ALTER TABLE hidden.h2 OWNER TO hider;
            ALTER TABLE hidden.h ALTER COLUMN id SET DEFAULT nextval('t_id_seq');
            SELECT worf.enable_sequence_range('hidden.hs', participants := '{}', range_size := 100);
            """, token);
        Assert.AreEqual("500", await node.SafePsqlAsync(Database, "SELECT high_water FROM worf.sequence_range WHERE seqoid = 'hidden.hs'::regclass", token));

        // A root starts above the whole values within bigint that the tables hold, which NaN, the infinities and values
        // past bigint don't move, as when the sequence is enabled again, with a column that took its default afterwards.
        _ = await node.SafePsqlAsync(Database, """
            CREATE SEQUENCE nan_seq;
            CREATE TABLE nan_rows (id numeric DEFAULT nextval('nan_seq'));
            INSERT INTO nan_rows SELECT FROM generate_series(1, 50);
            INSERT INTO nan_rows VALUES ('NaN'), ('Infinity'), ('-Infinity'), (1e30);
            SELECT worf.enable_sequence_range('nan_seq', participants := '{}', range_size := 100);
            """, token);
        Assert.AreEqual("51|50|51", await node.SafePsqlAsync(Database, """
            WITH i AS (INSERT INTO nan_rows DEFAULT VALUES RETURNING id)
            SELECT r.root_start, r.high_water, (SELECT id FROM i) FROM worf.sequence_range r WHERE r.seqoid = 'nan_seq'::regclass
            """, token));
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE nan_set (id numeric);
            INSERT INTO nan_set VALUES ('Infinity'), ('NaN');
            ALTER TABLE nan_set ALTER COLUMN id SET DEFAULT nextval('nan_seq');
            SELECT worf.disable_sequence_range('nan_seq');
            SELECT worf.enable_sequence_range('nan_seq', participants := '{}', range_size := 100);
            INSERT INTO nan_set DEFAULT VALUES;
            """, token);
        Assert.AreEqual("2|t", await node.SafePsqlAsync(Database,
            "SELECT epoch, root_start BETWEEN 152 AND 1000000 FROM worf.sequence_range WHERE seqoid = 'nan_seq'::regclass", token));

        // A partition whose own default draws from a managed sequence has the column guarded on its own, so attaching one
        // holds up no writers to the tree, and the tree's other partitions write any value in the column.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE pq (id bigint DEFAULT nextval('t_id_seq'), ref bigint, extra bigint, k text, PRIMARY KEY (id, k)) PARTITION BY LIST (k);
            CREATE TABLE pq_a PARTITION OF pq (ref DEFAULT nextval('nseq')) FOR VALUES IN ('a');
            CREATE TABLE pq_b PARTITION OF pq FOR VALUES IN ('b');
            CREATE TABLE pq_c (id bigint NOT NULL, ref bigint, extra bigint DEFAULT nextval('nseq'), k text NOT NULL CHECK (k = 'c'), PRIMARY KEY (id, k));
            INSERT INTO pq (ref, extra, k) VALUES (42, 42, 'b');
            """, token);
        await AssertOutsideAsync(node, "INSERT INTO pq (ref, k) VALUES (42, 'a')");
        await using (NpgsqlConnection attacher = await node.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand attach = new("BEGIN; ALTER TABLE pq ATTACH PARTITION pq_c FOR VALUES IN ('c')", attacher))
            {
                _ = await attach.ExecuteNonQueryAsync(token);
            }

            _ = await node.SafePsqlAsync(Database, "SET lock_timeout = '2s'; INSERT INTO pq (k) VALUES ('b')", token);
            await using NpgsqlCommand commit = new("COMMIT", attacher);
            _ = await commit.ExecuteNonQueryAsync(token);
        }

        await AssertOutsideAsync(node, "INSERT INTO pq (extra, k) VALUES (42, 'c')");
        _ = await node.SafePsqlAsync(Database, "INSERT INTO pq (extra, k) VALUES (42, 'a')", token);

        // A guarded column changes its type, unchecked when its values stay, as from one integer type to another without
        // USING, and drops without CASCADE. A default other than nextval of the sequence, alone or cast, is refused, and
        // a column the sequence owns takes one that is.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE widen (id int PRIMARY KEY DEFAULT nextval('t_id_seq'), extra bigint DEFAULT nextval('t_id_seq'));
            INSERT INTO widen DEFAULT VALUES;
            SET session_replication_role = replica;
            INSERT INTO widen VALUES (1200000, 1200000);
            RESET session_replication_role;
            ALTER TABLE widen ALTER COLUMN id TYPE bigint;
            ALTER TABLE widen DROP COLUMN extra;
            CREATE SEQUENCE owner_seq;
            SELECT worf.enable_sequence_range('owner_seq', participants := '{}', range_size := 100);
            CREATE TABLE owned (id bigint PRIMARY KEY);
            ALTER SEQUENCE owner_seq OWNED BY owned.id;
            ALTER TABLE owned ALTER COLUMN id SET DEFAULT nextval('owner_seq');
            INSERT INTO owned DEFAULT VALUES;
            CREATE TABLE plus (id bigint);
            CREATE DOMAIN plus_one AS bigint DEFAULT nextval('t_id_seq') + 1;
            """, token);
        await AssertOutsideAsync(node, "INSERT INTO widen VALUES (1300000)");
        await AssertOutsideAsync(node, "ALTER TABLE widen ALTER COLUMN id TYPE numeric USING id");

        // An identity column changes its type, and its sequence's with it, which keeps drawing from its block, or stays
        // quarantined until its root starts.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE ident (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v text);
            SELECT worf.enable_sequence_range('ident_id_seq', participants := '{}', range_size := 100);
            INSERT INTO ident (v) VALUES ('a');
            ALTER TABLE ident ALTER COLUMN id TYPE int;
            INSERT INTO ident (v) VALUES ('b');
            CREATE TABLE ident2 (id bigint GENERATED ALWAYS AS IDENTITY);
            SELECT worf.enable_sequence_range('ident2_id_seq');
            ALTER TABLE ident2 ALTER COLUMN id TYPE int;
            """, token);
        Assert.AreEqual("integer|t|2", await node.SafePsqlAsync(Database, """
            SELECT s.seqtypid::regtype::text, s.seqmin = r.active_lo AND s.seqmax = r.active_hi, (SELECT count(*) FROM ident)
            FROM pg_sequence s JOIN worf.sequence_ranges r ON r.sequence = s.seqrelid::regclass WHERE s.seqrelid = 'ident_id_seq'::regclass
            """, token));
        await AssertOutsideAsync(node, "INSERT INTO ident VALUES (999999, 'by hand')");
        PostgresException quarantinedIdentity = await Assert.ThrowsExactlyAsync<PostgresException>(
            () => node.SafePsqlAsync(Database, "INSERT INTO ident2 DEFAULT VALUES", token));
        Assert.AreEqual("2200H", quarantinedIdentity.SqlState);
        Assert.AreEqual("1\n2147483647", await node.SafePsqlAsync(Database, """
            SELECT worf.activate_sequence_range('ident2_id_seq');
            INSERT INTO ident2 DEFAULT VALUES RETURNING id;
            SELECT root_end FROM worf.sequence_range WHERE seqoid = 'ident2_id_seq'::regclass;
            """, token));

        // A default of currval issues nothing, so its column takes any value and enabling its sequence is no trouble. A
        // default that names a sequence as a constant string, looked up as it runs, guards its column as nextval of the
        // sequence does.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE cv (id bigint DEFAULT nextval('t_id_seq'), parent bigint DEFAULT currval('t_id_seq'));
            INSERT INTO cv DEFAULT VALUES;
            INSERT INTO cv (parent) VALUES (5);
            CREATE SEQUENCE cvseq;
            CREATE TABLE cv2 (id bigint DEFAULT nextval('t_id_seq'), ref bigint DEFAULT currval('cvseq'));
            SELECT worf.enable_sequence_range('cvseq', participants := '{}', range_size := 100);
            CREATE TABLE named (id bigint PRIMARY KEY DEFAULT nextval('t_id_seq'::text));
            CREATE DOMAIN named_domain AS bigint DEFAULT nextval('public.t_id_seq'::text);
            CREATE TABLE named2 (id named_domain);
            INSERT INTO named DEFAULT VALUES;
            INSERT INTO named2 DEFAULT VALUES;
            """, token);
        Assert.AreEqual("1", await node.SafePsqlAsync(Database, "SELECT count(*) FROM cv WHERE id = parent", token));
        await AssertOutsideAsync(node, "INSERT INTO named VALUES (999999999)");
        await AssertOutsideAsync(node, "INSERT INTO named2 VALUES (999999999)");

        // The text may be cast or put together in any way that folds to a constant, as a dump restored with an empty
        // search path writes it, and is looked up as the row is written, under the search path the default draws under,
        // rather than the DDL's. A name that gives no schema stands for the sequence in the table's.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE named_varchar (id bigint DEFAULT nextval('t_id_seq'::varchar));
            CREATE TABLE named_name (id bigint DEFAULT nextval('t_id_seq'::name::text));
            CREATE TABLE named_concat (id bigint DEFAULT nextval('t_id' || '_seq'));
            SET search_path = '';
            CREATE TABLE public.named_dump (id bigint DEFAULT nextval(('t_id_seq'::text)::regclass));
            CREATE SEQUENCE other.t_id_seq START 888890;
            SET search_path = other;
            CREATE TABLE public.named_other (id bigint DEFAULT nextval('t_id_seq'::text));
            """, token);
        foreach (string table in new[] { "named_varchar", "named_name", "named_concat", "named_dump", "named_other" })
        {
            _ = await node.SafePsqlAsync(Database, $"INSERT INTO {table} DEFAULT VALUES", token);
            await AssertOutsideAsync(node, $"INSERT INTO {table} VALUES (888888)");
        }

        // A value from a sequence of the name that isn't managed, as one the search path finds first or a temporary one,
        // or written where the name finds no sequence, as a restore with an empty search path writes rows, is checked
        // under the managed sequence the name stands for.
        foreach (string write in new[]
        {
            "SET search_path = other; INSERT INTO public.named_other DEFAULT VALUES",
            "CREATE TEMP SEQUENCE t_id_seq START 888888; INSERT INTO named DEFAULT VALUES",
            "SET search_path = ''; INSERT INTO public.named VALUES (888888)",
            "SET search_path = ''; COPY public.named FROM PROGRAM 'echo 888889'",
            "SET search_path = ''; INSERT INTO public.named2 VALUES (888888)",
        })
        {
            await AssertOutsideAsync(node, write);
        }

        // A name that gives no schema leaves alone a table in another schema whose default names a sequence of its own.
        _ = await node.SafePsqlAsync(Database, """
            CREATE ROLE ra;
            CREATE ROLE rb;
            GRANT USAGE ON SCHEMA worf TO ra;
            CREATE SCHEMA sa AUTHORIZATION ra;
            CREATE SCHEMA sb AUTHORIZATION rb;
            SET ROLE rb;
            CREATE SEQUENCE sb.myseq;
            CREATE TABLE sb.bt (id bigint DEFAULT nextval('myseq'::text));
            RESET ROLE;
            SET ROLE ra;
            CREATE SEQUENCE sa.myseq;
            SET search_path = sa;
            SELECT worf.enable_sequence_range('sa.myseq', participants := '{}', range_size := 100);
            """, token);
        Assert.AreEqual("0", await node.SafePsqlAsync(Database, "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'sb.bt'::regclass", token), "sb.bt's triggers");
        _ = await node.SafePsqlAsync(Database, """
            SET ROLE rb;
            SET search_path = sb;
            ALTER TABLE sb.bt ALTER COLUMN id SET DEFAULT nextval('sb.myseq');
            ALTER TABLE sb.bt ALTER COLUMN id DROP DEFAULT;
            ALTER TABLE sb.bt DISABLE TRIGGER ALL;
            """, token);

        // An update that moves a row into a partition whose trigger, run before the insert there, changes its key, is
        // checked on every update, as one whose trigger runs before the update.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE pm (id bigint DEFAULT nextval('t_id_seq'), region text) PARTITION BY LIST (region);
            CREATE TABLE pm_us PARTITION OF pm FOR VALUES IN ('us');
            CREATE TABLE pm_eu PARTITION OF pm FOR VALUES IN ('eu');
            CREATE FUNCTION rekey() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF current_setting('test.rekey', true) = 'on' THEN NEW.id := 888801; END IF;
                RETURN NEW;
            END$$;
            CREATE TRIGGER rekey BEFORE INSERT ON pm_eu FOR EACH ROW EXECUTE FUNCTION rekey();
            INSERT INTO pm (region) VALUES ('us');
            """, token);
        await AssertOutsideAsync(node, "SET test.rekey = on; UPDATE pm SET region = 'eu'");
        await AssertOutsideAsync(node, "SET test.rekey = on; MERGE INTO pm USING (VALUES (1)) v (x) ON true WHEN MATCHED THEN UPDATE SET region = 'eu'");
        _ = await node.SafePsqlAsync(Database, "UPDATE pm SET region = 'eu'", token);
        PostgresException unknown = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
            "CREATE TABLE named_unknown (id bigint DEFAULT nextval(current_setting('application_name')))", token));
        Assert.AreEqual("0A000", unknown.SqlState);
        Assert.Contains("regclass constant", unknown.Hint ?? "");
        foreach (string ddl in new[]
        {
            "CREATE TABLE plus2 (id bigint DEFAULT nextval('t_id_seq') + 1000000)",
            "ALTER TABLE plus ALTER COLUMN id SET DEFAULT nextval('t_id_seq') * 2",
            "CREATE TABLE plus3 (id plus_one)",
        })
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database, ddl, token), ddl);
            Assert.AreEqual("0A000", error.SqlState, ddl);
        }

        // A column with no default of its own takes its domain's, which guards it as its own would, and which can't change
        // while it uses a managed sequence. A domain that comes to use one checks its columns' rows.
        _ = await node.SafePsqlAsync(Database, """
            CREATE DOMAIN numbered AS bigint DEFAULT nextval('t_id_seq');
            CREATE TABLE via_domain (id numbered PRIMARY KEY, v text);
            INSERT INTO via_domain (v) VALUES ('numbered');
            CREATE DOMAIN later AS bigint;
            CREATE TABLE via_later (id later PRIMARY KEY);
            INSERT INTO via_later VALUES (979797);
            """, token);
        await AssertOutsideAsync(node, "INSERT INTO via_domain VALUES (979797, 'by hand')");
        await AssertOutsideAsync(node, "ALTER DOMAIN later SET DEFAULT nextval('t_id_seq')");
        foreach (string ddl in new[]
        {
            "ALTER DOMAIN numbered DROP DEFAULT",
            "ALTER DOMAIN numbered SET DEFAULT 0",
            "ALTER TABLE via_domain ALTER COLUMN id SET DEFAULT 0",
        })
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database, ddl, token), ddl);
            Assert.AreEqual("55000", error.SqlState, ddl);
        }

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

        PostgresException restoreRows = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
            "SET ROLE app; SET worf.sequence_range_restore = on", token));
        Assert.AreEqual("42501", restoreRows.SqlState);

        // A role granted SET on it restores structure under range management's rules all the same.
        _ = await node.SafePsqlAsync(Database, "GRANT SET ON PARAMETER worf.subscription_schema_restore TO app", token);
        foreach (string ddl in new[] { "DROP TRIGGER worf_sequence_range_guard ON app.app_rows", "ALTER TABLE app.app_rows ALTER COLUMN id SET DEFAULT 0" })
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
                $"SET ROLE app; SET worf.subscription_schema_restore = on; {ddl}", token), ddl);
            Assert.AreEqual("55000", error.SqlState, ddl);
        }

        PostgresException reserve = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
            "SET ROLE app; SELECT worf.reserve_sequence_values('t_id_seq', 1, 2)", token));
        Assert.AreEqual("42501", reserve.SqlState);
        Assert.Contains("must be owner", reserve.MessageText);

        // With USAGE on worf's schema, a role manages the range of a sequence it owns, though not of one it doesn't.
        _ = await node.SafePsqlAsync(Database, """
            SET ROLE app;
            SELECT worf.enable_sequence_range('app.own_rows_id_seq', participants := '{}', range_size := 100);
            SELECT worf.alter_sequence_range('app.own_rows_id_seq', range_size := 200);
            INSERT INTO app.own_rows DEFAULT VALUES;
            """, token);
        PostgresException notOwned = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
            "SET ROLE app; SELECT worf.disable_sequence_range('t_id_seq')", token));
        Assert.AreEqual("42501", notOwned.SqlState);

        // worf's own queries search the system catalog first, whatever search path the session that runs them sets, so an
        // operator a role puts ahead of PostgreSQL's neither keeps the guard off its table nor drops it later.
        _ = await node.SafePsqlAsync(Database, """
            SET ROLE app;
            CREATE FUNCTION app.never(oid, oid) RETURNS boolean LANGUAGE sql IMMUTABLE AS 'SELECT false';
            CREATE OPERATOR app.= (LEFTARG = oid, RIGHTARG = oid, FUNCTION = app.never);
            SET search_path = app, pg_catalog;
            CREATE TABLE app.shadowed (id bigint PRIMARY KEY DEFAULT nextval('public.t_id_seq'));
            ALTER TABLE app.shadowed ADD COLUMN v text;
            """, token);
        Assert.AreEqual("2", await node.SafePsqlAsync(Database, "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'app.shadowed'::regclass", token),
            "app.shadowed's guard");
        await AssertOutsideAsync(node, "SET ROLE app; SET search_path = app, pg_catalog; INSERT INTO app.shadowed VALUES (888888)");

        // worf's workers search only the system catalog, whatever search path the database sets, so the owner of a
        // database can't put an operator of its own into their queries, which run as a superuser.
        _ = await node.SafePsqlAsync(Database, "CREATE DATABASE owned OWNER app", token);
        _ = await node.SafePsqlAsync("owned", "CREATE EXTENSION worf; GRANT USAGE ON SCHEMA worf TO app", token);
        _ = await node.SafePsqlAsync("owned", $"SELECT * FROM worf.create_node(node_name := 'owned', dsn := '{node.Dsn("owned", SuperUser)}')", token);
        _ = await node.SafePsqlAsync("owned", """
            SET ROLE app;
            CREATE FUNCTION public.eq(oid, oid) RETURNS boolean LANGUAGE plpgsql AS $$
            BEGIN
                BEGIN
                    ALTER ROLE app SUPERUSER;
                EXCEPTION WHEN insufficient_privilege THEN
                    NULL;
                END;
                RETURN $1 OPERATOR(pg_catalog.=) $2;
            END$$;
            CREATE OPERATOR public.= (LEFTARG = oid, RIGHTARG = oid, FUNCTION = public.eq);
            ALTER DATABASE owned SET search_path = public, pg_catalog;
            CREATE TABLE owned_rows (id bigserial PRIMARY KEY);
            SELECT worf.enable_sequence_range('owned_rows_id_seq', participants := '{}', range_size := 100);
            """, token);
        Assert.IsTrue(await node.PollQueryUntilAsync("owned", "SELECT state = 'active' FROM worf.sequence_ranges", token), "owned_rows_id_seq active");
        Assert.AreEqual("f", await node.SafePsqlAsync(Database, "SELECT rolsuper FROM pg_roles WHERE rolname = 'app'", token), "app a superuser");

        // worf reads a role's table with PostgreSQL's own scans, on SET DEFAULT and in the range worker, which run none of
        // the functions that its CHECK constraint and index predicate call, so they can't make it a superuser.
        int escapes = node.ReadLog().Length;
        _ = await node.SafePsqlAsync(Database, """
            SET ROLE app;
            CREATE FUNCTION app.esc(what text) RETURNS boolean LANGUAGE plpgsql AS $$
            BEGIN
                RAISE LOG 'esc(%) runs as % in %', what, current_user, pg_backend_pid();
                ALTER ROLE app SUPERUSER;
                RETURN true;
            EXCEPTION WHEN insufficient_privilege THEN
                RETURN true;
            END$$;
            CREATE FUNCTION app.imm(what text) RETURNS boolean LANGUAGE sql IMMUTABLE AS $$SELECT app.esc(what)$$;
            CREATE TABLE app.x2 (id bigint PRIMARY KEY, CHECK (app.imm('check')));
            CREATE INDEX ON app.x2 (id) WHERE app.imm('index');
            ALTER TABLE app.x2 ALTER COLUMN id SET DEFAULT nextval('public.t_id_seq');
            """, token);

        _ = await node.SafePsqlAsync(Database, "SET ROLE app; INSERT INTO app.x2 SELECT FROM generate_series(1, 3)", token);

        // worf creates the guard's triggers as the table's owner too, so the functions a partition key runs as its tree is
        // read can't make it a superuser, and runs no event trigger for them, which would find the role's temporary
        // table of the name the event trigger writes to first.
        _ = await node.SafePsqlAsync(Database, """
            SET ROLE app;
            CREATE FUNCTION app.immi(what text) RETURNS bigint LANGUAGE sql IMMUTABLE AS $$SELECT CASE WHEN app.esc(what) THEN 0::bigint END$$;
            CREATE TABLE app.pk (id bigint, v text) PARTITION BY RANGE ((id + app.immi('pk')));
            CREATE TABLE app.pk_all PARTITION OF app.pk FOR VALUES FROM (MINVALUE) TO (MAXVALUE);
            CREATE FUNCTION app.audited() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN PERFORM app.esc('audit'); RETURN NEW; END$$;
            CREATE TABLE app.audited (id bigint);
            """, token);
        _ = await node.SafePsqlAsync(Database, "SET ROLE app; ALTER TABLE app.pk ALTER COLUMN id SET DEFAULT nextval('public.t_id_seq')", token);
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE ddl_audit (tag text);
            GRANT INSERT ON ddl_audit TO PUBLIC;
            CREATE FUNCTION audit() RETURNS event_trigger LANGUAGE plpgsql AS $$BEGIN INSERT INTO ddl_audit VALUES (tg_tag); END$$;
            CREATE EVENT TRIGGER audit ON ddl_command_end EXECUTE FUNCTION audit();
            SET ROLE app;
            CREATE TEMP TABLE ddl_audit (tag text);
            CREATE TRIGGER audited BEFORE INSERT ON ddl_audit FOR EACH ROW EXECUTE FUNCTION app.audited();
            ALTER TABLE app.audited ALTER COLUMN id SET DEFAULT nextval('public.t_id_seq');
            """, token);
        Assert.AreEqual("f|6", await node.SafePsqlAsync(Database, """
            SELECT rolsuper, (SELECT count(*) FROM pg_trigger WHERE tgrelid IN ('app.pk'::regclass, 'app.pk_all'::regclass, 'app.audited'::regclass))
            FROM pg_roles WHERE rolname = 'app';
            DROP EVENT TRIGGER audit;
            """, token));
        string rangeWorker = await node.SafePsqlAsync(Database, "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND backend_type LIKE 'worf range%'", token);

        // A few rows inserted at once are no rate kept up for minutes, which would size its blocks far beyond range_size.
        _ = await node.SafePsqlAsync(Database, "SET ROLE app; INSERT INTO app.own_rows SELECT FROM generate_series(1, 5)", token);

        // Triggers with the guard's names from before worf was installed are replaced as their table comes under the
        // guard, and once it is installed, no trigger takes the guard's names or function, while a restore skips them.
        _ = await node.SafePsqlAsync(Database, "CREATE DATABASE decoys", token);
        _ = await node.SafePsqlAsync("decoys", """
            CREATE TABLE d (id bigserial PRIMARY KEY);
            CREATE TABLE pre (id bigint DEFAULT nextval('d_id_seq'::text));
            CREATE TABLE fresh (id bigint);
            CREATE FUNCTION sequence_range_guard() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NULL; END$$;
            CREATE TRIGGER other AFTER INSERT ON fresh FOR EACH ROW EXECUTE FUNCTION sequence_range_guard();
            DO $$
            BEGIN
                EXECUTE format('CREATE TRIGGER worf_sequence_range_guard AFTER INSERT ON d FOR EACH ROW WHEN (false)
                    EXECUTE FUNCTION public.sequence_range_guard(%L)', '1:' || 'd_id_seq'::regclass::oid);
                EXECUTE format('CREATE TRIGGER worf_sequence_range_guard_update AFTER UPDATE ON d FOR EACH ROW WHEN (false)
                    EXECUTE FUNCTION public.sequence_range_guard(%L)', '1:' || 'd_id_seq'::regclass::oid);
            END
            $$;
            CREATE EXTENSION worf;
            """, token);
        _ = await node.SafePsqlAsync("decoys", $"SELECT * FROM worf.create_node(node_name := 'decoys', dsn := '{node.Dsn("decoys", SuperUser)}')", token);
        foreach (string ddl in new[]
        {
            "CREATE TRIGGER worf_sequence_range_guard AFTER INSERT ON fresh FOR EACH ROW WHEN (false) EXECUTE FUNCTION public.sequence_range_guard()",
            "CREATE TRIGGER copy AFTER INSERT ON fresh FOR EACH ROW WHEN (false) EXECUTE FUNCTION worf.sequence_range_guard('1:1')",
            "ALTER TRIGGER other ON fresh RENAME TO worf_sequence_range_guard",
        })
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync("decoys", ddl, token), ddl);
            Assert.AreEqual("55000", error.SqlState, ddl);
        }

        _ = await node.SafePsqlAsync("decoys", """
            SET worf.subscription_schema_restore = on;
            CREATE TRIGGER worf_sequence_range_guard AFTER INSERT ON fresh FOR EACH ROW WHEN (false) EXECUTE FUNCTION public.sequence_range_guard();
            RESET worf.subscription_schema_restore;
            SELECT worf.enable_sequence_range('d_id_seq', participants := '{}', range_size := 100);
            INSERT INTO d DEFAULT VALUES;
            """, token);
        Assert.AreEqual("other", await node.SafePsqlAsync("decoys", "SELECT string_agg(tgname, ',') FROM pg_trigger WHERE tgrelid = 'fresh'::regclass", token));

        // A role without USAGE on worf's schema creates triggers, and tables that come under the guard.
        _ = await node.SafePsqlAsync("decoys", """
            CREATE ROLE plain;
            CREATE SCHEMA plain AUTHORIZATION plain;
            GRANT USAGE ON SEQUENCE d_id_seq TO plain;
            SET ROLE plain;
            CREATE TABLE plain.numbered (id bigint PRIMARY KEY DEFAULT nextval('public.d_id_seq'));
            CREATE FUNCTION plain.noop() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
            CREATE TRIGGER noop BEFORE INSERT ON plain.numbered FOR EACH ROW EXECUTE FUNCTION plain.noop();
            """, token);
        foreach (string write in new[] { "INSERT INTO d VALUES (987654)", "UPDATE d SET id = 987655", "INSERT INTO plain.numbered VALUES (987656)", "INSERT INTO pre VALUES (987657)" })
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync("decoys", write, token), write);
            Assert.AreEqual("23514", error.SqlState, write);
        }

        // By now the range worker has read the role's table many times, and nothing ran the role's functions but the role.
        Assert.AreEqual("t", await node.SafePsqlAsync(Database, "SELECT peak_rate < 1 FROM worf.sequence_range WHERE seqoid = 'app.own_rows_id_seq'::regclass", token),
            "the rate of a few rows inserted at once");
        string escaped = node.ReadLog()[escapes..];
        Assert.DoesNotContain("runs as postgres", escaped);
        Assert.DoesNotContain($"in {rangeWorker}", escaped);

        async Task AssertOutsideAsync(TestNode target, string sql)
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => target.SafePsqlAsync(Database, sql, token), sql);
            Assert.AreEqual("23514", error.SqlState, sql);
        }

        // Runs two statements over the extended protocol in one transaction, parsing and binding both before the first
        // is executed for one row and the second in full, and returns the SQLSTATE of the error they end with, if any.
        async Task<string?> PartialExecuteAsync(string first, string second)
        {
            using TcpClient client = new();
            await client.ConnectAsync("127.0.0.1", node.Cluster.Port, token);
            NetworkStream stream = client.GetStream();
            byte[] startup = [.. new byte[8], .. Text("user"), .. Text("postgres"), .. Text("database"), .. Text(Database), 0];
            BinaryPrimitives.WriteInt32BigEndian(startup, startup.Length);
            BinaryPrimitives.WriteInt32BigEndian(startup.AsSpan(4), 3 << 16);
            await stream.WriteAsync(startup, token);
            _ = await ReadyAsync();
            byte[] extended = [
                .. Message('P', Text("s1"), Text(first), new byte[2]), .. Message('B', Text("p1"), Text("s1"), new byte[6]),
                .. Message('P', Text("s2"), Text(second), new byte[2]), .. Message('B', Text("p2"), Text("s2"), new byte[6]),
                .. Message('E', Text("p1"), Rows(1)), .. Message('E', Text("p2"), Rows(0)), .. Message('S')];
            await stream.WriteAsync(extended, token);
            return await ReadyAsync();

            static byte[] Text(string value) => [.. Encoding.UTF8.GetBytes(value), 0];

            static byte[] Rows(int rows)
            {
                byte[] count = new byte[4];
                BinaryPrimitives.WriteInt32BigEndian(count, rows);
                return count;
            }

            static byte[] Message(char type, params byte[][] parts)
            {
                byte[] message = [(byte)type, 0, 0, 0, 0, .. parts.SelectMany(part => part)];
                BinaryPrimitives.WriteInt32BigEndian(message.AsSpan(1), message.Length - 1);
                return message;
            }

            // Reads messages up to the next ReadyForQuery, keeping the SQLSTATE of an ErrorResponse.
            async Task<string?> ReadyAsync()
            {
                string? state = null;
                byte[] header = new byte[5];
                while (true)
                {
                    await stream.ReadExactlyAsync(header, token);
                    byte[] body = new byte[BinaryPrimitives.ReadInt32BigEndian(header.AsSpan(1)) - 4];
                    await stream.ReadExactlyAsync(body, token);
                    if (header[0] == (byte)'Z')
                    {
                        return state;
                    }

                    if (header[0] == (byte)'E')
                    {
                        state ??= Encoding.UTF8.GetString(body).Split('\0').FirstOrDefault(field => field.StartsWith('C'))?[1..];
                    }
                }
            }
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

        // A reservation the next block holds switches to it, though a load just ran fast, rather than taking a block sized
        // to a minute at that rate.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE bulk (id bigserial PRIMARY KEY);
            SELECT worf.enable_sequence_range('bulk_id_seq', participants := '{}', range_size := 100000, max_range_size := 100000);
            INSERT INTO bulk DEFAULT VALUES;
            """, token);
        await Task.Delay(TimeSpan.FromMilliseconds(500), token);
        _ = await node.SafePsqlAsync(Database, "INSERT INTO bulk SELECT FROM generate_series(1, 50000)", token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT peak_rate * 120 >= max_range_size FROM worf.sequence_range WHERE seqoid = 'bulk_id_seq'::regclass", token),
            "the load's rate, at which two minutes take the largest block");
        Assert.AreEqual("100001|100000", await node.SafePsqlAsync(Database, """
            SELECT worf.reserve_sequence_range('bulk_id_seq', 60000);
            SELECT r.active_lo, (SELECT max(hi - lo + 1) FROM worf.sequence_range_grant WHERE seqoid = 'bulk_id_seq'::regclass)
            FROM worf.sequence_ranges r WHERE r.sequence = 'bulk_id_seq'::regclass
            """, token));

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
                CREATE SEQUENCE nums_seq;
                CREATE TABLE nums (id numeric PRIMARY KEY DEFAULT nextval('nums_seq'));
                """, token);
        }

        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO narrow VALUES (32767, 'legacy'); INSERT INTO nums VALUES (150), (151), ('NaN')", token);
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO nums SELECT FROM generate_series(1, 3)", token);
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

        // node_a's first Snowflake ID needs the clock reserved, which the session does itself rather than wait on the
        // range worker, held up here in an exchange that waits on node_b's table.
        _ = await nodeA.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_timeout = '4s'", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        await using (await HoldExchangesAsync(nodeB, token))
        {
            Stopwatch elapsed = Stopwatch.StartNew();
            _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.snowflake_id()", token);
            Assert.IsLessThan(1500, elapsed.ElapsedMilliseconds, "node_a's first Snowflake ID while an exchange hangs");
        }

        Stopwatch sinceId = Stopwatch.StartNew();

        // The block runs out while an exchange hangs, and an insert that reaches its last twentieth waits for the range
        // worker to switch, rather than the inserts after it failing at the block's end. The switch reaches the backends that
        // cached the blocks as it commits, rather than once the exchange worker's call is done.
        string[] active = (await nodeA.SafePsqlAsync(Database, "SELECT active_lo, values_left FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass",
            token)).Split('|');
        int held = int.Parse(active[1], CultureInfo.InvariantCulture) - 10;
        await using (NpgsqlConnection writer = await nodeA.OpenAsync(Database, token))
        await using (await HoldExchangesAsync(nodeB, token))
        {
            await ExecuteAsync(writer, $"INSERT INTO t (v) SELECT 'held' FROM generate_series(1, {held})", token);
            for (int i = 0; i < 20; i++)
            {
                await ExecuteAsync(writer, "INSERT INTO t (v) VALUES ('held')", token);
            }

            Assert.AreEqual("t", await nodeA.SafePsqlAsync(Database,
                $"SELECT active_lo <> {active[0]} FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token), "node_a switching");
        }

        _ = await nodeA.SafePsqlAsync(Database, "ALTER SYSTEM RESET worf.sequence_range_timeout", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);

        await Task.WhenAll(InsertAsync(nodeA, "a"), InsertAsync(nodeB, "b"));
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        string total = (600 + held + 20).ToString(CultureInfo.InvariantCulture);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.AreEqual($"{total}|{total}|300|300", await node.SafePsqlAsync(Database,
                "SELECT count(*), count(DISTINCT id), count(*) FILTER (WHERE v = 'a'), count(*) FILTER (WHERE v = 'b') FROM t", token),
                node.Name);
        }

        // A root starts above the values both nodes hold, which a NaN doesn't move.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('nums_seq', participants := '{node_b}', range_size := 100)", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database,
            "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'nums_seq'::regclass", token), "nums_seq's root started");
        Assert.AreEqual("152|152", await nodeA.SafePsqlAsync(Database, """
            WITH i AS (INSERT INTO nums DEFAULT VALUES RETURNING id)
            SELECT r.root_start, (SELECT id FROM i) FROM worf.sequence_range r WHERE r.seqoid = 'nums_seq'::regclass
            """, token));

        // A root with no values left above node_b's can't start. The owner notes why and warns once, and takes in
        // node_b's report and request, which stay as they are, without writing, while node_b asks less often.
        int refused = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('narrow_id_seq', participants := '{node_b}')", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, """
            SELECT detail = 'sequence public.narrow_id_seq has no values left above 32767 for a root' FROM worf.sequence_ranges
            WHERE sequence = 'narrow_id_seq'::regclass
            """, token), "narrow_id_seq's root can't start");
        TimeSpan rest = TimeSpan.FromSeconds(10.5) - sinceId.Elapsed;
        if (rest > TimeSpan.Zero)
        {
            await Task.Delay(rest, token);
        }

        // Nor does the range worker reserve the Snowflake clock, which it did every five seconds, while no session issues
        // IDs: it stops ten seconds after the last.
        await AssertIdleAsync([nodeA, nodeB], true, token, TimeSpan.FromSeconds(6));
        Assert.AreEqual(1, nodeA.ReadLog()[refused..].Split("has no values left above 32767 for a root").Length - 1, "node_a warned once");

        // A session that can't write, here in a read-only transaction, can't reserve the clock after an idle stretch
        // itself, so the range worker reserves it for the session, even while an exchange hangs, which holds up only the
        // exchange worker.
        await using (await HoldExchangesAsync(nodeB, token))
        {
            Stopwatch readOnly = Stopwatch.StartNew();
            _ = await nodeA.SafePsqlAsync(Database, "BEGIN READ ONLY; SELECT worf.snowflake_id(); COMMIT", token);
            Assert.IsLessThan(3000, readOnly.ElapsedMilliseconds, "node_a's Snowflake ID in a read-only transaction while an exchange hangs");
        }

        // A table that comes to use the sequence can't hold the other node's keys, which may be ones it has yet to issue,
        // unless a session that restores rows brings them.
        _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE copied AS SELECT id, v FROM t WHERE v = 'a'", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT watermark >= (SELECT max(id) FROM copied) FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token), "node_a's grants heard of");
        PostgresException copied = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
            "ALTER TABLE copied ALTER COLUMN id SET DEFAULT nextval('t_id_seq')", token));
        Assert.AreEqual("23514", copied.SqlState);
        _ = await nodeB.SafePsqlAsync(Database,
            "SET worf.sequence_range_restore = on; ALTER TABLE copied ALTER COLUMN id SET DEFAULT nextval('t_id_seq')", token);

        // DDL from the other node, which its apply worker runs, checks rows as a restore does, since they hold that node's
        // keys.
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE shared (id bigint PRIMARY KEY)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT * FROM worf.replication_set_add_table('default', 'shared');
            INSERT INTO shared SELECT id FROM t WHERE v = 'a';
            SELECT worf.replicate_ddl_command('ALTER TABLE public.shared ALTER COLUMN id SET DEFAULT nextval(''public.t_id_seq'')');
            """, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT count(*) = 300 FROM shared WHERE EXISTS (SELECT 1 FROM pg_attrdef WHERE adrelid = 'shared'::regclass)", token),
            "node_a's rows and default on node_b");

        // node_b joins a sequence late. node_a loses its grants to node_b, and its record of node_b's reports, as a recovery
        // to before them would, after exchanges that carried none of the sequence, and resets: its fence waits for node_b,
        // which it names, though node_b is down for longer than a neighbor counts as in touch, and takes the grants back
        // into its ledger once node_b reports.
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE mv (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('mv_id_seq', participants := '{}', range_size := 100)", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'mv_id_seq'::regclass", token),
                $"{node.Name} drawing from mv_id_seq's blocks");
            _ = await node.SafePsqlAsync(Database, $"INSERT INTO mv (v) SELECT '{node.Name}' FROM generate_series(1, 20)", token);
        }

        // node_b stops first, so that no report of its can bring the grants back before node_a resets.
        await nodeB.StopAsync("fast", token);
        // Edits by hand lock the range row first, as every change to a sequence's ranges does, so that none deadlocks with an
        // exchange.
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT 1 FROM worf.sequence_range WHERE seqoid = 'mv_id_seq'::regclass FOR NO KEY UPDATE;
            DELETE FROM worf.sequence_range_grant WHERE seqoid = 'mv_id_seq'::regclass AND grantee_name = 'node_b';
            DELETE FROM worf.sequence_range_node WHERE seqoid = 'mv_id_seq'::regclass AND node_name = 'node_b';
            """, token);
        int lost = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('mv_id_seq')", token);
        await Task.Delay(TimeSpan.FromSeconds(8), token);
        Assert.AreEqual("t|t", await nodeA.SafePsqlAsync(Database,
            "SELECT fenced_at IS NOT NULL, detail LIKE 'fenced until node node_b reports%' FROM worf.sequence_range WHERE seqoid = 'mv_id_seq'::regclass", token),
            "node_a's fence waiting for node_b");
        await nodeB.StartServerAsync(token);
        Assert.IsTrue(await nodeA.PollLogAsync(lost, "which this allocator had no record of; it is back in the ledger", token), "node_b's grants back in the ledger");
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'mv_id_seq'::regclass", token),
                $"{node.Name} drawing from mv_id_seq's blocks again");
        }

        // Dropping a managed sequence on its owner ends its epoch, which node_b, keeping its blocks, hears of as disabled.
        // A dump of its table restores there, the sequence unmanaged, and enabling it again starts a new epoch above the
        // values node_b holds.
        using TemporaryDirectory dumps = new($"worf-sequence-range-dump-{Guid.NewGuid():N}");
        string dump = Path.Join(dumps.FullName, "mv.dump");
        (int dumped, string dumpOutput) = await nodeA.RunProgramAsync("pg_dump", ["-Fc", "-t", "mv", "-f", dump, "-d", Database], token);
        Assert.AreEqual(0, dumped, dumpOutput);
        int dropped = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "DROP TABLE mv", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'disabled' FROM worf.sequence_ranges WHERE sequence = 'mv_id_seq'::regclass", token),
            "node_b heard mv_id_seq's epoch end");
        (int restored, string restoreOutput) = await nodeA.RunProgramAsync("pg_restore",
            ["-d", $"dbname={Database} options='-c worf.sequence_range_restore=on'", dump], token);
        Assert.AreEqual(0, restored, restoreOutput);
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        Assert.AreEqual("20|f", await nodeA.SafePsqlAsync(Database,
            "SELECT count(*), EXISTS (SELECT 1 FROM worf.sequence_range WHERE seqoid = 'mv_id_seq'::regclass) FROM mv", token));
        Assert.DoesNotContain("it resets", nodeA.ReadLog()[dropped..]);
        string heldByB = await nodeB.SafePsqlAsync(Database, "SELECT max(hi) FROM worf.sequence_range_block WHERE seqoid = 'mv_id_seq'::regclass AND kind = 'n'", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('mv_id_seq', participants := '{node_b}', range_size := 100)", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database,
            $"SELECT state = 'active' AND epoch = 2 AND root_start > {heldByB} FROM worf.sequence_range WHERE seqoid = 'mv_id_seq'::regclass", token),
            "mv_id_seq's second epoch above node_b's blocks");
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT epoch = 2 FROM worf.sequence_range WHERE seqoid = 'mv_id_seq'::regclass", token),
            "node_b in mv_id_seq's second epoch");

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

        // Nor can it by streaming the slot itself, as a role with REPLICATION can, while it speaks for node_c, even once
        // it has dropped its own slot.
        string nodeCId = await nodeC.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        _ = await nodeB.SafePsqlAsync(Database, "ALTER ROLE c_user LOGIN REPLICATION", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_create_logical_replication_slot('c_link', 'worf')", token);
        _ = await nodeB.SafePsqlAsync(Database, $"INSERT INTO worf.sequence_range_link VALUES ('c_link', {nodeCId}, 'c_user'::regrole)", token);
        string asNodeC = $$"""{"node_id": {{nodeCId}}, "node_name": "node_c", "slot": "c_link", "sysid": "1", "timeline": 1, "wal_lsn": "0/0", "sequences": []}""";
        _ = await nodeB.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION c_user; SELECT worf.sequence_range_exchange('{asNodeC}')", token);
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
            foreach (string before in new[] { "SELECT 1", "SELECT pg_drop_replication_slot('c_link')" })
            {
                _ = await nodeB.SafePsqlAsync(Database, before, token);
                PostgresException takeover = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
                    $"SET SESSION AUTHORIZATION c_user; SELECT worf.sequence_range_exchange('{forged}')", token), before);
                Assert.AreEqual("42501", takeover.SqlState, before);
                Assert.Contains("already speaks for node", takeover.MessageText, before);
            }

            await streaming.CancelAsync();
        }

        // A node re-created under another name, with the same subscription name and role, takes over its old node's slot
        // once the role's row is deleted, rather than being locked out by the slot's link.
        _ = await nodeB.SafePsqlAsync(Database, "DELETE FROM worf.sequence_range_role WHERE role_id = 'c_user'::regrole", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_create_logical_replication_slot('c_link', 'worf')", token);
        string recreated = $$"""{"node_id": 2000001, "node_name": "node_c_again", "slot": "c_link", "sysid": "1", "timeline": 1, "wal_lsn": "0/0", "sequences": []}""";
        _ = await nodeB.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION c_user; SELECT worf.sequence_range_exchange('{recreated}')", token);
        Assert.AreEqual("2000001", await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.sequence_range_link WHERE slot_name = 'c_link'", token));
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_drop_replication_slot('c_link')", token);

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
        const string MadeUpKept = "SELECT count(*) FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND node_name LIKE 'made_up_%'";
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database,
            $"SELECT count(*) BETWEEN 200 AND 256 FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND via_node = {nodeAId} AND node_id <> via_node",
            token), "the nodes node_b keeps from node_a's relays");
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database, """
            SELECT count(*) <= 256 FROM generate_series(1000001, 1001100) g WHERE worf.sequence_range_heard(g::oid, false, '-infinity', false) > '-infinity'
            """, token), "the made-up nodes node_b notes in memory");
        Assert.IsTrue(await nodeB.PollLogAsync(flooded, "or the 256 it keeps from one neighbor; they are ignored", token), "the ignored reports");

        // A call that relays them no more leaves none of them.
        _ = await nodeB.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{flood.Replace(madeUp, "")}')", token);
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, MadeUpKept, token), "the made-up nodes node_b forgets");
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

        // A watermark node_b makes up in its own report, beyond any a node could have heard of, node_a takes only two of
        // its largest blocks beyond its own, with a warning, so that the fence it brings on gives up no more as it lifts.
        string nodeBId = await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        string bSlot = await nodeA.SafePsqlAsync(Database, $"SELECT slot_name FROM worf.sequence_range_link WHERE node_id = {nodeBId}", token);
        string[] owned = (await nodeA.SafePsqlAsync(Database,
            "SELECT epoch, range_size, max_range_size, root_start, root_end FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token)).Split('|');
        string madeUpWatermark = $$"""
            {"node_id": {{nodeBId}}, "node_name": "node_b", "slot": "{{bSlot}}", "sysid": "1", "timeline": 1, "wal_lsn": "0/0", "sequences": [
                {"name": "public.t_id_seq", "owner_id": {{nodeAId}}, "epoch": {{owned[0]}}, "range_size": {{owned[1]}}, "max_range_size": {{owned[2]}},
                    "distance": 1, "high_water": null, "watermark": 9000000000000000000, "state": "active", "root_start": {{owned[3]}}, "root_end": {{owned[4]}},
                    "reset": null, "reset_done": null, "held_complete": false, "held": [], "requests": [], "reports": [], "grants": [], "acks": []}]}
            """;
        int forgedAt = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{madeUpWatermark}')", token);
        Assert.IsTrue(await nodeA.PollLogAsync(forgedAt, "node node_b reports grants up to 9000000000000000000, beyond any a node could have heard of", token),
            "node_a's warning of the made-up watermark");
        Assert.IsTrue(await nodeA.PollLogAsync(forgedAt, "the allocator's fence lifted", token), "node_a's fence lifted");
        Assert.AreEqual("t", await nodeA.SafePsqlAsync(Database, """
            SELECT r.watermark < 9000000000000000000 AND EXISTS (
                SELECT 1 FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
                WHERE b.seqoid = r.seqoid AND b.kind = 'r' AND f.lo <= r.watermark + 1)
            FROM worf.sequence_range r WHERE r.seqoid = 't_id_seq'::regclass
            """, token), "node_a's root kept above its bound on the made-up watermark");

        // node_a waits for node_c's own report after a reset while node_c is down, though node_b relays a fresh report of
        // node_c after node_a has lost its record of node_c's grants and reports, as a recovery to before them would, and
        // takes node_c's grants back into its ledger once node_c reports.
        string nodeCSlot = await nodeA.SafePsqlAsync(Database, $"SELECT slot_name FROM worf.sequence_range_link WHERE node_id = {nodeCId}", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.activate_sequence_range('t_id_seq')", token);
        await nodeC.StopAsync("fast", token);
        int downC = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, $"""
            SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass FOR NO KEY UPDATE;
            DELETE FROM worf.sequence_range_grant WHERE seqoid = 't_id_seq'::regclass AND grantee_id = {nodeCId};
            DELETE FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND node_id = {nodeCId};
            SELECT worf.reset_sequence_range('t_id_seq');
            INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, epoch, state, reported_at)
            SELECT seqoid, {nodeCId}, 'node_c', {nodeBId}, epoch, 'active', now() + interval '1 hour' FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass;
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database,
            "SELECT fenced_at IS NOT NULL AND detail LIKE 'fenced until node node_c reports%' FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token),
            "node_a's fence waiting for node_c alone, once node_b has reported");
        await nodeC.StartServerAsync(token);
        Assert.IsTrue(await nodeA.PollLogAsync(downC, "node node_c holds block", token), "node_c's grants back in node_a's ledger");
        Assert.IsTrue(await nodeA.PollLogAsync(downC, "the allocator's fence lifted", token), "node_a's fence lifted once node_c reported");

        // Once the subscriptions between node_a and node_c are dropped, node_c leaving its slot as it drops its own, node_a
        // forgets node_c, and a fence no longer waits for it while it is down.
        _ = await nodeC.SafePsqlAsync(Database, "SELECT worf.drop_subscription('c_from_a')", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.drop_subscription('a_from_c')", token);
        Assert.AreEqual("f", await nodeA.SafePsqlAsync(Database,
            $"SELECT EXISTS (SELECT 1 FROM worf.sequence_range_link WHERE slot_name = '{nodeCSlot}' OR node_id = {nodeCId})", token), "node_c's slot left");
        await nodeC.StopAsync("fast", token);
        int forgotten = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('t_id_seq')", token);
        Assert.IsTrue(await nodeA.PollLogAsync(forgotten, "the allocator's fence lifted", token), "node_a's fence lifted without node_c");
    }

    /// <summary>
    /// In a cascade, a node two links from the owner gets its blocks from its parent's pool block, and a node whose
    /// link to the owner is dropped finds the owner through another neighbor, which relays its reports without writing.
    /// A late joiner whose values reach into the root is refused by the pool holder it asks, one sequence's failure
    /// leaves the others' exchanges alone, and a pool holder's standby, promoted without the grants its primary made
    /// last, fences its pool until its children report, and takes the grants it lost back into its ledger. A pool
    /// holder's Snowflake node numbers stay its while it reports, and it grants none of them once it hasn't heard from
    /// the owner for half a day.
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

        // With eighteen Snowflake node numbers, node_a and node_b take one each, and node_b a pool block of the other
        // sixteen, from which node_c takes its own. A day after the pool's grant, the numbers node_b has yet to grant on
        // stay its, though it reports holding only its own, and the root has none left.
        _ = await nodeA.SafePsqlAsync(Database, "ALTER SEQUENCE worf.snowflake_node MAXVALUE 17; SELECT worf.enable_snowflake('{node_b,node_c}')", token);
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT worf.snowflake_node() IS NOT NULL", token), $"{node.Name}'s node number");
        }

        string[] pool = (await nodeA.SafePsqlAsync(Database,
            "SELECT lo, hi FROM worf.sequence_range_grant WHERE seqoid = 'worf.snowflake_node'::regclass AND kind = 'p'", token)).Split('|');
        Assert.AreEqual("t", await nodeC.SafePsqlAsync(Database, $"SELECT worf.snowflake_node() BETWEEN {pool[0]} AND {pool[1]}", token), "node_c's number from node_b's pool");
        // Nor does node_b's own number, a day after its grant, come back on a report of node_b that lists no number and
        // that another node relays, as one it made up would be, while node_b doesn't report itself.
        string nodeBNumber = await nodeB.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token);
        string nodeCId = await nodeC.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('b_from_a')", token);
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        int reused = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, $"""
            SELECT 1 FROM worf.sequence_range WHERE seqoid = 'worf.snowflake_node'::regclass FOR NO KEY UPDATE;
            UPDATE worf.sequence_range_grant SET granted_at = now() - interval '2 days'
            WHERE seqoid = 'worf.snowflake_node'::regclass AND (kind = 'p' OR grantee_id = {nodeBId});
            UPDATE worf.sequence_range_node SET via_node = {nodeCId}, holds = ARRAY[]::bigint[], reported_at = now()
            WHERE seqoid = 'worf.snowflake_node'::regclass AND node_id = {nodeBId};
            """, token);
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.DoesNotContain("are free again", nodeA.ReadLog()[reused..], "node_b's numbers given back");
        Assert.AreEqual("0", await nodeA.SafePsqlAsync(Database, $"""
            SELECT count(*) FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
            WHERE b.seqoid = 'worf.snowflake_node'::regclass AND (f.lo <= {pool[1]} AND {pool[0]} <= f.hi OR f.lo <= {nodeBNumber} AND {nodeBNumber} <= f.hi)
            """, token), "node_b's numbers free in the root");
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('b_from_a')", token);
        Assert.HasCount(3, (await Task.WhenAll(nodes.Select(node => node.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token)))).Distinct());

        // node_a goes down, and node_b and node_c restart, having last heard from it half a day ago as far as they know.
        // node_c gives up its number and asks node_b for another, which grants none from its pool, whose numbers node_a
        // may give out again a day after it last heard from node_b, until it hears from node_a again.
        string ownerId = await nodeA.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        string ownerNumber = await nodeA.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token);
        _ = await nodeC.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('c_from_b')", token);
        await nodeA.StopAsync("fast", token);
        foreach (TestNode node in new[] { nodeB, nodeC })
        {
            await node.StopAsync("fast", token);
            await node.StartServerAsync(token);
            _ = await node.SafePsqlAsync(Database, $"UPDATE worf.sequence_range_node SET reported_at = now() - interval '13 hours' WHERE node_id = {ownerId}", token);
        }

        // Stale routes between node_b and node_c, which count upward for a while, still carry node_c's requests.
        string restarted = await nodeB.SafePsqlAsync(Database, "SELECT now()::text", token);
        _ = await nodeC.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('c_from_b')", token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_request WHERE seqoid = 'worf.snowflake_node'::regclass)", token), "node_c asking for a number");
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, $"""
            SELECT count(*) FROM worf.sequence_range_grant
            WHERE seqoid = 'worf.snowflake_node'::regclass AND grantee_name = 'node_c' AND granted_at > '{restarted}'
            """, token), "numbers for node_c from node_b's pool");
        await nodeA.StartServerAsync(token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, "SELECT worf.snowflake_node() IS NOT NULL", token), "node_c's number once node_b hears from node_a");
        Assert.AreNotEqual(ownerNumber, await nodeC.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token), "node_c's number and node_a's");
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
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_values('t_id_seq', 1000000000000, 1000000000099)", token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t (id, v) SELECT g, 'imported' FROM generate_series(1000000000000, 1000000000099) g", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_values('t_id_seq', 2000000000000, 2000000000009)", token);
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t (id, v) SELECT g, 'imported' FROM generate_series(2000000000000, 2000000000009) g", token);
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

        // A load's transaction holds the sequence from its reservation's switch until it commits, and the range worker
        // leaves the sequence for later rather than wait on it. Neither that load nor one after a reservation of its own,
        // here from several sessions at once, faster than any before, counts toward the sequence's rate, nor do their
        // switches toward the size of its next blocks. node_a settles first: it holds its next block, and nothing is asked
        // for or granted but the refusal, which moves no block.
        async Task SettleBlocksAsync()
        {
            Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, """
                SELECT next_lo IS NOT NULL
                    AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_request)
                    AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_grant WHERE state = 'sent' AND kind <> 'x')
                FROM worf.sequence_ranges
                """, token), "node_a holding its next block");
            await Task.Delay(1000, token);
        }

        await SettleBlocksAsync();
        string[] before = (await nodeA.SafePsqlAsync(Database, "SELECT request_size, peak_rate FROM worf.sequence_range", token)).Split('|');
        await using (NpgsqlConnection load = await nodeA.OpenAsync(Database, token))
        {
            await ExecuteAsync(load, "BEGIN; SELECT worf.reserve_sequence_range('t_id_seq', 5000)", token);
            await Task.Delay(1000, token);
            Assert.AreEqual("", await nodeA.SafePsqlAsync(Database, """
                SELECT string_agg(a.backend_type, ', ') FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                WHERE a.backend_type LIKE 'worf%' AND NOT l.granted
                """, token), "no worker waiting on the load");
            await ExecuteAsync(load, "INSERT INTO t (v) SELECT 'load' FROM generate_series(1, 5000); COMMIT", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_range('t_id_seq', 10000)", token);
        await Task.WhenAll(Enumerable.Range(0, 4).Select(_ =>
            nodeA.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'load' FROM generate_series(1, 2500)", token)));
        await SettleBlocksAsync();
        string[] after = (await nodeA.SafePsqlAsync(Database, "SELECT request_size, peak_rate FROM worf.sequence_range", token)).Split('|');
        Assert.AreEqual(before[0], after[0], "the request size");
        Assert.AreEqual(before[1], after[1], "the peak rate");

        // Nor does a load's transaction hold up another session's reservation, whose switch it then finds when it
        // reserves again.
        await using (NpgsqlConnection batch = await nodeA.OpenAsync(Database, token))
        {
            await ExecuteAsync(batch, "BEGIN; SELECT worf.reserve_sequence_range('t_id_seq', 10)", token);
            long left = long.Parse(await nodeA.SafePsqlAsync(Database, "SELECT values_left FROM worf.sequence_ranges", token), CultureInfo.InvariantCulture);
            _ = await nodeA.SafePsqlAsync(Database, $"SET statement_timeout = '10s'; SELECT worf.reserve_sequence_range('t_id_seq', {left + 100})", token);
            await ExecuteAsync(batch, "SELECT worf.reserve_sequence_range('t_id_seq', 10); INSERT INTO t (v) SELECT 'batch' FROM generate_series(1, 10); COMMIT", token);
        }

        // A load's transaction on node_b, whose reservation the range worker switched for, holds nothing that the range
        // worker or the exchange worker waits on, and another sequence there keeps switching meanwhile, to the blocks node_a
        // grants it.
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE u (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('u_id_seq', participants := '{node_b}', range_size := 10, max_range_size := 10)", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 'u_id_seq'::regclass",
            token), "node_b drawing from a block of u_id_seq");
        await using NpgsqlConnection hold = await nodeB.OpenAsync(Database, token);
        long heldLeft = long.Parse(await nodeB.SafePsqlAsync(Database, "SELECT values_left FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token),
            CultureInfo.InvariantCulture);
        await ExecuteAsync(hold, $"BEGIN; SELECT worf.reserve_sequence_range('t_id_seq', {heldLeft + 1000})", token);
        for (int i = 0; i < 3; i++)
        {
            string active = await nodeB.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 'u_id_seq'::regclass", token);
            _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO u (v) SELECT 'u' FROM generate_series(1, (SELECT values_left FROM worf.sequence_ranges WHERE sequence = 'u_id_seq'::regclass))",
                token);
            Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"""
                SELECT active_lo <> {active} AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 'u_id_seq'::regclass
                """, token), "node_b switching u_id_seq while t_id_seq is held");
        }

        await ExecuteAsync(hold, "INSERT INTO t (v) SELECT 'held' FROM generate_series(1, 1000); COMMIT", token);

        // node_b drops u and creates it again. The new sequence joins node_a's epoch as it is created, so that node_b
        // issues none of its own values, which node_a's blocks may hold, before its blocks come.
        _ = await nodeB.SafePsqlAsync(Database, "DROP TABLE u; CREATE TABLE u (id bigserial PRIMARY KEY, v text)", token);
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database, "SELECT EXISTS (SELECT 1 FROM worf.sequence_range WHERE seqoid = 'u_id_seq'::regclass)", token),
            "node_b's new u_id_seq joined as it was created");
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'u_id_seq'::regclass", token),
            "node_b drawing from a block of its new u_id_seq");
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database, """
            WITH i AS (INSERT INTO u (v) VALUES ('again') RETURNING id)
            SELECT i.id BETWEEN r.active_lo AND r.active_hi FROM i, worf.sequence_ranges r WHERE r.sequence = 'u_id_seq'::regclass
            """, token), "node_b's key from its block");

        // node_b hears of grants beyond node_a's watermark, as when node_a, the owner, went back to before them by less
        // than its WAL position shows. node_a resets, and its fence holds, here on a neighbor that never reports, while
        // node_b asks for a block. With an exchange interval of ten minutes, node_b still asks node_a every ten checks,
        // and draws from a block beyond that watermark soon after the fence lifts.
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_exchange_interval = '10min'; SELECT pg_reload_conf()", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_create_physical_replication_slot('ghost'); INSERT INTO worf.sequence_range_link VALUES ('ghost', 4242, 10)", token);
        string beyond = await nodeB.SafePsqlAsync(Database, "UPDATE worf.sequence_range SET watermark = watermark + 1000000 WHERE seqoid = 't_id_seq'::regclass RETURNING watermark", token);
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

        // Dropping the slot a node that is gone used, as drop_subscription asks when it can't reach the provider, lifts the
        // fence as deleting its link would.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_drop_replication_slot('ghost')", token);
        Stopwatch lifted = Stopwatch.StartNew();
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"SELECT state = 'active' AND active_lo > {beyond} FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token),
            "node_b drawing from a block beyond the watermark it heard of");
        Assert.IsLessThan(3.0, lifted.Elapsed.TotalSeconds, "node_b's block soon after node_a's fence lifted");
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT state = 'active' AND active_lo > {beyond} FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token),
            "node_a drawing from a block beyond the watermark");
    }

    /// <summary>
    /// Many sessions on a node that asks the owner for each of its 1,000-value blocks run through them at the default
    /// intervals, and none fails at a block's end. The node's exchanges start once its subscription has made its slot,
    /// which ties them to it.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ConcurrentInsertsOnANonOwnerOutlastSmallBlocks()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                ALTER SYSTEM RESET worf.sequence_range_check_interval;
                ALTER SYSTEM RESET worf.sequence_range_exchange_interval;
                SELECT pg_reload_conf();
                CREATE TABLE t (id bigserial PRIMARY KEY, v int);
                """, token);
        }

        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 1000, max_range_size := 1000)", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges", token), "node_b drawing from a block");
        Assert.DoesNotContain("carries this sequence range exchange", nodeA.ReadLog(), "node_a's log");

        const int Sessions = 16;
        const int Inserts = 500;
        await Task.WhenAll(Enumerable.Range(0, Sessions).Select(async session =>
        {
            await using NpgsqlConnection connection = await nodeB.OpenAsync(Database, token);
            await using NpgsqlCommand insert = new($"INSERT INTO t (v) VALUES ({session})", connection);
            for (int i = 0; i < Inserts; i++)
            {
                _ = await insert.ExecuteNonQueryAsync(token);
            }
        }));

        Assert.AreEqual($"{Sessions * Inserts}|{Sessions * Inserts}", await nodeB.SafePsqlAsync(Database, "SELECT count(*), count(DISTINCT id) FROM t", token));
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
                (SELECT count(*) FROM pg_replication_slots WHERE plugin = 'worf' AND database = current_database()), pg_current_wal_insert_lsn()
            """;
        string[][] before = await Task.WhenAll(nodes.Select(async node => (await node.SafePsqlAsync(Database, Next, token)).Split('|')));
        await Task.Delay(idle, token);
        string[][] after = await Task.WhenAll(nodes.Select(async node => (await node.SafePsqlAsync(Database, Next, token)).Split('|')));
        for (int i = 0; i < nodes.Length; i++)
        {
            Assert.AreEqual(before[i][0], after[i][0], $"{nodes[i].Name} used transaction IDs while idle");
            Assert.IsLessThanOrEqualTo((calls * long.Parse(after[i][2], CultureInfo.InvariantCulture)) + 2,
                long.Parse(after[i][1], CultureInfo.InvariantCulture) - long.Parse(before[i][1], CultureInfo.InvariantCulture), $"{nodes[i].Name}'s exchange calls");
            if (before[i][3] != after[i][3])
            {
                // An insert position at a page's start lies past the page's header, which is written only with the page's
                // first record, so the WAL is flushed once it reaches the page's start.
                string flushed = $"""
                    CASE WHEN ('{after[i][3]}'::pg_lsn - '0/0') % 8192 <= 40 THEN '{after[i][3]}'::pg_lsn - ('{after[i][3]}'::pg_lsn - '0/0') % 8192
                        ELSE '{after[i][3]}'::pg_lsn END
                    """;
                Assert.IsTrue(await nodes[i].PollQueryUntilAsync(Database, $"SELECT pg_current_wal_flush_lsn() >= {flushed}", token),
                    $"{nodes[i].Name} flushing its WAL");
                Assert.AreEqual("", await nodes[i].SafePsqlAsync(Database, $"""
                    SELECT string_agg(resource_manager || ' ' || record_type, ', ')
                    FROM pg_get_wal_records_info('{before[i][3]}', greatest('{before[i][3]}'::pg_lsn + 1, least('{after[i][3]}'::pg_lsn, pg_current_wal_flush_lsn())))
                    WHERE resource_manager <> 'Standby' AND record_type NOT IN ('CHECKPOINT_REDO', 'CHECKPOINT_ONLINE', 'FPI_FOR_HINT')
                        AND NOT (resource_manager = 'Heap2' AND record_type IN ('PRUNE', 'PRUNE_ON_ACCESS'))
                    """, token), $"{nodes[i].Name} wrote WAL while idle");
            }
        }
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken token)
    {
        await using NpgsqlCommand command = new(sql, connection);
        _ = await command.ExecuteNonQueryAsync(token);
    }

    /// <summary>
    /// Locks a node's sequence range table, so that its neighbors' exchanges with it wait until they time out, and
    /// returns once one waits. Closing the connection lets them go.
    /// </summary>
    private static async Task<NpgsqlConnection> HoldExchangesAsync(TestNode node, CancellationToken token)
    {
        NpgsqlConnection connection = await node.OpenAsync(Database, token);
        try
        {
            await ExecuteAsync(connection, "BEGIN; LOCK TABLE worf.sequence_range IN ACCESS EXCLUSIVE MODE", token);

            Assert.IsTrue(await node.PollQueryUntilAsync(Database, """
                SELECT EXISTS (
                    SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                    WHERE l.relation = 'worf.sequence_range'::regclass AND NOT l.granted AND a.query LIKE '%sequence_range_exchange%')
                """, token), $"an exchange waiting on {node.Name}");
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
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

        // A server that recovers refuses connections until it is consistent.
        Stopwatch starting = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                _ = await node.SafePsqlAsync(Database, "SELECT 1", token);
                break;
            }
            catch (PostgresException error) when (error.SqlState == "57P03" && starting.Elapsed < TestNode.PollTimeout)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            }
        }
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

            // For the idle checks, which read the WAL; created now, before the DDL can replicate.
            _ = await node.SafePsqlAsync(Database, "CREATE EXTENSION pg_walinspect", token);
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
