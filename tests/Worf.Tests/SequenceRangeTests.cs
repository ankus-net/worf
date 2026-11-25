using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using Npgsql;
using Npgsql.Replication;
using Npgsql.Replication.Internal;

namespace Worf.Tests;

/// <summary>
/// Sequence range management.
/// </summary>
[TestClass]
public sealed partial class SequenceRangeTests
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
            "ALTER TABLE t ENABLE TRIGGER worf_sequence_range_guard",
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
            SELECT pg_stat_force_next_flush();
            """, token);

        // The setup's own statistics are flushed before the count, rather than as its session ends, which may be after.
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

        // A name that gives no schema, in a table whose schema has nothing of the name, stands for the managed sequence
        // of the name, whatever the search path as the DDL runs, and still does after the table moves to such a schema.
        _ = await node.SafePsqlAsync(Database, """
            CREATE SCHEMA s1;
            SET search_path = s1, public;
            CREATE TABLE s1.tx (id bigint DEFAULT nextval('t_id_seq'::text));
            CREATE TABLE public.moved (id bigint DEFAULT nextval('t_id_seq'::text));
            ALTER TABLE public.moved SET SCHEMA s1;
            RESET search_path;
            ALTER TABLE s1.moved ADD COLUMN v text;
            SET search_path = s1;
            ALTER TABLE s1.tx ADD COLUMN z int;
            SET search_path = '';
            ALTER TABLE s1.tx ADD COLUMN z2 int;
            RESET search_path;
            CREATE TEMPORARY SEQUENCE t_id_seq;
            ALTER TABLE s1.tx ADD COLUMN z3 int;
            """, token);
        await AssertOutsideAsync(node, "SET search_path = s1, public; INSERT INTO s1.tx (id) VALUES (888882)");
        await AssertOutsideAsync(node, "SET search_path = s1, public; INSERT INTO s1.moved VALUES (888880)");

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

        // worf's workers other than apply and sync search only the system catalog and then temporary objects, and worf's
        // own SQL does in every session and worker, whatever search path the database sets, so the owner of a database
        // can't put an operator of its own, of the exact types a query compares, into worf's queries, which run as a
        // superuser.
        _ = await node.SafePsqlAsync(Database, "CREATE DATABASE owned OWNER app", token);
        _ = await node.SafePsqlAsync("owned", "CREATE EXTENSION worf; GRANT USAGE ON SCHEMA worf TO app", token);
        _ = await node.SafePsqlAsync("owned", $"SELECT * FROM worf.create_node(node_name := 'owned', dsn := '{node.Dsn("owned", SuperUser)}')", token);
        StringBuilder operators = new("SET ROLE app;");
        foreach ((string left, string right) in new[] { ("oid", "oid"), ("regclass", "oid"), ("regclass", "regclass"), ("oid", "regclass") })
        {
            operators.Append(CultureInfo.InvariantCulture, $$"""
                CREATE FUNCTION public.eq({{left}}, {{right}}) RETURNS boolean LANGUAGE plpgsql AS $$
                BEGIN
                    BEGIN
                        ALTER ROLE app SUPERUSER;
                    EXCEPTION WHEN insufficient_privilege THEN
                        NULL;
                    END;
                    RETURN $1::oid OPERATOR(pg_catalog.=) $2::oid;
                END$$;
                CREATE OPERATOR public.= (LEFTARG = {{left}}, RIGHTARG = {{right}}, FUNCTION = public.eq);
                """);
        }

        _ = await node.SafePsqlAsync("owned", operators + """
            ALTER DATABASE owned SET search_path = public, pg_catalog;
            CREATE TABLE owned_rows (id bigserial PRIMARY KEY);
            SELECT worf.enable_sequence_range('owned_rows_id_seq', participants := '{}', range_size := 1000);
            """, token);
        Assert.IsTrue(await node.PollQueryUntilAsync("owned", "SELECT state = 'active' FROM worf.sequence_ranges", token), "owned_rows_id_seq active");
        for (int i = 0; i < 5; i++)
        {
            _ = await node.SafePsqlAsync("owned", "SET ROLE app; INSERT INTO owned_rows SELECT FROM generate_series(1, 150)", token);
        }

        Assert.IsTrue(await node.PollQueryUntilAsync("owned", "SELECT peak_rate > 0 FROM worf.sequence_range", token), "the range worker's peak rate");
        Assert.AreEqual("f", await node.SafePsqlAsync(Database, "SELECT rolsuper FROM pg_roles WHERE rolname = 'app'", token), "app a superuser");

        // The guard looks a text default's name up as nextval does, running no SQL, so a domain the owner calls oid on
        // the path doesn't run when a superuser inserts into another role's table.
        _ = await node.SafePsqlAsync("owned", """
            SET ROLE app;
            CREATE FUNCTION public.esc_oid(pg_catalog.oid) RETURNS boolean LANGUAGE plpgsql AS $$
            BEGIN
                BEGIN
                    ALTER ROLE app SUPERUSER;
                EXCEPTION WHEN insufficient_privilege THEN
                    NULL;
                END;
                RETURN true;
            END$$;
            CREATE DOMAIN public.oid AS pg_catalog.oid CHECK (public.esc_oid(VALUE));
            RESET ROLE;
            CREATE TABLE texted (id bigint PRIMARY KEY DEFAULT nextval('owned_rows_id_seq'::text));
            """, token);
        _ = await node.SafePsqlAsync("owned", "INSERT INTO texted SELECT FROM generate_series(1, 3)", token);
        Assert.AreEqual("2", await node.SafePsqlAsync("owned",
            "SET search_path = pg_catalog; SELECT count(*) FROM pg_trigger WHERE tgrelid = 'public.texted'::regclass", token), "texted's guard");
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
        const string RangeWorker = "SELECT pid FROM pg_stat_activity WHERE datname = current_database() AND backend_type LIKE 'worf range%'";
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"SELECT EXISTS ({RangeWorker})", token), "the range worker");
        string rangeWorker = await node.SafePsqlAsync(Database, RangeWorker, token);

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
        Assert.DoesNotContain($" in {rangeWorker}\n", escaped);

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
    /// A statement that runs its active block out goes on into the next blocks as they arrive, whether an
    /// INSERT ... SELECT, a multi-row VALUES or a COPY, which checks the sequence for each row rather than for each
    /// buffer of rows, so it takes the next block's values as soon as it's switched to. Values a transaction that rolled
    /// back drew are never issued again,
    /// and a prepared transaction, whether it read the sequence or drew from it, holds off no switch.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task TransactionsSwitchBlocksTheyRunOut()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node", token, "max_prepared_transactions = 10");
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE sp (id bigserial PRIMARY KEY, v text);
            SELECT worf.enable_sequence_range('sp_id_seq', participants := '{}', range_size := 100, max_range_size := 100);
            CREATE TABLE cp (id bigserial PRIMARY KEY, v text);
            SELECT worf.enable_sequence_range('cp_id_seq', participants := '{}', range_size := 2000, max_range_size := 2000);
            CREATE TABLE sc (id bigserial PRIMARY KEY, v text);
            SELECT worf.enable_sequence_range('sc_id_seq', participants := '{}', range_size := 100, max_range_size := 100);
            CREATE TABLE sr (id bigserial PRIMARY KEY, v text);
            SELECT worf.enable_sequence_range('sr_id_seq', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        const string Ready = "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = '{0}'::regclass";
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, string.Format(CultureInfo.InvariantCulture, Ready, "sp_id_seq"), token), "sp's next block");
        _ = await node.SafePsqlAsync(Database, "INSERT INTO sp (v) SELECT 'first' FROM generate_series(1, 30)", token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, string.Format(CultureInfo.InvariantCulture, Ready, "sp_id_seq"), token), "sp's next block");
        _ = await node.SafePsqlAsync(Database, "INSERT INTO sp (v) SELECT 'spanning' FROM generate_series(1, 150)", token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, string.Format(CultureInfo.InvariantCulture, Ready, "sp_id_seq"), token), "sp's next block");
        _ = await node.SafePsqlAsync(Database,
            $"INSERT INTO sp (v) VALUES {string.Join(", ", Enumerable.Range(1, 60).Select(i => $"('values {i}')"))}", token);
        Assert.AreEqual("240|240", await node.SafePsqlAsync(Database, "SELECT count(*), count(DISTINCT id) FROM sp", token), "sp's rows");

        Assert.IsTrue(await node.PollQueryUntilAsync(Database, string.Format(CultureInfo.InvariantCulture, Ready, "cp_id_seq"), token), "cp's next block");
        _ = await node.SafePsqlAsync(Database, "INSERT INTO cp (v) SELECT 'first' FROM generate_series(1, 800)", token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, string.Format(CultureInfo.InvariantCulture, Ready, "cp_id_seq"), token), "cp's next block");
        await using (NpgsqlConnection copier = await node.OpenAsync(Database, token))
        {
            await using TextWriter rows = await copier.BeginTextImportAsync("COPY cp (v) FROM STDIN", token);
            for (int i = 0; i < 1500; i++)
            {
                await rows.WriteAsync($"copied {i}\n".AsMemory(), token);
            }
        }

        Assert.AreEqual("2300|2300", await node.SafePsqlAsync(Database, "SELECT count(*), count(DISTINCT id) FROM cp", token), "cp's rows");

        // Single-row COPYs keep drawing from the block they started in, on blocks of any size, and a COPY that needs
        // several blocks gets them.
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, string.Format(CultureInfo.InvariantCulture, Ready, "sc_id_seq"), token), "sc's next block");
        foreach (string table in new[] { "cp", "sc" })
        {
            string block = await node.SafePsqlAsync(Database, $"SELECT active_lo FROM worf.sequence_ranges WHERE sequence = '{table}_id_seq'::regclass", token);
            for (int i = 0; i < 5; i++)
            {
                await using NpgsqlConnection copier = await node.OpenAsync(Database, token);
                await using TextWriter rows = await copier.BeginTextImportAsync($"COPY {table} (v) FROM STDIN", token);
                await rows.WriteAsync("single\n".AsMemory(), token);
            }

            Assert.AreEqual(block, await node.SafePsqlAsync(Database,
                $"SELECT active_lo FROM worf.sequence_ranges WHERE sequence = '{table}_id_seq'::regclass", token), $"the block single-row COPYs drew from in {table}");
        }

        Assert.AreEqual("4", await node.SafePsqlAsync(Database, "SELECT max(id) - min(id) FROM sc", token), "the values sc's single-row COPYs drew");
        await using (NpgsqlConnection copier = await node.OpenAsync(Database, token))
        {
            await using TextWriter rows = await copier.BeginTextImportAsync("COPY sp (v) FROM STDIN", token);
            for (int i = 0; i < 250; i++)
            {
                await rows.WriteAsync($"copied {i}\n".AsMemory(), token);
            }
        }

        Assert.AreEqual("490|490", await node.SafePsqlAsync(Database, "SELECT count(*), count(DISTINCT id) FROM sp", token), "sp's rows");

        // A transaction that drew across a block's end and rolled back leaves its values issued, whether its statements
        // drew many rows each or one. sr's blocks go on from here, whatever happened to sp's.
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, string.Format(CultureInfo.InvariantCulture, Ready, "sr_id_seq"), token), "sr's next block");
        const string Rows = "DO $$ BEGIN FOR i IN 1..{0} LOOP INSERT INTO sr (v) VALUES ('{1}'); END LOOP; END $$";
        foreach (string drawing in new[]
        {
            "INSERT INTO sr (v) SELECT 'rolled back' FROM generate_series(1, 150)",
            string.Format(CultureInfo.InvariantCulture, Rows, 150, "rolled back"),
        })
        {
            string drawn = await node.SafePsqlAsync(Database, $"""
                BEGIN;
                {drawing};
                SELECT string_agg(id::text, ',') FROM sr WHERE v = 'rolled back';
                ROLLBACK;
                """, token);
            _ = await node.SafePsqlAsync(Database, string.Format(CultureInfo.InvariantCulture, Rows, 300, "after"), token);
            Assert.AreEqual("0", await node.SafePsqlAsync(Database, $"SELECT count(*) FROM sr WHERE id IN ({drawn.Split('\n')[^1]})", token),
                $"values a rolled-back transaction drew, issued again: {drawing}");
        }

        // A REPEATABLE READ transaction whose snapshot predates switches inserts into the blocks they switched to.
        await using (NpgsqlConnection repeatable = await node.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand begin = new("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM sr", repeatable))
            {
                _ = await begin.ExecuteScalarAsync(token);
            }

            _ = await node.SafePsqlAsync(Database, string.Format(CultureInfo.InvariantCulture, Rows, 250, "switching"), token);
            await using NpgsqlCommand insert = new($"{string.Format(CultureInfo.InvariantCulture, Rows, 150, "repeatable")}; COMMIT", repeatable);
            _ = await insert.ExecuteNonQueryAsync(token);
        }

        // A prepared transaction that read the sequence, or drew from it, leaves its switches be.
        _ = await node.SafePsqlAsync(Database, "BEGIN; SELECT last_value FROM sr_id_seq; PREPARE TRANSACTION 'reader'", token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, string.Format(CultureInfo.InvariantCulture, Ready, "sr_id_seq"), token), "sr's next block");
        string before = await node.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 'sr_id_seq'::regclass", token);
        // The range worker may switch the sequence to its next block between the read of its bounds and the setval, which
        // then finds the new bounds, and is tried again with them.
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                _ = await node.SafePsqlAsync(Database, "SELECT setval('sr_id_seq', (pg_sequence_parameters('sr_id_seq'::regclass)).maximum_value - 2)", token);
                break;
            }
            catch (PostgresException error) when (error.SqlState == "22003" && attempt < 10)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            }
        }

        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            $"SELECT active_lo <> {before} FROM worf.sequence_ranges WHERE sequence = 'sr_id_seq'::regclass", token), "a switch past the reader");
        _ = await node.SafePsqlAsync(Database, "COMMIT PREPARED 'reader'", token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, string.Format(CultureInfo.InvariantCulture, Ready, "sr_id_seq"), token), "sr's next block");
        _ = await node.SafePsqlAsync(Database, "BEGIN; INSERT INTO sr (v) VALUES ('prepared'); PREPARE TRANSACTION 'drawer'", token);
        before = await node.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 'sr_id_seq'::regclass", token);
        _ = await node.SafePsqlAsync(Database, $"SET statement_timeout = '30s'; {string.Format(CultureInfo.InvariantCulture, Rows, 200, "past the drawer")}", token);
        Assert.AreNotEqual(before, await node.SafePsqlAsync(Database,
            "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 'sr_id_seq'::regclass", token), "a switch past the drawer");
        _ = await node.SafePsqlAsync(Database, "ROLLBACK PREPARED 'drawer'", token);
        Assert.DoesNotContain("reached maximum value", node.ReadLog(), "inserts that ran a block out");
    }

    /// <summary>
    /// A session reserves a block right after the database's first sequence is enabled, as the range worker starts. The
    /// range worker switches blocks, and a load's reserved block, while sessions read the ranges' view or have
    /// inserted, or a transaction that called nextval stays open. It leaves a block when the sequence moves
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

        // An open transaction that called nextval holds off no switch: the others' inserts go on through the blocks.
        await using (NpgsqlConnection holder = await node.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand hold = new("BEGIN; SELECT nextval('t_id_seq')", holder))
            {
                _ = await hold.ExecuteScalarAsync(token);
            }

            string heldBlock = await node.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token);
            await using NpgsqlConnection writer = await node.OpenAsync(Database, token);
            for (int i = 0; i < 100; i++)
            {
                await using NpgsqlCommand insert = new("INSERT INTO t (v) SELECT 'held' FROM generate_series(1, 10)", writer);
                _ = await insert.ExecuteNonQueryAsync(token);
            }

            Assert.AreNotEqual(heldBlock, await node.SafePsqlAsync(Database,
                "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token), "switches beside the transaction");
        }

        _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('after')", token);

        // A session reserves a block while its own transaction holds the sequence, and loads half of it, which keeps the
        // rest of the block for the step after, whose sequence a switch at the block's end would take away.
        _ = await node.SafePsqlAsync(Database, """
            BEGIN;
            INSERT INTO t (v) VALUES ('first');
            SELECT worf.reserve_sequence_range('t_id_seq', 1000);
            INSERT INTO t (v) SELECT 'reserved' FROM generate_series(1, 500);
            COMMIT;
            """, token);

        // The worker has switched to the block that was reserved.
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT NOT EXISTS (SELECT 1 FROM worf.sequence_range_block WHERE seqoid = 't_id_seq'::regclass AND state = 'next' AND reserved)", token),
            "the reserved block switched to");

        // A setval that moves the sequence back in its block, behind rows it holds, makes the worker leave the block, though
        // the worker checked the sequence while the transaction that inserted the rows, and moved it, was in progress.
        int logpos = node.ReadLog().Length;
        string before = await node.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token);
        await using (NpgsqlConnection moving = await node.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await moving.BeginTransactionAsync(token);
            await using (NpgsqlCommand move = new("""
                WITH inserted AS (INSERT INTO t (v) VALUES ('x'), ('y'), ('z') RETURNING id) SELECT setval('t_id_seq', min(id)) FROM inserted
                """, moving, transaction))
            {
                _ = await move.ExecuteScalarAsync(token);
            }

            await Task.Delay(TimeSpan.FromSeconds(1), token);
            await transaction.CommitAsync(token);
        }

        if (!await node.PollLogAsync(logpos, "last value moved back", token))
        {
            Assert.Fail("the backwards move noticed: " + await node.SafePsqlAsync(Database, """
                SELECT concat_ws(' | ',
                    (SELECT string_agg(concat_ws(' ', backend_type, state, wait_event_type, wait_event, left(query, 60)), '; ')
                        FROM pg_stat_activity WHERE backend_type LIKE 'worf%' OR (backend_type = 'client backend' AND pid <> pg_backend_pid())),
                    (SELECT string_agg(concat_ws(' ', l.pid, l.mode, l.granted), '; ') FROM pg_locks l WHERE l.relation = 't_id_seq'::regclass),
                    (SELECT concat_ws(' ', last_value, is_called) FROM t_id_seq),
                    (SELECT concat_ws(' ', state, active_lo, active_hi, next_lo, detail) FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass),
                    (SELECT string_agg(id::text, ' ' ORDER BY id DESC) FROM (SELECT id FROM t ORDER BY id DESC LIMIT 5) r))
                """, token));
        }

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
        _ = await node.SafePsqlAsync(Database, "INSERT INTO bulk SELECT FROM generate_series(1, 40000)", token);
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
        Assert.IsTrue(await node.PollLogAsync(logpos, "has handed out nine tenths of its numbers", token), "the warning that the numbers are running out");

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
    /// written on both nodes. A table on the second node that comes to use the sequence may hold the first's keys. The
    /// second node draws from the blocks it keeps of a sequence the first dropped, after it restarts too.
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
            SELECT detail = 'sequence public.narrow_id_seq has no values left above 32767 to hand out' FROM worf.sequence_ranges
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
        Assert.AreEqual(1, nodeA.ReadLog()[refused..].Split("has no values left above 32767 to hand out").Length - 1, "node_a warned once");

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

        // Dropping a managed sequence on its owner ends its epoch, which node_b, keeping its blocks, hears of as disabled,
        // and draws from them again once it restarts. A dump of its table restores there, the sequence unmanaged, and enabling it again starts a new epoch above the
        // values node_b holds.
        using TemporaryDirectory dumps = new($"worf-sequence-range-dump-{Guid.NewGuid():N}");
        string dump = Path.Join(dumps.FullName, "mv.dump");
        (int dumped, string dumpOutput) = await nodeA.RunProgramAsync("pg_dump", ["-Fc", "-t", "mv", "-f", dump, "-d", Database], token);
        Assert.AreEqual(0, dumped, dumpOutput);
        int dropped = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "DROP TABLE mv", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'disabled' FROM worf.sequence_ranges WHERE sequence = 'mv_id_seq'::regclass", token),
            "node_b heard mv_id_seq's epoch end");
        // node_b restarts, and draws from the blocks it kept once node_a has answered, as it does for a running epoch.
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        Stopwatch kept = Stopwatch.StartNew();
        while (!await nodeB.PsqlAsync(Database, "SET statement_timeout = '3s'; INSERT INTO mv (v) VALUES ('kept')", token))
        {
            Assert.IsLessThan(TimeSpan.FromSeconds(30), kept.Elapsed, "node_b drawing from mv_id_seq's kept blocks once restarted");
            await Task.Delay(TimeSpan.FromMilliseconds(500), token);
        }

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
    /// nothing else. A neighbor's made-up watermark resets the owner once, and its made-up requests to reset, in the
    /// ten minutes after, not again.
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

        // Nor through a join, which binds only a slot that is bound to no node and that nothing streams, for a role that
        // may stream slots, as c_user may not yet.
        string forgedJoin = $$"""{"node_id": {{nodeAId}}, "node_name": "node_a", "slot": "{{slot}}", "join": true}""";
        PostgresException joined = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
            $"SET SESSION AUTHORIZATION c_user; SELECT worf.sequence_range_exchange('{forgedJoin}')", token));
        Assert.AreEqual("42501", joined.SqlState);
        Assert.Contains("is bound to another role", joined.MessageText);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_create_logical_replication_slot('c_join', 'worf')", token);
        string joinAsNodeA = $$"""{"node_id": {{nodeAId}}, "node_name": "node_a", "slot": "c_join", "join": true}""";
        Assert.AreEqual("""{"streamed":false}""",
            await nodeB.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION c_user; SELECT worf.sequence_range_exchange('{joinAsNodeA}')", token),
            "c_user's join, without REPLICATION");
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM worf.sequence_range_link WHERE slot_name = 'c_join'", token),
            "c_join's link after c_user's join");

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

        // Nor, with REPLICATION, can it join as node_a through a slot bound to no node.
        PostgresException joinedAsOther = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
            $"SET SESSION AUTHORIZATION c_user; SELECT worf.sequence_range_exchange('{joinAsNodeA}')", token));
        Assert.AreEqual("42501", joinedAsOther.SqlState);
        Assert.Contains("already speaks for node", joinedAsOther.MessageText);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_drop_replication_slot('c_join')", token);

        // A node re-created under another name, with the same subscription name and role, takes over its old node's slot
        // once the role's row is deleted, rather than being locked out by the slot's link, and forgets the old node in
        // node_b's exchange turn, as every write of several nodes' reports does: here it waits there for a report node_c
        // relayed, which a session holds.
        _ = await nodeB.SafePsqlAsync(Database, "DELETE FROM worf.sequence_range_role WHERE role_id = 'c_user'::regrole", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_create_logical_replication_slot('c_link', 'worf')", token);
        string recreated = $$"""{"node_id": 2000001, "node_name": "node_c_again", "slot": "c_link", "sysid": "1", "timeline": 1, "wal_lsn": "0/0", "sequences": []}""";
        await using (NpgsqlConnection holder = await nodeB.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction holding = await holder.BeginTransactionAsync(token);
            await HoldRelayedReportAsync(nodeB, holder, holding, "t_id_seq", nodeCId, token);

            Task takeover = nodeB.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION c_user; SELECT worf.sequence_range_exchange('{recreated}')", token);
            Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, """
                SELECT EXISTS (
                    SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                    WHERE l.locktype = 'advisory' AND l.objsubid = 5 AND l.granted AND a.query LIKE '%sequence_range_exchange(%node_c_again%')
                """, token), "the takeover, forgetting node_c in node_b's turn");
            await holding.CommitAsync(token);
            await takeover;
        }

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
        const string MadeUpKept = "SELECT count(*) FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND node_name LIKE 'made_up_%'";
        await ExchangeUntilAsync(nodeB, flood, async () => await nodeB.SafePsqlAsync(Database, MadeUpKept, token) != "0", "node_b's take-in of the relays", token);
        Assert.AreEqual("t|t", await nodeB.SafePsqlAsync(Database, $"""
            SET SESSION AUTHORIZATION {SuperUser};
            SELECT worf.sequence_range_heard({nodeAId}, true, '-infinity', false) > now() - interval '1 minute',
                worf.sequence_range_heard({nodeCId}, false, '-infinity', false) > now() - interval '1 minute'
            """, token), "node_a and node_c heard from");
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database,
            $"SELECT count(*) BETWEEN 200 AND 256 FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND via_node = {nodeAId} AND node_id <> via_node",
            token), "the nodes node_b keeps from node_a's relays");
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database, """
            SELECT count(*) <= 256 FROM generate_series(1000001, 1001100) g WHERE worf.sequence_range_heard(g::oid, false, '-infinity', false) > '-infinity'
            """, token), "the made-up nodes node_b notes in memory");
        Assert.IsTrue(await nodeB.PollLogAsync(flooded, "or the 256 it keeps from one neighbor; they are ignored", token), "the ignored reports");

        // A call that relays them no more leaves none of them.
        await ExchangeUntilAsync(nodeB, flood.Replace(madeUp, ""), async () => await nodeB.SafePsqlAsync(Database, MadeUpKept, token) == "0",
            "the made-up nodes node_b forgets", token);
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
        Assert.IsTrue(await nodeA.PollLogAsync(unreachable, "exchange with node node_c failed: node node_c didn't answer within 5000 ms", token),
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
        await ExchangeUntilAsync(nodeA, madeUpWatermark,
            () => Task.FromResult(nodeA.ReadLog()[forgedAt..].Contains("node node_b has heard of grants up to", StringComparison.Ordinal)), "node_a's reset on the made-up watermark", token);
        Assert.IsTrue(await nodeA.PollLogAsync(forgedAt, "node node_b reports grants up to 9000000000000000000, beyond any a node could have heard of", token),
            "node_a's warning of the made-up watermark");
        Assert.IsTrue(await nodeA.PollLogAsync(forgedAt, "the allocator's fence lifted", token), "node_a's fence lifted");
        Assert.AreEqual("t", await nodeA.SafePsqlAsync(Database, """
            SELECT r.watermark < 9000000000000000000 AND EXISTS (
                SELECT 1 FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
                WHERE b.seqoid = r.seqoid AND b.kind = 'r' AND f.lo <= r.watermark + 1)
            FROM worf.sequence_range r WHERE r.seqoid = 't_id_seq'::regclass
            """, token), "node_a's root kept above its bound on the made-up watermark");

        // The blocks node_a grants itself beyond the watermark it took, which it hasn't drawn from, don't raise the bound
        // for the next report, so the same report coming again resets node_a no more, however often it comes.
        for (int i = 0; i < 5; i++)
        {
            _ = await nodeA.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{madeUpWatermark}')", token);
            await Task.Delay(TimeSpan.FromMilliseconds(500), token);
        }

        Assert.AreEqual(1, nodeA.ReadLog()[forgedAt..].Split("node node_b has heard of grants up to").Length - 1, "node_a's resets on node_b's reports");
        Assert.AreEqual(1, nodeA.ReadLog()[forgedAt..].Split("the allocator's fence lifted").Length - 1, "node_a's fences lifted");

        // Requests to reset that node_b makes up, each claiming to have seen node_a's WAL position further on than it is,
        // reset the sequence no more in the ten minutes in which node_b's made-up watermark reset it, however many come
        // in the minute that one rollback's resets take.
        for (int i = 1; i <= 5; i++)
        {
            string madeUpReset = madeUpWatermark
                .Replace("\"watermark\": 9000000000000000000", "\"watermark\": null", StringComparison.Ordinal)
                .Replace("\"reset\": null", $"\"reset\": \"FF000000/{i}\"", StringComparison.Ordinal);
            _ = await nodeA.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{madeUpReset}')", token);
        }

        Assert.DoesNotContain("node node_b saw this node's WAL position move backwards", nodeA.ReadLog()[forgedAt..], "node_a's resets on node_b's requests");
        Assert.AreEqual(1, nodeA.ReadLog()[forgedAt..].Split("the allocator's fence lifted").Length - 1, "node_a's fences lifted after the requests");

        await LiftsResetOnlyAsTheyNameHeldBlocksAsync(nodeA, nodeB, nodeAId, slot, token);

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

        // node_a dropping its subscription to node_c and deleting node_c's link, as drop_subscription asks when it can't
        // reach the provider, leaves the fence that is up waiting for node_c, whose grants the reset may have lost, until
        // node_c reports through its own subscription.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.drop_subscription('a_from_c')", token);
        _ = await nodeA.SafePsqlAsync(Database, $"DELETE FROM worf.sequence_range_link WHERE node_id = {nodeCId}", token);
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        Assert.AreEqual("t", await nodeA.SafePsqlAsync(Database,
            "SELECT fenced_at IS NOT NULL AND detail LIKE 'fenced until node node_c reports%' FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token),
            "node_a's fence still waiting for node_c");
        await nodeC.StartServerAsync(token);
        Assert.IsTrue(await nodeA.PollLogAsync(downC, "node node_c holds block", token), "node_c's grants back in node_a's ledger");
        Assert.IsTrue(await nodeA.PollLogAsync(downC, "the allocator's fence lifted", token), "node_a's fence lifted once node_c reported");

        // Once the subscriptions between node_a and node_c are dropped, node_c leaving its slot as it drops its own, node_a
        // forgets node_c, and a fence that goes up later doesn't wait for it while it is down.
        _ = await nodeC.SafePsqlAsync(Database, "SELECT worf.drop_subscription('c_from_a')", token);
        Assert.AreEqual("f", await nodeA.SafePsqlAsync(Database,
            $"SELECT EXISTS (SELECT 1 FROM worf.sequence_range_link WHERE slot_name = '{nodeCSlot}' OR node_id = {nodeCId})", token), "node_c's slot left");
        await nodeC.StopAsync("fast", token);
        int forgotten = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('t_id_seq')", token);
        Assert.IsTrue(await nodeA.PollLogAsync(forgotten, "the allocator's fence lifted", token), "node_a's fence lifted without node_c");
    }

    /// <summary>
    /// A forced lift over grants that overlap resets both holders, the other as it hears the owner's next message, and
    /// marks the grants, so that they fence the root no more. A lift over a new overlap within ten minutes, here among
    /// three holders, resets them all at once, as a node takes the resets of the node that granted its blocks whenever it lifts, though it takes
    /// another neighbor's no more than once in ten minutes. A lift that stops waiting for a node that's down asks it to
    /// reset once it's heard from again, whatever it reports, at once too, and doesn't take the blocks it reports back into
    /// its ledger; the node draws nothing meanwhile, nor while a prepared transaction holds either node's range row.
    /// Requests to reset that a neighbor's role makes up reset the owner once in a window for the sequence.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ForcedLiftsResetTheHoldersTheyPassOver()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "max_prepared_transactions = 2");
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "max_prepared_transactions = 2");
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id bigserial PRIMARY KEY, v text);
                SELECT * FROM worf.replication_set_add_table('default', 't');
                """, token);
        }

        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        string nodeBId = await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        const string Fence = "SELECT coalesce(detail, '') FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass AND fenced_at IS NOT NULL";

        for (int lift = 1; lift <= 2; lift++)
        {
            // node_a's ledger comes to hold a grant to itself over node_b's active block, and node_a resets, so its fence
            // finds the grants overlapping once node_b reports.
            string activeB = await nodeB.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token);
            int logA = nodeA.ReadLog().Length;
            int logB = nodeB.ReadLog().Length;
            _ = await nodeA.SafePsqlAsync(Database, $"""
                INSERT INTO worf.sequence_range_grant (grant_id, request_id, seqoid, grantee_id, grantee_name, kind, lo, hi, state, granted_at)
                SELECT gen_random_uuid(), gen_random_uuid(), g.seqoid, n.node_id, n.node_name, g.kind, g.lo, g.hi, 'acknowledged', now()
                FROM worf.sequence_range_grant g CROSS JOIN worf.local_node l JOIN worf.node n ON n.node_id = l.node_id
                WHERE g.seqoid = 't_id_seq'::regclass AND g.grantee_id = {nodeBId} AND g.lo = {activeB};
                {(lift == 2 ? $"""
                    INSERT INTO worf.sequence_range_grant (grant_id, request_id, seqoid, grantee_id, grantee_name, kind, lo, hi, state, granted_at)
                    SELECT gen_random_uuid(), gen_random_uuid(), g.seqoid, 4242, 'node_x', g.kind, g.lo, g.hi, 'acknowledged', now()
                    FROM worf.sequence_range_grant g WHERE g.seqoid = 't_id_seq'::regclass AND g.grantee_id = {nodeBId} AND g.lo = {activeB};
                    """ : "")}
                SELECT worf.reset_sequence_range('t_id_seq');
                """, token);
            Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT ({Fence}) LIKE '%overlap%'", token), $"node_a's fence held by the overlap {lift}");

            _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.activate_sequence_range('t_id_seq')", token);
            Assert.Contains(lift == 1 ? "overlap, so both nodes reset their blocks" : "among others, so the 3 nodes that hold grants that overlap reset their blocks",
                nodeA.ReadLog()[logA..], $"node_a's lift over the overlap {lift}");
            Assert.IsTrue(await nodeB.PollLogAsync(logB, "node node_a lifted a fence over grants of this node's that overlap another's", token),
                $"node_b's reset on the lift {lift}");

            // node_b logs its reset before the reset commits, so its range row may still show the block it gave up.
            Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
                $"SELECT state = 'active' AND active_lo IS NOT NULL AND active_lo <> {activeB} FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token),
                $"node_b drawing from a block granted after its reset on the lift {lift}");
            await WaitActiveAsync([nodeA, nodeB], token);
            Assert.AreEqual("0", await nodeA.SafePsqlAsync(Database, """
                SELECT count(*) FROM worf.sequence_range_grant a JOIN worf.sequence_range_grant b
                    ON b.seqoid = a.seqoid AND a.grant_id < b.grant_id AND a.lo <= b.hi AND b.lo <= a.hi
                WHERE a.seqoid = 't_id_seq'::regclass AND a.state NOT IN ('unclaimed', 'overlapped') AND b.state NOT IN ('unclaimed', 'overlapped')
                    AND a.kind <> 'x' AND b.kind <> 'x'
                """, token), $"the overlapping grants marked after the lift {lift}");

            if (lift == 1)
            {
                // The marked grants fence the root no more: node_a's next reset lifts once node_b reports.
                _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('t_id_seq')", token);
                await WaitActiveAsync([nodeA, nodeB], token);
            }
        }

        // node_a's fence stops waiting for node_b while it's down, and node_b resets once node_a hears from it. An insert
        // on node_b as soon as it's back waits for node_a's first message, which asks it to reset, so it draws from none
        // of the blocks it held, whose values the lift may have given out again. A prepared transaction holds node_b's
        // range row as it stops, which leaves the sequence out of the exchanges, so that the insert waits until it ends.
        string nodeAId = await nodeA.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        string bSlot = await nodeB.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription WHERE sub_name = 'b_from_a'", token);
        string[] heldBefore = (await nodeB.SafePsqlAsync(Database,
            "SELECT active_lo, active_hi FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token)).Split('|');
        _ = await nodeB.SafePsqlAsync(Database,
            "BEGIN; SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass FOR NO KEY UPDATE; PREPARE TRANSACTION 'held_b'", token);
        await nodeB.StopAsync("fast", token);
        (int controlExit, string control) = await nodeB.RunProgramAsync("pg_controldata", ["-D", nodeB.Cluster.DataDirectory], token);
        Assert.AreEqual(0, controlExit, control);
        string stoppedAt = control.Split('\n').Single(line => line.StartsWith("Latest checkpoint location:", StringComparison.Ordinal))
            .Split(':', 2)[1].Trim();
        int logged = nodeB.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('t_id_seq')", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT ({Fence}) LIKE 'fenced until node node_b%'", token), "node_a's fence waiting for node_b");
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.activate_sequence_range('t_id_seq', 'node_b')", token);
        await WaitActiveAsync([nodeA], token);

        // A message of node_b's that reports just what node_a recorded of it, at the WAL position it stopped at, is asked
        // to reset all the same. While a prepared transaction holds node_a's range row, which leaves the sequence out,
        // node_a asks nothing, and so its answers don't say back the start that node_b's message gave.
        string[] recorded = (await nodeA.SafePsqlAsync(Database, $"""
            SELECT n.sysid, n.timeline, n.epoch, n.state, coalesce(n.distance::text, 'null'), coalesce(n.high_water::text, 'null'),
                coalesce(n.watermark::text, 'null'), coalesce(pg_catalog.array_to_json(n.holds)::text, 'null'), coalesce(n.clock::text, 'null'),
                r.range_size, r.max_range_size, coalesce(r.root_start::text, 'null'), coalesce(r.root_end::text, 'null')
            FROM worf.sequence_range_node n JOIN worf.sequence_range r ON r.seqoid = n.seqoid
            WHERE n.seqoid = 't_id_seq'::regclass AND n.node_id = {nodeBId}
            """, token)).Split('|');
        string Report(string started) => $$"""
            {"node_id": {{nodeBId}}, "node_name": "node_b", "slot": "{{bSlot}}", "sysid": "{{recorded[0]}}", "timeline": {{recorded[1]}}, "wal_lsn": "{{stoppedAt}}",{{started}} "sequences": [
                {"name": "public.t_id_seq", "owner_id": {{nodeAId}}, "epoch": {{recorded[2]}}, "range_size": {{recorded[9]}}, "max_range_size": {{recorded[10]}},
                    "distance": {{recorded[4]}}, "high_water": {{recorded[5]}}, "watermark": {{recorded[6]}}, "state": "{{recorded[3]}}", "root_start": {{recorded[11]}},
                    "root_end": {{recorded[12]}}, "holds": {{recorded[7]}}, "clock": {{recorded[8]}}, "reset": null, "reset_done": null, "held_complete": false,
                    "held": [], "requests": [], "reports": [], "grants": [], "acks": []}]}
            """;
        const string MadeUpStart = "4242";
        _ = await nodeA.SafePsqlAsync(Database,
            "BEGIN; SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass FOR NO KEY UPDATE; PREPARE TRANSACTION 'held_a'", token);
        _ = await nodeA.SafePsqlAsync(Database,
            $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{Report($" \"started_at\": {MadeUpStart},")}')", token);
        using (JsonDocument answer = JsonDocument.Parse(await nodeA.SafePsqlAsync(Database,
            $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{Report("")}')", token)))
        {
            JsonElement entry = answer.RootElement.GetProperty("sequences").EnumerateArray().Single(e => e.GetProperty("name").GetString() == "public.t_id_seq");
            Assert.IsFalse(entry.TryGetProperty("checked_start", out JsonElement checkedStart) && checkedStart.ValueKind == JsonValueKind.Number &&
                checkedStart.GetInt64() == long.Parse(MadeUpStart, CultureInfo.InvariantCulture), "node_a's word on t_id_seq, while it still owes node_b a request to reset");
        }

        _ = await nodeA.SafePsqlAsync(Database, "ROLLBACK PREPARED 'held_a'", token);
        await ExchangeUntilAsync(nodeA, Report(""), async () => await nodeA.SafePsqlAsync(Database,
            $"SELECT reset_lsn::text LIKE 'FFFFFFFE/%' FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND node_id = {nodeBId}", token) == "t",
            "node_a's request that node_b reset, at node_b's message that reports what it reported before", token);

        await nodeB.StartServerAsync(token);
        int waited = nodeB.ReadLog().Length;
        Assert.IsFalse(await nodeB.PsqlAsync(Database, "SET statement_timeout = '5s'; INSERT INTO t (v) VALUES ('back')", token),
            "node_b's insert while a prepared transaction holds its range row");
        Assert.Contains("canceling statement due to statement timeout", nodeB.ReadLog()[waited..], "node_b's insert waiting for node_a's request to reset");
        _ = await nodeB.SafePsqlAsync(Database, "ROLLBACK PREPARED 'held_b'", token);
        long drawn = long.Parse(await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO t (v) VALUES ('back') RETURNING id", token),
            CultureInfo.InvariantCulture);
        Assert.IsFalse(drawn >= long.Parse(heldBefore[0], CultureInfo.InvariantCulture) && drawn <= long.Parse(heldBefore[1], CultureInfo.InvariantCulture),
            $"node_b's first value once back, {drawn}, drawn from the block it held before, [{heldBefore[0]}, {heldBefore[1]}]");
        Assert.IsTrue(await nodeB.PollLogAsync(logged, "node node_a lifted its fence without this node's report", token), "node_b's reset once back");
        await WaitActiveAsync([nodeB], token);
        await AssertNoSharedValuesAsync([nodeA, nodeB], token);

        // Requests to reset that node_b's role makes up, as a neighbor's role can, reset node_a, the owner, once in a window
        // for the sequence, which grows from ten minutes as such resets keep coming: the first resets it, and one right
        // after doesn't.
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('a')", token);
        string[] range = (await nodeA.SafePsqlAsync(Database, """
            SELECT epoch, range_size, max_range_size, coalesce(watermark::text, 'null'), coalesce(root_start::text, 'null'), coalesce(root_end::text, 'null')
            FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass
            """, token)).Split('|');

        // Each request carries node_b's WAL position as it is then, so that node_a doesn't take an earlier one for node_b
        // going back, though a call of node_b's own may come between with a later one, at which node_a asks node_b to
        // reset, rather than resetting itself.
        async Task<string> RequestAsync(string reset)
        {
            string[] identity = (await nodeB.SafePsqlAsync(Database,
                "SELECT (SELECT system_identifier FROM pg_control_system()), (SELECT timeline_id FROM pg_control_checkpoint()), pg_current_wal_insert_lsn()", token)).Split('|');
            return $$"""
            {"node_id": {{nodeBId}}, "node_name": "node_b", "slot": "{{bSlot}}", "sysid": "{{identity[0]}}", "timeline": {{identity[1]}}, "wal_lsn": "{{identity[2]}}", "sequences": [
                {"name": "public.t_id_seq", "owner_id": {{nodeAId}}, "epoch": {{range[0]}}, "range_size": {{range[1]}}, "max_range_size": {{range[2]}},
                    "distance": 1, "high_water": null, "watermark": {{range[3]}}, "state": "active", "root_start": {{range[4]}}, "root_end": {{range[5]}},
                    "reset": "{{reset}}", "reset_done": null, "held_complete": false, "held": [], "requests": [], "reports": [], "grants": [], "acks": []}]}
            """;
        }

        // A take-in that finds a row it writes held by another transaction, as the range worker's or a call's, leaves the
        // sequence to the neighbor's next message, rolling back what it did, as node_b's own messages repeat a request
        // until it's answered; so each made-up request is sent until node_a records it answered.
        async Task AskAsync(string reset)
        {
            Stopwatch asking = Stopwatch.StartNew();
            do
            {
                Assert.IsLessThan(TestNode.PollTimeout, asking.Elapsed, $"node_a's answer to node_b's made-up request {reset}");
                _ = await nodeA.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{await RequestAsync(reset)}')", token);
            }
            while (await nodeA.SafePsqlAsync(Database,
                $"SELECT reset_done = '{reset}' FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND node_id = {nodeBId}", token) != "t");
        }

        // The first is sent while a transaction holds node_a's blocks, which the reset gives up: the take-ins leave it until
        // the transaction ends, and the reset is logged once, as the take-in that makes it commits.
        int asked = nodeA.ReadLog().Length;
        await using (NpgsqlConnection holder = await nodeA.OpenAsync(Database, token))
        {
            NpgsqlTransaction holding = await holder.BeginTransactionAsync(token);
            await ExecuteAsync(holder, "SELECT 1 FROM worf.sequence_range_block WHERE seqoid = 't_id_seq'::regclass AND state IN ('active', 'next') FOR UPDATE", token);
            Task releasing = Task.Run(async () =>
            {
                await Task.Delay(TimeSpan.FromSeconds(1.5), token);
                await holding.RollbackAsync(token);
            }, token);
            await AskAsync("0/10");
            await releasing;
        }

        Assert.AreEqual(1, nodeA.ReadLog()[asked..].Split("node node_b asked this node to reset").Length - 1, "node_a's resets at node_b's made-up request");

        // node_a draws from a block again, so that a reset would have something to give up.
        await WaitActiveAsync([nodeA, nodeB], token);
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('a')", token);
        asked = nodeA.ReadLog().Length;
        await AskAsync("0/11");
        Assert.DoesNotContain("node node_b asked this node to reset", nodeA.ReadLog()[asked..], "node_a's reset at node_b's next made-up request");
        await WaitActiveAsync([nodeA, nodeB], token);
        await AssertNoSharedValuesAsync([nodeA, nodeB], token);
    }

    /// <summary>
    /// A session that ends while it waits for a switch, as one terminated, leaves no count of it behind: the range worker
    /// switches the block it waited on, and then leaves the next one alone until it runs low.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SessionsThatEndWhileWaitingLeaveNoTrace()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node_a", token);
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE w (id bigserial PRIMARY KEY, v int);
            SELECT worf.enable_sequence_range('w_id_seq', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        const string Active = "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 'w_id_seq'::regclass";
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 'w_id_seq'::regclass", token), "w's blocks");
        // The range row, held, keeps every switch off, so a session that reaches the block's last twentieth waits.
        string active = await node.SafePsqlAsync(Database, Active, token);
        await using (NpgsqlConnection holder = await node.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction held = await holder.BeginTransactionAsync(token);
            await using (NpgsqlCommand hold = new("SELECT 1 FROM worf.sequence_range WHERE seqoid = 'w_id_seq'::regclass FOR NO KEY UPDATE", holder, held))
            {
                _ = await hold.ExecuteNonQueryAsync(token);
            }

            _ = await node.SafePsqlAsync(Database, "INSERT INTO w (v) SELECT 1 FROM generate_series(1, 94)", token);

            await using NpgsqlConnection waiter = await node.OpenAsync(Database, token);
            await using NpgsqlCommand insert = new("INSERT INTO w (v) SELECT 1 FROM generate_series(1, 5)", waiter);
            Task<int> waiting = insert.ExecuteNonQueryAsync(token);
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"""
                SELECT state = 'active' AND now() - query_start > interval '1 second' FROM pg_stat_activity WHERE pid = {waiter.ProcessID}
                """, token), "the insert waiting for the switch");
            _ = await node.SafePsqlAsync(Database, $"SELECT pg_terminate_backend({waiter.ProcessID})", token);
            _ = await Assert.ThrowsAsync<Exception>(() => waiting, "the terminated insert");
            await held.RollbackAsync(token);
        }

        // The block it waited on is switched, as it ran low, and the next stays, with nothing waiting for it.
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"SELECT ({Active}) <> {active}", token), "the switch from the low block");
        string next = await node.SafePsqlAsync(Database, Active, token);
        _ = await node.SafePsqlAsync(Database, "INSERT INTO w (v) SELECT 1 FROM generate_series(1, 5)", token);
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.AreEqual(next, await node.SafePsqlAsync(Database, Active, token), "the next block, which has most of its values left");
    }

    /// <summary>
    /// A transaction that runs its block out while the range worker can't switch it, here as another session holds the
    /// sequence's range row, switches to the next block itself, once it has waited twice worf.sequence_range_timeout,
    /// without taking the range row, which would hold up every exchange until it ended, and with the lock_timeout the
    /// session set left as it was. Its switch is logged once it commits, and one that switched can't be prepared, as a
    /// prepared transaction would hold the sequence across restarts; the refused one logs no switch, and
    /// worf.sequence_ranges shows its sequence meanwhile. A switch that rolls
    /// back, or that a savepoint rolls back, leaves no insert to fail, and the savepoint's transaction may switch again or
    /// be prepared, without the switch logged. Nor can a transaction that ran a function that takes the sequence's range
    /// row be prepared.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task TransactionsThatSwitchThemselvesHoldNoRangeRow()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node_a", token, "max_prepared_transactions = 2");
        _ = await node.SafePsqlAsync(Database, """
            ALTER SYSTEM SET worf.sequence_range_timeout = 1000;
            SELECT pg_reload_conf();
            CREATE TABLE ts (id bigserial PRIMARY KEY, v int);
            SELECT worf.enable_sequence_range('ts_id_seq', participants := '{}', range_size := 10, max_range_size := 10);
            """, token);
        const string Range = "SELECT 1 FROM worf.sequence_range WHERE seqoid = 'ts_id_seq'::regclass FOR NO KEY UPDATE";
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 'ts_id_seq'::regclass", token), "ts's blocks");
        string active = await node.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges WHERE sequence = 'ts_id_seq'::regclass", token);

        await using NpgsqlConnection holder = await node.OpenAsync(Database, token);
        await using NpgsqlTransaction held = await holder.BeginTransactionAsync(token);
        await using (NpgsqlCommand hold = new(Range, holder, held))
        {
            _ = await hold.ExecuteNonQueryAsync(token);
        }

        await using NpgsqlConnection switcher = await node.OpenAsync(Database, token);
        await using (NpgsqlCommand timeout = new("SET lock_timeout = '1s'", switcher))
        {
            _ = await timeout.ExecuteNonQueryAsync(token);
        }

        await using NpgsqlTransaction switching = await switcher.BeginTransactionAsync(token);
        await using (NpgsqlCommand insert = new("INSERT INTO ts (v) SELECT 1 FROM generate_series(1, 15)", switcher, switching))
        {
            Assert.AreEqual(15, await insert.ExecuteNonQueryAsync(token), "the rows inserted past the block's end");
        }

        await using (NpgsqlCommand show = new("SHOW lock_timeout", switcher, switching))
        {
            Assert.AreEqual("1s", await show.ExecuteScalarAsync(token), "the session's lock_timeout after its own switch");
        }

        // Once the other session lets the range row go, the switching transaction, still open, doesn't hold it.
        await held.RollbackAsync(token);
        Assert.AreEqual("1", await node.SafePsqlAsync(Database, $"{Range} NOWAIT", token), "the range row, free while the switch is open");

        // worf.sequence_ranges reads the sequence's position without waiting for the switch, which holds the sequence.
        Assert.AreEqual("t", await node.SafePsqlAsync(Database,
            "SET statement_timeout = '500ms'; SELECT values_left IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 'ts_id_seq'::regclass", token),
            "the sequence's position while the switch is open");
        await switching.CommitAsync(token);
        Assert.AreEqual("15|15|t", await node.SafePsqlAsync(Database,
            $"SELECT count(*), count(DISTINCT id), min(id) >= {active} FROM ts", token), "the rows, each with its own key");
        int switcherPid = switcher.ProcessID;
        string Switched(string log) => string.Join('\n', log.Split('\n').Where(line => line.Contains($"[{switcherPid}]", StringComparison.Ordinal) &&
            line.Contains("sequence public.ts_id_seq switched to its block", StringComparison.Ordinal)));
        Assert.AreNotEqual("", Switched(node.ReadLog()), "the transaction's own switch, logged as it committed");

        // Once the next block is ready, a transaction that switches itself again can't be prepared, and logs nothing.
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 'ts_id_seq'::regclass", token), "ts's next block");
        await using NpgsqlTransaction heldAgain = await holder.BeginTransactionAsync(token);
        await using (NpgsqlCommand hold = new(Range, holder, heldAgain))
        {
            _ = await hold.ExecuteNonQueryAsync(token);
        }

        int logpos = node.ReadLog().Length;
        await using (NpgsqlTransaction preparing = await switcher.BeginTransactionAsync(token))
        {
            await using (NpgsqlCommand insert = new("INSERT INTO ts (v) SELECT 2 FROM generate_series(1, 15)", switcher, preparing))
            {
                _ = await insert.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand prepare = new("PREPARE TRANSACTION 'switched'", switcher, preparing);
            PostgresException refused = await Assert.ThrowsExactlyAsync<PostgresException>(() => prepare.ExecuteNonQueryAsync(token));
            Assert.AreEqual("0A000", refused.SqlState, "PREPARE after the transaction's own switch");
        }

        await heldAgain.RollbackAsync(token);
        Assert.AreEqual("0|", await node.SafePsqlAsync(Database, "SELECT count(*) FILTER (WHERE v = 2), (SELECT string_agg(gid, ',') FROM pg_prepared_xacts) FROM ts", token),
            "the refused transaction's rows and prepared transactions");
        Assert.AreEqual("", Switched(node.ReadLog()[logpos..]), "the refused transaction's switch, never logged");

        // A switch that rolls back leaves the sequence in the next block, as its values may have been seen, while its
        // bounds stay the active block's. A later insert then waits for the switch as at the block's end, or switches
        // in its transaction, rather than fail. A savepoint that rolls a switch back lets its transaction switch again
        // and be prepared, and its switch is never logged.
        await using NpgsqlConnection other = await node.OpenAsync(Database, token);
        foreach (string rollback in new[] { "ROLLBACK", "ROLLBACK TO SAVEPOINT s", "PREPARE" })
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database,
                "SELECT values_left > 0 AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 'ts_id_seq'::regclass", token),
                $"ts's active and next blocks before {rollback}");
            await using NpgsqlTransaction heldOnce = await holder.BeginTransactionAsync(token);
            await using (NpgsqlCommand hold = new(Range, holder, heldOnce))
            {
                _ = await hold.ExecuteNonQueryAsync(token);
            }

            // The run draws the active block's last value and one of the next block's.
            long left = long.Parse(await node.SafePsqlAsync(Database,
                "SELECT values_left + 1 FROM worf.sequence_ranges WHERE sequence = 'ts_id_seq'::regclass", token), CultureInfo.InvariantCulture);
            logpos = node.ReadLog().Length;
            string runOut = $"INSERT INTO ts (v) SELECT 3 FROM generate_series(1, {left})";
            string sql = rollback switch
            {
                "ROLLBACK" => $"BEGIN; {runOut}; ROLLBACK",
                "ROLLBACK TO SAVEPOINT s" => $"BEGIN; SAVEPOINT s; {runOut}; ROLLBACK TO SAVEPOINT s; INSERT INTO ts (v) SELECT 4 FROM generate_series(1, 3); COMMIT",
                _ => $"BEGIN; SAVEPOINT s; {runOut}; ROLLBACK TO SAVEPOINT s; PREPARE TRANSACTION 'unswitched'",
            };
            await using (NpgsqlCommand command = new(sql, switcher))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            if (rollback == "PREPARE")
            {
                Assert.AreEqual("", Switched(node.ReadLog()[logpos..]), "the switch the savepoint rolled back, never logged");
                _ = await node.SafePsqlAsync(Database, "COMMIT PREPARED 'unswitched'", token);
            }

            await using (NpgsqlCommand insert = new("INSERT INTO ts (v) SELECT 5 FROM generate_series(1, 3)", other))
            {
                Assert.AreEqual(3, await insert.ExecuteNonQueryAsync(token), $"another session's rows after {rollback}");
            }

            await heldOnce.RollbackAsync(token);
        }

        Assert.AreEqual("3|9|t", await node.SafePsqlAsync(Database,
            "SELECT count(*) FILTER (WHERE v = 4), count(*) FILTER (WHERE v = 5), count(*) = count(DISTINCT id) FROM ts", token),
            "the rows after the switches that rolled back, each with its own key");

        // A transaction that ran a function that takes the sequence's range row can't be prepared either, as the row
        // would stay held across restarts, unless a savepoint rolled the function back.
        PostgresException altered = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
            "BEGIN; SELECT worf.alter_sequence_range('ts_id_seq', range_size := 10); PREPARE TRANSACTION 'altered'", token));
        Assert.AreEqual("0A000|cannot PREPARE a transaction that ran worf.alter_sequence_range", $"{altered.SqlState}|{altered.MessageText}");
        _ = await node.SafePsqlAsync(Database, """
            BEGIN; SAVEPOINT a; SELECT worf.alter_sequence_range('ts_id_seq', range_size := 10); ROLLBACK TO SAVEPOINT a;
            PREPARE TRANSACTION 'unaltered';
            COMMIT PREPARED 'unaltered';
            """, token);

        // Nor can one that changed the type of an identity column the sequence numbers, which takes its range row too.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE ti (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY);
            SELECT worf.enable_sequence_range('ti_id_seq', participants := '{}', range_size := 10, max_range_size := 10);
            """, token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'ti_id_seq'::regclass", token), "ti's blocks");
        PostgresException retyped = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
            "BEGIN; ALTER TABLE ti ALTER COLUMN id TYPE integer; PREPARE TRANSACTION 'retyped'", token));
        Assert.AreEqual("0A000|cannot PREPARE a transaction that changed the type of a column that sequence public.ti_id_seq numbers",
            $"{retyped.SqlState}|{retyped.MessageText}");
        Assert.AreEqual("", await node.SafePsqlAsync(Database, "SELECT string_agg(gid, ',') FROM pg_prepared_xacts", token), "prepared transactions");
    }

    /// <summary>
    /// A prepared transaction that holds a sequence's range row, as a superuser's that locked it, holds up that sequence's
    /// exchanges alone: a neighbor's blocks of another sequence still come, the sequence's row in worf.sequence_ranges
    /// names the transaction, and the log says once what to do.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task PreparedHoldersHoldUpOnlyTheirSequence()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "max_prepared_transactions = 2");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE ti (id bigserial PRIMARY KEY); CREATE TABLE tj (id bigserial PRIMARY KEY)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('ti_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100);
            SELECT worf.enable_sequence_range('tj_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100);
            """, token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database,
                "SELECT count(*) = 2 FROM worf.sequence_ranges WHERE sequence IN ('ti_id_seq'::regclass, 'tj_id_seq'::regclass) AND state = 'active'", token),
                $"{node.Name} drawing from blocks of both");
        }

        int logpos = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database,
            "BEGIN; SELECT 1 FROM worf.sequence_range WHERE seqoid = 'ti_id_seq'::regclass FOR NO KEY UPDATE; PREPARE TRANSACTION 'held'", token);
        try
        {
            // node_b's inserts into tj run through ten of its blocks from node_a.
            await using (NpgsqlConnection writer = await nodeB.OpenAsync(Database, token))
            {
                await ExecuteAsync(writer, "SET statement_timeout = '60s'", token);
                for (int i = 0; i < 10; i++)
                {
                    await ExecuteAsync(writer, "INSERT INTO tj SELECT FROM generate_series(1, 100)", token);
                }
            }

            Assert.AreEqual("prepared transaction 'held' holds the range row, so the sequence exchanges no blocks until it ends", await nodeA.SafePsqlAsync(Database,
                "SELECT detail FROM worf.sequence_ranges WHERE sequence = 'ti_id_seq'::regclass", token), "ti's row in the view");
            Assert.IsTrue(await nodeA.PollLogAsync(logpos, "sequence public.ti_id_seq: prepared transaction \"held\" holds its range row", token),
                "node_a's warning");
        }
        finally
        {
            _ = await nodeA.SafePsqlAsync(Database, "ROLLBACK PREPARED 'held'", token);
        }

        Assert.AreEqual("1000", await nodeB.SafePsqlAsync(Database, "SELECT count(DISTINCT id) FROM tj", token), "tj's rows");
    }

    /// <summary>
    /// A provider that takes calls but never answers holds up none of a node's exchanges with its other neighbors, as the
    /// node's exchange worker connects and calls without waiting on it, and probes it once its call has run out of time,
    /// so the node's inserts go on through small blocks from the owner, none waiting on it. Once it answers again, though
    /// slowly, a second away each way, a request that waits on it reaches it, over the probe's connection, though a
    /// request on another node stays open and the node keeps drawing blocks from the owner, none of whose calls its slow
    /// ones hold up.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task FrozenNeighborsHoldUpNoOtherExchange()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        // u's blocks come to node_b from node_c alone, and w's from node_a alone.
        foreach (TestNode node in new[] { nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE u (id bigserial PRIMARY KEY)", token);
        }

        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE SEQUENCE w_seq MAXVALUE 200;
                CREATE TABLE w (id bigint PRIMARY KEY DEFAULT nextval('w_seq'));
                ALTER SEQUENCE w_seq OWNED BY w.id;
                """, token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('w_seq', participants := '{node_b}', range_size := 50, max_range_size := 50)", token);
        _ = await nodeC.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('u_id_seq', participants := '{node_b}', range_size := 10, max_range_size := 10)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        const string UNext = "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 'u_id_seq'::regclass";
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, UNext, token), "node_b's blocks of u from node_c");
        string nodeCDsn = await nodeB.SafePsqlAsync(Database,
            "SELECT if_dsn FROM worf.node_interface WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')", token);

        // node_b's calls to node_c go to a listener that takes connections and never answers.
        using TcpListener frozen = new(IPAddress.Loopback, 0);
        frozen.Start();
        List<TcpClient> taken = [];
        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task taking = Task.Run(async () =>
        {
            while (!stop.Token.IsCancellationRequested)
            {
                taken.Add(await frozen.AcceptTcpClientAsync(stop.Token));
            }
        }, stop.Token);
        int logpos = nodeB.ReadLog().Length;
        _ = await nodeB.SafePsqlAsync(Database, $"""
            UPDATE worf.node_interface SET if_dsn = 'host=127.0.0.1 port={((IPEndPoint)frozen.LocalEndpoint).Port} dbname={Database} user={SuperUser}'
            WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
            """, token);
        Assert.IsTrue(await nodeB.PollLogAsync(logpos, "node node_c didn't answer within 5000 ms", token), "node_b's first call to node_c timed out");

        // node_b runs into its next block of u, so that it waits on node_c for another.
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO u SELECT FROM generate_series(1, 15)", token);

        // node_b draws two blocks a second from node_a for fifteen seconds, and no insert waits for one. The server times
        // each insert, as the test host's own clock counts the time its other tests keep it from reading the result.
        const string Insert = "INSERT INTO t (v) VALUES ('b')";
        int timed = nodeB.ReadLog().Length;
        await using (NpgsqlConnection writer = await nodeB.OpenAsync(Database, token))
        {
            // The session runs an insert before the timed ones, so that its backend's first use of worf, which a loaded host
            // may hold up for a second, isn't counted as a wait.
            await using (NpgsqlCommand logged = new($"{Insert}; SET log_min_duration_statement = 0", writer))
            {
                _ = await logged.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand insert = new(Insert, writer);
            Stopwatch running = Stopwatch.StartNew();
            while (running.Elapsed < TimeSpan.FromSeconds(15))
            {
                _ = await insert.ExecuteNonQueryAsync(token);
                await Task.Delay(TimeSpan.FromMilliseconds(5), token);
            }
        }

        await stop.CancelAsync();
        _ = await Task.WhenAny(taking);
        foreach (TcpClient client in taken)
        {
            client.Dispose();
        }

        // Without the probes, each of node_b's calls to node_c held up its calls to node_a for 5 s, so its inserts waited
        // seconds for blocks; with them, none waits for a call to node_c, only for node_a's answers.
        List<double> waits = [.. nodeB.ReadLog()[timed..].Split('\n')
            .Where(line => line.Contains($": {Insert}", StringComparison.Ordinal) && line.Contains("  execute ", StringComparison.Ordinal))
            .Select(line => double.Parse(line[(line.IndexOf("duration: ", StringComparison.Ordinal) + 10)..line.IndexOf(" ms", StringComparison.Ordinal)],
                CultureInfo.InvariantCulture))];
        waits.Sort();
        Assert.IsGreaterThan(100, waits.Count, "node_b's inserts");
        Assert.IsLessThan(2500.0, waits[^1], "node_b's slowest insert, against the 5000 ms each call to node_c took");
        Assert.IsLessThan(100.0, waits[(int)(waits.Count * 0.99)], "node_b's 99th percentile insert");

        // node_b runs w's root on node_a out, so that its request to node_a for another block stays open.
        await using (NpgsqlConnection writer = await nodeB.OpenAsync(Database, token))
        {
            for (int i = 0; i < 250; i++)
            {
                try
                {
                    await ExecuteAsync(writer, "SET statement_timeout = '2s'; INSERT INTO w DEFAULT VALUES", token);
                }
                catch (PostgresException)
                {
                    break;
                }
            }
        }

        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (SELECT 1 FROM worf.sequence_range_request q JOIN worf.node n ON n.node_id = q.parent_node
                WHERE q.state = 'open' AND n.node_name = 'node_a')
            """, token), "node_b's request to node_a, open as w's root has run out");

        // Once node_c answers again, over a link 1 s away each way, node_b's request for a block of u reaches it, though a
        // request on node_a stays open and node_b keeps drawing blocks of t from node_a: neither the connection, which
        // takes a few round trips, nor the calls to node_c, which take two seconds each, hold up those calls, and no
        // insert waits a second.
        using TcpListener slow = new(IPAddress.Loopback, 0);
        slow.Start();
        string[] dsnParts = nodeCDsn.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        int nodeCPort = int.Parse(dsnParts.Single(part => part.StartsWith("port=", StringComparison.Ordinal))[5..], CultureInfo.InvariantCulture);
        using CancellationTokenSource relaying = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task relay = Task.Run(async () =>
        {
            async Task PumpAsync(NetworkStream from, NetworkStream to)
            {
                byte[] buffer = new byte[65536];
                int read;
                while ((read = await from.ReadAsync(buffer, relaying.Token)) > 0)
                {
                    await Task.Delay(1000, relaying.Token);
                    await to.WriteAsync(buffer.AsMemory(0, read), relaying.Token);
                }
            }

            List<Task> links = [];
            try
            {
                while (true)
                {
                    TcpClient client = await slow.AcceptTcpClientAsync(relaying.Token);
                    links.Add(Task.Run(async () =>
                    {
                        using TcpClient near = client;
                        using TcpClient far = new();
                        try
                        {
                            await far.ConnectAsync(IPAddress.Loopback, nodeCPort, relaying.Token);
                            _ = await Task.WhenAny(PumpAsync(near.GetStream(), far.GetStream()), PumpAsync(far.GetStream(), near.GetStream()));
                        }
                        catch (Exception error) when (error is IOException or SocketException or OperationCanceledException)
                        {
                            // A link ends as either side closes, or as the relay stops.
                        }
                    }, relaying.Token));
                }
            }
            catch (OperationCanceledException)
            {
                // The relay stops.
            }

            await Task.WhenAll(links.Select(link => link.ContinueWith(_ => { }, TaskScheduler.Default)));
        }, relaying.Token);
        try
        {
            // A connection's round trips to a node a second away each way, with libpq's request for SSL, take about
            // the test's 5 s, so node_b has worf.sequence_range_timeout's default for each.
            _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_timeout = '10s'", token);
            _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
            string slowDsn = string.Join(' ', dsnParts.Select(part =>
                part.StartsWith("port=", StringComparison.Ordinal) ? $"port={((IPEndPoint)slow.LocalEndpoint).Port}"
                : part.StartsWith("host=", StringComparison.Ordinal) ? "host=127.0.0.1"
                : part));
            _ = await nodeB.SafePsqlAsync(Database,
                $"UPDATE worf.node_interface SET if_dsn = '{slowDsn}' WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')", token);
            using CancellationTokenSource drawing = CancellationTokenSource.CreateLinkedTokenSource(token);
            int slowFrom = nodeB.ReadLog().Length;
            Task draws = Task.Run(async () =>
            {
                await using NpgsqlConnection writer = await nodeB.OpenAsync(Database, token);
                // The session runs an insert before the timed ones, so that its backend's first use of worf, which a loaded host
                // may hold up for a second, isn't counted as a wait.
                await using (NpgsqlCommand logged = new($"{Insert}; SET log_min_duration_statement = 0", writer))
                {
                    _ = await logged.ExecuteNonQueryAsync(token);
                }

                await using NpgsqlCommand insert = new(Insert, writer);
                while (!drawing.Token.IsCancellationRequested)
                {
                    _ = await insert.ExecuteNonQueryAsync(token);
                    await Task.Delay(TimeSpan.FromMilliseconds(5), token);
                }
            }, token);
            bool reached = await nodeB.PollQueryUntilAsync(Database, UNext, token);
            await Task.Delay(TimeSpan.FromSeconds(10), token);
            await drawing.CancelAsync();
            await draws;
            Assert.IsTrue(reached, "node_b's next block of u, from node_c once it answers again, while node_b draws from node_a");
            List<double> slowWaits = [.. nodeB.ReadLog()[slowFrom..].Split('\n')
                .Where(line => line.Contains($": {Insert}", StringComparison.Ordinal) && line.Contains("  execute ", StringComparison.Ordinal))
                .Select(line => double.Parse(line[(line.IndexOf("duration: ", StringComparison.Ordinal) + 10)..line.IndexOf(" ms", StringComparison.Ordinal)],
                    CultureInfo.InvariantCulture))];
            Assert.IsGreaterThan(100, slowWaits.Count, "node_b's inserts while node_c answers slowly");
            Assert.IsLessThan(1000.0, slowWaits.Max(), "node_b's slowest insert while node_c answers a call in two seconds");
        }
        finally
        {
            await relaying.CancelAsync();
            await relay;
        }
    }

    /// <summary>
    /// A provider whose host drops connection attempts holds up none of a node's exchanges with its other neighbors, as
    /// one that takes calls and never answers doesn't: it's probed by a connection made without waiting on it, so the
    /// node's inserts go on through small blocks from the owner, none waiting on it. A probe whose connection libpq finds
    /// bad at once, as for a host name nothing resolves, fails, and the exchange worker goes on.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task UnreachableNeighborsHoldUpNoOtherExchange()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "log_min_messages = debug2");
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);

        // node_c's host drops node_b's connection attempts.
        int logpos = nodeB.ReadLog().Length;
        _ = await nodeB.SafePsqlAsync(Database,
            $"UPDATE worf.node_interface SET if_dsn = 'host=192.0.2.1 port=5432 dbname={Database} user={SuperUser}' WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')",
            token);
        if (!await nodeB.PollLogAsync(logpos, "exchange with node node_c failed: ", token))
        {
            Assert.Fail("node_b's first connection attempt to node_c failed; node_b's workers: " + await nodeB.SafePsqlAsync(Database, """
                SELECT string_agg(concat_ws(' ', a.backend_type, a.pid, a.state, a.wait_event_type, a.wait_event, left(a.query, 120),
                    (SELECT string_agg(concat_ws(':', l.locktype, l.mode, l.relation::regclass, l.objid, l.objsubid, l.transactionid), ';')
                        FROM pg_locks l WHERE l.pid = a.pid AND NOT l.granted)), ' | ')
                FROM pg_stat_activity a WHERE a.backend_type LIKE 'worf%'
                """, token));
        }

        // node_b draws two blocks a second from node_a for fifteen seconds, and no insert waits for one. The server times
        // each insert, as the test host's own clock counts the time its other tests keep it from reading the result.
        const string Insert = "INSERT INTO t (v) VALUES ('b')";
        int timed = nodeB.ReadLog().Length;
        await using (NpgsqlConnection writer = await nodeB.OpenAsync(Database, token))
        {
            // The session runs an insert before the timed ones, so that its backend's first use of worf, which a loaded host
            // may hold up for a second, isn't counted as a wait.
            await using (NpgsqlCommand logged = new($"{Insert}; SET log_min_duration_statement = 0", writer))
            {
                _ = await logged.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand insert = new(Insert, writer);
            Stopwatch running = Stopwatch.StartNew();
            while (running.Elapsed < TimeSpan.FromSeconds(15))
            {
                _ = await insert.ExecuteNonQueryAsync(token);
                await Task.Delay(TimeSpan.FromMilliseconds(5), token);
            }
        }

        // Each attempt to reach node_c held up node_b's calls to node_a for 5 s, until the first counted as running out of
        // time; now none does after the first.
        List<double> waits = [.. nodeB.ReadLog()[timed..].Split('\n')
            .Where(line => line.Contains($": {Insert}", StringComparison.Ordinal) && line.Contains("  execute ", StringComparison.Ordinal))
            .Select(line => double.Parse(line[(line.IndexOf("duration: ", StringComparison.Ordinal) + 10)..line.IndexOf(" ms", StringComparison.Ordinal)],
                CultureInfo.InvariantCulture))];
        waits.Sort();
        Assert.IsGreaterThan(100, waits.Count, "node_b's inserts");
        Assert.IsLessThan(2500.0, waits[^1], "node_b's slowest insert, against the 5000 ms each attempt to reach node_c took");
        Assert.IsLessThan(100.0, waits[(int)(waits.Count * 0.99)], "node_b's 99th percentile insert");

        // A connection to node_c that libpq finds bad at once, for a host name nothing resolves or an option it doesn't
        // know, fails its probe, and the exchange worker goes on.
        const string Worker = "SELECT pid FROM pg_stat_activity WHERE backend_type LIKE 'worf exchange%'";
        string worker = await nodeB.SafePsqlAsync(Database, Worker, token);
        foreach ((string dsn, string failure) in new[]
        {
            ($"host=nonexistent.invalid port=5432 dbname={Database} user={SuperUser}", "could not translate host name \"nonexistent.invalid\""),
            ($"host=127.0.0.1 port=5432 dbname={Database} user={SuperUser} sslmode=bogus", "sslmode"),
        })
        {
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database,
                $"UPDATE worf.node_interface SET if_dsn = '{dsn}' WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')", token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, failure, token), $"node_b's probe of node_c failing with {failure}");
        }

        Assert.AreEqual(worker, await nodeB.SafePsqlAsync(Database, Worker, token), "node_b's exchange worker, which went on");
        Assert.DoesNotContain("cannot wait on socket", nodeB.ReadLog(), "node_b's log");
    }

    /// <summary>
    /// A provider whose host name a service file gives, and whose lookup takes seconds and fails, holds up none of a
    /// node's exchanges with its other neighbors, as the node looks up the names libpq would connect to on other threads,
    /// those from a service file or the environment too; a host name with two addresses, the first refusing, is reached at
    /// the second; an address that drops connection attempts, as a listener whose queue is full does, gives way to the
    /// next at the connection's connect_timeout, or with none, once its share of the probe's time is up, a name whose
    /// lookup still runs counting toward the share; addresses are connected to in the order of their names, as libpq
    /// connects to them; a server that refuses the connection ends the probe, as it ends libpq's, though a later name's
    /// lookup still runs; a service file's connect_timeout holds, rather than
    /// worf's own; a URI takes worf's options after it; worf.extra_connection_options reaches the exchange's
    /// connections, even when it's set while one is being made; a name with no address is passed over for the next; and
    /// an address is connected to as soon as it's known, while a later name's lookup runs, and as an earlier name's
    /// lookup fails. On Linux, where a library the server preloads answers the lookups of those names.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux)]
    public async Task LookedUpHostsHoldUpNoOtherExchange()
    {
        CancellationToken token = TestContext.CancellationToken;
        DirectoryInfo work = Directory.CreateTempSubdirectory("worf-lookup-");
        try
        {
            // worf-slow.test takes 7 s to fail, worf-two.test has 127.0.0.2, where nothing listens, before 127.0.0.1, and
            // worf-delay.test and worf-late.test take 0.3 s and 1 s to find 127.0.0.1.
            string library = await CompileAsync(work, "lookup.c", LookupLibrary, token);
            string services = Path.Join(work.FullName, "pg_service.conf");
            await File.WriteAllTextAsync(services, "[slowsvc]\nhost=worf-slow.test\nport=5432\n", token);

            await using TestNode nodeA = await StartNodeAsync("node_a", token);
            await using TestNode nodeB = await StartNodeAsync("node_b", token, "log_min_messages = debug2",
                new Dictionary<string, string?> { ["LD_PRELOAD"] = library, ["PGSERVICEFILE"] = services });
            await using TestNode nodeC = await StartNodeAsync("node_c", token);
            foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
            {
                _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
            }

            await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
            await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
            _ = await nodeA.SafePsqlAsync(Database,
                "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
            await WaitActiveAsync([nodeA, nodeB], token);

            // node_c's interface names the service, whose host's lookup takes 7 s and fails, again and again.
            int logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database,
                $"UPDATE worf.node_interface SET if_dsn = 'service=slowsvc dbname={Database} user={SuperUser}' WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')",
                token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "exchange with node node_c starting", token), "node_b calling node_c");

            // node_b draws two blocks a second from node_a for fifteen seconds, and no insert waits for one. The server
            // times each insert.
            const string Insert = "INSERT INTO t (v) VALUES ('b')";
            int timed = nodeB.ReadLog().Length;
            await using (NpgsqlConnection writer = await nodeB.OpenAsync(Database, token))
            {
                // The session runs an insert before the timed ones, so that its backend's first use of worf, which a loaded host
                // may hold up for a second, isn't counted as a wait.
                await using (NpgsqlCommand logged = new($"{Insert}; SET log_min_duration_statement = 0", writer))
                {
                    _ = await logged.ExecuteNonQueryAsync(token);
                }

                await using NpgsqlCommand insert = new(Insert, writer);
                Stopwatch running = Stopwatch.StartNew();
                while (running.Elapsed < TimeSpan.FromSeconds(15))
                {
                    _ = await insert.ExecuteNonQueryAsync(token);
                    await Task.Delay(TimeSpan.FromMilliseconds(5), token);
                }
            }

            string log = nodeB.ReadLog()[timed..];
            List<double> waits = [.. log.Split('\n')
                .Where(line => line.Contains($": {Insert}", StringComparison.Ordinal) && line.Contains("  execute ", StringComparison.Ordinal))
                .Select(line => double.Parse(line[(line.IndexOf("duration: ", StringComparison.Ordinal) + 10)..line.IndexOf(" ms", StringComparison.Ordinal)],
                    CultureInfo.InvariantCulture))];
            waits.Sort();
            Assert.IsGreaterThan(100, waits.Count, "node_b's inserts");
            Assert.IsLessThan(3500.0, waits[^1], "node_b's slowest insert, against the 7 s each lookup of node_c's host takes");
            Assert.Contains("exchange with node node_c failed", nodeB.ReadLog()[logpos..], "node_b's calls to node_c, whose host isn't found");

            // A host name whose first address refuses is reached at its second. No call through the service before it
            // was done, so one done now went over the new connection string.
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface SET if_dsn = 'host=worf-two.test port={nodeC.Cluster.Port.ToString(CultureInfo.InvariantCulture)} dbname={Database} user={SuperUser}'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "sequence range exchange with node node_c done", token), "node_b's call to node_c at its second address");

            // An address whose listener's queue is full, so that the system drops attempts to connect to it, gives way to
            // the next at connect_timeout.
            using Socket hole = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            hole.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            hole.Listen(0);

            // The connection the listener never accepts fills its queue, after which the system drops attempts to connect.
            using Socket queued = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            await queued.ConnectAsync(hole.LocalEndPoint!, token);
            using Socket dropped = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            using (CancellationTokenSource quick = CancellationTokenSource.CreateLinkedTokenSource(token))
            {
                quick.CancelAfter(TimeSpan.FromMilliseconds(500));
                _ = await Assert.ThrowsAsync<OperationCanceledException>(() => dropped.ConnectAsync(hole.LocalEndPoint!, quick.Token).AsTask(),
                    "an attempt to connect to the listener whose queue is full");
            }

            string port = nodeC.Cluster.Port.ToString(CultureInfo.InvariantCulture);
            await RefuseNodeCAsync(nodeB, token);
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface
                SET if_dsn = 'host=127.0.0.1,127.0.0.1 port={((IPEndPoint)hole.LocalEndPoint!).Port.ToString(CultureInfo.InvariantCulture)},{port} connect_timeout=2 dbname={Database} user={SuperUser}'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "sequence range exchange with node node_c done", token), "node_b's call to node_c past the address that drops attempts");

            // With no connect_timeout, the address that drops attempts gets its share of the probe's time, and gives way.
            await RefuseNodeCAsync(nodeB, token);
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface
                SET if_dsn = 'host=127.0.0.1,127.0.0.1 port={((IPEndPoint)hole.LocalEndPoint!).Port.ToString(CultureInfo.InvariantCulture)},{port} dbname={Database} user={SuperUser}'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "sequence range exchange with node node_c done", token),
                "node_b's call to node_c past the address that drops attempts, with no connect_timeout");

            // So it does when the next address comes from a name whose lookup takes 0.3 s, which counts toward the share as
            // it runs, with the connect_timeout of 30 s worf gives.
            await RefuseNodeCAsync(nodeB, token);
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface
                SET if_dsn = 'host=127.0.0.1,worf-delay.test port={((IPEndPoint)hole.LocalEndPoint!).Port.ToString(CultureInfo.InvariantCulture)},{port} dbname={Database} user={SuperUser}'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "sequence range exchange with node node_c done", token),
                "node_b's call to node_c past the address that drops attempts, at a name whose lookup was still running");

            // Addresses are connected to in the order of their names, as libpq connects to them: node_c's name, whose
            // lookup takes 0.3 s, before node_a's address, given as it is.
            await RefuseNodeCAsync(nodeB, token);
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface
                SET if_dsn = 'host=worf-delay.test,127.0.0.1 port={port},{nodeA.Cluster.Port.ToString(CultureInfo.InvariantCulture)} dbname={Database} user={SuperUser}'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "sequence range exchange with node node_c done", token), "node_b's call to node_c at its name, before node_a's address");

            // A server that refuses the connection, as for a role it doesn't have, ends the probe there, as libpq does,
            // rather than have it go on to the next.
            _ = await nodeC.SafePsqlAsync(Database, "CREATE ROLE onlyc LOGIN SUPERUSER", token);
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface
                SET if_dsn = 'host=127.0.0.1,127.0.0.1 port={nodeA.Cluster.Port.ToString(CultureInfo.InvariantCulture)},{port} dbname={Database} user=onlyc'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "role \"onlyc\" does not exist", token), "node_b's call to node_c, refused by node_a");
            await Task.Delay(TimeSpan.FromSeconds(3), token);
            Assert.DoesNotContain("bound to another role", nodeB.ReadLog()[logpos..], "node_b's calls, which reached node_c as onlyc");

            // So does one that refuses it while a later name's lookup, which takes 1 s, still runs.
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface
                SET if_dsn = 'host=127.0.0.1,worf-late.test port={nodeA.Cluster.Port.ToString(CultureInfo.InvariantCulture)},{port} dbname={Database} user=onlyc'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "role \"onlyc\" does not exist", token), "node_b's call to node_c, refused by node_a before worf-late.test is found");
            await Task.Delay(TimeSpan.FromSeconds(4), token);
            Assert.DoesNotContain("sequence range exchange with node node_c done", nodeB.ReadLog()[logpos..], "node_b's calls, which reached node_c past node_a's refusal");

            // A service file's connect_timeout holds, rather than worf's own, though each call has 60 s.
            await File.AppendAllTextAsync(services,
                $"\n[holesvc]\nhost=127.0.0.1,127.0.0.1\nport={((IPEndPoint)hole.LocalEndPoint!).Port.ToString(CultureInfo.InvariantCulture)},{port}\nconnect_timeout=2\n", token);
            _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_timeout = '60s'", token);
            _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
            await RefuseNodeCAsync(nodeB, token);
            logpos = nodeB.ReadLog().Length;
            Stopwatch passing = Stopwatch.StartNew();
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface SET if_dsn = 'service=holesvc dbname={Database} user={SuperUser}'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "sequence range exchange with node node_c done", token), "node_b's call to node_c through the service");
            Assert.IsLessThan(TimeSpan.FromSeconds(20), passing.Elapsed, "the time node_b's call took to pass the address that drops attempts, with the service's connect_timeout of 2 s");
            _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM RESET worf.sequence_range_timeout", token);
            _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);

            // A URI takes worf's options after it.
            await RefuseNodeCAsync(nodeB, token);
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface SET if_dsn = 'postgresql://{SuperUser}@127.0.0.1:{port}/{Database}'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "sequence range exchange with node node_c done", token), "node_b's call to node_c through a URI");

            // worf.extra_connection_options reaches the exchange's connections, as it does the others: here one that asks
            // for SSL, which the servers don't take. It's set while a connection is under way, passing the address that
            // drops attempts, and the connection that was made without it is made again with it.
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface
                SET if_dsn = 'host=127.0.0.1,127.0.0.1 port={((IPEndPoint)hole.LocalEndPoint!).Port.ToString(CultureInfo.InvariantCulture)},{port} connect_timeout=3 dbname={Database} user={SuperUser}'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            for (int under = logpos; ; await Task.Delay(TimeSpan.FromMilliseconds(100), token))
            {
                // A connection under way after half a second is the one passing the address that drops attempts.
                const string Connecting = "sequence range exchange with node node_c connecting";
                Assert.IsTrue(await nodeB.PollLogAsync(under, Connecting, token), "node_b connecting to node_c past the address that drops attempts");
                under += nodeB.ReadLog()[under..].IndexOf(Connecting, StringComparison.Ordinal) + Connecting.Length;
                await Task.Delay(TimeSpan.FromMilliseconds(500), token);
                string since = nodeB.ReadLog()[under..];
                if (!since.Contains("exchange with node node_c done", StringComparison.Ordinal) && !since.Contains("exchange with node node_c failed", StringComparison.Ordinal))
                {
                    break;
                }
            }

            _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.extra_connection_options = 'sslmode=require'", token);
            _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
            Stopwatch asking = Stopwatch.StartNew();
            while (!nodeB.ReadLog()[logpos..].Split('\n').Any(line => line.Contains("exchange with node node_c failed: ", StringComparison.Ordinal) &&
                line.Contains("SSL", StringComparison.Ordinal)))
            {
                Assert.IsLessThan(TestNode.PollTimeout, asking.Elapsed, "node_b's call to node_c, failed for the SSL it asked for, as the servers or libpq have none");
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            }

            _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM RESET worf.extra_connection_options", token);
            _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);

            // A name with no address is passed over.
            await RefuseNodeCAsync(nodeB, token);
            logpos = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, $"""
                UPDATE worf.node_interface SET if_dsn = 'host=worf-none.test,127.0.0.1 port={port} dbname={Database} user={SuperUser}'
                WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                """, token);
            Assert.IsTrue(await nodeB.PollLogAsync(logpos, "sequence range exchange with node node_c done", token), "node_b's call to node_c past the name with no address");

            // An address is connected to as soon as it's known, while a later name's lookup, which takes 7 s, still runs,
            // and a first name whose lookup takes 7 s and fails gives way to the next as it fails.
            foreach ((string hosts, TimeSpan bound) in new[] { ("127.0.0.1,worf-slow.test", TimeSpan.FromSeconds(5)), ("worf-slow.test,127.0.0.1", TimeSpan.FromSeconds(20)) })
            {
                await RefuseNodeCAsync(nodeB, token);
                logpos = nodeB.ReadLog().Length;
                Stopwatch looking = Stopwatch.StartNew();
                _ = await nodeB.SafePsqlAsync(Database, $"""
                    UPDATE worf.node_interface SET if_dsn = 'host={hosts} port={port},{port} dbname={Database} user={SuperUser}'
                    WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                    """, token);
                Assert.IsTrue(await nodeB.PollLogAsync(logpos, "sequence range exchange with node node_c done", token), $"node_b's call to node_c at {hosts}");
                Assert.IsLessThan(bound, looking.Elapsed, $"the time node_b's call to node_c at {hosts} took, with worf-slow.test's lookup taking 7 s");
            }
        }
        finally
        {
            work.Delete(true);
        }
    }

    /// <summary>
    /// The exchange connects to the server libpq would, from the same lists: lists whose counts libpq refuses are refused;
    /// past a server that is in hot standby, as the session wants a read-write one, the probe goes to the next host rather
    /// than to the name's next address, which it never connects to, and names the server as psql does; a server that
    /// refuses the connection ends a pass that takes a standby only, with no second pass; a standby found at a name still
    /// looked up is preferred, as libpq prefers it, to a server listed before it that isn't one; the machine's own name has
    /// the addresses getaddrinfo gives it, rather than every interface's; hostaddr entries are taken with their spaces, as
    /// addresses; a name whose hostaddr is empty is looked up away from the worker, which holds up no insert, and so is a
    /// name that has no address, when the connection prefers a standby; an address that refuses gives way to one still
    /// looked up, without the call failing; and from PostgreSQL 17, an address that never answers gives way at a
    /// connect_timeout of 1 s, where libpq waits 2 s before it. On Linux, where a library the server preloads answers the
    /// lookups of those names.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [TestCategory("EveryVersion")]
    [OSCondition(OperatingSystems.Linux)]
    public async Task ConnectionsReachTheServerLibpqWould()
    {
        CancellationToken token = TestContext.CancellationToken;
        DirectoryInfo work = Directory.CreateTempSubdirectory("worf-libpq-");
        try
        {
            string library = await CompileAsync(work, "lookup.c", LookupLibrary, token);
            await using TestNode nodeA = await StartNodeAsync("node_a", token);
            await using TestNode nodeB = await StartNodeAsync("node_b", token, "log_min_messages = debug2", new Dictionary<string, string?> { ["LD_PRELOAD"] = library });
            await using TestNode nodeC = await StartNodeAsync("node_c", token);
            foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
            {
                _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
            }

            await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
            await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
            _ = await nodeA.SafePsqlAsync(Database,
                "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
            await WaitActiveAsync([nodeA, nodeB], token);

            // node_c's hot standby listens at 127.0.0.2, on node_c's port.
            string port = nodeC.Cluster.Port.ToString(CultureInfo.InvariantCulture);
            string standbyDir = Path.Join(work.FullName, "standby");
            (int backedUp, string backupOutput) = await nodeC.RunProgramAsync("pg_basebackup",
                ["-D", standbyDir, "-d", nodeC.Dsn(Database, SuperUser), "-R", "-X", "stream", "-c", "fast"], token);
            Assert.AreEqual(0, backedUp, backupOutput);
            (int started, string startOutput) = await nodeC.RunProgramAsync("pg_ctl",
                ["start", "-w", "-D", standbyDir, "-l", Path.Join(work.FullName, "standby.log"), "-o",
                    $"-p {port} -c listen_addresses=127.0.0.2 -c unix_socket_directories={standbyDir} -c hot_standby=on -c log_connections=on"], token);
            Assert.AreEqual(0, started, startOutput);
            try
            {
                async Task<string> CallAsync(string dsn, string outcome, string user = SuperUser)
                {
                    await RefuseNodeCAsync(nodeB, token);
                    int logpos = nodeB.ReadLog().Length;
                    _ = await nodeB.SafePsqlAsync(Database, $"""
                        UPDATE worf.node_interface SET if_dsn = '{dsn} dbname={Database} user={user}'
                        WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                        """, token);
                    Assert.IsTrue(await nodeB.PollLogAsync(logpos, outcome, token), $"node_b's call to node_c over {dsn}: {outcome}");
                    return nodeB.ReadLog()[logpos..];
                }

                // Lists whose counts libpq refuses are refused.
                _ = await CallAsync($"host=127.0.0.1,127.0.0.1 port=1,{port},5", "could not match 3 port numbers to 2 hosts");
                _ = await CallAsync($"host=127.0.0.1,127.0.0.1 hostaddr=127.0.0.1 port={port}", "could not match 2 host names to 1 hostaddr values");

                // worf-two.test has the standby's address before node_c's. A read-write session passes the standby for the
                // next host, of which there is none, rather than for node_c at the name's next address.
                string readOnly = await CallAsync($"host=worf-two.test port={port} target_session_attrs=read-write", "exchange with node node_c failed");
                Assert.Contains("session is read-only", readOnly, "node_b's call to node_c, refused at the standby");
                Assert.DoesNotContain("exchange with node node_c done", readOnly, "node_b's calls to node_c, past the standby");

                // worf-first.test has node_c's address before the standby's. A connection that prefers a standby passes
                // node_c for the next host, of which there is none, and takes node_c in its second pass, from the first
                // host, as psql does, never connecting to the standby; one that takes a standby only fails, naming node_c's
                // address alone, with the name, as psql does, rather than the standby's, which libpq never tries.
                string standbyLog = Path.Join(work.FullName, "standby.log");
                int standbyAt = File.ReadAllText(standbyLog).Length;
                string preferring = await CallAsync($"host=worf-first.test port={port} target_session_attrs=prefer-standby", "sequence range exchange with node node_c done");
                Assert.IsFalse(BareFailureLine().IsMatch(preferring), "node_b's calls to node_c, ending a failure with a line for an address never tried");
                Assert.DoesNotContain("connection received", File.ReadAllText(standbyLog)[standbyAt..], "the standby's log, as node_b's call passed node_c");
                string standbyOnly = await CallAsync($"host=worf-first.test port={port} target_session_attrs=standby", "server is not in hot standby mode");
                Assert.DoesNotContain("cannot exchange sequence ranges during recovery", standbyOnly, "node_b's call to node_c, made at the standby libpq never tries");
                Assert.Contains($"connection to server at \"worf-first.test\" (127.0.0.1), port {port} failed: server is not in hot standby mode", standbyOnly,
                    "node_b's call to node_c, naming node_c as psql does");

                // A server that refuses the connection, as node_a refuses a role only node_c has, ends a pass that takes a
                // standby only, as psql's does, after node_c, which isn't one, with no second pass that logs in at node_c.
                _ = await nodeC.SafePsqlAsync(Database, "CREATE ROLE onlyc LOGIN", token);
                string onlyC = await CallAsync($"host=127.0.0.1,127.0.0.1 port={port},{nodeA.Cluster.Port.ToString(CultureInfo.InvariantCulture)} target_session_attrs=prefer-standby",
                    "exchange with node node_c failed", "onlyc");
                Assert.Contains("server is not in hot standby mode", onlyC, "node_b's call to node_c as onlyc, at node_c");
                Assert.Contains("role \"onlyc\" does not exist", onlyC, "node_b's call to node_c as onlyc, at node_a");

                // A standby found at a name still looked up is preferred to node_c, listed before it.
                string preferred = await CallAsync($"host=127.0.0.1,worf-standby.test port={port} target_session_attrs=prefer-standby",
                    "exchange with node node_c failed");
                Assert.Contains("cannot exchange sequence ranges during recovery", preferred, "node_b's call to node_c, made at the standby");

                // The machine's own name has 127.0.0.3, where nothing listens, rather than the address of an interface where
                // a listener takes connections and closes them.
                IPAddress? own = NetworkInterface.GetAllNetworkInterfaces()
                    .Where(each => each.OperationalStatus == OperationalStatus.Up && each.NetworkInterfaceType != NetworkInterfaceType.Loopback)
                    .SelectMany(each => each.GetIPProperties().UnicastAddresses).Select(each => each.Address)
                    .FirstOrDefault(address => address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(address));
                if (own is not null)
                {
                    using Socket closing = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    closing.Bind(new IPEndPoint(own, 0));
                    closing.Listen(16);
                    using CancellationTokenSource stopClosing = CancellationTokenSource.CreateLinkedTokenSource(token);
                    Task closer = Task.Run(async () =>
                    {
                        while (!stopClosing.IsCancellationRequested)
                        {
                            using Socket accepted = await closing.AcceptAsync(stopClosing.Token);
                        }
                    }, token);
                    string named = await CallAsync($"host={Dns.GetHostName()} port={((IPEndPoint)closing.LocalEndPoint!).Port.ToString(CultureInfo.InvariantCulture)}",
                        "exchange with node node_c failed");
                    Assert.Contains("127.0.0.3", named, "node_b's call to node_c at the address getaddrinfo gives the machine's name");
                    Assert.DoesNotContain("closed the connection", named, "node_b's call to node_c at an interface's address");
                    await stopClosing.CancelAsync();
                    _ = await Task.WhenAny(closer);
                }

                // A lookup that takes longer than a call goes on past it, and the next call connects with what it found, as
                // psql does once it ends.
                _ = await CallAsync($"host=worf-later.test port={port}", "sequence range exchange with node node_c done");

                // A host entry is taken with its spaces, as libpq takes it, and so is a hostaddr entry, as an address.
                _ = await CallAsync($"host=''127.0.0.4, worf-delay.test'' port={port}", "could not translate host name \" worf-delay.test\"");
                _ = await CallAsync($"hostaddr=''127.0.0.1, 127.0.0.1'' port=1,{port}", "could not parse network address \" 127.0.0.1\"");

                // An address that refuses gives way to a name still looked up, and the call doesn't fail meanwhile.
                string waited = await CallAsync($"host=127.0.0.1,worf-delay.test port=1,{port}", "sequence range exchange with node node_c done");
                Assert.DoesNotContain("exchange with node node_c failed", waited, "node_b's call to node_c, waiting for worf-delay.test");

                // A name whose hostaddr is empty is looked up away from the worker: its lookup takes 7 s and fails, which
                // a call has 20 s for, and node_b's inserts, which draw a block from node_a every 100 rows, wait for none of it;
                // nor do they when the connection prefers a standby and the name is all it has, which has no second pass.
                _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_timeout = '20s'", token);
                _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
                foreach ((string dsn, string outcome) in new[]
                {
                    ("host=worf-slow.test,127.0.0.1 hostaddr=,127.0.0.1", "sequence range exchange with node node_c done"),
                    ("host=worf-slow.test target_session_attrs=prefer-standby", "could not translate host name \"worf-slow.test\""),
                })
                {
                    await RefuseNodeCAsync(nodeB, token);
                    int slow = nodeB.ReadLog().Length;
                    _ = await nodeB.SafePsqlAsync(Database, $"""
                        UPDATE worf.node_interface SET if_dsn = '{dsn} port={port} dbname={Database} user={SuperUser}'
                        WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                        """, token);
                    await using (NpgsqlConnection writer = await nodeB.OpenAsync(Database, token))
                    {
                        await using NpgsqlCommand insert = new("INSERT INTO t (v) VALUES ('b')", writer);

                        // The session runs an insert before the timed ones, so that its backend's first use of worf, which a
                        // loaded host may hold up for a second, isn't counted as a wait.
                        _ = await insert.ExecuteNonQueryAsync(token);
                        Stopwatch running = Stopwatch.StartNew();
                        TimeSpan slowest = TimeSpan.Zero;
                        // The inserts go on until the call ends, at least 12 s.
                        while (running.Elapsed < TimeSpan.FromSeconds(12) || !nodeB.ReadLog()[slow..].Contains(outcome, StringComparison.Ordinal))
                        {
                            Assert.IsLessThan(TestNode.PollTimeout, running.Elapsed, $"node_b's call to node_c at {dsn}: {outcome}");
                            Stopwatch one = Stopwatch.StartNew();
                            _ = await insert.ExecuteNonQueryAsync(token);
                            slowest = one.Elapsed > slowest ? one.Elapsed : slowest;
                            await Task.Delay(TimeSpan.FromMilliseconds(10), token);
                        }

                        Assert.IsLessThan(TimeSpan.FromSeconds(3.5), slowest, $"node_b's slowest insert, while worf-slow.test is looked up for {dsn}, against the lookup's 7 s");
                    }

                    Assert.IsTrue(await nodeB.PollLogAsync(slow, outcome, token), $"node_b's call to node_c at {dsn}: {outcome}");
                }

                // An address that never answers, as a listener whose queue is full, gives way at connect_timeout, which is 1 s
                // from PostgreSQL 17, and at least 2 s before it, as libpq has it: node_c logs the connection that much after
                // node_b starts connecting, whatever the call takes after.
                _ = await nodeC.SafePsqlAsync(Database, "ALTER SYSTEM SET log_connections = 'on'", token);
                _ = await nodeC.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
                int received = nodeC.ReadLog().Length;
                using Socket hole = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                hole.Bind(new IPEndPoint(IPAddress.Loopback, 0));
                hole.Listen(0);
                using Socket queued = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await queued.ConnectAsync(hole.LocalEndPoint!, token);
                string timedOut = await CallAsync($"host=127.0.0.1,127.0.0.1 port={((IPEndPoint)hole.LocalEndPoint!).Port.ToString(CultureInfo.InvariantCulture)},{port} connect_timeout=1",
                    "sequence range exchange with node node_c done");
                static DateTime At(string line) => DateTime.ParseExact(line[1..24], "yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture);
                string[] lines = timedOut.Split('\n');
                DateTime connecting = At(lines.Last(line => line.Contains("exchange with node node_c connecting", StringComparison.Ordinal)));
                TimeSpan took = nodeC.ReadLog()[received..].Split('\n').Where(line => line.Contains("connection received", StringComparison.Ordinal))
                    .Select(At).First(at => at >= connecting) - connecting;
                int version = int.Parse(await nodeB.SafePsqlAsync(Database, "SHOW server_version_num", token), CultureInfo.InvariantCulture);
                if (version >= 170000)
                {
                    Assert.IsLessThan(TimeSpan.FromSeconds(1.6), took, "node_b's connection to node_c past the address that never answers, with connect_timeout=1");
                }
                else
                {
                    Assert.IsGreaterThan(TimeSpan.FromSeconds(1.9), took, "node_b's connection to node_c past the address that never answers, with connect_timeout=1");
                }
            }
            finally
            {
                _ = await nodeC.RunProgramAsync("pg_ctl", ["stop", "-w", "-m", "immediate", "-D", standbyDir], token);
            }
        }
        finally
        {
            work.Delete(recursive: true);
        }
    }

    // Points node_b's calls to node_c at a port that refuses them, and waits for one to be refused, after which node_b
    // holds no connection to node_c, so that a call done after node_c's connection string next changes went over a
    // connection made with the new one, rather than one made before it.
    private static async Task RefuseNodeCAsync(TestNode nodeB, CancellationToken token)
    {
        int logpos = nodeB.ReadLog().Length;
        _ = await nodeB.SafePsqlAsync(Database, $"""
            UPDATE worf.node_interface SET if_dsn = 'host=127.0.0.1 port=1 dbname={Database} user={SuperUser}'
            WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
            """, token);
        Stopwatch refusing = Stopwatch.StartNew();
        while (!nodeB.ReadLog()[logpos..].Split('\n').Any(line => line.Contains("exchange with node node_c failed: ", StringComparison.Ordinal) &&
            line.Contains("port 1 failed", StringComparison.Ordinal)))
        {
            Assert.IsLessThan(TestNode.PollTimeout, refusing.Elapsed, "node_b's call to node_c at the port that refuses it");
            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }
    }

    /// <summary>
    /// A node's exchange turn is held only by the work that needs it, and waits on no other session's lock, so that
    /// node_b's inserts, which draw a block from node_a every 100 rows, go on drawing: an exchange passes over the range
    /// rows a configuration change of six sequences left open holds, rather than wait on them, which would deadlock with
    /// the change as it reaches the rows the exchange took in; a forced lift on node_a waits for a configuration change or
    /// a drop left open, and for a drop under way, before it takes the turn rather than holding it, and so doesn't deadlock
    /// with the drop; a drop that waits for a report another session holds waits for it as for a lock, outside the turn,
    /// and holds nothing once canceled; sessions' own advisory locks, of whatever key, are never worf's; and a drop left
    /// open whose deletes a reply's take-in would wait for, or a fence's lift in the range worker, holds up neither the
    /// turn nor the worker, so that node_a's own inserts, which switch blocks of 10, go on too. The rows run alone: their
    /// 24 servers would take most of the 32 System V shared memory segments macOS allows beside other tests', and node_b's
    /// draws wait on how fast the exchanges of four nodes supply blocks. node_a logs the lock waits that outlast a tenth of
    /// a second.
    /// </summary>
    /// <param name="how">What holds, or would hold, the turn.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DoNotParallelize]
    [DataRow("configuration open")]
    [DataRow("drop open")]
    [DataRow("canceled drop")]
    [DataRow("waiting drop")]
    [DataRow("advisory keys")]
    [DataRow("fence beside drop")]
    public Task TurnsWaitOnNoSession(string how) => TurnsWaitOnNoSessionAsync(how);

    /// <summary>
    /// A forced lift on node_a waits for a drop under way, which waits on node_d, paused, before it takes the turn, rather
    /// than holding it, and so doesn't deadlock with the drop, as <see cref="TurnsWaitOnNoSession"/> runs it, where a
    /// process can be paused.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [DoNotParallelize]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public Task TurnsWaitOnNoSessionBesideAPausedDrop() => TurnsWaitOnNoSessionAsync("lift during drop");

    private async Task TurnsWaitOnNoSessionAsync(string how)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "log_lock_waits = on\ndeadlock_timeout = 100ms");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        await using TestNode nodeD = await StartNodeAsync("node_d", token);
        const string Tables = "CREATE TABLE t (id bigserial PRIMARY KEY, v text); CREATE TABLE u (id bigserial PRIMARY KEY); "
            + "DO $$ BEGIN FOR g IN 1..5 LOOP EXECUTE format('CREATE SEQUENCE many_%s', lpad(g::text, 2, '0')); END LOOP; END $$";
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC, nodeD })
        {
            _ = await node.SafePsqlAsync(Database, Tables, token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeA, "a_from_c", nodeC, token);
        await SubscribeAsync(nodeA, "a_from_d", nodeD, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c,node_d}', range_size := 100, max_range_size := 100);
            SELECT worf.enable_sequence_range('u_id_seq', participants := '{}', range_size := 10, max_range_size := 10);
            SELECT count(worf.enable_sequence_range(format('many_%s', lpad(g::text, 2, '0'))::regclass, participants := '{node_c,node_d}', range_size := 100))
            FROM generate_series(1, 5) g;
            """, token);
        await WaitActiveAsync([nodeA, nodeB, nodeC, nodeD], token);
        string nodeCId = await nodeC.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);

        // node_b, or node_a into u, inserts for 8 s, each insert within 5 s.
        async Task<string> InsertAsync(TestNode node, string table)
        {
            List<string> failed = [];
            await using NpgsqlConnection writer = await node.OpenAsync(Database, token);
            await using (NpgsqlCommand timeout = new("SET statement_timeout = '5s'", writer))
            {
                _ = await timeout.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand insert = new($"INSERT INTO {table} DEFAULT VALUES", writer);
            Stopwatch running = Stopwatch.StartNew();
            while (running.Elapsed < TimeSpan.FromSeconds(8))
            {
                try
                {
                    _ = await insert.ExecuteNonQueryAsync(token);
                }
                catch (PostgresException failure)
                {
                    failed.Add($"{failure.SqlState} {failure.MessageText}");
                }

                await Task.Delay(TimeSpan.FromMilliseconds(5), token);
            }

            return string.Join("\n", failed.Distinct());
        }

        const string Lift = "SELECT worf.activate_sequence_range('t_id_seq')";
        await using NpgsqlConnection holder = await nodeA.OpenAsync(Database, token);
        await using NpgsqlTransaction holding = await holder.BeginTransactionAsync(token);
        async Task HoldAsync(string sql)
        {
            await using NpgsqlCommand hold = new(sql, holder, holding);
            _ = await hold.ExecuteNonQueryAsync(token);
        }

        // A lift's outcome, which, as no fence is up, may be a refusal, but never a deadlock.
        static async Task<string> OutcomeAsync(Task work)
        {
            try
            {
                await work;
                return "done";
            }
            catch (PostgresException failure)
            {
                return failure.SqlState;
            }
        }

        switch (how)
        {
            case "configuration open" or "drop open":
            {
                // A configuration change left open holds the range rows of u_id_seq and the five others, which node_c's
                // answers leave out, and, deleted, the positions node_a recorded of node_c's sequences left out before,
                // which node_a records again for those its answers leave out.
                if (how == "configuration open")
                {
                    _ = await nodeA.SafePsqlAsync(Database, """
                        INSERT INTO worf.sequence_range_back
                        SELECT n.seqoid, n.node_id, c.call, n.sysid, n.timeline, '0/1', n.reported_start
                        FROM worf.sequence_range_node n CROSS JOIN (VALUES (true), (false)) c (call)
                        WHERE n.node_name = 'node_c' AND n.via_node = n.node_id AND n.sysid IS NOT NULL AND n.reported_start IS NOT NULL
                        ON CONFLICT DO NOTHING
                        """, token);
                }

                await HoldAsync(how == "drop open" ? "SELECT worf.drop_subscription('a_from_c')" : """
                    SELECT worf.alter_sequence_range('u_id_seq', range_size := 10);
                    SELECT count(worf.alter_sequence_range(format('many_%s', lpad(g::text, 2, '0'))::regclass, range_size := 50)) FROM generate_series(1, 5) g;
                    DELETE FROM worf.sequence_range_back WHERE node_id = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
                    """);
                Task lift = nodeA.SafePsqlAsync(Database, Lift, token);

                // node_b draws from u_id_seq too, which runs out as the change holds its range row, so that each of node_b's
                // calls takes that row in after t_id_seq's, and, once the second a take-in that began as the change ran may
                // wait has passed, none of node_a's exchanges joins the queue of a row the change holds: one that waited
                // there, however short its lock timeout, could be found in a cycle with a change that waits for a row the
                // exchange holds, and fail it as a deadlock.
                Task<long> queued = how == "configuration open" ? QueuedOnRangeRowsAsync(nodeA, TimeSpan.FromSeconds(1.5), TimeSpan.FromSeconds(6), token)
                    : Task.FromResult(0L);
                Task drawsOfU = how == "configuration open" ? DrawUntilTimeoutAsync(nodeB, "INSERT INTO u SELECT FROM generate_series(1, 1000)", token) : Task.CompletedTask;
                Assert.AreEqual("", await InsertAsync(nodeB, "t"), $"node_b's inserts as a lift waits beside a {how}");
                Assert.AreEqual(0L, await queued, "node_a's exchanges found waiting in the queue of a range row the configuration change holds");
                await holding.CommitAsync(token);
                await drawsOfU;
                Assert.AreNotEqual("40P01", await OutcomeAsync(lift), "the lift");
                break;
            }

            case "lift during drop":
            {
                // The drop holds node_a's configuration as it waits on paused node_d, when the lift starts.
                await SuspendAsync(nodeD, true, token);
                try
                {
                    Task drop = nodeA.SafePsqlAsync(Database, "SELECT worf.drop_subscription('a_from_d')", token);
                    await Task.Delay(TimeSpan.FromSeconds(2), token);
                    Task lift = nodeA.SafePsqlAsync(Database, Lift, token);
                    await Task.Delay(TimeSpan.FromSeconds(2), token);
                    await SuspendAsync(nodeD, false, token);
                    Assert.AreEqual("done", await OutcomeAsync(drop), "the drop of a_from_d beside a lift");
                    Assert.AreNotEqual("40P01", await OutcomeAsync(lift), "the lift beside the drop");
                }
                finally
                {
                    await SuspendAsync(nodeD, false, token);
                }

                Assert.AreEqual("", await InsertAsync(nodeB, "t"), "node_b's inserts after the drop and the lift");
                break;
            }

            case "canceled drop":
            {
                // A report node_c relayed of u_id_seq is held as the drop forgets node_c, which waits for it as for a lock,
                // outside the turn, until statement_timeout cancels it, as it does each of 10 times over: on Windows, whose
                // timer has the lock timeout of a try come due with the statement's about one time in eight, PostgreSQL
                // reports the lock timeout then.
                await HoldRelayedReportAsync(nodeA, holder, holding, "u_id_seq", nodeCId, token);
                await using NpgsqlConnection dropping = await nodeA.OpenAsync(Database, token);
                for (int i = 0; i < 10; i++)
                {
                    await using NpgsqlCommand drop = new("SET statement_timeout = '400ms'; SELECT worf.drop_subscription('a_from_c')", dropping) { CommandTimeout = 10 };
                    PostgresException canceled = await Assert.ThrowsExactlyAsync<PostgresException>(() => drop.ExecuteNonQueryAsync(token), $"drop {i}");
                    Assert.AreEqual("57014", canceled.SqlState, canceled.MessageText);
                }

                // No take-in waited for the held report longer than its millisecond, as node_a logs a lock wait that
                // outlasts a tenth of a second; the drops' own waits are theirs.
                Assert.AreEqual("", string.Join("\n", ReportRowWaits(nodeA.ReadLog()).Where(wait => !wait.Contains($"process {dropping.ProcessID} ", StringComparison.Ordinal))),
                    "node_a's waits for report rows");
                await holding.CommitAsync(token);
                await using (NpgsqlCommand locks = new("SET statement_timeout = 0; SELECT count(*) FROM pg_locks WHERE pid = pg_backend_pid() AND locktype = 'advisory'", dropping))
                {
                    Assert.AreEqual(0L, (long)(await locks.ExecuteScalarAsync(token))!, "the advisory locks the canceled drop's session holds");
                }

                Assert.AreEqual("", await InsertAsync(nodeB, "t"), "node_b's inserts after the canceled drop");
                break;
            }

            case "waiting drop":
            {
                // A report node_c relayed of u_id_seq is held as the drop forgets node_c, which waits for it, letting the
                // turn go meanwhile, until the holder ends.
                await HoldRelayedReportAsync(nodeA, holder, holding, "u_id_seq", nodeCId, token);
                Task<string> drop = nodeA.SafePsqlAsync(Database, "SET statement_timeout = '25s'; SELECT worf.drop_subscription('a_from_c')", token);
                Assert.AreEqual("", await InsertAsync(nodeB, "t"), "node_b's inserts as the drop waits for the held report");
                Assert.IsFalse(drop.IsCompleted, "the drop, waiting for the held report");
                await holding.CommitAsync(token);
                _ = await drop;
                break;
            }

            case "advisory keys":
            {
                // Sessions hold advisory locks of the keys of worf's turn, drop and message-building locks until 454a1ab.
                await HoldAsync("SELECT pg_advisory_lock(pg_catalog.hashtext('worf.sequence_range_exchange_turn'))");
                await using NpgsqlConnection other = await nodeB.OpenAsync(Database, token);
                await using (NpgsqlCommand key = new("""
                    SELECT pg_advisory_lock((pg_catalog.hashtext('worf.subscription')::int8 << 32) | sub_id::int8) FROM worf.subscription WHERE sub_name = 'b_from_a';
                    SELECT pg_advisory_lock(pg_catalog.hashtext('worf.sequence_range_exchange'));
                    """, other))
                {
                    _ = await key.ExecuteNonQueryAsync(token);
                }

                Assert.AreEqual("", await InsertAsync(nodeB, "t"), "node_b's inserts beside sessions' advisory locks");
                await holding.CommitAsync(token);
                break;
            }

            default:
            {
                // node_a's fence on t_id_seq waits for node_d, stopped, as a drop of a_from_c is left open; once node_d is
                // back and reports, the lift would delete node_c's wait, which the drop deleted.
                await nodeD.StopAsync("fast", token);
                _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('t_id_seq')", token);
                Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database,
                    "SELECT detail LIKE 'fenced until node node_d reports%' FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token),
                    "node_a's fence, waiting for node_d alone");
                await HoldAsync("SELECT worf.drop_subscription('a_from_c')");
                await nodeD.StartServerAsync(token);
                Assert.AreEqual("", await InsertAsync(nodeA, "u"), "node_a's inserts into u, beside its fence and the drop left open");
                await holding.CommitAsync(token);
                break;
            }
        }
    }

    /// <summary>
    /// A drop of a subscription holds no exchange up past its own deletes: left open in a transaction, or followed in
    /// the same statement by a drop whose provider never answers, it lets the node's turn go as its deletes are done, so
    /// that node_b's inserts, which draw a block from node_a every 100 rows, go on drawing.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public async Task DropsLetTheTurnGoAsTheirDeletesEnd()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        await using TestNode nodeD = await StartNodeAsync("node_d", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC, nodeD })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        await SubscribeAsync(nodeB, "b_from_d", nodeD, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);

        // node_b inserts for 12 s, each insert within 5 s.
        async Task<List<string>> InsertAsync()
        {
            List<string> failed = [];
            await using NpgsqlConnection writer = await nodeB.OpenAsync(Database, token);
            await using (NpgsqlCommand timeout = new("SET statement_timeout = '5s'", writer))
            {
                _ = await timeout.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand insert = new("INSERT INTO t (v) VALUES ('b')", writer);
            Stopwatch running = Stopwatch.StartNew();
            while (running.Elapsed < TimeSpan.FromSeconds(12))
            {
                try
                {
                    _ = await insert.ExecuteNonQueryAsync(token);
                }
                catch (PostgresException failure)
                {
                    failed.Add($"{failure.SqlState} {failure.MessageText}");
                }

                await Task.Delay(TimeSpan.FromMilliseconds(5), token);
            }

            return failed;
        }

        // A drop of b_from_c left open in a transaction.
        await using (NpgsqlConnection dropping = await nodeB.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction open = await dropping.BeginTransactionAsync(token);
            await using (NpgsqlCommand drop = new("SELECT worf.drop_subscription('b_from_c')", dropping, open))
            {
                _ = await drop.ExecuteScalarAsync(token);
            }

            List<string> failed = await InsertAsync();
            Assert.AreEqual("", string.Join("\n", failed.Distinct()), "node_b's inserts as the drop of b_from_c is left open");
            await open.CommitAsync(token);
        }

        // A drop of b_from_d, whose provider is paused, in the statement after a drop of b_from_a's sibling, b_from_c,
        // made again, once node_c has checked node_b's report since node_b started, as node_b's draws wait for it to.
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT confirmed_start IS NOT NULL FROM worf.sequence_range_node WHERE node_name = 'node_c' AND seqoid = 't_id_seq'::regclass", token),
            "node_c's check of node_b's report");
        await SuspendAsync(nodeD, true, token);
        try
        {
            Task<string> both = nodeB.SafePsqlAsync(Database, "SELECT worf.drop_subscription('b_from_c'), worf.drop_subscription('b_from_d')", token);
            List<string> failed = await InsertAsync();
            Assert.AreEqual("", string.Join("\n", failed.Distinct()), "node_b's inserts as the drop of b_from_d waits on node_d");
            Assert.IsFalse(both.IsCompleted, "the drops, waiting on node_d");
            await SuspendAsync(nodeD, false, token);
            _ = await both;
        }
        finally
        {
            await SuspendAsync(nodeD, false, token);
        }
    }

    /// <summary>
    /// Two nodes that drop their subscriptions to each other at once, each after another drop in the same statement, each
    /// tell the other they leave its slot, so that neither keeps a link to the other, which its fences would wait for.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task MutualDropsLeaveEachOthersSlots()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeA, "a_from_c", nodeC, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeC.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_a,node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB, nodeC], token);
        string nodeAId = await nodeA.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        string nodeBId = await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT EXISTS (SELECT 1 FROM worf.sequence_range_link WHERE node_id = {nodeBId})", token), "node_a's link to node_b");
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"SELECT EXISTS (SELECT 1 FROM worf.sequence_range_link WHERE node_id = {nodeAId})", token), "node_b's link to node_a");

        Stopwatch dropping = Stopwatch.StartNew();
        await Task.WhenAll(
            nodeA.SafePsqlAsync(Database, "SELECT worf.drop_subscription('a_from_c'), worf.drop_subscription('a_from_b')", token),
            nodeB.SafePsqlAsync(Database, "SELECT worf.drop_subscription('b_from_c'), worf.drop_subscription('b_from_a')", token));
        TimeSpan took = dropping.Elapsed;
        Assert.AreEqual("0|0", $"{await nodeA.SafePsqlAsync(Database, $"SELECT count(*) FROM worf.sequence_range_link WHERE node_id = {nodeBId}", token)}|" +
            await nodeB.SafePsqlAsync(Database, $"SELECT count(*) FROM worf.sequence_range_link WHERE node_id = {nodeAId}", token),
            $"the links node_a and node_b keep to each other after the drops, which took {took}");
    }

    /// <summary>
    /// A forced lift, and the exchange worker's forgetting of a neighbor it no longer exchanges with, write reports in
    /// orders of their own, and so wait for the node's turn, as an exchange's take-in does: here, while a node that takes
    /// over another's slot forgets the old node in the turn, waiting for a report the old node relayed, which a session
    /// holds.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task LiftsAndForgettingTakeTheTurn()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);

        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            // A made-up node takes over the slot turn_link, bound to another, and forgets that one in the node's turn,
            // waiting for the report it relayed, which a session holds; each node's are their own, as node_a's reports
            // reach node_b.
            int old = node == nodeA ? 3000001 : 4000001;
            _ = await node.SafePsqlAsync(Database, "SELECT pg_create_logical_replication_slot('turn_link', 'worf')", token);
            _ = await node.SafePsqlAsync(Database, $"""
                CREATE ROLE turn_user; GRANT USAGE ON SCHEMA worf TO turn_user; GRANT EXECUTE ON FUNCTION worf.sequence_range_exchange(text) TO turn_user;
                INSERT INTO worf.sequence_range_link VALUES ('turn_link', {old}, 'turn_user'::regrole);
                INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, reported_at) VALUES ('t_id_seq', {old + 1}, 'relayed', {old}, now());
                """, token);
            await using NpgsqlConnection holder = await node.OpenAsync(Database, token);
            await using NpgsqlTransaction holding = await holder.BeginTransactionAsync(token);
            await using (NpgsqlCommand hold = new($"SELECT 1 FROM worf.sequence_range_node WHERE via_node = {old} FOR UPDATE", holder, holding))
            {
                _ = await hold.ExecuteNonQueryAsync(token);
            }

            string taker = $$"""{"node_id": {{old + 2}}, "node_name": "taker", "slot": "turn_link", "sysid": "1", "timeline": 1, "wal_lsn": "0/0", "sequences": []}""";
            Task takeover = node.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION turn_user; SELECT worf.sequence_range_exchange('{taker}')", token);
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, """
                SELECT EXISTS (
                    SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                    WHERE l.locktype = 'advisory' AND l.objsubid = 5 AND l.granted AND a.query LIKE '%sequence_range_exchange(%taker%')
                """, token), $"the takeover, holding {node.Name}'s turn");

            if (node == nodeA)
            {
                // A lift on node_a, the owner, waits for the turn.
                Task<bool> lifting = node.PsqlAsync(Database, "SELECT worf.activate_sequence_range('t_id_seq')", token);
                await Task.Delay(TimeSpan.FromSeconds(3), token);
                Assert.IsFalse(lifting.IsCompleted, "the lift, waiting for node_a's turn");
                Assert.AreEqual("t", await node.SafePsqlAsync(Database, """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                        WHERE l.locktype = 'advisory' AND l.objsubid = 5 AND NOT l.granted AND a.query LIKE '%activate_sequence_range%')
                    """, token), "the lift, waiting for node_a's turn itself");
                await holding.CommitAsync(token);
                await takeover;
                _ = await lifting;
            }
            else
            {
                // A neighbor node_b no longer exchanges with, of which it keeps a report, is forgotten in the turn.
                _ = await node.SafePsqlAsync(Database,
                    "INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, reported_at) VALUES ('t_id_seq', 424242, 'gone', 424242, now())", token);
                await Task.Delay(TimeSpan.FromSeconds(3), token);
                Assert.AreEqual("1", await node.SafePsqlAsync(Database, "SELECT count(*) FROM worf.sequence_range_node WHERE via_node = 424242", token),
                    "the report of the neighbor node_b no longer exchanges with, while the takeover holds node_b's turn");
                await holding.CommitAsync(token);
                await takeover;
                Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT NOT EXISTS (SELECT 1 FROM worf.sequence_range_node WHERE via_node = 424242)", token),
                    "the report of the neighbor node_b no longer exchanges with, forgotten once the takeover lets the turn go");
            }
        }
    }

    /// <summary>
    /// A provider whose rows come back once its subscription is dropped, as a reply to a call does that the exchange
    /// worker takes in after the drop forgot the provider, stays forgotten: the rows go at the exchange worker's next
    /// round, and stay gone. A reply that a trigger holds up until the drop has committed, which saw the subscription as
    /// it began and meets the rows the drop deleted only then, rolls back, rather than write them again.
    /// </summary>
    /// <param name="when">When the rows come back: after the drop, or as a reply held up across it writes them.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("late")]
    [DataRow("held")]
    public async Task DroppedProvidersStayForgotten(string when)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        // Once it's turned on, taking in node_a's report on node_b waits 4 s as it updates node_b's own range row, before
        // it locks node_a's, and each row of node_a's written is noted, with when it was, as the transaction that wrote it
        // commits.
        _ = await nodeB.SafePsqlAsync(Database, """
            CREATE TABLE slow_on (); CREATE FUNCTION slow_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT FROM public.slow_on) THEN PERFORM pg_sleep(4); END IF; RETURN NEW; END $$;
            CREATE TRIGGER slow_insert BEFORE UPDATE ON worf.sequence_range FOR EACH ROW EXECUTE FUNCTION slow_insert();
            CREATE TABLE written_rows (node_id oid, written_at timestamptz);
            CREATE FUNCTION note_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT FROM public.slow_on) THEN INSERT INTO public.written_rows VALUES (NEW.node_id, clock_timestamp()); END IF; RETURN NEW; END $$;
            CREATE TRIGGER note_insert AFTER INSERT ON worf.sequence_range_node FOR EACH ROW EXECUTE FUNCTION note_insert();
            """, token);

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        string nodeAId = await nodeA.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        string Rows = $"SELECT count(*) FROM worf.sequence_range_node WHERE via_node = {nodeAId}";
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"SELECT ({Rows}) > 0", token), "node_a's rows on node_b");
        string rows = await nodeB.SafePsqlAsync(Database, $"SELECT json_agg(n) FROM worf.sequence_range_node n WHERE via_node = {nodeAId}", token);

        if (when == "held")
        {
            // node_b draws through its blocks, so that the reply bringing its next one changes node_a's report, and its
            // exchange worker, which saw the subscription as it took the reply in, waits in the trigger as it writes
            // node_a's row, while the drop commits.
            _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO slow_on DEFAULT VALUES", token);
            using CancellationTokenSource drawing = CancellationTokenSource.CreateLinkedTokenSource(token);
            Task draws = Task.Run(async () =>
            {
                while (!drawing.IsCancellationRequested)
                {
                    try
                    {
                        _ = await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '2s'; INSERT INTO t (v) SELECT 'b' FROM generate_series(1, 20)", drawing.Token);
                    }
                    catch (PostgresException failed) when (failed.SqlState is "57014" or "55000" or "2200H")
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(100), drawing.Token);
                    }
                }
            }, drawing.Token);
            try
            {
                Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE backend_type LIKE 'worf exchange%' AND wait_event = 'PgSleep')", token),
                    "node_b's exchange worker writing node_a's row");
                _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.drop_subscription('b_from_a')", token);
                string dropped = await nodeB.SafePsqlAsync(Database, "SELECT clock_timestamp()", token);
                Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
                    "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE backend_type LIKE 'worf exchange%' AND wait_event = 'PgSleep')", token),
                    "node_b's exchange worker, past the trigger");
                await Task.Delay(TimeSpan.FromSeconds(1), token);
                Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, $"SELECT count(*) FROM written_rows WHERE node_id = {nodeAId} AND written_at > '{dropped}'", token),
                    "node_a's rows the reply held up across the drop wrote, once the drop had committed");
            }
            finally
            {
                await drawing.CancelAsync();
                _ = await Task.WhenAny(draws);
            }
        }
        else
        {
            // The drop forgets node_a, and node_a's rows come back after it, as a late reply's would, or as one does.
            _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.drop_subscription('b_from_a')", token);
            _ = await nodeB.SafePsqlAsync(Database,
                $"INSERT INTO worf.sequence_range_node SELECT * FROM json_populate_recordset(NULL::worf.sequence_range_node, '{rows.Replace("'", "''", StringComparison.Ordinal)}') ON CONFLICT DO NOTHING",
                token);
        }

        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"SELECT ({Rows}) = 0", token), "node_a's rows on node_b, once the subscription is dropped");
        List<string> counts = [];
        for (int i = 0; i < 20; i++)
        {
            counts.Add(await nodeB.SafePsqlAsync(Database, Rows, token));
            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }

        Assert.AreEqual(string.Join(',', Enumerable.Repeat("0", 20)), string.Join(',', counts), "node_a's rows on node_b over 2 s after they went");
    }

    /// <summary>
    /// A warning a neighbor sends a node's exchange connection lands in the node's log as its own line, with its time and
    /// process, naming the neighbor, rather than bare on the server's standard error, as the warnings a trigger raises as
    /// the node's call writes the neighbor's reports do, and so does one the neighbor sends any of the node's connections
    /// as it logs in, as a role's setting that can't apply gives each. The neighbor's messages below a warning, as the
    /// debug and log messages the trigger also sends, aren't logged, and of its 30 warnings each time, only the first ten
    /// in a minute are, with one line that says so, across the node's apply and exchange workers as they restart, each
    /// of whose connections gets the warning as it logs in.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task NeighborsWarningsAreLoggedNamingThem()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE FUNCTION warn_report() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM set_config('client_min_messages', 'debug1', true);
                RAISE DEBUG 'worf-notice-debug';
                RAISE LOG 'worf-notice-log';
                FOR i IN 1..30 LOOP
                    RAISE WARNING 'worf-notice-flood %', i;
                END LOOP;
                RETURN NULL;
            END $$;
            CREATE TRIGGER warn_report AFTER INSERT OR UPDATE ON worf.sequence_range_node FOR EACH STATEMENT EXECUTE FUNCTION warn_report();
            """, token);
        _ = await nodeA.SafePsqlAsync(Database, $"ALTER ROLE {SuperUser} IN DATABASE {Database} SET default_text_search_config = 'pg_catalog.worf_notice_probe'", token);
        int logpos = nodeB.ReadLog().Length;
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        Assert.IsTrue(await nodeB.PollLogAsync(logpos, "node node_a sent WARNING: worf-notice-flood 1", token), "node_a's warning in node_b's log");
        Assert.IsTrue(await nodeB.PollLogAsync(logpos, "node node_a sent more than 10 warnings within a minute", token), "the line that says node_a's warnings went unlogged");
        Assert.IsTrue(await nodeB.PollLogAsync(logpos, "sent WARNING: invalid value for parameter \"default_text_search_config\"", token),
            "the warning node_a sends node_b's connections as they log in");
        Assert.Contains($"the server at 127.0.0.1:{nodeA.Cluster.Port.ToString(CultureInfo.InvariantCulture)} sent WARNING: invalid value for parameter", nodeB.ReadLog()[logpos..],
            "the warning node_a sends create_subscription's connections, made before node_a is node_b's node, naming its host and port");
        Assert.DoesNotContain("a server sent WARNING", nodeB.ReadLog()[logpos..], "warnings of a server not named");

        // node_b's apply and exchange workers restart three times, each time connecting to node_a again, as connections
        // whose name, process and start differ from those before, since Windows gives a new process an ID an ended one had.
        Stopwatch restarting = Stopwatch.StartNew();
        const string Connections = """
            SELECT coalesce(string_agg(application_name || ' ' || pid || ' ' || backend_start, ',' ORDER BY application_name), '')
            FROM pg_stat_activity WHERE application_name IN ('b_from_a', 'b_from_a_range')
            """;
        for (int restart = 0; restart < 3; restart++)
        {
            string[] before = (await nodeA.SafePsqlAsync(Database, Connections, token)).Split(',');
            _ = await nodeB.SafePsqlAsync(Database,
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type LIKE 'worf apply%' OR backend_type LIKE 'worf exchange%'", token);
            Stopwatch waiting = Stopwatch.StartNew();
            while ((await nodeA.SafePsqlAsync(Database, Connections, token)).Split(',') is var now && (now.Length != 2 || now.Intersect(before).Any()))
            {
                Assert.IsLessThan(TestNode.PollTimeout, waiting.Elapsed, $"node_b's apply and exchange connections to node_a after restart {restart + 1}");
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            }
        }

        await Task.Delay(TimeSpan.FromSeconds(3), token);
        TestContext.WriteLine($"restarts: {restarting.Elapsed.TotalSeconds:F1} s");
        Assert.IsLessThan(TimeSpan.FromSeconds(45), restarting.Elapsed, "the restarts, which must end within node_a's minute");
        string log = nodeB.ReadLog()[logpos..];
        string[] lines = log.Split('\n');
        Assert.DoesNotContain("\nWARNING:  worf-notice-flood", log, "node_a's warning, bare, in node_b's log");
        Assert.DoesNotContain("\nWARNING:  invalid value for parameter", log, "the warning node_a sends as a connection logs in, bare, in node_b's log");
        _ = Assert.ContainsSingle(line => line.Contains(" sent more than 10 warnings within a minute", StringComparison.Ordinal), lines,
            "the lines that say node_a's warnings went unlogged, within a minute");
        Assert.DoesNotContain("worf-notice-debug", log, "node_a's debug message in node_b's log");
        Assert.DoesNotContain("worf-notice-log", log, "node_a's log message in node_b's log");
        Assert.IsLessThanOrEqualTo(10, lines.Count(line => line.Contains(" sent WARNING: ", StringComparison.Ordinal) && line.Contains(": LOG:  ", StringComparison.Ordinal) &&
            line[(line.IndexOf(": LOG:  ", StringComparison.Ordinal) + 8)..].StartsWith("node node_a sent", StringComparison.Ordinal)),
            "node_a's warnings logged in node_b's log within a minute, from its exchange, apply and sync connections");
        Assert.DoesNotContain(line => line.Contains(" sent WARNING: ", StringComparison.Ordinal) && !line.Contains("node node_a sent WARNING: ", StringComparison.Ordinal) &&
            line.Contains("[]: ", StringComparison.Ordinal), lines, "node_a's warnings to node_b's workers, which name it");
    }

    /// <summary>
    /// A provider that stops reading once its connection is set up holds up no other exchange, though the node's calls to
    /// it, which carry its 151 managed sequences, are larger than its sockets hold: the node sends what they take as they
    /// take it, without waiting on them, goes on exchanging with its other provider, and gives the call up at its time.
    /// The owner starts all 151 roots with each call given 5 s, and the calls, over 85 KB, carry nothing after their
    /// message. The stalled call has 15 s, which spans several of a slow host's exchanges with the other provider. The
    /// test runs alone, as starting 151 roots on a host whose cores other tests' clusters share takes minutes.
    /// On Linux, where a library the server preloads gives the node's connections to the provider a small send buffer.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [DoNotParallelize]
    [OSCondition(OperatingSystems.Linux)]
    public async Task LargeCallsToStalledProvidersHoldUpNoOtherExchange()
    {
        CancellationToken token = TestContext.CancellationToken;
        DirectoryInfo work = Directory.CreateTempSubdirectory("worf-stall-");
        try
        {
            await StalledProviderAsync(work, token);
        }
        finally
        {
            work.Delete(true);
        }
    }

    // The stalled-provider test, its preloaded library and the files it reads in a directory of its own.
    private static async Task StalledProviderAsync(DirectoryInfo work, CancellationToken token)
    {
        string portFile = Path.Join(work.FullName, "port");
        string library = await CompileAsync(work, "small_send.c", """
            #define _GNU_SOURCE
            #include <dlfcn.h>
            #include <netinet/in.h>
            #include <stdio.h>
            #include <stdlib.h>
            #include <sys/socket.h>

            int connect(int fd, const struct sockaddr *addr, socklen_t len)
            {
                static int (*next)(int, const struct sockaddr *, socklen_t);
                const char *file = getenv("WORF_SMALL_SEND_PORT_FILE");
                if (!next)
                    next = dlsym(RTLD_NEXT, "connect");
                if (file && addr && addr->sa_family == AF_INET)
                {
                    FILE *f = fopen(file, "r");
                    int port = 0;
                    if (f)
                    {
                        if (fscanf(f, "%d", &port) != 1)
                            port = 0;
                        fclose(f);
                    }

                    if (port != 0 && ntohs(((const struct sockaddr_in *) addr)->sin_port) == port)
                    {
                        int size = 4096;
                        setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof size);
                    }
                }

                return next(fd, addr, len);
            }
            """, token);
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "log_min_messages = debug2",
            new Dictionary<string, string?> { ["LD_PRELOAD"] = library, ["WORF_SMALL_SEND_PORT_FILE"] = portFile });
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        const string Sequences = "CREATE TABLE t (id bigserial PRIMARY KEY, v text); DO $$ BEGIN FOR g IN 1..150 LOOP EXECUTE format('CREATE SEQUENCE many_%s', g); END LOOP; END $$";
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, Sequences, token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100);
            SELECT count(worf.enable_sequence_range(format('many_%s', g)::regclass, participants := '{node_b}', range_size := 1000)) FROM generate_series(1, 150) g;
            """, token);
        await WaitActiveAsync([nodeA, nodeB], token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 151 FROM worf.sequence_ranges WHERE state = 'active'", token),
            "node_b drawing from blocks of every sequence");

        // node_b gives each call 15 s from now, which a stalled call to node_c spans several exchanges with node_a of,
        // though each carries 151 sequences.
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_timeout = '15s'", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);

        // node_b's calls to node_c go through a relay that stops reading each connection once its settings have passed.
        await using ProviderRelay relay = new(nodeC.Cluster.Port, stall: true);
        await File.WriteAllTextAsync(portFile, relay.Port.ToString(CultureInfo.InvariantCulture), token);
        _ = await nodeB.SafePsqlAsync(Database, $"""
            UPDATE worf.node_interface SET if_dsn = 'host=127.0.0.1 port={relay.Port.ToString(CultureInfo.InvariantCulture)} dbname={Database} user={SuperUser} sslmode=disable gssencmode=disable'
            WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
            """, token);
        Stopwatch connecting = Stopwatch.StartNew();
        while (relay.Queries.IsEmpty)
        {
            Assert.IsLessThan(TestNode.PollTimeout, connecting.Elapsed, "node_b's connection to node_c through the relay");
            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
        }

        // node_b goes on exchanging with node_a while its call to node_c can't be sent whole, and gives that call up at
        // its time.
        const string GivenUp = "exchange with node node_c failed: node node_c didn't answer within 15000 ms";
        int logpos = nodeB.ReadLog().Length;
        Assert.IsTrue(await nodeB.PollLogAsync(logpos, GivenUp, token), "node_b's call to node_c, given up at its time");
        string log = nodeB.ReadLog()[logpos..];
        Assert.IsGreaterThan(1, log[..log.IndexOf(GivenUp, StringComparison.Ordinal)].Split('\n')
            .Count(line => line.Contains("sequence range exchange with node node_a done", StringComparison.Ordinal)),
            "node_b's exchanges with node_a while its call to node_c stalled, before it was given up");

        // Calls of this size, over 85 KB, carry their message as it was, with nothing after it.
        Assert.AreEqual(0, (nodeA.ReadLog() + nodeB.ReadLog()).Split('\n').Count(line => line.Contains("after a single JSON value", StringComparison.Ordinal)),
            "the nodes' log lines about a call with bytes after its message");
    }

    /// <summary>
    /// Statements that each draw one value with a SELECT of nextval, which holds no switch gate, wait for the switch from
    /// the block's last twentieth, as inserts do, rather than run the block out, on the owner and a participant alike;
    /// so do DO blocks and calls of a PL/pgSQL function that each draw one, on the participant, and calls of the function
    /// within one transaction, whose expression PL/pgSQL starts only once; and so does a single SELECT that draws more
    /// values than a block holds, row by row or within an aggregate. Statements of the same text, as SELECT
    /// nextval('sx') under a search path of "$user", public, wait for the switch of the sequence they draw from as the role
    /// they run as, though another role drew from its own sequence of that name with the same text in the session.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SeparateDrawsWaitForTheSwitch()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id bigserial PRIMARY KEY, v text); CREATE SEQUENCE s1;
                CREATE FUNCTION f_s1() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN nextval('s1'); END $$;
                CREATE FUNCTION f_loop(n int) RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE x bigint; BEGIN FOR i IN 1..n LOOP x := nextval('s1'); END LOOP; RETURN x; END $$;
                CREATE TABLE trg (id bigint, v text);
                CREATE FUNCTION trg_id() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.id := nextval('s1'); RETURN NEW; END $$;
                CREATE TRIGGER trg_id BEFORE INSERT ON trg FOR EACH ROW EXECUTE FUNCTION trg_id();
                CREATE TABLE vals (id bigint);
                CREATE TABLE seqnames (n text); INSERT INTO seqnames VALUES ('s1');
                CREATE TABLE g13t (id bigint DEFAULT nextval('s1'), w bigint);
                CREATE FUNCTION g12() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.w := nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1'); RETURN NEW; END $$;
                CREATE TRIGGER g12 BEFORE INSERT ON g13t FOR EACH ROW EXECUTE FUNCTION g12();
                CREATE TABLE g13c (id bigint DEFAULT nextval('s1'), v text CHECK (nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') > 0));
                CREATE TABLE g13l (id bigint DEFAULT nextval('s1'), w bigint);
                CREATE FUNCTION g12_loop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.w := 0; FOR i IN 1..12 LOOP NEW.w := NEW.w + nextval('s1'); END LOOP; RETURN NEW; END $$;
                CREATE TRIGGER g12_loop BEFORE INSERT ON g13l FOR EACH ROW EXECUTE FUNCTION g12_loop();
                CREATE TABLE g13h (id bigint DEFAULT nextval('s1'), w bigint);
                CREATE FUNCTION draw12() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1') + nextval('s1'); END $$;
                CREATE FUNCTION g12_helper() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.w := draw12(); RETURN NEW; END $$;
                CREATE TRIGGER g12_helper BEFORE INSERT ON g13h FOR EACH ROW EXECUTE FUNCTION g12_helper();
                CREATE TABLE ck (v text CHECK (nextval('s1') > 0));
                CREATE TABLE tw (v text);
                CREATE FUNCTION passed() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
                CREATE TRIGGER tw_when BEFORE INSERT ON tw FOR EACH ROW WHEN (nextval('s1') > 0) EXECUTE FUNCTION passed();
                CREATE FUNCTION f_sql() RETURNS bigint LANGUAGE sql AS 'SELECT nextval(''s1'')';
                CREATE FUNCTION f_sql_loop(n int) RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE x bigint; BEGIN FOR i IN 1..n LOOP x := f_sql(); END LOOP; RETURN x; END $$;
                CREATE FUNCTION f_named_loop(n int, name text) RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE x bigint; BEGIN FOR i IN 1..n LOOP x := nextval(name); END LOOP; RETURN x; END $$;
                CREATE FUNCTION f_exec(n int) RETURNS void LANGUAGE plpgsql AS $$
                BEGIN EXECUTE format('DO $d$ DECLARE x bigint; BEGIN FOR i IN 1..%s LOOP x := nextval(''s1''); END LOOP; END $d$', n); END $$;
                CREATE EXTENSION autoinc;
                CREATE TABLE ai (id int, v text);
                CREATE TRIGGER ai_id BEFORE INSERT ON ai FOR EACH ROW EXECUTE FUNCTION autoinc(id, s1);
                """, token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100);
            SELECT worf.enable_sequence_range('s1', participants := '{node_b}', range_size := 100, max_range_size := 100);
            """, token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 's1'::regclass", token),
                $"{node.Name} drawing from a block of s1, with the next one ready");
            await using NpgsqlConnection connection = await node.OpenAsync(Database, token);
            await using NpgsqlCommand draw = new("SELECT nextval('s1')", connection);
            HashSet<long> drawn = [];
            for (int i = 0; i < 250; i++)
            {
                _ = drawn.Add((long)(await draw.ExecuteScalarAsync(token))!);
            }

            Assert.HasCount(250, drawn, $"the distinct values {node.Name}'s 250 statements drew");
        }

        // Besides, a PL/pgSQL loop's assignment, in a function or a DO block, which PL/pgSQL starts once; an ARRAY or a
        // scalar subquery, which runs as the statement's initPlan; three calls in each row; rows a PL/pgSQL trigger draws
        // for, inserted from a SELECT or copied; a statement like one that failed as it drew, which left its plan's
        // nodes counted; calls that first start as the statement runs, in a VALUES list's rows, a table's CHECK
        // constraint and a trigger's WHEN condition; PL/pgSQL loops that draw through a SQL function, with a computed name,
        // or in a DO block that EXECUTE runs; rows of 40 calls, more than the block's last twentieth; rows that contrib's
        // autoinc trigger draws for; calls whose sequence a column names; a prepared statement whose parameter names it,
        // under a generic plan; a VALUES list of six rows of 40 calls each, which each row starts before it draws, alone,
        // through UNION ALL or in a MATERIALIZED CTE; and rows that hold the gate and draw 13 values each, a default's and
        // twelve of a trigger's or a CHECK constraint's, or of a trigger's loop or a function it calls, from a SELECT or in
        // single-row inserts of one transaction.
        string values40 = $"({string.Join(" + ", Enumerable.Repeat("nextval('s1')", 40))})";
        List<string> problems = [];
        foreach (string statement in new[] { "DO $$ BEGIN PERFORM nextval('s1'); END $$", "SELECT f_s1()", "BEGIN", "SELECT nextval('s1') FROM generate_series(1, 250)",
            "SELECT count(DISTINCT nextval('s1')) FROM generate_series(1, 250)", "SELECT f_loop(250) /* generate_series */",
            "DO $$ DECLARE x bigint; BEGIN FOR i IN 1..250 LOOP x := nextval('s1'); END LOOP; END $$ /* generate_series */",
            "SELECT array_length(ARRAY(SELECT nextval('s1') FROM generate_series(1, 250)), 1)",
            "SELECT (SELECT count(nextval('s1')) FROM generate_series(1, 250))", "SELECT nextval('s1'), nextval('s1'), nextval('s1') FROM generate_series(1, 100)",
            "INSERT INTO trg (v) SELECT 'x' FROM generate_series(1, 250)", "COPY", "FAILED",
            $"INSERT INTO vals VALUES {string.Join(", ", Enumerable.Repeat("(nextval('s1'))", 250))} /* generate_series */",
            "INSERT INTO ck SELECT 'x' FROM generate_series(1, 250)", "INSERT INTO tw SELECT 'x' FROM generate_series(1, 250)",
            "SELECT f_sql_loop(250) /* generate_series */", "SELECT f_named_loop(250, 's1') /* generate_series */", "SELECT f_exec(250) /* generate_series */",
            $"SELECT {string.Join(", ", Enumerable.Repeat("nextval('s1')", 40))} FROM generate_series(1, 6)",
            "INSERT INTO ai (v) SELECT 'x' FROM generate_series(1, 250)", "SELECT nextval(n) FROM seqnames, generate_series(1, 250)",
            "PREPARED generic rows", "PREPARED generic", "PREPARED default",
            $"INSERT INTO vals VALUES {string.Join(", ", Enumerable.Repeat($"({string.Join(" + ", Enumerable.Repeat("nextval('s1')", 40))})", 6))} /* generate_series */",
            "INSERT INTO g13t (w) SELECT 0 FROM generate_series(1, 20)", "INSERT INTO g13c (v) SELECT 'x' FROM generate_series(1, 20)",
            $"INSERT INTO vals SELECT * FROM (VALUES {string.Join(", ", Enumerable.Repeat(values40, 3))}) a UNION ALL SELECT * FROM (VALUES {string.Join(", ", Enumerable.Repeat(values40, 3))}) b /* generate_series */",
            $"WITH v AS MATERIALIZED (VALUES {string.Join(", ", Enumerable.Repeat(values40, 6))}) INSERT INTO vals SELECT * FROM v /* generate_series */",
            "INSERT INTO g13l (w) SELECT 0 FROM generate_series(1, 20)", "INSERT INTO g13h (w) SELECT 0 FROM generate_series(1, 20)", "BEGIN g13l", "BEGIN g13h" })
        {
            Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 's1'::regclass", token),
                "node_b drawing from a block of s1, with the next one ready");
            await using NpgsqlConnection connection = await nodeB.OpenAsync(Database, token);
            try
            {
                if (statement == "COPY")
                {
                    // The COPY ends as its writer is disposed, leaving this block.
                    await using TextWriter rows = await connection.BeginTextImportAsync("COPY trg (v) FROM STDIN", token);
                    for (int i = 0; i < 250; i++)
                    {
                        await rows.WriteLineAsync("x");
                    }

                    continue;
                }

                if (statement == "FAILED")
                {
                    // The statement fails as it draws, at its 21st row, and the same one that doesn't fail runs after it.
                    await using NpgsqlCommand failing = new("SELECT count(*) FROM (SELECT nextval('s1') n FROM generate_series(1, 250) g WHERE 1 / (g - 21) IS NOT NULL) x", connection);
                    PostgresException divided = await Assert.ThrowsExactlyAsync<PostgresException>(() => failing.ExecuteScalarAsync(token));
                    Assert.AreEqual("22012", divided.SqlState, divided.MessageText);
                    await using NpgsqlCommand counting = new("SELECT count(*) FROM (SELECT nextval('s1') n FROM generate_series(1, 250) g WHERE 1 / (g + 1) IS NOT NULL) x", connection);
                    _ = await counting.ExecuteScalarAsync(token);
                    continue;
                }

                if (statement.StartsWith("PREPARED ", StringComparison.Ordinal))
                {
                    // A prepared statement whose parameter names the sequence, under a generic plan, which leaves it unknown,
                    // in one 250-row execution or 250 one-row ones, or under the default plan_cache_mode, which turns to the
                    // generic plan after five.
                    bool rows = statement.EndsWith(" rows", StringComparison.Ordinal);
                    string mode = statement.StartsWith("PREPARED generic", StringComparison.Ordinal) ? "force_generic_plan" : "auto";
                    await using (NpgsqlCommand prepare = new($"""
                        SET plan_cache_mode = {mode};
                        PREPARE named (regclass) AS SELECT nextval($1){(rows ? " FROM generate_series(1, 250)" : "")}
                        """, connection))
                    {
                        _ = await prepare.ExecuteNonQueryAsync(token);
                    }

                    await using NpgsqlCommand execute = new("EXECUTE named('s1')", connection);
                    for (int i = 0; i < (rows ? 1 : 250); i++)
                    {
                        _ = await execute.ExecuteNonQueryAsync(token);
                    }

                    continue;
                }

                if (statement.StartsWith("BEGIN g13", StringComparison.Ordinal))
                {
                    // Single-row inserts in one transaction, whose trigger draws 12 in a loop or through a function it calls.
                    await using NpgsqlTransaction inserting = await connection.BeginTransactionAsync(token);
                    await using NpgsqlCommand insert = new($"INSERT INTO {statement["BEGIN ".Length..]} (w) VALUES (0)", connection, inserting);
                    for (int i = 0; i < 250; i++)
                    {
                        _ = await insert.ExecuteNonQueryAsync(token);
                    }

                    await inserting.CommitAsync(token);
                    continue;
                }

                if (statement == "BEGIN")
                {
                    // The calls of a PL/pgSQL function within one transaction.
                    await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(token);
                    await using NpgsqlCommand call = new("SELECT f_s1()", connection, transaction);
                    for (int i = 0; i < 250; i++)
                    {
                        _ = await call.ExecuteScalarAsync(token);
                    }

                    await transaction.CommitAsync(token);
                    continue;
                }

                await using NpgsqlCommand draw = new(statement, connection);
                for (int i = 0; i < (statement.Contains("generate_series", StringComparison.Ordinal) ? 1 : 250); i++)
                {
                    _ = await draw.ExecuteNonQueryAsync(token);
                }
            }
            catch (PostgresException failed)
            {
                problems.Add($"{statement}: {failed.SqlState} {failed.MessageText}");
            }
        }

        // Two roles each have a sequence sx of their own in a schema of their name, which "$user" finds.
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE ROLE ua; CREATE ROLE ub;
            CREATE SCHEMA ua AUTHORIZATION ua; CREATE SCHEMA ub AUTHORIZATION ub;
            CREATE SEQUENCE ua.sx; CREATE SEQUENCE ub.sx;
            ALTER SEQUENCE ua.sx OWNER TO ua; ALTER SEQUENCE ub.sx OWNER TO ub;
            SELECT worf.enable_sequence_range('ua.sx', participants := '{}', range_size := 100, max_range_size := 100);
            SELECT worf.enable_sequence_range('ub.sx', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        foreach (string sx in new[] { "ua.sx", "ub.sx" })
        {
            Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = '{sx}'::regclass", token),
                $"node_a drawing from a block of {sx}, with the next one ready");
        }

        await using (NpgsqlConnection roles = await nodeA.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand first = new("SET search_path = \"$user\", public; SET ROLE ua; SELECT nextval('sx'); SET ROLE ub", roles))
            {
                _ = await first.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand draw = new("SELECT nextval('sx')", roles);
            try
            {
                for (int i = 0; i < 250; i++)
                {
                    _ = await draw.ExecuteScalarAsync(token);
                }
            }
            catch (PostgresException failed)
            {
                problems.Add($"SELECT nextval('sx') as ub, after ua: {failed.SqlState} {failed.MessageText}");
            }
        }

        Assert.AreEqual("", string.Join("\n", problems), "the draws that ran a block out");
    }

    /// <summary>
    /// worf's apply waits for a grantor only for the triggers that fire as it applies, which run as a replica's: after
    /// node_b restarts with node_a, its grantor, down, a row of x, whose autoinc trigger is enabled ORIGIN, and an update
    /// of z, whose ALWAYS autoinc trigger fires on insert alone, apply at once, while a row of y, whose ALWAYS trigger fires
    /// on insert, waits for node_a, and so does a row of u that conflicts with node_b's own and is applied as an update,
    /// whose ALWAYS autoinc trigger fires on update alone; and a local insert into w, whose autoinc trigger is enabled
    /// REPLICA, runs at once.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task AppliedRowsWaitOnlyForTriggersThatFire()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id bigserial PRIMARY KEY, v text);
                CREATE TABLE x (k int PRIMARY KEY, id int); CREATE TABLE y (k int PRIMARY KEY, id int); CREATE TABLE z (k int PRIMARY KEY, id int, v text);
                CREATE TABLE w (k int PRIMARY KEY, id int); CREATE TABLE u (k int PRIMARY KEY, id int, v text);
                """, token);
        }

        _ = await nodeB.SafePsqlAsync(Database, """
            CREATE EXTENSION autoinc;
            CREATE TRIGGER x_id BEFORE INSERT ON x FOR EACH ROW EXECUTE FUNCTION autoinc(id, t_id_seq);
            CREATE TRIGGER y_id BEFORE INSERT ON y FOR EACH ROW EXECUTE FUNCTION autoinc(id, t_id_seq);
            ALTER TABLE y ENABLE ALWAYS TRIGGER y_id;
            CREATE TRIGGER z_id BEFORE INSERT ON z FOR EACH ROW EXECUTE FUNCTION autoinc(id, t_id_seq);
            ALTER TABLE z ENABLE ALWAYS TRIGGER z_id;
            CREATE TRIGGER w_id BEFORE INSERT ON w FOR EACH ROW EXECUTE FUNCTION autoinc(id, t_id_seq);
            ALTER TABLE w ENABLE REPLICA TRIGGER w_id;
            CREATE TRIGGER u_id BEFORE UPDATE ON u FOR EACH ROW EXECUTE FUNCTION autoinc(id, t_id_seq);
            ALTER TABLE u ENABLE ALWAYS TRIGGER u_id;
            INSERT INTO u VALUES (1, NULL, 'node_b');
            """, token);
        _ = await nodeC.SafePsqlAsync(Database, """
            SELECT worf.replication_set_add_table('default', 'x'); SELECT worf.replication_set_add_table('default', 'y');
            SELECT worf.replication_set_add_table('default', 'z'); SELECT worf.replication_set_add_table('default', 'u');
            """, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO z VALUES (1, NULL, 'before')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 1 FROM z", token), "node_b's apply of z's row");

        // node_a stops, and node_b restarts, so that it hasn't heard from node_a since.
        await nodeA.StopAsync("fast", token);
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        foreach (string write in new[] { "INSERT INTO x VALUES (1, NULL)", "UPDATE z SET v = 'after' WHERE k = 1", "INSERT INTO u VALUES (1, NULL, 'node_c')", "INSERT INTO y VALUES (1, NULL)" })
        {
            _ = await nodeC.SafePsqlAsync(Database, write, token);
        }

        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT (SELECT count(*) FROM x) = 1 AND (SELECT v FROM z) = 'after'", token),
            "node_b's apply of x's row and z's update, while node_a is down");
        await using (NpgsqlConnection local = await nodeB.OpenAsync(Database, token))
        {
            await using NpgsqlCommand insert = new("SET statement_timeout = '5s'; INSERT INTO w VALUES (1, NULL)", local);
            _ = await insert.ExecuteNonQueryAsync(token);
        }

        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM y", token), "the rows of y node_b applied while node_a was down");
        Assert.AreEqual("node_b|t", await nodeB.SafePsqlAsync(Database, "SELECT v, id IS NULL FROM u", token), "u's row on node_b while node_a was down");
        await nodeA.StartServerAsync(token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 1 FROM y WHERE id IS NOT NULL", token), "node_b's apply of y's row once node_a is back");
        Assert.AreEqual("node_c|f", await nodeB.SafePsqlAsync(Database, "SELECT v, id IS NULL FROM u", token), "u's row on node_b once node_a is back");
    }

    /// <summary>
    /// A PL/pgSQL statement counts down only the managed sequences its function draws from: after the transaction drew
    /// s1, and another session drew s1 into its block's last twentieth, which can't switch as its owner is down, a loop
    /// that draws from a sequence no range manages, and a trigger that does, run at once, rather than wait for s1's switch,
    /// and so does a statement whose calls take that sequence's name from a column, and 100 statements that draw nothing.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DrawsWaitOnlyForTheirOwnSequences()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "worf.sequence_range_offline_wait = -1");
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE SEQUENCE s1; CREATE SEQUENCE plain;
                CREATE FUNCTION f_plain_loop(n int) RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE x bigint; BEGIN FOR i IN 1..n LOOP x := nextval('plain'); END LOOP; RETURN x; END $$;
                CREATE TABLE pt (id bigint, v text);
                CREATE FUNCTION pt_id() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.id := nextval('plain'); RETURN NEW; END $$;
                CREATE TRIGGER pt_id BEFORE INSERT ON pt FOR EACH ROW EXECUTE FUNCTION pt_id();
                CREATE TABLE plainnames (n text);
                """, token);
        }

        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO plainnames SELECT 'plain' FROM generate_series(1, 100)", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('s1', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 's1'::regclass", token),
            "node_b drawing from a block of s1, with the next one ready");

        // With node_a down, and node_b never going on without it, as worf.sequence_range_offline_wait -1 has it, a
        // transaction on node_b draws s1, and another session then draws s1 until a draw waits for a switch that can't
        // come, in the last block's last twentieth.
        await nodeA.StopAsync("fast", token);
        await using NpgsqlConnection connection = await nodeB.OpenAsync(Database, token);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(token);
        await using (NpgsqlCommand first = new("SELECT nextval('s1')", connection, transaction))
        {
            _ = await first.ExecuteNonQueryAsync(token);
        }

        await using (NpgsqlConnection other = await nodeB.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand timeout = new("SET statement_timeout = '2s'", other))
            {
                _ = await timeout.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand draw = new("SELECT nextval('s1')", other);
            for (int i = 0; ; i++)
            {
                Assert.IsLessThan(300, i, "node_b's draws of s1 before one waits");
                try
                {
                    _ = await draw.ExecuteScalarAsync(token);
                }
                catch (PostgresException waited) when (waited.SqlState == "57014")
                {
                    break;
                }
            }
        }

        // The transaction then runs the loop and the trigger, which draw from plain alone, a statement whose calls take
        // plain's name from a column, and statements that draw nothing.
        foreach (string statement in new[] { "SELECT f_plain_loop(1000)", "INSERT INTO pt (v) SELECT 'x' FROM generate_series(1, 1000)", "SELECT nextval(n) FROM plainnames" })
        {
            await using NpgsqlCommand run = new($"SET LOCAL statement_timeout = '5s'; {statement}", connection, transaction);
            Stopwatch running = Stopwatch.StartNew();
            _ = await run.ExecuteNonQueryAsync(token);
            Assert.IsLessThan(TimeSpan.FromSeconds(3), running.Elapsed, statement);
        }

        await using (NpgsqlCommand nothing = new("SET LOCAL statement_timeout = '5s'", connection, transaction))
        {
            _ = await nothing.ExecuteNonQueryAsync(token);
        }

        Stopwatch selecting = Stopwatch.StartNew();
        for (int i = 0; i < 100; i++)
        {
            await using NpgsqlCommand one = new("SELECT 1", connection, transaction);
            _ = await one.ExecuteScalarAsync(token);
        }

        Assert.IsLessThan(TimeSpan.FromSeconds(3), selecting.Elapsed, "100 statements of SELECT 1 in the transaction");

        await transaction.CommitAsync(token);
    }

    /// <summary>
    /// A statement run again in a transaction that drew a managed sequence takes about as long as in one that drew
    /// nothing, each in a session of its own that has run nothing else: its calls of nextval are read once for its text,
    /// rather than from its plan's text each time it starts, and so are a statement's with parameters, as a driver sends
    /// its values, which makes none. The two sessions' inserts of 4,000 literal rows, and of 1,000 rows of parameters,
    /// alternate, the first 20 of each warm up, and of the 40 that follow, each session's fastest 30 count, as a busy
    /// runner stalls a statement now and then. The test runs alone, as other tests' load hides the difference.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [DoNotParallelize]
    public async Task StatementsAfterADrawCostWhatTheyDidBefore()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1; CREATE TABLE vt (x int);
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 's1'::regclass", token),
            "node_a drawing from a block of s1");
        string values = $"INSERT INTO vt VALUES {string.Join(", ", Enumerable.Range(1, 4000).Select(i => string.Create(CultureInfo.InvariantCulture, $"({i})")))}";
        string parameters = $"INSERT INTO vt VALUES {string.Join(", ", Enumerable.Range(1, 1000).Select(i => string.Create(CultureInfo.InvariantCulture, $"(${i})")))}";
        await using NpgsqlConnection drawingConnection = await nodeA.OpenAsync(Database, token);
        await using NpgsqlConnection plainConnection = await nodeA.OpenAsync(Database, token);
        await using NpgsqlTransaction drawingTransaction = await drawingConnection.BeginTransactionAsync(token);
        await using NpgsqlTransaction plainTransaction = await plainConnection.BeginTransactionAsync(token);
        await using (NpgsqlCommand drawOnce = new("SELECT nextval('s1')", drawingConnection, drawingTransaction))
        {
            _ = await drawOnce.ExecuteNonQueryAsync(token);
        }

        foreach (string sql in new[] { values, parameters })
        {
            NpgsqlCommand Insert(NpgsqlConnection inserting, NpgsqlTransaction within)
            {
                NpgsqlCommand insert = new(sql, inserting, within);
                for (int i = 1; sql == parameters && i <= 1000; i++)
                {
                    _ = insert.Parameters.Add(new NpgsqlParameter { Value = i });
                }

                return insert;
            }

            await using NpgsqlCommand drawingInsert = Insert(drawingConnection, drawingTransaction);
            await using NpgsqlCommand plainInsert = Insert(plainConnection, plainTransaction);
            List<TimeSpan> drawingTimes = [];
            List<TimeSpan> plainTimes = [];
            for (int i = 0; i < 60; i++)
            {
                foreach ((NpgsqlCommand insert, bool drew) in i % 2 == 0 ? new[] { (drawingInsert, true), (plainInsert, false) } : [(plainInsert, false), (drawingInsert, true)])
                {
                    Stopwatch timing = Stopwatch.StartNew();
                    _ = await insert.ExecuteNonQueryAsync(token);
                    if (i >= 20)
                    {
                        (drew ? drawingTimes : plainTimes).Add(timing.Elapsed);
                    }
                }
            }

            static TimeSpan Fastest(List<TimeSpan> times) => TimeSpan.FromTicks(times.Order().Take(30).Sum(time => time.Ticks));
            TimeSpan drawing = Fastest(drawingTimes);
            TimeSpan plainly = Fastest(plainTimes);
            string what = sql == values ? "4,000 literal rows" : "1,000 rows of parameters";
            Assert.IsLessThan(plainly * 1.2, drawing, $"the fastest 30 of 40 inserts of {what} in a transaction that drew s1, against {plainly} in one that didn't; " +
                $"the slowest took {drawingTimes.Max()} and {plainTimes.Max()}");
        }

        await drawingTransaction.RollbackAsync(token);
        await plainTransaction.RollbackAsync(token);
    }

    /// <summary>
    /// A node cut off from the allocator takes blocks from its reserve that fit in it: a smallserial's reserve, which
    /// holds fewer values than its largest block, still gives node_b blocks while node_a is down (2151).
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SmallReservesServeOfflineNodes()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE ts (id smallserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('ts_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 1000)", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'ts_id_seq'::regclass", token),
                $"{node.Name} drawing from ts_id_seq");
        }

        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_block WHERE seqoid = 'ts_id_seq'::regclass AND kind = 'p' AND state = 'open')", token),
            "node_b's reserve");
        await nodeA.StopAsync("fast", token);
        int took = nodeB.ReadLog().Length;
        int inserted = 0;
        while (await nodeB.PsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO ts (v) SELECT 'b' FROM generate_series(1, 10)", token))
        {
            inserted += 10;
            Assert.IsLessThan(2000, inserted, "node_b's inserts");
        }

        Assert.Contains("from this node's reserve", nodeB.ReadLog()[took..], "node_b's blocks from its smallserial reserve");
        Assert.IsGreaterThan(300, inserted, "node_b's rows past its held blocks");
    }

    /// <summary>
    /// A reservation that times out while node_b is cut off leaves its reserve to its other inserts: the request it made
    /// goes with the session, and a load that doesn't fit the reserve doesn't keep plain blocks from coming out of it,
    /// so node_b inserts 4,000 rows from its reserve after the reservation of 20,000 timed out (2152).
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task TimedOutReservationsLeaveTheReserve()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_block WHERE seqoid = 't_id_seq'::regclass AND kind = 'p' AND state = 'open')", token),
            "node_b's reserve");
        await nodeA.StopAsync("fast", token);
        PostgresException timedOut = await Assert.ThrowsExactlyAsync<PostgresException>(() =>
            nodeB.SafePsqlAsync(Database, "BEGIN; SELECT worf.reserve_sequence_range('t_id_seq', 20000, '5 seconds')", token));
        Assert.AreEqual("57014", timedOut.SqlState, "node_b's reservation while node_a is down");
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT NOT EXISTS (SELECT 1 FROM worf.sequence_range_request WHERE seqoid = 't_id_seq'::regclass AND reserved AND state = 'open')", token),
            "the timed-out reservation's request withdrawn");
        _ = await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '90s'; INSERT INTO t (v) SELECT 'b' FROM generate_series(1, 4000)", token);
        Assert.AreEqual("4000", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM t", token), "node_b's rows from its reserve");
    }

    /// <summary>
    /// Once node_b's reserve has no free values left while node_a is down, statements at the end of its last block
    /// stop waiting for a block that can't come and fail at once with 2200H, and worf.sequence_ranges says why; once
    /// node_a is back, node_b draws again (2154).
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SpentReservesStopTheWait()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_block WHERE seqoid = 't_id_seq'::regclass AND kind = 'p' AND state = 'open')", token),
            "node_b's reserve");
        await nodeA.StopAsync("fast", token);
        while (await nodeB.PsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO t (v) SELECT 'b' FROM generate_series(1, 10)", token))
        {
            Assert.IsLessThan(3000L, long.Parse(await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM t", token), CultureInfo.InvariantCulture),
                "node_b's rows before its reserve is spent");
        }

        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT detail LIKE 'no next block can come%' FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token), "node_b's stranded sequence");
        for (int i = 0; i < 3; i++)
        {
            Stopwatch failing = Stopwatch.StartNew();
            PostgresException spent = await Assert.ThrowsExactlyAsync<PostgresException>(() =>
                nodeB.SafePsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO t (v) VALUES ('spent')", token));
            Assert.AreEqual("2200H", spent.SqlState, spent.MessageText);
            Assert.IsLessThan(TimeSpan.FromSeconds(3), failing.Elapsed, "a statement at the end of node_b's last block");
        }

        await nodeA.StartServerAsync(token);
        Stopwatch back = Stopwatch.StartNew();
        while (!await nodeB.PsqlAsync(Database, "SET statement_timeout = '10s'; INSERT INTO t (v) VALUES ('back')", token))
        {
            Assert.IsLessThan(TestNode.PollTimeout, back.Elapsed, "node_b drawing once node_a is back");
            await Task.Delay(TimeSpan.FromMilliseconds(500), token);
        }
    }

    /// <summary>
    /// An allocator cut off from its participant, whose connection hangs, waits worf.sequence_range_offline_wait once for
    /// the participant to hear of a block it granted itself, and then switches to its own blocks without waiting again
    /// until the participant answers: of node_a's fifty 100-row inserts, at most one waits that long (2153).
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task CutOffAllocatorsWaitOnce()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await using ProviderRelay relay = new(nodeA.Cluster.Port);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token,
            providerDsn: $"host=127.0.0.1 port={relay.Port.ToString(CultureInfo.InvariantCulture)} dbname={Database} user={SuperUser}");
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        relay.Hang();
        int slow = 0;
        await using NpgsqlConnection writer = await nodeA.OpenAsync(Database, token);
        for (int i = 0; i < 50; i++)
        {
            Stopwatch inserting = Stopwatch.StartNew();
            await using NpgsqlCommand insert = new("SET statement_timeout = '60s'; INSERT INTO t (v) SELECT 'a' FROM generate_series(1, 100)", writer);
            _ = await insert.ExecuteNonQueryAsync(token);
            slow += inserting.Elapsed > TimeSpan.FromSeconds(8) ? 1 : 0;
        }

        Assert.IsLessThanOrEqualTo(1, slow, "node_a's inserts that waited for node_b to hear of its block");
        relay.Pass();
    }

    /// <summary>
    /// A node cut off from the allocator for over a minute can't start a range of its own without force: it hasn't heard
    /// the allocator disable the sequence, and the allocator may still be handing out its values (2150).
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task CutOffNodesEnableOnlyWithForce()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await using ProviderRelay relay = new(nodeA.Cluster.Port);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token,
            providerDsn: $"host=127.0.0.1 port={relay.Port.ToString(CultureInfo.InvariantCulture)} dbname={Database} user={SuperUser}");
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        relay.Hang();
        await Task.Delay(TimeSpan.FromSeconds(70), token);
        PostgresException refused = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{}', range_size := 100, max_range_size := 1000)", token));
        Assert.AreEqual("55006", refused.SqlState, refused.MessageText);
        Assert.Contains("node node_a", refused.MessageText, "the refusal names the allocator");
        relay.Pass();
    }

    /// <summary>
    /// Four sessions that load after one reservation stay in the reserved block: the guard counts a plain row's draws
    /// from its text rather than from how far other sessions moved the sequence meanwhile, so none waits for a switch,
    /// the block isn't switched off with values left, and the load doesn't drive up the peak rate (2102).
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SessionsLoadingAfterOneReservationStayInItsBlock()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE TABLE t (id bigserial PRIMARY KEY, v text);
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{}', range_size := 1000, max_range_size := 1000);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges", token), "node_a drawing");
        for (int round = 0; round < 10; round++)
        {
            _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_range('t_id_seq', 10000)", token);
            string[] block = (await nodeA.SafePsqlAsync(Database, "SELECT active_lo, active_hi FROM worf.sequence_ranges", token)).Split('|');
            await Task.WhenAll(Enumerable.Range(0, 4).Select(session =>
                nodeA.SafePsqlAsync(Database, $"INSERT INTO t (v) SELECT 'r{round}' FROM generate_series(1, 2500)", token)));
            Assert.AreEqual("0", await nodeA.SafePsqlAsync(Database,
                $"SELECT count(*) FROM t WHERE v = 'r{round}' AND id NOT BETWEEN {block[0]} AND {block[1]}", token),
                $"round {round}'s rows outside its reserved block [{block[0]}, {block[1]}]");
        }
    }

    /// <summary>
    /// worf's drop beside inserts that wait for a block, holding the locks on worf's tables they took as they drew,
    /// finishes long before the inserts' 30 s timeout, in seconds on a slow runner: the waits see the drop waiting for
    /// those tables and fail, rather than hold it up until they time out (2120).
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DropsBesideWaitingInsertsFinishAtOnce()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 10, max_range_size := 10)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        using CancellationTokenSource stopping = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task[] loops = [.. Enumerable.Range(0, 4).Select(loop => Task.Run(async () =>
        {
            while (!stopping.IsCancellationRequested)
            {
                _ = await nodeB.PsqlAsync(Database, "SET statement_timeout = '30s'; INSERT INTO t DEFAULT VALUES", CancellationToken.None);
            }
        }, CancellationToken.None))];
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        Stopwatch dropping = Stopwatch.StartNew();
        _ = await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '60s'; DROP EXTENSION worf CASCADE", token);
        Assert.IsLessThan(TimeSpan.FromSeconds(15), dropping.Elapsed, "the drop beside the waiting inserts");
        await stopping.CancelAsync();
        await Task.WhenAll(loops);
    }

    /// <summary>
    /// A session that idles in a transaction that drew from a managed sequence holds up none of the sequence's switches
    /// (2200): with a cursor that fetched some of its rows, one only declared, or a DO block's assignment behind it, another
    /// session's 300 inserts, which switch blocks of 100 twice and more, finish in seconds, rather than wait at the
    /// switch until they time out; the cursor then fetches on, and no value is drawn twice.
    /// </summary>
    /// <param name="idle">What the idle session ran.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("fetched cursor")]
    [DataRow("declared cursor")]
    [DataRow("do block")]
    public async Task IdleTransactionsThatDrewHoldUpNoSwitch(string idle)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1;
            CREATE TABLE t1 (id bigint DEFAULT nextval('s1'), v text);
            CREATE TABLE drawn (id bigint);
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges", token),
            "node_a drawing from a block of s1, with the next one ready");

        await using NpgsqlConnection idling = await nodeA.OpenAsync(Database, token);
        await ExecuteAsync(idling, "BEGIN", token);
        await ExecuteAsync(idling, idle switch
        {
            "do block" => "DO $$ DECLARE x bigint; BEGIN x := nextval('s1'); END $$",
            _ => "DECLARE c CURSOR FOR SELECT nextval('s1') FROM generate_series(1, 1000)",
        }, token);
        async Task<List<long>> FetchAsync(int count)
        {
            await using NpgsqlCommand fetch = new($"FETCH {count} FROM c", idling);
            await using NpgsqlDataReader rows = await fetch.ExecuteReaderAsync(token);
            List<long> ids = [];
            while (await rows.ReadAsync(token))
            {
                ids.Add(rows.GetInt64(0));
            }

            return ids;
        }

        List<long> fetched = idle == "fetched cursor" ? await FetchAsync(5) : [];
        Assert.HasCount(idle == "fetched cursor" ? 5 : 0, fetched, "the cursor's first rows");

        Stopwatch inserting = Stopwatch.StartNew();
        _ = await nodeA.SafePsqlAsync(Database, "SET statement_timeout = '20s'; INSERT INTO t1 (v) SELECT 'x' FROM generate_series(1, 300)", token);
        Assert.IsLessThan(TimeSpan.FromSeconds(10), inserting.Elapsed, $"the inserts beside the idle transaction's {idle}");

        // The idle session goes on drawing, from the block the sequence has switched to since.
        if (idle == "do block")
        {
            await ExecuteAsync(idling, "INSERT INTO drawn SELECT currval('s1')", token);
        }
        else
        {
            List<long> later = await FetchAsync(150);
            Assert.HasCount(150, later, "the cursor's later rows");
            fetched.AddRange(later);
            await ExecuteAsync(idling, $"INSERT INTO drawn SELECT unnest(ARRAY[{string.Join(',', fetched)}]::bigint[])", token);
        }

        await ExecuteAsync(idling, "COMMIT", token);
        Assert.AreEqual("0", await nodeA.SafePsqlAsync(Database, "SELECT count(*) FROM drawn d JOIN t1 USING (id)", token),
            "the values both the idle session and the inserts drew");
        Assert.AreEqual("300", await nodeA.SafePsqlAsync(Database, "SELECT count(DISTINCT id) FROM t1", token), "the inserts' ids");
    }

    /// <summary>
    /// A statement of one row that draws holds no values back from other sessions: it counts no draws for later rows, which
    /// it hasn't, so while one runs on, sleeping in its row, another session draws 85 of the block's 100 values without
    /// waiting, all from that block, rather than find it short sooner and wait for a switch that the first holds up.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task OneRowStatementsHoldNoValuesBack()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1;
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges", token),
            "node_a drawing from a block of s1, with the next one ready");
        string[] block = (await nodeA.SafePsqlAsync(Database, "SELECT active_lo || ' ' || active_hi FROM worf.sequence_ranges", token)).Split(' ');
        _ = await nodeA.SafePsqlAsync(Database, "SELECT nextval('s1')", token);
        Task<string> sleeping = nodeA.SafePsqlAsync(Database, "SELECT nextval('s1'), pg_sleep(4)", token);
        await Task.Delay(TimeSpan.FromMilliseconds(500), token);
        await using NpgsqlConnection drawing = await nodeA.OpenAsync(Database, token);
        await using NpgsqlCommand draw = new("SELECT nextval('s1')", drawing);
        Stopwatch timing = Stopwatch.StartNew();
        List<long> drawn = [];
        for (int i = 0; i < 85; i++)
        {
            drawn.Add((long)(await draw.ExecuteScalarAsync(token))!);
        }

        Assert.IsLessThan(TimeSpan.FromSeconds(3), timing.Elapsed, "the 85 draws beside the statement that sleeps in its row");
        Assert.IsLessThanOrEqualTo(long.Parse(block[1], CultureInfo.InvariantCulture), drawn.Max(), $"the draws' values, all from the block [{block[0]}, {block[1]}]");
        _ = await sleeping;
    }

    /// <summary>
    /// A row that draws keeps its sequence's switch gate once a statement its function ran has ended, which had the gate
    /// as its own while it drew from the sequence too: the row, which sleeps after its function's insert and draws after
    /// that, holds the gate as it sleeps, so that a switch beside it moves the sequence only once the row has drawn, and
    /// holds none once it has.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RowsKeepTheirGatesPastTheStatementsTheyRun()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1;
            CREATE TABLE t1 (id bigint DEFAULT nextval('s1'), v text);
            CREATE FUNCTION ins() RETURNS bigint LANGUAGE sql AS $$ INSERT INTO t1 (v) VALUES ('in') RETURNING id $$;
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges", token),
            "node_a drawing from a block of s1, with the next one ready");
        await using NpgsqlConnection drawing = await nodeA.OpenAsync(Database, token);
        string gates = $"""
            SELECT count(*) FROM pg_locks
            WHERE pid = {drawing.ProcessID} AND locktype = 'advisory' AND classid = 's1'::regclass::oid AND objsubid = 3
                AND mode = 'ShareLock' AND granted
            """;
        await using NpgsqlCommand draw = new("SELECT ins() || ' ' || pg_sleep(3)::text || nextval('s1')", drawing);
        Task<object?> row = draw.ExecuteScalarAsync(token);
        string held = "";
        for (int i = 0; i < 20 && held != "t"; i++)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            held = await nodeA.SafePsqlAsync(Database,
                $"SELECT ({gates}) = 1 FROM pg_stat_activity WHERE pid = {drawing.ProcessID} AND wait_event = 'PgSleep'", token);
        }

        string[] ids = ((string)(await row)!).Split(' ');
        Assert.AreEqual("t", held, "whether the row holds the gate of s1 as it sleeps after its function's insert");
        Assert.AreEqual(long.Parse(ids[0], CultureInfo.InvariantCulture) + 1, long.Parse(ids[1], CultureInfo.InvariantCulture),
            "the row's own draw, next after its function's insert's");
        Assert.AreEqual("0", await nodeA.SafePsqlAsync(Database, gates, token), "the gates of s1 the session holds once the row is out");
    }

    /// <summary>
    /// A statement whose rows that draw feed the rest of its plan, or a table worf doesn't guard, keeps its sequence's
    /// gate from row to row and lets a waiting switch pass at each (2300): beside one whose rows take half a second each,
    /// under an aggregate, in CREATE TABLE AS, or inserted into a table without a managed default, another session's 300
    /// inserts, which switch blocks of 100, wait a row at each switch, where they waited eight, 4 s each. So does a
    /// PL/pgSQL loop of one assignment that draws, which keeps the gate from turn to turn (2301), at each turn.
    /// </summary>
    /// <param name="form">What the rows that draw feed, or the loop.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("aggregate")]
    [DataRow("create table as")]
    [DataRow("insert into a plain table")]
    [DataRow("plpgsql loop")]
    public async Task StatementsThatKeepAGateLetSwitchesPassAtEachRow(string form)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1;
            CREATE TABLE t1 (id bigint DEFAULT nextval('s1'), v text);
            CREATE TABLE plain_t (id bigint, v text);
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges", token),
            "node_a drawing from a block of s1, with the next one ready");
        await using NpgsqlConnection slow = await nodeA.OpenAsync(Database, token);
        await using NpgsqlCommand draw = new(form switch
        {
            "aggregate" => "SELECT count(*) FROM (SELECT nextval('s1'), pg_sleep(0.5) FROM generate_series(1, 40) OFFSET 0) q",
            "create table as" => "CREATE TABLE made AS SELECT nextval('s1') AS id, pg_sleep(0.5)::text AS v FROM generate_series(1, 40)",
            "plpgsql loop" => "DO $$ DECLARE x bigint; BEGIN FOR i IN 1..40 LOOP x := nextval('s1') + length(pg_sleep(0.5)::text); END LOOP; END $$",
            _ => "INSERT INTO plain_t (id, v) SELECT nextval('s1'), pg_sleep(0.5)::text FROM generate_series(1, 40)",
        }, slow) { CommandTimeout = 120 };
        Task<object?> drawing = draw.ExecuteScalarAsync(token);
        await Task.Delay(TimeSpan.FromMilliseconds(700), token);
        Stopwatch inserting = Stopwatch.StartNew();
        string failed = "";
        try
        {
            _ = await nodeA.SafePsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO t1 (v) SELECT 'x' FROM generate_series(1, 300)", token);
        }
        catch (PostgresException failure)
        {
            failed = $"{failure.SqlState} {failure.MessageText}";
        }

        TimeSpan took = inserting.Elapsed;
        TestContext.WriteLine($"the inserts took {took.TotalSeconds:F1} s");

        // The slow statement has shown what it had to, and is canceled rather than waited for.
        _ = await nodeA.SafePsqlAsync(Database, $"SELECT pg_cancel_backend({slow.ProcessID})", token);
        string ended = "";
        try
        {
            _ = await drawing;
        }
        catch (PostgresException canceled) when (canceled.SqlState == "57014")
        {
            ended = canceled.SqlState;
        }

        Assert.AreEqual("", failed, $"the inserts beside the {form} that draws");
        Assert.IsLessThan(TimeSpan.FromSeconds(10), took, $"the inserts beside the {form} that draws, half a second a row");
        Assert.AreEqual("57014", ended, $"the {form}, still drawing as the inserts finished, and canceled");
        Assert.AreEqual("300", await nodeA.SafePsqlAsync(Database, "SELECT count(DISTINCT id) FROM t1", token), "the inserts' ids");
    }

    /// <summary>
    /// A PL/pgSQL loop that draws from a managed sequence at every turn takes about as long as one that draws from a plain
    /// sequence (2301): its assignment keeps the sequence's gate from one turn to the next, and PL/pgSQL calls worf once a
    /// turn, as it does for any statement, where it called twice and took the gate each time. Loops of 100,000 draws
    /// alternate, the first two of each warm up, and each one's fastest 8 of the 12 that follow count. The test runs alone,
    /// as other tests' load hides the difference.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [DoNotParallelize]
    public async Task PlpgsqlLoopsThatDrawCostWhatPlainOnesDo()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1; CREATE SEQUENCE plain_s;
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100000000);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 's1'::regclass", token),
            "node_a drawing from a block of s1");
        await using NpgsqlConnection connection = await nodeA.OpenAsync(Database, token);
        Dictionary<string, List<TimeSpan>> times = new() { ["s1"] = [], ["plain_s"] = [] };
        for (int i = 0; i < 14; i++)
        {
            foreach (string sequence in i % 2 == 0 ? new[] { "s1", "plain_s" } : ["plain_s", "s1"])
            {
                await using NpgsqlCommand loop = new(
                    $"DO $$ DECLARE x bigint; BEGIN FOR i IN 1..100000 LOOP x := nextval('{sequence}'); END LOOP; END $$", connection) { CommandTimeout = 120 };
                Stopwatch timing = Stopwatch.StartNew();
                _ = await loop.ExecuteNonQueryAsync(token);
                if (i >= 2)
                {
                    times[sequence].Add(timing.Elapsed);
                }
            }
        }

        static TimeSpan Fastest(List<TimeSpan> times) => TimeSpan.FromTicks(times.Order().Take(8).Sum(time => time.Ticks));
        TimeSpan managed = Fastest(times["s1"]);
        TimeSpan plain = Fastest(times["plain_s"]);
        Assert.IsLessThan(plain * 1.5, managed, $"the fastest 8 of 12 loops of 100,000 draws from a managed sequence, against {plain} from a plain one");
    }

    /// <summary>
    /// A PL/pgSQL loop keeps its sequence's gate only while it draws at every turn (2301): a function that sleeps in an
    /// assignment after such a loop holds no gate as it sleeps, nor does the row that called one whose last statements
    /// all draw, as the row sleeps, nor the transaction once the statement has ended, so that none holds up a switch.
    /// </summary>
    /// <param name="after">What follows the loop.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("an assignment that sleeps")]
    [DataRow("the function's end")]
    public async Task PlpgsqlLoopsKeepTheirGateOnlyWhileTheyDraw(string after)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1;
            CREATE FUNCTION draws_then_sleeps() RETURNS void LANGUAGE plpgsql AS $$
            DECLARE x bigint; y text;
            BEGIN
                FOR i IN 1..20 LOOP x := nextval('s1'); END LOOP;
                y := pg_sleep(3)::text;
            END $$;
            CREATE FUNCTION draws() RETURNS bigint LANGUAGE plpgsql AS $$
            DECLARE x bigint;
            BEGIN
                FOR i IN 1..20 LOOP x := nextval('s1'); END LOOP;
                RETURN nextval('s1');
            END $$;
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges", token),
            "node_a drawing from a block of s1, with the next one ready");
        await using NpgsqlConnection drawing = await nodeA.OpenAsync(Database, token);
        string gates = $"""
            SELECT count(*) FROM pg_locks
            WHERE pid = {drawing.ProcessID} AND locktype = 'advisory' AND classid = 's1'::regclass::oid AND objsubid = 3
            """;
        await ExecuteAsync(drawing, "BEGIN", token);
        string held = "";
        await using (NpgsqlCommand draw = new(after == "an assignment that sleeps" ? "SELECT draws_then_sleeps()" : "SELECT draws()::text || pg_sleep(3)::text", drawing))
        {
            Task<object?> call = draw.ExecuteScalarAsync(token);
            for (int i = 0; i < 20 && held.Length == 0; i++)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
                held = await nodeA.SafePsqlAsync(Database,
                    $"SELECT ({gates}) FROM pg_stat_activity WHERE pid = {drawing.ProcessID} AND wait_event = 'PgSleep'", token);
            }

            _ = await call;
        }

        string idle = await nodeA.SafePsqlAsync(Database, gates, token);
        long drawn;
        await using (NpgsqlCommand last = new("SELECT currval('s1')", drawing))
        {
            drawn = (long)(await last.ExecuteScalarAsync(token))!;
        }

        await ExecuteAsync(drawing, "COMMIT", token);
        Assert.AreEqual("0", held, $"the gates of s1 the session holds after its loop, at {after}");
        Assert.AreEqual("0", idle, "the gates of s1 the session holds once the statement has ended");
        Assert.IsGreaterThanOrEqualTo(20L, drawn, "the loop's last draw");
    }

    /// <summary>
    /// A PL/pgSQL loop that draws passes the switches other sessions' inserts need, though its transaction's portals and
    /// subtransactions take and let go of the sequence's gate under resource owners of their own (2201): a FOR loop over a
    /// query that draws, sleeping between its rows, and an insert whose trigger runs another insert in a block with an
    /// exception handler, each beside 300 inserts of another session, both finish, with no deadlock and no lock let go
    /// under the wrong owner.
    /// </summary>
    /// <param name="form">What draws beside the other session's inserts.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("loop over a query")]
    [DataRow("insert in a subtransaction")]
    public async Task LoopsThatDrawPassTheSwitchesOthersNeed(string form)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1; CREATE SEQUENCE s2;
            CREATE TABLE t1 (id bigint DEFAULT nextval('s1'), v text);
            CREATE TABLE t2 (id bigint DEFAULT nextval('s2'), v text);
            CREATE TABLE slow_t (id bigint DEFAULT nextval('s1'), v text);
            CREATE FUNCTION slow() RETURNS void LANGUAGE plpgsql AS $$
            DECLARE r record;
            BEGIN
                FOR r IN SELECT nextval('s1') FROM generate_series(1, 40) LOOP PERFORM pg_sleep(0.1); END LOOP;
            END $$;
            CREATE FUNCTION slow_row() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM pg_sleep(0.1);
                BEGIN
                    INSERT INTO t2 (v) VALUES ('in');
                EXCEPTION WHEN division_by_zero THEN NULL;
                END;
                RETURN NEW;
            END $$;
            CREATE TRIGGER slow_row BEFORE INSERT ON slow_t FOR EACH ROW EXECUTE FUNCTION slow_row();
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100, max_range_size := 100);
            SELECT worf.enable_sequence_range('s2', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT bool_and(state = 'active' AND next_lo IS NOT NULL) FROM worf.sequence_ranges", token),
            "node_a drawing from blocks of s1 and s2, with the next ones ready");
        int logpos = nodeA.ReadLog().Length;
        Task<string> looping = nodeA.SafePsqlAsync(Database, form == "loop over a query"
            ? "SET statement_timeout = '60s'; SELECT slow()"
            : "SET statement_timeout = '60s'; INSERT INTO slow_t (v) SELECT 'slow' FROM generate_series(1, 40)", token);
        await Task.Delay(TimeSpan.FromMilliseconds(500), token);
        Stopwatch inserting = Stopwatch.StartNew();
        _ = await nodeA.SafePsqlAsync(Database, "SET statement_timeout = '20s'; INSERT INTO t1 (v) SELECT 'x' FROM generate_series(1, 300)", token);
        Assert.IsLessThan(TimeSpan.FromSeconds(10), inserting.Elapsed, $"the inserts beside the {form}");
        _ = await looping;
        string log = nodeA.ReadLog()[logpos..];
        Assert.DoesNotContain("you don't own a lock", log, "node_a's log of locks let go under another owner");
        Assert.DoesNotContain("deadlock detected", log, "node_a's log of deadlocks");
        Assert.AreEqual("300", await nodeA.SafePsqlAsync(Database, "SELECT count(DISTINCT id) FROM t1", token), "the inserts' ids");
    }

    /// <summary>
    /// Dropping worf widens a managed sequence whose values no other node was given to the end of its range, with or
    /// without CASCADE, so it goes on past its block (2021).
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DroppingWorfFreesUnsharedSequences()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1;
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges", token), "node_a drawing");
        _ = await nodeA.SafePsqlAsync(Database, "SELECT nextval('s1') FROM generate_series(1, 10)", token);
        int logpos = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "DROP EXTENSION worf", token);
        Assert.Contains("sequence public.s1 goes on to", nodeA.ReadLog()[logpos..], "the drop's word on s1");
        Assert.AreEqual("500", await nodeA.SafePsqlAsync(Database, "SELECT count(nextval('s1')) FROM generate_series(1, 500)", token), "s1's draws past its block");
    }

    /// <summary>
    /// A session's first draw reads the blocks with JIT off, as worf's tables have no statistics until they're analyzed
    /// and the read's estimated cost would have JIT compile it (2142, 2202). auto_explain lists a setting with a statement
    /// only where it differs from the server's built-in value, so in a session with jit on, the read is logged with
    /// jit off where jit is on by default, and without jit on where it's off by default.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task BlockReadsRunWithoutJit()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1;
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges", token), "node_a drawing");
        string builtIn = await nodeA.SafePsqlAsync(Database, "SELECT boot_val FROM pg_settings WHERE name = 'jit'", token);
        int logpos = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, """
            LOAD 'auto_explain';
            SET auto_explain.log_min_duration = 0;
            SET auto_explain.log_nested_statements = on;
            SET auto_explain.log_settings = on;
            SET jit = on;
            SELECT nextval('s1');
            """, token);
        string log = nodeA.ReadLog()[logpos..];
        int read = log.IndexOf("LEFT JOIN worf.sequence_range_block b ON b.seqoid = r.seqoid AND b.kind IN ('n', 'i')", StringComparison.Ordinal);
        Assert.IsGreaterThanOrEqualTo(0, read, "the logged read of the blocks");
        int settings = log.IndexOf("Settings:", read, StringComparison.Ordinal);
        Assert.IsGreaterThanOrEqualTo(0, settings, "the read's settings");
        string listed = log[settings..log.IndexOf('\n', settings)];
        if (builtIn == "on")
        {
            Assert.Contains("jit = 'off'", listed, "the read's JIT, where it's on by default");
        }
        else
        {
            Assert.DoesNotContain("jit = 'on'", listed, "the read's JIT, where it's off by default");
        }
    }

    /// <summary>
    /// At the shipped defaults, a node that restarted draws nothing another node holds: a participant restored from a
    /// backup, whose rows from blocks it took from its reserve after the backup node_a holds, waits for node_a, down,
    /// rather than take those blocks again (2110, 2112); an allocator restored from a backup, whose rows since node_b
    /// holds, waits for node_b, down (2113); and a participant that only restarted, whose blocks the restored allocator
    /// no longer records, and the restored allocator, wait for each other (2114). Each wait fails with 55000 naming the
    /// node waited for, and once both nodes are up, no id is on both as different rows.
    /// </summary>
    /// <param name="how">Which node goes back or restarts.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("participant restored")]
    [DataRow("allocator restored")]
    [DataRow("participant restarted")]
    public async Task RestartedNodesDrawNothingAnotherNodeHolds(string how)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        TestNode[] nodes = [nodeA, nodeB];
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id bigserial PRIMARY KEY, v text);
                SELECT * FROM worf.replication_set_add_table('default', 't');
                """, token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 1000)", token);
        await WaitActiveAsync(nodes, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_block WHERE seqoid = 't_id_seq'::regclass AND kind = 'p' AND state = 'open')", token),
            "node_b's reserve");

        using TemporaryDirectory copies = new($"worf-sequence-range-restarts-{Guid.NewGuid():N}");
        string copy = Path.Join(copies.FullName, "copy");
        async Task SeenAsync(TestNode node, string value, int count)
            => Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"SELECT count(*) = {count} FROM t WHERE v = '{value}'", token),
                $"{node.Name} holding the {count} '{value}' rows");
        async Task WaitsAsync(TestNode node, string waitedFor)
        {
            PostgresException waited = await Assert.ThrowsExactlyAsync<PostgresException>(() =>
                node.SafePsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO t (v) SELECT 'waiting' FROM generate_series(1, 20)", token),
                $"{node.Name}'s insert while {waitedFor} is down");
            Assert.AreEqual("55000", waited.SqlState, waited.MessageText);
            Assert.Contains($"node {waitedFor}", waited.MessageText, $"{node.Name}'s wait");
        }

        if (how == "participant restored")
        {
            // node_b is backed up, and with node_a down takes blocks from its reserve for 400 rows, which node_a applies
            // once it's back. node_a stops again, and node_b is restored.
            _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'before' FROM generate_series(1, 20)", token);
            await BackUpDataAsync(nodeB, copy, token, wal: true);
            await nodeA.StopAsync("fast", token);
            int took = nodeB.ReadLog().Length;
            _ = await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO t (v) SELECT 'after backup' FROM generate_series(1, 400)", token);
            Assert.Contains("from this node's reserve", nodeB.ReadLog()[took..], "node_b's blocks from its reserve");
            await nodeA.StartServerAsync(token);
            await SeenAsync(nodeA, "after backup", 400);
            await nodeA.StopAsync("fast", token);
            await RestoreDataAsync(nodeB, copy, token);
            await WaitsAsync(nodeB, "node_a");
            await nodeA.StartServerAsync(token);
        }
        else if (how == "allocator restored")
        {
            // node_a is backed up and draws 20 more, which node_b applies and hears of. node_b stops, and node_a is
            // restored.
            await BackUpDataAsync(nodeA, copy, token, wal: true);
            _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'after backup' FROM generate_series(1, 20)", token);
            await SeenAsync(nodeB, "after backup", 20);
            string drawn = await nodeA.SafePsqlAsync(Database, "SELECT max(id) FROM t WHERE v = 'after backup'", token);
            Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
                $"SELECT drawn >= {drawn} FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND node_name = 'node_a'", token),
                "node_b hearing of node_a's draws");
            await nodeB.StopAsync("fast", token);
            await RestoreDataAsync(nodeA, copy, token);
            await WaitsAsync(nodeA, "node_b");
            await nodeB.StartServerAsync(token);
        }
        else
        {
            // node_a is backed up, node_b draws 240 from blocks node_a granted since and stops, node_a is restored, and
            // both wait for the other.
            await BackUpDataAsync(nodeA, copy, token, wal: true);
            _ = await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO t (v) SELECT 'after backup' FROM generate_series(1, 240)", token);
            await SeenAsync(nodeA, "after backup", 240);
            await nodeB.StopAsync("fast", token);
            await RestoreDataAsync(nodeA, copy, token);
            await WaitsAsync(nodeA, "node_b");
            await nodeA.StopAsync("fast", token);
            await nodeB.StartServerAsync(token);
            await WaitsAsync(nodeB, "node_a");
            await nodeA.StartServerAsync(token);
        }

        // Once both are up, both draw again, and no id is on both nodes as different rows.
        foreach (TestNode node in nodes)
        {
            Stopwatch drawing = Stopwatch.StartNew();
            while (!await node.PsqlAsync(Database, "SET statement_timeout = '10s'; INSERT INTO t (v) SELECT 'back' FROM generate_series(1, 20)", token))
            {
                Assert.IsLessThan(TestNode.PollTimeout, drawing.Elapsed, $"{node.Name} drawing again once both are up");
            }
        }

        // A restore leaves its node's slots behind, so each node holds the rows the other could still send it.
        string a = await nodeA.SafePsqlAsync(Database, "SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM t", token);
        string b = await nodeB.SafePsqlAsync(Database, "SELECT string_agg(id || ':' || v, ',' ORDER BY id) FROM t", token);
        Dictionary<string, string> rows = [];
        foreach (string[] parts in a.Split(',').Concat(b.Split(',')).Where(row => row.Length > 0).Select(row => row.Split(':', 2)))
        {
            Assert.IsFalse(rows.TryGetValue(parts[0], out string? other) && other != parts[1], $"id {parts[0]} is '{other}' on one node and '{parts[1]}' on the other");
            rows[parts[0]] = parts[1];
        }
    }

    /// <summary>
    /// A node that can't reach the other nodes keeps taking writes, and needn't know which node hands out the numbers:
    /// node_b shows node_a as the allocator, and alter_sequence_range run on node_b names node_a. With node_a down, node_b
    /// inserts far past the blocks it holds and loads 3,000 rows it reserved for, taking its blocks from its
    /// reserve, and after it restarts draws, with a warning, once the worf.sequence_range_restart_wait it sets has
    /// passed; once node_a is back, its own rows take none of node_b's values.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task OfflineNodesKeepTakingWrites()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "worf.sequence_range_restart_wait = 10000");
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);

        // node_b shows which node hands out the numbers, and a function that has to run there names it.
        Assert.AreEqual("node_a", await nodeB.SafePsqlAsync(Database, "SELECT allocator FROM worf.sequence_ranges", token), "the allocator");
        PostgresException elsewhere = await Assert.ThrowsExactlyAsync<PostgresException>(() =>
            nodeB.SafePsqlAsync(Database, "SELECT worf.alter_sequence_range('t_id_seq', range_size := 200)", token));
        Assert.AreEqual("node node_a hands out sequence public.t_id_seq's numbers, not this node|Run this on node node_a.",
            $"{elsewhere.MessageText}|{elsewhere.Hint}", "the function run on node_b");
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_block WHERE seqoid = 't_id_seq'::regclass AND kind = 'p' AND state = 'open')", token),
            "node_b's reserve");

        await nodeA.StopAsync("fast", token);
        _ = await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO t (v) SELECT 'offline' FROM generate_series(1, 5000)", token);
        _ = await nodeB.SafePsqlAsync(Database, """
            SET statement_timeout = '60s';
            BEGIN;
            SELECT worf.reserve_sequence_range('t_id_seq', 3000);
            INSERT INTO t (v) SELECT 'load' FROM generate_series(1, 3000);
            COMMIT;
            """, token);
        Assert.Contains("from this node's reserve, as no other node answered its request for one", nodeB.ReadLog(), "node_b's blocks from its reserve");
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        int restarted = nodeB.ReadLog().Length;
        Stopwatch waited = Stopwatch.StartNew();
        _ = await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO t (v) SELECT 'restarted' FROM generate_series(1, 100)", token);
        Assert.IsLessThan(TimeSpan.FromSeconds(30), waited.Elapsed, "node_b's wait after its restart");
        Assert.Contains("draws though node node_a hasn't answered this server since it started", nodeB.ReadLog()[restarted..], "node_b's warning");
        Assert.AreEqual("8100|8100", await nodeB.SafePsqlAsync(Database, "SELECT count(*), count(DISTINCT id) FROM t", token), "node_b's rows and ids");

        await nodeA.StartServerAsync(token);
        _ = await nodeA.SafePsqlAsync(Database, "SET statement_timeout = '60s'; INSERT INTO t (v) SELECT 'a' FROM generate_series(1, 2000)", token);
        string bIds = await nodeB.SafePsqlAsync(Database, "SELECT string_agg(id::text, ',') FROM t", token);
        Assert.AreEqual("0", await nodeA.SafePsqlAsync(Database,
            $"SELECT count(*) FROM t WHERE v = 'a' AND id = ANY ('{{{bIds}}}'::bigint[])", token), "node_a's ids among node_b's");
    }

    /// <summary>
    /// A fresh session's first row waits for the switch before it runs node_b's last block out, though node_b has seen no
    /// row like it draw: a row into a table whose trigger draws 12 in a loop, through a function it calls or through a
    /// statement it builds, or 40 in a loop, counts for up to half the block until a row is seen, and a statement
    /// whose 40 calls take the sequence's name from a column waits while the block holds fewer than its row's calls. So
    /// does a row whose trigger draws 12 over a set's rows, in a SELECT or a PERFORM, whose default calls a PL/pgSQL or a
    /// SQL function that draws 12, or whose trigger draws 60 in a loop of constant bounds, at 55 left (2140). node_a, down
    /// as node_b reaches its last block with 8, 30 or 55 values left, starts again 3 s in.
    /// </summary>
    /// <param name="form">The row, and how many values the last block has left as it runs.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("loop")]
    [DataRow("helper")]
    [DataRow("execute")]
    [DataRow("merge")]
    [DataRow("loop of 40")]
    [DataRow("named by a column")]
    [DataRow("over a set")]
    [DataRow("perform over a set")]
    [DataRow("default function")]
    [DataRow("default sql function")]
    [DataRow("loop of 60")]
    public async Task FreshRowsWaitBeforeRunningTheLastBlockOut(string form)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE SEQUENCE s1;
                CREATE FUNCTION draw(n int) RETURNS bigint LANGUAGE plpgsql AS $$ DECLARE w bigint := 0; BEGIN FOR i IN 1..n LOOP w := w + nextval('s1'); END LOOP; RETURN w; END $$;
                CREATE FUNCTION draw12() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN draw(12); END $$;
                CREATE TABLE g13l (id bigint DEFAULT nextval('s1'), w bigint);
                CREATE FUNCTION g12_loop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.w := 0; FOR i IN 1..12 LOOP NEW.w := NEW.w + nextval('s1'); END LOOP; RETURN NEW; END $$;
                CREATE TRIGGER g12_loop BEFORE INSERT ON g13l FOR EACH ROW EXECUTE FUNCTION g12_loop();
                CREATE TABLE g13h (id bigint DEFAULT nextval('s1'), w bigint);
                CREATE FUNCTION g12_helper() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.w := draw12(); RETURN NEW; END $$;
                CREATE TRIGGER g12_helper BEFORE INSERT ON g13h FOR EACH ROW EXECUTE FUNCTION g12_helper();
                CREATE TABLE g13e (id bigint DEFAULT nextval('s1'), w bigint);
                CREATE FUNCTION g12_execute() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN EXECUTE format('SELECT %s', (SELECT string_agg('nextval(''s1'')', ' + ') FROM generate_series(1, 12))) INTO NEW.w; RETURN NEW; END $$;
                CREATE TRIGGER g12_execute BEFORE INSERT ON g13e FOR EACH ROW EXECUTE FUNCTION g12_execute();
                CREATE TABLE g41l (id bigint DEFAULT nextval('s1'), w bigint);
                CREATE FUNCTION g40_loop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.w := 0; FOR i IN 1..40 LOOP NEW.w := NEW.w + nextval('s1'); END LOOP; RETURN NEW; END $$;
                CREATE TRIGGER g40_loop BEFORE INSERT ON g41l FOR EACH ROW EXECUTE FUNCTION g40_loop();
                CREATE TABLE seqnames (n text);
                CREATE TABLE g13s (id bigint DEFAULT nextval('s1'), w bigint);
                CREATE FUNCTION g12_set() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.w := (SELECT sum(nextval('s1')) FROM generate_series(1, 12)); RETURN NEW; END $$;
                CREATE TRIGGER g12_set BEFORE INSERT ON g13s FOR EACH ROW EXECUTE FUNCTION g12_set();
                CREATE TABLE g13p (id bigint DEFAULT nextval('s1'), w bigint);
                CREATE FUNCTION g12_perform() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('s1') FROM generate_series(1, 12); RETURN NEW; END $$;
                CREATE TRIGGER g12_perform BEFORE INSERT ON g13p FOR EACH ROW EXECUTE FUNCTION g12_perform();
                CREATE TABLE g13d (id bigint DEFAULT nextval('s1'), w bigint DEFAULT draw(12));
                CREATE FUNCTION draw12sql() RETURNS bigint LANGUAGE sql AS 'SELECT sum(nextval(''s1''))::bigint FROM generate_series(1, 12)';
                CREATE TABLE g13q (id bigint DEFAULT nextval('s1'), w bigint DEFAULT draw12sql());
                CREATE TABLE g61l (id bigint DEFAULT nextval('s1'), w bigint);
                CREATE FUNCTION g60_loop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.w := 0; FOR i IN 1..60 LOOP NEW.w := NEW.w + nextval('s1'); END LOOP; RETURN NEW; END $$;
                CREATE TRIGGER g60_loop BEFORE INSERT ON g61l FOR EACH ROW EXECUTE FUNCTION g60_loop();
                """, token);
        }

        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO seqnames VALUES ('s1')", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('s1', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 's1'::regclass", token),
            "node_b drawing from a block of s1, with the next one ready");

        // With node_a down, node_b draws s1 into its last block until the form's values are left.
        int left = form is "loop of 40" or "named by a column" ? 30 : form == "loop of 60" ? 55 : 8;
        await nodeA.StopAsync("fast", token);
        await using (NpgsqlConnection drawing = await nodeB.OpenAsync(Database, token))
        {
            await using NpgsqlCommand remaining = new("""
                SELECT next_lo IS NULL AND active_lo <= (SELECT last_value FROM s1), active_hi - (SELECT last_value FROM s1)
                FROM worf.sequence_ranges WHERE sequence = 's1'::regclass
                """, drawing);
            await using NpgsqlCommand draw = new("SET statement_timeout = '5s'; SELECT nextval('s1')", drawing);
            for (int i = 0; ; i++)
            {
                Assert.IsLessThan(300, i, $"node_b's draws of s1 before its last block has {left} left");
                await using (NpgsqlDataReader reader = await remaining.ExecuteReaderAsync(token))
                {
                    _ = await reader.ReadAsync(token);
                    if (reader.GetBoolean(0) && reader.GetInt64(1) == left)
                    {
                        break;
                    }
                }

                _ = await draw.ExecuteNonQueryAsync(token);
            }
        }

        // A fresh session's row runs, as node_a starts again 3 s in.
        Task starting = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromSeconds(3), token);
            await nodeA.StartServerAsync(token);
        }, token);
        string statement = form switch
        {
            "loop" => "INSERT INTO g13l (w) VALUES (0)",
            "helper" => "INSERT INTO g13h (w) VALUES (0)",
            "execute" => "INSERT INTO g13e (w) VALUES (0)",
            "merge" => "MERGE INTO g13l USING (VALUES (1)) v (k) ON false WHEN NOT MATCHED THEN INSERT (w) VALUES (0)",
            "loop of 40" => "INSERT INTO g41l (w) VALUES (0)",
            "over a set" => "INSERT INTO g13s (w) VALUES (0)",
            "perform over a set" => "INSERT INTO g13p (w) VALUES (0)",
            "default function" => "INSERT INTO g13d DEFAULT VALUES",
            "default sql function" => "INSERT INTO g13q DEFAULT VALUES",
            "loop of 60" => "INSERT INTO g61l (w) VALUES (0)",
            _ => $"SELECT {string.Join(" + ", Enumerable.Repeat("nextval(n)", 40))} FROM seqnames",
        };
        _ = await nodeB.SafePsqlAsync(Database, $"SET statement_timeout = '30s'; {statement}", token);
        await starting;
    }

    /// <summary>
    /// An upsert, or a merge, whose rows conflict, or which it matches, checks its sequence between them, though such
    /// rows fire no guard trigger, so that the defaults its proposed rows draw, and the draws of its SET, go on through
    /// the blocks as the switches come rather than run one out: 250 rows that each draw, from blocks of 100.
    /// </summary>
    /// <param name="form">The statement.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("do nothing")]
    [DataRow("do update")]
    [DataRow("do update drawing")]
    [DataRow("merge")]
    public async Task UpsertsAndMergesCheckBetweenRows(string form)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1;
            CREATE TABLE g1k (k int PRIMARY KEY, id bigint DEFAULT nextval('s1'), w bigint);
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 's1'::regclass", token),
            "node_a drawing from a block of s1, with the next one ready");
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO g1k (k, w) SELECT g, 0 FROM generate_series(1, 250) g", token);
        _ = await nodeA.SafePsqlAsync(Database, form switch
        {
            "do nothing" => "INSERT INTO g1k (k, w) SELECT g, 0 FROM generate_series(1, 250) g ON CONFLICT DO NOTHING",
            "do update" => "INSERT INTO g1k (k, w) SELECT g, 0 FROM generate_series(1, 250) g ON CONFLICT (k) DO UPDATE SET w = 0",
            "do update drawing" => "INSERT INTO g1k (k, w) SELECT g, 0 FROM generate_series(1, 250) g ON CONFLICT (k) DO UPDATE SET w = nextval('s1')",
            _ => """
                MERGE INTO g1k USING generate_series(1, 250) g ON g1k.k = g
                WHEN MATCHED THEN UPDATE SET w = nextval('s1') WHEN NOT MATCHED THEN INSERT (k, w) VALUES (g, 0)
                """,
        }, token);
    }

    /// <summary>
    /// Sessions that each draw with SELECT nextval at once leave each other values enough: each counts among those
    /// drawing from the sequence from its first check until it ends, so that the block's last twentieth, under five values
    /// for a block of 100, isn't run out by six of them drawing past it before any switch. So do calls whose sequence the
    /// plan doesn't name, from a column or a parameter, which wait once the block holds fewer values than they and the
    /// other drawing statements may take, a SELECT of five rows, whose rows drawing without a check are shared among
    /// the statements drawing (2042), a PL/pgSQL loop's five assignments, each a unit of its own, and a loop of nothing
    /// but an assignment that draws, which keeps the sequence's gate from turn to turn (2301). No value is drawn twice.
    /// </summary>
    /// <param name="form">How each session draws.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("named")]
    [DataRow("from a column")]
    [DataRow("from a parameter")]
    [DataRow("five rows")]
    [DataRow("loop of five")]
    [DataRow("tight loop of five")]
    public async Task ConcurrentSelectsOfNextvalLeaveEachOtherValues(string form)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s2;
            CREATE TABLE seqnames (n text);
            INSERT INTO seqnames VALUES ('s2');
            CREATE FUNCTION draw5() RETURNS SETOF bigint LANGUAGE plpgsql AS $$
            DECLARE x bigint;
            BEGIN FOR i IN 1..5 LOOP x := nextval('s2'); RETURN NEXT x; END LOOP; END $$;
            CREATE FUNCTION draw5_tightly() RETURNS bigint[] LANGUAGE plpgsql AS $$
            DECLARE a bigint[] := '{}';
            BEGIN FOR i IN 1..5 LOOP a := a || nextval('s2'); END LOOP; RETURN a; END $$;
            SELECT worf.enable_sequence_range('s2', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 's2'::regclass", token),
            "node_a drawing from a block of s2, with the next one ready");
        int draws = 0;
        List<string> failures = [];
        ConcurrentBag<long> drawn = [];
        DateTime until = DateTime.UtcNow.AddSeconds(10);
        await Task.WhenAll(Enumerable.Range(0, 6).Select(async session =>
        {
            await using NpgsqlConnection connection = await nodeA.OpenAsync(Database, token);
            await using NpgsqlCommand draw = new(form switch
            {
                "from a column" => "SELECT nextval(n) FROM seqnames",
                "from a parameter" => "SELECT nextval($1::regclass)",
                "five rows" => "SELECT nextval('s2') FROM generate_series(1, 5)",
                "loop of five" => "SELECT draw5()",
                "tight loop of five" => "SELECT unnest(draw5_tightly())",
                _ => "SELECT nextval('s2')",
            }, connection);
            if (form == "from a parameter")
            {
                _ = draw.Parameters.Add(new NpgsqlParameter { Value = "s2" });
                await draw.PrepareAsync(token);
            }

            while (DateTime.UtcNow < until)
            {
                try
                {
                    await using NpgsqlDataReader reader = await draw.ExecuteReaderAsync(token);
                    while (await reader.ReadAsync(token))
                    {
                        _ = Interlocked.Increment(ref draws);
                        drawn.Add(reader.GetInt64(0));
                    }
                }
                catch (PostgresException failed)
                {
                    lock (failures)
                    {
                        failures.Add($"{failed.SqlState} {failed.MessageText}");
                    }
                }
            }
        }));
        Assert.IsEmpty(failures, $"{failures.Count} failures of {draws + failures.Count} draws: {string.Join("; ", failures.Distinct().Take(3))}");
        Assert.IsGreaterThan(1000, draws, "the six sessions' draws");
        Assert.HasCount(drawn.Count, drawn.Distinct().ToList(), "the values drawn, each once");
    }

    /// <summary>
    /// A statement whose wait for a block's switch gave up, as the block's owner was down, leaves the session's later
    /// statements to wait for the switch again, in later transactions too: once the owner is back, a transaction's 20
    /// draws wait for the switch rather than run the block out.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task GivenUpSwitchWaitsWaitAgain()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "worf.sequence_range_offline_wait = -1");
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE SEQUENCE s1", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('s1', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 's1'::regclass", token),
            "node_b drawing from a block of s1, with the next one ready");

        // With node_a down, and node_b never going on without it, as worf.sequence_range_offline_wait -1 has it, a
        // transaction on node_b draws s1, and another session then draws s1 until a draw waits for a switch that can't
        // come, in the last block's last twentieth.
        await nodeA.StopAsync("fast", token);
        await using NpgsqlConnection connection = await nodeB.OpenAsync(Database, token);
        await using (NpgsqlTransaction first = await connection.BeginTransactionAsync(token))
        {
            await using (NpgsqlCommand draw = new("SELECT nextval('s1')", connection, first))
            {
                _ = await draw.ExecuteNonQueryAsync(token);
            }

            await using (NpgsqlConnection other = await nodeB.OpenAsync(Database, token))
            {
                await using (NpgsqlCommand timeout = new("SET statement_timeout = '2s'", other))
                {
                    _ = await timeout.ExecuteNonQueryAsync(token);
                }

                await using NpgsqlCommand draw = new("SELECT nextval('s1')", other);
                for (int i = 0; ; i++)
                {
                    Assert.IsLessThan(300, i, "node_b's draws of s1 before one waits");
                    try
                    {
                        _ = await draw.ExecuteScalarAsync(token);
                    }
                    catch (PostgresException waited) when (waited.SqlState == "57014")
                    {
                        break;
                    }
                }
            }

            // The transaction's next draw waits for the switch until it gives up, and draws from the block's tail.
            await using (NpgsqlCommand givingUp = new("SET LOCAL statement_timeout = '60s'; SELECT nextval('s1')", connection, first))
            {
                Stopwatch waiting = Stopwatch.StartNew();
                _ = await givingUp.ExecuteNonQueryAsync(token);
                Assert.IsGreaterThan(TimeSpan.FromSeconds(5), waiting.Elapsed, "the draw that gave up waiting for s1's switch");
            }

            await first.RollbackAsync(token);
        }

        // A later transaction draws 20 values, while node_a starts again 3 s in, each waiting for the switch as it must.
        Task starting = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromSeconds(3), token);
            await nodeA.StartServerAsync(token);
        }, token);
        await using (NpgsqlTransaction later = await connection.BeginTransactionAsync(token))
        {
            await using NpgsqlCommand draw = new("SET LOCAL statement_timeout = '15s'; SELECT nextval('s1')", connection, later);
            for (int i = 0; i < 20; i++)
            {
                _ = await draw.ExecuteNonQueryAsync(token);
            }

            await later.CommitAsync(token);
        }

        await starting;
    }

    /// <summary>
    /// Idle calls stay cheap, and an owner doesn't wait for them. A position a call kept for a sequence of a neighbor's
    /// other system or timeline goes once the next call takes the sequence in, after which idle calls write nothing. Idle
    /// calls read a neighbor's grants and acknowledgements through their keys, rather than scanning every grant the owner
    /// made, which a sequence's switches add to without end: here 20,000 grants of another sequence. Each runs a few
    /// statements, however many sequences the nodes manage, rather than several for each: the owner reads each part of its
    /// answer for every sequence at once, and checks what each sequence's report depends on at once, passing over those
    /// whose reports, and all they depend on, are as they were, though another sequence's ranges change as the owner draws
    /// from it. And an owner that only answers calls draws from the blocks it grants itself without waiting for its
    /// neighbor's next call, though that comes only every minute: it tells its subscribers, through its WAL, to call it as
    /// it grants itself one, so that the neighbor hears of the block before the owner needs it; each of its inserts draws
    /// one value, and blocks hold ten.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task IdleCallsStayCheapAndOwnersDontWaitForThem()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "shared_preload_libraries = 'worf,pg_stat_statements'\npg_stat_statements.track = all");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        const string Sequences = """
            CREATE TABLE t (id bigserial PRIMARY KEY, v text);
            CREATE SEQUENCE past;
            DO $$ BEGIN FOR g IN 1..10 LOOP EXECUTE format('CREATE SEQUENCE many_%s', lpad(g::text, 2, '0')); END LOOP; END $$
            """;
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, Sequences, token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "CREATE EXTENSION pg_stat_statements", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 10, max_range_size := 10);
            SELECT count(worf.enable_sequence_range(format('many_%s', lpad(g::text, 2, '0'))::regclass, participants := '{node_b}', range_size := 100)) FROM generate_series(1, 10) g;
            """, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 11 FROM worf.sequence_ranges WHERE state = 'active'", token),
            "node_b drawing from blocks of the 11 sequences");

        // node_b draws once from each, so that its idle calls report the draws, as they were.
        _ = await nodeB.SafePsqlAsync(Database, """
            INSERT INTO t (v) VALUES ('b');
            SELECT count(nextval(format('many_%s', lpad(g::text, 2, '0'))::regclass)) FROM generate_series(1, 10) g;
            """, token);

        // A kept position of another timeline goes, and idle calls, which report node_b's draws as they were, then write
        // nothing.
        string nodeBId = await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        _ = await nodeA.SafePsqlAsync(Database, $"""
            INSERT INTO worf.sequence_range_back (seqoid, node_id, call, sysid, timeline, lsn, started)
            SELECT 't_id_seq'::regclass, {nodeBId}, true, sysid, 99, '0/1', 0 FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND node_id = {nodeBId}
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT NOT EXISTS (SELECT 1 FROM worf.sequence_range_back)", token),
            "the kept position of another timeline, gone");

        // The exchanges that hand out, acknowledge and hear of the next blocks change reports, and settle first: node_b
        // and node_a hold next blocks of every sequence, node_b has acknowledged its own and reports node_a's watermark
        // of each, and a second goes without writes, which calls that each write never give.
        const string AllNext = "SELECT count(*) = 11 FROM worf.sequence_ranges WHERE state = 'active' AND next_lo IS NOT NULL";
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, AllNext, token), "node_b's next blocks");
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"""
            SELECT ({AllNext}) AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_grant WHERE state = 'sent')
                AND NOT EXISTS (
                    SELECT 1 FROM worf.sequence_range r JOIN worf.sequence_range_node n ON n.seqoid = r.seqoid AND n.node_id = {nodeBId}
                    WHERE n.watermark IS DISTINCT FROM r.watermark)
            """, token), "node_a's next blocks, and node_b's report of them");
        const string NextXid = "SELECT pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot())";
        Stopwatch settling = Stopwatch.StartNew();
        long firstXid = long.Parse(await nodeA.SafePsqlAsync(Database, NextXid, token), CultureInfo.InvariantCulture);
        while (true)
        {
            await Task.Delay(TimeSpan.FromSeconds(1), token);
            long settled = long.Parse(await nodeA.SafePsqlAsync(Database, NextXid, token), CultureInfo.InvariantCulture);
            if (settled == firstXid)
            {
                break;
            }

            firstXid = settled;
            Assert.IsLessThan(TestNode.PollTimeout, settling.Elapsed, "node_a's calls, settled to a second without writes");
        }

        await Task.Delay(TimeSpan.FromSeconds(5), token);
        long lastXid = long.Parse(await nodeA.SafePsqlAsync(Database, NextXid, token), CultureInfo.InvariantCulture);
        Assert.AreEqual(firstXid, lastXid, "the next transaction ID on node_a, after 5 s of node_b's idle calls, which use none");
        _ = await nodeA.SafePsqlAsync(Database, """
            INSERT INTO worf.sequence_range_grant (grant_id, request_id, seqoid, grantee_id, grantee_name, kind, lo, hi, state, granted_at)
            SELECT gen_random_uuid(), gen_random_uuid(), 'past'::regclass, 1, 'gone', 'n', g * 10, g * 10 + 9, 'abandoned', now()
            FROM generate_series(1, 20000) g;
            ANALYZE worf.sequence_range_grant;
            """, token);
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        const string Scans = "SELECT seq_scan || '|' || seq_tup_read FROM pg_catalog.pg_stat_all_tables WHERE relid = 'worf.sequence_range_grant'::regclass";
        string before = await nodeA.SafePsqlAsync(Database, Scans, token);
        await Task.Delay(TimeSpan.FromSeconds(5), token);
        string after = await nodeA.SafePsqlAsync(Database, Scans, token);
        Assert.IsLessThan(20_000L, long.Parse(after.Split('|')[1], CultureInfo.InvariantCulture) - long.Parse(before.Split('|')[1], CultureInfo.InvariantCulture),
            $"the grants node_a read by scanning them as node_b's idle calls came, from {before} to {after}");

        // The statements each of node_b's calls ran on node_a, which pg_stat_statements counts as nested in the call's, over
        // six calls at least: the fewest of three such windows, as a busy host's first calls may still take in what
        // changed as the sequences started. A call that finds what a sequence's report depends on changed, or about to,
        // as a report that's due to go stale on a busy host, checks it in full, about a dozen statements, as every call
        // did each sequence before.
        const string Counted = """
            SELECT coalesce(sum(calls) FILTER (WHERE toplevel AND query LIKE 'SELECT worf.sequence_range_exchange(%'), 0) || '|' ||
                coalesce(sum(calls) FILTER (WHERE NOT toplevel), 0)
            FROM pg_stat_statements
            """;
        async Task<long> FewestStatementsAsync()
        {
            long fewest = long.MaxValue;
            for (int window = 0; window < 3 && fewest >= 60; window++)
            {
                _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_stat_statements_reset()", token);
                Stopwatch counting = Stopwatch.StartNew();
                (long Calls, long Statements) counted = (0, 0);
                while (counted.Calls < 6)
                {
                    Assert.IsLessThan(TestNode.PollTimeout, counting.Elapsed, $"node_b's calls to node_a, {counted.Calls} so far");
                    await Task.Delay(TimeSpan.FromSeconds(1), token);
                    string[] read = (await nodeA.SafePsqlAsync(Database, Counted, token)).Split('|');
                    counted = (long.Parse(read[0], CultureInfo.InvariantCulture), long.Parse(read[1], CultureInfo.InvariantCulture));
                }

                TestContext.WriteLine($"{counted.Calls} calls ran {counted.Statements} statements on node_a, most often:");
                TestContext.WriteLine(await nodeA.SafePsqlAsync(Database, """
                    SELECT string_agg(calls || ' ' || left(regexp_replace(query, '\s+', ' ', 'g'), 150), E'\n' ORDER BY calls DESC)
                    FROM (SELECT calls, query FROM pg_stat_statements WHERE NOT toplevel ORDER BY calls DESC LIMIT 12) q
                    """, token));
                fewest = Math.Min(fewest, counted.Statements / counted.Calls);
            }

            return fewest;
        }

        long fewest = await FewestStatementsAsync();
        Assert.IsLessThan(60L, fewest, "the statements each of node_b's idle calls ran on node_a, managing 11 sequences");

        // While node_a draws from t_id_seq 20 times a second, switching to a block it grants itself every half second,
        // which changes the ranges, node_b's calls check t_id_seq in full, and pass over the other ten as before.
        using CancellationTokenSource stopDrawing = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task drawing = Task.Run(async () =>
        {
            await using NpgsqlConnection busy = await nodeA.OpenAsync(Database, token);
            await using NpgsqlCommand draw = new("INSERT INTO t (v) VALUES ('busy')", busy);
            while (!stopDrawing.IsCancellationRequested)
            {
                _ = await draw.ExecuteNonQueryAsync(token);
                await Task.Delay(TimeSpan.FromMilliseconds(50), token);
            }
        }, token);
        long busiest = await FewestStatementsAsync();

        // Meanwhile node_a's range worker checks t_id_seq in full at every tick, and the other ten only every second, rather
        // than every sequence at every tick that follows a change to any of them.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_stat_statements_reset()", token);
        await Task.Delay(TimeSpan.FromSeconds(10), token);
        long fullChecks = long.Parse(await nodeA.SafePsqlAsync(Database,
            "SELECT coalesce(sum(calls), 0) FROM pg_stat_statements WHERE query LIKE 'SELECT fenced_at IS NOT NULL, owner_node FROM worf.sequence_range WHERE seqoid = $1'", token),
            CultureInfo.InvariantCulture);
        await stopDrawing.CancelAsync();
        await drawing;

        Assert.IsLessThan(60L, busiest, "the statements each of node_b's idle calls ran on node_a, as node_a drew from t_id_seq");
        Assert.IsLessThan(450L, fullChecks, "the full checks node_a's range worker ran in 10 s at ticks of 100 ms, as it drew from t_id_seq alone of 11 sequences");

        // node_a draws through six of its own blocks while node_b calls only every minute.
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_exchange_interval = '60s'", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        await using NpgsqlConnection connection = await nodeA.OpenAsync(Database, token);
        await using (NpgsqlCommand timeout = new("SET statement_timeout = '10s'", connection))
        {
            _ = await timeout.ExecuteNonQueryAsync(token);
        }

        await using NpgsqlCommand insert = new("INSERT INTO t (v) VALUES ('a')", connection);
        Stopwatch inserting = Stopwatch.StartNew();
        for (int i = 0; i < 60; i++)
        {
            _ = await insert.ExecuteNonQueryAsync(token);
        }

        Assert.IsLessThan(TimeSpan.FromSeconds(30), inserting.Elapsed, "node_a's 60 inserts, through six of its own blocks");
    }

    /// <summary>
    /// A call that has taken a third of its time, and taken a sequence in, leaves the rest it would have to check to the
    /// next call, unread, rather than check those whose reports change nothing.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task LateCallsLeaveTheRestUnread()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "log_min_messages = debug1");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        const string Sequences = "CREATE TABLE slow_on (); DO $$ BEGIN FOR g IN 1..12 LOOP EXECUTE format('CREATE SEQUENCE many_%s', lpad(g::text, 2, '0')); END LOOP; END $$";
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, Sequences, token);
        }

        // Once it's turned on, each report node_a takes in costs it 1.2 s, past a third of node_b's 3 s calls.
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE FUNCTION slow_report() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT FROM public.slow_on) THEN PERFORM pg_sleep(1.2); END IF; RETURN NEW; END $$;
            CREATE TRIGGER slow_report BEFORE UPDATE ON worf.sequence_range_node FOR EACH ROW EXECUTE FUNCTION slow_report();
            """, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT count(worf.enable_sequence_range(format('many_%s', lpad(g::text, 2, '0'))::regclass, participants := '{node_b}', range_size := 100)) FROM generate_series(1, 12) g",
            token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 12 FROM worf.sequence_ranges WHERE state = 'active'", token),
            "node_b drawing from blocks of the 12 sequences");
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_timeout = '3s'", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        await Task.Delay(TimeSpan.FromSeconds(3), token);

        // node_b's calls find no exchange on node_a while node_a's process for them ends and node_b resets many_01, giving
        // up its blocks and asking for new ones, so that its report of it changes, and many_01 alone, which sorts first, so
        // that no later sequence's change cuts a call; a draw alone changes no report. node_b's next call then comes to a
        // new process on node_a, which has checked none of its reports: taking many_01 in passes a third of the call's
        // time, and the call leaves the rest. A call that came to the new process before the reset would find every report
        // as it was, which the process then passes over, unread, at every call after.
        _ = await nodeA.SafePsqlAsync(Database,
            "INSERT INTO slow_on DEFAULT VALUES; ALTER FUNCTION worf.sequence_range_exchange(text) RENAME TO sequence_range_exchange_paused", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = 'b_from_a_range'", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('many_01')", token);
        int logpos = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange_paused(text) RENAME TO sequence_range_exchange", token);
        Stopwatch cutting = Stopwatch.StartNew();
        while (!LeftToNextCall().IsMatch(nodeA.ReadLog()[logpos..]))
        {
            Assert.IsLessThan(TestNode.PollTimeout, cutting.Elapsed, "node_a's late call, leaving the sequences it would have had to check");
            await Task.Delay(TimeSpan.FromMilliseconds(200), token);
        }
    }

    // The peak resident memory of a process of the server, which runs beside the test, in kB, as Linux reports it, or 0
    // elsewhere.
    private static long PeakResident(int pid)
    {
        if (!OperatingSystem.IsLinux())
        {
            return 0;
        }

        string line = File.ReadLines($"/proc/{pid.ToString(CultureInfo.InvariantCulture)}/status").First(line => line.StartsWith("VmHWM:", StringComparison.Ordinal));
        return long.Parse(line["VmHWM:".Length..].Replace("kB", "", StringComparison.Ordinal).Trim(), CultureInfo.InvariantCulture);
    }

    // The CPU time log_statement_stats gives a message, in seconds of user and system time.
    [GeneratedRegex(@"!\s*([0-9.]+) s user, ([0-9.]+) s system, [0-9.]+ s elapsed")]
    private static partial Regex StatementUsage();

    // The CPU time a backend's messages took since a position in its server's log, each of which log_statement_stats
    // logs, once the log shows a line that comes after them.
    private static async Task<TimeSpan> StatementsCpuAsync(TestNode node, int logged, int pid, string after, CancellationToken token)
    {
        Stopwatch reading = Stopwatch.StartNew();
        while (!node.ReadLog()[logged..].Contains(after, StringComparison.Ordinal))
        {
            Assert.IsLessThan(TestNode.PollTimeout, reading.Elapsed, $"the statements' statistics in {node.Name}'s log");
            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
        }

        string marker = string.Create(CultureInfo.InvariantCulture, $"[{pid}]");
        string[] lines = node.ReadLog()[logged..].Split('\n');
        double cpu = 0;
        for (int i = 0; i < lines.Length; i++)
        {
            if (lines[i].Contains(marker, StringComparison.Ordinal) && lines[i].Contains("MESSAGE STATISTICS", StringComparison.Ordinal) &&
                lines.Skip(i + 1).Select(line => StatementUsage().Match(line)).FirstOrDefault(match => match.Success) is { } usage)
            {
                cpu += double.Parse(usage.Groups[1].Value, CultureInfo.InvariantCulture) + double.Parse(usage.Groups[2].Value, CultureInfo.InvariantCulture);
            }
        }

        return TimeSpan.FromSeconds(cpu);
    }

    // A line of a failure that names an address and says nothing more, as libpq begins one for the next address.
    [GeneratedRegex("failed: *$", RegexOptions.Multiline)]
    private static partial Regex BareFailureLine();

    [GeneratedRegex(@"left [0-9]+ sequences to its next call")]
    private static partial Regex LeftToNextCall();

    /// <summary>
    /// An owner that only answers calls draws from the blocks it grants itself without waiting for its subscriber's next
    /// call, though the subscriber applies the owner's changes 40 seconds late, or its stream is behind a transaction it
    /// can't apply yet: the owner's word to call it reaches the connection its exchanges keep, as a notification, and the
    /// subscriber calls at once. The word goes out only once the grant is seen, as past a synchronous standby's reply,
    /// which here comes half a second late, through a proxy that holds what the standby sends, while the inserts commit
    /// locally; and the inserts start as the subscriber's read-ahead of 90 MB of the owner's changes fills up, which reads
    /// no more of them, so that the word that comes in the stream can't reach it. A grant in one database makes only its
    /// own database write the word: another database of the owner's that manages a sequence writes none. A session idle
    /// in a transaction that only read the ranges doesn't keep node_b's calls from waiting for the grants' rows. A
    /// subscriber whose kept connection the owner ended hears no notification until it calls again, which its read-ahead,
    /// once full, has it do each second.
    /// </summary>
    /// <param name="how">The owner as it is, with a synchronous standby that answers late, beside a session idle after
    /// reading the ranges too, writing 90 MB first, with the subscriber's kept connection ended too, or with its
    /// subscriber's apply waiting on a lock, without a delay.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("word")]
    [DataRow("synchronous")]
    [DataRow("synchronous read")]
    [DataRow("busy")]
    [DataRow("busy unkept")]
    [DataRow("behind")]
    public async Task OwnersDontWaitForDelayedSubscribers(string how)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "log_min_messages = debug2");
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "log_min_messages = debug2");
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text); CREATE TABLE big (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token, applyDelay: how == "behind" ? "0" : "40s");
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 10, max_range_size := 10)", token);
        await WaitActiveAsync([nodeA, nodeB], token);

        // node_a draws through six of its own blocks while node_b calls only every minute.
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_exchange_interval = '60s'", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        await Task.Delay(TimeSpan.FromSeconds(2), token);

        using TemporaryDirectory received = new($"worf-sequence-range-receivewal-{Guid.NewGuid():N}");
        using CancellationTokenSource relaying = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task relayed = Task.CompletedTask;

        // The listener goes once pg_receivewal has connected through it, which the standby's sync state shows.
        async Task<Process> StartStandbyAsync()
        {
            // pg_receivewal, as the synchronous standby, reaches node_a through a proxy that holds each of its messages,
            // its replies among them, for half a second.
            using TcpListener proxy = new(IPAddress.Loopback, 0);
            proxy.Start();
            relayed = Task.Run(async () =>
            {
                try
                {
                    using TcpClient inbound = await proxy.AcceptTcpClientAsync(relaying.Token);
                    using TcpClient outbound = new();
                    await outbound.ConnectAsync(IPAddress.Loopback, nodeA.Cluster.Port, relaying.Token);
                    NetworkStream client = inbound.GetStream();
                    NetworkStream server = outbound.GetStream();
                    Channel<(long Due, byte[] Data)> held = Channel.CreateUnbounded<(long, byte[])>();
                    Task down = server.CopyToAsync(client, relaying.Token);
                    Task reading = Task.Run(async () =>
                    {
                        byte[] buffer = new byte[65536];
                        for (int read; (read = await client.ReadAsync(buffer, relaying.Token)) > 0;)
                        {
                            await held.Writer.WriteAsync((Stopwatch.GetTimestamp() + Stopwatch.Frequency / 2, buffer[..read]), relaying.Token);
                        }

                        held.Writer.Complete();
                    }, relaying.Token);
                    await foreach ((long due, byte[] data) in held.Reader.ReadAllAsync(relaying.Token))
                    {
                        TimeSpan wait = Stopwatch.GetElapsedTime(Stopwatch.GetTimestamp(), due);
                        if (wait > TimeSpan.Zero)
                        {
                            await Task.Delay(wait, relaying.Token);
                        }

                        await server.WriteAsync(data, relaying.Token);
                    }

                    await Task.WhenAll(down, reading);
                }
                catch (Exception relayFailed) when (relayFailed is OperationCanceledException or IOException or SocketException)
                {
                    // The proxy goes as the test ends.
                }
            }, CancellationToken.None);
            string proxied = $"host=127.0.0.1 port={((IPEndPoint)proxy.LocalEndpoint).Port.ToString(CultureInfo.InvariantCulture)} dbname={Database} user={SuperUser}";
            Process started = Process.Start(new ProcessStartInfo(Path.Join(nodeA.Installation.BinDirectory, "pg_receivewal"))
            {
                ArgumentList = { "-D", received.FullName, "--synchronous", "--no-loop", "-d", proxied },
                RedirectStandardError = true,
            })!;
            _ = await nodeA.SafePsqlAsync(Database, "ALTER SYSTEM SET synchronous_standby_names = 'pg_receivewal'", token);
            _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
            Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_replication WHERE application_name = 'pg_receivewal' AND sync_state = 'sync')", token),
                "node_a's synchronous standby");
            return started;
        }

        // Another database of node_a's manages a sequence of its own, whose words a decoding slot there would show.
        if (how == "word")
        {
            _ = await nodeA.SafePsqlAsync(Database, "CREATE DATABASE other", token);
            _ = await nodeA.SafePsqlAsync("other", $"""
                CREATE EXTENSION worf;
                SELECT * FROM worf.create_node(node_name := 'node_x', dsn := '{nodeA.Dsn("other", SuperUser)}');
                CREATE TABLE o (id bigserial PRIMARY KEY);
                SELECT worf.enable_sequence_range('o_id_seq', participants := '{"{}"}', range_size := 10, max_range_size := 10);
                """, token);
            Assert.IsTrue(await nodeA.PollQueryUntilAsync("other", "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'o_id_seq'::regclass", token),
                "other's sequence active");
            _ = await nodeA.SafePsqlAsync("other", "SELECT pg_create_logical_replication_slot('words', 'test_decoding')", token);
        }

        // node_b's apply waits on a lock of big, held on node_b, as it applies the first of node_a's rows of it, so that
        // the rest of node_a's stream, the word among it, waits behind them.
        await using NpgsqlConnection locker = await nodeB.OpenAsync(Database, token);
        await using NpgsqlTransaction locking = await locker.BeginTransactionAsync(token);
        using Process? standby = how.StartsWith("synchronous", StringComparison.Ordinal) ? await StartStandbyAsync() : null;
        await using NpgsqlConnection reader = await nodeA.OpenAsync(Database, token);
        await using NpgsqlTransaction reading = await reader.BeginTransactionAsync(token);
        if (how == "synchronous read")
        {
            await using NpgsqlCommand read = new("SELECT count(*) FROM worf.sequence_ranges", reader, reading);
            _ = await read.ExecuteScalarAsync(token);
        }

        if (how is "busy" or "busy unkept" or "behind")
        {
            _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.replication_set_add_table('default', 'big')", token);
            if (how == "behind")
            {
                await using NpgsqlCommand lockBig = new("LOCK TABLE big IN ACCESS EXCLUSIVE MODE", locker, locking);
                _ = await lockBig.ExecuteNonQueryAsync(token);
                _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO big (v) VALUES ('first')", token);
                Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
                    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE relation = 'big'::regclass AND NOT granted)", token), "node_b's apply, waiting on big");
            }
            else
            {
                // node_b reads 64 MB of the 90 MB ahead as node_a's walsender sends them, and the inserts start at once,
                // after node_a ends the session node_b's exchanges keep, which node_b makes again only as it calls.
                _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO big (v) SELECT repeat('x', 1000) FROM generate_series(1, 90000)", token);
                if (how == "busy unkept")
                {
                    // Here the inserts start once node_b's read-ahead is full, which takes a slow walsender a while.
                    _ = await nodeA.SafePsqlAsync(Database, "SELECT count(pg_terminate_backend(pid)) FROM pg_catalog.pg_stat_activity WHERE application_name = 'b_from_a_range'", token);
                    Assert.IsTrue(await nodeB.PollLogAsync(0, "of node node_a's changes read ahead as it waits out its delay", token), "node_b's read-ahead, full");
                }
            }
        }

        try
        {
            await using NpgsqlConnection connection = await nodeA.OpenAsync(Database, token);
            await using (NpgsqlCommand timeout = new("SET statement_timeout = '10s'; SET synchronous_commit = local", connection))
            {
                _ = await timeout.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand insert = new("INSERT INTO t (v) VALUES ('a')", connection);
            Stopwatch inserting = Stopwatch.StartNew();
            for (int i = 0; i < 60; i++)
            {
                _ = await insert.ExecuteNonQueryAsync(token);
            }

            Assert.IsLessThan(TimeSpan.FromSeconds(30), inserting.Elapsed, "node_a's 60 inserts, through six of its own blocks");
            if (how == "word")
            {
                Assert.AreEqual("0", await nodeA.SafePsqlAsync("other",
                    "SELECT count(*) FROM pg_logical_slot_get_changes('words', NULL, NULL) WHERE data LIKE '%prefix: worf_sequence_range%'", token),
                    "the words other wrote for Database's grants");
            }
        }
        finally
        {
            if (standby is not null)
            {
                _ = await nodeA.PsqlAsync(Database, "ALTER SYSTEM RESET synchronous_standby_names; SELECT pg_reload_conf()", token);
                standby.Kill();
                await standby.WaitForExitAsync(CancellationToken.None);
            }

            await relaying.CancelAsync();
            await relayed;
        }
    }

    /// <summary>
    /// The block sizes alter_sequence_range sets on the owner reach its participants, an idle one too, which request
    /// blocks of them from then on, and the owner grants none larger than its largest size, though a participant
    /// requested one before it heard.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task AlteredSizesReachParticipants()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 250, token);

        // node_b settles first, so that the owner's report of the sizes is all that changes in its answers.
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT next_lo IS NOT NULL AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_request) FROM worf.sequence_ranges", token), "node_b settled");
        string nodeBId = await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        string altered = await nodeA.SafePsqlAsync(Database, "SELECT worf.alter_sequence_range('t_id_seq', 10, 10); SELECT clock_timestamp()", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT range_size = 10 AND max_range_size = 10 AND request_size = 10 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token),
            "node_b's block sizes after the owner's change");

        // node_b uses up the blocks it held before the change, which its rate may have made larger than the old size, and
        // draws from those node_a grants it after.
        string after = $"FROM worf.sequence_range_grant WHERE seqoid = 't_id_seq'::regclass AND grantee_id = {nodeBId} AND kind = 'n' AND granted_at > '{altered}'";
        for (int i = 0; i < 30 && await nodeA.SafePsqlAsync(Database, $"SELECT count(*) < 3 {after}", token) == "t"; i++)
        {
            await InsertAsync(nodeB, 100, token);
        }

        Assert.AreEqual("t", await nodeA.SafePsqlAsync(Database, $"SELECT count(*) >= 3 AND max(hi - lo + 1) <= 10 {after}", token),
            "the blocks node_a granted node_b after the change");
    }

    /// <summary>
    /// An owner's draws after it restarts wait for each node that calls it to answer since, up to twice
    /// worf.sequence_range_timeout, here ten seconds: node_b, whose calls and probe ran out of time as node_a hung before
    /// it restarted, calls it as its replication stream from node_a starts again, rather than only once its retry,
    /// twenty seconds after the third ran out, is due. node_b reaches node_a through a relay, which hangs as a paused
    /// server would.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestartedOwnersAskTheirCallersToCall()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await using ProviderRelay relay = new(nodeA.Cluster.Port);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token,
            providerDsn: $"host=127.0.0.1 port={relay.Port.ToString(CultureInfo.InvariantCulture)} dbname={Database} user={SuperUser}");
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeA, 10, token);
        await InsertAsync(nodeB, 10, token);

        // node_b's calls and the probe between them run out of time, so that its next call waits twenty seconds.
        int logpos = nodeB.ReadLog().Length;
        relay.Hang();
        Stopwatch hanging = Stopwatch.StartNew();
        while (nodeB.ReadLog()[logpos..].Split("node node_a didn't answer within 5000 ms").Length < 4)
        {
            Assert.IsLessThan(TestNode.PollTimeout, hanging.Elapsed, "node_b's call and probe running out of time");
            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }

        relay.Pass();
        await nodeA.StopAsync("fast", token);
        await nodeA.StartServerAsync(token);
        await using NpgsqlConnection writer = await nodeA.OpenAsync(Database, token);
        await using NpgsqlCommand insert = new("INSERT INTO t (v) VALUES ('a')", writer);
        _ = await insert.ExecuteNonQueryAsync(token);
    }

    /// <summary>
    /// A node dropped with its subscription lets the connection its exchanges kept to the provider go: the provider's
    /// word to call it, or the end of the provider's backend, which makes the socket readable, then has the exchange
    /// worker, which runs on while the database still has ranges, wait as before rather than wake at once over and over.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux)]
    public async Task DroppedNodesLetTheirKeptConnectionsGo()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 10, max_range_size := 10)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 30, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE application_name = 'b_from_a_range')", token),
            "node_b's kept connection to node_a");

        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.drop_subscription('b_from_a'); SELECT worf.drop_node('node_b')", token);

        // node_a grants itself blocks, each with a word to call it, and ends the backend node_b's connection had.
        await InsertAsync(nodeA, 40, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT count(pg_catalog.pg_terminate_backend(pid)) FROM pg_catalog.pg_stat_activity WHERE application_name = 'b_from_a_range'", token);
        await Task.Delay(TimeSpan.FromSeconds(1), token);

        // The exchange worker's CPU time over three seconds, in clock ticks, of 100 a second.
        string pid = await nodeB.SafePsqlAsync(Database, "SELECT pid FROM pg_catalog.pg_stat_activity WHERE backend_type LIKE 'worf exchange %'", token);
        Assert.AreNotEqual("", pid, "node_b's exchange worker, which runs on while the database has ranges");
        long Ticks()
        {
            string[] stat = File.ReadAllText($"/proc/{pid}/stat").Split(')')[1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
            return long.Parse(stat[11], CultureInfo.InvariantCulture) + long.Parse(stat[12], CultureInfo.InvariantCulture);
        }

        long before = Ticks();
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.IsLessThan(30L, Ticks() - before, "node_b's exchange worker's CPU ticks over 3 s");
    }

    /// <summary>
    /// A participant's reservation for a load gets a block of all the values it asked for, though they are more than the
    /// owner's largest size, which caps only next blocks: node_b, with sizes 100 and 1000, reserves 5,000 values and
    /// loads them in its transaction from the one block.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ParticipantsReserveWholeLoads()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        _ = await nodeB.SafePsqlAsync(Database, """
            BEGIN;
            SELECT worf.reserve_sequence_range('t_id_seq', 5000);
            INSERT INTO t (v) SELECT 'load' FROM generate_series(1, 5000);
            COMMIT;
            """, token);
        Assert.AreEqual("5000", await nodeB.SafePsqlAsync(Database, "SELECT max(id) - min(id) + 1 FROM t WHERE v = 'load'", token),
            "the span of node_b's load's ids, from the one block it reserved");
    }

    /// <summary>
    /// The sessions that read node_b's neighbors as a drop of its subscription to node_c changes them read them again:
    /// node_b, restarted with node_c down, waits for node_c in the dropping transaction until the drop of b_from_c
    /// commits, as the drop may yet roll back, whole, to a savepoint or out of a PL/pgSQL exception block, and again
    /// once it has; and once a drop commits, a session that already gave up on node_c, and one waiting for it, go on
    /// drawing.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SessionsHearOfSubscriptionDrops()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        await SubscribeAsync(nodeC, "c_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB, nodeC], token);
        await InsertAsync(nodeB, 10, token);
        await InsertAsync(nodeC, 10, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_node WHERE node_name = 'node_c' AND seqoid = 't_id_seq'::regclass)", token), "node_c's report on node_b");
        await nodeC.StopAsync("fast", token);
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);

        async Task WaitsForNodeCAsync(NpgsqlConnection connection, string statements, string what)
        {
            PostgresException waited = await Assert.ThrowsExactlyAsync<PostgresException>(() => ExecuteAsync(connection, statements, token), what);
            Assert.AreEqual("55000", waited.SqlState, $"{what}: {waited.MessageText}");
            Assert.Contains("node_c", waited.MessageText, what);
        }

        await using (NpgsqlConnection rolledBack = await nodeB.OpenAsync(Database, token))
        {
            await ExecuteAsync(rolledBack, "SET statement_timeout = '30s'; BEGIN; SELECT worf.drop_subscription('b_from_c')", token);
            await WaitsForNodeCAsync(rolledBack, "SELECT nextval('t_id_seq') FROM generate_series(1, 20)", "the draws after the drop, in its transaction");
            await ExecuteAsync(rolledBack, "ROLLBACK", token);
            await WaitsForNodeCAsync(rolledBack, "INSERT INTO t (v) VALUES ('after the rollback')", "the insert after the drop rolled back");
        }

        await using (NpgsqlConnection toSavepoint = await nodeB.OpenAsync(Database, token))
        {
            await ExecuteAsync(toSavepoint, "SET statement_timeout = '30s'; BEGIN; SAVEPOINT s; SELECT worf.drop_subscription('b_from_c')", token);
            await WaitsForNodeCAsync(toSavepoint, "SELECT nextval('t_id_seq') FROM generate_series(1, 20)", "the draws after the drop, under its savepoint");
            await ExecuteAsync(toSavepoint, "ROLLBACK TO SAVEPOINT s", token);
            await WaitsForNodeCAsync(toSavepoint, "INSERT INTO t (v) VALUES ('after the savepoint')", "the insert after the drop rolled back to its savepoint");
            await ExecuteAsync(toSavepoint, "ROLLBACK", token);
        }

        await using (NpgsqlConnection excepted = await nodeB.OpenAsync(Database, token))
        {
            await WaitsForNodeCAsync(excepted, """
                SET statement_timeout = '30s';
                DO $$
                BEGIN
                    BEGIN
                        PERFORM worf.drop_subscription('b_from_c');
                        PERFORM nextval('t_id_seq') FROM generate_series(1, 20);
                        RAISE EXCEPTION 'undo the drop';
                    EXCEPTION WHEN raise_exception THEN
                        NULL;
                    END;
                END $$
                """, "the draws after the drop, in an exception block");
        }

        await using NpgsqlConnection gaveUp = await nodeB.OpenAsync(Database, token);
        await WaitsForNodeCAsync(gaveUp, "SET statement_timeout = '30s'; INSERT INTO t (v) VALUES ('before the drop')", "the insert before the drop");
        await using NpgsqlConnection waiting = await nodeB.OpenAsync(Database, token);
        Task waitingInsert = ExecuteAsync(waiting, "SET statement_timeout = '30s'; INSERT INTO t (v) VALUES ('during the drop')", token);
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        Assert.IsFalse(waitingInsert.IsCompleted, "the insert waiting for node_c");
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.drop_subscription('b_from_c')", token);
        Stopwatch dropped = Stopwatch.StartNew();
        await waitingInsert;
        Assert.IsLessThan(TimeSpan.FromSeconds(5), dropped.Elapsed, "the waiting insert, once the drop committed");
        dropped.Restart();
        await ExecuteAsync(gaveUp, "INSERT INTO t (v) VALUES ('after the drop')", token);
        Assert.IsLessThan(TimeSpan.FromSeconds(5), dropped.Elapsed, "the insert after the drop, in the session that gave up on node_c");
    }

    /// <summary>
    /// Has a transaction hold a report that node_c relayed of a sequence, of a node made up for it. An exchange with
    /// node_c, which relays no such node, takes the report away now and then, so it's put back until the transaction
    /// holds it: a session that locked nothing would hold up no drop.
    /// </summary>
    private static async Task HoldRelayedReportAsync(TestNode node, NpgsqlConnection holder, NpgsqlTransaction? holding, string sequence, string via,
        CancellationToken token)
    {
        for (int i = 0; i < 50; i++)
        {
            _ = await node.SafePsqlAsync(Database, $"""
                INSERT INTO worf.sequence_range_node (seqoid, node_id, node_name, via_node, reported_at)
                VALUES ('{sequence}'::regclass, 2000002, 'relayed_by_c', {via}, now()) ON CONFLICT DO NOTHING
                """, token);
            await using NpgsqlCommand hold = new(
                $"SELECT 1 FROM worf.sequence_range_node WHERE seqoid = '{sequence}'::regclass AND node_id = 2000002 FOR UPDATE", holder, holding);
            if (await hold.ExecuteScalarAsync(token) is not null)
            {
                return;
            }
        }

        Assert.Fail("the relayed report was taken away 50 times before a transaction held it");
    }

    /// <summary>
    /// A drop of a subscription that waits for a report another session holds waits for it as for any lock: it fails at
    /// the session's lock_timeout, and a holder that then waits for a lock the dropping transaction holds is a deadlock
    /// PostgreSQL finds, rather than the drop trying again until its statement_timeout.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DroppingSubscriptionsWaitForHeldReportsAsLocks()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE u (id bigserial PRIMARY KEY)", token);
        }

        await SubscribeAsync(nodeA, "a_from_c", nodeC, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('u_id_seq', participants := '{}', range_size := 10)", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database,
            "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'u_id_seq'::regclass", token), "node_a drawing from a block of u_id_seq");
        string nodeCId = await nodeC.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        await using NpgsqlConnection holder = await nodeA.OpenAsync(Database, token);
        await ExecuteAsync(holder, "BEGIN", token);
        await HoldRelayedReportAsync(nodeA, holder, null, "u_id_seq", nodeCId, token);

        await using (NpgsqlConnection dropper = await nodeA.OpenAsync(Database, token))
        {
            Stopwatch dropping = Stopwatch.StartNew();
            PostgresException timedOut = await Assert.ThrowsExactlyAsync<PostgresException>(() =>
                ExecuteAsync(dropper, "SET lock_timeout = '2s'; SET statement_timeout = '30s'; SELECT worf.drop_subscription('a_from_c')", token));
            Assert.AreEqual("55P03", timedOut.SqlState, timedOut.MessageText);
            Assert.IsLessThan(TimeSpan.FromSeconds(10), dropping.Elapsed, "the drop's wait for the held report, at its 2 s lock_timeout");
        }

        // The dropping transaction holds an advisory lock that the holder then waits for.
        await using NpgsqlConnection deadlocked = await nodeA.OpenAsync(Database, token);
        Stopwatch waiting = Stopwatch.StartNew();
        Task drop = ExecuteAsync(deadlocked,
            "SET statement_timeout = '30s'; BEGIN; SELECT pg_advisory_xact_lock(42); SELECT worf.drop_subscription('a_from_c'); COMMIT", token);
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        Task holderWait = ExecuteAsync(holder, "SELECT pg_advisory_xact_lock(42)", token);
        List<string> failures = [];
        foreach (Task waited in new[] { drop, holderWait })
        {
            try
            {
                await waited;
            }
            catch (PostgresException failure)
            {
                failures.Add(failure.SqlState);
            }
        }

        Assert.Contains("40P01", failures, string.Join(", ", failures));
        Assert.IsLessThan(TimeSpan.FromSeconds(15), waiting.Elapsed, "PostgreSQL finding the deadlock between the drop and the holder");
    }

    /// <summary>
    /// A participant's reservation of more than a sixteenth of what an integer sequence has left fails at once, with
    /// 22023, and its inserts go on (2101): the allocator refuses the load though nothing else in the participant's
    /// message changed, and though no free span of its root could hold it.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task OversizedLoadsAreRefusedAtOnce()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE ti (id serial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('ti_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 1000)", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'ti_id_seq'::regclass", token),
            "node_b drawing from ti_id_seq");

        // Once node_b's exchanges with node_a carry nothing else new, its request for the load is all that changes.
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Stopwatch refusing = Stopwatch.StartNew();
        PostgresException tooLarge = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
            "BEGIN; SELECT worf.reserve_sequence_range('ti_id_seq', 3000000000, timeout := '20s')", token));
        Assert.AreEqual("22023", tooLarge.SqlState, "node_b's reservation of more than ti_id_seq has left");
        Assert.IsLessThan(TimeSpan.FromSeconds(15), refusing.Elapsed, "node_b's refused reservation");
        Stopwatch inserting = Stopwatch.StartNew();
        _ = await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '30s'; INSERT INTO ti (v) SELECT 'b' FROM generate_series(1, 2000)", token);
        Assert.IsLessThan(TimeSpan.FromSeconds(15), inserting.Elapsed, "node_b's inserts after the refused reservation");
    }

    /// <summary>
    /// A reservation is served whole on a participant and on the owner, however many of the owner's largest blocks it
    /// takes: node_b and node_a each reserve 20,000 values, more than sixteen blocks of 1,000, and load them. A request for
    /// a pool block that a participant makes up gets no more than sixteen of the owner's largest blocks, the most a pool
    /// asks for: node_b's made-up request for nearly all of the sequence gets 16,000 values, and node_a goes on
    /// inserting. Each grant node_a logs, its grants in the calls that carry those requests too, goes without the call's
    /// statement, which holds the whole message. Five more made-up pool requests take no more than two pools in ten
    /// minutes, and a made-up load of everything left is refused.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ReservationsAreServedWholeAndMadeUpPoolRequestsCapped()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        foreach (TestNode node in new[] { nodeB, nodeA })
        {
            _ = await node.SafePsqlAsync(Database, $"""
                BEGIN;
                SELECT worf.reserve_sequence_range('t_id_seq', 20000);
                INSERT INTO t (v) SELECT '{node.Name}' FROM generate_series(1, 20000);
                COMMIT;
                """, token);
            Assert.AreEqual("20000", await node.SafePsqlAsync(Database, $"SELECT count(*) FROM t WHERE v = '{node.Name}'", token), $"{node.Name}'s reserved load");
        }

        string nodeAId = await nodeA.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        string nodeBId = await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        string bSlot = await nodeB.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription WHERE sub_name = 'b_from_a'", token);
        string[] recorded = (await nodeA.SafePsqlAsync(Database, $"""
            SELECT n.sysid, n.timeline, n.epoch, n.state, coalesce(n.distance::text, 'null'), coalesce(n.high_water::text, 'null'),
                coalesce(n.watermark::text, 'null'), coalesce(pg_catalog.array_to_json(n.holds)::text, 'null'), coalesce(n.clock::text, 'null'),
                r.range_size, r.max_range_size, coalesce(r.root_start::text, 'null'), coalesce(r.root_end::text, 'null')
            FROM worf.sequence_range_node n JOIN worf.sequence_range r ON r.seqoid = n.seqoid
            WHERE n.seqoid = 't_id_seq'::regclass AND n.node_id = {nodeBId}
            """, token)).Split('|');
        string walLsn = await nodeB.SafePsqlAsync(Database, "SELECT pg_current_wal_lsn()", token);
        string requestId = Guid.NewGuid().ToString();
        string madeUp = $$"""
            {"node_id": {{nodeBId}}, "node_name": "node_b", "slot": "{{bSlot}}", "sysid": "{{recorded[0]}}", "timeline": {{recorded[1]}}, "wal_lsn": "{{walLsn}}", "sequences": [
                {"name": "public.t_id_seq", "owner_id": {{nodeAId}}, "epoch": {{recorded[2]}}, "range_size": {{recorded[9]}}, "max_range_size": {{recorded[10]}},
                    "distance": {{recorded[4]}}, "high_water": {{recorded[5]}}, "watermark": {{recorded[6]}}, "state": "{{recorded[3]}}", "root_start": {{recorded[11]}},
                    "root_end": {{recorded[12]}}, "holds": {{recorded[7]}}, "clock": {{recorded[8]}}, "reset": null, "reset_done": null, "held_complete": false,
                    "held": [], "requests": [{"id": "{{requestId}}", "size": 9223372036854770000, "kind": "p", "lo": null, "hi": null}],
                    "reports": [], "grants": [], "acks": []}]}
            """;
        await ExchangeUntilAsync(nodeA, madeUp, async () => await nodeA.SafePsqlAsync(Database,
            $"SELECT count(*) FROM worf.sequence_range_grant WHERE request_id = '{requestId}'", token) != "0", "node_a's grant for the made-up pool request", token);
        Assert.AreEqual("16000", await nodeA.SafePsqlAsync(Database,
            $"SELECT hi - lo + 1 FROM worf.sequence_range_grant WHERE request_id = '{requestId}'", token), "the block node_a granted for the made-up pool request");

        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'after' FROM generate_series(1, 10000)", token);
        Assert.AreEqual("10000", await nodeA.SafePsqlAsync(Database, "SELECT count(*) FROM t WHERE v = 'after'", token), "node_a's rows after the made-up request");
        string log = nodeA.ReadLog();
        Assert.Contains("granted block", log, "node_a's grants");
        Assert.DoesNotContain("STATEMENT:  SELECT worf.sequence_range_exchange(", log, "node_a's log lines of the calls' statements");

        // More made-up pool requests take no more than two pools in ten minutes, node_b's reserve among them (1900); a
        // made-up load of all that node_a has left is refused, and node_a goes on inserting (2100).
        async Task<string> MadeUpAsync(string request) => madeUp
            .Replace($"\"id\": \"{requestId}\", \"size\": 9223372036854770000, \"kind\": \"p\"", request, StringComparison.Ordinal)
            .Replace($"\"wal_lsn\": \"{walLsn}\"", $"\"wal_lsn\": \"{await nodeB.SafePsqlAsync(Database, "SELECT pg_current_wal_lsn()", token)}\"", StringComparison.Ordinal);
        for (int i = 0; i < 5; i++)
        {
            _ = await nodeA.SafePsqlAsync(Database,
                $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{await MadeUpAsync($"\"id\": \"{Guid.NewGuid()}\", \"size\": 16000, \"kind\": \"p\"")}')", token);
        }

        Assert.AreEqual("t", await nodeA.SafePsqlAsync(Database,
            $"SELECT sum(hi - lo + 1) <= 32000 FROM worf.sequence_range_grant WHERE seqoid = 't_id_seq'::regclass AND grantee_id = {nodeBId} AND kind = 'p'", token),
            "the pools node_a granted node_b in ten minutes (1900)");
        string load = Guid.NewGuid().ToString();
        _ = await nodeA.SafePsqlAsync(Database,
            $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{await MadeUpAsync($"\"id\": \"{load}\", \"size\": 9223372036854770000, \"kind\": \"n\", \"reserved\": true")}')", token);
        Assert.AreEqual("x", await nodeA.SafePsqlAsync(Database, $"SELECT string_agg(kind::text, ',') FROM worf.sequence_range_grant WHERE request_id = '{load}'", token),
            "node_a's answer to the made-up load (2100)");
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'after load' FROM generate_series(1, 10000)", token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'b after load' FROM generate_series(1, 1000)", token);
    }

    /// <summary>
    /// A cursor whose rows' calls of nextval first start in a savepoint that rolls back goes on: its next FETCH, and a
    /// PL/pgSQL block's after an exception block it fetched in, draw as the first did.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task CursorsDrawPastRolledBackSavepoints()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE s1;
            SELECT worf.enable_sequence_range('s1', participants := '{}', range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 's1'::regclass", token),
            "s1 active");
        Assert.AreEqual("1\n2", await nodeA.SafePsqlAsync(Database, """
            BEGIN;
            DECLARE c CURSOR FOR SELECT * FROM (VALUES (nextval('s1')), (nextval('s1')), (nextval('s1')), (nextval('s1'))) v (n);
            SAVEPOINT a;
            FETCH 1 FROM c;
            ROLLBACK TO SAVEPOINT a;
            FETCH 1 FROM c;
            COMMIT;
            """, token), "the cursor's rows, fetched across the rolled-back savepoint");
        _ = await nodeA.SafePsqlAsync(Database, """
            DO $$
            DECLARE
                c refcursor := 'c2';
                v bigint;
            BEGIN
                OPEN c FOR SELECT * FROM (VALUES (nextval('s1')), (nextval('s1')), (nextval('s1'))) v (n);
                BEGIN
                    FETCH c INTO v;
                    RAISE EXCEPTION 'fetched %', v;
                EXCEPTION WHEN raise_exception THEN
                    NULL;
                END;
                FETCH c INTO v;
            END $$
            """, token);
    }

    /// <summary>
    /// worf's drop takes the tables its triggers guard before worf's own, so a transaction that holds one as it inserts,
    /// and then reads worf's tables, as an insert's draw does, goes on and commits, and the drop follows it rather than
    /// deadlocking with it; on node_a, which provides a subscription, the drop is refused at once, without waiting for the
    /// table.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task WorfDropsBesideInserts()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);

        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            await using NpgsqlConnection inserting = await node.OpenAsync(Database, token);
            await ExecuteAsync(inserting, "BEGIN; INSERT INTO t (v) VALUES ('held')", token);
            await using NpgsqlConnection dropping = await node.OpenAsync(Database, token);
            Stopwatch dropped = Stopwatch.StartNew();
            Task drop = ExecuteAsync(dropping, "SET statement_timeout = '30s'; DROP EXTENSION worf CASCADE", token);
            if (node == nodeA)
            {
                PostgresException refused = await Assert.ThrowsExactlyAsync<PostgresException>(() => drop);
                Assert.AreEqual("55006", refused.SqlState, refused.MessageText);
                Assert.IsLessThan(TimeSpan.FromSeconds(5), dropped.Elapsed, "node_a's refusal, while an insert holds t");
                await ExecuteAsync(inserting, "COMMIT", token);
                continue;
            }

            await Task.Delay(TimeSpan.FromSeconds(1), token);
            Assert.IsFalse(drop.IsCompleted, "node_b's drop, waiting for the insert");
            await ExecuteAsync(inserting, "SET LOCAL statement_timeout = '10s'; SELECT count(*) FROM worf.sequence_ranges; INSERT INTO t (v) VALUES ('held again'); COMMIT", token);
            await drop;
            Assert.AreEqual("0", await node.SafePsqlAsync(Database, "SELECT count(*) FROM pg_extension WHERE extname = 'worf'", token), "worf on node_b");
            Assert.AreEqual("2", await node.SafePsqlAsync(Database, "SELECT count(*) FROM t WHERE v LIKE 'held%'", token), "node_b's rows from the transaction the drop waited for");
        }
    }

    /// <summary>
    /// worf's drop takes the tables it needs at once or not at all, so it never waits for one while holding another, and
    /// a transaction that read worf's tables, reserved a block or wrote another guarded table before it inserts into t
    /// goes on and commits, the drop following it rather than deadlocking with it; DROP SCHEMA worf CASCADE does too,
    /// and on node_a, which provides a subscription, is refused at once. A drop PostgreSQL refuses, by a role that doesn't
    /// own worf or without CASCADE while the guard's triggers depend on worf, takes no table before it's refused, so an
    /// insert into t goes on beside it. A drop that runs warns that t_id_seq keeps its block's bounds.
    /// </summary>
    /// <param name="how">What the other transaction did first, the schema's drop, or a drop PostgreSQL refuses.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("worf read")]
    [DataRow("reserved")]
    [DataRow("guarded written")]
    [DataRow("schema")]
    [DataRow("not owner")]
    [DataRow("restrict")]
    public async Task WorfDropsTakeTheirTablesAtOnce(string how)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text); CREATE TABLE t2 (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100);
            SELECT worf.enable_sequence_range('t2_id_seq', participants := '{node_b}', range_size := 100);
            """, token);
        await WaitActiveAsync([nodeA, nodeB], token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 2 FROM worf.sequence_ranges WHERE state = 'active'", token),
            "node_b drawing from blocks of t_id_seq and t2_id_seq");
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('first'); INSERT INTO t2 (v) VALUES ('first')", token);

        if (how is "not owner" or "restrict")
        {
            // A reader holds t, and a drop PostgreSQL refuses fails at once, leaving an insert into t free to go on.
            _ = await nodeB.SafePsqlAsync(Database, "CREATE ROLE nobody LOGIN", token);
            await using NpgsqlConnection reading = await nodeB.OpenAsync(Database, token);
            await using NpgsqlTransaction read = await reading.BeginTransactionAsync(token);
            await using (NpgsqlCommand hold = new("SELECT count(*) FROM t", reading, read))
            {
                _ = await hold.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlConnection refusedConnection = await nodeB.OpenAsync(Database, token, how == "not owner" ? "nobody" : "postgres");
            await using NpgsqlCommand refusedDrop = new($"SET statement_timeout = '8s'; DROP EXTENSION worf{(how == "not owner" ? " CASCADE" : "")}", refusedConnection);
            Stopwatch refusing = Stopwatch.StartNew();
            PostgresException refused = await Assert.ThrowsExactlyAsync<PostgresException>(() => refusedDrop.ExecuteNonQueryAsync(token));
            Assert.AreEqual(how == "not owner" ? "42501" : "2BP01", refused.SqlState, refused.MessageText);
            Assert.IsLessThan(TimeSpan.FromSeconds(2), refusing.Elapsed, $"the {how} drop's refusal while a reader holds t");
            Stopwatch inserting = Stopwatch.StartNew();
            _ = await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '3s'; INSERT INTO t (v) VALUES ('beside')", token);
            Assert.IsLessThan(TimeSpan.FromSeconds(2), inserting.Elapsed, "the insert after the refused drop");
            await read.RollbackAsync(token);
            return;
        }

        if (how == "schema")
        {
            // node_a provides b_from_a, and refuses the schema's drop at once, though an insert holds t.
            await using NpgsqlConnection holdingA = await nodeA.OpenAsync(Database, token);
            await using NpgsqlTransaction heldA = await holdingA.BeginTransactionAsync(token);
            await using (NpgsqlCommand insert = new("INSERT INTO t (v) VALUES ('held')", holdingA, heldA))
            {
                _ = await insert.ExecuteNonQueryAsync(token);
            }

            Stopwatch refusing = Stopwatch.StartNew();
            PostgresException providing = await Assert.ThrowsExactlyAsync<PostgresException>(() =>
                nodeA.SafePsqlAsync(Database, "SET statement_timeout = '30s'; DROP SCHEMA worf CASCADE", token));
            Assert.AreEqual("55006", providing.SqlState, providing.MessageText);
            Assert.IsLessThan(TimeSpan.FromSeconds(5), refusing.Elapsed, "node_a's refusal of the schema's drop");
            await heldA.CommitAsync(token);
        }

        // On node_b, a transaction first reads worf's tables, reserves a block or writes t2, then the drop starts, and
        // the transaction inserts into t and commits.
        await using NpgsqlConnection holding = await nodeB.OpenAsync(Database, token);
        await using NpgsqlTransaction held = await holding.BeginTransactionAsync(token);
        await using (NpgsqlCommand first = new(how switch
        {
            "worf read" => "SELECT count(*) FROM worf.sequence_ranges",
            "reserved" => "SELECT worf.reserve_sequence_range('t_id_seq', 50)",
            _ => "INSERT INTO t2 (v) VALUES ('held')",
        }, holding, held))
        {
            _ = await first.ExecuteNonQueryAsync(token);
        }

        int logged = nodeB.ReadLog().Length;
        Task<string> dropping = nodeB.SafePsqlAsync(Database,
            $"SET statement_timeout = '30s'; {(how == "schema" ? "DROP SCHEMA worf CASCADE" : "DROP EXTENSION worf CASCADE")}", token);
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        Assert.IsFalse(dropping.IsCompleted, "node_b's drop, waiting for the transaction");
        await using (NpgsqlCommand then = new("SET LOCAL statement_timeout = '10s'; INSERT INTO t (v) VALUES ('then')", holding, held))
        {
            _ = await then.ExecuteNonQueryAsync(token);
        }

        await held.CommitAsync(token);
        _ = await dropping;
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM pg_extension WHERE extname = 'worf'", token), "worf on node_b");
        Assert.AreEqual("1", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM t WHERE v = 'then'", token), "node_b's row the drop waited for");
        Assert.Contains("sequence public.t_id_seq keeps its bounds", nodeB.ReadLog()[logged..], "node_b's warning of t_id_seq's bounds");
    }

    /// <summary>
    /// worf's drop doesn't deadlock with its range worker, which checks the database's managed sequences as the drop
    /// takes worf's tables, its configuration first: five times, a session draws 8 of a sequence's block of 10, so that
    /// the worker's next check switches it, which reads the configuration, and takes the configuration's and then the
    /// ranges' locks as the drop does, half a second apart, with no deadlock; worf is then dropped.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task WorfDropsBesideItsRangeWorker()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE TABLE busy (id bigserial PRIMARY KEY);
            SELECT worf.enable_sequence_range('busy_id_seq', participants := '{}', range_size := 10, max_range_size := 10);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges", token), "busy_id_seq active");
        int logpos = nodeA.ReadLog().Length;
        for (int round = 0; round < 5; round++)
        {
            _ = await nodeA.SafePsqlAsync(Database, """
                INSERT INTO busy SELECT FROM generate_series(1, 8);
                BEGIN;
                LOCK TABLE worf.local_node IN ACCESS EXCLUSIVE MODE;
                SELECT pg_catalog.pg_sleep(0.5);
                LOCK TABLE worf.sequence_range IN ACCESS EXCLUSIVE MODE;
                ROLLBACK;
                """, token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "DROP EXTENSION worf CASCADE", token);
        Assert.DoesNotContain("deadlock detected", nodeA.ReadLog()[logpos..], "node_a's log");
    }

    /// <summary>
    /// A take-in waits a millisecond for a report row another transaction holds, after the range row it may wait for
    /// longer has come: node_a's open transaction holds node_b's report of t while node_b's calls, which take t's range
    /// row in, ask for blocks, and node_a logs no wait for a report row that outlasts a tenth of a second, where the range
    /// row's wait had left the rest of the sequence's take-in waiting up to a second, in the node's turn.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task TakeInsWaitAMillisecondForHeldReports()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "log_lock_waits = on\ndeadlock_timeout = 100ms");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 10, max_range_size := 10)", token);
        await WaitActiveAsync([nodeA, nodeB], token);

        await using NpgsqlConnection holder = await nodeA.OpenAsync(Database, token);
        await using NpgsqlTransaction holding = await holder.BeginTransactionAsync(token);
        await using (NpgsqlCommand hold = new("SELECT 1 FROM worf.sequence_range_node WHERE node_name = 'node_b' AND seqoid = 't_id_seq'::regclass FOR UPDATE", holder, holding))
        {
            _ = await hold.ExecuteNonQueryAsync(token);
        }

        // node_b draws its blocks out, and asks for more in each call, which can't take its report in, for 5 s.
        int logged = nodeA.ReadLog().Length;
        await using (NpgsqlConnection writer = await nodeB.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand timeout = new("SET statement_timeout = '1s'", writer))
            {
                _ = await timeout.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand insert = new("INSERT INTO t (v) VALUES ('b')", writer);
            Stopwatch drawing = Stopwatch.StartNew();
            while (drawing.Elapsed < TimeSpan.FromSeconds(5))
            {
                try
                {
                    _ = await insert.ExecuteNonQueryAsync(token);
                }
                catch (PostgresException ranOut) when (ranOut.SqlState is "57014" or "2200H")
                {
                    // node_b's next blocks wait for its report to be taken in.
                }
            }
        }

        string[] waits = ReportRowWaits(nodeA.ReadLog()[logged..]);
        await holding.RollbackAsync(token);
        Assert.AreEqual("", string.Join("\n", waits), "node_a's waits for node_b's held report");
    }

    /// <summary>
    /// A neighbor that drops its copy of a sequence's table, and so stops carrying the sequence, holds no restarted node's
    /// draws up: node_c, a participant that node_b subscribes to and that calls node_a, drops t, and node_b and node_a,
    /// each restarted with nothing restored, insert within a few seconds, rather than wait for node_c to check their
    /// reports of a sequence it no longer has; though node_b hears node_c's old report relayed by node_a, which node_c
    /// can't reach as it drops t, after node_c's own message went without it, and node_a relays it until node_c is back.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task NeighborsThatDropTheSequenceHoldNoStartUp()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        await SubscribeAsync(nodeC, "c_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB, nodeC], token);
        await InsertAsync(nodeB, 10, token);
        await InsertAsync(nodeC, 10, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_node WHERE node_name = 'node_c' AND seqoid = 't_id_seq'::regclass)", token), "node_c's report on node_b");

        // node_c drops t while it can't reach node_a, which goes on relaying node_c's report to node_b after node_b took
        // in node_c's message without t.
        string ToNodeA(string dsn) => $"""
            UPDATE worf.node_interface SET if_dsn = '{dsn}' WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_a')
            """;
        int unreached = nodeC.ReadLog().Length;
        _ = await nodeC.SafePsqlAsync(Database, ToNodeA($"host=127.0.0.1 port=1 dbname={Database} user={SuperUser}"), token);
        Assert.IsTrue(await nodeC.PollLogAsync(unreached, "exchange with node node_a failed", token), "node_c's call to node_a, which it can't reach");
        _ = await nodeC.SafePsqlAsync(Database, "DROP TABLE t", token);
        const string NodeCReports = "SELECT count(*) FROM worf.sequence_range_node WHERE node_name = 'node_c' AND seqoid = 't_id_seq'::regclass";
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"SELECT ({NodeCReports}) = 0", token), "node_b taking in node_c's message without t");
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, NodeCReports, token), "node_b's reports of node_c, after node_a relayed node_c's old one");
        Assert.AreEqual("1", await nodeA.SafePsqlAsync(Database, NodeCReports, token), "node_a's report of node_c, while node_c can't reach it");

        // node_b restarts; node_c reaches node_a again, which takes in node_c's message without t, and restarts.
        foreach (TestNode node in new[] { nodeB, nodeA })
        {
            if (node == nodeA)
            {
                _ = await nodeC.SafePsqlAsync(Database, ToNodeA(nodeA.Dsn(Database, SuperUser)), token);
                Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT ({NodeCReports}) = 0", token), "node_a taking in node_c's message without t");
            }

            await node.StopAsync("fast", token);
            await node.StartServerAsync(token);
            await using NpgsqlConnection writer = await node.OpenAsync(Database, token);
            await using NpgsqlCommand insert = new("SET statement_timeout = '8s'; INSERT INTO t (v) VALUES ('after')", writer);
            _ = await insert.ExecuteNonQueryAsync(token);
        }
    }

    /// <summary>
    /// A subscriber that waits out an apply delay longer than the provider's wal_sender_timeout keeps its stream: it
    /// answers the walsender's requests for a reply as it reads ahead, and replies each status interval, so that the
    /// walsender doesn't end the stream, here over 30 seconds of a 40-second delay with wal_sender_timeout 10 s, and the
    /// row comes once the delay is over.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DelayedSubscribersKeepTheirStream()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "wal_sender_timeout = 10s");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id int PRIMARY KEY)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.replication_set_add_table('default', 't')", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token, applyDelay: "40s");
        const string Walsenders = "SELECT string_agg(pid::text, ',' ORDER BY pid) FROM pg_catalog.pg_stat_replication";
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT count(*) = 1 FROM pg_catalog.pg_stat_replication", token), "node_b's stream");
        string streaming = await nodeA.SafePsqlAsync(Database, Walsenders, token);
        int logged = nodeA.ReadLog().Length;
        Stopwatch delayed = Stopwatch.StartNew();
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t VALUES (1)", token);
        await Task.Delay(TimeSpan.FromSeconds(30), token);
        Assert.AreEqual(streaming, await nodeA.SafePsqlAsync(Database, Walsenders, token), "node_a's walsender to node_b, 30 s into the delay");
        Assert.DoesNotContain("replication timeout", nodeA.ReadLog()[logged..], "node_a's log");
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 1 FROM t", token), "the row, once the delay is over");
        Assert.IsGreaterThanOrEqualTo(TimeSpan.FromSeconds(39), delayed.Elapsed, "the row's delay");
    }

    /// <summary>
    /// A neighbor that draws from many sequences, here twenty times a second from each of ten, makes each of its calls
    /// record how far it drew in one statement, which uses one transaction ID, rather than take each sequence in in full.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DrawsCostCallsOneStatement()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "shared_preload_libraries = 'worf,pg_stat_statements'\npg_stat_statements.track = all");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        const string Sequences = "DO $$ BEGIN FOR g IN 1..10 LOOP EXECUTE format('CREATE SEQUENCE many_%s', lpad(g::text, 2, '0')); END LOOP; END $$";
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, Sequences, token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "CREATE EXTENSION pg_stat_statements", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT count(worf.enable_sequence_range(format('many_%s', lpad(g::text, 2, '0'))::regclass, participants := '{node_b}', range_size := 1000)) FROM generate_series(1, 10) g",
            token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 10 FROM worf.sequence_ranges WHERE state = 'active' AND next_lo IS NOT NULL", token),
            "node_b drawing from blocks of the ten sequences");

        using CancellationTokenSource stopDrawing = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task drawing = Task.Run(async () =>
        {
            await using NpgsqlConnection busy = await nodeB.OpenAsync(Database, token);
            await using NpgsqlCommand draw = new(string.Join("; ", Enumerable.Range(1, 10).Select(g => $"SELECT nextval('many_{g:00}')")), busy);
            while (!stopDrawing.IsCancellationRequested)
            {
                _ = await draw.ExecuteNonQueryAsync(token);
                await Task.Delay(TimeSpan.FromMilliseconds(50), token);
            }
        }, token);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(3), token);
            const string NextXid = "SELECT pg_catalog.pg_snapshot_xmax(pg_catalog.pg_current_snapshot())";
            _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_stat_statements_reset()", token);
            long firstXid = long.Parse(await nodeA.SafePsqlAsync(Database, NextXid, token), CultureInfo.InvariantCulture);
            await Task.Delay(TimeSpan.FromSeconds(5), token);
            long lastXid = long.Parse(await nodeA.SafePsqlAsync(Database, NextXid, token), CultureInfo.InvariantCulture);
            // The statements each call ran, which pg_stat_statements counts as nested in the call's.
            string[] counts = (await nodeA.SafePsqlAsync(Database, """
                SELECT coalesce(sum(calls) FILTER (WHERE toplevel AND query LIKE 'SELECT worf.sequence_range_exchange(%'), 0) || '|' ||
                    coalesce(sum(calls) FILTER (WHERE NOT toplevel), 0)
                FROM pg_stat_statements
                """, token)).Split('|');
            long calls = long.Parse(counts[0], CultureInfo.InvariantCulture);
            long statements = long.Parse(counts[1], CultureInfo.InvariantCulture);
            string often = await nodeA.SafePsqlAsync(Database, """
                SELECT string_agg(calls || ' ' || left(regexp_replace(query, '\s+', ' ', 'g'), 150), E'\n' ORDER BY calls DESC) FROM (
                    SELECT calls, query FROM pg_stat_statements WHERE NOT toplevel ORDER BY calls DESC LIMIT 8) s
                """, token);
            Assert.IsTrue(calls > 3 && lastXid - firstXid < 3 * calls && statements < 150 * calls,
                $"node_b's {calls} calls in 5 s, which used {lastXid - firstXid} transaction IDs and ran {statements} statements on node_a, most often:\n{often}");
        }
        finally
        {
            await stopDrawing.CancelAsync();
            await drawing;
        }
    }

    /// <summary>
    /// The range worker checks in full a sequence whose ranges nothing changed only every ten check intervals, however
    /// another sequence's ranges change: here as node_a draws through t_id_seq's blocks of ten, and resets t_id_seq, with
    /// u_seq, which shares t_id_seq's counter of changes under a hash of 1,024 counters, idle.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task OtherSequencesChangesDontCount()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "log_min_messages = debug2");
        _ = await nodeA.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);

        // Sequences are made until one's counter would be t_id_seq's, as the hash of the database and the sequence
        // gives it.
        uint dboid = uint.Parse(await nodeA.SafePsqlAsync(Database, "SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database()", token), CultureInfo.InvariantCulture);
        uint tid = uint.Parse(await nodeA.SafePsqlAsync(Database, "SELECT 't_id_seq'::regclass::oid", token), CultureInfo.InvariantCulture);
        static ulong Slot(uint dboid, uint seqoid) => ((dboid * 2654435761UL) ^ seqoid) % 1024;
        uint u = 0;
        for (int round = 0; u == 0; round++)
        {
            Assert.IsLessThan(20, round, "a sequence that shares t_id_seq's counter");
            string made = await nodeA.SafePsqlAsync(Database, $$"""
                DO $$ BEGIN FOR g IN 1..512 LOOP EXECUTE format('CREATE SEQUENCE try_{{round}}_%s', g); END LOOP; END $$;
                SELECT string_agg(c.oid::text, ',') FROM pg_catalog.pg_class c WHERE c.relname LIKE 'try\_{{round}}\_%'
                """, token);
            u = made.Split('\n')[^1].Split(',').Select(oid => uint.Parse(oid, CultureInfo.InvariantCulture)).FirstOrDefault(oid => Slot(dboid, oid) == Slot(dboid, tid));
        }

        _ = await nodeA.SafePsqlAsync(Database, $$"""
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{}', range_size := 10, max_range_size := 10);
            SELECT worf.enable_sequence_range({{u.ToString(CultureInfo.InvariantCulture)}}::regclass, participants := '{}', range_size := 100);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT count(*) = 2 FROM worf.sequence_ranges WHERE state = 'active'", token), "both sequences active");
        await Task.Delay(TimeSpan.FromSeconds(2), token);

        // For ten seconds node_a draws through t_id_seq's blocks, and then resets it twenty times.
        string Checked(uint seqoid) => $"checking sequence {seqoid.ToString(CultureInfo.InvariantCulture)} in full";
        int logpos = nodeA.ReadLog().Length;
        Stopwatch drawing = Stopwatch.StartNew();
        while (drawing.Elapsed < TimeSpan.FromSeconds(10))
        {
            _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'a' FROM generate_series(1, 5)", token);
            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
        }

        for (int i = 0; i < 20; i++)
        {
            _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('t_id_seq')", token);
            await Task.Delay(TimeSpan.FromMilliseconds(250), token);
        }

        // The idle sequence is checked in full once every ten check intervals, and at most once a second, over however
        // long the draws and resets took, as a slow runner's psql stretches them.
        string log = nodeA.ReadLog()[logpos..];
        TimeSpan took = drawing.Elapsed;
        int interval = int.Parse(await nodeA.SafePsqlAsync(Database, "SELECT setting FROM pg_settings WHERE name = 'worf.sequence_range_check_interval'", token), CultureInfo.InvariantCulture);
        int rested = (int)(took.TotalMilliseconds / Math.Max(1000, 10 * interval)) + 2;
        int own = log.Split(Checked(tid)).Length - 1;
        int other = log.Split(Checked(u)).Length - 1;
        Assert.IsTrue(own > 40 && other <= rested,
            $"the full checks of t_id_seq, {own}, and of the idle sequence that shares its counter under a hash, {other}, at most {rested}, over {took.TotalSeconds:F1} s");
    }

    /// <summary>
    /// Calls cut short go on where the last one stopped: with every sequence asking for a block on every call, as a node
    /// that draws each small block through as soon as it has it keeps them, and each grant costing the owner time, the
    /// sequence whose name sorts last still has its root started, as the calls take the rest in turn.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task CallsCutShortGoOnWhereTheyStopped()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "log_min_messages = debug1");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        const string Sequences = "CREATE TABLE slow_on (); CREATE SEQUENCE zz_last; DO $$ BEGIN FOR g IN 1..20 LOOP EXECUTE format('CREATE SEQUENCE many_%s', lpad(g::text, 2, '0')); END LOOP; END $$";
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, Sequences, token);
        }

        // Once it's turned on, each grant node_a makes costs it 0.6 s, so that a call of 9 s makes fewer than half the 20
        // asked for, and the sequences it leaves, which ask again on the next call, are more than that call makes.
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE FUNCTION slow_grant() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT FROM public.slow_on) THEN PERFORM pg_sleep(0.6); END IF; RETURN NEW; END $$;
            CREATE TRIGGER slow_grant BEFORE INSERT ON worf.sequence_range_grant FOR EACH ROW EXECUTE FUNCTION slow_grant();
            """, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT count(worf.enable_sequence_range(format('many_%s', lpad(g::text, 2, '0'))::regclass, participants := '{node_b}', range_size := 10, max_range_size := 10)) FROM generate_series(1, 20) g",
            token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 20 FROM worf.sequence_ranges WHERE state = 'active'", token),
            "node_b drawing from blocks of the 20 sequences");

        // node_b draws each sequence's block through soon after it has one, on a connection for each, so that every
        // sequence asks for another on every call, which it gives 9 s, of which node_a takes in for a third, leaving the
        // rest for its answer.
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_timeout = '9s'", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        _ = await nodeA.SafePsqlAsync(Database, "ALTER SYSTEM SET log_min_duration_statement = 0", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO slow_on DEFAULT VALUES", token);
        using CancellationTokenSource drawing = CancellationTokenSource.CreateLinkedTokenSource(token);
        async Task DrawAsync(int sequence)
        {
            await using NpgsqlConnection connection = await nodeB.OpenAsync(Database, drawing.Token);
            await using (NpgsqlCommand timeout = new("SET statement_timeout = '100ms'", connection))
            {
                _ = await timeout.ExecuteNonQueryAsync(drawing.Token);
            }

            await using NpgsqlCommand draw = new(string.Create(CultureInfo.InvariantCulture,
                $"SELECT count(nextval('many_{sequence:D2}')) FROM generate_series(1, 20)"), connection);
            while (!drawing.IsCancellationRequested)
            {
                try
                {
                    _ = await draw.ExecuteScalarAsync(drawing.Token);
                }
                catch (PostgresException failed) when (failed.SqlState is "57014" or "55000" or "2200H")
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(300), drawing.Token);
                }
            }
        }

        Task draws = Task.WhenAll(Enumerable.Range(1, 20).Select(DrawAsync));
        try
        {
            int logpos = nodeA.ReadLog().Length;
            _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('zz_last', participants := '{node_b}', range_size := 1000)", token);
            Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'zz_last'::regclass", token),
                "node_b drawing from a block of zz_last, whose name sorts last");
            string log = nodeA.ReadLog()[logpos..];
            Assert.Contains("sequences to its next call", log, "node_a's calls, cut short");

            // A call cut at a third of its 9 s makes five grants of 0.6 s each, and the sixth it may have under way then;
            // one cut at half would make eight. The count holds however slow the machine, which only has each grant take
            // longer, where the calls' durations, which were compared with 4.2 s, depend on it (2304).
            Dictionary<string, int> granting = [];
            int most = 0;
            foreach (string line in log.Split('\n'))
            {
                string[] fields = line.Split('[', ']');
                string process = fields.Length > 3 ? fields[3] : "";
                if (line.Contains(" granted block ", StringComparison.Ordinal) && line.Contains(" to node node_b", StringComparison.Ordinal))
                {
                    granting[process] = granting.GetValueOrDefault(process) + 1;
                }
                else if (line.Contains("statement: SELECT worf.sequence_range_exchange(", StringComparison.Ordinal))
                {
                    most = Math.Max(most, granting.GetValueOrDefault(process));
                    granting[process] = 0;
                }
            }

            Assert.IsGreaterThanOrEqualTo(2, most, "the most grants one of node_a's calls made");
            Assert.IsLessThanOrEqualTo(6, most, "the most grants one of node_a's calls made");
        }
        finally
        {
            await drawing.CancelAsync();
            _ = await Task.WhenAny(draws);
        }
    }

    /// <summary>
    /// A restore that first starts in single-user mode, writing and checkpointing there before it starts again, counts that
    /// start, as RestoredNodesWaitForCallsThatTakeEverySequenceIn's other rows show of theirs. Windows runs no single-user
    /// server under the CI runners' administrator account, which pg_ctl's servers drop.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public Task RestoredNodesCountSingleUserStarts() => RestoredNodesWaitForCallsThatTakeEverySequenceIn("twice single");

    /// <summary>
    /// A restore whose start neither worf_starts.tmp, a directory, nor worf_starts, at mode 000, can keep draws nothing
    /// while it runs, and leaves worf_starts.lost, so that once both are put right while it's down, and it starts again
    /// past the position its grantor heard, the next start counts the starts before it as lost, as
    /// RestoredNodesWaitForCallsThatTakeEverySequenceIn's other rows show of theirs. File modes are Unix's.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public Task RestoredNodesCountStartsTheirFileCouldNotKeep() => RestoredNodesWaitForCallsThatTakeEverySequenceIn("twice unkept");

    /// <summary>
    /// A restore whose start neither worf_starts nor worf_starts.tmp can keep, and that can't leave worf_starts.lost, a
    /// directory too, removes worf_starts instead, so that once all three are put right while it's down the next start
    /// counts the starts before it as lost all the same. File modes are Unix's.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public Task RestoredNodesCountStartsNoFileCouldMark() => RestoredNodesWaitForCallsThatTakeEverySequenceIn("twice unmarked");

    /// <summary>
    /// Has a restored node insert until it either fails with 55000 naming the neighbor it waits for, as the neighbor that
    /// heard of its draws since the backup is down, or draws after it reset at another node's word, from new blocks, which
    /// is as safe; until then it may wait for its grantor, starting up.
    /// </summary>
    /// <param name="node">The restored node.</param>
    /// <param name="waitedFor">The names the neighbor that's down may go by in the wait's message.</param>
    /// <param name="restored">The length of the node's log at its restore.</param>
    /// <param name="token">The cancellation token.</param>
    /// <returns>The ids drawn after a reset, or none.</returns>
    private static async Task<HashSet<string>> WaitsOrResetsAsync(TestNode node, string[] waitedFor, int restored, CancellationToken token)
    {
        Stopwatch waiting = Stopwatch.StartNew();
        while (true)
        {
            string? waited = null;
            HashSet<string> drawn = [];
            try
            {
                drawn = [.. (await node.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'after' FROM generate_series(1, 20) RETURNING id", token)).Split('\n')];
            }
            catch (PostgresException failed) when (failed.SqlState == "55000")
            {
                waited = failed.MessageText;
            }

            if (waited is null)
            {
                Assert.Contains("abandons its blocks", node.ReadLog()[restored..], $"{node.Name} drawing only once it reset");
                return drawn;
            }

            if (waitedFor.Any(name => waited.Contains($"node {name}", StringComparison.Ordinal)))
            {
                return [];
            }

            Assert.IsLessThan(TimeSpan.FromSeconds(30), waiting.Elapsed, $"{node.Name}'s wait: {waited}");
        }
    }

    /// <summary>
    /// A node restored from a base backup taken as it ran, whose WAL ends past the last position its grantor's tables
    /// recorded, as a snapshot of a running server's storage does, or ends where they recorded it, draws from a sequence's
    /// blocks only once its grantor has taken in its report of that sequence since it started, and seen its WAL position
    /// move back: though the grantor is so slow that each call leaves most sequences out, and the node's own WAL then
    /// passes the position it had before the restore; though a prepared transaction, or an open one, holds the sequence's
    /// range row on the grantor as the node draws, so that only the grantor's shared memory hears of the draws, and after
    /// the restore, while the node draws from another sequence, which the grantor took in; though the grantor's calls fail
    /// after taking sequences in, while such a transaction holds the row; and though the grantor restarts while the node is
    /// restored, and the node writes and checkpoints before the grantor is back, or writes and restarts once more, so
    /// that the start it reports began past the position the grantor heard; though the grantor restarts after the draws
    /// it held the row through, before the restore; and though the node only answers the grantor's calls, and the grantor
    /// isn't in its worf.node; and though the file of the node's starts couldn't be written as it was renamed in place at
    /// the restore's start, or was deleted after it, or the restore first started in single-user mode, writing and
    /// checkpointing there. The node's draws after the backup move its report, so that the grantor records a WAL
    /// position past them. So the node never draws again the values it drew before the restore.
    /// </summary>
    /// <param name="how">How the grantor is held up: slow, holding the row in a prepared transaction or an open one,
    /// failing, restarted, or restarted while the node goes back to a whole backup, after which it writes nothing, or
    /// writes and restarts; "stopped" after the draws until the restore; "answered" as the node only answers; the twice
    /// row with worf_starts.tmp a directory at the restore's start, worf_starts deleted after it, or the restore's first
    /// start in single-user mode.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("cut")]
    [DataRow("held")]
    [DataRow("locked")]
    [DataRow("failing")]
    [DataRow("restarted")]
    [DataRow("plain")]
    [DataRow("twice")]
    [DataRow("twice tmp")]
    [DataRow("twice deleted")]
    [DataRow("held stopped")]
    [DataRow("locked stopped")]
    [DataRow("locked stopped written")]
    [DataRow("answered held")]
    [DataRow("answered plain")]
    public async Task RestoredNodesWaitForCallsThatTakeEverySequenceIn(string how)
    {
        CancellationToken token = TestContext.CancellationToken;
        bool answered = how.StartsWith("answered ", StringComparison.Ordinal);
        bool written = how.EndsWith(" written", StringComparison.Ordinal);
        how = written ? how[..^" written".Length] : how;
        bool stopped = how.EndsWith(" stopped", StringComparison.Ordinal);
        string variant = how.StartsWith("twice ", StringComparison.Ordinal) ? how["twice ".Length..] : "";
        string kind = variant.Length > 0 ? "twice" : how.Replace("answered ", "", StringComparison.Ordinal).Replace(" stopped", "", StringComparison.Ordinal);
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "log_min_messages = debug1\nmax_prepared_transactions = 2");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);

        // The calls cut short need many sequences to take in, the held row one besides t_id_seq to draw from, and the
        // failing calls one to take in, which fails them, while t_id_seq is held.
        int extra = kind switch { "cut" => 15, "held" or "locked" or "failing" => 1, _ => 0 };
        string tables = string.Create(CultureInfo.InvariantCulture,
            $"CREATE TABLE slow_on (); CREATE TABLE t (id bigserial PRIMARY KEY, v text); DO $$ BEGIN FOR g IN 1..{extra} LOOP EXECUTE format('CREATE SEQUENCE many_%s', lpad(g::text, 2, '0')); END LOOP; END $$");
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, tables, token);
        }

        // Once it's turned on, each report node_a takes in costs it 0.2 s, and a call's third of its 5 s takes in 8 of the
        // 16 that change, t_id_seq's, which sorts last, among those it leaves.
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE FUNCTION slow_report() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT FROM public.slow_on) THEN PERFORM pg_sleep(0.2); END IF; RETURN NEW; END $$;
            CREATE TRIGGER slow_report BEFORE UPDATE ON worf.sequence_range_node FOR EACH ROW EXECUTE FUNCTION slow_report();
            """, token);
        // node_b calls node_a, its grantor, or, for the answered rows, only answers node_a's calls, and node_a isn't in
        // its worf.node.
        if (answered)
        {
            await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        }
        else
        {
            await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        }

        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 1000);
            """, token);
        _ = await nodeA.SafePsqlAsync(Database, string.Create(CultureInfo.InvariantCulture, $$"""
            SELECT count(worf.enable_sequence_range(format('many_%s', lpad(g::text, 2, '0'))::regclass, participants := '{node_b}', range_size := 1000)) FROM generate_series(1, {{extra}}) g
            """), token);
        await WaitActiveAsync([nodeA, nodeB], token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"SELECT count(*) = {extra + 1} FROM worf.sequence_ranges WHERE state = 'active'", token),
            "node_b drawing from blocks of every sequence");

        // node_b's inserts wait, failing at their time, until node_a answers it once it has started, and after a restore,
        // until a call has taken every sequence in, and then for the block that follows the reset, failing at the end of
        // the quarantine until it comes.
        async Task<HashSet<string>> InsertIdsAsync(string label)
        {
            Stopwatch waiting = Stopwatch.StartNew();
            while (true)
            {
                Assert.IsLessThan(TestNode.PollTimeout, waiting.Elapsed, $"node_b's insert {label}");
                try
                {
                    return [.. (await nodeB.SafePsqlAsync(Database, $"INSERT INTO t (v) SELECT '{label}' FROM generate_series(1, 20) RETURNING id", token)).Split('\n')];
                }
                catch (PostgresException waited) when (waited.SqlState is "55000" or "57014" or "2200H")
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(200), token);
                }
            }
        }

        // node_b writes, which node_a hears of, and is backed up as it runs, after which node_a's tables record its
        // position, as the backup's segment switches move it a segment: whole for the plain and twice rows, where the
        // position they record is where the backup's WAL ends, and otherwise its data files first, after which node_b
        // writes on, and then its WAL, as a snapshot of its storage keeps both. node_b draws 20 ids from the block it
        // holds, which node_a hears of, and goes back to the backup, whose WAL ends at or past every position node_a's
        // tables recorded before the draws.
        const string HeardFromB = """
            SELECT worf.sequence_range_heard(node_id, true, '-infinity', false)::text FROM worf.sequence_range_node
            WHERE node_name = 'node_b' AND seqoid = 't_id_seq'::regclass
            """;
        async Task HearFromBAsync(string what)
        {
            string heard = await nodeA.SafePsqlAsync(Database, "SELECT clock_timestamp()::text", token);
            for (int call = 0; call < 2; call++)
            {
                Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT ({HeardFromB})::timestamptz > '{heard}'", token), $"node_a hearing from node_b {what}");
                heard = await nodeA.SafePsqlAsync(Database, HeardFromB, token);
            }
        }

        using TemporaryDirectory copies = new($"worf-sequence-range-slow-restore-{Guid.NewGuid():N}");
        string copy = Path.Join(copies.FullName, "node_b");
        _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE written AS SELECT g, repeat('x', 100) AS v FROM generate_series(1, 30000) g", token);
        await HearFromBAsync("after its writes");
        await BackUpDataAsync(nodeB, copy, token, wal: kind is "plain" or "twice");
        if (answered)
        {
            CopySlots(nodeB, copy);
        }

        await HearFromBAsync("past the backup");
        if (kind is not ("plain" or "twice"))
        {
            _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE written_more AS SELECT g, repeat('x', 100) AS v FROM generate_series(1, 200) g", token);
            CopyWal(nodeB, copy);
            if (answered)
            {
                CopySlots(nodeB, copy);
            }
        }

        // node_a holds t_id_seq's range row as node_b draws, in a prepared transaction or an open one, so that its tables
        // record nothing of the draws, which only shared memory hears of.
        const string LockRow = "SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass FOR NO KEY UPDATE";
        if (kind is "held" or "failing")
        {
            _ = await nodeA.SafePsqlAsync(Database, $"BEGIN; {LockRow}; PREPARE TRANSACTION 'held_t'", token);
        }

        await using NpgsqlConnection? locker = kind == "locked" ? await nodeA.OpenAsync(Database, token) : null;
        await using NpgsqlTransaction? locking = locker is null ? null : await locker.BeginTransactionAsync(token);
        if (locker is not null)
        {
            await using NpgsqlCommand lockRow = new(LockRow, locker, locking);
            _ = await lockRow.ExecuteScalarAsync(token);
        }

        // For the written row, node_b's calls fail as it draws and writes a WAL segment, so that the call that carries
        // its draws reports the segment too, which node_a takes in in full, waiting for the row it can't take and
        // leaving the sequence out, with only the position the call kept for it in the tables.
        if (written)
        {
            _ = await nodeA.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange(text) RENAME TO sequence_range_exchange_paused", token);
        }

        HashSet<string> before = await InsertIdsAsync("before");
        if (written)
        {
            _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE written_segment AS SELECT g, repeat('x', 100) AS v FROM generate_series(1, 200000) g", token);
            _ = await nodeA.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange_paused(text) RENAME TO sequence_range_exchange", token);
            Assert.IsTrue(await nodeA.PollLogAsync(0, "left out sequence public.t_id_seq, whose range row another transaction holds", token),
                "node_a leaving t_id_seq out of node_b's call");
        }

        await HearFromBAsync("after its draws");

        // node_a stops and starts again only once node_b is restored, so that its shared memory forgets what node_b's
        // messages told it; the open transaction ends with it, and the prepared one holds on.
        if (stopped)
        {
            await nodeA.StopAsync("fast", token);
        }

        int logpos = nodeA.ReadLog().Length;
        switch (stopped ? "" : kind)
        {
            case "cut":
                _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO slow_on DEFAULT VALUES", token);
                break;
            case "" or "held" or "locked":
                break;
            case "failing":
                // node_a's calls fail as they commit, after they have taken node_b's reports in.
                _ = await nodeA.SafePsqlAsync(Database, """
                    CREATE TABLE fail_on ();
                    CREATE FUNCTION fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN IF EXISTS (SELECT FROM public.fail_on) THEN RAISE EXCEPTION 'worf-test-failing-call'; END IF; RETURN NULL; END $$;
                    CREATE CONSTRAINT TRIGGER fail_commit AFTER INSERT OR UPDATE ON worf.sequence_range_node DEFERRABLE INITIALLY DEFERRED
                        FOR EACH ROW EXECUTE FUNCTION fail_commit();
                    INSERT INTO fail_on DEFAULT VALUES;
                    """, token);
                break;
            default:
                await nodeA.StopAsync("fast", token);
                break;
        }

        // The restore's start can't rename worf_starts.tmp, a directory, in place, or runs in single-user mode, writing
        // and checkpointing there, before it starts again.
        const string WrittenAgain = "CREATE TABLE written_again AS SELECT g, repeat('x', 100) AS v FROM generate_series(1, 8000) g";
        if (variant is "tmp" or "unkept" or "unmarked")
        {
            _ = Directory.CreateDirectory(Path.Join(copy, "worf_starts.tmp"));
        }

        if (variant == "unmarked")
        {
            _ = Directory.CreateDirectory(Path.Join(copy, "worf_starts.lost"));
        }

        int restoredAt = nodeB.ReadLog().Length;
        await RestoreDataAsync(nodeB, copy, token, beforeStart: variant switch
        {
            "single" => async () =>
            {
                (int exit, string output) = await nodeB.RunSingleUserAsync(Database, $"{WrittenAgain}\nCHECKPOINT\n", token);
                Assert.AreEqual(0, exit, output);
            },
            "unkept" or "unmarked" => () =>
            {
                if (!OperatingSystem.IsWindows())
                {
                    File.SetUnixFileMode(Path.Join(nodeB.Cluster.DataDirectory, "worf_starts"), UnixFileMode.None);
                }

                return Task.CompletedTask;
            },
            _ => null,
        });
        if (stopped)
        {
            await nodeA.StartServerAsync(token);
        }

        if (kind == "cut")
        {
            // node_b's own WAL passes the position it had before the restore before the calls take t_id_seq in.
            _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE written_again AS SELECT g, repeat('x', 100) AS v FROM generate_series(1, 60000) g", token);
        }
        else if (answered && kind == "held")
        {
            // node_b's own WAL passes the position it had before the restore, and the row is let go 3 s later.
            _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE written_again AS SELECT g, repeat('x', 100) AS v FROM generate_series(1, 60000) g", token);
            await Task.Delay(TimeSpan.FromSeconds(3), token);
            _ = await nodeA.SafePsqlAsync(Database, "COMMIT PREPARED 'held_t'", token);
        }
        else if (kind == "held" || (kind == "locked" && !stopped))
        {
            // Another sequence, which node_a took in, is drawn from while t_id_seq's row is held.
            Stopwatch drawing = Stopwatch.StartNew();
            while (await nodeB.PsqlAsync(Database, "SET statement_timeout = '2s'; SELECT nextval('many_01')", token) is false)
            {
                Assert.IsLessThan(TimeSpan.FromSeconds(60), drawing.Elapsed, "node_b drawing from many_01 while node_a holds t_id_seq's range row");
            }

            Assert.IsFalse(await nodeB.PsqlAsync(Database, "SET statement_timeout = '3s'; INSERT INTO t (v) VALUES ('held')", token),
                "node_b's insert into t while node_a holds t_id_seq's range row");
            if (locking is not null)
            {
                await locking.CommitAsync(token);
            }
            else
            {
                _ = await nodeA.SafePsqlAsync(Database, "COMMIT PREPARED 'held_t'", token);
            }
        }
        else if (kind == "failing")
        {
            // node_b's own WAL passes the positions node_a's tables recorded before the restore, and node_a's calls go on
            // failing a while, after taking many_01 in and leaving t_id_seq out, and then the row is let go.
            _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE written_again AS SELECT g, repeat('x', 100) AS v FROM generate_series(1, 10000) g", token);
            await Task.Delay(TimeSpan.FromSeconds(5), token);
            _ = await nodeA.SafePsqlAsync(Database, "COMMIT PREPARED 'held_t'", token);
            await Task.Delay(TimeSpan.FromSeconds(2), token);
            _ = await nodeA.SafePsqlAsync(Database, "DELETE FROM fail_on", token);
            Assert.Contains("worf-test-failing-call", nodeB.ReadLog(), "node_b's calls to node_a, failing as node_a committed them");
        }
        else if (kind == "restarted")
        {
            // node_b writes, and a checkpoint moves its redo point past its old WAL position, before node_a is back.
            _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE written_again AS SELECT g, repeat('x', 100) AS v FROM generate_series(1, 8000) g; CHECKPOINT", token);
            await nodeA.StartServerAsync(token);
        }
        else if (kind == "plain")
        {
            await nodeA.StartServerAsync(token);
        }
        else if (kind == "twice")
        {
            // node_b writes and restarts, so that the start it reports began past its old WAL position, before node_a is
            // back, with worf_starts.tmp writable again, or worf_starts deleted first.
            if (variant == "tmp")
            {
                Directory.Delete(Path.Join(nodeB.Cluster.DataDirectory, "worf_starts.tmp"));
            }
            else if (variant == "deleted")
            {
                File.Delete(Path.Join(nodeB.Cluster.DataDirectory, "worf_starts"));
            }
            else if (variant is "unkept" or "unmarked" && !OperatingSystem.IsWindows())
            {
                // node_b, whose start neither file keeps, draws nothing, and both are put right while it's down. One that
                // can't leave worf_starts.lost either removes worf_starts, which worf's supervisor may write again, with
                // the start, as it next wakes.
                Assert.IsTrue(await nodeB.PollLogAsync(restoredAt, "could not keep this server's start", token), "node_b's start, unkept");
                if (variant == "unkept")
                {
                    PostgresException unkept = await Assert.ThrowsExactlyAsync<PostgresException>(
                        () => nodeB.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('unkept')", token), "node_b's insert while its start is unkept");
                    Assert.AreEqual("55000", unkept.SqlState, unkept.MessageText);
                    Assert.Contains("worf_starts", unkept.MessageText);
                }

                _ = await nodeB.SafePsqlAsync(Database, WrittenAgain, token);
                await nodeB.StopAsync("fast", token);
                Directory.Delete(Path.Join(nodeB.Cluster.DataDirectory, "worf_starts.tmp"));
                if (variant == "unmarked")
                {
                    Directory.Delete(Path.Join(nodeB.Cluster.DataDirectory, "worf_starts.lost"));
                }

                if (File.Exists(Path.Join(nodeB.Cluster.DataDirectory, "worf_starts")))
                {
                    File.SetUnixFileMode(Path.Join(nodeB.Cluster.DataDirectory, "worf_starts"), UnixFileMode.UserRead | UnixFileMode.UserWrite);
                }

                await nodeB.StartServerAsync(token);
            }

            if (variant is not ("single" or "unkept" or "unmarked"))
            {
                _ = await nodeB.SafePsqlAsync(Database, WrittenAgain, token);
                await nodeB.StopAsync("fast", token);
                await nodeB.StartServerAsync(token);
            }

            await nodeA.StartServerAsync(token);
        }

        HashSet<string> after = await InsertIdsAsync("after");
        if (kind == "cut")
        {
            Assert.Contains("sequences to its next call", nodeA.ReadLog()[logpos..], "node_a's calls, cut short");
        }

        Assert.AreEqual("", string.Join(",", before.Intersect(after)), "the ids node_b drew both before the restore and after it");
        if (variant == "unmarked")
        {
            Assert.Contains("removing file \"worf_starts\" instead", nodeB.ReadLog(), "node_b's start, which couldn't mark itself unkept");
        }
    }

    // Inserts 20 rows into t on a node, as its inserts wait until its neighbors have checked its report since it started,
    // and for a new block after a reset, failing at their time meanwhile, and returns their ids.
    private static async Task<HashSet<string>> InsertIdsAsync(TestNode node, string label, CancellationToken token)
    {
        Stopwatch waiting = Stopwatch.StartNew();
        while (true)
        {
            Assert.IsLessThan(TestNode.PollTimeout, waiting.Elapsed, $"{node.Name}'s insert {label}");
            try
            {
                return [.. (await node.SafePsqlAsync(Database, $"INSERT INTO t (v) SELECT '{label}' FROM generate_series(1, 20) RETURNING id", token)).Split('\n')];
            }
            catch (PostgresException waited) when (waited.SqlState is "55000" or "57014" or "2200H")
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200), token);
            }
        }
    }

    /// <summary>
    /// A dropped database's sequences give up their change counters as the drop commits, and so do a database's as worf's
    /// drop there commits, so that a database whose sequences outnumber the counters, dropped or without worf, leaves the
    /// other databases' sequences counters of their own. The server has 64 counters.
    /// </summary>
    /// <param name="how">The database dropped, or worf dropped there.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("database")]
    [DataRow("worf")]
    public async Task DroppedDatabasesGiveUpTheirCounters(string how)
    {
        (int full, string counted) = await IdleChecksBesideOtherDatabaseAsync(how);
        Assert.AreEqual("t", counted, "whether the idle sequence has a counter of its own");
        Assert.IsLessThan(30, full, "the idle sequence's full checks in 10 s");
    }

    /// <summary>
    /// Sequences past the server's 64 counters share a count of their changes only with the few that hash with them, so
    /// that an idle sequence with no counter of its own, as another database's 80 sequences took them all, is checked in
    /// full about once a second, though another sequence of its database switches blocks of 10 meanwhile.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SequencesPastTheCountersShareFewCounts()
    {
        (int full, string counted) = await IdleChecksBesideOtherDatabaseAsync("kept");
        Assert.AreEqual("f", counted, "whether the idle sequence has a counter of its own, as the other database's sequences took them all");
        Assert.IsLessThan(30, full, "the idle sequence's full checks in 10 s");
    }

    // Starts a node with 64 change counters and a database of 80 managed sequences, which it drops, drops worf in, or
    // keeps, and returns how often, in 10 s, an idle sequence of the test's database is checked in full while another
    // switches blocks of 10, and whether the idle one has a counter of its own.
    private async Task<(int Full, string Counted)> IdleChecksBesideOtherDatabaseAsync(string how)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "log_min_messages = debug2\nworf.max_sequence_range_counters = 64");
        _ = await nodeA.SafePsqlAsync(Database, "CREATE DATABASE big", token);
        _ = await nodeA.SafePsqlAsync("big", $"""
            CREATE EXTENSION worf; SELECT * FROM worf.create_node(node_name := 'node_big', dsn := '{nodeA.Dsn("big", SuperUser)}');
            """, token);
        _ = await nodeA.SafePsqlAsync("big", """
            DO $$ BEGIN FOR g IN 1..80 LOOP EXECUTE format('CREATE SEQUENCE s%s', g); END LOOP; END $$
            """, token);
        _ = await nodeA.SafePsqlAsync("big", """
            DO $$ BEGIN
                FOR g IN 1..80 LOOP
                    PERFORM worf.enable_sequence_range(format('s%s', g)::regclass, participants := '{}', range_size := 1000);
                END LOOP;
            END $$
            """, token);

        // Each of big's sequences has its counter once its range has changed.
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        if (how == "database")
        {
            _ = await nodeA.SafePsqlAsync(Database, "DROP DATABASE big WITH (FORCE)", token);
        }
        else if (how == "worf")
        {
            _ = await nodeA.SafePsqlAsync("big", "DROP EXTENSION worf CASCADE", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE TABLE idle (id bigserial PRIMARY KEY); CREATE TABLE busy (id bigserial PRIMARY KEY);
            SELECT worf.enable_sequence_range('idle_id_seq', participants := '{}', range_size := 1000);
            SELECT worf.enable_sequence_range('busy_id_seq', participants := '{}', range_size := 10, max_range_size := 10);
            """, token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT count(*) = 2 FROM worf.sequence_ranges WHERE state = 'active'", token), "both sequences active");
        string idle = await nodeA.SafePsqlAsync(Database, "SELECT 'idle_id_seq'::regclass::oid", token);
        await Task.Delay(TimeSpan.FromSeconds(2), token);

        // busy switches blocks of 10 for 10 s.
        int logged = nodeA.ReadLog().Length;
        await using (NpgsqlConnection writer = await nodeA.OpenAsync(Database, token))
        {
            await using NpgsqlCommand insert = new("INSERT INTO busy DEFAULT VALUES", writer);
            Stopwatch inserting = Stopwatch.StartNew();
            while (inserting.Elapsed < TimeSpan.FromSeconds(10))
            {
                _ = await insert.ExecuteNonQueryAsync(token);
                await Task.Delay(TimeSpan.FromMilliseconds(20), token);
            }
        }

        int full = nodeA.ReadLog()[logged..].Split('\n').Count(line => line.Contains($"checking sequence {idle} in full", StringComparison.Ordinal));
        string counted;
        try
        {
            counted = await nodeA.SafePsqlAsync(Database, "SELECT worf.sequence_range_counted('idle_id_seq')", token);
        }
        catch (PostgresException missing) when (missing.SqlState == "42883")
        {
            counted = "no worf.sequence_range_counted";
        }

        TestContext.WriteLine($"{how}: {full} full checks of the idle sequence in 10 s, counted apart: {counted}");
        return (full, counted);
    }

    /// <summary>
    /// A restored owner draws from the blocks it granted itself only once its neighbors have checked its report since it
    /// started: node_b heard of the owner's draws after its backup, and finds its WAL position went back, so the owner
    /// never draws them again.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestoredOwnersDrawNothingTwice()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        _ = await InsertIdsAsync(nodeA, "first", token);

        // node_a is backed up whole, with its slots, as a snapshot of its storage keeps them, and draws, which node_b
        // hears of.
        using TemporaryDirectory copies = new($"worf-sequence-range-owner-restore-{Guid.NewGuid():N}");
        string copy = Path.Join(copies.FullName, "node_a");
        await BackUpDataAsync(nodeA, copy, token, wal: true);
        CopySlots(nodeA, copy);
        HashSet<string> before = await InsertIdsAsync(nodeA, "before", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            $"SELECT drawn >= {before.Max(long.Parse)} FROM worf.sequence_range_node WHERE node_name = 'node_a' AND seqoid = 't_id_seq'::regclass", token),
            "node_b hearing of node_a's draws");

        await RestoreDataAsync(nodeA, copy, token);
        HashSet<string> after = await InsertIdsAsync(nodeA, "after", token);
        Assert.AreEqual("", string.Join(",", before.Intersect(after)), "the ids node_a drew both before the restore and after it");
    }

    /// <summary>
    /// A restored node waits, before it draws, for every neighbor it exchanges with that reports the sequence, not only
    /// its block's grantor: node_c heard of node_b's draws while node_a, the grantor, was down, and is down itself as
    /// node_b comes back from its backup, so node_b's inserts wait for node_c, naming it, unless node_a has node_b reset
    /// first, though a drop of node_b's subscription to node_c is under way, which may roll back, or node_b's backup is
    /// from before node_c first reported, so that the restored tables have no report of node_c's to wait for, and once
    /// node_c finds node_b went back, node_b never draws those values again.
    /// </summary>
    /// <param name="how">node_c down, down with a drop of b_from_c left open, which then rolls back, or down after first
    /// reporting since the backup.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("down")]
    [DataRow("drop rolled back")]
    [DataRow("first reported after backup")]
    public async Task RestoredNodesWaitForEveryNeighborThatHeardTheirDraws(string how)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        // node_c can't answer node_b's calls, and so reports nothing to it, until node_b's backup is taken, when it's to
        // report first after the backup; node_a's root then starts without node_c's report.
        bool late = how == "first reported after backup";
        const string Paused = "ALTER FUNCTION worf.sequence_range_exchange(text) RENAME TO sequence_range_exchange_paused";
        if (late)
        {
            _ = await nodeC.SafePsqlAsync(Database, Paused, token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 1000)", token);
        if (late)
        {
            Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass)", token),
                "t_id_seq's range");
            await Task.Delay(TimeSpan.FromSeconds(2), token);
            _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.activate_sequence_range('t_id_seq')", token);
        }

        await WaitActiveAsync([nodeA, nodeB], token);
        _ = await InsertIdsAsync(nodeB, "first", token);
        const string HeardOfB = "FROM worf.sequence_range_node WHERE node_name = 'node_b' AND seqoid = 't_id_seq'::regclass";
        if (!late)
        {
            Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, $"SELECT EXISTS (SELECT 1 {HeardOfB})", token), "node_c hearing of node_b");
        }

        // node_b is backed up whole, and draws while node_a is down, which node_c hears of; node_c stops before the restore.
        using TemporaryDirectory copies = new($"worf-sequence-range-neighbor-restore-{Guid.NewGuid():N}");
        string copy = Path.Join(copies.FullName, "node_b");
        await BackUpDataAsync(nodeB, copy, token, wal: true);
        if (late)
        {
            Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM worf.sequence_range_node WHERE node_name = 'node_c'", token),
                "node_c's reports on node_b at the backup");
            _ = await nodeC.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange_paused(text) RENAME TO sequence_range_exchange", token);
            Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, $"SELECT EXISTS (SELECT 1 {HeardOfB})", token), "node_c hearing of node_b");
        }

        await nodeA.StopAsync("fast", token);
        HashSet<string> before = await InsertIdsAsync(nodeB, "before", token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, $"SELECT drawn >= {before.Max(long.Parse)} {HeardOfB}", token), "node_c hearing of node_b's draws");
        await nodeC.StopAsync("fast", token);
        int restored = nodeB.ReadLog().Length;
        await RestoreDataAsync(nodeB, copy, token);
        await nodeA.StartServerAsync(token);

        await using NpgsqlConnection dropping = await nodeB.OpenAsync(Database, token);
        await using NpgsqlTransaction open = await dropping.BeginTransactionAsync(token);
        if (how == "drop rolled back")
        {
            await using NpgsqlCommand drop = new("SELECT worf.drop_subscription('b_from_c')", dropping, open);
            _ = await drop.ExecuteScalarAsync(token);
        }

        HashSet<string> after = await WaitsOrResetsAsync(nodeB, ["node_c"], restored, token);
        await open.RollbackAsync(token);

        await nodeC.StartServerAsync(token);
        after.UnionWith(await InsertIdsAsync(nodeB, "after", token));
        Assert.AreEqual("", string.Join(",", before.Intersect(after)), "the ids node_b drew both before the restore and after it");
    }

    /// <summary>
    /// A restored node waits, before it draws, for a node that only calls it, though the backup is from before that node's
    /// first exchange: node_c's sync ties its slot on node_b to node_c as it makes it, while node_b refuses to tie it at
    /// a call while node_c's apply streams it, as a node_b that a backup restores from before node_c's first call has no
    /// tie for it. node_c hears of node_b's draws after the backup and is down as node_b comes back, so node_b's inserts
    /// wait for node_c, naming it, unless node_a has node_b reset first, and once node_c finds node_b went back, node_b
    /// never draws those values again.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestoredNodesWaitForSubscribersThatFirstCalledAfterTheBackup()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        // node_b refuses, while its unlogged table refusing holds a row, which neither replicates nor survives a restore,
        // to tie a slot to a node at a call made while the slot is streamed, as a node's first exchange through its slot
        // ties it.
        _ = await nodeB.SafePsqlAsync(Database, """
            CREATE UNLOGGED TABLE refusing (); INSERT INTO refusing DEFAULT VALUES;
            CREATE FUNCTION refuse_ties() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF EXISTS (SELECT FROM public.refusing)
                    AND EXISTS (SELECT FROM pg_catalog.pg_replication_slots WHERE slot_name = NEW.slot_name AND active) THEN
                    RAISE EXCEPTION 'worf-test-refused-tie';
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER refuse_ties BEFORE INSERT ON worf.sequence_range_link FOR EACH ROW EXECUTE FUNCTION refuse_ties();
            """, token);

        // node_a's root starts before node_c subscribes to node_b, which it reaches node_a through.
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 1000)", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass)", token),
            "t_id_seq's range");
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.activate_sequence_range('t_id_seq')", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await SubscribeAsync(nodeC, "c_from_b", nodeB, token);
        string nodeCId = await nodeC.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        _ = await InsertIdsAsync(nodeB, "first", token);

        // node_b is backed up whole, with its slots, as a snapshot keeps them; then it ties slots at calls again, and draws
        // while node_a is down, which node_c hears of; node_c stops before the restore.
        using TemporaryDirectory copies = new($"worf-sequence-range-caller-restore-{Guid.NewGuid():N}");
        string copy = Path.Join(copies.FullName, "node_b");
        await BackUpDataAsync(nodeB, copy, token, wal: true);
        CopySlots(nodeB, copy);
        _ = await nodeB.SafePsqlAsync(Database, "DELETE FROM refusing", token);
        const string HeardOfB = "FROM worf.sequence_range_node WHERE node_name = 'node_b' AND seqoid = 't_id_seq'::regclass";
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, $"SELECT EXISTS (SELECT 1 {HeardOfB})", token), "node_c hearing of node_b");
        await nodeA.StopAsync("fast", token);
        HashSet<string> before = await InsertIdsAsync(nodeB, "before", token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, $"SELECT drawn >= {before.Max(long.Parse)} {HeardOfB}", token), "node_c hearing of node_b's draws");
        await nodeC.StopAsync("fast", token);
        int restored = nodeB.ReadLog().Length;
        await RestoreDataAsync(nodeB, copy, token);
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM refusing", token), "node_b's refusing rows after the restore");
        await nodeA.StartServerAsync(token);

        // node_b names node_c by its id while it knows no name for it.
        HashSet<string> after = await WaitsOrResetsAsync(nodeB, ["node_c", $"{nodeCId},"], restored, token);
        await nodeC.StartServerAsync(token);
        after.UnionWith(await InsertIdsAsync(nodeB, "after", token));
        Assert.AreEqual("", string.Join(",", before.Intersect(after)), "the ids node_b drew both before the restore and after it");
    }

    /// <summary>
    /// A node restored from a backup taken before a neighbor first called it, here node_c, whose subscription to node_b
    /// was made while node_b's exchange function was renamed, waits for that neighbor though its restored tables don't
    /// know it: node_b registers node_c with node_a, its other neighbor, as node_c first calls, and draws only once node_a
    /// has told it back, and node_a's answer to the restored node_b names node_c, which node_b then waits for, down,
    /// unless node_a has it reset, rather than draw again what node_c holds. With node_a down as node_c first calls,
    /// node_b draws nothing until node_a is back to hear of node_c (1605). And with what node_a recorded of node_b's WAL
    /// positions gone, so that node_a can't tell the restore, node_b waits for node_c, and draws once node_c is back
    /// and has it reset (2302). The restored node_b's exchange function gets its name again, as the backup was taken
    /// while it was renamed, which kept node_c's calls from a restored node_b.
    /// </summary>
    /// <param name="order">Whether node_a goes down after node_c first calls node_b, or before, or after it with its
    /// tables recording nothing of node_b since the backup.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("allocator down after")]
    [DataRow("allocator down before")]
    [DataRow("allocator down after, recording nothing")]
    public async Task RestoredNodesWaitForNeighborsTakenOnAfterTheBackup(string order)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text); SELECT * FROM worf.replication_set_add_table('default', 't')", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 1000)", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass)", token),
            "t_id_seq's range");
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.activate_sequence_range('t_id_seq')", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        _ = await InsertIdsAsync(nodeB, "first", token);

        bool unrecorded = order == "allocator down after, recording nothing";

        // node_c subscribes while node_b's exchange function is renamed, so it doesn't call node_b, and node_b is backed
        // up with its slots, as a snapshot keeps them, before node_c first calls.
        _ = await nodeB.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange(text) RENAME TO sequence_range_exchange_paused", token);
        await SubscribeAsync(nodeC, "c_from_b", nodeB, token);
        using TemporaryDirectory copies = new($"worf-sequence-range-new-neighbor-{Guid.NewGuid():N}");
        string copy = Path.Join(copies.FullName, "node_b");
        await BackUpDataAsync(nodeB, copy, token, wal: true);
        CopySlots(nodeB, copy);
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM worf.sequence_range_link", token), "node_b's links at the backup");
        if (order == "allocator down before")
        {
            // With node_a down as node_c first calls, node_b draws nothing until node_a hears of node_c, and then draws.
            await nodeA.StopAsync("fast", token);
            _ = await nodeB.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange_paused(text) RENAME TO sequence_range_exchange", token);
            Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_link)", token), "node_c calling node_b");
            PostgresException gated = await Assert.ThrowsExactlyAsync<PostgresException>(() =>
                nodeB.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('gated')", token), "node_b's insert before node_a hears of node_c");
            Assert.AreEqual("55000", gated.SqlState, gated.MessageText);
            Assert.Contains("node node_a", gated.MessageText, "node_b's wait for node_a");
            await nodeA.StartServerAsync(token);
            _ = await InsertIdsAsync(nodeB, "registered", token);
            return;
        }

        // node_c first calls, node_a hears of it and tells node_b back, and node_a stops; node_b draws, which node_c hears
        // of, and node_c stops.
        _ = await nodeB.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange_paused(text) RENAME TO sequence_range_exchange", token);
        Assert.IsTrue(await NamedNodeCAsync(nodeB, token), "node_a telling node_b back of node_c");
        if (unrecorded)
        {
            // Nothing keeps node_a from telling that node_b went back: it compares every WAL position node_b reports. So
            // that only node_c can tell, node_a takes no more calls, and what it recorded of node_b's positions goes.
            _ = await nodeA.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange(text) RENAME TO sequence_range_exchange_paused", token);
            Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, """
                SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE state = 'active' AND pid <> pg_backend_pid() AND query LIKE '%sequence_range_exchange(%')
                """, token), "node_a's last call from node_b ending");
            _ = await nodeA.SafePsqlAsync(Database, """
                UPDATE worf.sequence_range_node SET wal_lsn = '0/1', reply_lsn = '0/1' WHERE node_name = 'node_b';
                DELETE FROM worf.sequence_range_back;
                """, token);
        }

        await nodeA.StopAsync("fast", token);
        HashSet<string> before = await InsertIdsAsync(nodeB, "before", token);
        const string HeardOfB = "FROM worf.sequence_range_node WHERE node_name = 'node_b' AND seqoid = 't_id_seq'::regclass";
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, $"SELECT drawn >= {before.Max(long.Parse)} {HeardOfB}", token), "node_c hearing of node_b's draws");
        await nodeC.StopAsync("fast", token);

        // node_b is restored and node_a started: node_a names node_c back to node_b, which waits for node_c, down, unless
        // node_a, from node_b's WAL position, has it reset and draw from new blocks; either way it draws none of what node_c
        // holds. The backup has node_b's exchange function under the name it had as node_c subscribed, which no restore
        // of a node in use has, so it gets its own again, for node_c to call.
        int restored = nodeB.ReadLog().Length;
        await RestoreDataAsync(nodeB, copy, token);
        _ = await nodeB.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange_paused(text) RENAME TO sequence_range_exchange", token);
        await nodeA.StartServerAsync(token);
        if (unrecorded)
        {
            _ = await nodeA.SafePsqlAsync(Database, "ALTER FUNCTION worf.sequence_range_exchange_paused(text) RENAME TO sequence_range_exchange", token);
        }

        Assert.IsTrue(await NamedNodeCAsync(nodeB, token), "node_a naming node_c to the restored node_b");
        HashSet<string> after = await WaitsOrResetsAsync(nodeB, ["node_c"], restored, token);
        if (unrecorded)
        {
            Assert.IsEmpty(after, "the ids node_b drew before node_c is back, as node_a can't tell it went back");
            Assert.DoesNotContain("abandons its blocks", nodeB.ReadLog()[restored..], "node_b's resets before node_c is back");
        }

        // Once node_c is back and finds node_b went back, node_b never draws those values again.
        await nodeC.StartServerAsync(token);
        after.UnionWith(await InsertIdsAsync(nodeB, "after", token));
        Assert.AreEqual("", string.Join(",", before.Intersect(after)), "the ids node_b drew both before the restore and after it");
        if (unrecorded)
        {
            // node_c tells node_b that it went back, or node_b finds node_c holding blocks it has no record of granting.
            string log = nodeB.ReadLog()[restored..];
            Assert.IsTrue(log.Contains("abandons its blocks", StringComparison.Ordinal) || log.Contains("this node went back; it resets", StringComparison.Ordinal),
                "node_b's reset once node_c is back");
        }
    }

    /// <summary>
    /// Waits until a neighbor has named node_c to a node as a neighbor of its own. A build from before nodes told each
    /// other of their neighbors has no table of them and nothing to wait for, so the wait is over at once there (2203).
    /// </summary>
    private static async Task<bool> NamedNodeCAsync(TestNode node, CancellationToken token)
    {
        try
        {
            return await node.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_named WHERE neighbor_name = 'node_c')", token);
        }
        catch (PostgresException missing) when (missing.SqlState == "42P01")
        {
            return true;
        }
    }

    /// <summary>
    /// Nodes that subscribe to each other record a draw for whichever way first carries it: here node_a's call to node_b
    /// carries node_b's draws, as node_b calls only each minute, and node_b, restored without its slots, so that node_a
    /// can't call it, is checked through its own calls alone. Its start, from before the position node_a's call heard,
    /// shows the restore, and it never draws again the values it drew after the backup.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestoredMutualNeighborsDrawNothingTwice()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 20, token);

        // node_b's inserts wait until node_a has answered it since it started, and for a new block after a reset.
        async Task<HashSet<string>> InsertIdsAsync(string label)
        {
            Stopwatch waiting = Stopwatch.StartNew();
            while (true)
            {
                Assert.IsLessThan(TestNode.PollTimeout, waiting.Elapsed, $"node_b's insert {label}");
                try
                {
                    return [.. (await nodeB.SafePsqlAsync(Database, $"INSERT INTO t (v) SELECT '{label}' FROM generate_series(1, 20) RETURNING id", token)).Split('\n')];
                }
                catch (PostgresException waited) when (waited.SqlState is "55000" or "57014" or "2200H")
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(200), token);
                }
            }
        }

        // node_b calls node_a only each minute from now on, and is backed up whole; its draws reach node_a in node_a's
        // calls, as their answers' positions.
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_exchange_interval = '60s'; SELECT pg_reload_conf()", token);
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        using TemporaryDirectory copies = new($"worf-sequence-range-mutual-restore-{Guid.NewGuid():N}");
        string copy = Path.Join(copies.FullName, "node_b");
        await BackUpDataAsync(nodeB, copy, token, wal: true);
        string drawnFrom = await nodeB.SafePsqlAsync(Database, "SELECT pg_current_wal_lsn()", token);
        HashSet<string> before = await InsertIdsAsync("before");
        string drawnTo = await nodeB.SafePsqlAsync(Database, "SELECT pg_current_wal_lsn()", token);

        // node_a records the last value node_b drew, with the position of the answer that carried it, past the draws'
        // start, as a later answer that changes nothing else is kept in shared memory only.
        const string Recorded = "FROM worf.sequence_range_node WHERE node_name = 'node_b' AND seqoid = 't_id_seq'::regclass";
        long last = before.Max(id => long.Parse(id, CultureInfo.InvariantCulture));
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT drawn >= {last} AND reply_lsn > '{drawnFrom}' {Recorded}", token),
            "node_a's calls, carrying node_b's draws");
        Assert.AreEqual("t", await nodeA.SafePsqlAsync(Database, $"SELECT coalesce(wal_lsn < '{drawnTo}', true) {Recorded}", token),
            "node_b's own calls, which haven't carried its draws");

        await RestoreDataAsync(nodeB, copy, token);
        HashSet<string> after = await InsertIdsAsync("after");
        Assert.AreEqual("", string.Join(",", before.Intersect(after)), "the ids node_b drew both before the restore and after it");
    }

    /// <summary>
    /// A node restored to a backup on an older timeline than one its grantor heard it on resets, though its grantor
    /// recorded no position on that timeline past the backup: node_b is copied on timeline 1 and draws, recovers past
    /// the draws onto timeline 2, where it resets and draws from a new block, and goes back to the copy on timeline 1.
    /// It never draws again the values it drew after the copy.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestoredToOlderTimelinesDrawNothingTwice()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 20, token);

        // node_b's inserts wait until node_a has answered it since it started, and for a new block after a reset.
        async Task<HashSet<string>> InsertIdsAsync(string label)
        {
            Stopwatch waiting = Stopwatch.StartNew();
            while (true)
            {
                Assert.IsLessThan(TestNode.PollTimeout, waiting.Elapsed, $"node_b's insert {label}");
                try
                {
                    return [.. (await nodeB.SafePsqlAsync(Database, $"INSERT INTO t (v) SELECT '{label}' FROM generate_series(1, 20) RETURNING id", token)).Split('\n')];
                }
                catch (PostgresException waited) when (waited.SqlState is "55000" or "57014" or "2200H")
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(200), token);
                }
            }
        }

        const string HeardTimeline = "SELECT timeline FROM worf.sequence_range_node WHERE node_name = 'node_b' AND seqoid = 't_id_seq'::regclass";
        using TemporaryDirectory copies = new($"worf-sequence-range-timeline-{Guid.NewGuid():N}");
        string copy = Path.Join(copies.FullName, "node_b");
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET wal_keep_size = '1GB'; SELECT pg_reload_conf()", token);
        await CopyDataAsync(nodeB, copy, token);
        HashSet<string> before = await InsertIdsAsync("before");
        string drawnTo = await nodeB.SafePsqlAsync(Database, "SELECT pg_current_wal_lsn()", token);

        // node_a hears of node_b's WAL position past the draws: node_b makes its calls one at a time, so the second that
        // node_a hears after the draws was made after them. A call that changes nothing else has node_a keep the position
        // in shared memory rather than in its tables.
        const string HeardFromB = """
            SELECT worf.sequence_range_heard(node_id, true, '-infinity', false)::text FROM worf.sequence_range_node
            WHERE node_name = 'node_b' AND seqoid = 't_id_seq'::regclass
            """;
        string heard = await nodeA.SafePsqlAsync(Database, "SELECT clock_timestamp()::text", token);
        for (int call = 0; call < 2; call++)
        {
            Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT ({HeardFromB})::timestamptz > '{heard}'", token), "node_a hearing of node_b's draws");
            heard = await nodeA.SafePsqlAsync(Database, HeardFromB, token);
        }

        // node_b recovers past its draws onto timeline 2, resets as its identity changed, and draws from a new block,
        // which node_a hears of on timeline 2.
        await RestoreDataAsync(nodeB, copy, token, target: drawnTo);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT NOT pg_is_in_recovery()", token), "node_b recovered");
        HashSet<string> second = await InsertIdsAsync("second");
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT ({HeardTimeline}) = 2", token), "node_a hearing node_b on timeline 2");

        // node_b goes back to the copy on timeline 1.
        await RestoreDataAsync(nodeB, copy, token);
        Assert.AreEqual("00000001", await nodeB.SafePsqlAsync(Database, "SELECT substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8)", token),
            "node_b's timeline after the restore");
        HashSet<string> after = await InsertIdsAsync("after");
        Assert.AreEqual("", string.Join(",", before.Union(second).Intersect(after)), "the ids node_b drew again after going back to timeline 1");
    }

    /// <summary>
    /// A node whose file of starts can't be read, or whose worf_starts.tmp is a directory, starts as usual: its supervisor
    /// keeps running and keeps the start, writing the file in place or anew, and the node's inserts go on, though a file
    /// that lost the starts before has its neighbors take the positions they heard of before as gone back.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public Task UnusableStartFilesStopNothing() => StartFileStopsNothingAsync("tmp directory");

    /// <summary>
    /// A node whose file of starts has mode 000 starts as <see cref="UnusableStartFilesStopNothing"/> has it, where files
    /// have Unix modes.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public Task UnreadableStartFilesStopNothing() => StartFileStopsNothingAsync("unreadable");

    // Runs a node whose file of starts has mode 000, or whose worf_starts.tmp is a directory, as it starts.
    private async Task StartFileStopsNothingAsync(string how)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 20, token);

        await nodeB.StopAsync("fast", token);
        string starts = Path.Join(nodeB.Cluster.DataDirectory, "worf_starts");
        if (how == "unreadable" && !OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(starts, UnixFileMode.None);
        }
        else if (how == "tmp directory")
        {
            _ = Directory.CreateDirectory(starts + ".tmp");
        }

        int logged = nodeB.ReadLog().Length;
        await nodeB.StartServerAsync(token);
        Stopwatch inserting = Stopwatch.StartNew();
        await using (NpgsqlConnection connection = await nodeB.OpenAsync(Database, token))
        {
            await using NpgsqlCommand insert = new("SET statement_timeout = '20s'; INSERT INTO t (v) SELECT 'b' FROM generate_series(1, 200)", connection);
            _ = await insert.ExecuteNonQueryAsync(token);
        }

        Assert.IsLessThan(TimeSpan.FromSeconds(20), inserting.Elapsed, "node_b's insert after its start");
        Assert.DoesNotContain("\"worf supervisor\" (PID", nodeB.ReadLog()[logged..], "node_b's log, of its supervisor exiting");
        string[] kept = await File.ReadAllLinesAsync(starts, token);
        Assert.AreEqual("end", kept[^1], "worf_starts, whole");
        Assert.IsGreaterThanOrEqualTo(2, kept.Length, "worf_starts, keeping the start");
    }

    /// <summary>
    /// A node's message reports a WAL position past every draw it reports, though a session draws as the message is made:
    /// here the session holds the sequence locked until the call waits to read its last value, and draws before it lets
    /// go, once the node is backed up, from a checkpoint after the call began, and its WAL copied. The node, restored to
    /// the copy as its grantor restarts, which forgets what its shared memory heard, never draws again the values the
    /// session drew.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task MessagesReportPositionsPastTheirDraws()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 1000)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 1 FROM worf.sequence_ranges WHERE state = 'active'", token),
            "node_b drawing from a block of t_id_seq");

        using TemporaryDirectory copies = new($"worf-sequence-range-position-{Guid.NewGuid():N}");
        string copy = Path.Join(copies.FullName, "node_b");

        // A session holds t_id_seq locked until node_b's call waits to read its last value, as it makes its message, which
        // reads worf.sequence_range_dropped with it, and node_b is backed up, from a checkpoint after the call began, and
        // its WAL copied; the session draws 40 ids, and lets go.
        HashSet<string> before = [];
        await using (NpgsqlConnection holder = await nodeB.OpenAsync(Database, token))
        {
            NpgsqlTransaction holding;
            Stopwatch trying = Stopwatch.StartNew();
            while (true)
            {
                holding = await holder.BeginTransactionAsync(token);
                await using (NpgsqlCommand alter = new("ALTER SEQUENCE t_id_seq OWNED BY t.id", holder, holding))
                {
                    _ = await alter.ExecuteNonQueryAsync(token);
                }

                Stopwatch held = Stopwatch.StartNew();
                bool reading = false;
                while (!reading && held.Elapsed < TimeSpan.FromSeconds(3))
                {
                    reading = await nodeB.SafePsqlAsync(Database, """
                        SELECT EXISTS (
                            SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
                            WHERE NOT l.granted AND l.relation = 't_id_seq'::regclass AND a.backend_type LIKE 'worf exchange%'
                                AND EXISTS (SELECT 1 FROM pg_locks d WHERE d.pid = l.pid AND d.relation = 'worf.sequence_range_dropped'::regclass))
                        """, token) == "t";
                    await Task.Delay(TimeSpan.FromMilliseconds(50), token);
                }

                if (reading)
                {
                    break;
                }

                await holding.RollbackAsync(token);
                await holding.DisposeAsync();
                Assert.IsLessThan(TimeSpan.FromSeconds(60), trying.Elapsed, "node_b's call, waiting to read t_id_seq's last value");
            }

            await BackUpDataAsync(nodeB, copy, token);
            CopyWal(nodeB, copy);
            await using (NpgsqlCommand draw = new("SELECT nextval('t_id_seq') FROM generate_series(1, 40)", holder, holding))
            {
                await using NpgsqlDataReader drawn = await draw.ExecuteReaderAsync(token);
                while (await drawn.ReadAsync(token))
                {
                    _ = before.Add(drawn.GetInt64(0).ToString(CultureInfo.InvariantCulture));
                }
            }

            await holding.CommitAsync(token);
            await holding.DisposeAsync();
        }

        // node_a takes in the call that reports the draws, and restarts as node_b goes back to the copy.
        long last = before.Max(id => long.Parse(id, CultureInfo.InvariantCulture));
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, string.Create(CultureInfo.InvariantCulture, $"""
            SELECT drawn >= {last} FROM worf.sequence_range_node WHERE node_name = 'node_b' AND seqoid = 't_id_seq'::regclass AND via_node = node_id
            """), token), "node_a's record of node_b's draws");
        await nodeA.StopAsync("fast", token);
        await RestoreDataAsync(nodeB, copy, token);
        await nodeA.StartServerAsync(token);

        HashSet<string> after;
        Stopwatch waiting = Stopwatch.StartNew();
        while (true)
        {
            Assert.IsLessThan(TestNode.PollTimeout, waiting.Elapsed, "node_b's insert after the restore");
            try
            {
                after = [.. (await nodeB.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'after' FROM generate_series(1, 20) RETURNING id", token)).Split('\n')];
                break;
            }
            catch (PostgresException waited) when (waited.SqlState is "55000" or "57014" or "2200H")
            {
                await Task.Delay(TimeSpan.FromMilliseconds(200), token);
            }
        }

        Assert.AreEqual("", string.Join(",", before.Intersect(after)), "the ids node_b drew both before the restore and after it");
    }

    /// <summary>
    /// After its owner drops a sequence, a participant draws from the blocks it keeps only once the owner has checked its
    /// report since it started: restored from a backup taken after the drop, it never draws again the values it drew
    /// after the backup, as the owner finds its WAL position went back, and asks it to give up those blocks. Nor does it
    /// when it drew before the drop, while a prepared transaction held the range row on the owner, so that its calls
    /// left the sequence out and only kept their positions, which the owner keeps with the drop, whether the transaction
    /// ends before the restore or not. An owner's answer to a call that was cut short before the dropped sequence, which
    /// it left unread, tells the participant the sequence is dropped, but confirms nothing.
    /// </summary>
    /// <param name="how">The drop before the backup, or after draws while the row was held, let go before the restore or
    /// not, or before the backup with the owner's calls cut short.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("dropped")]
    [DataRow("cut")]
    [DataRow("held")]
    [DataRow("held committed")]
    public async Task RestoredParticipantsOfDroppedSequencesDrawNothingTwice(string how)
    {
        CancellationToken token = TestContext.CancellationToken;
        bool held = how.StartsWith("held", StringComparison.Ordinal);
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "max_prepared_transactions = 2\nlog_min_messages = debug1");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        int extra = how == "cut" ? 15 : 0;
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, string.Create(CultureInfo.InvariantCulture,
                $"CREATE TABLE slow_on (); CREATE TABLE mv (id bigserial PRIMARY KEY, v text); DO $$ BEGIN FOR g IN 1..{extra} LOOP EXECUTE format('CREATE SEQUENCE many_%s', lpad(g::text, 2, '0')); END LOOP; END $$"),
                token);
        }

        // Once it's turned on, each report node_a takes in costs it 0.2 s, so that a call's third of its 5 s takes in
        // the many_ sequences' reports, which sort before mv_id_seq's, and leaves mv_id_seq's unread.
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE FUNCTION slow_report() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT FROM public.slow_on) THEN PERFORM pg_sleep(0.2); END IF; RETURN NEW; END $$;
            CREATE TRIGGER slow_report BEFORE UPDATE ON worf.sequence_range_node FOR EACH ROW EXECUTE FUNCTION slow_report();
            """, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('mv_id_seq', participants := '{node_b}', range_size := 1000)", token);
        _ = await nodeA.SafePsqlAsync(Database, string.Create(CultureInfo.InvariantCulture, $$"""
            SELECT count(worf.enable_sequence_range(format('many_%s', lpad(g::text, 2, '0'))::regclass, participants := '{node_b}', range_size := 1000)) FROM generate_series(1, {{extra}}) g
            """), token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"SELECT count(*) = {extra} FROM worf.sequence_ranges WHERE state = 'active' AND sequence::text LIKE 'many_%'", token),
            "node_b drawing from the many_ sequences' blocks");
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges WHERE sequence = 'mv_id_seq'::regclass", token),
            "node_b drawing from mv_id_seq's blocks");

        // node_a drops mv, and node_b, keeping its block, is backed up whole, and draws 20 ids, which node_a hears of; or
        // a prepared transaction holds mv_id_seq's range row on node_a first, and node_a drops mv after the draws.
        if (held)
        {
            _ = await nodeA.SafePsqlAsync(Database,
                "BEGIN; SELECT 1 FROM worf.sequence_range WHERE seqoid = 'mv_id_seq'::regclass FOR NO KEY UPDATE; PREPARE TRANSACTION 'held_mv'", token);
        }
        else
        {
            _ = await nodeA.SafePsqlAsync(Database, "DROP TABLE mv", token);
            Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'disabled' FROM worf.sequence_ranges WHERE sequence = 'mv_id_seq'::regclass", token),
                "node_b heard mv_id_seq's epoch end");
        }

        using TemporaryDirectory copies = new($"worf-sequence-range-dropped-restore-{Guid.NewGuid():N}");
        string copy = Path.Join(copies.FullName, "node_b");
        await BackUpDataAsync(nodeB, copy, token, wal: true);
        HashSet<string> before = [.. (await nodeB.SafePsqlAsync(Database, "INSERT INTO mv (v) SELECT 'before' FROM generate_series(1, 20) RETURNING id", token)).Split('\n')];
        string nodeBId = await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);
        for (int call = 0; call < 2; call++)
        {
            string heard = await nodeA.SafePsqlAsync(Database, "SELECT clock_timestamp()::text", token);
            Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT worf.sequence_range_heard({nodeBId}, true, '-infinity', false) > '{heard}'", token),
                "node_a hearing from node_b after its draws");
        }

        if (held)
        {
            _ = await nodeA.SafePsqlAsync(Database, "DROP TABLE mv", token);
            if (how == "held committed")
            {
                _ = await nodeA.SafePsqlAsync(Database, "COMMIT PREPARED 'held_mv'", token);
            }
        }

        // node_b goes back to the backup. Its inserts fail, or draw none of the ids it drew before.
        int logpos = nodeA.ReadLog().Length;
        if (how == "cut")
        {
            _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO slow_on DEFAULT VALUES", token);
        }

        await RestoreDataAsync(nodeB, copy, token);

        // node_a reaches mv_id_seq's report once its calls have taken in the many_ sequences' reports ahead of it, each
        // with node_b's reserve: about 8 s here, and 15 on a macOS runner.
        Stopwatch trying = Stopwatch.StartNew();
        while (trying.Elapsed < TimeSpan.FromSeconds(30))
        {
            try
            {
                HashSet<string> after = [.. (await nodeB.SafePsqlAsync(Database,
                    "SET statement_timeout = '3s'; INSERT INTO mv (v) SELECT 'after' FROM generate_series(1, 20) RETURNING id", token)).Split('\n')];
                Assert.AreEqual("", string.Join(",", before.Intersect(after)), "the ids node_b drew both before the restore and after it");
            }
            catch (PostgresException failed) when (failed.SqlState is "55000" or "57014" or "2200H")
            {
                await Task.Delay(TimeSpan.FromMilliseconds(500), token);
            }
        }

        Assert.Contains("which this node recorded before it dropped the sequence, so it must give up the blocks it keeps of it", nodeA.ReadLog()[logpos..],
            "node_a's word that node_b went back");
        if (how == "cut")
        {
            Assert.Contains("sequences to its next call", nodeA.ReadLog()[logpos..], "node_a's calls, cut short");
        }

        if (how == "held")
        {
            _ = await nodeA.SafePsqlAsync(Database, "COMMIT PREPARED 'held_mv'", token);
        }
    }

    /// <summary>
    /// The manager's pass over replicated sequences passes over one that a session holds, rather than wait for it holding
    /// another, which the session then synchronizes: neither waits on the other. The session holds sb's row through its
    /// worf.synchronize_sequence, or sb itself through ALTER SEQUENCE, which the pass would wait for as it reads sb.
    /// </summary>
    /// <param name="how">How the session holds sb.</param>
    /// <returns>The test.</returns>
    [TestMethod]
    [DataRow("synchronized")]
    [DataRow("renamed")]
    public async Task SequencePassesWaitOnNoSession(string how)
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE SEQUENCE sa; CREATE SEQUENCE sb;
            SELECT worf.replication_set_add_sequence('default', 'sa');
            SELECT worf.replication_set_add_sequence('default', 'sb');
            """, token);

        // A session holds sb; sa, and sb unless it's renamed, then draw past half their cache, so that the next pass takes
        // sa's row and comes to sb.
        await using NpgsqlConnection session = await nodeA.OpenAsync(Database, token);
        await using NpgsqlTransaction holding = await session.BeginTransactionAsync(token);
        await using (NpgsqlCommand first = new(how == "renamed" ? "ALTER SEQUENCE sb RENAME TO sb_renamed" : "SELECT worf.synchronize_sequence('sb')", session, holding))
        {
            _ = await first.ExecuteNonQueryAsync(token);
        }

        _ = await nodeA.SafePsqlAsync(Database, how == "renamed"
            ? "SELECT count(nextval('sa')) FROM generate_series(1, 600)"
            : "SELECT count(nextval('sa')), count(nextval('sb')) FROM generate_series(1, 600)", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, """
            SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid WHERE NOT l.granted AND a.backend_type LIKE 'worf manager%')
                OR (SELECT last_value FROM worf.sequence_state WHERE seqoid = 'sa'::regclass) > (SELECT last_value FROM sa)
            """, token), "the manager's pass, past sa");

        // The session synchronizes sa, which the pass took first, and nothing deadlocks.
        int logpos = nodeA.ReadLog().Length;
        await using (NpgsqlCommand second = new("SET LOCAL statement_timeout = '30s'; SELECT worf.synchronize_sequence('sa')", session, holding))
        {
            _ = await second.ExecuteNonQueryAsync(token);
        }

        await holding.CommitAsync(token);
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        Assert.DoesNotContain("deadlock detected", nodeA.ReadLog()[logpos..], "node_a's log");
    }

    /// <summary>
    /// A fenced allocator with 20,000 grants of the sequence looks for grants that overlap in one pass over them, and again
    /// only once they change; a neighbor's calls that report the same blocks as the fence waits for another node take
    /// nothing in; and sessions that draw from another sequence wait no longer for the range worker than without a fence.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task FencesSearchTheirLedgerOnceUntilItChanges()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "shared_preload_libraries = 'worf,pg_stat_statements'\npg_stat_statements.track = all");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text); CREATE TABLE u (id bigserial PRIMARY KEY)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "CREATE EXTENSION pg_stat_statements", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeC, "c_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 100);
            SELECT worf.enable_sequence_range('u_id_seq', participants := '{node_b,node_c}', range_size := 10, max_range_size := 10);
            """, token);
        await WaitActiveAsync([nodeA, nodeB, nodeC], token);
        await InsertAsync(nodeB, 10, token);
        await InsertAsync(nodeC, 10, token);

        // node_a granted 20,000 blocks of t_id_seq to a node long gone.
        _ = await nodeA.SafePsqlAsync(Database, """
            INSERT INTO worf.sequence_range_grant (grant_id, request_id, seqoid, grantee_id, grantee_name, kind, lo, hi, state, granted_at)
            SELECT gen_random_uuid(), gen_random_uuid(), 't_id_seq'::regclass, 1, 'gone', 'n', 900000000000000000 + g * 10, 900000000000000000 + g * 10 + 9, 'abandoned', now()
            FROM generate_series(1, 20000) g;
            ANALYZE worf.sequence_range_grant;
            """, token);

        // A session on node_a inserts 60 rows into u, a row at a time, with node_a's blocks of u_id_seq ten values long.
        async Task<TimeSpan> InsertIntoU()
        {
            await using NpgsqlConnection connection = await nodeA.OpenAsync(Database, token);
            await using NpgsqlCommand insert = new("INSERT INTO u DEFAULT VALUES", connection);
            Stopwatch inserting = Stopwatch.StartNew();
            for (int i = 0; i < 60; i++)
            {
                _ = await insert.ExecuteNonQueryAsync(token);
            }

            return inserting.Elapsed;
        }

        TimeSpan unfenced = await InsertIntoU();

        // node_c stops, and node_a loses its grants to node_c, and its record of node_c's reports, and resets: its fence
        // waits for node_c, as node_b keeps calling. Edits by hand lock the range row first, as every change to a
        // sequence's ranges does.
        await nodeC.StopAsync("fast", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass FOR NO KEY UPDATE;
            DELETE FROM worf.sequence_range_grant WHERE seqoid = 't_id_seq'::regclass AND grantee_name = 'node_c';
            DELETE FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND node_name = 'node_c';
            """, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_stat_statements_reset()", token);
        Stopwatch fenced = Stopwatch.StartNew();
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('t_id_seq')", token);
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.sequence_range_node n JOIN worf.sequence_range r ON r.seqoid = n.seqoid
                WHERE n.seqoid = 't_id_seq'::regclass AND n.node_name = 'node_b' AND n.via_node = n.node_id AND n.held_at > r.fenced_at)
            """, token), "node_b's report of the blocks it holds since the fence went up");
        TimeSpan during = await InsertIntoU();

        // node_b's calls over 5 s, once node_c's last report is stale, 6.5 s after it, and the statements each ran on
        // node_a, which pg_stat_statements counts as nested in the call's.
        await Task.Delay(TimeSpan.FromSeconds(10) - fenced.Elapsed, token);
        const string Calls = """
            SELECT coalesce(sum(calls) FILTER (WHERE toplevel AND query LIKE 'SELECT worf.sequence_range_exchange(%'), 0) || '|' ||
                coalesce(sum(calls) FILTER (WHERE NOT toplevel), 0)
            FROM pg_stat_statements
            """;
        long[] before = [.. (await nodeA.SafePsqlAsync(Database, Calls, token)).Split('|').Select(count => long.Parse(count, CultureInfo.InvariantCulture))];
        await Task.Delay(TimeSpan.FromSeconds(5), token);
        long[] after = [.. (await nodeA.SafePsqlAsync(Database, Calls, token)).Split('|').Select(count => long.Parse(count, CultureInfo.InvariantCulture))];
        long calls = after[0] - before[0];
        long statements = after[1] - before[1];

        await Task.Delay(TimeSpan.FromSeconds(20) - fenced.Elapsed, token);
        // A full search of the ledger orders every grant by where it starts, which a search of the grants changed since
        // the last doesn't.
        string[] costs = (await nodeA.SafePsqlAsync(Database, """
            SELECT round(coalesce(sum(total_exec_time) FILTER (WHERE query LIKE '%worf.sequence_range_grant%'), 0))::bigint || '|' ||
                coalesce(sum(calls) FILTER (WHERE query LIKE '%ROWS BETWEEN UNBOUNDED PRECEDING AND%'), 0) || '|' ||
                (SELECT fenced_at IS NOT NULL FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass)
            FROM pg_stat_statements
            """, token)).Split('|');
        long searching = long.Parse(costs[0], CultureInfo.InvariantCulture);
        long fullSearches = long.Parse(costs[1], CultureInfo.InvariantCulture);
        TestContext.WriteLine($"statements on grants {searching} ms, {fullSearches} full searches; {calls} calls ran {statements} statements; u's inserts {unfenced} unfenced, {during} fenced");
        Assert.AreEqual("true", costs[2], "node_a's fence, waiting for node_c");

        // Over the 20 s from the reset, node_a searches its whole ledger of grants a few times at most, rather than at
        // each of the fence's checks, ten a second; node_b's calls as the fence waits run fewer than 30 statements each on
        // node_a, as they would if each took t_id_seq in, or found the range row's version changed by each check of the
        // fence; and the inserts take no more than twice as long, and a second, as without the fence.
        Assert.IsLessThan(5L, fullSearches, "node_a's full searches of its ledger of grants over the 20 s from the reset");
        Assert.IsTrue(calls >= 3 && statements < 30 * calls, $"node_b's {calls} calls in 5 s, which ran {statements} statements on node_a");
        Assert.IsLessThan(2 * unfenced + TimeSpan.FromSeconds(1), during, $"60 inserts into u during the fence, against {unfenced} without it");
    }

    /// <summary>
    /// A fenced allocator looks for grants that overlap among the sequence's alone, leaving out many of other sequences'
    /// over the same values, and looks before it locks the sequence's range row, which it holds only to lift the fence, so
    /// that an exchange seldom finds the row held as the fence waits; and it takes back the blocks a neighbor holds that
    /// it lost through the index on spans.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task FencedAllocatorsSearchOnlyTheSequencesGrants()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "shared_preload_libraries = 'worf,pg_stat_statements'\npg_stat_statements.track = all");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database,
            "CREATE EXTENSION pg_stat_statements; DO $$ BEGIN FOR g IN 1..100 LOOP EXECUTE format('CREATE SEQUENCE other_%s', g); END LOOP; END $$", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 10, token);

        // node_a granted 2,000 blocks of t_id_seq to a node long gone, and as many of each of 100 other sequences over the
        // same values.
        _ = await nodeA.SafePsqlAsync(Database, """
            INSERT INTO worf.sequence_range_grant (grant_id, request_id, seqoid, grantee_id, grantee_name, kind, lo, hi, state, granted_at)
            SELECT gen_random_uuid(), gen_random_uuid(), s, 1, 'gone', 'n', 900000000000000000 + g * 10, 900000000000000000 + g * 10 + 9, 'abandoned', now()
            FROM (SELECT 't_id_seq'::regclass UNION ALL SELECT format('other_%s', o)::regclass FROM generate_series(1, 100) o) q (s), generate_series(1, 2000) g;
            ANALYZE worf.sequence_range_grant;
            """, token);

        // node_b stops, and node_a loses its grants to node_b, and its record of node_b's reports, and resets: its fence
        // waits for node_b. Edits by hand lock the range row first, as every change to a sequence's ranges does.
        await nodeB.StopAsync("fast", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass FOR NO KEY UPDATE;
            DELETE FROM worf.sequence_range_grant WHERE seqoid = 't_id_seq'::regclass AND grantee_name = 'node_b';
            DELETE FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND node_name = 'node_b';
            """, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT pg_stat_statements_reset()", token);
        Stopwatch fenced = Stopwatch.StartNew();
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('t_id_seq')", token);

        // As the fence waits, node_a checks it again and again, and t_id_seq's range row is held at fewer than a quarter of
        // tries to lock it.
        int held = 0;
        int samples = 0;
        await using (NpgsqlConnection sampler = await nodeA.OpenAsync(Database, token))
        {
            Stopwatch sampling = Stopwatch.StartNew();
            while (sampling.Elapsed < TimeSpan.FromSeconds(10))
            {
                await using NpgsqlTransaction trying = await sampler.BeginTransactionAsync(token);
                await using NpgsqlCommand tryLock = new(
                    "SELECT count(*) FROM (SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass FOR NO KEY UPDATE SKIP LOCKED) l", sampler, trying);
                held += (long)(await tryLock.ExecuteScalarAsync(token))! == 0 ? 1 : 0;
                samples++;
                await trying.RollbackAsync(token);
                await Task.Delay(TimeSpan.FromMilliseconds(50), token);
            }
        }

        Assert.AreEqual("t", await nodeA.SafePsqlAsync(Database, "SELECT fenced_at IS NOT NULL FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token),
            "node_a's fence, waiting for node_b");

        // node_b starts, and node_a takes the blocks it holds back into its ledger, and lifts the fence.
        int lost = nodeA.ReadLog().Length;
        await nodeB.StartServerAsync(token);
        Assert.IsTrue(await nodeA.PollLogAsync(lost, "which this allocator had no record of; it is back in the ledger", token), "node_b's grants back in the ledger");
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT fenced_at IS NULL FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token),
            "node_a's fence, lifted");

        // Over the fence, each of node_a's searches of its ledger of grants reads about one buffer for each of t_id_seq's
        // grants, rather than many times that, as it would if it compared each grant of t_id_seq with every other, or with
        // every sequence's grants over the same values; and each check of a block taken back reads a few buffers, rather
        // than every grant of t_id_seq. Buffers are counted rather than the statements' time, which a loaded runner
        // stretches.
        const string Search = "query LIKE 'WITH g AS MATERIALIZED%worf.sequence_range_grant%'";
        string[] costs = (await nodeA.SafePsqlAsync(Database, $"""
            SELECT coalesce(sum(calls) FILTER (WHERE {Search}), 0) || '|' ||
                coalesce(sum(shared_blks_hit + shared_blks_read) FILTER (WHERE {Search}), 0) || '|' ||
                (sum(shared_blks_hit + shared_blks_read) FILTER (WHERE query LIKE '%reset_lsn IS NOT NULL)%OR EXISTS%') /
                    sum(calls) FILTER (WHERE query LIKE '%reset_lsn IS NOT NULL)%OR EXISTS%'))::bigint || '|' ||
                (SELECT count(*) FROM worf.sequence_range_grant WHERE seqoid = 't_id_seq'::regclass)
            FROM pg_stat_statements
            """, token)).Split('|');
        long searches = long.Parse(costs[0], CultureInfo.InvariantCulture);
        long searched = long.Parse(costs[1], CultureInfo.InvariantCulture);
        long checkedHeld = long.Parse(costs[2], CultureInfo.InvariantCulture);
        long grants = long.Parse(costs[3], CultureInfo.InvariantCulture);
        Assert.IsTrue(searches > 0 && searched < searches * grants * 3 / 2 && checkedHeld < 200 && held * 4 < samples,
            $"node_a's {searches} searches of its ledger over the {fenced.ElapsedMilliseconds} ms of the fence, which read {searched} buffers against " +
            $"t_id_seq's {grants} grants; the buffers each check of a block taken back read, {checkedHeld}; t_id_seq's range row, held at {held} of {samples} tries");
    }

    /// <summary>
    /// Creating, altering and attaching tables that use no managed sequence takes about as much of the server's time with
    /// 150 managed sequences as with one, as the guard reads the columns of every managed sequence at once rather than one
    /// sequence at a time. Each is measured once the range worker has checked the sequences just enabled, which takes a
    /// slow host longer than the statements, with its checks every ten minutes, in a session that ran a statement before,
    /// so that a backend's start isn't counted, by the CPU time log_statement_stats gives the statements, which the load
    /// of the other tests leaves alone as it doesn't their elapsed time.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task TablesChangeAsQuicklyWithManyManagedSequences()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        _ = await nodeA.SafePsqlAsync(Database, """
            DO $$ BEGIN FOR g IN 1..150 LOOP EXECUTE format('CREATE TABLE many_%s (id bigserial PRIMARY KEY)', g); END LOOP; END $$;
            CREATE TABLE parted (k int, v text, w int) PARTITION BY LIST (k);
            SELECT worf.enable_sequence_range('many_1_id_seq', participants := '{}', range_size := 1000);
            """, token);
        _ = await nodeA.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_check_interval = '10min'; SELECT pg_reload_conf()", token);
        int made = 0;
        async Task<(TimeSpan Elapsed, TimeSpan Cpu)> ChangeAsync()
        {
            // The range worker rests, waiting on its latch, at five looks in a row.
            int rested = 0;
            Stopwatch resting = Stopwatch.StartNew();
            while (rested < 5)
            {
                Assert.IsLessThan(TestNode.PollTimeout, resting.Elapsed, "the range worker's checks of the sequences enabled");
                rested = await nodeA.SafePsqlAsync(Database,
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE backend_type LIKE 'worf range %' AND wait_event_type = 'Extension'", token) == "1"
                    ? rested + 1 : 0;
                await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            }

            await using NpgsqlConnection changing = await nodeA.OpenAsync(Database, token);
            await using (NpgsqlCommand warm = new("SELECT count(*) FROM worf.sequence_ranges; SET log_statement_stats = on", changing))
            {
                _ = await warm.ExecuteNonQueryAsync(token);
            }

            int logged = nodeA.ReadLog().Length;
            Stopwatch timing = Stopwatch.StartNew();
            for (int i = 0; i < 5; i++, made++)
            {
                string table = string.Create(CultureInfo.InvariantCulture, $"plain_{made}");
                await using NpgsqlCommand change = new($"""
                    CREATE TABLE {table} (k int, v text);
                    ALTER TABLE {table} ADD COLUMN w int;
                    ALTER TABLE parted ATTACH PARTITION {table} FOR VALUES IN ({made.ToString(CultureInfo.InvariantCulture)});
                    """, changing);
                _ = await change.ExecuteNonQueryAsync(token);
            }

            TimeSpan elapsed = timing.Elapsed;
            await using (NpgsqlCommand off = new("SET log_statement_stats = off", changing))
            {
                _ = await off.ExecuteNonQueryAsync(token);
            }

            return (elapsed, await StatementsCpuAsync(nodeA, logged, changing.ProcessID, "SET log_statement_stats = off", token));
        }

        (TimeSpan few, TimeSpan fewCpu) = await ChangeAsync();
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT count(worf.enable_sequence_range(format('many_%s_id_seq', g)::regclass, participants := '{}', range_size := 1000)) FROM generate_series(2, 150) g",
            token);
        (TimeSpan many, TimeSpan manyCpu) = await ChangeAsync();
        TestContext.WriteLine($"5 tables created, altered and attached: {few.TotalMilliseconds:F0} ms, {fewCpu.TotalMilliseconds:F0} ms CPU with 1 managed sequence; {many.TotalMilliseconds:F0} ms, {manyCpu.TotalMilliseconds:F0} ms CPU with 150");
        Assert.IsLessThan((fewCpu * 3) + TimeSpan.FromMilliseconds(100), manyCpu, "the CPU time to create, alter and attach 5 tables with 150 managed sequences, against 1");
    }

    /// <summary>
    /// A call that would take longer than its time, as one that starts the roots of many sequences on a slow owner,
    /// takes in what it can in half its time and leaves the rest to the caller's next call, so that the owner keeps
    /// what each call took in, and every sequence starts.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task CallsLongerThanTheirTimeTakeInWhatTheyCan()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "log_min_messages = debug1");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        const string Sequences = "DO $$ BEGIN FOR g IN 1..60 LOOP EXECUTE format('CREATE SEQUENCE many_%s', g); END LOOP; END $$";
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, Sequences, token);
        }

        // Each root node_a starts takes 0.1 s, so that starting all 60 takes longer than a call's 5 s.
        _ = await nodeA.SafePsqlAsync(Database, """
            CREATE FUNCTION slow_start() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.1); RETURN NEW; END $$;
            CREATE TRIGGER slow_start BEFORE UPDATE ON worf.sequence_range FOR EACH ROW
                WHEN (OLD.state = 'collecting' AND NEW.state <> 'collecting') EXECUTE FUNCTION slow_start();
            """, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        int logpos = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT count(worf.enable_sequence_range(format('many_%s', g)::regclass, participants := '{node_b}', range_size := 1000)) FROM generate_series(1, 60) g",
            token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 60 FROM worf.sequence_ranges WHERE state = 'active'", token),
            "node_b drawing from blocks of every sequence");
        Assert.Contains("sequence range exchange with node node_b left", nodeA.ReadLog()[logpos..], "node_a's calls, which left sequences to the next");
    }

    /// <summary>
    /// A provider that takes a connection and its settings but never answers a call holds up no other exchange, and is
    /// called again after a delay that doubles for each such call in a row, though its probes answer.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ProvidersThatTakeNoCallBackOff()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "log_min_messages = debug1");
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100, max_range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);

        // node_b reaches node_c through a relay that passes a connection's settings and holds its calls.
        await using ProviderRelay relay = new(nodeC.Cluster.Port, holdCalls: true);
        int logpos = nodeB.ReadLog().Length;
        _ = await nodeB.SafePsqlAsync(Database, $"""
            UPDATE worf.node_interface SET if_dsn = 'host=127.0.0.1 port={relay.Port.ToString(CultureInfo.InvariantCulture)} dbname={Database} user={SuperUser} sslmode=disable gssencmode=disable'
            WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')
            """, token);
        // node_b draws small blocks from node_a meanwhile, for 30 s at least, and no insert waits for one. The server
        // times each insert.
        const string Insert = "INSERT INTO t (v) VALUES ('b')";
        string delays = "";
        await using (NpgsqlConnection writer = await nodeB.OpenAsync(Database, token))
        {
            // The session runs an insert before the timed ones, so that its backend's first use of worf, which a loaded host
            // may hold up for a second, isn't counted as a wait.
            await using (NpgsqlCommand logged = new($"{Insert}; SET log_min_duration_statement = 0", writer))
            {
                _ = await logged.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand insert = new(Insert, writer);
            Stopwatch waiting = Stopwatch.StartNew();
            TimeSpan looked = TimeSpan.Zero;
            while (waiting.Elapsed < TimeSpan.FromSeconds(90) && (waiting.Elapsed < TimeSpan.FromSeconds(30) || delays.Split(',').Length < 2))
            {
                _ = await insert.ExecuteNonQueryAsync(token);
                await Task.Delay(TimeSpan.FromMilliseconds(5), token);
                if (waiting.Elapsed - looked > TimeSpan.FromSeconds(1))
                {
                    looked = waiting.Elapsed;
                    delays = string.Join(',', nodeB.ReadLog()[logpos..].Split('\n')
                        .Where(line => line.Contains("exchange with node node_c is made again in ", StringComparison.Ordinal))
                        .Select(line => line[(line.IndexOf(" in ", StringComparison.Ordinal) + 4)..].Trim()));
                }
            }
        }

        string log = nodeB.ReadLog()[logpos..];
        List<double> waits = [.. log.Split('\n')
            .Where(line => line.Contains($": {Insert}", StringComparison.Ordinal) && line.Contains("  execute ", StringComparison.Ordinal))
            .Select(line => double.Parse(line[(line.IndexOf("duration: ", StringComparison.Ordinal) + 10)..line.IndexOf(" ms", StringComparison.Ordinal)],
                CultureInfo.InvariantCulture))];
        waits.Sort();
        Assert.IsGreaterThan(100, waits.Count, "node_b's inserts");
        Assert.IsLessThan(2500.0, waits[^1], "node_b's slowest insert, against the 5000 ms each call to node_c held up its calls to node_a");
        Assert.StartsWith("5000 ms,10000 ms", delays, "node_b's delays after its calls to node_c ran out of time, though its probe answered between them");
        Assert.Contains("exchange with node node_c answered a probe", log, "node_b's probe of node_c");
    }

    /// <summary>
    /// A neighbor that reports a route to the owner longer than any route can be has no route, and one that reports
    /// holding more Snowflake node numbers than there are holds none, so that a node keeps nothing of them, nor passes them
    /// on.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task LongRoutesCountAsNone()
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

        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 100)", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeC, "c_from_b", nodeB, token);
        await WaitActiveAsync(nodes, token);

        // node_c's calls to node_b go through a relay that keeps their text.
        await using ProviderRelay relay = new(nodeB.Cluster.Port);
        _ = await nodeC.SafePsqlAsync(Database, $"""
            UPDATE worf.node_interface SET if_dsn = 'host=127.0.0.1 port={relay.Port.ToString(CultureInfo.InvariantCulture)} dbname={Database} user={SuperUser} sslmode=disable gssencmode=disable'
            WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_b')
            """, token);
        const string Call = "SELECT worf.sequence_range_exchange(";
        string? call = null;
        Stopwatch capturing = Stopwatch.StartNew();
        while (call is null)
        {
            Assert.IsLessThan(TestNode.PollTimeout, capturing.Elapsed, "a call of node_c's through the relay");
            call = relay.Queries.LastOrDefault(query => query.StartsWith(Call, StringComparison.Ordinal));
            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }

        // The call made again with a route of 100,000 nodes that holds neither, at a distance of one, and 100,000 Snowflake
        // node numbers held.
        // The call LISTENs for node_b's words after it runs.
        const string Listen = "; LISTEN worf_sequence_range";
        Assert.EndsWith(Listen, call, "node_c's call");
        string literal = call[Call.Length..^(Listen.Length + 1)];
        JsonObject message = JsonNode.Parse(literal[1..^1].Replace("''", "'", StringComparison.Ordinal))!.AsObject();
        foreach (JsonNode? sequence in message["sequences"]!.AsArray())
        {
            sequence!["distance"] = 1;
            sequence["route"] = new JsonArray([.. Enumerable.Range(0, 100000).Select(hop => (JsonNode?)(1000000 + hop))]);
            sequence["holds"] = new JsonArray([.. Enumerable.Range(0, 100000).Select(number => (JsonNode?)number)]);
        }

        _ = await nodeB.SafePsqlAsync(Database, $"{Call}'{message.ToJsonString().Replace("'", "''", StringComparison.Ordinal)}')", token, SuperUser);
        Assert.AreEqual("t|t|t", await nodeB.SafePsqlAsync(Database, """
            SELECT route IS NULL, distance IS NULL, holds IS NULL FROM worf.sequence_range_node
            WHERE seqoid = 't_id_seq'::regclass AND node_name = 'node_c' AND via_node = node_id
            """, token), "node_c's route, distance and Snowflake node numbers, as node_b keeps them");
    }

    /// <summary>
    /// After its server restarts, a node draws nothing from a block its grantor granted until the grantor has answered,
    /// whatever calls nextval: a SELECT, an insert into a table whose defaults don't draw from the sequence, an insert into
    /// the sequence's table, a PL/pgSQL assignment and RETURN, a SQL function given the name as text, nextval of a name as
    /// text, a SELECT that also reads the sequence, a session or a SECURITY DEFINER function whose search path is worf's
    /// own, an UPDATE or a MERGE that sets an identity or a serial column to its default, a partitioned table's too, an
    /// INSERT, MERGE or COPY straight into a partition whose parent's identity column draws, on 17 and later, as into one
    /// attached in a session that wrote it before, a domain whose CHECK draws, a COPY whose default draws, a multi-row
    /// VALUES list, a CHECK constraint or BEFORE trigger condition, alone or of a data-modifying WITH's table beside a
    /// partition-routing statement, a stable PL/pgSQL function a generic plan's partition pruning calls as the plan starts,
    /// explained or not, beside a call of an unmanaged sequence, and a domain's CHECK that partition pruning coerces a
    /// value to, explained, a call through an operator, a domain's or table's CHECK that calls a SQL function the planner
    /// inlines, written as text or with a body of its own, or through another, or given the sequence's name as its
    /// argument, or naming the sequence without its schema, after the search path changed, a PL/pgSQL function's return to
    /// a domain whose CHECK draws, after its last expression named another sequence, and a polymorphic function's variable
    /// of an array or composite of such a domain, called first with another type; an UPDATE or a MERGE through a trigger
    /// whose C code draws from a sequence it names, as contrib's autoinc, and an INSERT, COPY or moving UPDATE that routes
    /// a row to a partition with such a trigger; and, with one sequence's grantor alone answered, a call worf can't tell
    /// the sequence of. A call C code starts as a statement runs, as a domain's CHECK that a record's or a value's input
    /// runs, waits as one whose sequence worf can't tell, or, where it may take the place of a call the statement names
    /// that starts later or never, for the sequences the domains' CHECKs draw from, as every call a statement names does
    /// while it calls a function that checks a value against a domain it names as text, as pg_input_is_valid does, though
    /// the domain's CHECK was added in the same session. Each waits, naming the grantor and the sequence, or, for a call
    /// whose sequence worf can't tell, as one a PL/pgSQL function computes the name of, saying so, here until
    /// statement_timeout while the grantor is down, or for twice worf.sequence_range_timeout, after which it fails naming
    /// the grantor. What draws nothing from those blocks doesn't wait, though domains whose CHECKs draw from them, or from
    /// a sequence they don't name, exist: a read of the sequence or of worf.sequence_ranges, pg_sequence_last_value, an
    /// EXPLAIN, nextval of a sequence no range manages, by name or as text, or of a temporary one, in a statement, beside a
    /// library's C function or not, an insert into its table, one whose other default calls such a function, a PL/pgSQL
    /// assignment, a BEFORE trigger, an EXECUTE's parameter, a CALL's argument, a COPY's WHERE or the CHECK of the domain
    /// of a column COPY reads, a multi-row VALUES list, alone or as an INSERT's rows or defaults, a CHECK constraint of a
    /// table an INSERT or COPY writes, a BEFORE trigger's condition, or a RETURNING list of an INSERT or UPDATE of a
    /// partitioned table, beside another table whose CHECK draws, an EXPLAIN whose pruning draws only from those, a COPY
    /// whose defaults draw from those, ALTER TABLE adding a serial column to a temporary table, an UPDATE or a MERGE that
    /// sets such a column to a value, a PL/pgSQL function's RETURN of a variable of a domain whose CHECK draws from a
    /// sequence no range manages, an UPDATE that sets a value already drawn from the active block, and nextval in a read-
    /// only transaction, which fails at once, as do, before 17, an INSERT, MERGE or COPY straight into a partition, which
    /// shares no identity column with its parent, and from 17 on an insert into a partition of a table attached as a
    /// partition. PostgreSQL's apply of a subscription applies a row whose domain draws from a sequence no range manages at
    /// once, and waits for one whose table's ALWAYS trigger's C code draws. ENABLE TRIGGER USER and ALL, which would leave
    /// guards firing only in sessions that write as origins do, are refused. Once the grantor is back, the node draws
    /// again.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [TestCategory("EveryVersion")]
    public async Task RestartedNodesDrawNothingBeforeTheirGrantorAnswers()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id bigserial PRIMARY KEY, v text); CREATE TABLE other (id bigint);
                CREATE TABLE ti (id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, v text);
                CREATE TABLE tp (id bigint GENERATED BY DEFAULT AS IDENTITY, v text) PARTITION BY RANGE (id);
                CREATE TABLE tp1 PARTITION OF tp FOR VALUES FROM (MINVALUE) TO (MAXVALUE);
                CREATE TABLE tl (id bigint GENERATED BY DEFAULT AS IDENTITY, v text) PARTITION BY LIST (v);
                CREATE TABLE tl1 PARTITION OF tl FOR VALUES IN ('a');
                CREATE TABLE tq (id bigint GENERATED BY DEFAULT AS IDENTITY, v text) PARTITION BY LIST (v);
                CREATE TABLE tq1 PARTITION OF tq FOR VALUES IN ('a');
                """, token);
        }

        using TemporaryDirectory files = new($"worf-sequence-range-copy-{Guid.NewGuid():N}");
        string rows = Path.Join(files.FullName, "rows.txt").Replace('\\', '/');
        await File.WriteAllTextAsync(rows, "x\n", token);
        string rowsA = Path.Join(files.FullName, "rows_a.txt").Replace('\\', '/');
        await File.WriteAllTextAsync(rowsA, "a\n", token);
        _ = await nodeB.SafePsqlAsync(Database, """
            CREATE FUNCTION drawn() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN nextval('t_id_seq'); END $$;
            CREATE FUNCTION drawn_from(name text) RETURNS bigint LANGUAGE sql AS 'SELECT nextval($1)';
            CREATE FUNCTION pinned_draw() RETURNS bigint LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
                AS 'SELECT nextval(''public.t_id_seq'')';
            CREATE FUNCTION computed_draw(name text) RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RETURN nextval(name); END $$;
            CREATE SEQUENCE plain;
            CREATE TABLE plain_t (id bigserial, v text);
            CREATE DOMAIN dd AS bigint CHECK (nextval('public.t_id_seq') > 0);
            CREATE TABLE trig_t (id bigint, v text);
            CREATE FUNCTION plain_id() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.id := nextval('public.plain'); RETURN NEW; END $$;
            CREATE TRIGGER plain_id BEFORE INSERT ON trig_t FOR EACH ROW EXECUTE FUNCTION plain_id();
            CREATE PROCEDURE pr(x bigint) LANGUAGE plpgsql AS $$ BEGIN END $$;
            CREATE DOMAIN pd AS text CHECK (nextval('public.plain') > 0);
            CREATE TABLE pd_t (v pd);
            CREATE TABLE pq (k int, v text) PARTITION BY LIST (k);
            CREATE TABLE pq1 PARTITION OF pq FOR VALUES IN (1);
            CREATE TABLE pq2 PARTITION OF pq FOR VALUES IN (2);
            INSERT INTO pq VALUES (1, 'a');
            CREATE TABLE pp (k int, x bigint) PARTITION BY LIST (k);
            CREATE TABLE pp1 PARTITION OF pp FOR VALUES IN (1);
            CREATE TABLE pp2 PARTITION OF pp FOR VALUES IN (2);
            CREATE FUNCTION sneaky(i int) RETURNS int LANGUAGE plpgsql STABLE AS $$ DECLARE v bigint; BEGIN v := nextval('public.t_id_seq'); RETURN i; END $$;
            CREATE DOMAIN posint AS int CHECK (VALUE > 0);
            CREATE TABLE tpos (x posint, y bigint DEFAULT nextval('plain'));
            CREATE TABLE tint (x int, y bigint DEFAULT nextval('plain'));
            INSERT INTO pp VALUES (1, 1);
            CREATE FUNCTION passed() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
            CREATE TABLE ckp (v text CHECK (nextval('plain') > 0));
            CREATE TABLE ckm (v text CHECK (nextval('public.t_id_seq') > 0));
            CREATE TABLE tw (v text);
            CREATE TRIGGER tw_when BEFORE INSERT ON tw FOR EACH ROW WHEN (nextval('plain') > 0) EXECUTE FUNCTION passed();
            CREATE TABLE twm (v text);
            CREATE TRIGGER twm_when BEFORE INSERT ON twm FOR EACH ROW WHEN (nextval('public.t_id_seq') > 0) EXECUTE FUNCTION passed();
            CREATE DOMAIN ddk AS int CHECK (nextval('public.t_id_seq') > 0);
            CREATE FUNCTION fd(i int) RETURNS ddk LANGUAGE sql STABLE AS 'SELECT i::ddk';
            CREATE TABLE tl2 (id bigint NOT NULL, v text);
            CREATE TABLE tlt (id bigint NOT NULL, v text) PARTITION BY LIST (v);
            CREATE TABLE tlt1 PARTITION OF tlt FOR VALUES IN ('t');
            CREATE DOMAIN pdk AS int CHECK (nextval('public.plain') > 0);
            CREATE FUNCTION fdraw() RETURNS bigint LANGUAGE sql AS 'SELECT nextval(''public.ti_id_seq'')';
            CREATE FUNCTION freturn() RETURNS bigint LANGUAGE sql RETURN nextval('public.ti_id_seq');
            DO $$ BEGIN EXECUTE 'CREATE FUNCTION fatomic() RETURNS bigint LANGUAGE sql BEGIN ATOMIC SELECT nextval(''public.ti_id_seq''); END'; END $$;
            CREATE FUNCTION fnested() RETURNS bigint LANGUAGE sql AS 'SELECT fatomic()';
            CREATE DOMAIN ddf AS int CHECK (fdraw() > 0);
            CREATE DOMAIN ddr AS int CHECK (freturn() > 0);
            CREATE DOMAIN ddn AS int CHECK (fnested() > 0);
            CREATE TABLE ckf (v text CHECK (fdraw() > 0));
            CREATE OPERATOR @@@ (RIGHTARG = regclass, FUNCTION = nextval);
            CREATE DOMAIN ddr2 AS int CHECK (nextval('public.ti_id_seq') > 0);
            CREATE FUNCTION fret(i int) RETURNS ddr2 LANGUAGE plpgsql AS $$
                DECLARE x int; BEGIN SELECT CASE WHEN i < 0 THEN nextval('plain')::int ELSE 1 END INTO x; RETURN x; END $$;
            CREATE SCHEMA other;
            CREATE SEQUENCE other.ti_id_seq;
            CREATE FUNCTION fdu() RETURNS bigint LANGUAGE sql AS 'SELECT nextval(''ti_id_seq'')';
            CREATE DOMAIN ddu AS int CHECK (fdu() > 0);
            CREATE DOMAIN ddt AS int CHECK (drawn_from('public.ti_id_seq') > 0);
            CREATE EXTENSION fuzzystrmatch;
            CREATE TABLE fz (id bigserial, s text DEFAULT soundex(random()::text));
            CREATE DOMAIN tplain AS int;
            CREATE DOMAIN tdom AS int CHECK (nextval('public.ti_id_seq') > 0);
            CREATE FUNCTION fpoly(x anyelement) RETURNS anyelement LANGUAGE plpgsql AS $$ BEGIN RETURN x; END $$;
            CREATE TYPE cdk AS (a ddk);
            CREATE FUNCTION fsw(i int) RETURNS int LANGUAGE sql AS 'SELECT (nextval(''public.ti_id_seq'') > 0)::int + i';
            CREATE DOMAIN dsw AS int CHECK (fsw(VALUE) > 0);
            CREATE EXTENSION autoinc;
            CREATE TABLE ai (id int, v text);
            CREATE TRIGGER ai_id BEFORE INSERT ON ai FOR EACH ROW EXECUTE FUNCTION autoinc(id, ti_id_seq);
            CREATE FUNCTION fp6(i int, x anyelement) RETURNS int LANGUAGE plpgsql AS $$
                DECLARE y x%TYPE; z bigint;
                BEGIN
                    IF i < 0 THEN z := nextval('plain'); END IF;
                    SELECT ARRAY[CASE WHEN i < 0 THEN nextval('plain')::int ELSE 1 END] INTO y;
                    RETURN 1;
                END $$;
            CREATE FUNCTION fp7(i int, x anyelement) RETURNS int LANGUAGE plpgsql AS $$
                DECLARE y x%TYPE; z bigint;
                BEGIN
                    IF i < 0 THEN z := nextval('plain'); END IF;
                    SELECT CASE WHEN i < 0 THEN nextval('plain')::int ELSE 1 END INTO y;
                    RETURN 1;
                END $$;
            CREATE TYPE ctd AS (a tdom);
            CREATE TABLE au (id int, v text);
            INSERT INTO au VALUES (1, 'a');
            CREATE TRIGGER au_id BEFORE INSERT OR UPDATE ON au FOR EACH ROW EXECUTE FUNCTION autoinc(id, ti_id_seq);
            CREATE TABLE pa (id int, v text) PARTITION BY LIST (v);
            CREATE TABLE pa1 PARTITION OF pa FOR VALUES IN ('a');
            CREATE TRIGGER pa1_id BEFORE INSERT ON pa1 FOR EACH ROW EXECUTE FUNCTION autoinc(id, ti_id_seq);
            CREATE TABLE pb (k int, id int, v text) PARTITION BY LIST (k);
            CREATE TABLE pb1 PARTITION OF pb FOR VALUES IN (1);
            CREATE TABLE pb2 PARTITION OF pb FOR VALUES IN (2);
            CREATE TRIGGER pb2_id BEFORE INSERT ON pb2 FOR EACH ROW EXECUTE FUNCTION autoinc(id, ti_id_seq);
            INSERT INTO pb VALUES (1, 1, 'b');
            CREATE FUNCTION frp(i int) RETURNS pdk LANGUAGE plpgsql AS $$ BEGIN RETURN i; END $$;
            CREATE TABLE npd (v pdk);
            CREATE TABLE ai2 (id int, v text);
            CREATE TRIGGER ai2_id BEFORE INSERT ON ai2 FOR EACH ROW EXECUTE FUNCTION autoinc(id, 'public.ti_id_seq');
            ALTER TABLE ai2 ENABLE ALWAYS TRIGGER ai2_id;
            CREATE TABLE auu (k int PRIMARY KEY, id int, v text);
            INSERT INTO auu VALUES (1, 1, 'a');
            CREATE TRIGGER auu_id BEFORE UPDATE ON auu FOR EACH ROW EXECUTE FUNCTION autoinc(id, ti_id_seq);
            CREATE TABLE apl (id int, v text);
            CREATE TRIGGER apl_id BEFORE INSERT ON apl FOR EACH ROW EXECUTE FUNCTION autoinc(id, 'public.plain');
            ALTER TABLE apl ENABLE ALWAYS TRIGGER apl_id;
            """, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100);
            SELECT worf.enable_sequence_range('ti_id_seq', participants := '{node_b}', range_size := 100);
            SELECT worf.enable_sequence_range('tp_id_seq', participants := '{node_b}', range_size := 100);
            SELECT worf.enable_sequence_range('tl_id_seq', participants := '{node_b}', range_size := 100);
            SELECT worf.enable_sequence_range('tq_id_seq', participants := '{node_b}', range_size := 100);
            """, token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 10, token);
        _ = await nodeB.SafePsqlAsync(Database, """
            INSERT INTO ti (v) SELECT 'b' FROM generate_series(1, 10); INSERT INTO tp (v) SELECT 'b' FROM generate_series(1, 10);
            INSERT INTO tl (v) SELECT 'a' FROM generate_series(1, 10);
            """, token);

        await nodeA.StopAsync("fast", token);
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        int version = int.Parse(await nodeB.SafePsqlAsync(Database, "SHOW server_version_num", token), CultureInfo.InvariantCulture);
        bool sharedIdentity = version >= 170000;
        if (!sharedIdentity)
        {
            // Before 17, a partition can have an identity column of its own, which an insert into it draws from.
            _ = await nodeB.SafePsqlAsync(Database, "ALTER TABLE tq1 ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY", token);
        }

        const string Generic = "SET plan_cache_mode = force_generic_plan; PREPARE q(int) AS";

        // Each form is checked, and what fails is reported together.
        List<string> problems = [];
        (string Draw, string? Sequence)[] forms = [.. new (string, string?)[]
        {
            ("SELECT nextval('t_id_seq')", "t_id_seq"), ("INSERT INTO other VALUES (nextval('t_id_seq'))", "t_id_seq"),
            ("INSERT INTO t (v) VALUES ('x')", "t_id_seq"), ("SELECT nextval('t_id_seq'), last_value FROM t_id_seq", "t_id_seq"),
            ("SET search_path = pg_catalog, pg_temp; SELECT nextval('public.t_id_seq')", "t_id_seq"), ("SELECT pinned_draw()", "t_id_seq"),
            ("UPDATE ti SET id = DEFAULT WHERE id = (SELECT min(id) FROM ti) RETURNING id", "ti_id_seq"),
            ("MERGE INTO ti USING (SELECT 1) s ON true WHEN MATCHED THEN UPDATE SET id = DEFAULT", "ti_id_seq"),
            ("UPDATE t SET id = DEFAULT WHERE id = (SELECT min(id) FROM t)", "t_id_seq"),
            ("UPDATE tp SET id = DEFAULT WHERE id = (SELECT min(id) FROM tp) RETURNING id", "tp_id_seq"),
            ("MERGE INTO tp USING (SELECT 1) s ON true WHEN MATCHED THEN UPDATE SET id = DEFAULT", "tp_id_seq"),
            ("SELECT nextval('plain')::dd", "t_id_seq"), ($"COPY t (v) FROM '{rows}'", "t_id_seq"),
            ("DO $$ DECLARE x bigint; BEGIN x := nextval('t_id_seq'); END $$", "t_id_seq"), ("SELECT drawn()", "t_id_seq"),
            ("SELECT drawn_from('t_id_seq')", "t_id_seq"), ("SELECT nextval('t_id_seq'::text)", "t_id_seq"),
            ("SELECT nextval('public.\"t_id_seq\"'::text)", "t_id_seq"), ("SELECT computed_draw('t_id_seq')", null),
            ($"{Generic} SELECT x FROM pp WHERE k = sneaky($1) AND x <> nextval('plain'); EXECUTE q(1)", "t_id_seq"),
            ($"{Generic} SELECT nextval('plain') FROM pp WHERE k = sneaky($1); EXECUTE q(1)", "t_id_seq"),
            ($"{Generic} SELECT nextval('plain') FROM pp WHERE k = sneaky($1); EXPLAIN EXECUTE q(1)", "t_id_seq"),
            ($"{Generic} SELECT x FROM pp WHERE k = $1::ddk; EXPLAIN EXECUTE q(1)", "t_id_seq"),
            ($"{Generic} SELECT x FROM pp WHERE k = fd($1); EXPLAIN EXECUTE q(1)", "t_id_seq"),
            ("EXPLAIN SELECT x FROM pp WHERE k = 1::ddk", "t_id_seq"), ("EXPLAIN SELECT x FROM pp WHERE k = fd(1)", "t_id_seq"),
            ("INSERT INTO other VALUES (nextval('t_id_seq')), (nextval('t_id_seq'))", "t_id_seq"),
            ("INSERT INTO ckm VALUES ('c')", "t_id_seq"), ($"COPY ckm (v) FROM '{rows}'", "t_id_seq"), ("INSERT INTO twm VALUES ('c')", "t_id_seq"),
            ("WITH c AS (INSERT INTO ckm VALUES ('c')) INSERT INTO pq VALUES (1, 'w') RETURNING nextval('plain')", "t_id_seq"),
            ("WITH c AS (INSERT INTO twm VALUES ('c')) INSERT INTO pq VALUES (1, 'w') RETURNING nextval('plain')", "t_id_seq"),
            ("WITH c AS (SELECT * FROM (VALUES (nextval('public.t_id_seq')), (nextval('public.t_id_seq'))) v (n)) INSERT INTO pq SELECT 1, 'r' FROM c RETURNING nextval('plain')",
                "t_id_seq"),
            ("SELECT nextval('plain') FROM jsonb_to_record('{\"a\":1}') AS j(a ddk)", "t_id_seq"),
            ("SELECT nextval('plain') FROM json_to_recordset('[{\"a\":1}]') AS j(a ddk)", "t_id_seq"),
            ("INSERT INTO other SELECT nextval('plain') FROM jsonb_to_record('{\"a\":1}') AS j(a ddk)", "t_id_seq"),
            ("EXPLAIN ANALYZE SELECT nextval('plain') FROM jsonb_to_record('{\"a\":1}') AS j(a ddk)", "t_id_seq"),
            ("DO $$ BEGIN PERFORM nextval('plain') FROM jsonb_to_record('{\"a\":1}') AS j(a ddk); END $$", "t_id_seq"),
            ("INSERT INTO ckp SELECT 'x' FROM jsonb_to_record('{\"a\":1}') AS j(a ddk) WHERE random() < 0", "t_id_seq"),
            ("INSERT INTO pq SELECT 1, 'r' FROM jsonb_to_record('{\"a\":1}') AS j(a ddk) RETURNING nextval('plain')", "t_id_seq"),
            ("SELECT 1 FROM jsonb_to_record('{\"a\":1}') AS j(a ddk) UNION ALL SELECT 1 FROM (VALUES (nextval('plain')), (nextval('plain'))) v(n) LIMIT 1", "t_id_seq"),
            ("INSERT INTO ckp SELECT 'x' FROM jsonb_to_record('{\"a\":1}') AS j(a ddf) WHERE random() < 0", "ti_id_seq"),
            ("INSERT INTO ckp SELECT 'x' FROM jsonb_to_record('{\"a\":1}') AS j(a ddr) WHERE random() < 0", "ti_id_seq"),
            ("INSERT INTO ckp SELECT 'x' FROM jsonb_to_record('{\"a\":1}') AS j(a ddn) WHERE random() < 0", "ti_id_seq"),
            ("INSERT INTO ckf VALUES ('x') RETURNING CASE WHEN random() < 0 THEN nextval('plain') END", "ti_id_seq"),
            ("SELECT @@@ 'public.ti_id_seq'::regclass, CASE WHEN random() < 0 THEN nextval('plain') END", "ti_id_seq"),
            ("SELECT fret(1)", "ti_id_seq"),
            ("EXPLAIN SELECT 1::ddu; SET search_path = other, public; INSERT INTO ckp SELECT 'x' FROM jsonb_to_record('{\"a\":1}') AS j(a ddu) WHERE random() < 0",
                "ti_id_seq"),
            ("INSERT INTO ckp SELECT 'x' FROM jsonb_to_record('{\"a\":1}') AS j(a ddt) WHERE random() < 0", "ti_id_seq"),
            ("SELECT fpoly(NULL::tplain); SELECT fpoly(NULL::tdom)", "ti_id_seq"),
            ("INSERT INTO ai (v) VALUES ('x')", "ti_id_seq"),
            ("SELECT fp6(-1, NULL::int[]); SELECT fp6(1, NULL::tdom[])", "ti_id_seq"),
            ("UPDATE au SET id = NULL RETURNING id", "ti_id_seq"), ("MERGE INTO au USING (SELECT 1) s ON true WHEN MATCHED THEN UPDATE SET id = 0", "ti_id_seq"),
            ("INSERT INTO pa (v) VALUES ('a') RETURNING id", "ti_id_seq"), ($"COPY pa (v) FROM '{rowsA}'", "ti_id_seq"),
            ("UPDATE pb SET k = 2, id = NULL WHERE k = 1", "ti_id_seq"),
            ("SELECT record_in('(1)'::cstring, 'cdk'::regtype, -1) FROM (VALUES (nextval('plain')), (nextval('plain'))) v(n) LIMIT 1", "t_id_seq"),
            ("SELECT domain_in('1'::cstring, 'ddk'::regtype, -1) FROM (VALUES (nextval('plain')), (nextval('plain'))) v(n) LIMIT 1", "t_id_seq"),
            ("EXPLAIN SELECT 1::dsw; CREATE OR REPLACE FUNCTION fsw(i int) RETURNS int LANGUAGE sql AS 'SELECT (nextval(''plain'') > 0)::int + i'; " +
                "INSERT INTO ckp SELECT 'x' FROM jsonb_to_record('{\"a\":1}') AS j(a dsw) WHERE random() < 0", "ti_id_seq"),
        }.Concat(version >= 160000 ? [
            ("CREATE FUNCTION valid_input(text, text) RETURNS boolean LANGUAGE internal STABLE STRICT AS 'pg_input_is_valid'; " +
                "SELECT valid_input('1', 'ddk') FROM (VALUES (nextval('plain')), (nextval('plain'))) v(n) LIMIT 1", "t_id_seq"),
            ("SELECT pg_input_is_valid('1', 'ddk') FROM (VALUES (nextval('plain')), (nextval('plain'))) v(n) LIMIT 1", "t_id_seq"),
            ("SELECT nextval('plain'), pg_input_is_valid('1', 'ddk')", "t_id_seq"), ("SELECT nextval('plain'), * FROM pg_input_error_info('1', 'ddk')", "t_id_seq"),
        ] : []).Concat(sharedIdentity ? [
            // A variable %TYPE of a composite argument, which PL/pgSQL takes from 17 on.
            ("SELECT fp7(-1, NULL::int); SELECT fp7(1, NULL::ctd)", "ti_id_seq"),
            ("INSERT INTO tp1 (v) VALUES ('x') RETURNING id", "tp_id_seq"),
            ("MERGE INTO tp1 USING (SELECT 1) s ON false WHEN NOT MATCHED THEN INSERT (v) VALUES ('m')", "tp_id_seq"),
            ($"COPY tp1 (v) FROM '{rows}'", "tp_id_seq"),
            ("INSERT INTO tl2 SELECT max(id), 'u' FROM tl; ALTER TABLE tl ATTACH PARTITION tl2 FOR VALUES IN ('u', 'v'); INSERT INTO tl2 (v) VALUES ('v') RETURNING id",
                "tl_id_seq"),
        ] : []).Concat([
            // An INSERT's ON CONFLICT DO UPDATE fires the update trigger, whose C code, contrib's autoinc, draws, and so
            // does one, or an UPDATE, in a data-modifying WITH.
            ("INSERT INTO auu VALUES (1, 5, 'b') ON CONFLICT (k) DO UPDATE SET id = NULL RETURNING id", "ti_id_seq"),
            ("WITH u AS (INSERT INTO auu VALUES (1, 6, 'c') ON CONFLICT (k) DO UPDATE SET id = NULL RETURNING id) SELECT id FROM u", "ti_id_seq"),
            ("WITH u AS (UPDATE au SET id = NULL RETURNING id) SELECT id FROM u", "ti_id_seq"),
            // A DO block whose variable is of a domain that draws, right after another DO block, whose compiled state its
            // own may take the place of.
            ("DO $$ DECLARE z bigint; BEGIN z := nextval('plain'); END $$; DO $$ DECLARE y tdom; BEGIN y := 1; END $$", "ti_id_seq"),
        ])];

        // Each form waits out its time, so they run in four lanes at once; forms that update the same table share one, so
        // that none waits on another's row lock rather than for node_a.
        async Task<string?> TryAsync(string draw, string? sequence)
        {
            // A form that draws and then waits draws all the same, as its sequence's position shows.
            string position = sequence is null ? "" : $"SET lock_timeout = '2s'; SELECT last_value || '/' || is_called FROM {sequence}";
            string before = sequence is null ? "" : (await nodeB.SafePsqlAsync(Database, position, token)).Split('\n')[^1];
            string where;
            try
            {
                _ = await nodeB.SafePsqlAsync(Database, TimedLast(draw), token);
                return $"{draw}: drew";
            }
            catch (PostgresException refused) when (refused.SqlState == "57014")
            {
                where = refused.Where ?? "";
            }
            catch (ArgumentException)
            {
                // The statement ran, and returned a value of a type created after the connection's types were read.
                return $"{draw}: drew";
            }
            catch (PostgresException failed)
            {
                return $"{draw}: failed with {failed.SqlState} {failed.MessageText}, rather than wait";
            }

            if (sequence is not null && (await nodeB.SafePsqlAsync(Database, position, token)).Split('\n')[^1] is string after && after != before)
            {
                return $"{draw}: drew, from {before} to {after}, and then waited";
            }

            return !where.Contains(sequence is null ? "waiting for node node_a, which granted sequence" : $"waiting for node node_a, which granted sequence {sequence}'s block",
                StringComparison.Ordinal) || where.Contains("as worf can't tell which sequence", StringComparison.Ordinal) != (sequence is null)
                ? $"{draw}: waited {where}"
                : null;
        }

        string?[] found = new string?[forms.Length];
        async Task LaneAsync(int lane)
        {
            for (int i = 0; i < forms.Length; i++)
            {
                string draw = forms[i].Draw;
                int laneOf = draw.Contains(" ti ", StringComparison.Ordinal) ? 0 : draw.Contains(" tp ", StringComparison.Ordinal) ? 1
                    : draw.StartsWith("UPDATE t ", StringComparison.Ordinal) ? 2
                    : draw.Contains(" au ", StringComparison.Ordinal) || draw.Contains(" auu ", StringComparison.Ordinal) ? 3 : i % 4;
                if (laneOf == lane)
                {
                    found[i] = await TryAsync(draw, forms[i].Sequence);
                }
            }
        }

        await Task.WhenAll(Enumerable.Range(0, 4).Select(LaneAsync));
        problems.AddRange(found.OfType<string>());

        // What draws nothing from the blocks doesn't wait though domains whose CHECKs draw from them, or from a sequence
        // they don't name, exist.
        _ = await nodeB.SafePsqlAsync(Database, "CREATE DOMAIN du AS int CHECK (nextval(('pla' || 'in')::regclass) > 0)", token);
        foreach (string reads in new[]
        {
            "SELECT last_value FROM t_id_seq", "SELECT count(*) FROM worf.sequence_ranges", "SELECT pg_sequence_last_value('t_id_seq')",
            "EXPLAIN SELECT nextval('t_id_seq')", "SELECT nextval('plain')", "INSERT INTO plain_t (v) VALUES ('x')", "CREATE TEMP SEQUENCE tmp; SELECT nextval('tmp')",
            "SELECT nextval('plain'), soundex(random()::text)", "INSERT INTO fz DEFAULT VALUES",
            $"COPY plain_t (v) FROM '{rows}'", $"CREATE TEMP TABLE tt (id serial, v text); COPY tt (v) FROM '{rows}'",
            "CREATE TEMP TABLE ta (v text); INSERT INTO ta VALUES ('x'); ALTER TABLE ta ADD COLUMN n bigserial",
            "DO $$ DECLARE x bigint; BEGIN x := nextval('plain'); END $$", "INSERT INTO trig_t (v) VALUES ('x')",
            "SELECT nextval('plain'::text)", "PREPARE pe(bigint) AS SELECT $1; EXECUTE pe(nextval('plain'))", "CALL pr(nextval('plain'))",
            $"COPY plain_t (v) FROM '{rows}' WHERE nextval('plain') > 0", $"COPY pd_t (v) FROM '{rows}'",
            "INSERT INTO pq VALUES (1, 'x') RETURNING nextval('plain')", "UPDATE pq SET v = 'y' RETURNING nextval('plain')",
            "UPDATE pq SET k = 2 WHERE k = 1 RETURNING nextval('plain')", "UPDATE pq SET k = 1 WHERE k = 2 RETURNING nextval('plain')",
            "INSERT INTO pq SELECT 1, 'j' FROM pp a JOIN pp b ON a.k = b.k WHERE a.k = 1 RETURNING nextval('plain')",
            "INSERT INTO tint (x, y) VALUES (1, nextval('plain')), (2, nextval('plain'))", "INSERT INTO tint (x, y) VALUES (1, DEFAULT), (2, DEFAULT)",
            "SELECT * FROM (VALUES (nextval('plain')), (nextval('plain'))) v", "INSERT INTO ckp VALUES ('x')", $"COPY ckp (v) FROM '{rows}'",
            "EXPLAIN SELECT nextval('t_id_seq') FROM pp WHERE k = 1::pdk", "EXPLAIN INSERT INTO t (v) SELECT 'x' FROM pp WHERE k = 1::pdk",
            $"{Generic} SELECT nextval('t_id_seq') FROM pp WHERE k = $1::pdk; EXPLAIN EXECUTE q(1)",
            "INSERT INTO tw VALUES ('x')",
            "UPDATE ti SET id = id WHERE id = (SELECT min(id) FROM ti)", "UPDATE t SET id = id WHERE id = (SELECT min(id) FROM t)",
            "UPDATE ti SET (id, v) = (id, 'w') WHERE id = (SELECT min(id) FROM ti)",
            "MERGE INTO ti USING (SELECT min(id) AS m FROM ti) s ON ti.id = s.m WHEN MATCHED THEN UPDATE SET id = s.m",
            "SELECT frp(1)",
            "DELETE FROM ti WHERE id = (SELECT max(id) FROM ti); UPDATE ti SET id = id + 1 WHERE id = (SELECT max(id) FROM ti)",
            // An update of a table whose argument trigger fires only on insert, an upsert that conflicts with no row of a
            // table whose update trigger draws, and an UPDATE of such a table that matches no row.
            "UPDATE ai SET v = 'y'", "INSERT INTO auu VALUES (99, 5, 'n') ON CONFLICT (k) DO UPDATE SET id = NULL", "UPDATE auu SET id = NULL WHERE k = -1",
        })
        {
            try
            {
                _ = await nodeB.SafePsqlAsync(Database, $"SET statement_timeout = '2s'; {reads}", token);
            }
            catch (PostgresException failed)
            {
                problems.Add($"{reads}: failed with {failed.SqlState} {failed.MessageText} {failed.Where}");
            }
        }

        // ENABLE TRIGGER USER and ALL, which would leave a partition's guards firing only in sessions that write as origins
        // do, are refused.
        foreach (string enables in new[] { "ALTER TABLE tp1 ENABLE TRIGGER USER", "ALTER TABLE tp1 ENABLE TRIGGER ALL", "ALTER TABLE ti ENABLE TRIGGER ALL" })
        {
            string outcome;
            try
            {
                _ = await nodeB.SafePsqlAsync(Database, enables, token);
                outcome = "succeeded";
            }
            catch (PostgresException refused) when (refused.SqlState == "55000")
            {
                outcome = "refused";
            }

            if (outcome != "refused")
            {
                problems.Add($"{enables}: {outcome}");
            }
        }

        // A call worf can't tell the sequence of, waiting, goes on waiting for the grantors of the other sequences once
        // node_a's word on t_id_seq alone has come, and a change to the ranges has committed, which has the waiting
        // session read the words again.
        // The server's start, as a node it has no record of is taken as heard from then.
        string nodeAId = await nodeB.SafePsqlAsync(Database, "SELECT node_id FROM worf.node WHERE node_name = 'node_a'", token);
        string started = await nodeB.SafePsqlAsync(Database,
            "SELECT ((extract(epoch FROM worf.sequence_range_heard(0, true, '-infinity', true)) - 946684800) * 1000000)::bigint", token);
        string Confirm(string start) => $"UPDATE worf.sequence_range_node SET confirmed_start = {start} WHERE seqoid = 't_id_seq'::regclass AND node_id = {nodeAId}";
        const string Unnamed = "SELECT computed_draw('ti_id_seq')";
        Task<string?> partly = Task.Run(async () =>
        {
            try
            {
                _ = await nodeB.SafePsqlAsync(Database, $"SET statement_timeout = '8s'; {Unnamed}", token);
                return $"{Unnamed}, as t_id_seq's grantor alone answered: drew";
            }
            catch (PostgresException refused) when (refused.SqlState == "57014")
            {
                return (refused.Where ?? "").Contains("waiting for node node_a, which granted sequence ti_id_seq's block", StringComparison.Ordinal)
                    ? null : $"{Unnamed}, as t_id_seq's grantor alone answered: waited {refused.Where}";
            }
        }, token);
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        _ = await nodeB.SafePsqlAsync(Database, Confirm(started), token);
        _ = await nodeB.SafePsqlAsync(Database, "CREATE SEQUENCE confirm_bump; SELECT worf.enable_sequence_range('confirm_bump', participants := '{}')", token);
        if (await partly is string drewPartly)
        {
            problems.Add(drewPartly);
        }

        _ = await nodeB.SafePsqlAsync(Database, Confirm("NULL"), token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.disable_sequence_range('confirm_bump'); DROP SEQUENCE confirm_bump", token);

        // A session that read, as it drew, that a function checks no domain it isn't given, reads it again once another
        // session makes it one that may: here an internal function's alias of pg_input_is_valid.
        if (version >= 160000)
        {
            _ = await nodeB.SafePsqlAsync(Database, "CREATE FUNCTION h2(a text, b text) RETURNS boolean LANGUAGE sql AS 'SELECT true'", token);
            await using NpgsqlConnection held = await nodeB.OpenAsync(Database, token);
            await using (NpgsqlCommand first = new("SELECT nextval('plain'), CASE WHEN random() < 0 THEN h2('1', 'x') END", held))
            {
                _ = await first.ExecuteScalarAsync(token);
            }

            _ = await nodeB.SafePsqlAsync(Database,
                "CREATE OR REPLACE FUNCTION h2(a text, b text) RETURNS boolean LANGUAGE internal STABLE STRICT AS 'pg_input_is_valid'", token);
            const string Replaced = "SELECT h2('1', 'ddk') FROM (VALUES (nextval('plain')), (nextval('plain'))) v(n) LIMIT 1";
            try
            {
                await using NpgsqlCommand second = new($"SET statement_timeout = '2s'; {Replaced}", held);
                _ = await second.ExecuteScalarAsync(token);
                problems.Add($"{Replaced}, in a session that had read h2 before it was replaced: drew");
            }
            catch (PostgresException refused) when (refused.SqlState == "57014")
            {
                if (!(refused.Where ?? "").Contains("waiting for node node_a, which granted sequence t_id_seq's block", StringComparison.Ordinal))
                {
                    problems.Add($"{Replaced}, in a session that had read h2 before it was replaced: waited {refused.Where}");
                }
            }

            _ = await nodeB.SafePsqlAsync(Database, "DROP FUNCTION h2(text, text)", token);
        }

        // The domains whose CHECKs draw go, so that a wait shows which sequence the domain added next names.
        _ = await nodeB.SafePsqlAsync(Database, """
            DROP TYPE cdk, ctd; DROP FUNCTION fp6(int, anyelement), fp7(int, anyelement); DROP DOMAIN dsw, tdom, tplain; DROP FUNCTION fpoly(anyelement), fsw(int);
            DROP FUNCTION fd(int), fret(int); DROP DOMAIN dd, ddk, ddf, ddr, ddn, du, ddr2, ddu, ddt;
            """, token);

        // A domain's CHECK that a session adds counts in that session at once, though it isn't validated, which would
        // wait itself.
        string added = "CREATE DOMAIN dl AS int; SELECT nextval('plain'); ALTER DOMAIN dl ADD CHECK (nextval('public.ti_id_seq') > 0) NOT VALID; " +
            (version >= 160000 ? "SELECT nextval('plain'), pg_input_is_valid('1', 'dl')" : "SELECT nextval('plain'), 1::dl");
        try
        {
            _ = await nodeB.SafePsqlAsync(Database, TimedLast(added), token);
            problems.Add($"{added}: drew");
        }
        catch (PostgresException refused) when (refused.SqlState == "57014")
        {
            if (!(refused.Where ?? "").Contains("waiting for node node_a, which granted sequence ti_id_seq's block", StringComparison.Ordinal))
            {
                problems.Add($"{added}: waited {refused.Where}");
            }
        }

        // Before 17, a partition shares no identity column with its parent, and writing it straight fails at once, as
        // from 17 on does writing a partition of a table attached as a partition, which PostgreSQL gives none; and before
        // 17, a partition with an identity column of its own draws from its own sequence, which the guard refuses at
        // once.
        foreach ((string fails, string state) in sharedIdentity
            ? [("INSERT INTO tlt1 SELECT max(id), 't' FROM tl; ALTER TABLE tl ATTACH PARTITION tlt FOR VALUES IN ('t'); INSERT INTO tlt1 (v) VALUES ('t')", "23502")]
            : new[]
            {
                ("INSERT INTO tp1 (v) VALUES ('x') RETURNING id", "23502"), ("MERGE INTO tp1 USING (SELECT 1) s ON false WHEN NOT MATCHED THEN INSERT (v) VALUES ('m')", "23502"),
                ($"COPY tp1 (v) FROM '{rows}'", "23502"), ("INSERT INTO tq1 (v) VALUES ('a') RETURNING id", "23514"),
            })
        {
            string? outcome;
            try
            {
                _ = await nodeB.SafePsqlAsync(Database, TimedLast(fails), token);
                outcome = "succeeded";
            }
            catch (PostgresException failed)
            {
                outcome = failed.SqlState == state ? null : $"failed with {failed.SqlState} {failed.MessageText} {failed.Where}";
            }
            catch (ArgumentException)
            {
                outcome = "succeeded";
            }

            if (outcome is not null)
            {
                problems.Add($"{fails}: {outcome}");
            }
        }

        Assert.AreEqual("", string.Join("\n", problems), "the forms that drew, or waited, wrongly");

        // A plan that coerces many values to a domain reads the domain's CHECK constraints once, as it names the
        // sequences its calls of nextval draw from, and adds their text once, as a CHECK that calls no nextval and no
        // function names the same sequences however many values start it. wide's CHECK holds a 1,500-value array, whose
        // text, added for each of 4,000 values, made each insert build and collect hundreds of megabytes, as the peak
        // of the backend's resident memory shows where the server runs beside the test, on Linux, and the CPU time the
        // backend spends on the insert, as log_statement_stats logs it, does everywhere. Each insert runs in a backend of
        // its own, after one row has read the catalogs, so that neither psql's start nor the backend's counts, and again
        // in a database of node_b's without worf, which takes what PostgreSQL's own coercion takes, a VALUES row's domain
        // CHECK made ready for each row, which worf's are compared with. The time an insert takes, under the load of the
        // other tests, varies several times over from one to the next with worf or without it, while its CPU time doesn't.
        string values = string.Join(", ", Enumerable.Range(1, 4000).Select(i => string.Create(CultureInfo.InvariantCulture, $"({i})")));
        string excluded = string.Join(",", Enumerable.Range(1, 1500).Select(i => (-i).ToString(CultureInfo.InvariantCulture)));
        string wideTables = $"CREATE DOMAIN wide AS int CHECK (VALUE <> ALL ('{{{excluded}}}'::int[])); CREATE TABLE twide (x wide, y bigint DEFAULT nextval('plain'))";
        _ = await nodeB.SafePsqlAsync(Database, wideTables, token);
        _ = await nodeB.SafePsqlAsync(Database, "CREATE DATABASE noworf", token);
        _ = await nodeB.SafePsqlAsync("noworf", $"""
            CREATE SEQUENCE plain; CREATE DOMAIN posint AS int CHECK (VALUE > 0);
            CREATE TABLE tpos (x posint, y bigint DEFAULT nextval('plain')); CREATE TABLE tint (x int, y bigint DEFAULT nextval('plain'));
            {wideTables}
            """, token);
        async Task<(TimeSpan Elapsed, TimeSpan Cpu, long PeakGrowth)> InsertManyAsync(string table, string database = Database)
        {
            await using NpgsqlConnection inserting = await nodeB.OpenAsync(database, token);
            await using (NpgsqlCommand first = new($"INSERT INTO {table} (x) VALUES (1); SET log_statement_stats = on", inserting))
            {
                _ = await first.ExecuteNonQueryAsync(token);
            }

            int pid = inserting.ProcessID;
            long before = PeakResident(pid);
            int logged = nodeB.ReadLog().Length;
            await using NpgsqlCommand insert = new($"INSERT INTO {table} (x) VALUES {values}", inserting);
            Stopwatch timing = Stopwatch.StartNew();
            _ = await insert.ExecuteNonQueryAsync(token);
            TimeSpan elapsed = timing.Elapsed;

            return (elapsed, await StatementsCpuAsync(nodeB, logged, pid, "EXECUTE MESSAGE STATISTICS", token), PeakResident(pid) - before);
        }

        (TimeSpan uncoerced, TimeSpan uncoercedCpu, long uncoercedPeak) = await InsertManyAsync("tint");
        (TimeSpan coerced, TimeSpan coercedCpu, long coercedPeak) = await InsertManyAsync("tpos");
        (TimeSpan wide, TimeSpan wideCpu, long widePeak) = await InsertManyAsync("twide");
        (TimeSpan ownUncoerced, TimeSpan ownUncoercedCpu, _) = await InsertManyAsync("tint", "noworf");
        (TimeSpan ownCoerced, TimeSpan ownCoercedCpu, _) = await InsertManyAsync("tpos", "noworf");
        (TimeSpan ownWide, TimeSpan ownWideCpu, _) = await InsertManyAsync("twide", "noworf");
        TestContext.WriteLine($"""
            4,000 rows: {uncoerced.TotalMilliseconds:F0} ms, {uncoercedCpu.TotalMilliseconds:F0} ms CPU, {uncoercedPeak} kB; coerced to posint: {coerced.TotalMilliseconds:F0} ms, {coercedCpu.TotalMilliseconds:F0} ms CPU, {coercedPeak} kB; to wide: {wide.TotalMilliseconds:F0} ms, {wideCpu.TotalMilliseconds:F0} ms CPU, {widePeak} kB
            without worf: {ownUncoerced.TotalMilliseconds:F0} ms, {ownUncoercedCpu.TotalMilliseconds:F0} ms CPU; coerced to posint: {ownCoerced.TotalMilliseconds:F0} ms, {ownCoercedCpu.TotalMilliseconds:F0} ms CPU; to wide: {ownWide.TotalMilliseconds:F0} ms, {ownWideCpu.TotalMilliseconds:F0} ms CPU
            """);
        Assert.IsLessThan((ownCoercedCpu * 2) + TimeSpan.FromMilliseconds(50), coercedCpu, "the CPU time of an insert of 4,000 values coerced to posint, against PostgreSQL's own");
        Assert.IsLessThan((ownWideCpu * 2) + TimeSpan.FromMilliseconds(50), wideCpu, "the CPU time of an insert of 4,000 values coerced to wide, against PostgreSQL's own");
        if (OperatingSystem.IsLinux())
        {
            Assert.IsLessThan(uncoercedPeak + (48 * 1024), widePeak, "the growth in kB of the peak resident memory of the backend that inserted 4,000 values coerced to wide");
        }

        Assert.AreEqual("25006", (await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
            "SET statement_timeout = '2s'; BEGIN READ ONLY; SELECT nextval('t_id_seq')", token))).SqlState, "nextval in a read-only transaction");

        // The wait is bounded, and its failure names the grantor, and the sequence when the statement names it.
        Stopwatch bounded = Stopwatch.StartNew();
        PostgresException waited = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database, "SELECT nextval('t_id_seq')", token));
        Assert.AreEqual("55000", waited.SqlState, waited.MessageText);
        Assert.Contains("sequence t_id_seq can't draw from the block node node_a granted it until node node_a answers this server", waited.MessageText);
        Assert.IsLessThan(TimeSpan.FromSeconds(30), bounded.Elapsed, "the bounded wait");
        PostgresException unnamed = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database, "SELECT computed_draw('t_id_seq')", token));
        Assert.AreEqual("55000", unnamed.SqlState, unnamed.MessageText);
        Assert.Contains("nextval can't draw until node node_a, which granted sequence", unnamed.MessageText);

        // PostgreSQL's apply of a subscription of its own, here to another of node_b's databases, fills ti's identity
        // column from ti_id_seq with no call worf sees, and the guard's check of each row waits for node_a all the same,
        // so no row is written meanwhile; with a timeout of a second, each wait fails after two.
        _ = await nodeB.SafePsqlAsync(Database, "CREATE DATABASE nativepub", token);
        _ = await nodeB.SafePsqlAsync("nativepub", """
            CREATE TABLE ti (v text); CREATE PUBLICATION native FOR TABLE ti;
            CREATE TABLE npd (v int); CREATE PUBLICATION native_npd FOR TABLE npd;
            CREATE TABLE ai2 (id int, v text); CREATE PUBLICATION native_ai2 FOR TABLE ai2;
            CREATE TABLE apl (id int, v text); CREATE PUBLICATION native_apl FOR TABLE apl;
            """, token);
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_timeout = '1s'", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        foreach (string native in new[] { "native", "native_npd", "native_ai2", "native_apl" })
        {
            _ = await nodeB.SafePsqlAsync("nativepub", $"SELECT pg_create_logical_replication_slot('{native}', 'pgoutput')", token);
            _ = await nodeB.SafePsqlAsync(Database,
                $"CREATE SUBSCRIPTION {native} CONNECTION '{nodeB.Dsn("nativepub", SuperUser)}' PUBLICATION {native} WITH (copy_data = false, create_slot = false, slot_name = '{native}')",
                token);
        }

        // A row of a table whose domain's CHECK draws from a sequence no range manages applies at once, as the apply
        // draws from no sequence the subscription's tables name; and the row of a table whose ALWAYS trigger's C code,
        // contrib's autoinc, draws from ti_id_seq waits for node_a.
        _ = await nodeB.SafePsqlAsync("nativepub", "INSERT INTO npd VALUES (1)", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 1 FROM npd", token), "node_b's apply of the row of npd, whose domain draws from plain");

        // So does the row of a table whose ALWAYS trigger's C code draws from plain, though the triggers of other
        // tables that call the same function name ti_id_seq: ai3's, which no subscription writes yet.
        _ = await nodeB.SafePsqlAsync("nativepub", "CREATE TABLE ai3 (id int, v text)", token);
        _ = await nodeB.SafePsqlAsync(Database, """
            CREATE TABLE ai3 (id int, v text);
            CREATE TRIGGER ai3_id BEFORE INSERT ON ai3 FOR EACH ROW EXECUTE FUNCTION autoinc(id, 'public.ti_id_seq');
            ALTER TABLE ai3 ENABLE ALWAYS TRIGGER ai3_id;
            """, token);
        _ = await nodeB.SafePsqlAsync("nativepub", "INSERT INTO apl VALUES (NULL, 'native')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 1 FROM apl WHERE id IS NOT NULL", token),
            "node_b's apply of the row of apl, whose trigger draws from plain");

        // A table REFRESH PUBLICATION adds to that subscription, ai3, whose ALWAYS trigger draws from ti_id_seq, waits for
        // node_a, though the apply had read the subscription's tables' triggers before, and the refresh changes no table.
        _ = await nodeB.SafePsqlAsync("nativepub", "ALTER PUBLICATION native_apl ADD TABLE ai3", token);
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SUBSCRIPTION native_apl REFRESH PUBLICATION WITH (copy_data = false)", token);
        int refreshed = nodeB.ReadLog().Length;
        _ = await nodeB.SafePsqlAsync("nativepub", "INSERT INTO ai3 VALUES (NULL, 'native')", token);
        Assert.IsTrue(await nodeB.PollLogAsync(refreshed, "sequence ti_id_seq can't draw from the block node node_a granted it", token),
            "node_b's apply of the row of ai3, which REFRESH PUBLICATION added, waiting for node_a");
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM ai3", token), "the rows of ai3 node_b's native subscription applied while node_a was down");
        int drawn = nodeB.ReadLog().Length;
        _ = await nodeB.SafePsqlAsync("nativepub", "INSERT INTO ai2 VALUES (NULL, 'native')", token);
        Assert.IsTrue(await nodeB.PollLogAsync(drawn, "sequence ti_id_seq can't draw from the block node node_a granted it", token),
            "node_b's apply of the row of ai2, waiting for node_a");
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM ai2", token), "the rows of ai2 node_b's native subscription applied while node_a was down");
        foreach (string native in new[] { "native_npd", "native_ai2", "native_apl" })
        {
            _ = await nodeB.SafePsqlAsync(Database, $"ALTER SUBSCRIPTION {native} DISABLE", token);
            _ = await nodeB.SafePsqlAsync(Database, $"ALTER SUBSCRIPTION {native} SET (slot_name = NONE)", token);
            _ = await nodeB.SafePsqlAsync(Database, $"DROP SUBSCRIPTION {native}", token);
        }

        int applied = nodeB.ReadLog().Length;
        _ = await nodeB.SafePsqlAsync("nativepub", "INSERT INTO ti VALUES ('native'), ('native'), ('native')", token);
        Assert.IsTrue(await nodeB.PollLogAsync(applied, "sequence ti_id_seq can't draw from the block node node_a granted it", token),
            "node_b's apply of its native subscription, waiting for node_a");
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM ti WHERE v = 'native'", token),
            "the rows node_b's native subscription applied while node_a was down");
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SUBSCRIPTION native DISABLE", token);
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SUBSCRIPTION native SET (slot_name = NONE)", token);
        _ = await nodeB.SafePsqlAsync(Database, "DROP SUBSCRIPTION native", token);
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM RESET worf.sequence_range_timeout", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);

        await nodeA.StartServerAsync(token);
        Assert.AreNotEqual("", await nodeB.SafePsqlAsync(Database, "SELECT nextval('t_id_seq')", token), "nextval once node_a is back");
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO other VALUES (nextval('t_id_seq')); UPDATE ti SET id = DEFAULT WHERE id = (SELECT min(id) FROM ti)", token);
    }

    /// <summary>
    /// A subscriber whose origin has no progress yet, as one worf_create_subscriber made before its first apply, takes
    /// the position its subscription's sync status holds as how far it applied its provider's changes, so a provider
    /// whose WAL position is behind it has gone back, as one restored from a backup made before it, and must reset.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SubscribersWithoutProgressKnowWhereTheyStarted()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);

        // node_b's origin forgets its progress, and its sync status holds a position past node_a's, as the restore point
        // worf_create_subscriber records, which its apply worker starts from, and can't.
        string ahead = await nodeA.SafePsqlAsync(Database, "SELECT (pg_current_wal_lsn() + 1073741824)::text", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('b_from_a')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT NOT EXISTS (SELECT 1 FROM pg_stat_activity WHERE backend_type LIKE 'worf apply%')", token),
            "node_b's apply worker stopped");
        int logpos = nodeB.ReadLog().Length;
        _ = await nodeB.SafePsqlAsync(Database, $"""
            SELECT pg_replication_origin_advance((SELECT sub_slot_name FROM worf.subscription WHERE sub_name = 'b_from_a'), '0/0');
            UPDATE worf.local_sync_status SET sync_statuslsn = '{ahead}'
            WHERE sync_subid = (SELECT sub_id FROM worf.subscription WHERE sub_name = 'b_from_a') AND sync_relname IS NULL;
            SELECT worf.alter_subscription_enable('b_from_a');
            """, token);
        Assert.IsTrue(await nodeB.PollLogAsync(logpos, $"sequence public.t_id_seq: node node_a's WAL position moved back from {ahead} to ", token),
            "node_b finding node_a behind the position its sync status holds");
    }

    /// <summary>
    /// A statement a restarted node's apply worker applies for a user, as the DDL another node's replicate_ddl_command
    /// queues, waits for the grantor of the block it would draw from, as the user's own would, even under transdiscard,
    /// which leaves the transaction to be applied again rather than discarding it, and applies once the grantor has
    /// answered. A row the worker writes or a table it copies whose defaults draw from a sequence no range manages, for
    /// a column only the subscriber has, and whose CHECK constraint does too, applies at once, and a row written after
    /// the column's default is set to draw from a managed sequence waits; so does a row of a table whose ALWAYS trigger's
    /// C code, contrib's autoinc, draws from it.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestartedNodesApplyNothingThatDrawsBeforeTheirGrantorAnswers()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "worf.exception_behaviour = transdiscard");
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text); CREATE TABLE other (id bigint)", token);
        }

        _ = await nodeB.SafePsqlAsync(Database, "CREATE SEQUENCE plain; CREATE TABLE cx (id int PRIMARY KEY, v text, local_id bigserial, later bigint DEFAULT nextval('plain'), CHECK (nextval('plain') > 0))", token);
        _ = await nodeC.SafePsqlAsync(Database, "CREATE TABLE cx (id int PRIMARY KEY, v text); SELECT worf.replication_set_add_table('default', 'cx')", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeB.SafePsqlAsync(Database, $$"""
            SELECT worf.create_subscription(subscription_name := 'b_from_c', provider_dsn := '{{nodeC.Dsn(Database, SuperUser)}}',
                replication_sets := '{default,ddl_sql}', synchronize_structure := false, synchronize_data := false, forward_origins := '{}')
            """, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT count(*) = 2 FROM worf.show_subscription_status() WHERE status = 'replicating'", token), "node_b's subscriptions");

        // node_b's copy of ac, which a subscription of its own brings, has an ALWAYS trigger whose C code, contrib's
        // autoinc, fills a row's id from t_id_seq.
        _ = await nodeC.SafePsqlAsync(Database, """
            CREATE TABLE ac (k int PRIMARY KEY, id int);
            SELECT worf.create_replication_set('acset');
            SELECT worf.replication_set_add_table('acset', 'ac');
            """, token);
        _ = await nodeB.SafePsqlAsync(Database, $$"""
            CREATE EXTENSION autoinc;
            CREATE TABLE ac (k int PRIMARY KEY, id int);
            CREATE TRIGGER ac_id BEFORE INSERT ON ac FOR EACH ROW EXECUTE FUNCTION autoinc(id, 'public.t_id_seq');
            ALTER TABLE ac ENABLE ALWAYS TRIGGER ac_id;
            SELECT worf.create_subscription(subscription_name := 'b_from_c_ac', provider_dsn := '{{nodeC.Dsn(Database, SuperUser)}}',
                replication_sets := '{acset}', synchronize_structure := false, synchronize_data := false, forward_origins := '{}');
            """, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT count(*) = 3 FROM worf.show_subscription_status() WHERE status = 'replicating'", token), "node_b's subscriptions, ac's too");
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 10, token);
        string before = await nodeB.SafePsqlAsync(Database, "SELECT last_value FROM t_id_seq", token);

        await nodeA.StopAsync("fast", token);
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.show_subscription_status() WHERE subscription_name = 'b_from_c' AND status = 'replicating')", token),
            "node_b's subscription to node_c");

        // A row whose subscriber-only column draws from a sequence no range manages applies, and its table copies, at once.
        Stopwatch unmanaged = Stopwatch.StartNew();
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO cx VALUES (1, 'x')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 1 FROM cx", token), "node_c's row with node_a down");
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.alter_subscription_resynchronize_table('b_from_c', 'cx')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, """
            SELECT count(*) = 1 FROM cx WHERE EXISTS (SELECT 1 FROM worf.local_sync_status WHERE sync_relname = 'cx' AND sync_status = 'r')
            """, token), "cx copied again with node_a down");
        Assert.IsLessThan(TimeSpan.FromSeconds(20), unmanaged.Elapsed, "the row and the copy, against the 15 s between failed applies");

        // A row of ac waits, as the trigger's C code would draw from t_id_seq's block.
        int acpos = nodeB.ReadLog().Length;
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO ac VALUES (1, NULL)", token);
        Assert.IsTrue(await nodeB.PollLogAsync(acpos, "sequence t_id_seq can't draw from the block node node_a granted it", token), "node_b's apply of ac's row, waiting");
        Assert.AreEqual($"0|{before}", await nodeB.SafePsqlAsync(Database, "SELECT (SELECT count(*) FROM ac), (SELECT last_value FROM t_id_seq)", token),
            "ac's rows, and the sequence, while node_a is down");

        // So do the rows of ac that its sync copies, which fire the trigger too.
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.alter_subscription_resynchronize_table('b_from_c_ac', 'ac')", token);
        await Task.Delay(TimeSpan.FromSeconds(8), token);
        Assert.AreEqual($"0|{before}", await nodeB.SafePsqlAsync(Database, "SELECT (SELECT count(*) FROM ac WHERE id IS NOT NULL), (SELECT last_value FROM t_id_seq)", token),
            "ac's rows its sync copied, and the sequence, while node_a is down");

        // Once a subscriber-only column's default draws from the managed sequence, rather than the one it drew from, the
        // next row waits, though the apply worker read the defaults for a row before. The values the old default gave go
        // first, as the range's guard would refuse them.
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO cx VALUES (3, 'z')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT count(*) = 2 FROM cx", token), "node_c's row before the default changes");
        _ = await nodeB.SafePsqlAsync(Database, "UPDATE cx SET later = NULL; ALTER TABLE cx ALTER COLUMN later SET DEFAULT nextval('public.t_id_seq')", token);
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO cx VALUES (2, 'y')", token);

        // The statement draws only where it applies, on node_b: node_c, which manages the sequence too, as it heard of it
        // from node_b, may have no block of it.
        _ = await nodeC.SafePsqlAsync(Database, """
            SELECT worf.replicate_ddl_command($$INSERT INTO public.other SELECT nextval('public.t_id_seq') FROM worf.local_node l
                JOIN worf.node n ON n.node_id = l.node_id WHERE n.node_name = 'node_b'$$)
            """, token);
        await Task.Delay(TimeSpan.FromSeconds(5), token);
        Assert.AreEqual($"0|2|{before}", await nodeB.SafePsqlAsync(Database,
            "SELECT (SELECT count(*) FROM other), (SELECT count(*) FROM cx), (SELECT last_value FROM t_id_seq)", token),
            "node_b's rows from the applied statement and from node_c's row whose default draws, and its sequence, before node_a answered");

        await nodeA.StartServerAsync(token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT (SELECT count(*) FROM other) = 1 AND (SELECT count(*) FROM cx) = 3 AND (SELECT count(*) FROM ac WHERE id IS NOT NULL) = 1", token),
            "the applied statement and node_c's rows once node_a is back");
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM worf.exception_log", token), "the transactions transdiscard discarded");
    }

    /// <summary>
    /// A statement that waits for a restarted node's grantor makes the exchange with it due soon after a failed one,
    /// whatever its delay before the next, so that once the grantor is back the statement draws within its wait, rather
    /// than failing until the delay, which grows to the exchange interval, runs out.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task WaitingStatementsDrawSoonAfterTheirGrantorIsBack()
    {
        CancellationToken token = TestContext.CancellationToken;
        const string Slow = "worf.sequence_range_exchange_interval = '60s'";
        await using TestNode nodeA = await StartNodeAsync("node_a", token, Slow);
        await using TestNode nodeB = await StartNodeAsync("node_b", token, Slow);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 10, token);

        // node_b's exchanges with node_a fail for half a minute, and their delay grows past it.
        await nodeA.StopAsync("fast", token);
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        await Task.Delay(TimeSpan.FromSeconds(30), token);
        await nodeA.StartServerAsync(token);
        Stopwatch drawing = Stopwatch.StartNew();
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('back')", token);
        TestContext.WriteLine($"node_b's insert after {drawing.Elapsed.TotalSeconds:F1} s");
        Assert.IsLessThan(TimeSpan.FromSeconds(10), drawing.Elapsed, "node_b's insert once node_a is back, against its 10 s wait");
    }

    /// <summary>
    /// A pool holder whose server restarts grants its child nothing from a sequence's pool until its grantor has
    /// answered for that sequence, though the grantor answers its calls while a prepared transaction holds the
    /// sequence's range row there.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestartedPoolHoldersWaitForEachSequence()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "max_prepared_transactions = 2");
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        TestNode[] nodes = [nodeA, nodeB, nodeC];
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 100, max_range_size := 100)", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeC, "c_from_b", nodeB, token);
        await WaitActiveAsync(nodes, token);
        await InsertAsync(nodeC, 1500, token);
        Assert.AreNotEqual("0", await nodeB.SafePsqlAsync(Database,
            "SELECT count(*) FROM worf.sequence_range_block WHERE kind = 'p' AND seqoid = 't_id_seq'::regclass AND state = 'open'", token), "node_b's pool block");

        // A prepared transaction holds t_id_seq's range row on node_a, whose calls with node_b go on without it, and
        // node_b restarts.
        _ = await nodeA.SafePsqlAsync(Database,
            "BEGIN; SELECT 1 FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass FOR NO KEY UPDATE; PREPARE TRANSACTION 'held_t'", token);
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        string restarted = await nodeB.SafePsqlAsync(Database, "SELECT now()::text", token);
        await Task.Delay(TimeSpan.FromSeconds(10), token);

        bool drew = await nodeB.PsqlAsync(Database, "SET statement_timeout = '2s'; SELECT nextval('t_id_seq')", token);
        _ = await nodeC.PsqlAsync(Database, "SET statement_timeout = '20s'; INSERT INTO t (v) SELECT 'c' FROM generate_series(1, 1000)", token);
        string granted = await nodeB.SafePsqlAsync(Database,
            $"SELECT count(*) FROM worf.sequence_range_grant WHERE seqoid = 't_id_seq'::regclass AND grantee_name = 'node_c' AND granted_at > '{restarted}'", token);
        Assert.AreEqual("False|0", $"{drew}|{granted}", "node_b's nextval, and its grants to node_c, while node_a holds t_id_seq's range row");

        _ = await nodeA.SafePsqlAsync(Database, "COMMIT PREPARED 'held_t'", token);
        await InsertAsync(nodeB, 10, token);
        await InsertAsync(nodeC, 300, token);
        await AssertNoSharedValuesAsync(nodes, token);
    }

    /// <summary>
    /// A pool holder whose server restarts neither draws from the block its grantor granted, nor grants its children
    /// from its pool, blocks or values they reserve, until the grantor has answered, and in a ring of four of its
    /// descendants takes none of them as its parent once it no longer hears from the grantor, nor does any node take a
    /// route that goes through itself, as each node reports no route to its parent and its route to the others, so no
    /// distance counts up around the ring.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestartedPoolHoldersWaitForTheirGrantor()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        await using TestNode nodeD = await StartNodeAsync("node_d", token);
        await using TestNode nodeE = await StartNodeAsync("node_e", token);
        TestNode[] nodes = [nodeA, nodeB, nodeC, nodeD, nodeE];
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c,node_d,node_e}', range_size := 100, max_range_size := 100)", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeC, "c_from_b", nodeB, token);
        await SubscribeAsync(nodeD, "d_from_c", nodeC, token);
        await SubscribeAsync(nodeE, "e_from_d", nodeD, token);
        await SubscribeAsync(nodeB, "b_from_e", nodeE, token);
        await WaitActiveAsync(nodes, token);
        await InsertAsync(nodeC, 1500, token);
        await InsertAsync(nodeB, 10, token);
        Assert.AreNotEqual("0", await nodeB.SafePsqlAsync(Database,
            "SELECT count(*) FROM worf.sequence_range_block WHERE kind = 'p' AND seqoid = 't_id_seq'::regclass AND state = 'open'", token), "node_b's pool block");
        // The values node_c reserves are at the top of node_b's pool, which node_c's next blocks don't reach.
        long free = long.Parse(await nodeB.SafePsqlAsync(Database, """
            SELECT f.hi - 20 FROM worf.sequence_range_free f JOIN worf.sequence_range_block b ON b.block_id = f.block_id
            WHERE b.seqoid = 't_id_seq'::regclass AND b.kind = 'p' AND b.state = 'open' AND f.hi - f.lo >= 100
            ORDER BY f.hi DESC LIMIT 1
            """, token), CultureInfo.InvariantCulture);

        await nodeA.StopAsync("fast", token);

        // Once node_b no longer counts node_a's last report, it has no parent, rather than node_c, node_d or node_e,
        // whose routes go through it, and no node's distance counts up around the ring of node_b to node_e meanwhile,
        // past the four hops of the longest route five nodes have: every route goes to none.
        const string Parent = "SELECT coalesce((SELECT node_name::text FROM worf.node WHERE node_id = parent_node), 'none') FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass";
        const string Route = "SELECT coalesce((SELECT node_name::text FROM worf.node WHERE node_id = parent_node), 'none') || ':' || coalesce(distance::text, '-') FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass";
        HashSet<string> seen = [];
        Stopwatch sampling = Stopwatch.StartNew();
        while (sampling.Elapsed < TimeSpan.FromSeconds(20))
        {
            foreach (TestNode node in new[] { nodeB, nodeC, nodeD, nodeE })
            {
                string route = await node.SafePsqlAsync(Database, Route, token);
                string[] parts = route.Split(':');
                if ((node == nodeB && parts[0] is not ("none" or "node_a")) || (parts[1] != "-" && int.Parse(parts[1], CultureInfo.InvariantCulture) > 4))
                {
                    _ = seen.Add($"{node.Name}: {route}");
                }
            }

            await Task.Delay(TimeSpan.FromMilliseconds(250), token);
        }

        Assert.AreEqual("", string.Join(", ", seen.Order()), "node_b's parents, and the distances past 4, while node_a is down");
        foreach (TestNode node in new[] { nodeB, nodeC, nodeD, nodeE })
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"SELECT ({Parent}) = 'none'", token), $"{node.Name}'s parent while node_a is down");
        }

        // node_b restarts.
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        string restarted = await nodeB.SafePsqlAsync(Database, "SELECT now()::text", token);
        await Task.Delay(TimeSpan.FromSeconds(10), token);

        // node_b draws nothing, and node_c, which draws past the blocks it holds, gets nothing from node_b's pool.
        string drew;
        try
        {
            drew = $"drew {await nodeB.SafePsqlAsync(Database, "SET statement_timeout = '2s'; SELECT nextval('t_id_seq')", token)}";
        }
        catch (PostgresException refused) when (refused.SqlState == "57014")
        {
            drew = (refused.Where ?? "").Contains("waiting for node node_a, which granted sequence t_id_seq's block", StringComparison.Ordinal)
                ? "waited" : $"waited: {refused.Where}";
        }

        async Task<string> ReserveAsync(TestNode node, long lo)
        {
            try
            {
                _ = await node.SafePsqlAsync(Database, $"SELECT worf.reserve_sequence_values('t_id_seq', {lo}, {lo + 9}, '15 s')", token);
                return "reserved";
            }
            catch (PostgresException refusedValues) when (refusedValues.SqlState == "57014")
            {
                return "timed out";
            }
            catch (PostgresException refusedValues)
            {
                return $"failed: {refusedValues.MessageText}";
            }
        }

        string reserved = string.Join(',', await Task.WhenAll(ReserveAsync(nodeC, free), ReserveAsync(nodeE, free + 10)));

        _ = await nodeC.PsqlAsync(Database, "SET statement_timeout = '20s'; INSERT INTO t (v) SELECT 'c' FROM generate_series(1, 1000)", token);

        string granted = await nodeB.SafePsqlAsync(Database,
            $"SELECT count(*) FROM worf.sequence_range_grant WHERE seqoid = 't_id_seq'::regclass AND grantee_name = 'node_c' AND granted_at > '{restarted}'", token);
        Assert.AreEqual("waited|0|timed out,timed out", $"{drew}|{granted}|{reserved}",
            "node_b's nextval, its grants to node_c, and node_c's and node_e's reservations of values in node_b's pool, before node_a answered");

        await nodeA.StartServerAsync(token);
        await InsertAsync(nodeB, 10, token);
        await InsertAsync(nodeC, 300, token);
        await AssertNoSharedValuesAsync(nodes, token);
    }

    /// <summary>
    /// A node whose grantor calls it, and that calls no one, and that has no record of the grantor in its worf.node, waits
    /// to draw after its server restarts until the grantor's call has come, and then draws again: the statements that
    /// take in the call wait for nothing.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestartedNodesThatOnlyAnswerDrawAgain()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 10, token);
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM worf.node WHERE node_name = 'node_a'", token), "node_a in node_b's worf.node");

        // node_b restarts while node_a is down, and draws nothing until node_a is back.
        await nodeA.StopAsync("fast", token);
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        Assert.IsFalse(await nodeB.PsqlAsync(Database, "SET statement_timeout = '3s'; INSERT INTO t (v) VALUES ('waiting')", token),
            "node_b's insert after its restart, while node_a is down");
        await nodeA.StartServerAsync(token);
        Stopwatch drawing = Stopwatch.StartNew();
        await InsertAsync(nodeB, 10, token);
        Assert.IsLessThan(TimeSpan.FromSeconds(20), drawing.Elapsed, "node_b's first insert after node_a is back");
    }

    /// <summary>
    /// After its server restarts, a node draws from a block its grantor granted once the grantor has answered its first
    /// exchange, which says back the node's start, and which a statement waiting to draw makes at once, rather than after
    /// an exchange interval, or two.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RestartedNodesDrawAfterOneExchange()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token, "worf.sequence_range_exchange_interval = '1min'");
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "worf.sequence_range_exchange_interval = '1min'");
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);
        await InsertAsync(nodeB, 10, token);

        foreach (bool waiting in new[] { false, true })
        {
            await nodeB.StopAsync("fast", token);
            await nodeB.StartServerAsync(token);

            // The first time, node_b's exchange worker exchanges as it starts; the second, its first exchange has ended
            // once the insert comes, which then makes the next.
            if (waiting)
            {
                Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE backend_type LIKE 'worf exchange%')", token), "node_b's exchange worker");
                await Task.Delay(TimeSpan.FromSeconds(3), token);
            }

            Stopwatch drawing = Stopwatch.StartNew();
            await InsertAsync(nodeB, 10, token);
            Assert.IsLessThan(TimeSpan.FromSeconds(15), drawing.Elapsed, $"node_b's first insert after its restart, {(waiting ? "after" : "with")} its first exchange");
        }
    }

    /// <summary>
    /// An exchange worker paused past a call's time fails that call once it goes on, and reports it at once, and none of
    /// its calls to the other nodes after it. On Linux and macOS, where the exchange worker can be stopped.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public async Task PausedWorkersFailOnlyTheCallsPastTheirTime()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "log_min_messages = debug2");
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await WaitActiveAsync([nodeA, nodeB], token);

        // node_c's host drops node_b's connection attempts. The exchange worker stops while its first call waits on
        // node_c, past the call's five seconds.
        const string Worker = "SELECT pid FROM pg_stat_activity WHERE backend_type LIKE 'worf exchange%'";
        int logpos = nodeB.ReadLog().Length;
        _ = await nodeB.SafePsqlAsync(Database,
            $"UPDATE worf.node_interface SET if_dsn = 'host=192.0.2.1 port=5432 dbname={Database} user={SuperUser}' WHERE if_nodeid = (SELECT node_id FROM worf.node WHERE node_name = 'node_c')",
            token);
        int waiting = logpos;
        while (true)
        {
            Assert.IsTrue(await nodeB.PollLogAsync(waiting, "exchange with node node_c starting", token), "node_b calling node_c");
            waiting = nodeB.ReadLog().IndexOf("exchange with node node_c starting", waiting, StringComparison.Ordinal);
            await Task.Delay(TimeSpan.FromSeconds(1), token);
            string after = nodeB.ReadLog()[waiting..];
            if (!after.Contains("exchange with node node_c done", StringComparison.Ordinal) && !after.Contains("exchange with node node_c failed", StringComparison.Ordinal))
            {
                break;
            }

            waiting++;
        }

        logpos = waiting;
        string worker = await nodeB.SafePsqlAsync(Database, Worker, token);
        (int stopped, _, string error) = await ProcessRunner.RunAsync("kill", ["-STOP", worker], token);
        Assert.AreEqual(0, stopped, error);
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(6), token);
        }
        finally
        {
            (int resumed, _, error) = await ProcessRunner.RunAsync("kill", ["-CONT", worker], token);
            Assert.AreEqual(0, resumed, error);
        }

        // The call's failure is reported at once, and the cancel fails none of node_b's calls to node_a after it.
        int resumedAt = nodeB.ReadLog().Length;
        Stopwatch reporting = Stopwatch.StartNew();
        while (!nodeB.ReadLog()[logpos..].Contains("exchange with node node_c failed: ", StringComparison.Ordinal))
        {
            Assert.IsLessThan(TimeSpan.FromSeconds(5), reporting.Elapsed, "node_b reporting the call's failure once it went on");
            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }

        Assert.IsTrue(await nodeB.PollLogAsync(resumedAt, "exchange with node node_a done", token), "node_b's next call to node_a");
        Assert.DoesNotContain("exchange with node node_a failed", nodeB.ReadLog()[logpos..], "node_b's calls to node_a after the cancel");
    }

    /// <summary>
    /// Statements that draw from a sequence at once never run its block out, however many draw and however small the
    /// blocks: many single-row writers, with holders among them, multi-row writers, and a COPY beside writers. A
    /// statement waits for the switch, before it draws or after a row, once the block has no more values left than the
    /// statements drawing from it, and checks the next block once it comes, and a switch beside the holders moves the
    /// sequence only between their statements' rows, so that no nextval reads the old bounds with the new position.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ConcurrentStatementsNeverRunABlockOut()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node_a", token);
        List<string> failures = [];

        async Task<string> TableAsync(string name, int size)
        {
            _ = await node.SafePsqlAsync(Database, $"""
                CREATE TABLE {name} (id bigserial PRIMARY KEY, v int);
                SELECT worf.enable_sequence_range('{name}_id_seq', participants := '{"{}"}', range_size := {size}, max_range_size := {size});
                """, token);
            Assert.IsTrue(await node.PollQueryUntilAsync(Database,
                $"SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = '{name}_id_seq'::regclass", token),
                $"{name} drawing from a block with the next ready");
            return name;
        }

        async Task WriteAsync(string table, int sessions, int statements, string values, int holdEvery = 0)
            => await Task.WhenAll(Enumerable.Range(0, sessions).Select(async session =>
            {
                await using NpgsqlConnection connection = await node.OpenAsync(Database, token);
                for (int i = 0; i < statements; i++)
                {
                    try
                    {
                        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(token);
                        await using (NpgsqlCommand insert = new($"INSERT INTO {table} (v) {values}", connection, transaction))
                        {
                            _ = await insert.ExecuteNonQueryAsync(token);
                        }

                        if (holdEvery > 0 && i % holdEvery == session % holdEvery)
                        {
                            await using NpgsqlCommand sleep = new("SELECT pg_sleep(0.2)", connection, transaction);
                            _ = await sleep.ExecuteNonQueryAsync(token);
                        }

                        await transaction.CommitAsync(token);
                    }
                    catch (PostgresException error)
                    {
                        lock (failures)
                        {
                            failures.Add($"{table}: {error.SqlState} {error.MessageText}");
                        }
                    }
                }
            }));

        async Task CopyAsync(string table, int rows)
        {
            await using NpgsqlConnection connection = await node.OpenAsync(Database, token);
            try
            {
                await using TextWriter copy = await connection.BeginTextImportAsync($"COPY {table} (v) FROM STDIN", token);
                for (int i = 0; i < rows; i++)
                {
                    await copy.WriteLineAsync("1");
                }
            }
            catch (PostgresException error)
            {
                lock (failures)
                {
                    failures.Add($"{table}: {error.SqlState} {error.MessageText}");
                }
            }
        }

        // 24 single-row writers on 200-value blocks, without holders and with one transaction in ten holding the
        // sequence for 0.2 s; 8 writers of 30 rows on 100-value blocks; and 25-row writers on 10-value blocks.
        await WriteAsync(await TableAsync("single", 200), 24, 150, "VALUES (1)");
        await WriteAsync(await TableAsync("held", 200), 24, 100, "VALUES (1)", 10);
        await WriteAsync(await TableAsync("multi", 100), 8, 15, "SELECT 1 FROM generate_series(1, 30)");
        await WriteAsync(await TableAsync("tiny", 10), 2, 10, "SELECT 1 FROM generate_series(1, 25)");

        // A COPY of 1,500 rows beside two writers on 1,000-value blocks.
        string copied = await TableAsync("copied", 1000);
        Task writers = WriteAsync(copied, 2, 100, "SELECT 1 FROM generate_series(1, 10)");
        await CopyAsync(copied, 1500);
        await writers;

        Assert.IsEmpty(failures, string.Join("\n", failures.Distinct().Take(10)));
        Assert.AreEqual("3600|2400|3600|500|3500", await node.SafePsqlAsync(Database, """
            SELECT (SELECT count(DISTINCT id) FROM single), (SELECT count(DISTINCT id) FROM held), (SELECT count(DISTINCT id) FROM multi),
                (SELECT count(DISTINCT id) FROM tiny), (SELECT count(DISTINCT id) FROM copied)
            """, token), "the rows, each with its own key");
    }

    /// <summary>
    /// Statements that reach the end of a block whose next block is here wait for the switch however long the range
    /// worker takes to make it, as on a node too busy to check its sequences in time, rather than give up after twice the
    /// exchange timeout and run the block out, and statement_timeout ends such a wait, as it does a lock's. On Linux and
    /// macOS, where the worker can be stopped.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public async Task StatementsWaitOutASlowSwitch()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node_a", token, "worf.sequence_range_timeout = 1000");
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE s (id bigserial PRIMARY KEY, v int);
            SELECT worf.enable_sequence_range('s_id_seq', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 's_id_seq'::regclass", token),
            "s drawing from a block with the next ready");

        // The range worker stops while four writers draw past the block's end, for longer than twice the exchange timeout.
        // enable_sequence_range gives the sequence its blocks itself, so the worker may start a moment later.
        // It's stopped as it waits between its rounds, holding no lightweight lock, as one of the lock manager's, which
        // the waiting statements' reads of pg_locks would wait on past their statement_timeout.
        const string Worker = "SELECT pid FROM pg_stat_activity WHERE backend_type LIKE 'worf range%'";
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"SELECT EXISTS ({Worker})", token), "the range worker");
        string worker = await node.SafePsqlAsync(Database, Worker, token);
        string error;
        for (int attempt = 0; ; attempt++)
        {
            (int stopped, _, error) = await ProcessRunner.RunAsync("kill", ["-STOP", worker], token);
            Assert.AreEqual(0, stopped, error);
            if (await node.SafePsqlAsync(Database, $"SELECT coalesce(wait_event_type, '') FROM pg_stat_activity WHERE pid = {worker}", token)
                is "Extension" or "Activity" or "Timeout" or "Lock")
            {
                break;
            }

            (int going, _, error) = await ProcessRunner.RunAsync("kill", ["-CONT", worker], token);
            Assert.AreEqual(0, going, error);
            Assert.IsLessThan(200, attempt, "the range worker, stopped as it waits");
            await Task.Delay(TimeSpan.FromMilliseconds(7), token);
        }

        List<string> failures = [];
        Task writers = Task.WhenAll(Enumerable.Range(0, 4).Select(async _ =>
        {
            await using NpgsqlConnection connection = await node.OpenAsync(Database, token);
            for (int i = 0; i < 40; i++)
            {
                try
                {
                    await ExecuteAsync(connection, "INSERT INTO s (v) VALUES (1)", token);
                }
                catch (PostgresException failure)
                {
                    lock (failures)
                    {
                        failures.Add($"{failure.SqlState} {failure.MessageText}");
                    }
                }
            }
        }));
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(5), token);
            Assert.IsFalse(writers.IsCompleted, "the writers waiting for the switch");
            Assert.IsEmpty(failures, string.Join("; ", failures.Distinct()));
            Assert.AreEqual("t", await node.SafePsqlAsync(Database, "SELECT max(id) <= 100 FROM s", token), "the rows drawn from the first block");

            // A statement waiting for the switch ends at its statement_timeout.
            PostgresException timedOut = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database,
                "SET statement_timeout = '1s'; INSERT INTO s (v) VALUES (1)", token));
            Assert.AreEqual("57014", timedOut.SqlState);
        }
        finally
        {
            (int resumed, _, error) = await ProcessRunner.RunAsync("kill", ["-CONT", worker], token);
            Assert.AreEqual(0, resumed, error);
        }

        await writers;
        Assert.IsEmpty(failures, string.Join("; ", failures.Distinct()));
        Assert.AreEqual("160|160", await node.SafePsqlAsync(Database, "SELECT count(*), count(DISTINCT id) FROM s", token), "the rows, each with its own key");
    }

    /// <summary>
    /// A switch waits for the statements that hold the sequence's switch gate, as for a lock, and statements waiting for
    /// the switch wait for it, however long, rather than fail: a multi-row insert ON CONFLICT DO NOTHING waiting for a
    /// key another transaction holds draws its later rows from the bounds it then has, a COPY FROM STDIN paused for its
    /// client and a statement in pg_sleep hold the switch up until they go on, though not the range row, which the switch
    /// takes only once it has the gate, and a cycle through the gate, an insert
    /// waiting for the key of a transaction that then waits for the switch, is a deadlock the detector ends, with one of
    /// the two failing with 40P01 and the other going on.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SwitchesWaitForStatementsThatHoldTheGate()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node_a", token, "worf.sequence_range_timeout = 1000");
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE k (id bigserial PRIMARY KEY, u int UNIQUE);
            SELECT worf.enable_sequence_range('k_id_seq', participants := '{}', range_size := 100, max_range_size := 100);
            """, token);
        const string Ready = "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 'k_id_seq'::regclass";
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, Ready, token), "k's blocks");
        int next = 1_000_000;

        // A statement that runs past the block's end, from a fresh block, while another holds the gate.
        async Task<Task> PastTheEndAsync(NpgsqlConnection connection)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, Ready, token), "k's next block");
            int from = next;
            next += 1000;
            return ExecuteAsync(connection, $"INSERT INTO k (u) SELECT g FROM generate_series({from}, {from + 149}) g", token);
        }

        async Task<bool> WaitingAtDoorAsync(NpgsqlConnection connection) => await node.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = {connection.ProcessID} AND locktype = 'advisory' AND objsubid = 4 AND NOT granted)
            """, token);

        // An insert ON CONFLICT DO NOTHING of three rows waits for the key a transaction holds while another session's
        // rows run past the block's end and wait for the switch, which waits for the insert; once the transaction
        // commits, the insert draws its later rows, and the switch comes.
        await using (NpgsqlConnection holder = await node.OpenAsync(Database, token))
        await using (NpgsqlConnection waiter = await node.OpenAsync(Database, token))
        await using (NpgsqlConnection writer = await node.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction holding = await holder.BeginTransactionAsync(token);
            await ExecuteAsync(holder, "INSERT INTO k (u) VALUES (1)", token);
            Task waiting = ExecuteAsync(waiter, "INSERT INTO k (u) VALUES (1), (2), (3) ON CONFLICT DO NOTHING", token);
            Assert.IsTrue(await node.PollQueryUntilAsync(Database,
                $"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = {waiter.ProcessID} AND locktype = 'transactionid' AND NOT granted)", token),
                "the insert waiting for the key");
            Task past = await PastTheEndAsync(writer);
            Assert.IsTrue(await WaitingAtDoorAsync(writer), "the rows past the block's end waiting for the switch");
            await Task.Delay(TimeSpan.FromSeconds(3), token);
            Assert.IsFalse(past.IsCompleted, "the rows past the block's end, still waiting for the switch after twice the timeout");
            await holding.CommitAsync(token);
            await waiting;
            await past;
        }

        // A COPY FROM STDIN that has sent a row and waits for its client holds the switch up, and the rows past the
        // block's end wait for it until the COPY goes on.
        await using (NpgsqlConnection copier = await node.OpenAsync(Database, token))
        await using (NpgsqlConnection writer = await node.OpenAsync(Database, token))
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, Ready, token), "k's next block before the COPY");
            string drawn = await node.SafePsqlAsync(Database, "SELECT pg_sequence_last_value('k_id_seq')", token);
            Task past;
            await using (TextWriter copy = await copier.BeginTextImportAsync("COPY k (u) FROM STDIN", token))
            {
                // The COPY takes its row, and then waits for its client, before the other rows run past the block's end: a
                // row it took later would let the switch they wait for pass, as rows do.
                await copy.WriteAsync("10\n");
                await copy.FlushAsync(token);
                Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"""
                    SELECT pg_sequence_last_value('k_id_seq') IS DISTINCT FROM {(drawn == "" ? "NULL" : drawn)}
                        AND EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = {copier.ProcessID} AND wait_event = 'ClientRead')
                    """, token), "the COPY's row taken, and the COPY waiting for its client");
                past = await PastTheEndAsync(writer);
                Assert.IsTrue(await WaitingAtDoorAsync(writer), "the rows past the block's end waiting for the switch during the COPY");
                await Task.Delay(TimeSpan.FromSeconds(3), token);
                Assert.IsFalse(past.IsCompleted, "the rows past the block's end, waiting for the paused COPY");

                // The switch waits for the COPY without the range row, which exchanges and the range worker take meanwhile.
                bool free = false;
                for (int i = 0; i < 30 && !free; i++)
                {
                    free = await node.SafePsqlAsync(Database,
                        "SELECT count(*) = 1 FROM (SELECT 1 FROM worf.sequence_range WHERE seqoid = 'k_id_seq'::regclass FOR NO KEY UPDATE SKIP LOCKED) r", token) == "t";
                    await Task.Delay(TimeSpan.FromMilliseconds(100), token);
                }

                Assert.IsTrue(free, "the range row, while the switch waits for the paused COPY");
                Assert.IsFalse(past.IsCompleted, "the rows past the block's end, still waiting for the paused COPY");
                await copy.WriteAsync("11\n");
            }

            await past;
        }

        // So does a statement in pg_sleep within its rows.
        await using (NpgsqlConnection sleeper = await node.OpenAsync(Database, token))
        await using (NpgsqlConnection writer = await node.OpenAsync(Database, token))
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, Ready, token), "k's next block before the sleep");
            Task sleeping = ExecuteAsync(sleeper,
                "INSERT INTO k (u) SELECT g FROM (SELECT g, pg_sleep(CASE WHEN g = 21 THEN 4 ELSE 0 END) FROM generate_series(20, 22) g) s", token);
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, $"SELECT wait_event = 'PgSleep' FROM pg_stat_activity WHERE pid = {sleeper.ProcessID}", token),
                "the statement sleeping");
            Task past = await PastTheEndAsync(writer);
            await Task.WhenAll(sleeping, past);
        }

        // An insert waits for the key of a transaction that then runs past the block's end and waits for the switch,
        // which waits for the insert: the deadlock detector ends one of the two, and the other goes on.
        await using (NpgsqlConnection holder = await node.OpenAsync(Database, token))
        await using (NpgsqlConnection waiter = await node.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction holding = await holder.BeginTransactionAsync(token);
            await ExecuteAsync(holder, "INSERT INTO k (u) VALUES (30)", token);
            Task waiting = ExecuteAsync(waiter, "INSERT INTO k (u) VALUES (30), (31) ON CONFLICT DO NOTHING", token);
            Assert.IsTrue(await node.PollQueryUntilAsync(Database,
                $"SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = {waiter.ProcessID} AND locktype = 'transactionid' AND NOT granted)", token),
                "the insert waiting for the key");
            Task past = await PastTheEndAsync(holder);
            string[] outcomes = await Task.WhenAll(new[] { waiting, past }.Select(async task =>
            {
                try
                {
                    await task;
                    return "ok";
                }
                catch (PostgresException error)
                {
                    return error.SqlState;
                }
            }));
            Assert.AreEqual("40P01,ok", string.Join(",", outcomes.Order()), "the insert and the rows past the block's end");
            if (outcomes[1] == "ok")
            {
                await holding.CommitAsync(token);
            }
        }

        Assert.AreEqual("t", await node.SafePsqlAsync(Database, "SELECT count(*) = count(DISTINCT id) FROM k", token), "the rows, each with its own key");
    }

    /// <summary>
    /// A statement that waits for a switch a worker tries and can't make, here as a session holds the range row, waits
    /// twice the exchange timeout for the block in all, rather than again at each of its rows after; a load through
    /// small blocks retires none of them unused, as statements still waiting for a switch that has come, until they next
    /// look, don't bring another at once; and once more sequences than the shared table of drawing statements holds
    /// have been drawn from and dropped, a new one's statements are still counted, and their wait ends as before.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SwitchWaitsEndAndRetireNoBlockUnused()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node_a", token, "worf.sequence_range_timeout = 1000");
        await using NpgsqlConnection holder = await node.OpenAsync(Database, token);

        async Task<TimeSpan> TailAsync(string table)
        {
            _ = await node.SafePsqlAsync(Database, $"""
                CREATE TABLE {table} (id bigserial PRIMARY KEY, v int);
                SELECT worf.enable_sequence_range('{table}_id_seq', participants := '{"{}"}', range_size := 1000, max_range_size := 1000);
                """, token);
            Assert.IsTrue(await node.PollQueryUntilAsync(Database,
                $"SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = '{table}_id_seq'::regclass", token),
                $"{table}'s blocks");
            await using NpgsqlTransaction held = await holder.BeginTransactionAsync(token);
            await using (NpgsqlCommand hold = new($"SELECT 1 FROM worf.sequence_range WHERE seqoid = '{table}_id_seq'::regclass FOR NO KEY UPDATE",
                holder, held))
            {
                _ = await hold.ExecuteNonQueryAsync(token);
            }

            // The block's last twentieth starts 50 values from its end, where each row waits for the switch.
            _ = await node.SafePsqlAsync(Database, $"INSERT INTO {table} (v) SELECT 1 FROM generate_series(1, 945)", token);
            Stopwatch tail = Stopwatch.StartNew();
            _ = await node.SafePsqlAsync(Database, $"INSERT INTO {table} (v) SELECT 2 FROM generate_series(1, 40)", token);
            tail.Stop();
            await held.RollbackAsync(token);
            Assert.AreEqual("985|985", await node.SafePsqlAsync(Database, $"SELECT count(*), count(DISTINCT id) FROM {table}", token), $"{table}'s rows");
            return tail.Elapsed;
        }

        TimeSpan waited = await TailAsync("w");
        Assert.IsLessThan(TimeSpan.FromSeconds(3.5), waited, "40 rows in the block's last twentieth, which waited for the switch once");

        // Four writers draw one row at a time through blocks of ten.
        _ = await node.SafePsqlAsync(Database, """
            CREATE TABLE b (id bigserial PRIMARY KEY, v int);
            SELECT worf.enable_sequence_range('b_id_seq', participants := '{}', range_size := 10, max_range_size := 10);
            """, token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database,
            "SELECT state = 'active' AND next_lo IS NOT NULL FROM worf.sequence_ranges WHERE sequence = 'b_id_seq'::regclass", token), "b's blocks");
        await Task.WhenAll(Enumerable.Range(0, 4).Select(async _ =>
        {
            await using NpgsqlConnection connection = await node.OpenAsync(Database, token);
            for (int i = 0; i < 50; i++)
            {
                await ExecuteAsync(connection, "INSERT INTO b (v) VALUES (1)", token);
            }
        }));
        Assert.AreEqual("200|0", await node.SafePsqlAsync(Database, """
            SELECT (SELECT count(DISTINCT id) FROM b), count(*) FROM worf.sequence_range_block k
            WHERE k.seqoid = 'b_id_seq'::regclass AND k.kind = 'n' AND k.state = 'retired' AND NOT EXISTS (SELECT FROM b WHERE b.id BETWEEN k.lo AND k.hi)
            """, token), "b's rows, and its blocks retired unused");

        // More sequences than the shared table holds are drawn from and dropped.
        Stopwatch made = Stopwatch.StartNew();
        for (int batch = 0; batch < 1050; batch += 150)
        {
            _ = await node.SafePsqlAsync(Database, string.Join("\n", Enumerable.Range(batch + 1, 150).Select(i => $"""
                CREATE TABLE d{i} (id bigserial PRIMARY KEY);
                SELECT worf.enable_sequence_range('d{i}_id_seq', participants := '{"{}"}', range_size := 10);
                INSERT INTO d{i} DEFAULT VALUES;
                DROP TABLE d{i};
                """)), token);
        }

        TestContext.WriteLine($"1,050 sequences made, drawn from and dropped in {made.Elapsed}");
        waited = await TailAsync("n");
        Assert.IsLessThan(TimeSpan.FromSeconds(3.5), waited, "40 rows in a new sequence's block's last twentieth, after 1,050 others");
    }

    /// <summary>
    /// A subscription dropped as the exchange worker takes in a reply from its provider doesn't deadlock with the take-in:
    /// the take-in writes the provider's reports in the order of the sequences' names, and the drop deletes them in the
    /// order they're stored, so the drop takes the exchanges' turn first.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SubscriptionsDropWhileTheirRepliesAreTakenIn()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text); CREATE TABLE a (id bigserial PRIMARY KEY)", token);
        }

        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        _ = await nodeA.SafePsqlAsync(Database, """
            SELECT worf.enable_sequence_range('a_id_seq', participants := '{node_b}', range_size := 100);
            SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100);
            """, token);
        await WaitActiveAsync([nodeA, nodeB], token);
        string nodeAId = await nodeA.SafePsqlAsync(Database, "SELECT node_id FROM worf.local_node", token);

        // node_b stores its reports of node_a in the order of the sequences' oids, t_id_seq's first, and keeps them so for
        // two seconds, once the take-ins that settle them have passed, as idle calls leave them.
        Stopwatch ordering = Stopwatch.StartNew();
        while (true)
        {
            _ = await nodeB.SafePsqlAsync(Database, "CLUSTER worf.sequence_range_node USING sequence_range_node_pkey", token);
            await Task.Delay(TimeSpan.FromSeconds(2), token);
            if (await nodeB.SafePsqlAsync(Database, $"""
                SELECT (SELECT ctid FROM worf.sequence_range_node WHERE seqoid = 't_id_seq'::regclass AND node_id = {nodeAId})
                    < (SELECT ctid FROM worf.sequence_range_node WHERE seqoid = 'a_id_seq'::regclass AND node_id = {nodeAId})
                """, token) == "t")
            {
                break;
            }

            Assert.IsLessThan(TestNode.PollTimeout, ordering.Elapsed, "node_b's reports of node_a, t_id_seq's stored first");
        }

        // node_a draws from both sequences, and node_b's take-in holds its report of a_id_seq, before it writes that of
        // t_id_seq, until the test lets it go.
        _ = await nodeB.SafePsqlAsync(Database, """
            CREATE TABLE held ();
            CREATE FUNCTION hold_report() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    IF NEW.seqoid = 'public.a_id_seq'::regclass THEN
                        WHILE EXISTS (SELECT FROM public.held) LOOP PERFORM pg_sleep(0.05); END LOOP;
                    END IF;
                    RETURN NEW;
                END $$;
            CREATE TRIGGER hold_report BEFORE INSERT OR UPDATE ON worf.sequence_range_node FOR EACH ROW EXECUTE FUNCTION hold_report();
            INSERT INTO held DEFAULT VALUES;
            """, token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT count(nextval('a_id_seq')) + count(nextval('t_id_seq')) FROM generate_series(1, 5)", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE backend_type LIKE 'worf exchange%' AND wait_event = 'PgSleep')", token),
            "node_b's exchange worker, holding node_a's report of a_id_seq");

        // The drop starts, and the take-in goes on once it waits: a drop that deleted the report of t_id_seq and then
        // waited for that of a_id_seq would find the take-in waiting for it in turn.
        Task<string> dropping = nodeB.SafePsqlAsync(Database, "SELECT worf.drop_subscription('b_from_a')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'SELECT worf.drop_subscription%')", token),
            "the drop, waiting");
        _ = await nodeB.SafePsqlAsync(Database, "DELETE FROM held", token);
        _ = await dropping;
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM worf.subscription", token), "node_b's subscriptions");
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

        // node_c's request goes to node_b, its parent as c_from_b comes back, whatever node_c's route has become since:
        // node_b, whose own route has gone with node_a's report, answers it, and node_c takes in the answer, which
        // grants nothing and leaves node_c no route.
        string restarted = await nodeB.SafePsqlAsync(Database, "SELECT now()::text", token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_range_request WHERE seqoid = 'worf.snowflake_node'::regclass)", token), "node_c asking for a number");
        _ = await nodeC.SafePsqlAsync(Database, """
            SELECT 1 FROM worf.sequence_range WHERE seqoid = 'worf.snowflake_node'::regclass FOR NO KEY UPDATE;
            UPDATE worf.sequence_range SET parent_node = (SELECT node_id FROM worf.node WHERE node_name = 'node_b'), distance = 2
            WHERE seqoid = 'worf.snowflake_node'::regclass;
            UPDATE worf.sequence_range_request SET parent_node = (SELECT node_id FROM worf.node WHERE node_name = 'node_b')
            WHERE seqoid = 'worf.snowflake_node'::regclass;
            SELECT worf.alter_subscription_enable('c_from_b');
            """, token);
        Assert.IsTrue(await nodeC.PollQueryUntilAsync(Database, "SELECT parent_node IS NULL FROM worf.sequence_range WHERE seqoid = 'worf.snowflake_node'::regclass", token),
            "node_c taking in node_b's answer to its request");
        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.AreEqual("t|0", await nodeC.SafePsqlAsync(Database, "SELECT worf.snowflake_node() IS NULL", token) + "|" + await nodeB.SafePsqlAsync(Database, $"""
            SELECT count(*) FROM worf.sequence_range_grant
            WHERE seqoid = 'worf.snowflake_node'::regclass AND grantee_name = 'node_c' AND granted_at > '{restarted}'
            """, token), "node_c's number, and the numbers for node_c from node_b's pool");
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

        // node_b writes a few megabytes first, and node_a hears its WAL position past them, so that the restored copy,
        // which writes as it starts, stays below the position node_b last showed. node_b makes its calls one at a time,
        // so the second that node_a hears after the writes was made after them.
        _ = await nodeB.SafePsqlAsync(Database, "CREATE TABLE written AS SELECT g, repeat('x', 100) AS v FROM generate_series(1, 20000) g", token);
        await InsertAsync(nodeB, 150, token);
        const string HeardFromB = """
            SELECT worf.sequence_range_heard(node_id, true, '-infinity', false)::text FROM worf.sequence_range_node
            WHERE node_name = 'node_b' AND seqoid = 't_id_seq'::regclass
            """;
        string heard = await nodeA.SafePsqlAsync(Database, "SELECT clock_timestamp()::text", token);
        for (int call = 0; call < 2; call++)
        {
            Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, $"SELECT ({HeardFromB})::timestamptz > '{heard}'", token),
                "node_a heard node_b's WAL position past its writes");
            heard = await nodeA.SafePsqlAsync(Database, HeardFromB, token);
        }

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

        // The reset says so before it commits, and node_a draws from a new block once it has.
        long drawnTop = drawnBehind.Where(id => id.Length > 0).Max(id => long.Parse(id, CultureInfo.InvariantCulture));
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database,
            $"SELECT state = 'active' AND active_lo > {drawnTop} FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token),
            "node_a drawing from a block past the values node_b copied");
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

            Assert.AreEqual("node_b", await node.SafePsqlAsync(Database, "SELECT allocator FROM worf.sequence_ranges", token));
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

            // A load larger than a block goes through the blocks as they arrive, and a reserved one at once.
            _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'bulk' FROM generate_series(1, 1000)", token);
            _ = await node.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_range('t_id_seq', 1000)", token);

            _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'bulk' FROM generate_series(1, 1000)", token);
        }

        // Keys that rows carry: free ones are granted exactly, and ones another node holds are refused.
        // They lie just above every block handed out, node_b's reserve too.
        long free = long.Parse(await nodeA.SafePsqlAsync(Database, "SELECT max(hi) FROM worf.sequence_range_grant WHERE seqoid = 't_id_seq'::regclass", token),
            CultureInfo.InvariantCulture) + 1_000_000_000;
        _ = await nodeB.SafePsqlAsync(Database, $"SELECT worf.reserve_sequence_values('t_id_seq', {free}, {free + 99})", token);
        _ = await nodeB.SafePsqlAsync(Database, $"INSERT INTO t (id, v) SELECT g, 'imported' FROM generate_series({free}, {free + 99}) g", token);
        _ = await nodeA.SafePsqlAsync(Database, $"SELECT worf.reserve_sequence_values('t_id_seq', {free + 1_000_000_000}, {free + 1_000_000_009})", token);
        _ = await nodeA.SafePsqlAsync(Database,
            $"INSERT INTO t (id, v) SELECT g, 'imported' FROM generate_series({free + 1_000_000_000}, {free + 1_000_000_009}) g", token);
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

        Assert.HasCount(4110, keys);

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

        // node_a first draws from a block reserved for nothing, which it doesn't leave early as it would a block its
        // inserts are near the end of, where a switch the earlier loads' rate calls for, which grows the request, could
        // come once node_b has heard of the next block, during the loads.
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.reserve_sequence_range('t_id_seq', 2000)", token);
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

        // A reservation holds nothing that other sessions wait for: their inserts go on while the load's transaction is
        // open, and so does another session's reservation, with its switch.
        await using (NpgsqlConnection batch = await nodeA.OpenAsync(Database, token))
        {
            await ExecuteAsync(batch, "BEGIN; SELECT worf.reserve_sequence_range('t_id_seq', 10)", token);
            long left = long.Parse(await nodeA.SafePsqlAsync(Database, "SELECT values_left FROM worf.sequence_ranges", token), CultureInfo.InvariantCulture);
            _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('goes on')", token);
            _ = await nodeA.SafePsqlAsync(Database, $"SELECT worf.reserve_sequence_range('t_id_seq', {left + 100})", token);
            await ExecuteAsync(batch, "INSERT INTO t (v) SELECT 'batch' FROM generate_series(1, 10); COMMIT", token);
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

        // A sequence renamed or moved to the name of one node_b dropped joins the epoch as it takes the name, and one that
        // issued values before, which reach into the root, is refused rather than drawing from blocks they may lie in.
        _ = await nodeB.SafePsqlAsync(Database, "DROP TABLE u; CREATE SEQUENCE renamed_seq; ALTER SEQUENCE renamed_seq RENAME TO u_id_seq", token);
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database, "SELECT EXISTS (SELECT 1 FROM worf.sequence_range WHERE seqoid = 'u_id_seq'::regclass)", token),
            "node_b's renamed u_id_seq joined");
        _ = await nodeB.SafePsqlAsync(Database, """
            DROP SEQUENCE u_id_seq;
            CREATE SCHEMA elsewhere;
            CREATE SEQUENCE elsewhere.u_id_seq;
            SELECT nextval('elsewhere.u_id_seq') FROM generate_series(1, 3);
            ALTER SEQUENCE elsewhere.u_id_seq SET SCHEMA public;
            """, token);
        Assert.AreEqual("refused", await nodeB.SafePsqlAsync(Database, "SELECT state FROM worf.sequence_range WHERE seqoid = 'u_id_seq'::regclass", token),
            "node_b's moved u_id_seq, which issued values");
        PostgresException parked = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database, "SELECT nextval('u_id_seq')", token));
        Assert.AreEqual("2200H", parked.SqlState, "node_b's refused u_id_seq issues nothing");

        // Once node_a disables the epoch, node_b forgets the sequence it dropped, so a sequence it creates again under the
        // name doesn't join the disabled epoch.
        _ = await nodeB.SafePsqlAsync(Database, "DROP SEQUENCE u_id_seq", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.disable_sequence_range('u_id_seq')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT NOT EXISTS (SELECT 1 FROM worf.sequence_range_dropped WHERE name = 'public.u_id_seq')", token),
            "node_b's record of its dropped u_id_seq gone");
        _ = await nodeB.SafePsqlAsync(Database, "CREATE SEQUENCE u_id_seq", token);
        Assert.AreEqual("f", await nodeB.SafePsqlAsync(Database, "SELECT EXISTS (SELECT 1 FROM worf.sequence_range WHERE seqoid = 'u_id_seq'::regclass)", token),
            "node_b's new u_id_seq joined the disabled epoch");

        // One created again before the word of the disable arrives, as the record put back here stands for, joins the
        // epoch, and stays parked once the word comes, as the epoch's values may lie in node_a's blocks, until node_a
        // enables the sequence again, naming node_b, whose sequence then takes blocks of the new epoch.
        string record = await nodeA.SafePsqlAsync(Database, """
            SELECT owner_node || ', ' || epoch || ', ' || range_size || ', ' || max_range_size || ', ' || coalesce(root_start::text, 'NULL') || ', ' || root_end
            FROM worf.sequence_range WHERE seqoid = 'u_id_seq'::regclass
            """, token);
        _ = await nodeB.SafePsqlAsync(Database, $"""
            DROP SEQUENCE u_id_seq;
            INSERT INTO worf.sequence_range_dropped (name, owner_node, epoch, range_size, max_range_size, root_start, root_end, owned)
                VALUES ('public.u_id_seq', {record}, false);
            CREATE SEQUENCE u_id_seq START 5 CACHE 3;
            """, token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, """
            SELECT state = 'disabled' AND detail LIKE 'the epoch is disabled%' FROM worf.sequence_range WHERE seqoid = 'u_id_seq'::regclass
            """, token), "node_b's new u_id_seq parked in the disabled epoch");
        PostgresException disabled = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database, "SELECT nextval('u_id_seq')", token));
        Assert.AreEqual("2200H", disabled.SqlState, "node_b's parked u_id_seq issues nothing");
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('u_id_seq', participants := '{node_b}')", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_range WHERE seqoid = 'u_id_seq'::regclass", token),
            "node_b's u_id_seq in the new epoch");
        _ = await nodeB.SafePsqlAsync(Database, "SELECT nextval('u_id_seq')", token);

        // node_b hears of grants beyond node_a's watermark, as when node_a, the owner, went back to before them by less
        // than its WAL position shows. node_a resets, and its fence holds, here on a neighbor that never reports, while
        // node_b asks for a block. With an exchange interval of ten minutes, node_b still asks node_a every ten checks,
        // and draws from a block beyond that watermark soon after the fence lifts: about a second here, and over three on
        // a busy macOS runner, where node_b's switch alone takes most of one.
        _ = await nodeB.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.sequence_range_exchange_interval = '10min'; SELECT pg_reload_conf()", token);
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO worf.sequence_range_link VALUES ('ghost', 4242, 10)", token);
        int logpos = nodeA.ReadLog().Length;
        string beyond = await nodeB.SafePsqlAsync(Database, "UPDATE worf.sequence_range SET watermark = watermark + 1000000 WHERE seqoid = 't_id_seq'::regclass RETURNING watermark", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.reset_sequence_range('t_id_seq')", token);
        Assert.IsTrue(await nodeA.PollLogAsync(logpos, $"has heard of grants up to {beyond}, beyond what this node has handed out", token), "node_a reset");
        await Task.Delay(TimeSpan.FromSeconds(4), token);

        // The fence lifts just after one of node_b's calls, which go over the session node_b keeps on node_a, where each
        // starts a query.
        await using NpgsqlConnection calls = await nodeA.OpenAsync(Database, token);
        await using NpgsqlCommand started = new("SELECT max(query_start) FROM pg_stat_activity WHERE application_name = 'b_from_a_range'", calls);
        object? called = await started.ExecuteScalarAsync(token);
        Stopwatch asked = Stopwatch.StartNew();
        while (Equals(await started.ExecuteScalarAsync(token), called) && asked.Elapsed < TestNode.PollTimeout)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(20), token);
        }

        Assert.IsLessThan(TestNode.PollTimeout, asked.Elapsed, "node_b's next call");

        // The fence stops waiting for the node gone for good, named as the detail names it, and lifts, as node_b has
        // reported.
        Assert.Contains("node 4242", await nodeA.SafePsqlAsync(Database, "SELECT detail FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass", token),
            "node_a's fence waiting for the node gone for good");
        _ = await nodeA.SafePsqlAsync(Database, "DELETE FROM worf.sequence_range_link WHERE node_id = 4242; SELECT worf.activate_sequence_range('t_id_seq', '4242')", token);
        Stopwatch lifted = Stopwatch.StartNew();
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, $"SELECT state = 'active' AND active_lo > {beyond} FROM worf.sequence_ranges WHERE sequence = 't_id_seq'::regclass", token),
            "node_b drawing from a block beyond the watermark it heard of");
        Assert.IsLessThan(10.0, lifted.Elapsed.TotalSeconds, "node_b's block soon after node_a's fence lifted");
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
    /// Transactions that hold a sequence at its block's end, as slow ones among many sessions, or one for longer than the
    /// exchange timeout, hold up no inserts: the switch goes on beside them, and the other sessions' inserts go on into
    /// the next block rather than run the block out, as another sequence's go on switching.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task HoldersAtABlocksEndHoldUpOnlyTheirSequence()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode node = await StartNodeAsync("node_a", token);
        _ = await node.SafePsqlAsync(Database, """
            ALTER SYSTEM RESET worf.sequence_range_check_interval;
            ALTER SYSTEM SET worf.sequence_range_timeout = '2s';
            SELECT pg_reload_conf();
            CREATE TABLE t1 (id bigserial PRIMARY KEY, v int);
            CREATE TABLE t2 (id bigserial PRIMARY KEY, v int);
            SELECT worf.enable_sequence_range('t1_id_seq', participants := '{}', range_size := 1000, max_range_size := 1000);
            SELECT worf.enable_sequence_range('t2_id_seq', participants := '{}', range_size := 1000, max_range_size := 1000);
            """, token);
        Assert.IsTrue(await node.PollQueryUntilAsync(Database, """
            SELECT count(*) = 2 AND bool_and(state = 'active' AND next_lo IS NOT NULL) FROM worf.sequence_ranges
            """, token), "drawing from blocks with next ones ready");

        // One in thirty of each session's transactions holds t1 for a second, longer than a switch first waits.
        const int Sessions = 24;
        const int Transactions = 150;
        await Task.WhenAll(Enumerable.Range(0, Sessions).Select(async session =>
        {
            await using NpgsqlConnection connection = await node.OpenAsync(Database, token);
            for (int i = 0; i < Transactions; i++)
            {
                await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(token);
                await using (NpgsqlCommand insert = new("INSERT INTO t1 (v) VALUES (1)", connection, transaction))
                {
                    _ = await insert.ExecuteNonQueryAsync(token);
                }

                if (i % 30 == session % 30)
                {
                    await using NpgsqlCommand sleep = new("SELECT pg_sleep(1)", connection, transaction);
                    _ = await sleep.ExecuteNonQueryAsync(token);
                }

                await transaction.CommitAsync(token);
            }
        }));
        Assert.AreEqual($"{Sessions * Transactions}", await node.SafePsqlAsync(Database, "SELECT count(*) FROM t1", token));

        // A transaction holds t2 for 6 s once it reaches its block's last twentieth, which the other sessions' inserts
        // into t2 don't wait for, and t1's go on through its blocks, each switch in under a second and a half on a busy
        // runner, against the more than one and a half a switch that waited for the holders took. The server times each
        // of t1's inserts, as the test host's own clock counts the time its other tests keep it from reading the result,
        // seconds at a time on a Windows runner. Each session's backend has started, and run an insert, before then, as
        // a dozen backends that start at once take over a second each to run their first statement on a macOS runner.
        List<NpgsqlConnection> sessions = [];
        try
        {
            for (int started = 0; started < 12; started++)
            {
                NpgsqlConnection session = await node.OpenAsync(Database, token);
                sessions.Add(session);
                await using NpgsqlCommand warm = new($"{(started < 4 ? "SET log_min_duration_statement = 0; " : "")}INSERT INTO t1 (v) VALUES (0)", session);
                _ = await warm.ExecuteNonQueryAsync(token);
            }

            await HeldPhaseAsync();
        }
        finally
        {
            foreach (NpgsqlConnection session in sessions)
            {
                await session.DisposeAsync();
            }
        }

        async Task HeldPhaseAsync()
        {
        await using NpgsqlConnection holder = await node.OpenAsync(Database, token);
        await using NpgsqlTransaction held = await holder.BeginTransactionAsync(token);
        long left = long.Parse(await node.SafePsqlAsync(Database,
            "SELECT values_left FROM worf.sequence_ranges WHERE sequence = 't2_id_seq'::regclass", token), CultureInfo.InvariantCulture);
        await using (NpgsqlCommand insert = new($"INSERT INTO t2 (v) SELECT 0 FROM generate_series(1, {left - 30})", holder, held))
        {
            _ = await insert.ExecuteNonQueryAsync(token);
        }

        const string T1Insert = "INSERT INTO t1 (v) SELECT 2 FROM generate_series(1, 10)";
        int timed = node.ReadLog().Length;
        using CancellationTokenSource holding = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task<long>[] t1Inserts = [.. Enumerable.Range(0, 4).Select(async i =>
        {
            await using NpgsqlCommand insert = new(T1Insert, sessions[i]);
            long slowest = 0;
            while (!holding.IsCancellationRequested)
            {
                Stopwatch elapsed = Stopwatch.StartNew();
                _ = await insert.ExecuteNonQueryAsync(token);
                slowest = Math.Max(slowest, elapsed.ElapsedMilliseconds);
            }

            return slowest;
        })];
        Task[] t2Inserts = [.. Enumerable.Range(4, 8).Select(async t2Session =>
        {
            await using NpgsqlCommand insert = new("INSERT INTO t2 (v) VALUES (2)", sessions[t2Session]);
            for (int i = 0; i < 20; i++)
            {
                _ = await insert.ExecuteNonQueryAsync(token);
            }
        })];
        await Task.Delay(TimeSpan.FromSeconds(6), token);
        Assert.AreEqual(t2Inserts.Length, t2Inserts.Count(insert => insert.IsCompleted), "t2's inserts went on beside the holder");
        await held.CommitAsync(token);
        await Task.WhenAll(t2Inserts);
        await holding.CancelAsync();
        long t1Seen = (await Task.WhenAll(t1Inserts)).Max();
        double[] t1Times = [.. node.ReadLog()[timed..].Split('\n')
            .Where(line => line.Contains($": {T1Insert}", StringComparison.Ordinal) && line.Contains("  execute ", StringComparison.Ordinal))
            .Select(line => double.Parse(line[(line.IndexOf("duration: ", StringComparison.Ordinal) + 10)..line.IndexOf(" ms", StringComparison.Ordinal)],
                CultureInfo.InvariantCulture))];
        Assert.IsGreaterThan(100, t1Times.Length, "t1's inserts the server timed");
        Assert.IsLessThan(3000.0, t1Times.Max(), $"t1's slowest insert on the server, in ms, while t2 was held for 6 s; the test host saw {t1Seen}");
        Assert.AreEqual($"{left - 30 + 160}", await node.SafePsqlAsync(Database, "SELECT count(*) FROM t2", token));
        Assert.DoesNotContain("reached maximum value", node.ReadLog(), "inserts that ran a block out");
        }
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

        // A session's advisory lock, of whatever key, holds up no reservation of the clock, as one of
        // hashtext('worf.snowflake_clock') did until 454a1ab: node_a issues IDs for 15 s, each within 5 s, past the 10 s
        // its clock is reserved ahead.
        await using (NpgsqlConnection holder = await nodeA.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand hold = new("SELECT pg_advisory_lock(pg_catalog.hashtext('worf.snowflake_clock'))", holder))
            {
                _ = await hold.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlConnection issuer = await nodeA.OpenAsync(Database, token);
            await using (NpgsqlCommand timeout = new("SET statement_timeout = '5s'", issuer))
            {
                _ = await timeout.ExecuteNonQueryAsync(token);
            }

            List<string> failed = [];
            await using NpgsqlCommand issue = new("SELECT worf.snowflake_id()", issuer);
            Stopwatch issuing = Stopwatch.StartNew();
            while (issuing.Elapsed < TimeSpan.FromSeconds(15))
            {
                try
                {
                    _ = await issue.ExecuteScalarAsync(token);
                }
                catch (PostgresException failure)
                {
                    failed.Add($"{failure.SqlState} {failure.MessageText}");
                }

                await Task.Delay(TimeSpan.FromMilliseconds(50), token);
            }

            Assert.AreEqual("", string.Join("\n", failed.Distinct()), "node_a's IDs beside a session's advisory lock");
        }

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
        string database = Database, string applyDelay = "0", string? providerDsn = null)
    {
        _ = await subscriber.SafePsqlAsync(database, $$"""
            SELECT worf.create_subscription(
                subscription_name := '{{name}}',
                provider_dsn := '{{providerDsn ?? provider.Dsn(Database, SuperUser)}}',
                synchronize_structure := false,
                synchronize_data := false,
                forward_origins := '{}',
                apply_delay := '{{applyDelay}}')
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

        // A node's last draws raise its high water, and its blocks change its state, which reach the nodes it doesn't
        // exchange with only as a neighbor relays its report, so each node's record of every other node's report is waited
        // for too, before the while.
        if (managed)
        {
            Stopwatch heard = Stopwatch.StartNew();
            while (!await ReportsHeardAsync(nodes, token))
            {
                Assert.IsLessThan(TestNode.PollTimeout, heard.Elapsed, "every node's record of the others' reports");
                await Task.Delay(TimeSpan.FromMilliseconds(200), token);
            }
        }

        // Each exchange goes over the session its node keeps from the one before, so while the nodes manage sequences, each
        // slot's subscriber has a session for its exchanges that started before the while, where a session for each call
        // would last only as long as the call.
        TimeSpan idle = span ?? TimeSpan.FromSeconds(2);
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        const string Next = """
            SELECT pg_snapshot_xmax(pg_current_snapshot()), clock_timestamp(),
                (SELECT count(*) FROM pg_replication_slots WHERE plugin = 'worf' AND database = current_database()), pg_current_wal_insert_lsn()
            """;
        // What each node holds of the sequences' ranges, the nodes it heard of, its blocks, grants and requests, and the
        // nodes a lift stopped waiting for, for a failure's message, which names what changed.
        const string Held = """
            SELECT coalesce(string_agg(row, E'\n' ORDER BY row), '') FROM (
                SELECT 'range ' || to_jsonb(r)::text AS row FROM worf.sequence_range r
                UNION ALL SELECT 'node ' || to_jsonb(n)::text FROM worf.sequence_range_node n
                UNION ALL SELECT 'block ' || to_jsonb(b)::text FROM worf.sequence_range_block b
                UNION ALL SELECT 'grant ' || to_jsonb(g)::text FROM worf.sequence_range_grant g
                UNION ALL SELECT 'request ' || to_jsonb(q)::text FROM worf.sequence_range_request q
                UNION ALL SELECT 'absent ' || to_jsonb(a)::text FROM worf.sequence_range_absent a) rows
            """;
        string[] heldBefore = managed ? await Task.WhenAll(nodes.Select(node => node.SafePsqlAsync(Database, Held, token))) : [];
        string[][] before = await Task.WhenAll(nodes.Select(async node => (await node.SafePsqlAsync(Database, Next, token)).Split('|')));
        await Task.Delay(idle, token);
        string[][] after = await Task.WhenAll(nodes.Select(async node => (await node.SafePsqlAsync(Database, Next, token)).Split('|')));
        for (int i = 0; i < nodes.Length; i++)
        {
            // The records written meanwhile, which name what wrote them, are read first, for either check's message.
            string written = "";
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
                written = await nodes[i].SafePsqlAsync(Database, $"""
                    SELECT string_agg(concat_ws(' ', resource_manager, record_type, 'xid', xid, description, block_ref), '; ')
                    FROM pg_get_wal_records_info('{before[i][3]}', greatest('{before[i][3]}'::pg_lsn + 1, least('{after[i][3]}'::pg_lsn, pg_current_wal_flush_lsn())))
                    WHERE resource_manager <> 'Standby' AND record_type NOT IN ('CHECKPOINT_REDO', 'CHECKPOINT_ONLINE', 'FPI_FOR_HINT')
                        AND NOT (resource_manager = 'Heap2' AND record_type IN ('PRUNE', 'PRUNE_ON_ACCESS'))
                    """, token);
            }

            string changed = "";
            if (managed && before[i][0] != after[i][0])
            {
                string heldAfter = await nodes[i].SafePsqlAsync(Database, Held, token);
                HashSet<string> rows = [.. heldBefore[i].Split('\n')];
                changed = $"\nbefore: {string.Join("\n  ", heldBefore[i].Split('\n').Except(heldAfter.Split('\n')))}" +
                    $"\nafter: {string.Join("\n  ", heldAfter.Split('\n').Where(row => !rows.Contains(row)))}";
            }

            Assert.AreEqual(before[i][0], after[i][0], $"{nodes[i].Name} used transaction IDs while idle: {written}{changed}");
            if (managed)
            {
                Assert.AreEqual("t", await nodes[i].SafePsqlAsync(Database, $"""
                    SELECT count(*) >= {after[i][2]} FROM pg_stat_activity WHERE application_name LIKE '%\_range' AND backend_start < '{before[i][1]}'
                    """, token), $"{nodes[i].Name}'s neighbors' sessions for exchanges, kept from before the while");
            }

            Assert.AreEqual("", written, $"{nodes[i].Name} wrote WAL while idle");
        }
    }

    /// <summary>
    /// Checks whether each node's record of every other node, for each sequence both manage, is what the other node
    /// reports of itself: its high water, its state, as its reports give it, and its epoch, which an exchange that finds
    /// them unchanged compares, as it does the watermarks the nodes settle on.
    /// </summary>
    private static async Task<bool> ReportsHeardAsync(TestNode[] nodes, CancellationToken token)
    {
        string[] own = await Task.WhenAll(nodes.Select(node => node.SafePsqlAsync(Database, """
            SELECT coalesce(string_agg(r.seqoid::regclass::text || '=' || concat_ws('|', r.high_water, CASE WHEN r.fenced_at IS NOT NULL THEN 'fenced' ELSE r.state END,
                r.epoch), ','), '')
            FROM worf.sequence_range r
            """, token)));
        string[] heard = await Task.WhenAll(nodes.Select(node => node.SafePsqlAsync(Database, """
            SELECT coalesce(string_agg(n.node_name || ':' || n.seqoid::regclass::text || '=' || concat_ws('|', n.high_water, n.state, n.epoch), ','), '')
            FROM worf.sequence_range_node n CROSS JOIN worf.local_node l WHERE n.node_id <> l.node_id
            """, token)));
        Dictionary<string, string> owned = [];
        for (int i = 0; i < nodes.Length; i++)
        {
            foreach (string entry in own[i].Split(',', StringSplitOptions.RemoveEmptyEntries))
            {
                owned[$"{nodes[i].Name}:{entry[..entry.IndexOf('=', StringComparison.Ordinal)]}"] = entry[(entry.IndexOf('=', StringComparison.Ordinal) + 1)..];
            }
        }

        return heard.SelectMany(records => records.Split(',', StringSplitOptions.RemoveEmptyEntries)).All(record =>
        {
            int equals = record.IndexOf('=', StringComparison.Ordinal);
            return !owned.TryGetValue(record[..equals], out string? value) || value == record[(equals + 1)..];
        });
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken token)
    {
        await using NpgsqlCommand command = new(sql, connection);
        _ = await command.ExecuteNonQueryAsync(token);
    }

    /// <summary>
    /// Locks a node's sequence range table, so that its neighbors' exchanges with it wait until they time out, and
    /// returns once one waits: on the table, or on the node's exchange turn, which the node's own exchange worker holds
    /// as it waits on the table to take in a reply. Closing the connection lets them go.
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
                    WHERE (l.relation = 'worf.sequence_range'::regclass OR l.locktype = 'advisory') AND NOT l.granted
                        AND a.backend_type = 'client backend' AND a.query LIKE '%sequence_range_exchange(%')
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
    /// Finds the lock waits a log with log_lock_waits on reports for rows of worf.sequence_range_node, each with the lines
    /// of its own entry, the wait's and its DETAIL and CONTEXT, which name the relation of a row it waits for.
    /// </summary>
    private static string[] ReportRowWaits(string log)
    {
        string[] lines = log.Split('\n');
        List<string> waits = [];
        for (int i = 0; i < lines.Length; i++)
        {
            int prefix = lines[i].IndexOf("]: ", StringComparison.Ordinal);
            if (prefix < 0 || !lines[i].Contains("still waiting for", StringComparison.Ordinal))
            {
                continue;
            }

            string entry = string.Join("\n", lines.Skip(i).Take(4).TakeWhile((line, k) => k == 0 || line.StartsWith(lines[i][..prefix], StringComparison.Ordinal)));
            if (entry.Contains("relation \"sequence_range_node\"", StringComparison.Ordinal))
            {
                waits.Add(entry);
            }
        }

        return [.. waits];
    }

    /// <summary>
    /// Counts, sampling as fast as a session can for a while after a delay, the times a node's exchanges are found in the
    /// queue of a sequence's range row: holding the row's tuple lock and waiting for the transaction that holds the row.
    /// </summary>
    private static async Task<long> QueuedOnRangeRowsAsync(TestNode node, TimeSpan delay, TimeSpan window, CancellationToken token)
    {
        await Task.Delay(delay, token);
        await using NpgsqlConnection watcher = await node.OpenAsync(Database, token);
        await using NpgsqlCommand sample = new("""
            SELECT count(*) FROM pg_catalog.pg_locks w JOIN pg_catalog.pg_stat_activity a ON a.pid = w.pid
            WHERE NOT w.granted AND w.locktype = 'transactionid' AND (a.backend_type <> 'client backend' OR a.application_name LIKE '%\_range')
                AND EXISTS (SELECT 1 FROM pg_catalog.pg_locks t WHERE t.pid = w.pid AND t.granted AND t.locktype = 'tuple'
                    AND t.relation = 'worf.sequence_range'::pg_catalog.regclass)
            """, watcher);
        long queued = 0;
        Stopwatch watching = Stopwatch.StartNew();
        while (watching.Elapsed < window)
        {
            queued += (long)(await sample.ExecuteScalarAsync(token))!;
        }

        return queued;
    }

    /// <summary>
    /// Runs a statement that draws from a sequence on a session whose statements time out after 8 s, as one that waits
    /// for a block then does.
    /// </summary>
    private static async Task DrawUntilTimeoutAsync(TestNode node, string statement, CancellationToken token)
    {
        await using NpgsqlConnection drawer = await node.OpenAsync(Database, token);
        await using NpgsqlCommand draw = new($"SET statement_timeout = '8s'; {statement}", drawer);
        try
        {
            _ = await draw.ExecuteNonQueryAsync(token);
        }
        catch (PostgresException timedOut) when (timedOut.SqlState == "57014")
        {
            // The draw waited for a block until it timed out.
        }
    }

    /// <summary>
    /// Sends a node a message made up as a neighbor's until it shows taken in: a take-in that finds a row it writes held
    /// by another transaction, as the range worker's, leaves the sequence to the neighbor's next message, rolling back what
    /// it did, as a neighbor's own messages, which repeat what they carry until it's answered, send.
    /// </summary>
    private static async Task ExchangeUntilAsync(TestNode node, string message, Func<Task<bool>> taken, string what, CancellationToken token)
    {
        Stopwatch sending = Stopwatch.StartNew();
        do
        {
            Assert.IsLessThan(TestNode.PollTimeout, sending.Elapsed, what);
            _ = await node.SafePsqlAsync(Database, $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{message}')", token);
        }
        while (!await taken());
    }

    /// <summary>
    /// Requests to reset that node_a, node_b's grantor, makes up as a fence's lift asks, naming no block node_b holds
    /// from it, reset node_b's sequence no more often than any neighbor's requests do, however many come and though
    /// node_b draws between them. One that names a block node_b holds from node_a resets it at once, in a window of the
    /// lifts' own, and the same request again, once node_b has given the block up, doesn't, nor do ones that name the
    /// blocks node_b got since.
    /// </summary>
    private static async Task LiftsResetOnlyAsTheyNameHeldBlocksAsync(TestNode nodeA, TestNode nodeB, string nodeAId, string slot, CancellationToken token)
    {
        string[] ownedNow = (await nodeA.SafePsqlAsync(Database, """
            SELECT epoch, range_size, max_range_size, coalesce(watermark::text, 'null'), root_start, root_end
            FROM worf.sequence_range WHERE seqoid = 't_id_seq'::regclass
            """, token)).Split('|');
        string Lift(string reset, string grants) => $$"""
            {"node_id": {{nodeAId}}, "node_name": "node_a", "slot": "{{slot}}", "sysid": "1", "timeline": 1, "wal_lsn": "0/0", "sequences": [
                {"name": "public.t_id_seq", "owner_id": {{nodeAId}}, "epoch": {{ownedNow[0]}}, "range_size": {{ownedNow[1]}}, "max_range_size": {{ownedNow[2]}},
                    "distance": 0, "high_water": null, "watermark": {{ownedNow[3]}}, "state": "active", "root_start": {{ownedNow[4]}}, "root_end": {{ownedNow[5]}},
                    "reset": "{{reset}}", "reset_grants": [{{grants}}], "reset_done": null, "held_complete": false, "held": [], "requests": [], "reports": [],
                    "grants": [], "acks": []}]}
            """;
        const string Lifted = "node node_a lifted a fence over grants of this node's that overlap another's";
        int lifting = nodeB.ReadLog().Length;
        for (int i = 1; i <= 4; i++)
        {
            await InsertAsync(nodeB, 20, token);
            _ = await nodeB.SafePsqlAsync(Database,
                $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{Lift($"FFFFFFFF/0000000{i}", "")}')", token);
        }

        Assert.IsLessThanOrEqualTo(1, nodeB.ReadLog()[lifting..].Split(Lifted).Length - 1, "node_b's resets on lifts that name none of its blocks");
        await InsertAsync(nodeB, 20, token);
        string heldBlock = await nodeB.SafePsqlAsync(Database, $"""
            SELECT block_id FROM worf.sequence_range_block
            WHERE seqoid = 't_id_seq'::regclass AND from_node = {nodeAId} AND state <> 'abandoned' ORDER BY received_at DESC LIMIT 1
            """, token);
        Assert.AreNotEqual("", heldBlock, "a block node_b holds from node_a");
        lifting = nodeB.ReadLog().Length;
        Stopwatch window = Stopwatch.StartNew();
        await ExchangeUntilAsync(nodeB, Lift("FFFFFFFF/00000010", $"\"{heldBlock}\""),
            () => Task.FromResult(nodeB.ReadLog()[lifting..].Contains(Lifted, StringComparison.Ordinal)), "node_b's reset on a lift that names a block it holds", token);
        Assert.AreEqual(1, nodeB.ReadLog()[lifting..].Split(Lifted).Length - 1, "node_b's reset on a lift that names a block it holds");
        await InsertAsync(nodeB, 20, token);
        _ = await nodeB.SafePsqlAsync(Database,
            $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{Lift("FFFFFFFF/00000011", $"\"{heldBlock}\"")}')", token);
        Assert.AreEqual(1, nodeB.ReadLog()[lifting..].Split(Lifted).Length - 1, "node_b's resets on the same lift again, once it gave the block up");

        // Lifts that name the blocks node_b got since, as its answers list them, reset it no more often than other
        // requests do: the lifts' window began with the reset above, and lasts ten seconds, which a slow host's draws of
        // new blocks may outlast, when one more reset may come.
        bool late = false;
        for (int i = 0; i < 3; i++)
        {
            await InsertAsync(nodeB, 20, token);
            string fresh = await nodeB.SafePsqlAsync(Database, $"""
                SELECT block_id FROM worf.sequence_range_block
                WHERE seqoid = 't_id_seq'::regclass AND from_node = {nodeAId} AND state <> 'abandoned' ORDER BY received_at DESC LIMIT 1
                """, token);
            Assert.AreNotEqual("", fresh, "a block node_b got since");
            late |= window.Elapsed >= TimeSpan.FromSeconds(10);
            _ = await nodeB.SafePsqlAsync(Database,
                $"SET SESSION AUTHORIZATION {SuperUser}; SELECT worf.sequence_range_exchange('{Lift($"FFFFFFFF/0000002{i}", $"\"{fresh}\"")}')", token);
        }

        Assert.IsLessThanOrEqualTo(late ? 2 : 1, nodeB.ReadLog()[lifting..].Split(Lifted).Length - 1, "node_b's resets on lifts that name the blocks it got since");
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
    // Backs a running node's data files up, with the WAL written as the backup ran, or without WAL, which CopyWal adds,
    // as a snapshot of a running server's storage keeps its data files and WAL as of different moments.
    private static async Task BackUpDataAsync(TestNode node, string target, CancellationToken token, bool wal = false)
    {
        (int exit, string output) = await node.RunProgramAsync("pg_basebackup",
            ["-D", target, "-d", node.Dsn(Database, SuperUser), "-X", wal ? "stream" : "none", "-c", "fast"], token);
        Assert.AreEqual(0, exit, output);
    }

    // Copies a running node's replication slots as they are now into a backup of its data files, as a snapshot of its
    // storage keeps them.
    private static void CopySlots(TestNode node, string target)
    {
        string slots = Path.Join(target, "pg_replslot");
        Directory.Delete(slots, recursive: true);
        CopyDirectory(Path.Join(node.Cluster.DataDirectory, "pg_replslot"), slots);
    }

    // Copies a running node's WAL as it is now into a backup of its data files.
    private static void CopyWal(TestNode node, string target)
    {
        foreach (string file in Directory.EnumerateFiles(Path.Join(node.Cluster.DataDirectory, "pg_wal")))
        {
            File.Copy(file, Path.Join(target, "pg_wal", Path.GetFileName(file)), overwrite: true);
        }
    }

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
        string? target = null, Func<Task>? beforeStart = null)
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

        if (beforeStart is not null)
        {
            await beforeStart();
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

    // A library the server preloads that answers the lookups of test names: worf-slow.test takes 7 s to fail,
    // worf-two.test has 127.0.0.2 before 127.0.0.1, worf-first.test 127.0.0.1 before 127.0.0.2, worf-later.test takes 8 s
    // to find 127.0.0.1, worf-none.test has none, worf-delay.test and worf-late.test take 0.3 s
    // and 1 s to find 127.0.0.1, worf-standby.test takes 0.3 s to find 127.0.0.2, and the machine's own name has
    // 127.0.0.3 alone.
    private const string LookupLibrary = """
        #define _GNU_SOURCE
        #include <arpa/inet.h>
        #include <dlfcn.h>
        #include <netdb.h>
        #include <stdlib.h>
        #include <string.h>
        #include <unistd.h>

        static struct addrinfo *v4(const char *ip, const char *service, const struct addrinfo *hints)
        {
            struct addrinfo *ai = calloc(1, sizeof(struct addrinfo) + sizeof(struct sockaddr_in));
            struct sockaddr_in *sin = (struct sockaddr_in *) (ai + 1);
            sin->sin_family = AF_INET;
            sin->sin_port = htons(service ? atoi(service) : 0);
            inet_pton(AF_INET, ip, &sin->sin_addr);
            ai->ai_family = AF_INET;
            ai->ai_socktype = hints && hints->ai_socktype ? hints->ai_socktype : SOCK_STREAM;
            ai->ai_protocol = hints ? hints->ai_protocol : 0;
            ai->ai_addrlen = sizeof(struct sockaddr_in);
            ai->ai_addr = (struct sockaddr *) sin;
            return ai;
        }

        int getaddrinfo(const char *node, const char *service, const struct addrinfo *hints, struct addrinfo **res)
        {
            static int (*next)(const char *, const char *, const struct addrinfo *, struct addrinfo **);
            char me[256] = "";
            gethostname(me, sizeof(me) - 1);
            if (node && strcmp(node, "worf-slow.test") == 0)
            {
                sleep(7);
                return EAI_NONAME;
            }

            if (node && strcmp(node, "worf-two.test") == 0)
            {
                *res = v4("127.0.0.2", service, hints);
                (*res)->ai_next = v4("127.0.0.1", service, hints);
                return 0;
            }

            if (node && strcmp(node, "worf-none.test") == 0)
                return EAI_NONAME;

            if (node && strcmp(node, "worf-first.test") == 0)
            {
                *res = v4("127.0.0.1", service, hints);
                (*res)->ai_next = v4("127.0.0.2", service, hints);
                return 0;
            }

            if (node && strcmp(node, "worf-later.test") == 0)
            {
                sleep(8);
                *res = v4("127.0.0.1", service, hints);
                return 0;
            }

            if (node && (strcmp(node, "worf-delay.test") == 0 || strcmp(node, "worf-late.test") == 0 || strcmp(node, "worf-standby.test") == 0))
            {
                usleep(strcmp(node, "worf-late.test") == 0 ? 1000000 : 300000);
                *res = v4(strcmp(node, "worf-standby.test") == 0 ? "127.0.0.2" : "127.0.0.1", service, hints);
                return 0;
            }

            if (node && me[0] && strcmp(node, me) == 0)
            {
                *res = v4("127.0.0.3", service, hints);
                return 0;
            }

            if (!next)
                next = dlsym(RTLD_NEXT, "getaddrinfo");
            return next(node, service, hints, res);
        }
        """;

    /// <summary>
    /// Copies a directory as a backup takes a data directory, leaving out the files that belong to the server that ran
    /// it, as pg_basebackup does: Windows may still list a stopped server's postmaster.pid, deleted, which can't be read.
    /// </summary>
    private static void CopyDirectory(string source, string target)
    {
        _ = Directory.CreateDirectory(target);
        foreach (string file in Directory.EnumerateFiles(source).Where(file => Path.GetFileName(file) is not ("postmaster.pid" or "postmaster.opts")))
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

    // Compiles a C source into a shared library in a directory, for a server to preload, and returns the library's path.
    private static async Task<string> CompileAsync(DirectoryInfo directory, string name, string source, CancellationToken token)
    {
        string file = Path.Join(directory.FullName, name);
        string library = Path.Join(directory.FullName, "lib" + Path.GetFileNameWithoutExtension(name) + ".so");
        await File.WriteAllTextAsync(file, source, token);
        (int compiled, string output, string error) = await ProcessRunner.RunAsync("cc", ["-shared", "-fPIC", "-O2", "-o", library, file, "-ldl"], token);
        Assert.AreEqual(0, compiled, output + error);
        return library;
    }

    // A form of statements with a 2 s statement_timeout set right before its last, so that the statements that set it up
    // don't spend the time the last has to reach a wait. A form with a dollar-quoted body is one statement.
    private static string TimedLast(string form)
    {
        // The session reads the catalogs the guard reads first, so that a busy host's first reads of them, in a new
        // session, don't use up the statement's time before its wait.
        const string Warm = "SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_language l ON l.oid = p.prolang " +
            "JOIN pg_catalog.pg_type t ON t.oid = p.prorettype JOIN pg_catalog.pg_constraint c ON c.contypid = t.oid; ";
        int last = form.Contains("$$", StringComparison.Ordinal) ? -1 : form.LastIndexOf("; ", StringComparison.Ordinal);
        return last < 0 ? $"{Warm}SET statement_timeout = '2s'; {form}" : $"{Warm}{form[..(last + 2)]}SET statement_timeout = '2s'; {form[(last + 2)..]}";
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token, string? setting = null,
        IReadOnlyDictionary<string, string?>? environment = null)
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
            .. setting is null ? Array.Empty<string>() : [setting],
        ], token, environment: environment);
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
