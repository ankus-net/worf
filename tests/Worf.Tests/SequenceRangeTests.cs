using System.Globalization;
using Npgsql;

namespace Worf.Tests;

/// <summary>
/// Sequence range management on a single node, its owner.
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
    }

    /// <summary>
    /// Range management rejects the DDL and settings that would let values escape it, and the guard checks every row
    /// of a statement but stays silent under session_replication_role replica.
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
            """, token);
        foreach (string sequence in new[] { "temp_seq", "cycling", "replicated" })
        {
            Assert.IsFalse(await node.PsqlAsync(Database, $"SELECT worf.enable_sequence_range('{sequence}', participants := '{{}}')", token),
                sequence);
        }

        _ = await node.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{}', range_size := 100)", token);
        Assert.AreEqual("0", await node.SafePsqlAsync(Database, """
            SELECT count(*) FROM pg_class c, aclexplode(c.relacl) a WHERE c.oid = 't_id_seq'::regclass AND a.privilege_type = 'UPDATE'
            """, token), "UPDATE revoked");

        foreach (string ddl in new[]
        {
            "ALTER SEQUENCE t_id_seq RESTART",
            "GRANT UPDATE ON SEQUENCE t_id_seq TO PUBLIC",
            "GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO PUBLIC",
            "ALTER TABLE t ALTER COLUMN id SET DEFAULT 0",
            "ALTER TABLE t ALTER COLUMN id DROP DEFAULT",
            "DROP TRIGGER worf_sequence_range_guard ON t",
            "ALTER TABLE t DISABLE TRIGGER worf_sequence_range_guard",
            "ALTER TABLE t DISABLE TRIGGER ALL",
            "ALTER TRIGGER worf_sequence_range_guard ON t RENAME TO other",
            "TRUNCATE t RESTART IDENTITY",
        })
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => node.SafePsqlAsync(Database, ddl, token), ddl);
            Assert.AreEqual("55000", error.SqlState, ddl);
        }

        Assert.IsFalse(await node.PsqlAsync(Database, "SELECT worf.replication_set_add_sequence('default', 't_id_seq')", token));
        Assert.IsFalse(await node.PsqlAsync(Database, "SELECT worf.synchronize_sequence('t_id_seq')", token));
        _ = await node.SafePsqlAsync(Database, "GRANT USAGE ON SEQUENCE t_id_seq TO PUBLIC; TRUNCATE t", token);

        // Every row of a statement is checked, and none under replica.
        string first = await node.SafePsqlAsync(Database, "INSERT INTO t (v) VALUES ('a') RETURNING id", token);
        string ahead = Next(Next(first));
        Assert.IsFalse(await node.PsqlAsync(Database, $"INSERT INTO t (id, v) VALUES ({first}, 'dup?'), ({ahead}, 'ahead')", token));
        _ = await node.SafePsqlAsync(Database, $"SET session_replication_role = replica; INSERT INTO t (id, v) VALUES ({ahead}, 'replica')", token);
    }

    /// <summary>
    /// A descending sequence with an increment of -5 and a cache, filling a column narrower than itself, draws from
    /// blocks below the values its table holds, within the column's range, with its cache forced to 1.
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
            "SELECT root_end FROM worf.sequence_range", token));
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
