using System.Globalization;
using Npgsql;

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

    /// <summary>
    /// Two nodes that replicate both ways insert into the same table at once with keys from the same sequence, which the
    /// first node manages: the second learns of it through the exchange, gets blocks from the first, and no key is
    /// written on both nodes. Idle, neither takes transaction IDs, across exchanges, with Snowflake IDs enabled too.
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
                """, token);
        }

        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await AssertIdleAsync([nodeA, nodeB], "managing nothing", TimeSpan.FromSeconds(2), token);
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

        // Exchanges, every half second here, write nothing when nothing changed, nor does the range worker reserve the
        // Snowflake clock, which it did every five seconds, while no session issues IDs.
        await SettleAsync([nodeA, nodeB], token);
        await AssertIdleAsync([nodeA, nodeB], "managing sequences", TimeSpan.FromSeconds(6), token);

        await Task.WhenAll(InsertAsync(nodeA, "a"), InsertAsync(nodeB, "b"));
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.AreEqual("600|600|300|300", await node.SafePsqlAsync(Database,
                "SELECT count(*), count(DISTINCT id), count(*) FILTER (WHERE v = 'a'), count(*) FILTER (WHERE v = 'b') FROM t", token),
                node.Name);
        }

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
    /// In a cascade, a node two links from the owner gets its blocks from its parent's pool block, and every node's
    /// keys stay apart.
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
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database,
            "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b,node_c}', range_size := 100)", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        await SubscribeAsync(nodeC, "c_from_b", nodeB, token);
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges", token),
                $"{node.Name} drawing from a block");
        }

        Assert.AreEqual("node_b|2", await nodeC.SafePsqlAsync(Database,
            "SELECT p.node_name, r.distance FROM worf.sequence_range r JOIN worf.node p ON p.node_id = r.parent_node", token));
        Assert.AreEqual("1", await nodeB.SafePsqlAsync(Database,
            "SELECT count(*) FROM worf.sequence_range_block WHERE kind = 'p'", token), "node_b's pool block");

        // node_b relays each end's reports to the other, and writes nothing when nothing changed.
        await SettleAsync(nodes, token);
        await AssertIdleAsync(nodes, "relaying reports", TimeSpan.FromSeconds(2), token);

        HashSet<long> keys = [];
        foreach (TestNode node in nodes)
        {
            _ = await node.SafePsqlAsync(Database, "INSERT INTO t (v) SELECT 'x' FROM generate_series(1, 50)", token);
            foreach (string id in (await node.SafePsqlAsync(Database, "SELECT id FROM t", token)).Split('\n'))
            {
                Assert.IsTrue(keys.Add(long.Parse(id, CultureInfo.InvariantCulture)), $"{node.Name}'s key {id} is another node's too");
            }
        }
    }

    /// <summary>
    /// A node whose WAL position a neighbor sees move backwards abandons its blocks for new ones. An allocator whose
    /// timeline changed fences its root until its neighbors report the blocks they hold, and takes back into its ledger
    /// the grants it lost.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task RollbacksResetBlocksAndFencedAllocatorsRecoverLostGrants()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE t (id bigserial PRIMARY KEY, v text)", token);
        }

        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_sequence_range('t_id_seq', participants := '{node_b}', range_size := 100)", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges", token),
                $"{node.Name} drawing from a block");
        }

        // node_a sees node_b's WAL position move backwards, and node_b resets.
        string before = await nodeB.SafePsqlAsync(Database, "SELECT active_lo FROM worf.sequence_ranges", token);
        _ = await nodeA.SafePsqlAsync(Database, "UPDATE worf.sequence_range_node SET wal_lsn = 'FFFFFFFF/0' WHERE node_name = 'node_b'", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            $"SELECT state = 'active' AND active_lo <> {before} FROM worf.sequence_ranges", token), "node_b reset");
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database,
            "SELECT reset_lsn IS NULL FROM worf.sequence_range_node WHERE node_name = 'node_b'", token), "node_b's reset acknowledged");

        // node_a loses its grants to node_b with an old timeline, fences its root, and recovers them from node_b's report.
        // The loss holds the ledger's lock, as a grant does, so that none lands between its statements.
        int logpos = nodeA.ReadLog().Length;
        _ = await nodeA.SafePsqlAsync(Database, """
            BEGIN;
            SELECT 1 FROM worf.sequence_range FOR UPDATE;
            INSERT INTO worf.sequence_range_free SELECT from_block, lo, hi FROM worf.sequence_range_grant WHERE grantee_name = 'node_b';
            DELETE FROM worf.sequence_range_grant WHERE grantee_name = 'node_b';
            UPDATE worf.sequence_range_block SET timeline = 99;
            COMMIT;
            """, token);
        Assert.IsTrue(await nodeA.PollLogAsync(logpos, "the allocator's fence lifted", token), "node_a's fence lifted");
        Assert.IsTrue(await nodeA.PollQueryUntilAsync(Database, "SELECT state = 'active' FROM worf.sequence_ranges", token), "node_a drawing again");

        // node_b's blocks are read first: node_a, drawing again, may grant it another between the reads.
        string[] blocks = (await nodeB.SafePsqlAsync(Database, "SELECT lo, hi FROM worf.sequence_range_block", token)).Split('\n');
        string[] grants = (await nodeA.SafePsqlAsync(Database, "SELECT lo, hi FROM worf.sequence_range_grant WHERE grantee_name = 'node_b'", token)).Split('\n');
        foreach (string block in blocks)
        {
            Assert.Contains(block, grants, "node_b's block back in node_a's ledger");
        }

        Assert.AreEqual("0", await nodeA.SafePsqlAsync(Database, """
            SELECT count(*) FROM worf.sequence_range_free f JOIN worf.sequence_range_grant g ON f.lo <= g.hi AND g.lo <= f.hi
            """, token), "granted values left in the free spans");
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
    /// on the owner and on a node that asks the owner, which refuses keys that may be in use.
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
    /// node's IDs only grow, across a restart too.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SnowflakeIdsAreUniqueAndOnlyGrow()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        TestNode[] nodes = [nodeA, nodeB];
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.enable_snowflake('{node_b}')", token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);

        HashSet<long> ids = [];
        Dictionary<TestNode, string> numbers = [];
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, "SELECT worf.snowflake_node() IS NOT NULL", token), $"{node.Name}'s node number");

            numbers[node] = await node.SafePsqlAsync(Database, "SELECT worf.snowflake_node()", token);
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
    }

    /// <summary>
    /// Waits until each node holds its next blocks, with no request open and no grant unacknowledged, and the neighbors
    /// have heard so.
    /// </summary>
    private static async Task SettleAsync(TestNode[] nodes, CancellationToken token)
    {
        foreach (TestNode node in nodes)
        {
            Assert.IsTrue(await node.PollQueryUntilAsync(Database, """
                SELECT bool_and(next_lo IS NOT NULL)
                    AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_request)
                    AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_grant WHERE state = 'sent')
                FROM worf.sequence_ranges
                """, token), $"{node.Name} holding its next blocks");
        }

        await Task.Delay(1000, token);
    }

    /// <summary>
    /// Asserts that the nodes take no transaction IDs over a while, as their workers tick and exchange.
    /// </summary>
    private static async Task AssertIdleAsync(TestNode[] nodes, string phase, TimeSpan span, CancellationToken token)
    {
        const string NextXid = "SELECT pg_snapshot_xmax(pg_current_snapshot())";
        string[] before = await Task.WhenAll(nodes.Select(node => node.SafePsqlAsync(Database, NextXid, token)));
        await Task.Delay(span, token);
        for (int i = 0; i < nodes.Length; i++)
        {
            Assert.AreEqual(before[i], await nodes[i].SafePsqlAsync(Database, NextXid, token),
                $"{nodes[i].Name} taking no transaction IDs while idle, {phase}");
        }
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
