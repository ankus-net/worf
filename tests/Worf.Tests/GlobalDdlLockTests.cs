using System.Diagnostics;
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

    // How many databases shared memory notes as having read their kept fences back, WorfFenceTable.MaxDatabases.
    private const int WorfFenceTableDatabases = 64;

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// A column type change replicates under the lock and lets the other node's writes through once it applies. A fence
    /// holds writes back until the session that asked for it ends, or, once kept, until it's released, and covers a
    /// partitioned table's partitions, which writes through the table reach. DDL fails when a node's write under way
    /// outlasts worf.ddl_lock_timeout, which bounds every wait together. Only the session that asked for a fence keeps
    /// it, a TRUNCATE that may not run doesn't queue for its lock, a unique index built concurrently takes the lock,
    /// and lets it go as soon as it fails, a concurrent detach leaves no fence and, cancelled and finished later,
    /// replicates, and the provider role's idle session timeout doesn't end a fence.
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
                CREATE TABLE p (id integer PRIMARY KEY, v integer) PARTITION BY RANGE (id);
                CREATE TABLE p1 PARTITION OF p FOR VALUES FROM (0) TO (100);
                CREATE TABLE p2 PARTITION OF p FOR VALUES FROM (100) TO (200);
                CREATE TABLE p3 PARTITION OF p FOR VALUES FROM (200) TO (300);
                CREATE TABLE dupe (id integer PRIMARY KEY, v integer);
                SELECT * FROM worf.replication_set_add_table('default', 't');
                SELECT * FROM worf.replication_set_add_table('default', 'p1');
                SELECT * FROM worf.replication_set_add_table('default', 'p2');
                SELECT * FROM worf.replication_set_add_table('default', 'p3');
                SELECT * FROM worf.replication_set_add_table('default', 'dupe');
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

        // A fence holds back writes while the session that asked for it lasts, and on a partitioned table, writes
        // that reach its partitions, which aren't named.
        await using (NpgsqlConnection fencing = await nodeB.OpenAsync(Database, token))
        {
            await using NpgsqlCommand fence = new(
                "SELECT worf.fence_writes(42, (SELECT node_id FROM worf.node WHERE node_name = 'node_a'), ARRAY['public.t', 'public.p'], 1000)", fencing);
            _ = await fence.ExecuteScalarAsync(token);
            foreach (string write in new[] { "INSERT INTO t VALUES (2, 2)", "INSERT INTO p VALUES (2, 2)", "COPY p FROM STDIN" })
            {
                PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database, write, token), write);
                Assert.AreEqual("55P03", error.SqlState, write);
            }

            // Only the session that asked for a fence keeps it.
            _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.keep_fence(42, (SELECT node_id FROM worf.node WHERE node_name = 'node_a'), 0)", token);
            Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM worf.fence", token));
        }

        Assert.IsTrue(await PollSucceedsAsync(nodeB, "INSERT INTO p VALUES (2, 2)", token), "the fence went with its session");
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

        // The timeout bounds the waits for every write under way together: one that finishes after a while leaves the
        // next only the rest.
        await using (NpgsqlConnection first = await nodeB.OpenAsync(Database, token))
        await using (NpgsqlConnection second = await nodeB.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction finishing = await first.BeginTransactionAsync(token);
            await using NpgsqlTransaction lasting = await second.BeginTransactionAsync(token);
            await using (NpgsqlCommand insert = new("INSERT INTO p VALUES (10, 10)", first, finishing))
            {
                _ = await insert.ExecuteNonQueryAsync(token);
            }

            await using (NpgsqlCommand insert = new("INSERT INTO p VALUES (110, 110)", second, lasting))
            {
                _ = await insert.ExecuteNonQueryAsync(token);
            }

            Task finish = Task.Delay(TimeSpan.FromMilliseconds(1500), token).ContinueWith(_ => finishing.CommitAsync(token), token, TaskContinuationOptions.None, TaskScheduler.Default).Unwrap();
            Stopwatch elapsed = Stopwatch.StartNew();
            PostgresException timeout = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
                "SELECT worf.fence_writes(44, (SELECT node_id FROM worf.node WHERE node_name = 'node_a'), ARRAY['public.p1', 'public.p2'], 2000)", token));
            elapsed.Stop();
            await finish;
            Assert.AreEqual("55P03", timeout.SqlState);
            Assert.IsLessThan(3.2, elapsed.Elapsed.TotalSeconds, "the waits share the timeout");
            await lasting.RollbackAsync(token);
        }

        // A user who may not truncate a table fails at once, rather than queueing for its lock behind a reader.
        _ = await nodeB.SafePsqlAsync(Database, "CREATE ROLE nobody", token);
        await using (NpgsqlConnection reading = await nodeB.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await reading.BeginTransactionAsync(token);
            await using (NpgsqlCommand select = new("SELECT count(*) FROM t", reading, transaction))
            {
                _ = await select.ExecuteScalarAsync(token);
            }

            PostgresException denied = await Assert.ThrowsExactlyAsync<PostgresException>(() => nodeB.SafePsqlAsync(Database,
                "SET ROLE nobody; SET lock_timeout = '5s'; TRUNCATE t", token));
            Assert.AreEqual("42501", denied.SqlState);
        }

        // A unique index built concurrently takes the lock across the transactions it commits, and the fence goes once
        // the index has applied.
        _ = await nodeA.SafePsqlAsync(Database, "CREATE UNIQUE INDEX CONCURRENTLY t_v_key ON t (v)", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("t_v_key|0", await nodeB.SafePsqlAsync(Database,
            "SELECT (SELECT indexname FROM pg_indexes WHERE indexname = 't_v_key'), (SELECT count(*) FROM worf.fence)", token));
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (6, 6)", token);

        // Detaching a partition applies in any order, even concurrently, and leaves no fence.
        _ = await nodeA.SafePsqlAsync(Database, "ALTER TABLE p DETACH PARTITION p2 CONCURRENTLY", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("0|0", await nodeB.SafePsqlAsync(Database,
            "SELECT (SELECT count(*) FROM pg_inherits WHERE inhrelid = 'p2'::regclass), (SELECT count(*) FROM worf.fence)", token));
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO p1 VALUES (20, 20); INSERT INTO p2 VALUES (120, 120)", token);

        // A concurrent detach cancelled after its first commit, and finished later, detaches the partition on node_b
        // too, where a later attach then applies.
        await using (NpgsqlConnection reading = await nodeA.OpenAsync(Database, token))
        await using (NpgsqlConnection detaching = await nodeA.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await reading.BeginTransactionAsync(token);
            await using (NpgsqlCommand select = new("SELECT count(*) FROM p", reading, transaction))
            {
                _ = await select.ExecuteScalarAsync(token);
            }

            await using (NpgsqlCommand timeout = new("SET statement_timeout = '1s'", detaching))
            {
                _ = await timeout.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlCommand detach = new("ALTER TABLE p DETACH PARTITION p3 CONCURRENTLY", detaching);
            PostgresException cancelled = await Assert.ThrowsExactlyAsync<PostgresException>(() => detach.ExecuteNonQueryAsync(token));
            Assert.AreEqual("57014", cancelled.SqlState);
        }

        _ = await nodeA.SafePsqlAsync(Database, "ALTER TABLE p DETACH PARTITION p3 FINALIZE", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("0", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM pg_inherits WHERE inhrelid = 'p3'::regclass", token));
        _ = await nodeA.SafePsqlAsync(Database, "ALTER TABLE p ATTACH PARTITION p3 FOR VALUES FROM (200) TO (300)", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("1", await nodeB.SafePsqlAsync(Database, "SELECT count(*) FROM pg_inherits WHERE inhrelid = 'p3'::regclass", token));

        // A fence's session that idles through the provider role's idle_session_timeout keeps holding back writes.
        _ = await nodeB.SafePsqlAsync(Database, $"ALTER ROLE {SuperUser} SET idle_session_timeout = '1s'", token);
        await using (NpgsqlConnection migrating = await nodeA.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await migrating.BeginTransactionAsync(token);
            await using (NpgsqlCommand change = new("ALTER TABLE t ALTER COLUMN v TYPE numeric", migrating, transaction))
            {
                _ = await change.ExecuteNonQueryAsync(token);
            }

            await Task.Delay(TimeSpan.FromSeconds(2), token);
            Assert.IsFalse(await nodeB.PsqlAsync(Database, "INSERT INTO t VALUES (7, 7)", token), "a write while the DDL's transaction idles");
            await transaction.CommitAsync(token);
        }

        _ = await nodeB.SafePsqlAsync(Database, $"ALTER ROLE {SuperUser} RESET idle_session_timeout", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (7, 7)", token);

        // A unique index built concurrently that fails after the transactions it committed, in its build or once it
        // has built, lets its fence go as it fails, while its session lasts, and the session's next DDL locks node_b
        // afresh. It leaves an invalid index, which stays on this node, as does DDL on it, and which REINDEX can't make
        // valid here alone.
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO dupe VALUES (1, 1), (2, 1)", token);
        await using (NpgsqlConnection building = await nodeA.OpenAsync(Database, token))
        {
            await using (NpgsqlCommand build = new("CREATE UNIQUE INDEX CONCURRENTLY dupe_v ON dupe (v)", building))
            {
                PostgresException duplicate = await Assert.ThrowsExactlyAsync<PostgresException>(() => build.ExecuteNonQueryAsync(token));
                Assert.AreEqual("23505", duplicate.SqlState);
            }

            await WritesGoSoonAsync(nodeB, "UPDATE dupe SET v = v WHERE id = 0", "the failed build's fence went", token);
            await using (NpgsqlConnection locking = await nodeA.OpenAsync(Database, token))
            {
                await using NpgsqlTransaction transaction = await locking.BeginTransactionAsync(token);
                await using (NpgsqlCommand lockQueue = new("LOCK TABLE worf.queue IN ACCESS EXCLUSIVE MODE", locking, transaction))
                {
                    _ = await lockQueue.ExecuteNonQueryAsync(token);
                }

                await using (NpgsqlCommand timeout = new("SET lock_timeout = '1s'", building))
                {
                    _ = await timeout.ExecuteNonQueryAsync(token);
                }

                await using NpgsqlCommand build = new("CREATE UNIQUE INDEX CONCURRENTLY dupe_id ON dupe (id)", building);
                PostgresException unqueued = await Assert.ThrowsExactlyAsync<PostgresException>(() => build.ExecuteNonQueryAsync(token));
                Assert.AreEqual("55P03", unqueued.SqlState);
                await WritesGoSoonAsync(nodeB, "UPDATE dupe SET v = v WHERE id = 0", "the build's fence went as its DDL couldn't be queued", token);
            }

            foreach (string sql in new[] { "RESET lock_timeout", "COMMENT ON INDEX dupe_v IS 'here'", "ALTER INDEX dupe_id DEPENDS ON EXTENSION plpgsql" })
            {
                await using NpgsqlCommand command = new(sql, building);
                _ = await command.ExecuteNonQueryAsync(token);
            }

            await using (NpgsqlCommand reindex = new("REINDEX INDEX dupe_id", building))
            {
                PostgresException invalid = await Assert.ThrowsExactlyAsync<PostgresException>(() => reindex.ExecuteNonQueryAsync(token));
                Assert.AreEqual("55000", invalid.SqlState);
            }

            await using NpgsqlCommand change = new("DROP INDEX dupe_v, dupe_id; ALTER TABLE dupe ALTER COLUMN v TYPE bigint", building);
            _ = await change.ExecuteNonQueryAsync(token);
        }

        Assert.AreEqual("0", await nodeA.SafePsqlAsync(Database, "SELECT count(*) FROM worf.queue WHERE message->>'command' ~ 'dupe_(v|id)'", token));
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO dupe VALUES (3, 3)", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("3|bigint", await nodeB.SafePsqlAsync(Database, "SELECT count(*), pg_typeof(min(v))::text FROM dupe", token));
    }

    /// <summary>
    /// The DDL waits until this node has applied the other node's earlier writes, and the other node's writes fail
    /// while it's in flight. DDL in a transaction block, run by a table owner who logs in directly, holds the lock
    /// from its first statement that locks out writes, through one connection to each node; a savepoint rolled back
    /// lets its fences go while the others hold; DDL after a lock that the transaction took itself fails at once; a
    /// schema's owner takes the lock to drop a table in it; and a fence holds back the other node's DDL but not
    /// EXPLAIN, across a restart, until released, after which a node that is also a template replicates. A unique index
    /// built concurrently that loses its hold before its first commit fails. The lock covers the tables of renamed,
    /// dropped and moved tables and schemas, of a domain, an enum, a composite type or a range, however deep, a
    /// partition attached, and the table a new foreign key references.
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
                CREATE ROLE owner LOGIN;
                CREATE ROLE schema_owner;
                CREATE TABLE t (id integer PRIMARY KEY, v integer);
                ALTER TABLE t OWNER TO owner;
                CREATE SCHEMA s AUTHORIZATION schema_owner;
                CREATE TABLE s.st (id integer PRIMARY KEY);
                CREATE TYPE e AS ENUM ('a', 'b');
                CREATE DOMAIN d AS e;
                CREATE TYPE c AS (x d);
                CREATE TABLE td (id integer PRIMARY KEY, v d);
                CREATE TABLE tc (id integer PRIMARY KEY, v c[]);
                CREATE TABLE inner_t (id integer PRIMARY KEY, v e);
                CREATE TABLE t_row (id integer PRIMARY KEY, x inner_t);
                CREATE DOMAIN positive AS integer;
                CREATE TABLE tp (id integer PRIMARY KEY, v positive);
                CREATE TABLE parent (id integer PRIMARY KEY);
                CREATE TABLE child (id integer PRIMARY KEY, parent_id integer);
                CREATE TABLE q (id integer PRIMARY KEY) PARTITION BY RANGE (id);
                CREATE TABLE q1 (id integer PRIMARY KEY);
                SELECT worf.replication_set_add_table('default', oid::regclass)
                FROM pg_class WHERE relname IN ('t', 'st', 'td', 'tc', 't_row', 'tp', 'parent', 'child', 'q1');
                """, token);
        }

        await SubscribeAsync(nodeA, "a_from_b", nodeB, token, "3 seconds");
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);

        // node_a applies node_b's changes three seconds late, so its DDL waits for node_b's earlier write, which it
        // has applied once the DDL returns, and node_b's writes fail meanwhile.
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (1, 1)", token);
        Task<string> ddl = nodeA.SafePsqlAsync(Database, "ALTER TABLE t ALTER COLUMN v TYPE bigint", token, "owner");
        Assert.IsTrue(await PollFailsAsync(nodeB, "UPDATE t SET v = v WHERE id = 1", token), "node_b's writes held back");
        _ = await ddl;
        Assert.AreEqual("1", await nodeA.SafePsqlAsync(Database, "SELECT count(*) FROM t WHERE id = 1", token), "node_b's earlier write");
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (2, 20000000000)", token);
        Assert.AreEqual("1|bigint", await nodeA.SafePsqlAsync(Database, """
            SELECT count(*), format_type(atttypid, atttypmod) FROM t, pg_attribute
            WHERE id = 1 AND attrelid = 't'::regclass AND attname = 'v' GROUP BY 2
            """, token));

        // A unique index built concurrently that loses its hold on node_b's writes before its first commit, as it
        // waits for node_b's earlier write, fails, and builds nothing.
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (100, 100)", token);
        Task<string> build = nodeA.SafePsqlAsync(Database, "CREATE UNIQUE INDEX CONCURRENTLY t_id_key ON t (id)", token);
        Assert.IsTrue(await PollFailsAsync(nodeB, "UPDATE t SET v = v WHERE id = 1", token), "node_b's writes held back");
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database,
            "SELECT count(*) FILTER (WHERE pg_terminate_backend(pid, 10000)) > 0 FROM pg_stat_activity WHERE application_name = 'worf_ddl_lock'", token));
        PostgresException lost = await Assert.ThrowsExactlyAsync<PostgresException>(() => build);
        Assert.AreEqual("55P03", lost.SqlState);
        Assert.Contains("lost the hold", lost.MessageText);
        Assert.AreEqual("0", await nodeA.SafePsqlAsync(Database, "SELECT count(*) FROM pg_class WHERE relname = 't_id_key'", token));

        // In a transaction block, the first DDL that locks out writes takes the lock, as dropping an index does, so
        // later DDL there needn't wait for node_b's writes. All of it holds node_b back through one connection.
        _ = await nodeA.SafePsqlAsync(Database, "CREATE INDEX t_v ON t (v)", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        await using (NpgsqlConnection migrating = await nodeA.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await migrating.BeginTransactionAsync(token);
            await using (NpgsqlCommand drop = new("DROP INDEX t_v", migrating, transaction))
            {
                _ = await drop.ExecuteNonQueryAsync(token);
            }

            PostgresException held = await Assert.ThrowsExactlyAsync<PostgresException>(
                () => nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (3, 3)", token));
            Assert.AreEqual("55P03", held.SqlState);
            await using (NpgsqlCommand change = new("""
                SET worf.ddl_lock_timeout = '10s';
                ALTER TABLE t ADD COLUMN c integer;
                CREATE TRIGGER t_nothing BEFORE UPDATE ON td FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger();
                ALTER TABLE t ALTER COLUMN v TYPE numeric;
                ALTER TABLE td ADD COLUMN w integer NOT NULL;
                """, migrating, transaction))
            {
                _ = await change.ExecuteNonQueryAsync(token);
            }

            Assert.AreEqual("1", await nodeB.SafePsqlAsync(Database,
                "SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'worf_ddl_lock%'", token), "one connection holds node_b back");
            await transaction.CommitAsync(token);
        }

        // A savepoint rolled back lets its fences go, while the transaction's others hold, and one released keeps them.
        await using (NpgsqlConnection migrating = await nodeA.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await migrating.BeginTransactionAsync(token);
            await using (NpgsqlCommand change = new("""
                ALTER TABLE child ALTER COLUMN parent_id TYPE bigint;
                SAVEPOINT s;
                ALTER TABLE t ALTER COLUMN v TYPE text;
                ROLLBACK TO SAVEPOINT s;
                """, migrating, transaction))
            {
                _ = await change.ExecuteNonQueryAsync(token);
            }

            Assert.IsTrue(await PollSucceedsAsync(nodeB, "INSERT INTO t VALUES (4, 4)", token), "the savepoint's fence went");
            Assert.IsFalse(await nodeB.PsqlAsync(Database, "INSERT INTO child VALUES (4, 4)", token), "the transaction's other fence holds");
            await transaction.CommitAsync(token);
        }

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

        // DDL after a lock the transaction took itself, which node_b's write then waits for here, fails at once.
        await using (NpgsqlConnection locking = await nodeA.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await locking.BeginTransactionAsync(token);
            await using (NpgsqlCommand lockTable = new("LOCK TABLE t IN SHARE MODE", locking, transaction))
            {
                _ = await lockTable.ExecuteNonQueryAsync(token);
            }

            _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (5, 5)", token);
            Stopwatch elapsed = Stopwatch.StartNew();
            await using NpgsqlCommand change = new("ALTER TABLE t ALTER COLUMN c TYPE numeric", locking, transaction);
            PostgresException blocked = await Assert.ThrowsExactlyAsync<PostgresException>(() => change.ExecuteNonQueryAsync(token));
            Assert.AreEqual("55P03", blocked.SqlState);
            Assert.Contains("holds a lock", blocked.MessageText);
            Assert.IsLessThan(30, elapsed.Elapsed.TotalSeconds, "failed before worf.ddl_lock_timeout");
        }

        // A schema's owner takes the lock to drop a table in it that they don't own.
        _ = await nodeA.SafePsqlAsync(Database, "SET ROLE schema_owner; DROP TABLE s.st CASCADE", token);
        Assert.AreEqual("1", await nodeA.SafePsqlAsync(Database,
            "SELECT count(*) FROM worf.queue WHERE message->>'command' LIKE 'DROP TABLE s.st%' AND message->>'fence' IS NOT NULL", token));

        // The lock covers the tables each kind of DDL changes.
        await HeldBackAsync(nodeA, nodeB, "ALTER TABLE t RENAME COLUMN v TO w", ["INSERT INTO t VALUES (6, 6)"], token);
        await HeldBackAsync(nodeA, nodeB, "DROP TABLE child CASCADE", ["INSERT INTO child VALUES (6, 6)"], token);
        await HeldBackAsync(nodeA, nodeB, "ALTER SCHEMA public RENAME TO main", ["INSERT INTO t VALUES (6, 6)", "INSERT INTO q1 VALUES (6)"], token);
        await HeldBackAsync(nodeA, nodeB, "ALTER TABLE child SET SCHEMA s", ["INSERT INTO child VALUES (6, 6)"], token);
        await HeldBackAsync(nodeA, nodeB, "ALTER DOMAIN positive ADD CHECK (VALUE > 0)", ["INSERT INTO tp VALUES (6, 6)"], token);
        await HeldBackAsync(nodeA, nodeB, "ALTER TYPE e RENAME VALUE 'a' TO 'z'",
            ["INSERT INTO td VALUES (6, 'a', 0)", "INSERT INTO tc VALUES (6, ARRAY[ROW('a')::c])", "INSERT INTO t_row VALUES (6, ROW(1, 'a'))"], token);
        await HeldBackAsync(nodeA, nodeB, "ALTER TYPE c ADD ATTRIBUTE y integer CASCADE", ["INSERT INTO tc VALUES (6, NULL)"], token);
        await HeldBackAsync(nodeA, nodeB, "ALTER TABLE q ATTACH PARTITION q1 FOR VALUES FROM (0) TO (100)", ["INSERT INTO q1 VALUES (6)"], token);
        await HeldBackAsync(nodeA, nodeB, "ALTER TABLE child ADD FOREIGN KEY (parent_id) REFERENCES parent",
            ["INSERT INTO child VALUES (6, 6)", "DELETE FROM parent"], token);

        // A kept fence holds back node_b's DDL as well as its writes, but not EXPLAIN, and lasts across a restart.
        _ = await nodeB.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await nodeB.SafePsqlAsync(Database, """
            SELECT worf.fence_writes(44, (SELECT node_id FROM worf.node WHERE node_name = 'node_a'), ARRAY['public.t'], 1000);
            SELECT worf.keep_fence(44, (SELECT node_id FROM worf.node WHERE node_name = 'node_a'), 0);
            """, token);
        PostgresException ddlHeld = await Assert.ThrowsExactlyAsync<PostgresException>(
            () => nodeB.SafePsqlAsync(Database, "ALTER TABLE t ALTER COLUMN v TYPE text", token));
        Assert.AreEqual("55P03", ddlHeld.SqlState);
        _ = await nodeB.SafePsqlAsync(Database, "EXPLAIN INSERT INTO t VALUES (7, 7)", token);
        _ = await nodeB.SafePsqlAsync(Database, "ALTER DATABASE postgres IS_TEMPLATE true", token);
        await nodeB.StopAsync("fast", token);
        await nodeB.StartServerAsync(token);
        Assert.IsFalse(await nodeB.PsqlAsync(Database, "INSERT INTO t VALUES (7, 7)", token), "a write through a kept fence after a restart");
        Assert.AreEqual("1", await nodeB.SafePsqlAsync(Database, "SELECT worf.release_fence(44)", token));
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t VALUES (7, 7)", token);

        // A node that is also a template keeps replicating after the restart.
        _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO t VALUES (8, 8)", token);
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT FROM t WHERE id = 8)", token), "node_a's write on node_b, a template");
    }

    /// <summary>
    /// A node that doesn't receive the DDL holds nothing back and isn't waited for, and the DDL's message carries the
    /// position of each node that does, which each node applying the DDL waits for. A unique index built concurrently
    /// on a node with no subscriptions needs no lock.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task OnlyNodesTheDdlReachesAreLocked()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode nodeA = await StartNodeAsync("node_a", token);
        await using TestNode nodeB = await StartNodeAsync("node_b", token);
        await using TestNode nodeC = await StartNodeAsync("node_c", token);
        // node_c, which doesn't receive node_a's DDL at first, has the column type that the DDL gives the others.
        foreach (TestNode node in new[] { nodeA, nodeB, nodeC })
        {
            _ = await node.SafePsqlAsync(Database, $"""
                CREATE TABLE t (id integer PRIMARY KEY, v {(node == nodeC ? "bigint" : "integer")});
                SELECT * FROM worf.replication_set_add_table('default', 't');
                """, token);
        }

        // node_b notes whether t had node_a's new column when each row arrived.
        _ = await nodeB.SafePsqlAsync(Database, """
            CREATE TABLE seen (id integer PRIMARY KEY, had_c boolean);
            CREATE FUNCTION note_seen() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO seen VALUES (NEW.id, EXISTS (SELECT FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c'));
                RETURN NEW;
            END $$;
            CREATE TRIGGER note_seen AFTER INSERT ON t FOR EACH ROW EXECUTE FUNCTION note_seen();
            ALTER TABLE t ENABLE ALWAYS TRIGGER note_seen;
            """, token);

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
        _ = await nodeC.SafePsqlAsync(Database, "CREATE UNIQUE INDEX CONCURRENTLY t_v_key ON t (v)", token);

        // Once node_c receives node_a's DDL too, and node_b applies node_c's changes three seconds late, node_b applies
        // node_a's DDL only after node_c's write from before it.
        await SubscribeAsync(nodeC, "c_from_a", nodeA, token);
        await SubscribeAsync(nodeB, "b_from_c", nodeC, token, "3 seconds");
        _ = await nodeC.SafePsqlAsync(Database, "INSERT INTO t VALUES (10, 10)", token);
        _ = await nodeA.SafePsqlAsync(Database, "BEGIN; ALTER TABLE t ADD COLUMN c integer; COMMIT", token);
        Assert.AreEqual("2", await nodeA.SafePsqlAsync(Database, """
            SELECT count(*) FROM worf.queue, json_object_keys(message->'positions')
            WHERE message_type = 'D' AND message->>'command' LIKE 'ALTER TABLE t ADD COLUMN c%'
            """, token), "positions for node_b and node_c");
        Assert.IsTrue(await nodeB.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'c') AND EXISTS (SELECT FROM seen WHERE id = 10)",
            token), "node_a's DDL and node_c's write on node_b");
        Assert.AreEqual("f", await nodeB.SafePsqlAsync(Database, "SELECT had_c FROM seen WHERE id = 10", token), "node_c's earlier write applied first");
    }

    /// <summary>
    /// Every database worf is in reads its kept fences back before its first write, however many databases there are,
    /// although shared memory notes only some of them as read, and copying a template with worf works.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task WritesWorkInAnyNumberOfDatabases()
    {
        CancellationToken token = TestContext.CancellationToken;

        // Each database is copied from template1 between two checkpoints, which needn't reach the disk. template1, which
        // isn't a node, keeps no manager that would keep it busy, as one starts there with the server.
        await using TestNode node = await StartNodeAsync("node", token, "fsync = off");
        _ = await node.SafePsqlAsync("template1", "CREATE EXTENSION worf; CREATE TABLE w (id integer)", token);
        await node.StopAsync("fast", token);
        await node.StartServerAsync(token);
        await using NpgsqlConnection admin = await node.OpenAsync(Database, token);
        for (int i = 0; i < WorfFenceTableDatabases + 6; i++)
        {
            await using (NpgsqlCommand create = new($"CREATE DATABASE many{i} STRATEGY FILE_COPY", admin))
            {
                _ = await create.ExecuteNonQueryAsync(token);
            }

            await using NpgsqlConnection connection = await node.OpenAsync($"many{i}", token);
            await using NpgsqlCommand insert = new("INSERT INTO w VALUES (1)", connection);
            _ = await insert.ExecuteNonQueryAsync(token);
        }

        // worf leaves a statement's transaction without an ID, as DROP INDEX CONCURRENTLY needs.
        _ = await node.SafePsqlAsync("many0", "CREATE INDEX w_id ON w (id)", token);
        _ = await node.SafePsqlAsync("many0", "DROP INDEX CONCURRENTLY w_id", token);
    }

    /// <summary>
    /// A kept fence whose DDL a node skips with its transaction, or whose prepared transaction rolls back, goes once
    /// the node has applied past it or learns that it aborted, while its origin keeps writing. A prepared
    /// transaction's DDL that commits applies and lets its fence go. A unique index built concurrently keeps its fence
    /// while it builds, after the connection that asked for it is gone.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task KeptFencesGoOnceTheirDdlIsSkippedOrAborted()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode nodeA = await StartNodeAsync("node_a", token, "max_prepared_transactions = 2");
        await using TestNode nodeB = await StartNodeAsync("node_b", token, "worf.exception_behaviour = transdiscard");
        foreach (TestNode node in new[] { nodeA, nodeB })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id integer PRIMARY KEY, v integer);
                CREATE TABLE t2 (id integer PRIMARY KEY, v integer);
                CREATE TABLE t3 (id integer PRIMARY KEY, v integer);
                CREATE TABLE busy (id serial PRIMARY KEY);
                CREATE TABLE cic (id integer PRIMARY KEY, v integer);
                INSERT INTO cic VALUES (1, 1);
                SELECT worf.replication_set_add_table('default', oid::regclass)
                FROM pg_class WHERE relname IN ('t', 't2', 't3', 'busy', 'cic');
                """, token);
        }

        // node_b can't change t's column, which its view uses, so it skips the DDL with its transaction.
        _ = await nodeB.SafePsqlAsync(Database, "CREATE VIEW tv AS SELECT v FROM t", token);
        await SubscribeAsync(nodeA, "a_from_b", nodeB, token);
        await SubscribeAsync(nodeB, "b_from_a", nodeA, token);

        // A unique index built concurrently waits for a write under way on node_a, all the while node_b lets go of
        // the other fences below, and its fence holds until the transaction that queues it commits.
        await using NpgsqlConnection writer = await nodeA.OpenAsync(Database, token);
        await using NpgsqlTransaction pending = await writer.BeginTransactionAsync(token);
        await using (NpgsqlCommand insert = new("INSERT INTO cic VALUES (100, 100)", writer, pending))
        {
            _ = await insert.ExecuteNonQueryAsync(token);
        }

        Task<string> building = nodeA.SafePsqlAsync(Database, "CREATE UNIQUE INDEX CONCURRENTLY cic_v ON cic (v)", token);
        Assert.IsTrue(await PollFailsAsync(nodeB, "UPDATE cic SET v = v WHERE id = 0", token), "the build's fence");

        _ = await nodeA.SafePsqlAsync(Database, "ALTER TABLE t ALTER COLUMN v TYPE bigint", token);
        _ = await nodeA.SafePsqlAsync(Database, "BEGIN; ALTER TABLE t2 ALTER COLUMN v TYPE bigint; PREPARE TRANSACTION 'aborted'", token);
        Assert.IsFalse(await nodeB.PsqlAsync(Database, "INSERT INTO t2 VALUES (1, 1)", token), "a write through a prepared transaction's fence");
        _ = await nodeA.SafePsqlAsync(Database, "ROLLBACK PREPARED 'aborted'", token);
        _ = await nodeA.SafePsqlAsync(Database, "BEGIN; ALTER TABLE t3 ALTER COLUMN v TYPE bigint; PREPARE TRANSACTION 'committed'", token);
        _ = await nodeA.SafePsqlAsync(Database, "COMMIT PREPARED 'committed'", token);
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO t3 VALUES (1, 10000000000)", token);
        Assert.AreEqual("integer", await nodeB.SafePsqlAsync(Database,
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 't'::regclass AND attname = 'v'", token), "node_b skipped the DDL");

        // node_a keeps writing meanwhile, so its WAL keeps moving past where node_b has applied.
        using CancellationTokenSource stop = CancellationTokenSource.CreateLinkedTokenSource(token);
        Task writing = Task.Run(async () =>
        {
            while (!stop.Token.IsCancellationRequested)
            {
                _ = await nodeA.SafePsqlAsync(Database, "INSERT INTO busy DEFAULT VALUES", token);
                await Task.Delay(TimeSpan.FromMilliseconds(20), token);
            }
        }, token);
        try
        {
            Assert.IsTrue(await PollSucceedsAsync(nodeB, "INSERT INTO t VALUES (1, 1)", token), "the skipped DDL's fence went");
            Assert.IsTrue(await PollSucceedsAsync(nodeB, "INSERT INTO t2 VALUES (1, 1)", token), "the rolled back DDL's fence went");
        }
        finally
        {
            await stop.CancelAsync();
            await writing;
        }

        // node_b keeps the build's fence while the build goes on, long past when it asks node_a about kept fences, and
        // once the connection that asked for it is gone.
        Assert.AreEqual("t", await nodeB.SafePsqlAsync(Database,
            "SELECT count(*) FILTER (WHERE pg_terminate_backend(pid, 10000)) > 0 FROM pg_stat_activity WHERE application_name = 'worf_ddl_lock'", token));
        PostgresException held = await Assert.ThrowsExactlyAsync<PostgresException>(
            () => nodeB.SafePsqlAsync(Database, "INSERT INTO cic VALUES (2, 1)", token), "a duplicate while the build waits");
        Assert.AreEqual("55P03", held.SqlState);
        await pending.CommitAsync(token);
        _ = await building;
        _ = await nodeA.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("cic_v|0", await nodeB.SafePsqlAsync(Database,
            "SELECT (SELECT indexname FROM pg_indexes WHERE indexname = 'cic_v'), (SELECT count(*) FROM worf.fence)", token));
        _ = await nodeB.SafePsqlAsync(Database, "INSERT INTO cic VALUES (2, 2)", token);
    }

    /// <summary>
    /// Runs DDL in a transaction on one node, checks that each write on the other node fails on its fence while the
    /// transaction lasts, and rolls it back.
    /// </summary>
    private static async Task HeldBackAsync(TestNode ddlNode, TestNode other, string ddl, string[] writes, CancellationToken token)
    {
        await using NpgsqlConnection connection = await ddlNode.OpenAsync(Database, token);
        await using NpgsqlTransaction transaction = await connection.BeginTransactionAsync(token);
        await using (NpgsqlCommand command = new(ddl, connection, transaction))
        {
            _ = await command.ExecuteNonQueryAsync(token);
        }

        foreach (string write in writes)
        {
            PostgresException error = await Assert.ThrowsExactlyAsync<PostgresException>(() => other.SafePsqlAsync(Database, write, token), $"{ddl}: {write}");
            Assert.AreEqual("55P03", error.SqlState, $"{ddl}: {write}");
        }

        await transaction.RollbackAsync(token);
    }

    /// <summary>
    /// Checks that a write gets through a fence soon, before the node would ask the fence's origin about it.
    /// </summary>
    private static async Task WritesGoSoonAsync(TestNode node, string sql, string message, CancellationToken token)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        Assert.IsTrue(await PollSucceedsAsync(node, sql, token), message);
        Assert.IsLessThan(10, elapsed.Elapsed.TotalSeconds, message);
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

    /// <summary>
    /// Polls until a statement succeeds, as when a fence goes.
    /// </summary>
    private static async Task<bool> PollSucceedsAsync(TestNode node, string sql, CancellationToken token)
    {
        Stopwatch elapsed = Stopwatch.StartNew();
        while (elapsed.Elapsed < TestNode.PollTimeout)
        {
            if (await node.PsqlAsync(Database, sql, token))
            {
                return true;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
        }

        return false;
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token, string? setting = null)
    {
        List<string> configuration =
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
            "worf.conflict_resolution = last_update_wins",
            "worf.enable_ddl_replication = on",
        ];
        if (setting is not null)
        {
            configuration.Add(setting);
        }

        TestNode node = await TestNode.StartAsync(name, configuration, token);
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
