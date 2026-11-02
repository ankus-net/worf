using Npgsql;

using System.Diagnostics;
using System.Text.RegularExpressions;

namespace Worf.Tests;

/// <summary>
/// <c>worf.exception_behaviour</c> keeps a remote transaction that fails from stopping replication, and records it.
/// </summary>
[TestClass]
public sealed partial class ExceptionBehaviourTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    // The sync worker's connection that reads the provider's managed sequences waits for a lock on them.
    private const string SyncWorkerHeldSql = """
        SELECT EXISTS (
            SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
            WHERE NOT l.granted AND l.relation = 'worf.sequence_range'::regclass AND a.application_name = 'sub_ranges')
        """;

    // A table's copy creates its slot, which waits for the provider's open transactions.
    private const string CopySlotSql = """
        SELECT EXISTS (
            SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
            WHERE NOT l.granted AND l.locktype = 'transactionid' AND a.backend_type = 'walsender')
        """;

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// A row the subscriber's check constraint rejects is skipped alone under discard, takes its transaction with it
    /// under transdiscard, and disables the subscription under sub_disable, which applies it once the constraint is
    /// dropped and the subscription enabled.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task FailedChangesAreDiscardedOrDisableTheSubscription()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        // Only the subscriber has the check constraint and the deferred check for a child's parent, which apply runs,
        // unlike a foreign key, since it fires always.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, v text);
            CREATE TABLE parent (id integer PRIMARY KEY);
            CREATE TABLE child (id integer PRIMARY KEY, parent_id integer);
            CREATE TABLE u (id integer PRIMARY KEY, v text);
            SELECT * FROM worf.replication_set_add_all_tables('default', '{public}');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, v text CONSTRAINT not_bad CHECK (v <> 'bad'));
            CREATE TABLE parent (id integer PRIMARY KEY);
            CREATE TABLE child (id integer PRIMARY KEY, parent_id integer);
            CREATE FUNCTION check_parent() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM parent WHERE id = NEW.parent_id) THEN
                    RAISE foreign_key_violation USING MESSAGE = 'no parent ' || NEW.parent_id;
                END IF;
                RETURN NULL;
            END $$;
            CREATE CONSTRAINT TRIGGER child_parent AFTER INSERT ON child DEFERRABLE INITIALLY DEFERRED
                FOR EACH ROW EXECUTE FUNCTION check_parent();
            ALTER TABLE child ENABLE ALWAYS TRIGGER child_parent;

            -- Temporary tables with different ON COMMIT settings and a foreign key between them fail the commit after
            -- its pre-commit callbacks.
            CREATE TABLE u (id integer PRIMARY KEY, v text);
            CREATE FUNCTION fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                CREATE TEMP TABLE IF NOT EXISTS on_commit_a (id integer PRIMARY KEY) ON COMMIT DELETE ROWS;
                CREATE TEMP TABLE IF NOT EXISTS on_commit_b (id integer REFERENCES on_commit_a);
                RETURN NULL;
            END $$;
            CREATE TRIGGER fail_commit AFTER INSERT ON u FOR EACH ROW EXECUTE FUNCTION fail_commit();
            ALTER TABLE u ENABLE ALWAYS TRIGGER fail_commit;
            """, token);
        await SubscribeAsync(provider, subscriber, token);

        // The worker that stops to apply a transaction again change by change restarts at once, rather than after
        // the usual five seconds, or one on Windows, where the time alone can't tell.
        await SetBehaviourAsync(subscriber, "discard", token);
        int logpos = subscriber.ReadLog().Length;
        Stopwatch restarting = Stopwatch.StartNew();
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (1, 'good'), (2, 'bad'), (3, 'good')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 3)", token),
            "the discarded change's transaction");
        Assert.IsLessThan(4500, restarting.ElapsedMilliseconds, "the worker restarted at once");
        Assert.Contains("restarting the apply worker for subscription sub at once", subscriber.ReadLog()[logpos..],
            "the worker restarted before the restart delay");
        Assert.AreEqual("1\n3", await subscriber.SafePsqlAsync(Database, "SELECT id FROM t ORDER BY id", token));
        Assert.AreEqual("discard|INSERT|public|t|2|bad|23514", await LastLogAsync(subscriber, token));

        // A queued DDL statement that fails is discarded too.
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE dup (id integer PRIMARY KEY)", token);
        _ = await provider.SafePsqlAsync(Database,
            "SELECT worf.replicate_ddl_command('CREATE TABLE public.dup (id integer PRIMARY KEY)')", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (4, 'good')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 4)", token),
            "the transaction after the discarded DDL");
        Assert.AreEqual("discard|QUEUED_SQL|t|42P07", await subscriber.SafePsqlAsync(Database, """
            SELECT behaviour, operation, queued_message LIKE '%CREATE TABLE public.dup%', error_sqlstate
            FROM worf.exception_log ORDER BY log_id DESC LIMIT 1
            """, token));

        // A child inserted before its parent passes the deferred check at commit.
        _ = await provider.SafePsqlAsync(Database, """
            INSERT INTO child VALUES (1, 1);
            INSERT INTO parent VALUES (1);
            INSERT INTO t VALUES (5, 'bad');
            """, token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (6, 'good')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 6)", token),
            "the transaction after the one with the deferred check");
        Assert.AreEqual("1|1", await subscriber.SafePsqlAsync(Database, "SELECT * FROM child", token));
        Assert.AreEqual("discard|INSERT|public|t|5|bad|23514", await LastLogAsync(subscriber, token));

        // A deferred check that fails at commit skips the whole transaction.
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO child VALUES (2, 99)", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (7, 'good')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 7)", token),
            "the transaction after the one that failed its deferred check");
        Assert.AreEqual("1|1", await subscriber.SafePsqlAsync(Database, "SELECT * FROM child", token));
        Assert.AreEqual("discard|INSERT|public|child|2||23503", await LastLogAsync(subscriber, token));

        // A commit that fails after its pre-commit callbacks, however the transaction is applied, is recorded and
        // the transaction skipped.
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO u VALUES (1, 'fails at commit')", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (8, 'good')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 8)", token),
            "the transaction after the one whose commit fails");
        Assert.AreEqual("0", await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM u", token));
        Assert.AreEqual("discard|INSERT|public|u|1|fails at commit|0A000", await LastLogAsync(subscriber, token));

        await SetBehaviourAsync(subscriber, "transdiscard", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (11, 'good'), (12, 'bad'), (13, 'good')", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (14, 'good')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 14)", token),
            "the transaction after the discarded one");
        Assert.AreEqual("1\n3\n4\n6\n7\n8\n14", await subscriber.SafePsqlAsync(Database, "SELECT id FROM t ORDER BY id", token));
        // Joined with \n, as the rows come, rather than written out, which takes the checkout's line endings.
        Assert.AreEqual(string.Join('\n', "transdiscard|INSERT|public|t|11|good|23514", "transdiscard|INSERT|public|t|12|bad|23514",
            "transdiscard|INSERT|public|t|13|good|23514"), await subscriber.SafePsqlAsync(Database, """
            SELECT behaviour, operation, schema_name, table_name, remote_new_tuple->>'id', remote_new_tuple->>'v', error_sqlstate
            FROM worf.exception_log WHERE remote_new_tuple->>'id' IN ('11', '12', '13') ORDER BY log_id
            """, token), "every change of the skipped transaction");
        Assert.AreEqual("1", await subscriber.SafePsqlAsync(Database,
            "SELECT count(DISTINCT xmin::text) FROM worf.exception_log WHERE remote_new_tuple->>'id' IN ('11', '12', '13')", token),
            "transactions recording the skipped changes, rather than a subtransaction each");

        // A skipped transaction's queued DDL is recorded as the statement.
        _ = await provider.SafePsqlAsync(Database, """
            SELECT worf.replicate_ddl_command('CREATE TABLE public.skipped (id integer PRIMARY KEY)');
            INSERT INTO t VALUES (15, 'bad');
            """, token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (16, 'good')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 16)", token),
            "the transaction after the one with the DDL");
        Assert.AreEqual("QUEUED_SQL|t,INSERT|15", await subscriber.SafePsqlAsync(Database, """
            SELECT string_agg(concat_ws('|', operation, remote_new_tuple->>'id', queued_message LIKE '%CREATE TABLE public.skipped%'), ','
                ORDER BY log_id)
            FROM worf.exception_log
            WHERE remote_commit_lsn = (SELECT remote_commit_lsn FROM worf.exception_log ORDER BY log_id DESC LIMIT 1)
            """, token), "the skipped transaction's changes");

        await SetBehaviourAsync(subscriber, "sub_disable", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (21, 'bad')", token);
        await WaitForStatusAsync(subscriber, "disabled", token);
        Assert.AreEqual("sub_disable|INSERT|public|t|21|bad|23514", await LastLogAsync(subscriber, token));

        _ = await subscriber.SafePsqlAsync(Database, "ALTER TABLE t DROP CONSTRAINT not_bad", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('sub')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 21)", token),
            "the transaction that disabled the subscription");

        // A table's copy waits for apply, which fails on a row until the constraint goes, without the snapshot that would
        // keep the provider from removing dead rows meanwhile. The provider holds the sync worker back until the row is
        // in.
        await SetBehaviourAsync(subscriber, "error", token);
        _ = await subscriber.SafePsqlAsync(Database, "ALTER TABLE t ADD CONSTRAINT not_bad CHECK (v <> 'bad') NOT VALID", token);
        await using (NpgsqlConnection held = await provider.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await held.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE worf.sequence_range", held, transaction))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_resynchronize_table('sub', 'parent')", token);
            Assert.IsTrue(await provider.PollQueryUntilAsync(Database,
                SyncWorkerHeldSql, token),
                "the sync worker reading the provider's managed sequences");
            _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (31, 'bad')", token);
            await transaction.RollbackAsync(token);
        }

        await Task.Delay(TimeSpan.FromSeconds(2), token);
        _ = await provider.SafePsqlAsync(Database, "CREATE TABLE churn AS SELECT generate_series(1, 1000) AS id", token);
        _ = await provider.SafePsqlAsync(Database, "DELETE FROM churn", token);
        _ = await provider.SafePsqlAsync(Database, "VACUUM churn", token);
        Assert.AreEqual("0", await provider.SafePsqlAsync(Database, "SELECT pg_relation_size('churn')", token),
            "the pages VACUUM emptied of the dead rows and truncated");
        _ = await subscriber.SafePsqlAsync(Database, "ALTER TABLE t DROP CONSTRAINT not_bad", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'parent'", token), "the table synchronized");
        Assert.AreEqual("1", await subscriber.SafePsqlAsync(Database, "SELECT string_agg(id::text, ',') FROM parent", token));

        // The copy lets its snapshot go when apply gets no further, here on a row a local transaction holds, and says
        // how far apply has got. Apply may then get no further for twice as long, and moves on with each change of a
        // transaction that takes a while: nine slow rows and one slower one don't let the next snapshot go. The
        // provider's open transactions hold the copy's slot until they commit.
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE FUNCTION slow() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM pg_sleep(CASE NEW.v WHEN 'slow' THEN 1 WHEN 'slower' THEN 14 ELSE 0 END);
                RETURN NEW;
            END $$;
            CREATE TRIGGER slow BEFORE INSERT ON t FOR EACH ROW EXECUTE FUNCTION slow();
            ALTER TABLE t ENABLE ALWAYS TRIGGER slow;
            """, token);
        string before = await provider.SafePsqlAsync(Database, "SELECT pg_current_wal_lsn()", token);
        _ = await provider.SafePsqlAsync(Database, $"SELECT worf.wait_slot_confirm_lsn(NULL, '{before}')", token);
        int copypos = subscriber.ReadLog().Length;
        await using (NpgsqlConnection local = await subscriber.OpenAsync(Database, token))
        await using (NpgsqlConnection held = await provider.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction localTransaction = await local.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("INSERT INTO t VALUES (41, 'local')", local, localTransaction))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            await using (NpgsqlTransaction transaction = await held.BeginTransactionAsync(token))
            {
                await using (NpgsqlCommand command = new("INSERT INTO t VALUES (41, 'held')", held, transaction))
                {
                    _ = await command.ExecuteNonQueryAsync(token);
                }

                _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_resynchronize_table('sub', 'parent')", token);
                Assert.IsTrue(await provider.PollQueryUntilAsync(Database, CopySlotSql, token), "the copy's slot");
                await transaction.CommitAsync(token);
            }

            Assert.IsTrue(await subscriber.PollLogAsync(copypos, "dropping the snapshot for table public.parent", token), "the snapshot let go");
            int droppos = subscriber.ReadLog().IndexOf("dropping the snapshot for table public.parent", copypos, StringComparison.Ordinal);
            Assert.IsTrue(await subscriber.PollLogAsync(droppos, "table public.parent waits to copy", token), "the wait without the snapshot");
            Match wait = AppliedPosition().Match(subscriber.ReadLog()[droppos..]);
            Assert.IsTrue(wait.Success, "the wait says how far apply is");
            string at = wait.Groups[1].Value;
            Assert.AreEqual("t", await provider.SafePsqlAsync(Database, $"SELECT '{at}'::pg_lsn >= '{before}'::pg_lsn", token),
                $"apply at {at}, past {before}");

            await using NpgsqlTransaction slowTransaction = await held.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("INSERT INTO t SELECT g, 'slow' FROM generate_series(42, 50) g; INSERT INTO t VALUES (51, 'slower')",
                held, slowTransaction))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            await localTransaction.RollbackAsync(token);
            Assert.IsTrue(await provider.PollQueryUntilAsync(Database, CopySlotSql, token), "the copy's next slot");
            await slowTransaction.CommitAsync(token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'parent'", token), "the table synchronized again");
        Assert.AreEqual(1, subscriber.ReadLog()[copypos..].Split("dropping the snapshot").Length - 1, "snapshots let go");
        Assert.AreEqual("11", await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM t WHERE id > 40", token));

        // The rolled back subtransactions released what they held.
        string[] leaks = [.. subscriber.ReadLog().Split('\n').Where(line => line.Contains("leak", StringComparison.Ordinal) || line.Contains("not closed", StringComparison.Ordinal))];
        Assert.IsEmpty(leaks, string.Join('\n', leaks));
    }

    /// <summary>
    /// A row that fails in a batch of inserts, which apply writes when the next message for another table or the
    /// commit arrives, is skipped under discard and recorded as the batch's failure under sub_disable.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task FailedBatchesOfInsertsAreDiscardedOrDisableTheSubscription()
    {
        CancellationToken token = TestContext.CancellationToken;

        // Apply batches inserts only when conflicts are errors.
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token, "worf.conflict_resolution = error");
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, v text);
            CREATE TABLE t2 (id integer PRIMARY KEY);
            SELECT * FROM worf.replication_set_add_all_tables('default', '{public}');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, v text);
            CREATE TABLE t2 (id integer PRIMARY KEY);
            INSERT INTO t VALUES (9, 'local'), (19, 'local');
            """, token);
        await SubscribeAsync(provider, subscriber, token);

        // Inserts after the seventh in a row are batched, and the subscriber has the ninth's key.
        await SetBehaviourAsync(subscriber, "discard", token);
        _ = await provider.SafePsqlAsync(Database, """
            INSERT INTO t SELECT g, 'remote' FROM generate_series(1, 10) g;
            INSERT INTO t2 VALUES (1);
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t2 WHERE id = 1)", token),
            "the second table's row");
        Assert.AreEqual("1,2,3,4,5,6,7,8,10|local", await subscriber.SafePsqlAsync(Database,
            "SELECT string_agg(id::text, ',' ORDER BY id) FILTER (WHERE v = 'remote'), (SELECT v FROM t WHERE id = 9) FROM t",
            token));
        Assert.AreEqual("discard|INSERT|public|t|9|remote|XX000", await LastLogAsync(subscriber, token));

        await SetBehaviourAsync(subscriber, "sub_disable", token);
        _ = await provider.SafePsqlAsync(Database, """
            INSERT INTO t SELECT g, 'remote' FROM generate_series(11, 20) g;
            INSERT INTO t2 VALUES (2);
            """, token);
        await WaitForStatusAsync(subscriber, "disabled", token);
        Assert.AreEqual("sub_disable|multi INSERT|public|t|||23505", await LastLogAsync(subscriber, token));

        _ = await subscriber.SafePsqlAsync(Database, "DELETE FROM t WHERE id = 19", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('sub')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t2 WHERE id = 2)", token),
            "the transaction that disabled the subscription");
        Assert.AreEqual("10", await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM t WHERE id BETWEEN 11 AND 20", token));
    }

    /// <summary>
    /// A change that deadlocks with a local transaction while discard applies its transaction again is applied
    /// again with it once the deadlock clears, rather than skipped as if its data were at fault. A table sync worker
    /// catching up after its copy applies the changes the apply worker passed over, though that worker restarted since,
    /// and discards a failed change as the apply worker does, rather than copying again, though the walsender it
    /// streamed from holds its slot. It leaves anything else that holds its slot, with a warning, and one that fails
    /// while catching up starts again.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DeadlocksDuringADiscardReplayAreRetried()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode provider = await StartNodeAsync("provider", token);
        // PostgreSQL looks for a deadlock once, when a wait has lasted deadlock_timeout, so the replay's lasts long
        // enough for the local transaction below to have queued behind it by then.
        await using TestNode subscriber = await StartNodeAsync("subscriber", token, "worf.exception_behaviour = discard", "deadlock_timeout = 5s");
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, v text);
            CREATE TABLE a (id integer PRIMARY KEY, v text);
            CREATE TABLE b (id integer PRIMARY KEY, v text);
            INSERT INTO a VALUES (1, 'initial');
            INSERT INTO b VALUES (1, 'initial');
            SELECT * FROM worf.replication_set_add_all_tables('default', '{public}');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, v text CONSTRAINT not_bad CHECK (v <> 'bad'));
            CREATE TABLE a (id integer PRIMARY KEY, v text);
            CREATE TABLE b (id integer PRIMARY KEY, v text);
            INSERT INTO a VALUES (1, 'initial');
            INSERT INTO b VALUES (1, 'initial');
            """, token);

        // The subscription forwards no other node's changes, as the table sync below relies on.
        await SubscribeAsync(provider, subscriber, token, "{}");

        // This transaction holds b. The remote transaction updates a, then fails on its bad row before it updates b,
        // so discard applies it again, and the replay updates a and waits for b. This transaction then waits for a,
        // and the replay, which waited first, detects the deadlock and is cancelled.
        int logpos = subscriber.ReadLog().Length;
        await using (NpgsqlConnection local = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await local.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("SET LOCAL deadlock_timeout = '1min'; UPDATE b SET v = 'local' WHERE id = 1",
                local, transaction))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database, """
                UPDATE a SET v = 'remote' WHERE id = 1;
                INSERT INTO t VALUES (1, 'bad');
                UPDATE b SET v = 'remote' WHERE id = 1;
                """, token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND backend_type <> 'client backend')",
                token), "the replay waiting for b");
            await using (NpgsqlCommand command = new("UPDATE a SET v = 'local' WHERE id = 1", local, transaction))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            await transaction.CommitAsync(token);
        }

        Assert.IsTrue(await subscriber.PollLogAsync(logpos, "deadlock detected", token), "the deadlock");
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT v = 'remote' FROM b", token), "the retried replay");
        Assert.AreEqual("remote", await subscriber.SafePsqlAsync(Database, "SELECT v FROM a", token));
        Assert.AreEqual("discard|INSERT|public|t|1|bad|23514", await subscriber.SafePsqlAsync(Database, """
            SELECT string_agg(concat_ws('|', behaviour, operation, schema_name, table_name, remote_new_tuple->>'id',
                remote_new_tuple->>'v', error_sqlstate), ',')
            FROM worf.exception_log
            """, token));

        // A table sync worker catches up from its own slot, with every origin's changes. Its copy waits on a lock
        // here, so that the bad row, inserted once the copy has its snapshot, comes in its catch-up. Its apply of the
        // bad row waits on an advisory lock until its walsender has gone on to a transaction from another origin and
        // waits for a lock to name it, holding the slot. The apply worker's walsender skips that transaction, so the
        // apply worker, between transactions, starts the catch-up.
        await using NpgsqlConnection holder = await subscriber.OpenAsync(Database, token);
        await using NpgsqlConnection blocker = await provider.OpenAsync(Database, token);
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE synced (id integer PRIMARY KEY, v text CHECK (v <> 'bad'))", token);
        }

        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE FUNCTION hold_bad() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.v = 'bad' THEN
                    PERFORM pg_advisory_xact_lock(4242);
                END IF;
                RETURN NEW;
            END
            $$;
            CREATE TRIGGER hold_bad BEFORE INSERT ON synced FOR EACH ROW EXECUTE FUNCTION hold_bad();
            ALTER TABLE synced ENABLE ALWAYS TRIGGER hold_bad;
            """, token);
        _ = await provider.SafePsqlAsync(Database, """
            ALTER TABLE synced DROP CONSTRAINT synced_v_check;
            INSERT INTO synced VALUES (1, 'copied');
            CREATE TABLE elsewhere (id integer);
            SELECT pg_replication_origin_create('elsewhere');
            """, token);
        await using (NpgsqlCommand command = new("SELECT pg_advisory_lock(4242)", holder))
        {
            _ = await command.ExecuteNonQueryAsync(token);
        }

        await using (NpgsqlConnection local = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await local.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE synced", local, transaction))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database,
                "SELECT * FROM worf.replication_set_add_table('default', 'synced', synchronize_data := true)", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'synced'::regclass AND NOT granted)", token), "the copy waiting");
            _ = await provider.SafePsqlAsync(Database, "INSERT INTO synced VALUES (2, 'bad'), (3, 'good')", token);
            await using NpgsqlTransaction subscriptions = await blocker.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE worf.subscription", blocker, subscriptions))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database,
                "SELECT pg_replication_origin_session_setup('elsewhere'); INSERT INTO elsewhere VALUES (1)", token);

            // The apply worker waits here too, with the bad row's transaction, so its catch-up stops past it.
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT count(*) = 2 FROM pg_locks WHERE relation = 'synced'::regclass AND NOT granted", token), "the apply worker waiting");
            await transaction.RollbackAsync(token);

            string slot = await subscriber.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event = 'advisory')", token), "the bad row's apply waiting");
            Assert.IsTrue(await provider.PollQueryUntilAsync(Database, $"""
                SELECT EXISTS (SELECT 1 FROM pg_replication_slots s JOIN pg_stat_activity a ON a.pid = s.active_pid
                    WHERE starts_with(s.slot_name, '{slot}_') AND a.wait_event_type = 'Lock')
                """, token), "the sync worker's walsender waiting");

            // The sync worker stops the walsender rather than failing, which would copy the table again.
            int providerLogpos = provider.ReadLog().Length;
            await using (NpgsqlCommand command = new("SELECT pg_advisory_unlock(4242)", holder))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            Assert.IsTrue(await provider.PollLogAsync(providerLogpos, "terminating connection due to administrator command", token),
                "the walsender holding the slot stopped");
            await subscriptions.CommitAsync(token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'synced'", token), "the table synchronized");
        Assert.AreEqual("1\n3", await subscriber.SafePsqlAsync(Database, "SELECT id FROM synced ORDER BY id", token));
        Assert.AreEqual("discard|INSERT|public|synced|2|bad|23514", await LastLogAsync(subscriber, token));
        Assert.AreEqual(1, subscriber.ReadLog().Split("starting sync of table public.synced ").Length - 1, "copies of the table");

        // A sync worker leaves anything but a walsender of its own that holds its slot, here a session reading the
        // slot's changes that waits for the same lock, and waits for it with a warning. One that fails while it catches
        // up, here as it waits, starts again and catches up from where it got to, rather than leaving the apply worker
        // waiting, or copying again over the rows it copied and the subscriber's own.
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE synced2 (id integer PRIMARY KEY)", token);
        }

        _ = await provider.SafePsqlAsync(Database, "INSERT INTO synced2 VALUES (1)", token);
        _ = await subscriber.SafePsqlAsync(Database, "INSERT INTO synced2 VALUES (100)", token);
        await using NpgsqlConnection reader = await provider.OpenAsync(Database, token);
        await using NpgsqlCommand peek = reader.CreateCommand();
        await using NpgsqlTransaction moreSubscriptions = await blocker.BeginTransactionAsync(token);
        Task<object?> reading;
        await using (NpgsqlConnection local = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await local.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE synced2", local, transaction))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database,
                "SELECT * FROM worf.replication_set_add_table('default', 'synced2', synchronize_data := true)", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'synced2'::regclass AND NOT granted)", token), "the second copy waiting");
            _ = await provider.SafePsqlAsync(Database, "INSERT INTO synced2 VALUES (2)", token);
            await using (NpgsqlCommand command = new("LOCK TABLE worf.subscription", blocker, moreSubscriptions))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database,
                "SELECT pg_replication_origin_session_setup('elsewhere'); INSERT INTO elsewhere VALUES (2)", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT count(*) = 2 FROM pg_locks WHERE relation = 'synced2'::regclass AND NOT granted", token), "the apply worker waiting again");

            string slot = await subscriber.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription", token);
            string syncSlot = await provider.SafePsqlAsync(Database,
                $"SELECT slot_name FROM pg_replication_slots WHERE starts_with(slot_name, '{slot}_')", token);
            peek.CommandText = $"""
                SELECT count(*) FROM pg_logical_slot_peek_binary_changes('{syncSlot}', NULL, NULL, 'startup_params_format', '1',
                    'min_proto_version', '1', 'max_proto_version', '1', 'worf.forward_origins', 'all')
                """;
            reading = peek.ExecuteScalarAsync(token);
            Assert.IsTrue(await provider.PollQueryUntilAsync(Database, $"""
                SELECT EXISTS (SELECT 1 FROM pg_replication_slots s JOIN pg_stat_activity a ON a.pid = s.active_pid
                    WHERE s.slot_name = '{syncSlot}' AND a.pid = {reader.ProcessID} AND a.wait_event_type = 'Lock')
                """, token), "the session reading the slot waiting");
            logpos = subscriber.ReadLog().Length;
            await transaction.RollbackAsync(token);
        }

        Assert.IsTrue(await subscriber.PollLogAsync(logpos, "which this table sync waits for", token), "the warning");
        Assert.IsFalse(reading.IsCompleted, "the session reading the slot stopped");
        Assert.AreEqual("u", await subscriber.SafePsqlAsync(Database,
            "SELECT sync_status FROM worf.local_sync_status WHERE sync_relname = 'synced2'", token), "the sync catching up");
        _ = await subscriber.SafePsqlAsync(Database,
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type LIKE 'worf sync%'", token);
        _ = await provider.SafePsqlAsync(Database, $"SELECT pg_cancel_backend({reader.ProcessID})", token);
        _ = await Assert.ThrowsExactlyAsync<PostgresException>(() => reading);
        await moreSubscriptions.CommitAsync(token);

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'synced2'", token), "the second table synchronized");
        Assert.AreEqual("1\n2\n100", await subscriber.SafePsqlAsync(Database, "SELECT id FROM synced2 ORDER BY id", token));
        Assert.Contains("resuming the catch-up of table public.synced2", subscriber.ReadLog()[logpos..], "the restarted sync catching up");
        Assert.AreEqual(1, subscriber.ReadLog().Split("finished synchronization of data for table public.synced2").Length - 1,
            "copies of the second table");

        // The sync worker's copy waits here on a row a local transaction inserts, while apply passes over changes to
        // the table that commit after the copy's snapshot, and then restarts on a failed change elsewhere. The copy's
        // catch-up applies the changes all the same, and discards the failed one as apply does.
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE synced3 (id integer PRIMARY KEY, v text CHECK (v <> 'bad'))", token);
        }

        _ = await provider.SafePsqlAsync(Database, "ALTER TABLE synced3 DROP CONSTRAINT synced3_v_check; INSERT INTO synced3 VALUES (1, 'copied')", token);
        await using (NpgsqlConnection local = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction transaction = await local.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("INSERT INTO synced3 VALUES (1, 'local')", local, transaction))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database,
                "SELECT * FROM worf.replication_set_add_table('default', 'synced3', synchronize_data := true)", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'transactionid' AND NOT granted)", token), "the copy waiting");
            _ = await provider.SafePsqlAsync(Database, "INSERT INTO synced3 VALUES (2, 'bad'), (3, 'good')", token);
            _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (2, 'bad')", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM worf.exception_log WHERE table_name = 't' AND remote_new_tuple->>'id' = '2')", token),
                "apply restarted past the changes to synced3");
            await transaction.RollbackAsync(token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'synced3'", token), "the third table synchronized");
        Assert.AreEqual("1\n3", await subscriber.SafePsqlAsync(Database, "SELECT id FROM synced3 ORDER BY id", token));
        Assert.AreEqual("discard|INSERT|public|synced3|2|bad|23514", await LastLogAsync(subscriber, token));

        // Apply that restarts while the table's sync worker catches up waits for the catch-up, which goes as far as the
        // worker before it had got, before it applies the table's changes after that. The provider holds the sync
        // worker back until this transaction holds the copy back, and a slow trigger holds the catch-up. The copy
        // leaves out the bad row the subscriber's check rejects.
        _ = await provider.SafePsqlAsync(Database, "DELETE FROM synced WHERE id = 2", token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE FUNCTION slow() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF NEW.v = 'slow' THEN
                    PERFORM pg_sleep(3);
                END IF;
                RETURN NEW;
            END $$;
            CREATE TRIGGER slow BEFORE UPDATE ON synced FOR EACH ROW EXECUTE FUNCTION slow();
            ALTER TABLE synced ENABLE ALWAYS TRIGGER slow;
            """, token);
        await using (NpgsqlConnection held = await provider.OpenAsync(Database, token))
        await using (NpgsqlConnection local = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction heldTransaction = await held.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE worf.sequence_range", held, heldTransaction))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_resynchronize_table('sub', 'synced')", token);
            Assert.IsTrue(await provider.PollQueryUntilAsync(Database,
                SyncWorkerHeldSql, token),
                "the sync worker reading the provider's managed sequences");
            await using NpgsqlTransaction transaction = await local.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE synced IN SHARE MODE", local, transaction))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            await heldTransaction.RollbackAsync(token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE NOT granted AND relation = 'synced'::regclass)", token), "the copy");
            _ = await provider.SafePsqlAsync(Database, "UPDATE synced SET v = 'slow' WHERE id = 1", token);
            await transaction.RollbackAsync(token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND application_name LIKE '%sync%')", token),
            "the catch-up in the trigger");
        _ = await subscriber.SafePsqlAsync(Database,
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name LIKE '%apply%'", token);
        _ = await provider.SafePsqlAsync(Database, "UPDATE synced SET v = 'after' WHERE id = 3", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'synced'", token), "the table synchronized again");
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (3, 'last')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 3)", token),
            "the transaction after the update");
        Assert.AreEqual("1|slow\n3|after", await subscriber.SafePsqlAsync(Database, "SELECT * FROM synced ORDER BY id", token));
    }

    /// <summary>
    /// A table sync worker stopped once its copy has committed, here while it waits for the apply worker, which waits on
    /// a lock with changes to the table from after the copy skipped, catches up from its copy when it starts again, with
    /// those changes, and leaves its slot to the apply worker when its cleanup can't reach the provider. When its slot is
    /// gone or invalidated, or its origin has no progress, it copies the table again and merges the copy in, in one
    /// transaction, so the table is never seen empty, though a key references the table, its own key is an identity or
    /// deferrable: the provider's rows replace those with the same key, and the table's other rows stay, those written
    /// here or under another origin, and those the provider has since deleted. One cancelled before its copy commits
    /// copies a table without a key once, and the cleanup after it waits for a session that holds its slot, with a
    /// warning, rather than for ever. Dropping the subscription drops the slots its syncs left.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task StoppedTableSyncsCatchUpOrCopyAgain()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE x (id serial PRIMARY KEY)", token);
        }

        _ = await provider.SafePsqlAsync(Database, """
            SELECT * FROM worf.replication_set_add_table('default', 'x');
            CREATE TABLE elsewhere (id integer);
            SELECT pg_replication_origin_create('elsewhere');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT pg_replication_origin_create('elsewhere')", token);
        await SubscribeAsync(provider, subscriber, token, "{}");
        string slot = await subscriber.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription", token);
        const string SyncWorkers = "SELECT pid FROM pg_stat_activity WHERE backend_type LIKE 'worf sync%'";

        // DDL on a table that commits while the apply worker waits for its lock to open it leaves the worker to look the
        // table up again.
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO x DEFAULT VALUES; SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        await using (NpgsqlConnection altering = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction alter = await altering.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE x; ALTER TABLE x SET (fillfactor = 90)", altering, alter))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database, "INSERT INTO x DEFAULT VALUES", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'x'::regclass AND NOT granted)", token), "the apply worker waiting");
            await alter.CommitAsync(token);
        }

        _ = await provider.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("2", await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM x", token), "x's rows");
        Assert.DoesNotContain("with OID 0", subscriber.ReadLog(), "the subscriber's log");

        foreach (string stop in new[] { "resume", "drop", "zero", "lost" })
        {
            string table = $"stopped_{stop}";
            _ = await provider.SafePsqlAsync(Database, $"CREATE TABLE {table} (id integer PRIMARY KEY, v text)", token);
            string key = stop switch
            {
                "zero" => "integer PRIMARY KEY DEFERRABLE UNIQUE",
                "lost" => "integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY",
                _ => "integer PRIMARY KEY",
            };
            _ = await subscriber.SafePsqlAsync(Database, $"CREATE TABLE {table} (id {key}, v text)", token);

            // A key on the subscriber references the table.
            _ = await subscriber.SafePsqlAsync(Database, $"CREATE TABLE {table}_ref (id integer REFERENCES {table} (id))", token);

            _ = await provider.SafePsqlAsync(Database, $"INSERT INTO {table} SELECT g, 'copied' FROM generate_series(1, 1000) g", token);
            int logpos = subscriber.ReadLog().Length;
            await using (NpgsqlConnection tableLocker = await subscriber.OpenAsync(Database, token))
            await using (NpgsqlConnection applyLocker = await subscriber.OpenAsync(Database, token))
            {
                // The copy waits on a row a local transaction inserts, and the apply worker, once past the changes from
                // after the copy, for x, so that the copy commits only once the apply worker can't let the sync catch up.
                await using NpgsqlTransaction tableLock = await tableLocker.BeginTransactionAsync(token);
                await using (NpgsqlCommand command = new($"INSERT INTO {table} OVERRIDING SYSTEM VALUE VALUES (1, 'local')", tableLocker, tableLock))
                {
                    _ = await command.ExecuteNonQueryAsync(token);
                }

                _ = await provider.SafePsqlAsync(Database,
                    $"SELECT * FROM worf.replication_set_add_table('default', '{table}', synchronize_data := true)", token);
                Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
                    SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                        WHERE l.locktype = 'transactionid' AND NOT l.granted AND a.backend_type LIKE 'worf sync%')
                    """, token), "the copy waiting");
                _ = await provider.SafePsqlAsync(Database,
                    $"UPDATE {table} SET v = 'updated' WHERE id = 500; DELETE FROM {table} WHERE id = 600; INSERT INTO {table} VALUES (700000, 'new')",
                    token);
                await using NpgsqlTransaction applyLock = await applyLocker.BeginTransactionAsync(token);
                await using (NpgsqlCommand command = new("LOCK TABLE x", applyLocker, applyLock))
                {
                    _ = await command.ExecuteNonQueryAsync(token);
                }

                _ = await provider.SafePsqlAsync(Database, "INSERT INTO x DEFAULT VALUES", token);
                Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'x'::regclass AND NOT granted)", token), "the apply worker waiting");
                await tableLock.RollbackAsync(token);
                Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"""
                    SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'x'::regclass AND NOT granted)
                        AND EXISTS (SELECT 1 FROM worf.local_sync_status WHERE sync_relname = '{table}' AND sync_status = 'w')
                    """, token), "the sync waiting for the apply worker");

                // The worker stops, and the next one can't start until the provider's slot or origin is changed.
                if (stop != "resume")
                {
                    _ = await provider.SafePsqlAsync(Database, $"ALTER ROLE {SuperUser} NOLOGIN", token);
                }

                _ = await subscriber.SafePsqlAsync(Database, $"SELECT pg_terminate_backend(pid) FROM ({SyncWorkers}) w", token);
                Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"SELECT NOT EXISTS ({SyncWorkers})", token), "the sync stopped");
                Assert.AreEqual("w", await subscriber.SafePsqlAsync(Database,
                    $"SELECT sync_status FROM worf.local_sync_status WHERE sync_relname = '{table}'", token), "the sync left waiting");

                // Rows written here and under another origin, and one that references the table.
                _ = await subscriber.SafePsqlAsync(Database, $"""
                    INSERT INTO {table} OVERRIDING SYSTEM VALUE VALUES (900001, 'here');
                    SELECT pg_replication_origin_session_setup('elsewhere');
                    INSERT INTO {table} OVERRIDING SYSTEM VALUE VALUES (900002, 'elsewhere');
                    INSERT INTO {table}_ref VALUES (1);
                    """, token);

                // The next worker catches up while the table is locked, and can't reach the provider once it's done.
                if (stop == "resume")
                {
                    await using NpgsqlTransaction catchupLock = await tableLocker.BeginTransactionAsync(token);
                    await using (NpgsqlCommand command = new($"LOCK TABLE {table}", tableLocker, catchupLock))
                    {
                        _ = await command.ExecuteNonQueryAsync(token);
                    }

                    await applyLock.RollbackAsync(token);
                    Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"""
                        SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                            WHERE l.relation = '{table}'::regclass AND NOT l.granted AND a.backend_type LIKE 'worf sync%')
                        """, token), "the catch-up waiting");
                    _ = await provider.SafePsqlAsync(Database, $"ALTER ROLE {SuperUser} NOLOGIN", token);
                    await catchupLock.RollbackAsync(token);
                    Assert.IsTrue(await subscriber.PollLogAsync(logpos, "The subscription's apply worker drops it once it can.", token),
                        "the slot left to the apply worker");
                    _ = await provider.SafePsqlAsync(Database, $"ALTER ROLE {SuperUser} LOGIN", token);
                    Assert.IsTrue(await provider.PollQueryUntilAsync(Database,
                        $"SELECT NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE starts_with(slot_name, '{slot}_'))", token),
                        "the slot the sync left dropped");
                }
                else
                {
                    await applyLock.RollbackAsync(token);
                }
            }

            string syncSlot = await provider.SafePsqlAsync(Database,
                $"SELECT slot_name FROM pg_replication_slots WHERE starts_with(slot_name, '{slot}_')", token);
            switch (stop)
            {
                case "drop":
                    _ = await provider.SafePsqlAsync(Database, $"SELECT pg_drop_replication_slot('{syncSlot}')", token);
                    break;
                case "zero":
                    _ = await subscriber.SafePsqlAsync(Database, $"SELECT pg_replication_origin_advance('{syncSlot}', '0/0')", token);
                    break;
                case "lost":
                    await InvalidateSlotAsync(provider, slot, syncSlot, token);
                    break;
            }

            // A session counts the table's rows while it's copied again.
            using CancellationTokenSource counting = CancellationTokenSource.CreateLinkedTokenSource(token);
            Task<long> fewest = Task.FromResult(long.MaxValue);
            if (stop != "resume")
            {
                fewest = FewestRowsAsync(subscriber, table, counting.Token);
                _ = await provider.SafePsqlAsync(Database, $"ALTER ROLE {SuperUser} LOGIN", token);
            }

            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                $"SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = '{table}'", token), $"the table synchronized after {stop}");
            await counting.CancelAsync();
            Assert.IsGreaterThanOrEqualTo(1000L, await fewest, $"the fewest rows seen during the sync after {stop}");
            Assert.AreEqual(stop == "resume" ? "1000|500500|updated|f|t|here|elsewhere|1" : "1001|501100|updated|t|t|here|elsewhere|1",
                await subscriber.SafePsqlAsync(Database, $"""
                SELECT count(*), sum(id) - 700000 + 600, (SELECT v FROM {table} WHERE id = 500),
                    EXISTS (SELECT 1 FROM {table} WHERE id = 600), EXISTS (SELECT 1 FROM {table} WHERE id = 700000),
                    (SELECT v FROM {table} WHERE id = 900001), (SELECT v FROM {table} WHERE id = 900002),
                    (SELECT count(*) FROM {table}_ref)
                FROM {table} WHERE id < 900000
                """, token), $"the rows after {stop}");
            Assert.Contains(stop == "resume" ? $"resuming the catch-up of table public.{table}" : $"copying table public.{table} again",
                subscriber.ReadLog()[logpos..], $"the sync after {stop}");
        }

        // The copy of a table without a key has its rows, but can't commit them with its status, which a session
        // holds. Cancelled, it cleans up once a session reading its slot's changes, which waits for a lock, lets the slot
        // go, and the next copies the table once.
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE nokey (id integer, v text)", token);
        }

        _ = await provider.SafePsqlAsync(Database, "INSERT INTO nokey SELECT g, 'copied' FROM generate_series(1, 100) g", token);
        await using NpgsqlConnection blocker = await provider.OpenAsync(Database, token);
        await using NpgsqlConnection reader = await provider.OpenAsync(Database, token);
        await using NpgsqlCommand peek = reader.CreateCommand();
        await using (NpgsqlConnection tableLocker = await subscriber.OpenAsync(Database, token))
        await using (NpgsqlConnection statusLocker = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction tableLock = await tableLocker.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE nokey", tableLocker, tableLock))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database,
                "SELECT * FROM worf.replication_set_add_table('default_insert_only', 'nokey', synchronize_data := true)", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'nokey'::regclass AND NOT granted)", token), "the copy of nokey waiting");
            await using NpgsqlTransaction subscriptions = await blocker.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE worf.subscription", blocker, subscriptions))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database,
                "SELECT pg_replication_origin_session_setup('elsewhere'); INSERT INTO elsewhere VALUES (1)", token);
            string syncSlot = await provider.SafePsqlAsync(Database,
                $"SELECT slot_name FROM pg_replication_slots WHERE starts_with(slot_name, '{slot}_')", token);
            peek.CommandText = $"""
                SELECT count(*) FROM pg_logical_slot_peek_binary_changes('{syncSlot}', NULL, NULL, 'startup_params_format', '1',
                    'min_proto_version', '1', 'max_proto_version', '1', 'worf.forward_origins', 'all')
                """;
            Task<object?> reading = peek.ExecuteScalarAsync(token);
            Assert.IsTrue(await provider.PollQueryUntilAsync(Database, $"""
                SELECT EXISTS (SELECT 1 FROM pg_replication_slots s JOIN pg_stat_activity a ON a.pid = s.active_pid
                    WHERE s.slot_name = '{syncSlot}' AND a.pid = {reader.ProcessID} AND a.wait_event_type = 'Lock')
                """, token), "the session reading the slot waiting");

            await using NpgsqlTransaction statusLock = await statusLocker.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("SELECT 1 FROM worf.local_sync_status WHERE sync_relname = 'nokey' FOR UPDATE",
                statusLocker, statusLock))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            int logpos = subscriber.ReadLog().Length;
            await tableLock.RollbackAsync(token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"""
                SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE backend_type LIKE 'worf sync%' AND wait_event = 'transactionid')
                """, token), "the copy waiting for its status");
            _ = await subscriber.SafePsqlAsync(Database, $"SELECT pg_cancel_backend(pid) FROM ({SyncWorkers}) w", token);
            Assert.IsTrue(await subscriber.PollLogAsync(logpos, "which this sync's cleanup waits for", token), "the cleanup's warning");
            _ = await provider.SafePsqlAsync(Database, $"SELECT pg_cancel_backend({reader.ProcessID})", token);
            _ = await Assert.ThrowsExactlyAsync<PostgresException>(() => reading);
            await statusLock.RollbackAsync(token);
            await subscriptions.CommitAsync(token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'nokey'", token), "nokey synchronized");
        Assert.AreEqual("100|100", await subscriber.SafePsqlAsync(Database, "SELECT count(*), count(DISTINCT id) FROM nokey", token),
            "nokey's rows");
        Assert.AreEqual("0", await provider.SafePsqlAsync(Database,
            $"SELECT count(*) FROM pg_replication_slots WHERE starts_with(slot_name, '{slot}_')", token), "the syncs' slots left");

        // Dropping the subscription drops the slots its table syncs left, as one stopped before it caught up leaves, but
        // not another subscription's own, whose name may look like one, as that of a subscription named sub_20261101 does.
        _ = await provider.SafePsqlAsync(Database, $"""
            SELECT pg_create_logical_replication_slot('{slot}_' || lpad(to_hex(hashtext('public.nokey')), 8, '0'), 'worf');
            SELECT pg_create_logical_replication_slot('{slot}_20261101', 'worf');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.drop_subscription('sub')", token);
        Assert.AreEqual($"{slot}_20261101", await provider.SafePsqlAsync(Database,
            $"SELECT string_agg(slot_name, ',') FROM pg_replication_slots WHERE starts_with(slot_name, '{slot}')", token), "the slots left after the drop");
    }

    /// <summary>
    /// Counts a table's rows over and over until cancelled, and returns the fewest seen.
    /// </summary>
    private static async Task<long> FewestRowsAsync(TestNode node, string table, CancellationToken token)
    {
        long fewest = long.MaxValue;
        await using NpgsqlConnection connection = await node.OpenAsync(Database, CancellationToken.None);
        await using NpgsqlCommand count = new($"SELECT count(*) FROM {table}", connection);
        while (!token.IsCancellationRequested)
        {
            fewest = Math.Min(fewest, (long)(await count.ExecuteScalarAsync(CancellationToken.None))!);
        }

        return fewest;
    }

    /// <summary>
    /// Has PostgreSQL invalidate a table sync's slot, which no process holds, for <c>max_slot_wal_keep_size</c>, once
    /// the subscription's own slot has moved on to a WAL segment the checkpoint keeps.
    /// </summary>
    private static async Task InvalidateSlotAsync(TestNode provider, string slot, string syncSlot, CancellationToken token)
    {
        string switched = await provider.SafePsqlAsync(Database, "SELECT pg_switch_wal()", token);
        _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO x DEFAULT VALUES", token);
        Assert.IsTrue(await provider.PollQueryUntilAsync(Database,
            $"SELECT restart_lsn > '{switched}' FROM pg_replication_slots WHERE slot_name = '{slot}'", token), "the subscription's slot moved on");
        _ = await provider.SafePsqlAsync(Database, "ALTER SYSTEM SET max_slot_wal_keep_size = 0", token);
        _ = await provider.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        string status = "";
        for (int attempt = 0; attempt < 50 && status != "lost"; attempt++)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100), token);
            _ = await provider.SafePsqlAsync(Database, "CHECKPOINT", token);
            status = await provider.SafePsqlAsync(Database, $"SELECT wal_status FROM pg_replication_slots WHERE slot_name = '{syncSlot}'", token);
        }

        _ = await provider.SafePsqlAsync(Database, "ALTER SYSTEM RESET max_slot_wal_keep_size", token);
        _ = await provider.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        Assert.AreEqual("lost", status, "the sync's slot invalidated");
        Assert.AreEqual("f", await provider.SafePsqlAsync(Database,
            $"SELECT wal_status = 'lost' FROM pg_replication_slots WHERE slot_name = '{slot}'", token), "the subscription's slot lost");
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

    private static async Task SubscribeAsync(TestNode provider, TestNode subscriber, CancellationToken token,
        string forwardOrigins = "{all}")
    {
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := false,
                forward_origins := '{forwardOrigins}')
            """, token);
        await WaitForStatusAsync(subscriber, "replicating", token);
    }

    /// <summary>
    /// Sets the subscriber's behaviour, which its running apply worker reads when the configuration reloads, before
    /// the next transaction it receives.
    /// </summary>
    private static async Task SetBehaviourAsync(TestNode subscriber, string behaviour, CancellationToken token)
    {
        _ = await subscriber.SafePsqlAsync(Database, $"ALTER SYSTEM SET worf.exception_behaviour = '{behaviour}'", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        await Task.Delay(TimeSpan.FromSeconds(1), token);
    }

    private static async Task WaitForStatusAsync(TestNode subscriber, string status, CancellationToken token)
        => Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"""
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'sub' AND status = '{status}')
            """, token), $"subscription failed to reach \"{status}\" state");

    private static Task<string> LastLogAsync(TestNode subscriber, CancellationToken token)
        => subscriber.SafePsqlAsync(Database, """
            SELECT behaviour, operation, schema_name, table_name, remote_new_tuple->>'id', remote_new_tuple->>'v',
                error_sqlstate
            FROM worf.exception_log ORDER BY log_id DESC LIMIT 1
            """, token);

    // How far a table's copy says apply has got.
    [GeneratedRegex("waits to copy until subscription sub applies up to [0-9A-F/]+, and it's at ([0-9A-F]+/[0-9A-F]+)")]
    private static partial Regex AppliedPosition();
}
