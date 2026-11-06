using Npgsql;

using System.Diagnostics;
using System.Globalization;
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

        // While the sync waits, VACUUM empties and truncates the pages of rows deleted since: once a moment's transactions
        // of other sessions, which hold the rows too, have ended.
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        _ = await provider.SafePsqlAsync(Database, "CREATE TABLE churn AS SELECT generate_series(1, 1000) AS id", token);
        _ = await provider.SafePsqlAsync(Database, "DELETE FROM churn", token);
        string size = "";
        for (int attempt = 0; attempt < 20 && size != "0"; attempt++)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(attempt == 0 ? 0 : 250), token);
            _ = await provider.SafePsqlAsync(Database, "VACUUM churn", token);
            size = await provider.SafePsqlAsync(Database, "SELECT pg_relation_size('churn')", token);
        }

        Assert.AreEqual("0", size, "the pages VACUUM emptied of the dead rows and truncated");
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

        // The session that read the slot ends, as it has the slot's statistics, which the slot's drop leaves to it.
        await reader.CloseAsync();
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
    /// The apply worker records that a table's sync may catch up without holding the workers' shared lock, so that the
    /// subscriptions' status shows while the record waits on a row lock.
    /// A table sync worker stopped once its copy has committed, here while it waits for the apply worker, which waits on a
    /// lock with changes to the table from after the copy skipped, catches up from its copy when it starts again, with
    /// those changes, and drops its slot over the connection it streamed on, which needs no new connection to the provider.
    /// When its slot is gone or invalidated, or its origin has no progress, it copies the table again and merges the copy
    /// in, in one transaction, so the table is never seen empty, though a key references the table, its own key is an
    /// identity or deferrable: the provider's rows replace those with the same key, and the table's other rows stay, those
    /// written here or under another origin, and those the provider has since deleted. Under transdiscard, as under
    /// discard, a row of the copy that a row here holds a unique value of, or that a CHECK constraint here refuses, is
    /// skipped and recorded under transdiscard, while the others merge and later changes arrive, those whose values rows
    /// the table keeps hold all at once, without a transaction ID each, and a merge of 300,000 rows in halves takes the
    /// sync worker little memory, on Linux, where /proc gives it. One cancelled before its copy
    /// commits copies a table without a key once, and the cleanup after it waits for a session that holds its slot, with a
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

        // So does a table renamed, with another created under its name, which the change applies to.
        await using (NpgsqlConnection renaming = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction rename = await renaming.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE x", renaming, rename))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database, "INSERT INTO x DEFAULT VALUES", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'x'::regclass AND NOT granted)", token), "the apply worker waiting for x");
            await using (NpgsqlCommand command = new("ALTER TABLE x RENAME TO x_old; CREATE TABLE x (id serial PRIMARY KEY)", renaming, rename))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            await rename.CommitAsync(token);
        }

        _ = await provider.SafePsqlAsync(Database, "SELECT worf.wait_slot_confirm_lsn(NULL, NULL)", token);
        Assert.AreEqual("2|1", await subscriber.SafePsqlAsync(Database, "SELECT (SELECT count(*) FROM x_old), (SELECT count(*) FROM x)", token),
            "the rows of the renamed x and the new x");

        // The apply worker records that a sync may catch up with the workers' shared lock released, so that while the
        // record waits on a lock, the subscriptions' status shows, and the other workers go on, rather than wait on the
        // lock, which nothing can cancel.
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, "CREATE TABLE status_wait (id integer PRIMARY KEY)", token);
        }

        _ = await provider.SafePsqlAsync(Database, "INSERT INTO status_wait VALUES (1)", token);
        await using (NpgsqlConnection tableLocker = await subscriber.OpenAsync(Database, token))
        await using (NpgsqlConnection applyLocker = await subscriber.OpenAsync(Database, token))
        await using (NpgsqlConnection statusLocker = await subscriber.OpenAsync(Database, token))
        await using (NpgsqlConnection reading = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction tableLock = await tableLocker.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("INSERT INTO status_wait VALUES (1)", tableLocker, tableLock))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'status_wait', synchronize_data := true)", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
                SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                    WHERE l.locktype = 'transactionid' AND NOT l.granted AND a.backend_type LIKE 'worf sync%')
                """, token), "status_wait's copy waiting");
            await using NpgsqlTransaction applyLock = await applyLocker.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE x", applyLocker, applyLock))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database, "INSERT INTO x DEFAULT VALUES", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = 'x'::regclass AND NOT granted)", token), "the apply worker waiting for x");
            await tableLock.RollbackAsync(token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM worf.local_sync_status WHERE sync_relname = 'status_wait' AND sync_status = 'w')", token),
                "status_wait's sync waiting for the apply worker");
            await using NpgsqlTransaction statusLock = await statusLocker.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("SELECT FROM worf.local_sync_status WHERE sync_relname = 'status_wait' FOR UPDATE", statusLocker,
                statusLock))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            await applyLock.RollbackAsync(token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
                SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                    WHERE l.locktype = 'transactionid' AND NOT l.granted AND a.backend_type LIKE 'worf apply%')
                """, token), "the apply worker's record waiting");
            await using NpgsqlCommand read = new("SELECT string_agg(status, ',') FROM worf.show_subscription_status()", reading);
            Task<object?> status = read.ExecuteScalarAsync(token);
            bool shown = await Task.WhenAny(status, Task.Delay(TimeSpan.FromSeconds(5), token)) == status;
            await statusLock.RollbackAsync(token);
            Assert.IsTrue(shown, "the status shown while the record waits");
            Assert.AreEqual("replicating", await status);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'status_wait'", token), "status_wait synchronized");
        Assert.AreEqual("1", await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM status_wait", token), "status_wait's row");

        long mergeFrom = 0;
        foreach (string stop in new[] { "resume", "drop", "zero", "lost", "conflict", "halves", "raises" })
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
            if (stop == "conflict")
            {
                _ = await provider.SafePsqlAsync(Database,
                    $"ALTER TABLE {table} ADD COLUMN u integer, ADD COLUMN k integer, ADD COLUMN j integer, ADD COLUMN r int4range", token);
                _ = await subscriber.SafePsqlAsync(Database, $"""
                    ALTER TABLE {table} ADD COLUMN u integer, ADD COLUMN k integer UNIQUE, ADD COLUMN j integer UNIQUE, ADD COLUMN r int4range,
                        ADD CONSTRAINT {table}_r EXCLUDE USING gist (r WITH &&);
                    CREATE UNIQUE INDEX {table}_u_key ON {table} (u) WHERE u > 0;
                    CREATE UNIQUE INDEX {table}_v ON {table} (v) WHERE v <> 'copied';
                    ALTER TABLE {table} ADD CHECK (id <> 700000);
                    """, token);
            }
            else if (stop == "raises")
            {
                _ = await subscriber.SafePsqlAsync(Database, $"""
                    CREATE FUNCTION {table}_fill() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN
                            IF NEW.id = 700000 THEN
                                RAISE EXCEPTION 'refused %', NEW.id;
                            END IF;
                            NEW.v := coalesce(NEW.v, 'filled');
                            RETURN NEW;
                        END $$;
                    CREATE TRIGGER {table}_fill BEFORE INSERT OR UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION {table}_fill();
                    ALTER TABLE {table} ENABLE ALWAYS TRIGGER {table}_fill;
                    ALTER TABLE {table} ALTER COLUMN v SET NOT NULL;
                    """, token);
            }
            else if (stop == "halves")
            {
                _ = await subscriber.SafePsqlAsync(Database, $"CREATE UNIQUE INDEX {table}_v ON {table} (v) WHERE v <> 'copied'", token);
            }

            // A key on the subscriber references the table.
            _ = await subscriber.SafePsqlAsync(Database, $"CREATE TABLE {table}_ref (id integer REFERENCES {table} (id))", token);

            _ = await provider.SafePsqlAsync(Database, $"INSERT INTO {table} SELECT g, 'copied' FROM generate_series(1, {(stop == "halves" ? 300000 : 1000)}) g", token);
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
                if (stop is "conflict" or "halves")
                {
                    _ = await subscriber.SafePsqlAsync(Database, $"INSERT INTO {table} VALUES (900003, 'updated')", token);
                }

                if (stop == "halves")
                {
                    _ = await subscriber.SafePsqlAsync(Database, $"UPDATE {table} SET v = 's' || id WHERE id IN (100, 200)", token);
                }

                // The next worker catches up while the table is locked, and can't connect to the provider again once it's
                // done, which it doesn't need to, as it drops its slot over the connection it streamed on.
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
                    Assert.IsTrue(await provider.PollQueryUntilAsync(Database,
                        $"SELECT NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE starts_with(slot_name, '{slot}_'))", token),
                        "the slot the sync dropped over its stream's connection");
                    Assert.DoesNotContain("could not drop replication slot", subscriber.ReadLog()[logpos..], "a slot left to the apply worker");
                    _ = await provider.SafePsqlAsync(Database, $"ALTER ROLE {SuperUser} LOGIN", token);
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
                case "conflict":
                    // The copy again merges row 500's new value, which a row here holds, and row 700000, which a CHECK
                    // constraint here refuses, so under transdiscard, as under discard, those rows are skipped, and
                    // recorded under transdiscard, while the others merge. Rows 1 to 300 take values of u that rows here
                    // the provider doesn't have hold in a partial unique index, and are skipped at once, without a
                    // transaction ID each, and so are row 402, whose j row 401 takes, while row 403's k, which row 402
                    // would have taken, is free, and rows 404 and 406, whose ranges a row here and row 405 overlap.
                    _ = await provider.SafePsqlAsync(Database, $"""
                        UPDATE {table} SET u = id + 2000 WHERE id <= 300;
                        UPDATE {table} SET k = CASE id WHEN 401 THEN 1 ELSE 2 END, j = CASE id WHEN 403 THEN 2 ELSE 1 END WHERE id IN (401, 402, 403);
                        UPDATE {table} SET r = CASE id WHEN 404 THEN int4range(5, 15) WHEN 405 THEN int4range(20, 30) ELSE int4range(25, 35) END
                            WHERE id IN (404, 405, 406);
                        SELECT pg_drop_replication_slot('{syncSlot}');
                        """, token);
                    _ = await subscriber.SafePsqlAsync(Database, $"""
                        INSERT INTO {table} SELECT g, 'held ' || g, g - 798000 FROM generate_series(800001, 800300) g;
                        INSERT INTO {table} (id, v, r) VALUES (800400, 'range', int4range(1, 10));
                        """, token);
                    await SetBehaviourAsync(subscriber, "transdiscard", token);
                    mergeFrom = long.Parse(await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text", token),
                        CultureInfo.InvariantCulture);
                    break;
                case "halves":
                    // The copy again merges 300,000 rows. Rows 100 and 200 swap the values rows here hold in a partial
                    // unique index, which only merging in halves finds, and both fail: the halves are ranges of the copy's
                    // rows, which take no memory however many rows they hold. Row 500's new value, held by a row here in
                    // that index, and the 300 rows the provider changed that a CHECK constraint here refuses are skipped
                    // at once, without a transaction ID each, while row 12345, which another refuses, as neither is
                    // valid, stays as it is here, as the merge doesn't write it.
                    _ = await provider.SafePsqlAsync(Database, $"""
                        UPDATE {table} SET v = 'changed' WHERE id % 1000 = 7;
                        UPDATE {table} SET v = CASE id WHEN 100 THEN 's200' ELSE 's100' END WHERE id IN (100, 200);
                        SELECT pg_drop_replication_slot('{syncSlot}');
                        """, token);
                    _ = await subscriber.SafePsqlAsync(Database, $"""
                        ALTER TABLE {table} ADD CONSTRAINT {table}_changed CHECK (v <> 'changed') NOT VALID;
                        ALTER TABLE {table} ADD CONSTRAINT {table}_12345 CHECK (id <> 12345) NOT VALID;
                        """, token);
                    await SetBehaviourAsync(subscriber, "transdiscard", token);
                    mergeFrom = long.Parse(await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text", token),
                        CultureInfo.InvariantCulture);
                    break;
                case "raises":
                    // The copy again merges row 7, whose v a trigger here fills in, which a NOT NULL check of the copy's
                    // rows would have refused, and row 700000, which the trigger refuses with an error of its own, which
                    // merging in halves finds, skips and records under transdiscard, while the others merge.
                    _ = await provider.SafePsqlAsync(Database, $"""
                        UPDATE {table} SET v = NULL WHERE id = 7;
                        SELECT pg_drop_replication_slot('{syncSlot}');
                        """, token);
                    await SetBehaviourAsync(subscriber, "transdiscard", token);
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
            Task<long> peakKb = Task.FromResult(0L);
            if (stop != "resume")
            {
                fewest = FewestRowsAsync(subscriber, table, counting.Token);
                if (stop == "halves" && OperatingSystem.IsLinux())
                {
                    peakKb = PeakSyncMemoryAsync(subscriber, counting.Token);
                }

                _ = await provider.SafePsqlAsync(Database, $"ALTER ROLE {SuperUser} LOGIN", token);
            }

            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
                $"SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = '{table}'", token), $"the table synchronized after {stop}");
            await counting.CancelAsync();
            Assert.IsGreaterThanOrEqualTo(1000L, await fewest, $"the fewest rows seen during the sync after {stop}");
            Assert.AreEqual(stop switch
            {
                "resume" => "1000|500500|updated|f|t|here|elsewhere|1",
                "conflict" or "raises" => $"1000|-198900|{(stop == "raises" ? "updated" : "copied")}|t|f|here|elsewhere|1",
                "halves" => "300001|45000150600|copied|t|t|here|elsewhere|1",
                _ => "1001|501100|updated|t|t|here|elsewhere|1",
            },
                await subscriber.SafePsqlAsync(Database, $"""
                SELECT count(*), sum(id) - 700000 + 600, (SELECT v FROM {table} WHERE id = 500),
                    EXISTS (SELECT 1 FROM {table} WHERE id = 600), EXISTS (SELECT 1 FROM {table} WHERE id = 700000),
                    (SELECT v FROM {table} WHERE id = 900001), (SELECT v FROM {table} WHERE id = 900002),
                    (SELECT count(*) FROM {table}_ref)
                FROM {table} WHERE id < 800000
                """, token), $"the rows after {stop}");
            Assert.Contains(stop == "resume" ? $"resuming the catch-up of table public.{table}" : $"copying table public.{table} again",
                subscriber.ReadLog()[logpos..], $"the sync after {stop}");
            if (stop == "conflict")
            {
                long merged = long.Parse(await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text", token),
                    CultureInfo.InvariantCulture) - mergeFrom;
                Assert.Contains($"skipped 305 of the rows of the copy of table public.{table}", subscriber.ReadLog()[logpos..]);
                Assert.AreEqual(string.Join('\n',
                    $"MERGE|402|23505|duplicate key value violates unique constraint \"{table}_j_key\"|transdiscard",
                    $"MERGE|404|23P01|conflicting key value violates exclusion constraint \"{table}_r\"|transdiscard",
                    $"MERGE|406|23P01|conflicting key value violates exclusion constraint \"{table}_r\"|transdiscard",
                    $"MERGE|500|23505|duplicate key value violates unique constraint \"{table}_v\"|transdiscard",
                    $"MERGE|700000|23514|new row for relation \"{table}\" violates check constraint \"{table}_id_check\"|transdiscard"),
                    await subscriber.SafePsqlAsync(Database, $"""
                    SELECT operation, remote_new_tuple->>'id', error_sqlstate, error_message, behaviour FROM worf.exception_log
                    WHERE table_name = '{table}' AND remote_new_tuple->>'u' IS NULL ORDER BY (remote_new_tuple->>'id')::integer
                    """, token), "the skipped rows recorded");
                Assert.AreEqual("401|1|1,402,403|2|2,404,405|[20,30),406", await subscriber.SafePsqlAsync(Database, $"""
                    SELECT string_agg(concat_ws('|', id, coalesce(k::text, r::text), j), ',' ORDER BY id) FROM {table} WHERE id BETWEEN 401 AND 406
                    """, token), "the rows of the copy that merged around those skipped");
                Assert.AreEqual($"300|300|300|duplicate key value violates unique constraint \"{table}_u_key\"", await subscriber.SafePsqlAsync(Database, $"""
                    SELECT count(*), (SELECT count(*) FROM {table} WHERE id <= 300 AND u IS NULL), (SELECT count(*) FROM {table} WHERE v LIKE 'held %'),
                        string_agg(DISTINCT error_message, ',')
                    FROM worf.exception_log WHERE table_name = '{table}' AND (remote_new_tuple->>'u')::integer > 2000
                    """, token), "the rows whose values rows here held, recorded, and the rows they'd have replaced and the rows that held them");
                Assert.IsLessThan(150L, merged, "transaction IDs the merge took for 300 rows whose values rows here held");

                // The changes after the copy still arrive.
                _ = await provider.SafePsqlAsync(Database, $"UPDATE {table} SET v = 'later' WHERE id = 501", token);
                Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"SELECT v = 'later' FROM {table} WHERE id = 501", token),
                    "a change after the copy");
                await SetBehaviourAsync(subscriber, "error", token);
            }

            if (stop == "halves")
            {
                long merged = long.Parse(await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text", token),
                    CultureInfo.InvariantCulture) - mergeFrom;
                Assert.Contains($"skipped 303 of the rows of the copy of table public.{table}", subscriber.ReadLog()[logpos..]);
                Assert.AreEqual("100|200|500|s100|s200", await subscriber.SafePsqlAsync(Database, $"""
                    SELECT string_agg(remote_new_tuple->>'id', '|' ORDER BY (remote_new_tuple->>'id')::integer),
                        (SELECT v FROM {table} WHERE id = 100), (SELECT v FROM {table} WHERE id = 200)
                    FROM worf.exception_log WHERE table_name = '{table}' AND error_sqlstate = '23505'
                    """, token), "the rows that swap values and row 500, recorded, and the values the swapping rows keep");
                Assert.AreEqual($"300|0|1|new row for relation \"{table}\" violates check constraint \"{table}_changed\"", await subscriber.SafePsqlAsync(Database, $"""
                    SELECT count(*), (SELECT count(*) FROM {table} WHERE v = 'changed'), (SELECT count(*) FROM {table} WHERE id = 12345),
                        string_agg(DISTINCT error_message, ',')
                    FROM worf.exception_log WHERE table_name = '{table}' AND error_sqlstate = '23514'
                    """, token), "the changed rows the CHECK refused, recorded and left as they were, and row 12345, left alone");
                Assert.IsLessThan(150L, merged, "transaction IDs the merge took for 300 rows a CHECK refused");
                if (OperatingSystem.IsLinux())
                {
                    long peak = await peakKb;
                    TestContext.WriteLine($"sync worker private memory merging in halves: {peak / 1024} MB");
                    Assert.IsLessThan(80L * 1024, peak, "the sync worker's private memory merging 300,000 rows in halves, in kB");
                }

                await SetBehaviourAsync(subscriber, "error", token);
            }

            if (stop == "raises")
            {
                Assert.AreEqual("filled|MERGE|700000|P0001|refused 700000|string|t", await subscriber.SafePsqlAsync(Database, $"""
                    SELECT (SELECT v FROM {table} WHERE id = 7), operation, remote_new_tuple->>'id', error_sqlstate, error_message,
                        jsonb_typeof(remote_new_tuple->'id'), action_counter > 0
                    FROM worf.exception_log WHERE table_name = '{table}'
                    """, token), "the row the trigger filled in, and the one it refused, recorded with its columns' text and its place in the copy");
                await SetBehaviourAsync(subscriber, "error", token);
            }
        }

        // Rows a merge skips are recorded with their columns' text, as rows of a copy are, and their places in the copy.
        Assert.AreEqual("0", await subscriber.SafePsqlAsync(Database, """
            SELECT count(*) FROM worf.exception_log
            WHERE operation = 'MERGE' AND (jsonb_typeof(remote_new_tuple->'id') <> 'string' OR action_counter <= 0)
            """, token), "merged rows recorded otherwise");

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

            // The session that read the slot ends, as it has the slot's statistics, which the slot's drop leaves to it.
            await reader.CloseAsync();
            await statusLock.RollbackAsync(token);
            await subscriptions.CommitAsync(token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'nokey'", token), "nokey synchronized");
        Assert.AreEqual("100|100", await subscriber.SafePsqlAsync(Database, "SELECT count(*), count(DISTINCT id) FROM nokey", token),
            "nokey's rows");
        Assert.AreEqual("0", await provider.SafePsqlAsync(Database,
            $"SELECT count(*) FROM pg_replication_slots WHERE starts_with(slot_name, '{slot}_')", token), "the syncs' slots left");

        // A table sync records its slot as it makes it, so that the apply worker drops it once left, though the table's
        // sync status has gone since, as once the table is dropped here or leaves the subscription.
        _ = await provider.SafePsqlAsync(Database, $"SELECT pg_create_logical_replication_slot('{slot}_deadbeef', 'worf')", token);
        _ = await subscriber.SafePsqlAsync(Database,
            $"INSERT INTO worf.sync_slot SELECT sub_id, '{slot}_deadbeef' FROM worf.subscription WHERE sub_name = 'sub'", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('sub', true)", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('sub', true)", token);
        Assert.IsTrue(await provider.PollQueryUntilAsync(Database,
            $"SELECT NOT EXISTS (SELECT FROM pg_replication_slots WHERE slot_name = '{slot}_deadbeef')", token), "the recorded slot dropped");
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT NOT EXISTS (SELECT FROM worf.sync_slot)", token), "the record forgotten");

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
    /// A table's copy under discard leaves out the rows the subscriber's NOT NULL columns and CHECK constraints refuse,
    /// and those whose values its column types refuse, without a transaction ID each, recording them in order with the
    /// provider's row numbers, while a duplicate key, which only COPY finds, still fails its batch's COPY alone. An error
    /// that ends a copy in batches names the provider's row. Under transdiscard, the rows are recorded as its own. Rows
    /// that a BEFORE ROW trigger fills in, or whose generated column a CHECK names, copy as COPY would copy them. Rows
    /// whose keys the table already holds are left out of COPY and recorded without a transaction ID each too.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task CopiedRowsThatFailTakeNoTransactionIdEach()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE f (id integer PRIMARY KEY, v integer, w text, k integer);
            INSERT INTO f SELECT g, CASE WHEN g % 7 = 0 THEN NULL ELSE g END, CASE WHEN g % 11 = 0 THEN 'long' ELSE 'ok' END,
                CASE WHEN g = 2501 THEN 1 ELSE g END
            FROM generate_series(1, 3000) g;
            SELECT * FROM worf.replication_set_add_all_tables('default', '{public}');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database,
            "CREATE TABLE f (id integer PRIMARY KEY CHECK (id % 5 <> 0), v integer NOT NULL, w varchar(2), k integer UNIQUE)", token);
        await SetBehaviourAsync(subscriber, "discard", token);
        long before = long.Parse(await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text", token),
            CultureInfo.InvariantCulture);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(subscription_name := 'sub', provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false, synchronize_data := true)
            """, token);
        await WaitForStatusAsync(subscriber, "replicating", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT 1 FROM worf.local_sync_status) AND NOT EXISTS (SELECT 1 FROM worf.local_sync_status WHERE sync_status <> 'r')", token),
            "the copy");
        long used = long.Parse(await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text", token),
            CultureInfo.InvariantCulture) - before;

        // The 272 multiples of 11 fail to read, the 390 more of 7 have a null in a NOT NULL column, the 468 more of 5 fail
        // the CHECK, and row 2501 duplicates row 1's key.
        Assert.AreEqual("1131|1869", await subscriber.SafePsqlAsync(Database,
            "SELECT (SELECT count(*) FROM worf.exception_log), (SELECT count(*) FROM f)", token), "the rows recorded and copied");
        Assert.AreEqual("5|23514,7|23502,11|22001,2501|23505", await subscriber.SafePsqlAsync(Database, """
            SELECT string_agg(concat_ws('|', action_counter, error_sqlstate), ',' ORDER BY action_counter)
            FROM worf.exception_log WHERE action_counter IN (5, 7, 11, 2501)
            """, token), "rows recorded with their provider row numbers");
        Assert.AreEqual("t", await subscriber.SafePsqlAsync(Database,
            "SELECT bool_and(a < b) FROM (SELECT action_counter AS a, lead(action_counter) OVER (ORDER BY log_id) AS b FROM worf.exception_log) s WHERE b IS NOT NULL",
            token), "the rows recorded in order");
        Assert.IsLessThan(60L, used, "transaction IDs the copy took for 1,131 failed rows");
        Assert.AreEqual("bigint", await subscriber.SafePsqlAsync(Database,
            "SELECT format_type(atttypid, atttypmod) FROM pg_attribute WHERE attrelid = 'worf.exception_log'::regclass AND attname = 'action_counter'", token),
            "action_counter, which numbers a copy's rows past 2^31");

        // An error that ends a copy in batches, here a cancel while a row waits for a transaction here that holds its key,
        // names the provider's row as COPY's line. Row 2501's key, which only COPY finds taken, starts the batches, and
        // under transdiscard is recorded as transdiscard's.
        await SetBehaviourAsync(subscriber, "transdiscard", token);
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE g (id integer PRIMARY KEY, k integer);
            INSERT INTO g SELECT g, CASE WHEN g = 2501 THEN 1 ELSE g END FROM generate_series(1, 3000) g;
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE g (id integer PRIMARY KEY, k integer UNIQUE)", token);
        int logpos = subscriber.ReadLog().Length;
        await using (NpgsqlConnection holding = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction held = await holding.BeginTransactionAsync(token);
            await using (NpgsqlCommand hold = new("INSERT INTO g VALUES (900000, 2600)", holding, held))
            {
                _ = await hold.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'g', synchronize_data := true)", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
                SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                    WHERE l.locktype = 'transactionid' AND NOT l.granted AND a.backend_type LIKE 'worf sync%')
                """, token), "the copy waiting for row 2600's key");
            _ = await subscriber.SafePsqlAsync(Database, "SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE backend_type LIKE 'worf sync%'", token);
            Assert.IsTrue(await subscriber.PollLogAsync(logpos, "COPY g, line 2600", token), "the cancelled row's line");
            await held.RollbackAsync(token);
        }

        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'g'", token), "g's copy once the key is free");
        Assert.AreEqual("f|discard|1131,g|transdiscard|1", await subscriber.SafePsqlAsync(Database, """
            SELECT string_agg(concat_ws('|', table_name, behaviour, n), ',' ORDER BY table_name)
            FROM (SELECT table_name, behaviour, count(*) AS n FROM worf.exception_log GROUP BY 1, 2) b
            """, token), "the behaviour each copy's rows were recorded under");

        // The checks run before COPY fires BEFORE ROW triggers and computes generated columns, so a row that a trigger
        // that fires under apply's replica role fills in, and one whose generated column a CHECK names, copy as COPY
        // would copy them.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE tr (id integer PRIMARY KEY, v integer);
            CREATE TABLE gen (id integer PRIMARY KEY, v integer);
            INSERT INTO tr SELECT g, CASE WHEN g % 2 = 0 THEN NULL ELSE g END FROM generate_series(1, 10) g;
            INSERT INTO gen SELECT g, g FROM generate_series(1, 10) g;
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE tr (id integer PRIMARY KEY, v integer NOT NULL);
            CREATE FUNCTION tr_fill() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.v := coalesce(NEW.v, 0); RETURN NEW; END $$;
            CREATE TRIGGER tr_fill BEFORE INSERT ON tr FOR EACH ROW EXECUTE FUNCTION tr_fill();
            ALTER TABLE tr ENABLE ALWAYS TRIGGER tr_fill;
            CREATE TABLE gen (id integer PRIMARY KEY, v integer, g integer GENERATED ALWAYS AS (v * 2) STORED CHECK (g IS NOT NULL));
            """, token);
        _ = await provider.SafePsqlAsync(Database, """
            SELECT * FROM worf.replication_set_add_table('default', 'tr', synchronize_data := true);
            SELECT * FROM worf.replication_set_add_table('default', 'gen', synchronize_data := true);
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT count(*) = 2 FROM worf.local_sync_status WHERE sync_relname IN ('tr', 'gen') AND sync_status = 'r'", token), "tr's and gen's copies");
        Assert.AreEqual("10|5|10|0", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM tr), (SELECT count(*) FROM tr WHERE v = 0), (SELECT count(*) FROM gen),
                (SELECT count(*) FROM worf.exception_log WHERE table_name IN ('tr', 'gen'))
            """, token), "the rows the trigger fills and those the generated column's CHECK passes");

        // Rows whose keys the table already holds are left out of their batches' COPY and recorded with the error COPY
        // raises, without a transaction ID each.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE held (id integer PRIMARY KEY, v text);
            INSERT INTO held SELECT g, 'provider' FROM generate_series(1, 3000) g;
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE held (id integer PRIMARY KEY, v text);
            INSERT INTO held SELECT g, 'here' FROM generate_series(2, 3000, 2) g;
            """, token);
        before = long.Parse(await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text", token),
            CultureInfo.InvariantCulture);
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.replication_set_add_table('default', 'held', synchronize_data := true)", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT sync_status = 'r' FROM worf.local_sync_status WHERE sync_relname = 'held'", token), "held's copy");
        used = long.Parse(await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text", token),
            CultureInfo.InvariantCulture) - before;
        Assert.AreEqual("1500|1500|1500|duplicate key value violates unique constraint \"held_pkey\"", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM held WHERE v = 'provider'), (SELECT count(*) FROM held WHERE v = 'here'), count(*),
                string_agg(DISTINCT error_message, ',')
            FROM worf.exception_log WHERE table_name = 'held' AND error_sqlstate = '23505'
            """, token), "the rows copied, those held, and those recorded");
        Assert.IsLessThan(60L, used, "transaction IDs the copy took for 1,500 rows whose keys were held");

        // A row left out for one index holds no keys in another, as rows written one after another would: row 2 takes j
        // from row 1, so row 3's k is free. Rows that a partial unique index or an exclusion constraint refuses, whether
        // a row here or an earlier row of the copy holds their keys, are left out too, without a transaction ID each.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE jk (id integer PRIMARY KEY, k integer, j integer);
            INSERT INTO jk VALUES (1, 1, 1), (2, 2, 1), (3, 2, 2);
            CREATE TABLE px (id integer PRIMARY KEY, k integer, e integer);
            INSERT INTO px SELECT g, CASE WHEN g % 2 = 0 THEN 7 ELSE g END, g FROM generate_series(1, 3000) g;
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE jk (id integer PRIMARY KEY, k integer UNIQUE, j integer UNIQUE);
            CREATE TABLE px (id integer PRIMARY KEY, k integer, e integer);
            CREATE UNIQUE INDEX px_k ON px (k) WHERE k > 0;
            ALTER TABLE px ADD CONSTRAINT px_e EXCLUDE USING btree (e WITH =);
            INSERT INTO px SELECT 100000 + g, NULL, g FROM generate_series(2001, 2500) g;
            """, token);
        before = long.Parse(await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text", token),
            CultureInfo.InvariantCulture);
        _ = await provider.SafePsqlAsync(Database, """
            SELECT * FROM worf.replication_set_add_table('default', 'jk', synchronize_data := true);
            SELECT * FROM worf.replication_set_add_table('default', 'px', synchronize_data := true);
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT count(*) = 2 FROM worf.local_sync_status WHERE sync_relname IN ('jk', 'px') AND sync_status = 'r'", token), "jk's and px's copies");
        used = long.Parse(await subscriber.SafePsqlAsync(Database, "SELECT pg_snapshot_xmax(pg_current_snapshot())::text", token),
            CultureInfo.InvariantCulture) - before;
        Assert.AreEqual("1,3|2|2|duplicate key value violates unique constraint \"jk_j_key\"", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT string_agg(id::text, ',' ORDER BY id) FROM jk), remote_new_tuple->>'id', action_counter, error_message
            FROM worf.exception_log WHERE table_name = 'jk'
            """, token), "jk's rows and the one left out");
        Assert.AreEqual("1250|1500|250|2001", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM px WHERE id < 100000),
                count(*) FILTER (WHERE error_message = 'duplicate key value violates unique constraint "px_k"'),
                count(*) FILTER (WHERE error_message = 'conflicting key value violates exclusion constraint "px_e"'),
                min(action_counter) FILTER (WHERE error_sqlstate = '23P01')
            FROM worf.exception_log WHERE table_name = 'px'
            """, token), "px's rows copied, and those its partial unique index and its exclusion constraint refused");
        Assert.IsLessThan(60L, used, "transaction IDs the copies took for 1,750 rows a partial unique index or an exclusion constraint refused");
    }

    /// <summary>
    /// A copy in which no row fails costs about the same CPU under error and under discard, which copies at once as
    /// error does unless a row fails: under error, which does no more than discard, within a fifth more than discard's,
    /// and under discard within half again of error's. The copy hands COPY every message received rather than one row
    /// at a time, which cost error a third more than discard, and discard reads the rows no differently. On Linux, where /proc gives the
    /// worker's CPU; the copies run at once.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux)]
    public async Task CopiesCostTheSameUnderErrorAndDiscard()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE n (id integer PRIMARY KEY, v integer, w text);
            INSERT INTO n SELECT g, g, 'x' FROM generate_series(1, 1000000) g;
            SELECT * FROM worf.replication_set_add_all_tables('default', '{public}');
            """, token);
        // The two copies run at once, so that whatever else the machine runs weighs on both alike.
        await using TestNode underError = await StartNodeAsync("sub_error", token);
        await using TestNode underDiscard = await StartNodeAsync("sub_discard", token);
        Dictionary<string, double> cpu = [];
        await Task.WhenAll(new[] { ("error", underError), ("discard", underDiscard) }.Select(async copy =>
        {
            (string behaviour, TestNode subscriber) = copy;
            _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE n (id integer PRIMARY KEY, v integer, w text)", token);
            await SetBehaviourAsync(subscriber, behaviour, token);
            (double used, _) = await CopyAsync(provider, subscriber, token, $"sub_{behaviour}");
            lock (cpu)
            {
                cpu[behaviour] = used;
            }

            Assert.AreEqual("1000000", await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM n", token), $"the rows copied under {behaviour}");
        }));

        TestContext.WriteLine($"apply worker CPU: error {cpu["error"]:0.00} s, discard {cpu["discard"]:0.00} s");
        Assert.IsLessThan(cpu["discard"] * 1.2, cpu["error"], "the copy's CPU under error, against discard's");
        Assert.IsLessThan(cpu["error"] * 1.5, cpu["discard"], "the copy's CPU under discard, against error's");
    }

    /// <summary>
    /// A copy under discard of rows wide enough that a thousand of them would take hundreds of megabytes copies them in
    /// runs of 8 MB at most, and records the ones that fail as each run fills, from the run's own buffer, so that the
    /// copying worker's memory stays bounded however wide and many they are: its private memory, which /proc gives on
    /// Linux, stays under 200 MB, where runs of a thousand rows took 3.2 GB here, and failed rows kept as strings until
    /// the collector took them 323 MB.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux)]
    public async Task WideRowsCopyInBoundedMemory()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE w (id integer PRIMARY KEY, v integer, t text);
            INSERT INTO w SELECT g, g, repeat(md5(g::text), 32768) FROM generate_series(1, 400) g;
            SELECT * FROM worf.replication_set_add_all_tables('default', '{public}');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE w (id integer PRIMARY KEY, v integer CHECK (v % 2 = 0), t text)", token);
        await SetBehaviourAsync(subscriber, "discard", token);
        (_, long peakKb) = await CopyAsync(provider, subscriber, token);

        // The odd rows fail the CHECK and are recorded, in order, each with its whole value.
        Assert.AreEqual("200|200|1048576", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM w WHERE v % 2 = 0 AND length(t) = 1048576),
                (SELECT count(*) FROM worf.exception_log WHERE (remote_new_tuple->>'id')::integer % 2 = 1),
                (SELECT min(length(remote_new_tuple->>'t')) FROM worf.exception_log)
            """, token), "the rows copied and recorded");
        Assert.AreEqual("t", await subscriber.SafePsqlAsync(Database,
            "SELECT bool_and(a < b) FROM (SELECT action_counter AS a, lead(action_counter) OVER (ORDER BY log_id) AS b FROM worf.exception_log) s WHERE b IS NOT NULL",
            token), "the rows recorded in order");
        TestContext.WriteLine($"apply worker private memory: {peakKb / 1024} MB");
        Assert.IsLessThan(200L * 1024, peakKb, "the copying worker's private memory, in kB");
    }

    /// <summary>
    /// A row of a copy under discard too large for jsonb, whose values hold at most 2^28 - 1 bytes, is recorded
    /// without its data, with a warning, rather than not at all, and isn't written as JSON at all once it's past what
    /// jsonb holds, so that the copying worker's private memory, which /proc gives on Linux, stays near what copying
    /// the row takes, about 3.2 GB under discard for a value of 270 MB: under 4.6 GB, where the JSON grew to a gigabyte
    /// and the worker to 6 GB.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux)]
    public async Task RowsTooLargeForJsonbAreRecordedWithoutTheirData()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE h (id integer PRIMARY KEY, v integer, t text);
            INSERT INTO h VALUES (1, 1, repeat('x', 270 * 1024 * 1024)), (2, 2, 'small');
            SELECT * FROM worf.replication_set_add_all_tables('default', '{public}');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE h (id integer PRIMARY KEY, v integer CHECK (v % 2 = 0), t text)", token);
        await SetBehaviourAsync(subscriber, "discard", token);
        int logpos = subscriber.ReadLog().Length;
        (_, long peakKb) = await CopyAsync(provider, subscriber, token);

        Assert.AreEqual("1|1|23514|t", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT count(*) FROM h), action_counter, error_sqlstate, remote_new_tuple IS NULL FROM worf.exception_log
            """, token), "the row copied, and the one recorded without its data");
        Assert.Contains("row 1 of the copy of table public.h is too large for worf.exception_log to record its data", subscriber.ReadLog()[logpos..]);
        TestContext.WriteLine($"apply worker private memory: {peakKb / 1024} MB");
        Assert.IsLessThan(4600L * 1024, peakKb, "the copying worker's private memory, in kB");
    }

    /// <summary>
    /// Subscribes with the table's data and waits for the copy, which the apply worker makes, taking the CPU it used and
    /// the most private memory it held meanwhile.
    /// </summary>
    private static async Task<(double Cpu, long PeakKb)> CopyAsync(TestNode provider, TestNode subscriber, CancellationToken token, string name = "sub")
    {
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(subscription_name := '{name}', provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false, synchronize_data := true)
            """, token);
        double cpu = 0;
        long peakKb = 0;
        Stopwatch waited = Stopwatch.StartNew();
        while (waited.Elapsed < TestNode.PollTimeout)
        {
            string pid = await subscriber.SafePsqlAsync(Database, "SELECT pid FROM pg_stat_activity WHERE backend_type LIKE 'worf apply%'", token);
            if (int.TryParse(pid, out int apply))
            {
                (double Cpu, long RssAnonKb)? process = ReadProcess(apply);
                if (process is (double used, long rss))
                {
                    cpu = Math.Max(cpu, used);
                    peakKb = Math.Max(peakKb, rss);
                }
            }

            if (await subscriber.SafePsqlAsync(Database,
                "SELECT EXISTS (SELECT 1 FROM worf.local_sync_status) AND NOT EXISTS (SELECT 1 FROM worf.local_sync_status WHERE sync_status <> 'r')",
                token) == "t")
            {
                return (cpu, peakKb);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(50), token);
        }

        Assert.Fail("the copy didn't finish");
        return default;
    }

    /// <summary>
    /// Watches the private memory of the subscriber's table sync workers until cancelled, and returns its peak, in kB.
    /// </summary>
    private static async Task<long> PeakSyncMemoryAsync(TestNode subscriber, CancellationToken token)
    {
        long peakKb = 0;
        try
        {
            while (true)
            {
                string pids = await subscriber.SafePsqlAsync(Database,
                    "SELECT string_agg(pid::text, ',') FROM pg_stat_activity WHERE backend_type LIKE 'worf sync%'", token);
                foreach ((double _, long rss) in pids.Split(',', StringSplitOptions.RemoveEmptyEntries)
                    .Select(pid => int.TryParse(pid, out int sync) ? ReadProcess(sync) : null).OfType<(double Cpu, long RssAnonKb)>())
                {
                    peakKb = Math.Max(peakKb, rss);
                }

                await Task.Delay(TimeSpan.FromMilliseconds(50), token);
            }
        }
        catch (OperationCanceledException)
        {
            return peakKb;
        }
    }

    /// <summary>
    /// Reads a process's CPU time and private memory from /proc, or null once it has exited.
    /// </summary>
    private static (double Cpu, long RssAnonKb)? ReadProcess(int pid)
    {
        try
        {
            string[] stat = File.ReadAllText($"/proc/{pid}/stat").Split(')')[1].Split(' ', StringSplitOptions.RemoveEmptyEntries);
            double cpu = (double.Parse(stat[11], CultureInfo.InvariantCulture) +
                double.Parse(stat[12], CultureInfo.InvariantCulture)) / 100.0;
            long rss = File.ReadAllLines($"/proc/{pid}/status").Where(line => line.StartsWith("RssAnon:", StringComparison.Ordinal))
                .Select(line => long.Parse(line.Split(' ', StringSplitOptions.RemoveEmptyEntries)[1], CultureInfo.InvariantCulture))
                .FirstOrDefault();
            return (cpu, rss);
        }
        catch (IOException)
        {
            return null;
        }
    }

    /// <summary>
    /// In SQL_ASCII databases, values that hold a byte that isn't UTF-8 copy and replicate byte for byte, inserted or
    /// updated, and through an update that conflicts with a local one, whose record keeps the local row's bytes, and a
    /// row of the copy that fails under discard, with such a value, is recorded with the value's bytes, apart from one
    /// whose text is their hex.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task SqlAsciiValuesKeepTheirBytes()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        const string Ascii = "ascii";
        const string Create = """
            DO $$
            BEGIN
                EXECUTE format('CREATE TABLE %I (id integer PRIMARY KEY CHECK (id < %s), %I text)', 'tab',
                    current_setting('worf_test.max_id'), 'caf');
            END $$;
            """;
        foreach ((TestNode node, string limit) in new[] { (provider, "100"), (subscriber, "5") })
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE DATABASE {Ascii} ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0", token);
            _ = await node.SafePsqlAsync(Ascii, $"""
                CREATE EXTENSION worf;
                SELECT * FROM worf.create_node(node_name := '{node.Name}_ascii', dsn := '{node.Dsn(Ascii, SuperUser)}');
                SET worf_test.max_id = '{limit}';
                {Create}
                """, token);
        }

        _ = await provider.SafePsqlAsync(Ascii, """
            DO $$
            BEGIN
                EXECUTE format('INSERT INTO %I VALUES (1, %L), (7, %L), (8, %L)', 'tab',
                    convert_from('\x6f6e65e9', 'SQL_ASCII'), convert_from('\x736576656ee9', 'SQL_ASCII'), '\x736576656ee9');
            END $$;
            SELECT * FROM worf.replication_set_add_all_tables('default', '{public}');
            """, token);
        _ = await subscriber.SafePsqlAsync(Ascii, "ALTER SYSTEM SET worf.exception_behaviour = 'discard'", token);
        _ = await subscriber.SafePsqlAsync(Ascii, "ALTER SYSTEM SET worf.conflict_history_enabled = on", token);
        _ = await subscriber.SafePsqlAsync(Ascii, "SELECT pg_reload_conf()", token);
        _ = await subscriber.SafePsqlAsync(Ascii, $"""
            SELECT worf.create_subscription(subscription_name := 'sub', provider_dsn := '{provider.Dsn(Ascii, SuperUser)}',
                synchronize_structure := false, synchronize_data := true)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Ascii,
            "SELECT EXISTS (SELECT 1 FROM worf.local_sync_status) AND NOT EXISTS (SELECT 1 FROM worf.local_sync_status WHERE sync_status <> 'r')", token),
            "the copy");
        _ = await provider.SafePsqlAsync(Ascii, """
            DO $$
            BEGIN
                EXECUTE format('INSERT INTO %I VALUES (2, %L)', 'tab', convert_from('\x74776fe9', 'SQL_ASCII'));
            END $$;
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Ascii, "SELECT count(*) = 2 FROM tab", token), "the replicated row");
        const string Bytes = "SELECT string_agg(id || ':' || encode(convert_to(caf, 'SQL_ASCII'), 'hex'), ',' ORDER BY id) FROM tab";
        Assert.AreEqual("1:6f6e65e9,2:74776fe9", await subscriber.SafePsqlAsync(Ascii, Bytes, token), "the rows' bytes");

        // A replicated update of such a row applies, and so does one that conflicts with a local update of another,
        // whose local row the conflict records.
        _ = await subscriber.SafePsqlAsync(Ascii, """
            DO $$
            BEGIN
                EXECUTE format('UPDATE %I SET %I = %L WHERE id = 1', 'tab', 'caf', convert_from('\x6c6f63e9', 'SQL_ASCII'));
            END $$;
            """, token);
        _ = await provider.SafePsqlAsync(Ascii, """
            DO $$
            BEGIN
                EXECUTE format('UPDATE %I SET %I = %L WHERE id = 1', 'tab', 'caf', convert_from('\x6e6577e9', 'SQL_ASCII'));
                EXECUTE format('UPDATE %I SET %I = %L WHERE id = 2', 'tab', 'caf', convert_from('\x74776f32e9', 'SQL_ASCII'));
            END $$;
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Ascii, $"SELECT ({Bytes}) = '1:6e6577e9,2:74776f32e9'", token), "the updated rows' bytes");

        // Apply writes the conflict's record in a transaction of its own once the update's has committed.
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Ascii, "SELECT EXISTS (SELECT 1 FROM worf.conflict_history)", token), "the conflict's record");
        Assert.AreEqual("1|6c6f63e9", await subscriber.SafePsqlAsync(Ascii,
            "SELECT count(*), string_agg(encode(convert_to(local_tuple->>'caf', 'SQL_ASCII'), 'hex'), ',') FROM worf.conflict_history", token),
            "the conflict's local row");
        Assert.AreEqual("COPY|7|23514|736576656ee9,COPY|8|23514|5c78373336353736363536656539", await subscriber.SafePsqlAsync(Ascii, """
            SELECT string_agg(concat_ws('|', operation, remote_new_tuple->>'id', error_sqlstate,
                encode(convert_to(remote_new_tuple->>'caf', 'SQL_ASCII'), 'hex')), ',' ORDER BY remote_new_tuple->>'id')
            FROM worf.exception_log
            """, token), "the copy's failed rows, one with such a value and one whose text is that value's hex");
    }

    /// <summary>
    /// Deferrable unique and exclusion constraints, whose rechecks don't fire under session_replication_role replica
    /// unless worf has them, hold for the rows apply and the copy write: rows that conflict with none apply, and under
    /// discard, a change that conflicts is skipped and recorded, and so is a copied row that conflicts with an earlier
    /// one. Apply waits for a transaction here that wrote a conflicting row, and constraints on expressions and with
    /// predicates hold as well. Rows that swap values, as a statement may under a constraint that is initially
    /// immediate, apply. Under error, a copy that conflicts fails naming the keys.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DeferrableConstraintsHoldForAppliedAndCopiedRows()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE dk (id integer PRIMARY KEY, email text);
            CREATE TABLE dx (id integer PRIMARY KEY, r int4range);
            CREATE TABLE dr (id integer PRIMARY KEY, r int4range);
            CREATE TABLE dd (id integer PRIMARY KEY, email text);
            INSERT INTO dk VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'c');
            INSERT INTO dd VALUES (1, 'd'), (2, 'd');
            INSERT INTO dr VALUES (1, '[1,3)'), (2, '[2,4)'), (3, '[3,5)'), (4, '[10,12)');
            SELECT * FROM worf.replication_set_add_all_tables('default', '{public}');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE dk (id integer PRIMARY KEY, email text UNIQUE DEFERRABLE);
            CREATE TABLE dx (id integer PRIMARY KEY, r int4range, EXCLUDE USING gist (r WITH &&) DEFERRABLE INITIALLY DEFERRED);
            CREATE TABLE dr (id integer PRIMARY KEY, r int4range, EXCLUDE USING gist (r WITH &&) DEFERRABLE);
            CREATE TABLE dd (id integer PRIMARY KEY, email text UNIQUE DEFERRABLE INITIALLY DEFERRED);
            """, token);
        await SetBehaviourAsync(subscriber, "discard", token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(subscription_name := 'sub', provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false, synchronize_data := true)
            """, token);
        await WaitForStatusAsync(subscriber, "replicating", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT NOT EXISTS (SELECT 1 FROM worf.local_sync_status WHERE sync_status <> 'r')", token), "the copy");
        Assert.AreEqual("1|a,2|b,3|c", await subscriber.SafePsqlAsync(Database,
            "SELECT string_agg(concat_ws('|', id, email), ',' ORDER BY id) FROM dk", token), "dk's copy");

        // Of copied rows that conflict, only those that conflict with an earlier row that stays go, as rows checked one
        // after another would: [3,5) conflicts only with [2,4), which goes. A row that goes is recorded with its columns'
        // text, as the copy's other rows are.
        Assert.AreEqual("1,3,4|COPY|2|[2,4)|string", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT string_agg(id::text, ',' ORDER BY id) FROM dr), operation, remote_new_tuple->>'id', remote_new_tuple->>'r',
                jsonb_typeof(remote_new_tuple->'id')
            FROM worf.exception_log WHERE table_name = 'dr'
            """, token), "dr's copy");

        // A conflict in an initially deferred constraint, checked once the table has copied, is deleted and recorded too.
        Assert.AreEqual("1|COPY|2|d|23505", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT string_agg(id::text, ',' ORDER BY id) FROM dd), operation, remote_new_tuple->>'id', remote_new_tuple->>'email', error_sqlstate
            FROM worf.exception_log WHERE table_name = 'dd'
            """, token), "dd's copy");

        _ = await provider.SafePsqlAsync(Database, "INSERT INTO dx SELECT g, int4range(g * 10, g * 10 + 5) FROM generate_series(1, 1000) g", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT count(*) = 1000 FROM dx", token), "dx's rows that conflict with none");
        _ = await provider.SafePsqlAsync(Database, "UPDATE dk SET email = 'a' WHERE id = 2", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO dx VALUES (1001, int4range(10, 20))", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO dk VALUES (5, 'e')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM dk WHERE id = 5)", token),
            "the changes after those that conflict");
        Assert.AreEqual("b|1000", await subscriber.SafePsqlAsync(Database,
            "SELECT (SELECT email FROM dk WHERE id = 2), (SELECT count(*) FROM dx)", token), "the rows the conflicting changes left");
        Assert.AreEqual("COPY|dk|4|23505,UPDATE|dk|2|23505,INSERT|dx|1001|23P01", await subscriber.SafePsqlAsync(Database, """
            SELECT string_agg(concat_ws('|', operation, table_name, remote_new_tuple->>'id', error_sqlstate), ',' ORDER BY log_id)
            FROM worf.exception_log WHERE table_name IN ('dk', 'dx')
            """, token), "the conflicts recorded");

        // A row that a transaction here has written and not yet committed holds its range too: apply's commit waits for
        // the transaction, as PostgreSQL's recheck does, and once it commits, the conflicting transaction is skipped as
        // a whole, as a transaction whose commit fails is.
        await using (NpgsqlConnection local = await subscriber.OpenAsync(Database, token))
        {
            await using NpgsqlTransaction writing = await local.BeginTransactionAsync(token);
            await using (NpgsqlCommand insert = new("INSERT INTO dx VALUES (2000, int4range(50000, 50010))", local, writing))
            {
                _ = await insert.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database, "INSERT INTO dx VALUES (2001, int4range(50005, 50020))", token);
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
                SELECT EXISTS (SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                    WHERE l.locktype = 'transactionid' AND NOT l.granted AND a.backend_type LIKE 'worf apply%')
                """, token), "apply waiting for the transaction that holds the range");
            await writing.CommitAsync(token);
        }

        _ = await provider.SafePsqlAsync(Database, "INSERT INTO dk VALUES (12, 'z')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM dk WHERE id = 12)", token), "the change after it");
        Assert.AreEqual("2000|INSERT|dx|2001|23P01", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT string_agg(id::text, ',') FROM dx WHERE id >= 2000),
                (SELECT string_agg(concat_ws('|', operation, table_name, remote_new_tuple->>'id', error_sqlstate), ',' ORDER BY log_id)
                    FROM worf.exception_log WHERE table_name = 'dx' AND remote_new_tuple->>'id' = '2001')
            """, token), "the row here kept, and the replicated one that conflicts with it recorded");
        _ = await subscriber.SafePsqlAsync(Database, "REINDEX TABLE dx", token);

        // An exclusion constraint on an expression, with a predicate, holds as well, and rows that conflict with none
        // apply.
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, node == provider
                ? "CREATE TABLE dz (id integer PRIMARY KEY, lo integer, hi integer)"
                : "CREATE TABLE dz (id integer PRIMARY KEY, lo integer, hi integer, EXCLUDE USING gist (int4range(lo, hi) WITH &&) WHERE (lo >= 0) DEFERRABLE)",
                token);
        }

        _ = await provider.SafePsqlAsync(Database, """
            SELECT * FROM worf.replication_set_add_table('default', 'dz');
            INSERT INTO dz SELECT g, g * 10, g * 10 + 5 FROM generate_series(1, 1000) g;
            INSERT INTO dz VALUES (1001, -5, 100), (1002, 10, 20);
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT count(*) = 1001 FROM dz", token), "dz's rows that conflict with none");
        Assert.AreEqual("INSERT|dz|1002|23P01", await subscriber.SafePsqlAsync(Database, """
            SELECT string_agg(concat_ws('|', operation, table_name, remote_new_tuple->>'id', error_sqlstate), ',')
            FROM worf.exception_log WHERE table_name = 'dz'
            """, token), "dz's conflict recorded");

        // Rows that swap values, which a deferrable constraint that is initially immediate lets a statement do, as it
        // checks them at the statement's end, apply, under discard and under error, with nothing recorded.
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE sw (id integer PRIMARY KEY, k integer UNIQUE DEFERRABLE);
                CREATE TABLE sr (id integer PRIMARY KEY, r int4range, EXCLUDE USING gist (r WITH &&) DEFERRABLE);
                """, token);
        }

        _ = await provider.SafePsqlAsync(Database, """
            SELECT * FROM worf.replication_set_add_table('default', 'sw');
            SELECT * FROM worf.replication_set_add_table('default', 'sr');
            INSERT INTO sw VALUES (1, 1), (2, 2);
            INSERT INTO sr VALUES (1, '[1,3)'), (2, '[3,5)');
            UPDATE sw SET k = 3 - k;
            UPDATE sr SET r = CASE id WHEN 1 THEN int4range(3, 5) ELSE int4range(1, 3) END;
            INSERT INTO sw VALUES (3, 3);
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM sw WHERE id = 3)", token), "the row after the swaps");
        await SetBehaviourAsync(subscriber, "error", token);
        _ = await provider.SafePsqlAsync(Database, """
            UPDATE sw SET k = 3 - k WHERE id < 3;
            UPDATE sr SET r = CASE id WHEN 1 THEN int4range(1, 3) ELSE int4range(3, 5) END;
            INSERT INTO sw VALUES (4, 4);
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM sw WHERE id = 4)", token), "the row after the swaps back");
        Assert.AreEqual("1=1,2=2,3=3,4=4|1=[1,3),2=[3,5)|0", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT string_agg(id || '=' || k, ',' ORDER BY id) FROM sw), (SELECT string_agg(id || '=' || r, ',' ORDER BY id) FROM sr),
                (SELECT count(*) FROM worf.exception_log WHERE table_name IN ('sw', 'sr'))
            """, token), "the swapped rows, and nothing recorded");

        // Under error, a copy whose rows conflict in a deferrable unique index or exclusion constraint fails with the
        // keys in its detail, as PostgreSQL's recheck names them.
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, node == provider
                ? "CREATE TABLE du (id integer PRIMARY KEY, email text); CREATE TABLE dv (id integer PRIMARY KEY, r int4range)"
                : "CREATE TABLE du (id integer PRIMARY KEY, email text UNIQUE DEFERRABLE); CREATE TABLE dv (id integer PRIMARY KEY, r int4range, EXCLUDE USING gist (r WITH &&) DEFERRABLE)",
                token);
        }

        int logpos = subscriber.ReadLog().Length;
        _ = await provider.SafePsqlAsync(Database, """
            INSERT INTO du VALUES (1, 'u'), (2, 'u');
            INSERT INTO dv VALUES (1, '[1,5)'), (2, '[3,8)');
            SELECT * FROM worf.replication_set_add_table('default', 'du', synchronize_data := true);
            SELECT * FROM worf.replication_set_add_table('default', 'dv', synchronize_data := true);
            """, token);
        Assert.IsTrue(await subscriber.PollLogAsync(logpos, "DETAIL:  Key (email)=(u) already exists.", token), "du's failed copy, naming the key");
        Assert.IsTrue(await subscriber.PollLogAsync(logpos, ") conflicts with existing key (r)=([", token), "dv's failed copy, naming the keys");
        _ = await provider.SafePsqlAsync(Database, """
            SELECT worf.replication_set_remove_table('default', 'du');
            SELECT worf.replication_set_remove_table('default', 'dv');
            """, token);
    }

    /// <summary>
    /// A deferrable constraint's recheck still fires for a row apply writes when the table's relcache entry is built
    /// again as the writer starts, here as the planner folds a column's immutable default that analyzes the table, which
    /// SPI's apply, worf.use_spi, plans for each change: the conflicting change is skipped and recorded under discard.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task DeferrableRechecksOutlastRelcacheRebuilds()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token, "worf.use_spi = on", "worf.conflict_resolution = 'error'");
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE rk (id integer PRIMARY KEY, k integer);
            SELECT * FROM worf.replication_set_add_table('default', 'rk');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE FUNCTION rk_analyze() RETURNS integer LANGUAGE plpgsql AS $$ BEGIN ANALYZE public.rk; RETURN 0; END $$;
            CREATE FUNCTION rk_analyzed() RETURNS integer IMMUTABLE LANGUAGE plpgsql AS $$ BEGIN RETURN rk_analyze(); END $$;
            CREATE TABLE rk (id integer PRIMARY KEY, k integer UNIQUE DEFERRABLE, x integer DEFAULT rk_analyzed());
            INSERT INTO rk VALUES (0, 5, 0);
            """, token);
        await SetBehaviourAsync(subscriber, "discard", token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(subscription_name := 'sub', provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false, synchronize_data := false)
            """, token);
        await WaitForStatusAsync(subscriber, "replicating", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO rk VALUES (1, 5)", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO rk VALUES (2, 6)", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM rk WHERE id = 2)", token), "the change after the conflict");
        Assert.AreEqual("0,2|INSERT|1|23505", await subscriber.SafePsqlAsync(Database, """
            SELECT (SELECT string_agg(id::text, ',' ORDER BY id) FROM rk),
                (SELECT string_agg(concat_ws('|', operation, remote_new_tuple->>'id', error_sqlstate), ',') FROM worf.exception_log)
            """, token), "the conflicting change skipped and recorded");
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
