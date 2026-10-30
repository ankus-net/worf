using Npgsql;

using System.Diagnostics;

namespace Worf.Tests;

/// <summary>
/// <c>worf.exception_behaviour</c> keeps a remote transaction that fails from stopping replication, and records it.
/// </summary>
[TestClass]
public sealed class ExceptionBehaviourTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

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
    /// catching up after its copy discards a failed change as the apply worker does, rather than copying again, though
    /// the walsender it streamed from holds its slot.
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
        await SubscribeAsync(provider, subscriber, token);

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

        // The sync worker's copy waits on a lock here until a change it applies after the copy has committed. Its apply
        // of the bad row waits on an advisory lock, so that it fails only once its walsender holds its slot.
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
        _ = await provider.SafePsqlAsync(Database, "ALTER TABLE synced DROP CONSTRAINT synced_v_check; INSERT INTO synced VALUES (1, 'copied')", token);
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
            Assert.IsTrue(await provider.PollQueryUntilAsync(Database,
                "SELECT count(*) = 2 FROM pg_replication_slots WHERE slot_name LIKE 'worf%'", token), "the sync worker's slot");
            _ = await provider.SafePsqlAsync(Database, "INSERT INTO synced VALUES (2, 'bad'), (3, 'good')", token);

            // Having sent the bad row, the walsender waits for this lock to name the origin of the next transaction
            // from the provider's subscriptions, and holds the sync worker's slot meanwhile.
            await using NpgsqlTransaction subscriptions = await blocker.BeginTransactionAsync(token);
            await using (NpgsqlCommand command = new("LOCK TABLE worf.subscription", blocker, subscriptions))
            {
                _ = await command.ExecuteNonQueryAsync(token);
            }

            _ = await provider.SafePsqlAsync(Database, "SELECT pg_replication_origin_create('elsewhere')", token);
            _ = await provider.SafePsqlAsync(Database,
                "SELECT pg_replication_origin_session_setup('elsewhere'); INSERT INTO synced VALUES (4, 'elsewhere')", token);
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
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM synced WHERE id = 4)", token),
            "the row after the bad one");
        Assert.AreEqual("1\n3\n4", await subscriber.SafePsqlAsync(Database, "SELECT id FROM synced ORDER BY id", token));
        Assert.AreEqual("discard|INSERT|public|synced|2|bad|23514", await LastLogAsync(subscriber, token));
        Assert.AreEqual(1, subscriber.ReadLog().Split("starting sync of table public.synced").Length - 1, "copies of the table");
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

    private static async Task SubscribeAsync(TestNode provider, TestNode subscriber, CancellationToken token)
    {
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := false)
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
}
