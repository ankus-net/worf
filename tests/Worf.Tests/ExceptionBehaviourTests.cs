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
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, v text);
            SELECT * FROM worf.replication_set_add_table('default', 't');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database,
            "CREATE TABLE t (id integer PRIMARY KEY, v text CONSTRAINT not_bad CHECK (v <> 'bad'))", token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := false)
            """, token);
        await WaitForStatusAsync(subscriber, "replicating", token);

        await SetBehaviourAsync(subscriber, "discard", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (1, 'good'), (2, 'bad'), (3, 'good')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 3)", token),
            "the discarded change's transaction");
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

        await SetBehaviourAsync(subscriber, "transdiscard", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (11, 'good'), (12, 'bad'), (13, 'good')", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t VALUES (14, 'good')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM t WHERE id = 14)", token),
            "the transaction after the discarded one");
        Assert.AreEqual("1\n3\n4\n14", await subscriber.SafePsqlAsync(Database, "SELECT id FROM t ORDER BY id", token));
        Assert.AreEqual("transdiscard|INSERT|public|t|12|bad|23514", await LastLogAsync(subscriber, token));

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

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
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

    /// <summary>
    /// Sets the subscriber's behaviour, and restarts its apply worker so the change takes effect before the next
    /// transaction.
    /// </summary>
    private static async Task SetBehaviourAsync(TestNode subscriber, string behaviour, CancellationToken token)
    {
        _ = await subscriber.SafePsqlAsync(Database, $"ALTER SYSTEM SET worf.exception_behaviour = '{behaviour}'", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_disable('sub', true)", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT worf.alter_subscription_enable('sub', true)", token);
        await WaitForStatusAsync(subscriber, "replicating", token);
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
