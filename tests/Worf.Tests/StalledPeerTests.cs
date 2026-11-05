using Npgsql;

namespace Worf.Tests;

/// <summary>
/// Waits on a provider that stops answering, or on a postmaster that doesn't start a worker, end, as PostgreSQL's own
/// apply worker and launcher bound them, and replication goes on once what held it does.
/// </summary>
[TestClass]
public sealed class StalledPeerTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Apply asks a provider that sends nothing for a reply once half of wal_receiver_timeout has passed, and stops
    /// once all of it has, as PostgreSQL's apply worker does. A call it makes to its provider as it starts, here a
    /// connection that a stopped postmaster never accepts, gives up after wal_receiver_timeout and names what it waited
    /// for. The manager gives up on a worker that the postmaster hasn't started after wal_receiver_timeout, with a
    /// warning, rather than wait for it while every other worker of the database waits on the manager. Each
    /// subscription replicates again once the process goes on. Processes are stopped with signals, which Windows lacks.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    [OSCondition(OperatingSystems.Linux | OperatingSystems.OSX)]
    public async Task StalledProvidersAndWorkerStartsTimeOut()
    {
        CancellationToken token = TestContext.CancellationToken;
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := false)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database,
            "SELECT EXISTS (SELECT FROM worf.show_subscription_status() WHERE status = 'replicating')", token), "the subscription replicating");
        await AppliesAsync(provider, subscriber, 1, token);
        string slot = await subscriber.SafePsqlAsync(Database, "SELECT sub_slot_name FROM worf.subscription", token);

        // The provider's walsender stops: apply's request for a reply goes unanswered, and it stops.
        int logpos = subscriber.ReadLog().Length;
        string walsender = await provider.SafePsqlAsync(Database, $"SELECT active_pid FROM pg_replication_slots WHERE slot_name = '{slot}'", token);
        await SignalAsync("-STOP", walsender, token);
        try
        {
            Assert.IsTrue(await subscriber.PollLogAsync(logpos,
                "terminating worf apply worker for subscription \"sub\" due to timeout", token), "apply stopping for the silent provider");
        }
        finally
        {
            await SignalAsync("-CONT", walsender, token);
        }

        await AppliesAsync(provider, subscriber, 2, token);

        // The provider's postmaster stops: the restarted apply worker's connection, which the kernel accepts, gets no
        // answer.
        logpos = subscriber.ReadLog().Length;
        string providerPostmaster = Postmaster(provider);
        await SignalAsync("-STOP", providerPostmaster, token);
        try
        {
            _ = await subscriber.SafePsqlAsync(Database,
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type LIKE 'worf apply%'", token);
            Assert.IsTrue(await subscriber.PollLogAsync(logpos,
                "timed out waiting for provider \"provider\" to accept the replication connection", token), "apply giving up on the connection");
        }
        finally
        {
            await SignalAsync("-CONT", providerPostmaster, token);
        }

        await AppliesAsync(provider, subscriber, 3, token);

        // The subscriber's postmaster stops: the manager can't start the apply worker of the subscription enabled again,
        // nor the exchange worker it starts from time to time, and gives up on the first it waits for. Only a session that
        // was open before can reach the server meanwhile.
        logpos = subscriber.ReadLog().Length;
        string subscriberPostmaster = Postmaster(subscriber);
        await using NpgsqlConnection session = await subscriber.OpenAsync(Database, token);
        await SignalAsync("-STOP", subscriberPostmaster, token);
        try
        {
            foreach (string sql in new[] { "SELECT worf.alter_subscription_disable('sub')", "SELECT worf.alter_subscription_enable('sub')" })
            {
                await using NpgsqlCommand command = new(sql, session);
                _ = await command.ExecuteScalarAsync(token);
            }

            Assert.IsTrue(await subscriber.PollLogAsync(logpos, "took too long to start; canceled", token), "the manager giving up on the worker");
        }
        finally
        {
            await SignalAsync("-CONT", subscriberPostmaster, token);
        }

        await AppliesAsync(provider, subscriber, 4, token);
    }

    private static async Task AppliesAsync(TestNode provider, TestNode subscriber, int id, CancellationToken token)
    {
        _ = await provider.SafePsqlAsync(Database, $"INSERT INTO t VALUES ({id})", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, $"SELECT EXISTS (SELECT FROM t WHERE id = {id})", token), $"row {id}");
    }

    private static string Postmaster(TestNode node) => File.ReadLines(Path.Join(node.Cluster.DataDirectory, "postmaster.pid")).First().Trim();

    private static async Task SignalAsync(string signal, string pid, CancellationToken token)
    {
        (int exitCode, _, string error) = await ProcessRunner.RunAsync("kill", [signal, pid], token);
        Assert.AreEqual(0, exitCode, error);
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "wal_receiver_timeout = '2s'",
        ], token);
        try
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER", token);
            _ = await node.SafePsqlAsync(Database, "CREATE EXTENSION worf", token);
            _ = await node.SafePsqlAsync(Database, $"SELECT * FROM worf.create_node(node_name := '{name}', dsn := '{node.Dsn(Database, SuperUser)}')", token);
            _ = await node.SafePsqlAsync(Database, """
                CREATE TABLE t (id integer PRIMARY KEY);
                SELECT * FROM worf.replication_set_add_table('default', 't');
                """, token);
        }
        catch
        {
            await node.DisposeAsync();
            throw;
        }

        return node;
    }
}
