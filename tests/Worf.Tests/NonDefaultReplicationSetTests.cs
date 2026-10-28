namespace Worf.Tests;

/// <summary>
/// Port of pglogical's <c>t/020_non_default_replication_set.pl</c>.
/// </summary>
[TestClass]
public sealed class NonDefaultReplicationSetTests
{
    private const string Database = "postgres";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// A table added to a non-default replication set with synchronization doesn't appear on a subscriber that
    /// doesn't have it: its sync worker fails, as pglogical expects.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task TableMissingOnSubscriberIsNotReplicated()
    {
        CancellationToken token = TestContext.CancellationToken;
        string[] configuration =
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
            "DateStyle = 'ISO, DMY'",
            "log_line_prefix = '[%m] [%p] [%d] '",
            "log_statement = 'all'",
            "worf.synchronous_commit = true",
        ];

        await using TestNode provider = await TestNode.StartAsync("provider", configuration, token);
        await using TestNode subscriber = await TestNode.StartAsync("subscriber", configuration, token);
        string providerDsn = provider.Dsn(Database, "super");
        string subscriberDsn = subscriber.Dsn(Database, "super");

        _ = await provider.SafePsqlAsync(Database, "CREATE USER super SUPERUSER", token);
        _ = await subscriber.SafePsqlAsync(Database, "CREATE USER super SUPERUSER", token);

        _ = await provider.SafePsqlAsync(Database, "CREATE EXTENSION IF NOT EXISTS worf VERSION '0.1.0'", token);
        _ = await provider.SafePsqlAsync(Database, "ALTER EXTENSION worf UPDATE", token);

        _ = await subscriber.SafePsqlAsync(Database, "CREATE EXTENSION IF NOT EXISTS worf", token);

        _ = await provider.SafePsqlAsync(Database,
            "SELECT * FROM worf.create_node(node_name := 'test_provider', dsn := 'dbname=postgres user=super')", token);

        _ = await subscriber.SafePsqlAsync(Database,
            $"SELECT * FROM worf.create_node(node_name := 'test_subscriber', dsn := '{subscriberDsn}')", token);

        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM worf.create_replication_set('delay')", token);

        _ = await subscriber.SafePsqlAsync(Database, $$"""
            SELECT * FROM worf.create_subscription(
                subscription_name := 'test_subscription_delay',
                provider_dsn := '{{providerDsn}}',
                    replication_sets := '{delay}',
                    forward_origins := '{}',
                    synchronize_structure := false,
                    synchronize_data := false
            )
            """, token);

        _ = await subscriber.SafePsqlAsync(Database, """
            DO $$
            BEGIN
                    FOR i IN 1..100 LOOP
                            IF EXISTS (SELECT 1 FROM worf.show_subscription_status() WHERE status = 'replicating') THEN
                                    RETURN;
                            END IF;
                            PERFORM pg_sleep(0.1);
                    END LOOP;
            END;
            $$
            """, token);

        TestContext.WriteLine(await subscriber.SafePsqlAsync(Database,
            "SELECT subscription_name, status, provider_node, replication_sets, forward_origins FROM worf.show_subscription_status()",
            token));

        _ = await subscriber.SafePsqlAsync(Database, """
            DO $$
            BEGIN
                FOR i IN 1..300 LOOP
                    IF EXISTS (SELECT 1 FROM worf.local_sync_status WHERE sync_status = 'r') THEN
                        EXIT;
                    END IF;
                    PERFORM pg_sleep(0.1);
                END LOOP;
            END;$$
            """, token);

        TestContext.WriteLine(await subscriber.SafePsqlAsync(Database,
            "SELECT sync_kind, sync_subid, sync_nspname, sync_relname, sync_status FROM worf.local_sync_status ORDER BY 2,3,4",
            token));

        _ = await provider.SafePsqlAsync(Database, """
            CREATE OR REPLACE FUNCTION public.pg_xlog_wait_remote_apply(i_pos pg_lsn, i_pid integer) RETURNS VOID
            AS $FUNC$
            BEGIN
                WHILE EXISTS(SELECT true FROM pg_stat_get_wal_senders() s WHERE s.replay_location < i_pos AND (i_pid = 0 OR s.pid = i_pid)) LOOP
                            PERFORM pg_sleep(0.01);
                    END LOOP;
            END;$FUNC$ LANGUAGE plpgsql
            """, token);

        _ = await provider.SafePsqlAsync(Database, """
            SELECT worf.replicate_ddl_command($$
                CREATE TABLE public.basic_dml1 (
                    id serial primary key,
                    other integer,
                    data text,
                    something interval
                );
            $$)
            """, token);
        TestContext.WriteLine(await subscriber.SafePsqlAsync(Database,
            "select * from worf.show_subscription_status('test_subscription_delay');", token));
        _ = await provider.SafePsqlAsync(Database,
            "SELECT * FROM worf.replication_set_add_table('delay', 'basic_dml1', true) ", token);

        // At this point subscriber sync starts crashing (`sync worker`) and recovering. As per Petr this is expected
        // behavior. But since the table does not exist on subscriber, the sync worker dies when trying to accessing
        // it. It even logs why it dies on the line above.
        await Task.Delay(TimeSpan.FromSeconds(10), token);
        Assert.IsFalse(await subscriber.PsqlAsync(Database, "SELECT * FROM basic_dml1", token), "replication check");
    }
}
