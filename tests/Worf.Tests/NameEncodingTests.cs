namespace Worf.Tests;

/// <summary>
/// Names hold the bytes of their database's encoding, which aren't always UTF-8: those of a LATIN1 database, and any
/// a SQL_ASCII database holds, which PostgreSQL never checks.
/// </summary>
[TestClass]
public sealed class NameEncodingTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// In SQL_ASCII databases, tables and columns whose names hold bytes that aren't UTF-8 copy, replicate and
    /// resynchronize, and two tables whose names differ only in such a byte copy at once, each into its own table.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public Task SqlAsciiNamesKeepTheirBytes()
        => NamesKeepTheirBytesAsync("SQL_ASCII", "C", ["\\x746162e9", "\\x746162e8"], "\\x636166e9", TestContext.CancellationToken);

    /// <summary>
    /// In LATIN1 databases, tables and columns whose names hold letters outside ASCII copy, replicate and
    /// resynchronize.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public Task Latin1NamesKeepTheirBytes()
        => NamesKeepTheirBytesAsync("LATIN1", "C", ["\\x746162e9", "\\x746162e8"], "\\x636166e9", TestContext.CancellationToken);

    private static async Task NamesKeepTheirBytesAsync(string encoding, string locale, string[] tables, string column,
        CancellationToken token)
    {
        const string Named = "named";
        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token);
        string create = string.Join("\n", tables.Select(table => $"""
            EXECUTE format('CREATE TABLE %I (id integer PRIMARY KEY, %I text)', convert_from('{table}', '{encoding}'),
                convert_from('{column}', '{encoding}'));
            """));
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database,
                $"CREATE DATABASE {Named} ENCODING '{encoding}' LC_COLLATE '{locale}' LC_CTYPE '{locale}' TEMPLATE template0", token);
            _ = await node.SafePsqlAsync(Named, $"""
                CREATE EXTENSION worf;
                SELECT * FROM worf.create_node(node_name := '{node.Name}_named', dsn := '{node.Dsn(Named, SuperUser)}');
                DO $$
                BEGIN
                    {create}
                END $$;
                """, token);
        }

        string insert = string.Join("\n", tables.Select((table, i) => $"""
            EXECUTE format('INSERT INTO %I SELECT g, %L FROM generate_series(%s, %s) g', convert_from('{table}', '{encoding}'),
                'v{i}', $1, $2);
            """));
        string Insert(int from, int to) => $"""
            DO $$
            BEGIN
                {insert.Replace("$1", from.ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal)
                    .Replace("$2", to.ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal)}
            END $$;
            """;
        _ = await provider.SafePsqlAsync(Named, Insert(1, 100), token);
        _ = await provider.SafePsqlAsync(Named, "SELECT * FROM worf.replication_set_add_all_tables('default', '{public}')", token);
        _ = await subscriber.SafePsqlAsync(Named, $"""
            SELECT worf.create_subscription(subscription_name := 'sub', provider_dsn := '{provider.Dsn(Named, SuperUser)}',
                synchronize_structure := false, synchronize_data := true)
            """, token);
        const string Synced = """
            SELECT count(*) = 2 AND bool_and(sync_status = 'r') FROM worf.local_sync_status WHERE sync_relname IS NOT NULL
            """;
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Named, Synced, token), "the copies");
        _ = await provider.SafePsqlAsync(Named, Insert(101, 150), token);
        string Counts() => string.Join(",", tables.Select(_ => "150:150"));
        string query = "SELECT string_agg(pg_temp.counted(t, c, v), ',' ORDER BY n) FROM (VALUES " + string.Join(", ", tables.Select((table, i) =>
            $"({i}, convert_from('{table}', '{encoding}'), convert_from('{column}', '{encoding}'), 'v{i}')")) + ") s (n, t, c, v)";
        const string Counted = """
            CREATE FUNCTION pg_temp.counted(tab text, col text, val text) RETURNS text LANGUAGE plpgsql AS $$
            DECLARE
                result text;
            BEGIN
                EXECUTE format('SELECT count(*) || '':'' || count(*) FILTER (WHERE %I = %L) FROM %I', col, val, tab) INTO result;
                RETURN result;
            END $$;
            """;
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Named, $"{Counted}\nSELECT ({query}) = '{Counts()}'", token), "the replicated rows");

        _ = await subscriber.SafePsqlAsync(Named, $"""
            SELECT worf.alter_subscription_resynchronize_table('sub', format('%I', convert_from('{tables[0]}', '{encoding}'))::regclass)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Named, Synced, token), "the resynchronization");
        Assert.AreEqual(Counts(), await subscriber.SafePsqlAsync(Named, $"{Counted}\n{query}", token), "the rows after resynchronization");
        // DDL on such a table replicates with its bytes.
        _ = await provider.SafePsqlAsync(Named, $"""
            SET worf.enable_ddl_replication = on;
            SET worf.allow_ddl_from_functions = on;
            DO $$
            BEGIN
                EXECUTE format('ALTER TABLE %I ADD COLUMN %I integer DEFAULT 7', convert_from('{tables[0]}', '{encoding}'),
                    convert_from('{column}32', '{encoding}'));
            END $$;
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Named, $"""
            SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = format('%I', convert_from('{tables[0]}', '{encoding}'))::regclass
                AND attname = convert_from('{column}32', '{encoding}'))
            """, token), "the replicated DDL");

        Assert.AreEqual(string.Join(",", tables.Select(table => $"{table[2..]}:replicating:{table[2..]}")), await subscriber.SafePsqlAsync(Named, """
            SELECT string_agg(concat_ws(':', encode(convert_to(sync_relname::text, current_setting('server_encoding')), 'hex'), t.status,
                    encode(convert_to(t.relname, current_setting('server_encoding')), 'hex')),
                ',' ORDER BY encode(convert_to(sync_relname::text, current_setting('server_encoding')), 'hex') DESC)
            FROM worf.local_sync_status s,
                LATERAL worf.show_subscription_table('sub', format('%I.%I', sync_nspname, sync_relname)::regclass) t
            WHERE sync_relname IS NOT NULL
            """, token), "the tables the subscription shows");
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
            "track_commit_timestamp = on",
            "max_worker_processes = 16",
        ], token);
        try
        {
            _ = await node.SafePsqlAsync(Database, $"CREATE USER {SuperUser} SUPERUSER", token);
        }
        catch
        {
            await node.DisposeAsync();
            throw;
        }

        return node;
    }
}
