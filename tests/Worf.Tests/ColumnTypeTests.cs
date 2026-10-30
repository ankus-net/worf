using System.Buffers.Binary;
using System.Globalization;
using System.Text;

namespace Worf.Tests;

/// <summary>
/// Apply converts a column's values from the provider's type when the subscriber's column has another, as when a node
/// that hasn't yet received a column's change of type keeps writing.
/// </summary>
[TestClass]
public sealed class ColumnTypeTests
{
    private const string Database = "postgres";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Values are read with the provider's type, PostgreSQL's own by OID, a domain as its base type and others by
    /// schema and name, or as the subscriber column's own type when that's the same type under another schema or name,
    /// and converted to the subscriber's as an assignment would, as ALTER COLUMN TYPE converts them. A value that
    /// doesn't fit, has no such conversion or can't be read here fails with PostgreSQL's own error, which
    /// exception_behaviour handles, recording the table and row, without affecting the next change. A client of the
    /// first protocol version gets RELATION messages without the column types.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ValuesConvertToTheSubscribersColumnTypes()
    {
        CancellationToken token = TestContext.CancellationToken;

        await using TestNode provider = await StartNodeAsync("provider", token);
        await using TestNode subscriber = await StartNodeAsync("subscriber", token, "worf.exception_behaviour = discard");
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
                CREATE TYPE pair AS (x integer, y integer);
                """, token);
        }

        // The subscriber's mood lacks a label, its emotion is the provider's app.feeling under another schema and
        // name, and its amount and span are defined differently.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TYPE mood AS ENUM ('sad', 'happy', 'angry');
            CREATE SCHEMA app;
            CREATE TYPE app.feeling AS ENUM ('calm', 'tense');
            CREATE DOMAIN amount AS integer;
            CREATE TYPE span AS RANGE (subtype = integer);
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TYPE mood AS ENUM ('sad', 'happy');
            CREATE TYPE emotion AS ENUM ('calm', 'tense');
            CREATE DOMAIN amount AS real;
            CREATE TYPE span AS RANGE (subtype = real);
            """, token);

        // int4 to int8 and back, int4 to float4, text to int4, int4 to text, an enum to text, a domain to int8,
        // numeric to int4, an enum to its own type under another name, a domain to text, a range to a same-named range
        // and a composite to text.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, a integer, b bigint, c integer, d text, e integer, f mood, g posint,
                h numeric, i app.feeling, j amount, k span, l pair);
            SELECT * FROM worf.replication_set_add_table('default', 't');
            """, token);
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM pg_create_logical_replication_slot('column_types', 'worf')", token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, a bigint, b integer, c real, d integer, e text, f text, g bigint,
                h integer, i emotion, j text, k span, l text)
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                synchronize_structure := false,
                synchronize_data := false)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'sub' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");

        _ = await provider.SafePsqlAsync(Database, """
            INSERT INTO t VALUES (1, 1, 2, 3, NULL, 5, 'happy', 6, 3.7, 'tense', 5, NULL, '(1,2)');
            UPDATE t SET a = 2147483647, h = 2.5 WHERE id = 1;
            """, token);

        // A value too big for the subscriber's column, a label it lacks, text that has no assignment cast to integer
        // and a range it defines differently are each discarded with their error, and the changes after them apply.
        _ = await provider.SafePsqlAsync(Database, """
            BEGIN;
            INSERT INTO t VALUES (2, 1, 3000000000, 3, NULL, 5, 'sad', 6, 1, 'calm', 5, NULL, NULL);
            INSERT INTO t VALUES (3, 1, 2, 3, NULL, 5, 'angry', 6, 1, 'calm', 5, NULL, NULL);
            INSERT INTO t VALUES (4, 1, 2, 3, NULL, 5, 'sad', 6, 3.7, 'calm', 7, NULL, '(3,4)');
            INSERT INTO t VALUES (5, 1, 2, 3, '40', 5, 'sad', 6, 1, 'calm', 5, NULL, NULL);
            INSERT INTO t VALUES (6, 1, 2, 3, NULL, 5, 'sad', 6, 1, 'calm', 5, '[1,5)', NULL);
            INSERT INTO t VALUES (7, 7, 7, 7, NULL, 7, 'happy', 7, -7.5, 'tense', 7, NULL, '(7,7)');
            COMMIT;
            """, token);

        // A client of the first protocol version gets the layout it reads, and one of the second each column's type:
        // a domain's base type, PostgreSQL's own by OID and others by name and definition.
        Assert.AreEqual("id,a,b,c,d,e,f,g,h,i,j,k,l", await RelationColumnsAsync(provider, 1, token));
        Assert.AreEqual(
            "id:23,a:23,b:20,c:23,d:25,e:23,f:public.mood=enum,g:23,h:1700,i:app.feeling=enum,j:23," +
            "k:public.span=range(23),l:public.pair=composite(23,23)",
            await RelationColumnsAsync(provider, 2, token));
        _ = await provider.SafePsqlAsync(Database, """
            SELECT pg_drop_replication_slot('column_types');
            SELECT worf.wait_slot_confirm_lsn(NULL, NULL);
            """, token);

        Assert.AreEqual("1|2147483647|2|3|5|happy|6|3|tense|5|(1,2)\n4|1|2|3|5|sad|6|4|calm|7|(3,4)\n7|7|7|7|7|happy|7|-8|tense|7|(7,7)",
            await subscriber.SafePsqlAsync(Database, "SELECT concat_ws('|', id, a, b, c, d, e, f, g, h, i, j, k, l) FROM t ORDER BY id", token));
        Assert.AreEqual(
            "INSERT|public|t|22003|2|3000000000|sad\n" +
            "INSERT|public|t|22P02|3|2|\\x616e677279\n" +
            "INSERT|public|t|42804|5|2|40|sad\n" +
            "INSERT|public|t|42804|6|2|sad|\\x0200000004000000010000000400000005",
            await subscriber.SafePsqlAsync(Database, """
                SELECT concat_ws('|', operation, schema_name, table_name, error_sqlstate, remote_new_tuple->>'id',
                    remote_new_tuple->>'b', remote_new_tuple->>'d', remote_new_tuple->>'f', remote_new_tuple->>'k')
                FROM worf.exception_log ORDER BY log_id
                """, token));
    }

    private static async Task<TestNode> StartNodeAsync(string name, CancellationToken token, params string[] settings)
    {
        TestNode node = await TestNode.StartAsync(name,
        [
            "wal_level = logical",
            "max_wal_senders = 10",
            "max_replication_slots = 10",
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

    /// <summary>
    /// Reads the columns of the RELATION message the provider sends a client of a protocol version, as the old
    /// layout's reader does, with the type that follows a column's name when its flag says one does.
    /// </summary>
    private static async Task<string> RelationColumnsAsync(TestNode provider, int protoVersion, CancellationToken token)
    {
        byte[] message = Convert.FromHexString(await provider.SafePsqlAsync(Database, $"""
            SELECT encode(data, 'hex') FROM pg_logical_slot_peek_binary_changes('column_types', NULL, NULL,
                'min_proto_version', '1', 'max_proto_version', '{protoVersion}', 'startup_params_format', '1',
                'worf.replication_set_names', 'default')
            WHERE get_byte(data, 0) = ascii('R') LIMIT 1
            """, token));

        // The message type, flags and relation id, then the schema and table names.
        int pos = 6;
        pos += 1 + message[pos];
        pos += 1 + message[pos];
        Assert.AreEqual((byte)'A', message[pos++]);
        int natts = BinaryPrimitives.ReadUInt16BigEndian(message.AsSpan(pos));
        pos += 2;

        StringBuilder columns = new();
        for (int i = 0; i < natts; i++)
        {
            Assert.AreEqual((byte)'C', message[pos++]);
            byte flags = message[pos++];
            Assert.AreEqual((byte)'N', message[pos++]);
            int len = BinaryPrimitives.ReadUInt16BigEndian(message.AsSpan(pos));
            pos += 2;
            columns.Append(i == 0 ? "" : ",").Append(Encoding.UTF8.GetString(message, pos, len - 1));
            pos += len;

            if ((flags & 2) != 0)
            {
                Assert.AreEqual((byte)'Y', message[pos++]);
                uint oid = BinaryPrimitives.ReadUInt32BigEndian(message.AsSpan(pos));
                pos += 4;
                if (oid < 16384)
                {
                    columns.Append(CultureInfo.InvariantCulture, $":{oid}");
                }
                else
                {
                    columns.Append(':').Append(Encoding.UTF8.GetString(message, pos + 1, message[pos] - 1));
                    pos += 1 + message[pos];
                    columns.Append('.').Append(Encoding.UTF8.GetString(message, pos + 1, message[pos] - 1));
                    pos += 1 + message[pos];
                    len = BinaryPrimitives.ReadInt32BigEndian(message.AsSpan(pos));
                    pos += 4;
                    columns.Append('=').Append(Encoding.UTF8.GetString(message, pos, len - 1));
                    pos += len;
                }
            }
        }

        Assert.AreEqual(message.Length, pos, "the message ends after its columns");
        return columns.ToString();
    }
}
