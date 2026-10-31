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
    /// Values sent in binary are read with the provider's type, PostgreSQL's own by OID, a domain as its base type and
    /// others by schema and name when defined the same, and converted to the subscriber's as an assignment would, as
    /// ALTER COLUMN TYPE converts them, or else, for a string type, by the column type's input. The column's own type
    /// reads values sent as text, an enum's labels, and those of a type the subscriber lacks or defines differently, as
    /// it always has. A value that converts neither way fails with PostgreSQL's own error, which exception_behaviour
    /// handles, recording the table and row, without affecting the next change. Conversions follow types the
    /// subscriber creates and drops. A client of the first protocol version gets RELATION messages without the column
    /// types, and values in PostgreSQL's internal format only from the second.
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
                CREATE DOMAIN numdom AS numeric;
                """, token);
        }

        // The subscriber's mood lacks a label, its emotion is the provider's app.feeling under another schema and
        // name, its amount and pair are defined differently, and it lacks gone, citext and, for now, span.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TYPE mood AS ENUM ('sad', 'happy', 'angry');
            CREATE SCHEMA app;
            CREATE TYPE app.feeling AS ENUM ('calm', 'tense');
            CREATE DOMAIN amount AS integer;
            CREATE TYPE span AS RANGE (subtype = integer);
            CREATE TYPE pair AS (x integer, y integer);
            CREATE TYPE gone AS ENUM ('x', 'y');
            CREATE EXTENSION citext;
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TYPE mood AS ENUM ('sad', 'happy');
            CREATE TYPE emotion AS ENUM ('calm', 'tense');
            CREATE DOMAIN amount AS real;
            CREATE TYPE pair AS (x bigint, y integer);
            """, token);

        // int4 to int8 and back, int4 to float4, text to int4, int4 to text, an enum to text, a domain to int8,
        // numeric to int4, an enum to its own type under another name, a domain to text, a range to text, a composite
        // to text, a missing enum and citext to text, composites and their arrays to their own types defined
        // differently, an enum to its own type, a domain's array, sent as text, to int4[], and boolean to int4.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, a integer, b bigint, c integer, d text, e integer, f mood, g posint,
                h numeric, i app.feeling, j amount, k span, l pair, m gone, n citext, o pair, p pair[], q mood, r numdom[],
                s boolean);
            SELECT * FROM worf.replication_set_add_table('default', 't');
            """, token);
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM pg_create_logical_replication_slot('column_types', 'worf')", token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, a bigint, b integer, c real, d integer, e text, f text, g bigint,
                h integer, i emotion, j text, k text, l text, m text, n text, o pair, p pair[], q mood, r integer[],
                s integer)
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
            INSERT INTO t VALUES (1, 1, 2, 3, '40', 5, 'happy', 6, 3.7, 'tense', 5, NULL, '(1,2)', 'x', 'Abc', '(1,2)',
                '{"(1,2)"}', 'happy', '{1}', NULL);
            UPDATE t SET a = 2147483647, h = 2.5 WHERE id = 1;
            """, token);

        // A value too big for the subscriber's column, text its integer input rejects, a label the subscriber's mood
        // lacks, a boolean, which has no assignment cast to integer, and a domain's array whose text integer[]'s input
        // rejects are each discarded with their error, and the changes after them apply. So does a label only its text
        // column lacks.
        _ = await provider.SafePsqlAsync(Database, """
            BEGIN;
            INSERT INTO t VALUES (2, 1, 3000000000, 3, NULL, 5, 'sad', 6, 1, 'calm', 5, NULL, NULL);
            INSERT INTO t VALUES (3, 1, 2, 3, NULL, 5, 'angry', 6, 1, 'calm', 5, NULL, NULL);
            INSERT INTO t VALUES (4, 1, 2, 3, 'x', 5, 'sad', 6, 3.7, 'calm', 7, NULL, '(3,4)');
            INSERT INTO t (id, f, q) VALUES (5, 'angry', 'angry');
            INSERT INTO t (id, s) VALUES (6, true);
            INSERT INTO t (id, r) VALUES (8, '{3.7}');
            INSERT INTO t VALUES (7, 7, 7, 7, NULL, 7, 'happy', 7, -7.5, 'tense', 7, NULL, '(7,7)', 'y', 'XyZ', '(7,7)',
                NULL, 'sad', '{7}', NULL);
            COMMIT;
            """, token);

        // A client of the first protocol version gets the layout it reads, and one of the second each column's type:
        // a domain's base type, PostgreSQL's own by OID and others by name and definition. Values come in PostgreSQL's
        // internal format, for its own base types, only with the second.
        Assert.AreEqual("id,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s", await RelationColumnsAsync(provider, 1, token));
        Assert.AreEqual(
            "id:23,a:23,b:20,c:23,d:25,e:23,f:public.mood=enum,g:23,h:1700,i:app.feeling=enum,j:23," +
            "k:public.span=range(23),l:public.pair=composite(23,23),m:public.gone=enum," +
            "n:public.citext=base(citextin,citextrecv,-1),o:public.pair=composite(23,23)," +
            "p:public._pair=array(composite(23,23)),q:public.mood=enum,r:public._numdom=array(1700),s:16",
            await RelationColumnsAsync(provider, 2, token));
        Assert.AreEqual("bbbbbbbbbbbntbbttbtn", await InsertKindsAsync(provider, 1, token));
        Assert.AreEqual("iiiiiibbibbntbbttbtn", await InsertKindsAsync(provider, 2, token));
        _ = await provider.SafePsqlAsync(Database, """
            SELECT pg_drop_replication_slot('column_types');
            SELECT worf.wait_slot_confirm_lsn(NULL, NULL);
            """, token);

        Assert.AreEqual(
            "1|2147483647|2|3|40|5|happy|6|3|tense|5|(1,2)|x|Abc|(1,2)|{\"(1,2)\"}|happy|{1}\n" +
            "3|1|2|3|5|angry|6|1|calm|5\n" +
            "7|7|7|7|7|happy|7|-8|tense|7|(7,7)|y|XyZ|(7,7)|sad|{7}",
            await subscriber.SafePsqlAsync(Database,
                "SELECT concat_ws('|', id, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s) FROM t ORDER BY id", token));
        Assert.AreEqual(
            "INSERT|public|t|22003|2|3000000000|sad\n" +
            "INSERT|public|t|22P02|4|2|x|sad\n" +
            "INSERT|public|t|22P02|5|angry|angry\n" +
            "INSERT|public|t|42804|6|t\n" +
            "INSERT|public|t|22P02|8|{3.7}",
            await subscriber.SafePsqlAsync(Database, """
                SELECT concat_ws('|', operation, schema_name, table_name, error_sqlstate, remote_new_tuple->>'id',
                    remote_new_tuple->>'b', remote_new_tuple->>'d', remote_new_tuple->>'f', remote_new_tuple->>'q',
                    remote_new_tuple->>'s', remote_new_tuple->>'r')
                FROM worf.exception_log ORDER BY log_id
                """, token));

        // A range the subscriber creates after the table's first change converts, and so does one it drops and
        // creates again, without the apply worker failing and starting over.
        int logpos = subscriber.ReadLog().Length;
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TYPE span AS RANGE (subtype = integer)", token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t (id, k) VALUES (10, '[1,5)')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, Applied(10), token), "row 10 wasn't applied or logged");
        _ = await subscriber.SafePsqlAsync(Database, """
            DROP TYPE span;
            CREATE TYPE span AS RANGE (subtype = integer);
            """, token);
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO t (id, k) VALUES (11, '[2,6)')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, Applied(11), token), "row 11 wasn't applied or logged");
        Assert.AreEqual("10|[1,5)\n11|[2,6)", await subscriber.SafePsqlAsync(Database,
            "SELECT concat_ws('|', id, k) FROM t WHERE id >= 10 ORDER BY id", token));
        Assert.DoesNotContain("exiting with error", subscriber.ReadLog()[logpos..]);
    }

    /// <summary>
    /// Checks whether the subscriber applied a row, or discarded it.
    /// </summary>
    private static string Applied(int id)
        => $"""
            SELECT EXISTS (SELECT 1 FROM t WHERE id = {id})
                OR EXISTS (SELECT 1 FROM worf.exception_log WHERE remote_new_tuple->>'id' = '{id}')
            """;

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

    /// <summary>
    /// Reads the form of each column of the first INSERT message the provider sends a client of a protocol version
    /// that asks for values in binary and PostgreSQL's internal format, as a subscriber of the same major version
    /// does.
    /// </summary>
    private static async Task<string> InsertKindsAsync(TestNode provider, int protoVersion, CancellationToken token)
    {
        byte[] message = Convert.FromHexString(await provider.SafePsqlAsync(Database, $"""
            SELECT encode(data, 'hex') FROM pg_logical_slot_peek_binary_changes('column_types', NULL, NULL,
                'min_proto_version', '1', 'max_proto_version', '{protoVersion}', 'startup_params_format', '1',
                'worf.replication_set_names', 'default', 'binary.want_internal_basetypes', '1',
                'binary.want_binary_basetypes', '1',
                'binary.basetypes_major_version', (current_setting('server_version_num')::integer / 100)::text,
                'binary.float4_byval', '0', 'binary.float8_byval', '1', 'binary.integer_datetimes', '0')
            WHERE get_byte(data, 0) = ascii('I') LIMIT 1
            """, token));

        // The message type, flags, relation id and new tuple marker, then the tuple.
        int pos = 7;
        Assert.AreEqual((byte)'T', message[pos++]);
        int natts = BinaryPrimitives.ReadUInt16BigEndian(message.AsSpan(pos));
        pos += 2;

        StringBuilder kinds = new();
        for (int i = 0; i < natts; i++)
        {
            char kind = (char)message[pos++];
            kinds.Append(kind);
            if (kind is 'i' or 'b' or 't')
            {
                pos += 4 + BinaryPrimitives.ReadInt32BigEndian(message.AsSpan(pos));
            }
        }

        Assert.AreEqual(message.Length, pos, "the message ends after its tuple");
        return kinds.ToString();
    }
}
