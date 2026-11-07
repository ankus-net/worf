using System.Buffers.Binary;
using System.ComponentModel;
using System.Globalization;
using System.Text;
using Npgsql;

namespace Worf.Tests;

/// <summary>
/// Apply converts a column's values from the provider's type when the subscriber's column has another, as when a node
/// that hasn't yet received a column's change of type keeps writing.
/// </summary>
[TestClass]
public sealed class ColumnTypeTests
{
    private const string Database = "postgres";
    private const string TextDatabase = "textsub";
    private const string SuperUser = "super";

    /// <summary>
    /// Gets or sets the test context.
    /// </summary>
    public TestContext TestContext { get; set; } = null!;

    /// <summary>
    /// Values are read with the provider's type, PostgreSQL's own by OID, a domain as its base type and others by
    /// schema and name when defined the same, and converted to the subscriber's as an assignment would, as ALTER COLUMN
    /// TYPE converts them, or else through text, which the column type's input reads with the column's type modifier.
    /// An enum's values are its labels, and a type made of enums is read only for a column made of them. The column's
    /// own type reads the text of a type the subscriber lacks or defines differently, whose values in binary fail. A
    /// value that doesn't convert fails with PostgreSQL's own error, which exception_behaviour handles, recording the
    /// table and row, without affecting the next change. Conversions follow types the subscriber creates and drops.
    /// The initial copy converts as replication does, and values convert under the subscriber's settings however
    /// they're sent. In the copy, exception_behaviour skips and records a row that fails, and under error the copy
    /// stops naming the row, column and value, and starts again only after a delay. Money fails when the provider
    /// formats it differently, and is copied again until the formats agree. A client of the first protocol version gets RELATION messages without the column
    /// types, and values in PostgreSQL's internal format only from the second.
    /// </summary>
    /// <returns>The test.</returns>
    [TestMethod]
    public async Task ValuesConvertToTheSubscribersColumnTypes()
    {
        CancellationToken token = TestContext.CancellationToken;

        // Locales generated for the provider, when the host has none that formats money otherwise.
        using TemporaryDirectory locales = new($"worf-locales-{Guid.NewGuid():N}");
        await using TestNode provider = await StartNodeAsync("provider", token, "TimeZone = 'Asia/Tokyo'",
            "bytea_output = escape");
        await using TestNode subscriber = await StartNodeAsync("subscriber", token, "worf.exception_behaviour = discard",
            "TimeZone = 'America/New_York'", "max_worker_processes = 16");
        foreach (TestNode node in new[] { provider, subscriber })
        {
            _ = await node.SafePsqlAsync(Database, """
                CREATE DOMAIN posint AS integer CHECK (VALUE > 0);
                CREATE DOMAIN numdom AS numeric;
                """, token);
        }

        // The subscriber's mood lacks a label, its emotion is the provider's app.feeling under another schema and
        // name, its amount, pair and fspan are defined differently, and it lacks gone, st, citext and, for now, span.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TYPE mood AS ENUM ('sad', 'happy', 'angry');
            CREATE TYPE mpair AS (m mood, n integer);
            CREATE SCHEMA app;
            CREATE TYPE app.feeling AS ENUM ('calm', 'tense');
            CREATE DOMAIN amount AS integer;
            CREATE TYPE span AS RANGE (subtype = integer);
            CREATE TYPE pair AS (x integer, y integer);
            CREATE TYPE gone AS ENUM ('x', 'y');
            CREATE TYPE st AS ENUM ('done', 'complete');
            CREATE TYPE fspan AS RANGE (subtype = integer);
            CREATE EXTENSION citext;
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TYPE mood AS ENUM ('sad', 'happy');
            CREATE TYPE mpair AS (m mood, n integer);
            CREATE TYPE emotion AS ENUM ('calm', 'tense');
            CREATE DOMAIN amount AS real;
            CREATE TYPE pair AS (x bigint, y integer);
            CREATE TYPE fspan AS RANGE (subtype = real);
            """, token);

        // Money is formatted another way on the provider, under a locale the host has or, on Linux, one generated.
        bool moneyDiffers = await FormatMoneyDifferentlyAsync(provider, subscriber, locales.FullName, token);

        // Tables the initial copy converts, copied before any other change, from a provider whose TimeZone and
        // bytea_output differ from the subscriber's: an enum's array and a composite made of it going into text, a
        // range with no assignment cast to the subscriber's, and a value too big for the subscriber's column.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE zt (id integer PRIMARY KEY, n numeric, tz timestamptz, by bytea, e mood[], p mpair, r tstzrange);
            CREATE TABLE mt (id integer PRIMARY KEY, m money);
            CREATE TABLE mnt (id integer PRIMARY KEY, m money);
            SELECT * FROM worf.create_replication_set('zs');
            SELECT * FROM worf.replication_set_add_table('zs', 'zt');
            SELECT * FROM worf.replication_set_add_table('zs', 'mt');
            SELECT * FROM worf.replication_set_add_table('zs', 'mnt');
            INSERT INTO zt VALUES (1, 3.7, '2024-01-02 03:04:05+00', 'A', '{angry}', '(angry,1)',
                '[2024-01-02 03:04:05+00,2024-01-03 03:04:05+00)'), (3, 1e10, NULL, NULL, NULL, NULL, NULL);
            INSERT INTO mt VALUES (1, '12.35');
            INSERT INTO mnt VALUES (1, '1234');
            """, token);

        // int4 to int8 and back, int4 to float4, text to int4, int4 to text, an enum to text, a domain to int8,
        // numeric to int4, an enum to its own type under another name, a domain to text, a range to text, a composite
        // to text, a missing enum to text, citext to int4, composites and their arrays to their own types defined
        // differently, an enum to its own type, a domain's array, sent as text, to int4[], boolean to int4, text to
        // bit(4) and varbit(3), an enum to int4, int8 and timestamp, a range to its own type defined differently, and a
        // missing range to boolean.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, a integer, b bigint, c integer, d text, e integer, f mood, g posint,
                h numeric, i app.feeling, j amount, k span, l pair, m gone, n citext, o pair, p pair[], q mood, r numdom[],
                s boolean, u text, v text, w st, x st, y st, z fspan, za span);
            SELECT * FROM worf.replication_set_add_table('default', 't');
            CREATE DOMAIN moods AS mood[];
            CREATE DOMAIN mpd AS mpair;
            CREATE TYPE moodrange AS RANGE (subtype = mood);
            CREATE TABLE ut (id integer PRIMARY KEY, a moods, c mpd, r moodrange);
            SELECT * FROM worf.replication_set_add_table('default', 'ut');
            """, token);
        _ = await provider.SafePsqlAsync(Database, "SELECT * FROM pg_create_logical_replication_slot('column_types', 'worf')", token);
        _ = await subscriber.SafePsqlAsync(Database, """
            CREATE TABLE t (id integer PRIMARY KEY, a bigint, b integer, c real, d integer, e text, f text, g bigint,
                h integer, i emotion, j text, k text, l text, m text, n integer, o pair, p pair[], q mood, r integer[],
                s integer, u bit(4), v varbit(3), w integer, x bigint, y timestamp, z fspan, za boolean);
            CREATE TABLE zt (id integer PRIMARY KEY, n integer, tz timestamp, by text, e text[], p text, r tsrange);
            CREATE TABLE mt (id integer PRIMARY KEY, m money);
            CREATE TABLE mnt (id integer PRIMARY KEY, m numeric);
            CREATE TABLE ut (id integer PRIMARY KEY, a text, c text, r text);
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, $"""
            SELECT worf.create_subscription(
                subscription_name := 'sub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                replication_sets := ARRAY['default', 'zs'],
                synchronize_structure := false,
                synchronize_data := false)
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, """
            SELECT EXISTS (
                SELECT 1 FROM worf.show_subscription_status()
                    WHERE subscription_name = 'sub' AND status = 'replicating')
            """, token), "subscription failed to reach \"replicating\" state");

        _ = await provider.SafePsqlAsync(Database, """
            INSERT INTO t VALUES (1, 1, 2, 3, '40', 5, 'happy', 6, 3.7, 'tense', 5, NULL, '(1,2)', 'x', NULL, '(1,2)',
                '{"(1,2)"}', 'happy', '{1}', NULL, '1010', '101');
            UPDATE t SET a = 2147483647, h = 2.5 WHERE id = 1;
            """, token);

        // A value too big for the subscriber's column, text its integer input rejects, a label the subscriber's mood
        // lacks, a boolean, whose text integer's input rejects, bits too many or too few for the column, an enum's
        // labels, which integer, bigint and timestamp reject, and citext, which the subscriber lacks, and ranges it
        // defines differently or lacks, which it can't read in binary, are each discarded with their error, and the
        // changes after them apply. So does a label only its text column lacks, and a domain's array, sent as text,
        // converts.
        _ = await provider.SafePsqlAsync(Database, """
            BEGIN;
            INSERT INTO t VALUES (2, 1, 3000000000, 3, NULL, 5, 'sad', 6, 1, 'calm', 5, NULL, NULL);
            INSERT INTO t VALUES (3, 1, 2, 3, NULL, 5, 'angry', 6, 1, 'calm', 5, NULL, NULL);
            INSERT INTO t VALUES (4, 1, 2, 3, 'x', 5, 'sad', 6, 3.7, 'calm', 7, NULL, '(3,4)');
            INSERT INTO t (id, f, q) VALUES (5, 'angry', 'angry');
            INSERT INTO t (id, s) VALUES (6, true);
            INSERT INTO t (id, r) VALUES (8, '{3.7}');
            INSERT INTO t (id, u) VALUES (12, '10101');
            INSERT INTO t (id, u) VALUES (13, '101');
            INSERT INTO t (id, v) VALUES (14, '1111');
            INSERT INTO t (id, w) VALUES (15, 'done');
            INSERT INTO t (id, x) VALUES (16, 'complete');
            INSERT INTO t (id, y) VALUES (17, 'complete');
            INSERT INTO t (id, n) VALUES (18, '1234');
            INSERT INTO t (id, z) VALUES (19, '[1,5)');
            INSERT INTO t (id, za) VALUES (20, 'empty');
            INSERT INTO t VALUES (7, 7, 7, 7, NULL, 7, 'happy', 7, -7.5, 'tense', 7, NULL, '(7,7)', 'y', NULL, '(7,7)',
                NULL, 'sad', '{7}', NULL);
            COMMIT;
            """, token);

        // A client of the first protocol version gets the layout it reads, and one of the second each column's type:
        // a domain's base type, PostgreSQL's own by OID and others by name and definition. Values come in PostgreSQL's
        // internal format, for its own base types, only with the second.
        Assert.AreEqual("id,a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,u,v,w,x,y,z,za",
            await RelationColumnsAsync(provider, 1, token));
        Assert.AreEqual(
            "id:23,a:23,b:20,c:23,d:25,e:23,f:public.mood=enum,g:23,h:1700,i:app.feeling=enum,j:23," +
            "k:public.span=range(23),l:public.pair=composite(23,23),m:public.gone=enum," +
            "n:public.citext=base(citextin,citextrecv,-1),o:public.pair=composite(23,23)," +
            "p:public._pair=array(composite(23,23)),q:public.mood=enum,r:public._numdom=array(1700),s:16,u:25,v:25," +
            "w:public.st=enum,x:public.st=enum,y:public.st=enum,z:public.fspan=range(23),za:public.span=range(23)",
            await RelationColumnsAsync(provider, 2, token));
        Assert.AreEqual("bbbbbbbbbbbntbnttbtnbbnnnnn", await InsertKindsAsync(provider, 1, token));
        Assert.AreEqual("iiiiiibbibbntbnttbtniinnnnn", await InsertKindsAsync(provider, 2, token));
        _ = await provider.SafePsqlAsync(Database, """
            SELECT pg_drop_replication_slot('column_types');
            SELECT worf.wait_slot_confirm_lsn(NULL, NULL);
            """, token);

        // Domains over an enum's array and over a composite, and a range of an enum, go into text, in binary streaming.
        _ = await provider.SafePsqlAsync(Database, "INSERT INTO ut VALUES (1, '{happy}', '(happy,1)', '[sad,happy)')", token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT EXISTS (SELECT 1 FROM ut)", token), "ut's row");
        Assert.AreEqual("{happy}|(happy,1)|[sad,happy)", await subscriber.SafePsqlAsync(Database, "SELECT concat_ws('|', a, c, r) FROM ut", token));

        Assert.AreEqual(
            "1|2147483647|2|3|40|5|happy|6|3|tense|5|(1,2)|x|(1,2)|{\"(1,2)\"}|happy|{1}|1010|101\n" +
            "3|1|2|3|5|angry|6|1|calm|5\n" +
            "7|7|7|7|7|happy|7|-8|tense|7|(7,7)|y|(7,7)|sad|{7}\n" +
            "8|{4}",
            await subscriber.SafePsqlAsync(Database, """
                SELECT concat_ws('|', id, a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, u, v, w, x, y, z, za)
                FROM t ORDER BY id
                """, token));
        Assert.AreEqual(
            "INSERT|public|t|22003|2|3000000000|sad\n" +
            "INSERT|public|t|22P02|4|2|x|sad\n" +
            "INSERT|public|t|22P02|5|angry|angry\n" +
            "INSERT|public|t|22P02|6|t\n" +
            "INSERT|public|t|22026|12|10101\n" +
            "INSERT|public|t|22026|13|101\n" +
            "INSERT|public|t|22001|14|1111\n" +
            "INSERT|public|t|22P02|15|done\n" +
            "INSERT|public|t|22P02|16|complete\n" +
            "INSERT|public|t|22007|17|complete\n" +
            "INSERT|public|t|42804|18|\\x31323334\n" +
            "INSERT|public|t|42804|19|\\x0200000004000000010000000400000005\n" +
            "INSERT|public|t|42804|20|\\x01",
            await subscriber.SafePsqlAsync(Database, """
                SELECT concat_ws('|', operation, schema_name, table_name, error_sqlstate, remote_new_tuple->>'id',
                    remote_new_tuple->>'b', remote_new_tuple->>'d', remote_new_tuple->>'f', remote_new_tuple->>'q',
                    remote_new_tuple->>'s', remote_new_tuple->>'u', remote_new_tuple->>'v', remote_new_tuple->>'w',
                    remote_new_tuple->>'x', remote_new_tuple->>'y', remote_new_tuple->>'n', remote_new_tuple->>'z',
                    remote_new_tuple->>'za')
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

        // The initial copy converts values as replication does, for a resynchronized table and for a subscription
        // that gets values as text, and so do the values sent afterwards, in binary and as text, all under the
        // subscriber's TimeZone and bytea_output. The row whose value is too big is skipped and recorded.
        int moneyLogpos = subscriber.ReadLog().Length;
        _ = await subscriber.SafePsqlAsync(Database, """
            SELECT worf.alter_subscription_resynchronize_table('sub', 'zt');
            SELECT worf.alter_subscription_resynchronize_table('sub', 'mt');
            SELECT worf.alter_subscription_resynchronize_table('sub', 'mnt');
            """, token);
        // A table whose types are the same on both nodes but whose rows the subscriber's own constraints refuse: a
        // shorter varchar, a domain only it has, and an enum that lacks a label.
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE ct (id integer PRIMARY KEY, v varchar(400), d integer, m mood);
            INSERT INTO ct VALUES (1, 'ok', 1, 'sad'), (2, 'too long', 1, 'sad'), (3, 'ok', 500, 'sad'), (4, 'ok', 1, 'angry'),
                (5, 'ok', 1, 'happy'), (6, repeat('x', 300), 1, 'sad');
            SELECT * FROM worf.replication_set_add_table('zs', 'ct');
            CREATE TABLE "zé" (id integer PRIMARY KEY);
            INSERT INTO "zé" VALUES (1);
            SELECT * FROM worf.replication_set_add_table('zs', '"zé"');
            """, token);
        _ = await subscriber.SafePsqlAsync(Database, $"CREATE DATABASE {TextDatabase}", token);
        _ = await subscriber.SafePsqlAsync(TextDatabase, "CREATE EXTENSION worf", token);
        _ = await subscriber.SafePsqlAsync(TextDatabase, $"""
            SELECT * FROM worf.create_node(node_name := 'subscriber_text', dsn := '{subscriber.Dsn(TextDatabase, SuperUser)}');
            CREATE TYPE mood AS ENUM ('sad', 'happy');
            CREATE TYPE mpair AS (m mood, n integer);
            CREATE TABLE zt (id integer PRIMARY KEY, n integer, tz timestamp, by text, e text[], p text, r tsrange);
            CREATE TABLE mt (id integer PRIMARY KEY, m money);
            CREATE TABLE mnt (id integer PRIMARY KEY, m numeric);
            CREATE DOMAIN small AS integer CHECK (VALUE < 100);
            CREATE TABLE ct (id integer PRIMARY KEY, v varchar(3), d small, m mood);
            CREATE TABLE "zé" (id integer PRIMARY KEY);
            """, token);
        _ = await subscriber.SafePsqlAsync(TextDatabase, $"""
            SELECT worf.create_subscription(
                subscription_name := 'textsub',
                provider_dsn := '{provider.Dsn(Database, SuperUser)}',
                replication_sets := ARRAY['zs'],
                synchronize_structure := false,
                synchronize_data := true,
                force_text_transfer := true)
            """, token);
        // Money from a provider that formats it differently fails every value, which says nothing of the rows, so the
        // copies try again rather than skip them, until the formats agree.
        if (moneyDiffers)
        {
            Assert.IsTrue(await subscriber.PollLogAsync(moneyLogpos, "gets money from a provider whose lc_monetary formats it as", token),
                "the copy of money failing");
            Assert.AreEqual("0", await subscriber.SafePsqlAsync(Database, "SELECT count(*) FROM worf.exception_log WHERE table_name IN ('mt', 'mnt')", token),
                "money skipped");
            _ = await provider.SafePsqlAsync(Database, "ALTER SYSTEM RESET lc_monetary", token);
            _ = await provider.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        }

        foreach (string database in new[] { Database, TextDatabase })
        {
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(database, """
                SELECT EXISTS (SELECT 1 FROM zt)
                    AND NOT EXISTS (SELECT 1 FROM worf.local_sync_status WHERE sync_status <> 'r')
                """, token), $"zt wasn't copied in {database}");
        }

        // A table whose name isn't ASCII syncs too, though a worker's name is ASCII.
        Assert.AreEqual("1", await subscriber.SafePsqlAsync(TextDatabase, "SELECT count(*) FROM \"zé\"", token), "zé's row");

        _ = await provider.SafePsqlAsync(Database, """
            INSERT INTO zt VALUES (2, 3.7, '2024-01-02 03:04:05+00', 'A', '{angry}', '(angry,1)',
                '[2024-01-02 03:04:05+00,2024-01-03 03:04:05+00)');
            INSERT INTO mt VALUES (2, '12.35');
            """, token);
        foreach (string database in new[] { Database, TextDatabase })
        {
            Assert.IsTrue(await subscriber.PollQueryUntilAsync(database, """
                SELECT (SELECT count(*) FROM zt) = 2
                    AND (SELECT count(*) FROM mt) + (SELECT count(*) FROM worf.exception_log WHERE table_name = 'mt') = 2
                """, token), $"zt's and mt's row 2 weren't applied in {database}");
            Assert.AreEqual(
                "1|4|2024-01-01 22:04:05|\\x41|{angry}|(angry,1)|[\"2024-01-01 22:04:05\",\"2024-01-02 22:04:05\")\n" +
                "2|4|2024-01-01 22:04:05|\\x41|{angry}|(angry,1)|[\"2024-01-01 22:04:05\",\"2024-01-02 22:04:05\")",
                await subscriber.SafePsqlAsync(database, "SELECT concat_ws('|', id, n, tz, by, e, p, r) FROM zt ORDER BY id", token));
            Assert.AreEqual("COPY|zt|discard|22003|2|3|10000000000", await subscriber.SafePsqlAsync(database, """
                SELECT concat_ws('|', operation, table_name, behaviour, error_sqlstate, action_counter,
                    remote_new_tuple->>'id', remote_new_tuple->>'n')
                FROM worf.exception_log WHERE table_name = 'zt'
                """, token));
            Assert.AreEqual(await provider.SafePsqlAsync(Database, "SELECT concat_ws('|', id, m) FROM mt ORDER BY id", token),
                await subscriber.SafePsqlAsync(database, "SELECT concat_ws('|', id, m) FROM mt ORDER BY id", token));
            Assert.AreEqual("", await subscriber.SafePsqlAsync(database, """
                SELECT concat_ws('|', operation, error_sqlstate, remote_new_tuple->>'id')
                FROM worf.exception_log WHERE table_name = 'mt' ORDER BY log_id
                """, token));

            // Money counted in other fractional units waited too, rather than turn ¥1,234 into 12.34, and then means what
            // it does on the provider.
            string mnt = await provider.SafePsqlAsync(Database, "SELECT concat_ws('|', id, m::numeric) FROM mnt", token);
            Assert.AreEqual(mnt, await subscriber.SafePsqlAsync(database, """
                SELECT concat_ws('|', (SELECT string_agg(concat_ws('|', id, m), ',') FROM mnt),
                    (SELECT string_agg(concat_ws('|', operation, error_sqlstate), ',') FROM worf.exception_log WHERE table_name = 'mnt'))
                """, token), "mnt's rows and failures");
        }

        // A row whose value fails to convert has the table copied once more, in each database, in batches, which leave it
        // out, as a copy that skips rows first copies them all at once, and again as a copy waits for the money format.
        Assert.IsGreaterThanOrEqualTo(2, CountOccurrences(subscriber.ReadLog()[moneyLogpos..], "copying table public.zt again"), "zt's copies");

        // Each row the copy can't write is skipped and recorded under discard, numbered as the provider's row it is.
        Assert.AreEqual("1\n5", await subscriber.SafePsqlAsync(TextDatabase, "SELECT id FROM ct ORDER BY id", token));
        Assert.AreEqual("COPY|2|22001|2|8\nCOPY|3|23514|3|2\nCOPY|4|22P02|4|2\nCOPY|6|22001|6|300", await subscriber.SafePsqlAsync(TextDatabase, """
            SELECT concat_ws('|', operation, action_counter, error_sqlstate, remote_new_tuple->>'id', length(remote_new_tuple->>'v'))
            FROM worf.exception_log WHERE table_name = 'ct' ORDER BY action_counter
            """, token));

        // Under error, a copy whose value fails to convert stops, naming the row, column and value as COPY does, and
        // starts again only after a delay: by the server's clock, it hasn't two seconds after it failed.
        logpos = subscriber.ReadLog().Length;
        _ = await subscriber.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.exception_behaviour = error", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE ft (id integer PRIMARY KEY, n integer)", token);
        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE ft (id integer PRIMARY KEY, n numeric);
            INSERT INTO ft VALUES (1, 1e10);
            SELECT * FROM worf.replication_set_add_table('default', 'ft', synchronize_data := true);
            """, token);
        Assert.IsTrue(await subscriber.PollLogAsync(logpos, "COPY ft, line 1, column n: \"10000000000\"", token),
            "ft's copy didn't fail");
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        DateTime failed = subscriber.LogTimes(logpos, "COPY ft, line 1, column n")[0];
        _ = Assert.ContainsSingle(subscriber.LogTimes(logpos, "starting sync of table public.ft").Where(at => at <= failed.AddSeconds(2)), "ft's syncs");

        // Under sub_disable, a copy whose row fails disables the subscription and records the row that COPY names,
        // without copying the rows again to find it, and says the row failed, whether a CHECK constraint failed it or,
        // in the other database, as ft's or dt's, a value failed to convert.
        logpos = subscriber.ReadLog().Length;
        _ = await subscriber.SafePsqlAsync(Database, "ALTER SYSTEM SET worf.exception_behaviour = sub_disable", token);
        _ = await subscriber.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        _ = await subscriber.SafePsqlAsync(Database, "CREATE TABLE dt (id integer PRIMARY KEY, n integer)", token);
        _ = await subscriber.SafePsqlAsync(TextDatabase, "CREATE TABLE dt (id integer PRIMARY KEY, n numeric CHECK (n <> 3))", token);

        _ = await provider.SafePsqlAsync(Database, """
            CREATE TABLE dt (id integer PRIMARY KEY, n numeric);
            INSERT INTO dt VALUES (1, 1), (2, 1e10), (3, 3);
            SELECT * FROM worf.replication_set_add_table('zs', 'dt', synchronize_data := true);
            """, token);
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(TextDatabase, "SELECT NOT sub_enabled FROM worf.subscription", token),
            "textsub wasn't disabled");
        Assert.AreEqual("sub_disable|COPY|3|3|23514", await subscriber.SafePsqlAsync(TextDatabase, """
            SELECT concat_ws('|', behaviour, operation, action_counter, remote_new_tuple->>'id', error_sqlstate)
            FROM worf.exception_log WHERE table_name = 'dt'
            """, token));
        Assert.IsTrue(await subscriber.PollQueryUntilAsync(Database, "SELECT NOT sub_enabled FROM worf.subscription", token), "sub wasn't disabled");

        // A sync worker says it disabled its subscription once that has committed, so the message may reach the log
        // after a query sees the subscription disabled.
        Assert.IsTrue(await subscriber.PollLogAsync(logpos, "disabling subscription textsub after a row of table public.dt failed in its copy", token),
            "textsub's message");
        Assert.IsTrue(await subscriber.PollLogAsync(logpos, "disabling subscription sub after a row of table public.", token), "sub's message");
        string disabled = subscriber.ReadLog()[logpos..];
        Assert.AreEqual(0, CountOccurrences(disabled, "copying table public.dt again"));
        Assert.DoesNotContain("failed to convert", disabled);

        // The subscriptions' first syncs, which their sequence range exchanges can get ahead of, logged no failure for
        // want of their slots, on either node.
        foreach (TestNode node in new[] { provider, subscriber })
        {
            Assert.DoesNotContain("carries this sequence range exchange", node.ReadLog(), node.Name);
        }
    }

    /// <summary>
    /// Sets the provider's lc_monetary to a locale that formats money otherwise than the subscriber's: one the host
    /// has, as macOS and Windows have ja_JP and en_GB, or else, on Linux, one generated with localedef, which the
    /// provider is started again to find. Says so when there's neither, and money then replicates as it is.
    /// </summary>
    /// <returns>Whether there was one.</returns>
    private async Task<bool> FormatMoneyDifferentlyAsync(TestNode provider, TestNode subscriber, string locales,
        CancellationToken token)
    {
        const string Format = "SELECT '-1234567'::money::text";
        string format = await subscriber.SafePsqlAsync(Database, Format, token);
        string? locale = await MoneyLocaleAsync(provider, format, token);
        if (locale is null && OperatingSystem.IsLinux() && await GenerateLocalesAsync(locales, token))
        {
            await provider.StopAsync("fast", token);
            await provider.StartServerAsync(token, environment: new Dictionary<string, string> { ["LOCPATH"] = locales });
            locale = await MoneyLocaleAsync(provider, format, token);
        }

        if (locale is null)
        {
            TestContext.WriteLine("No locale formats money otherwise than the subscriber's here, so the test skips money that fails.");
            return false;
        }

        _ = await provider.SafePsqlAsync(Database, $"ALTER SYSTEM SET lc_monetary = '{locale}'", token);
        _ = await provider.SafePsqlAsync(Database, "SELECT pg_reload_conf()", token);
        Assert.IsTrue(await provider.PollQueryUntilAsync(Database, $"SELECT '-1234567'::money::text <> '{format}'", token),
            "lc_monetary didn't take effect");
        return true;

        static async Task<string?> MoneyLocaleAsync(TestNode provider, string format, CancellationToken token)
        {
            await using NpgsqlConnection connection = await provider.OpenAsync(Database, token);
            foreach (string locale in new[] { "ja_JP.UTF-8", "ja_JP.utf8", "ja-JP", "en_GB.UTF-8", "en_GB.utf8", "en-GB" })
            {
                if (await MoneyFormatAsync(connection, locale, token) is string localeFormat && localeFormat != format)
                {
                    return locale;
                }
            }

            return null;
        }

        static async Task<string?> MoneyFormatAsync(NpgsqlConnection connection, string locale, CancellationToken token)
        {
            try
            {
                await using NpgsqlCommand set = new($"SET lc_monetary = '{locale}'", connection);
                _ = await set.ExecuteNonQueryAsync(token);
                await using NpgsqlCommand query = new(Format, connection);
                return (string?)await query.ExecuteScalarAsync(token);
            }
            catch (PostgresException)
            {
                // The host lacks the locale.
                return null;
            }
        }
    }

    /// <summary>
    /// Generates the Japanese locale, and the C.UTF-8 locale the nodes start under, for a server that looks for
    /// locales only in the directory, as LOCPATH makes it.
    /// </summary>
    /// <returns>Whether localedef made them.</returns>
    private static async Task<bool> GenerateLocalesAsync(string locales, CancellationToken token)
    {
        foreach ((string input, string name) in new[] { ("ja_JP", "ja_JP.utf8"), ("C", "C.utf8") })
        {
            try
            {
                _ = await ProcessRunner.RunAsync("localedef", ["-i", input, "-f", "UTF-8", Path.Join(locales, name)], token);
            }
            catch (Win32Exception)
            {
                // The host has no localedef.
                return false;
            }

            if (!File.Exists(Path.Join(locales, name, "LC_MONETARY")))
            {
                return false;
            }
        }

        return true;
    }

    private static int CountOccurrences(string text, string value)
    {
        int count = 0;
        for (int i = text.IndexOf(value, StringComparison.Ordinal); i >= 0; i = text.IndexOf(value, i + value.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
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
