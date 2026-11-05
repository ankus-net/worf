using System.Globalization;
using System.Text;
using Ankus.Postgres;

[assembly: PgSql("exception-log", """
    CREATE TABLE worf.exception_log (
        log_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        logged_at timestamptz NOT NULL DEFAULT clock_timestamp(),
        sub_id oid NOT NULL,
        sub_name name NOT NULL,
        remote_node_id oid NOT NULL,
        remote_xid xid NOT NULL,
        remote_commit_ts timestamptz NOT NULL,
        remote_commit_lsn pg_lsn NOT NULL,
        action_counter bigint NOT NULL,
        operation text,
        schema_name name,
        table_name name,
        remote_old_tuple jsonb,
        remote_new_tuple jsonb,
        queued_message text,
        behaviour text NOT NULL CHECK (behaviour IN ('discard', 'transdiscard', 'sub_disable')),
        error_sqlstate text NOT NULL,
        error_message text NOT NULL
    );

    COMMENT ON TABLE worf.exception_log IS
        'Remote changes and transactions that failed to apply, and what worf.exception_behaviour did with them';

    CREATE TABLE worf.exception_state (
        sub_id oid PRIMARY KEY,
        commit_lsn pg_lsn NOT NULL,
        level integer NOT NULL,
        error_sqlstate text NOT NULL,
        error_message text NOT NULL
    );

    COMMENT ON TABLE worf.exception_state IS
        'The remote transaction each subscription applies again under worf.exception_behaviour, and how';
    """, Requires = ["node-tables"])]

namespace Worf;

/// <summary>
/// Records remote changes and transactions that failed to apply in <c>worf.exception_log</c>.
/// </summary>
internal static class ExceptionLog
{
    private const string InsertSql =
        "INSERT INTO worf.exception_log (sub_id, sub_name, remote_node_id, remote_xid, remote_commit_ts, " +
        "remote_commit_lsn, action_counter, operation, schema_name, table_name, remote_old_tuple, remote_new_tuple, " +
        "queued_message, behaviour, error_sqlstate, error_message) " +
        "VALUES ($1, $2, $3, $4::text::xid, $5, $6, $7, $8, $9, $10, $11::jsonb, $12::jsonb, $13, $14, $15, $16)";

    /// <summary>
    /// Records a failed change, or the change that made a transaction fail.
    /// </summary>
    /// <param name="change">The change, and the transaction it belongs to.</param>
    /// <param name="behaviour">What apply did with it.</param>
    /// <param name="error">The error.</param>
    /// <param name="isolated">Whether a failure to record is reported and otherwise ignored, which takes a
    /// subtransaction of its own; otherwise it fails the transaction, as each change of a transaction skipped as a
    /// whole is recorded without a subtransaction's xid each.</param>
    internal static void Record(WorfFailedChange change, WorfExceptionBehaviour behaviour, PgException error, bool isolated = true)
    {
        (string? oldTuple, string? newTuple) = (null, change.NewTuple);
        (string? schemaName, string? tableName) = (change.Relation?.Nspname, change.Relation?.Relname);

        // A value this node can't read fails the first read of the rows, and the second records its bytes instead.
        for (int read = 0; change.Message is not null && read < 2; read++)
        {
            bool unreadableAsHex = read > 0;
            try
            {
                PgTransaction.RunInSubtransaction(() =>
                {
                    (oldTuple, newTuple, string schema, string table) = ReadTuples(change.Message, unreadableAsHex);
                    schemaName ??= schema;
                    tableName ??= table;
                });
                break;
            }
            catch (PgException)
            {
                // The rows stay unrecorded when the change can't be read again, as when its table is missing. The
                // rollback released the relation the read opened.
                RelCache.ForgetOpenRelations();
            }
        }

        void Insert() => _ = Worf.Execute(InsertSql,
            SpiParameter.Create(change.Subscription.Id),
            Worf.TextParameter(change.Subscription.Name),
            SpiParameter.Create(change.WriterNodeId),
            SpiParameter.Create(change.RemoteXid.ToString(CultureInfo.InvariantCulture)),
            SpiParameter.Create(new PgTimestampTz(change.CommitTime)),
            Worf.LsnParameter(change.CommitLsn),
            SpiParameter.Create(change.ActionCounter),
            SpiParameter.Create<string?>(change.Operation),
            Worf.TextParameter(schemaName),
            Worf.TextParameter(tableName),
            Worf.TextParameter(oldTuple),
            Worf.TextParameter(newTuple),
            Worf.TextParameter(change.QueuedMessage),
            SpiParameter.Create(behaviour switch
            {
                WorfExceptionBehaviour.Discard => "discard",
                WorfExceptionBehaviour.Transdiscard => "transdiscard",
                _ => "sub_disable",
            }),
            SpiParameter.Create(error.SqlState),
            Worf.TextParameter(error.Message));

        if (!isolated)
        {
            Insert();
            return;
        }

        // The insert's memory goes as it ends, so that the subtransaction's own stays empty, which PostgreSQL then frees as it
        // commits rather than keep it until the transaction ends, as a copy that records many rows would.
        try
        {
            PgTransaction.RunInSubtransaction(() => Worf.InOwnContext(Insert));
        }
        catch (PgException)
        {
            WorfLog.Warning("could not record the failed remote change in worf.exception_log");
        }
    }

    /// <summary>
    /// Records rows of a table's copy that failed, all in one statement, which takes one transaction ID for them all
    /// rather than one each, as a copy that skips many rows records them. A statement that fails, as on a row the log
    /// can't hold, rolls back alone, and the rows are recorded one at a time, as apply records its changes.
    /// </summary>
    /// <param name="change">The copy, which the rows share.</param>
    /// <param name="failures">The rows, of the copy, in order: each one's number, its JSON, by where it is in the
    /// rows' text, or a length of -1 when it has none, and its error.</param>
    /// <param name="json">The rows' text, in the database encoding.</param>
    internal static void RecordCopied(WorfFailedChange change, IReadOnlyList<(long ActionCounter, int Start, int Length, PgException Error)> failures,
        byte[] json)
    {
        if (failures.Count == 0)
        {
            return;
        }

        if (TryRecord(failures))
        {
            return;
        }

        for (int i = 0; i < failures.Count; i++)
        {
            if (!TryRecord([failures[i]]))
            {
                WorfLog.Warning("could not record the failed remote change in worf.exception_log");
            }
        }

        bool TryRecord(IReadOnlyList<(long ActionCounter, int Start, int Length, PgException Error)> rows)
        {
            try
            {
                PgTransaction.RunInSubtransaction(() => Worf.InOwnContext(() => InsertCopied(change, rows, json)));
                return true;
            }
            catch (PgException)
            {
                return false;
            }
        }
    }

    /// <summary>
    /// Records rows of a table's copy that failed, which a query gives, each as its JSON and error, in one statement with
    /// the query, so that however many and wide they are, none is kept but in the log.
    /// </summary>
    /// <param name="change">The copy, which the rows share.</param>
    /// <param name="rows">The query, of each row's JSON, SQLSTATE and message, in order, which may change the rows it
    /// gives, as a DELETE … RETURNING does.</param>
    /// <param name="parameters">The query's parameters.</param>
    /// <returns>Whether the rows were recorded; otherwise the query has had no effect, as when the log can't hold
    /// one of them.</returns>
    internal static bool TryRecordCopied(WorfFailedChange change, string rows, params SpiParameter[] parameters)
    {
        int n = parameters.Length;
        try
        {
            PgTransaction.RunInSubtransaction(() => Worf.InOwnContext(() => _ = Worf.Execute($"""
                WITH f (new_tuple, error_sqlstate, error_message) AS ({rows})
                INSERT INTO worf.exception_log (sub_id, sub_name, remote_node_id, remote_xid, remote_commit_ts, remote_commit_lsn,
                    action_counter, operation, schema_name, table_name, remote_new_tuple, behaviour, error_sqlstate, error_message)
                SELECT ${n + 1}, ${n + 2}, ${n + 3}, ${n + 4}::pg_catalog.text::pg_catalog.xid, ${n + 5}, ${n + 6}, 0, ${n + 7}, ${n + 8},
                    ${n + 9}, f.new_tuple::pg_catalog.jsonb, ${n + 10}, f.error_sqlstate, f.error_message
                FROM f
                """,
                [.. parameters,
                    SpiParameter.Create(change.Subscription.Id),
                    SpiParameter.Create(change.Subscription.Name),
                    SpiParameter.Create(change.WriterNodeId),
                    SpiParameter.Create(change.RemoteXid.ToString(CultureInfo.InvariantCulture)),
                    SpiParameter.Create(new PgTimestampTz(change.CommitTime)),
                    Worf.LsnParameter(change.CommitLsn),
                    SpiParameter.Create<string?>(change.Operation),
                    SpiParameter.Create<string?>(change.Relation?.Nspname),
                    SpiParameter.Create<string?>(change.Relation?.Relname),
                    SpiParameter.Create(CopyBehaviour)])));
            return true;
        }
        catch (PgException)
        {
            return false;
        }
    }

    /// <summary>
    /// Inserts rows of a table's copy that failed into the log, in one statement, which takes their text as it is.
    /// </summary>
    private static void InsertCopied(WorfFailedChange change, IReadOnlyList<(long ActionCounter, int Start, int Length, PgException Error)> failures,
        byte[] json)
        => _ = Worf.Execute("""
            INSERT INTO worf.exception_log (sub_id, sub_name, remote_node_id, remote_xid, remote_commit_ts, remote_commit_lsn,
                action_counter, operation, schema_name, table_name, remote_new_tuple, behaviour, error_sqlstate, error_message)
            SELECT $1, $2, $3, $4::text::xid, $5, $6, f.action_counter, $7, $8, $9, f.new_tuple::jsonb, $14,
                f.error_sqlstate, f.error_message
            FROM ROWS FROM (pg_catalog.unnest($10::bigint[]), pg_catalog.unnest($11::text[]), pg_catalog.unnest($12::text[]),
                pg_catalog.unnest($13::text[])) WITH ORDINALITY AS f (action_counter, new_tuple, error_sqlstate, error_message, n)
            ORDER BY f.n
            """,
            SpiParameter.Create(change.Subscription.Id),
            SpiParameter.Create(change.Subscription.Name),
            SpiParameter.Create(change.WriterNodeId),
            SpiParameter.Create(change.RemoteXid.ToString(CultureInfo.InvariantCulture)),
            SpiParameter.Create(new PgTimestampTz(change.CommitTime)),
            Worf.LsnParameter(change.CommitLsn),
            SpiParameter.Create<string?>(change.Operation),
            SpiParameter.Create<string?>(change.Relation?.Nspname),
            SpiParameter.Create<string?>(change.Relation?.Relname),
            SpiParameter.Create(failures.Select(failure => failure.ActionCounter).ToArray()),
            Worf.TextArrayParameter(json, [.. failures.Select(failure => (failure.Start, failure.Length))]),
            SpiParameter.Create(failures.Select(failure => failure.Error.SqlState).ToArray()),
            SpiParameter.Create(failures.Select(failure => failure.Error.Message).ToArray()),
            SpiParameter.Create(CopyBehaviour));

    // The behaviour a copy's skipped rows are recorded under: transdiscard skips a copy's rows one by one, as discard
    // does, and records them under its own name, as apply does the changes it skips.
    private static string CopyBehaviour => Apply.ExceptionBehaviour == WorfExceptionBehaviour.Transdiscard ? "transdiscard" : "discard";

    /// <summary>
    /// Records the remote transaction a subscription applies again under <c>worf.exception_behaviour</c>, how, and
    /// the error that made it, so that whichever worker applies it next, even after a restart, knows.
    /// </summary>
    /// <param name="subId">The subscription.</param>
    /// <param name="commitLsn">The transaction's commit position.</param>
    /// <param name="level">How it's applied again.</param>
    /// <param name="error">The error.</param>
    internal static void SaveState(uint subId, ulong commitLsn, int level, PgException error)
        => _ = Worf.Execute("""
            INSERT INTO worf.exception_state VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (sub_id) DO UPDATE SET commit_lsn = excluded.commit_lsn, level = excluded.level,
                error_sqlstate = excluded.error_sqlstate, error_message = excluded.error_message
            """, SpiParameter.Create(subId), Worf.LsnParameter(commitLsn), SpiParameter.Create(level), SpiParameter.Create(error.SqlState),
            SpiParameter.Create(error.Message));

    /// <summary>
    /// Reads the remote transaction a subscription applies again under <c>worf.exception_behaviour</c>.
    /// </summary>
    /// <param name="subId">The subscription.</param>
    /// <returns>Its commit position, how it's applied again and the error, or zeros and null for none.</returns>
    internal static (ulong CommitLsn, int Level, PgException? Error) State(uint subId)
    {
        WorfRows state = Worf.SelectCatalog(
            "SELECT commit_lsn::text, level, error_sqlstate, error_message FROM worf.exception_state WHERE sub_id = $1",
            SpiParameter.Create(subId));
        return state.Count == 0 || !Worf.TryParseLsn(state[0].Get<string>(0), out ulong lsn) ? (0, 0, null)
            : (lsn, state[0].Get<int>(1), new PgException(state[0].Get<string>(2), state[0].Get<string>(3)));
    }

    /// <summary>
    /// Forgets the remote transaction a subscription applied again.
    /// </summary>
    /// <param name="subId">The subscription.</param>
    internal static void ForgetState(uint subId)
        => _ = Worf.Execute("DELETE FROM worf.exception_state WHERE sub_id = $1", SpiParameter.Create(subId));

    /// <summary>
    /// Reads a change's rows from its message again, as JSON objects of the text of the columns it sent.
    /// </summary>
    /// <param name="message">The INSERT, UPDATE or DELETE message.</param>
    /// <param name="unreadableAsHex">Whether a value this node can't read is recorded as its bytes rather than failing
    /// the read, which then reads each value in a subtransaction.</param>
    /// <returns>The key of the row an update or delete changes, and the row an insert or update writes.</returns>
    private static unsafe (string? OldTuple, string? NewTuple, string Schema, string Table) ReadTuples(byte[] message,
        bool unreadableAsHex)
    {
        fixed (byte* data = message)
        {
            StringInfoData s;
            s.data = (sbyte*)data;
            s.len = message.Length;
            s.maxlen = -1;
            s.cursor = 0;

            WorfRelation rel;
            WorfTupleData? oldtup = null;
            WorfTupleData? newtup = null;
            switch ((char)NativeMethods.pq_getmsgbyte(&s))
            {
                case 'I':
                    rel = ProtoNative.ReadInsert(&s, PgLockMode.AccessShare, out WorfTupleData inserted);
                    newtup = inserted;
                    break;

                case 'U':
                    rel = ProtoNative.ReadUpdate(&s, PgLockMode.AccessShare, out bool hasoldtup, out WorfTupleData? key,
                        out WorfTupleData updated);
                    oldtup = hasoldtup ? key : null;
                    newtup = updated;
                    break;

                default:
                    rel = ProtoNative.ReadDelete(&s, PgLockMode.AccessShare, out WorfTupleData deleted);
                    oldtup = deleted;
                    break;
            }

            try
            {
                return (TupleToJson(rel, oldtup, unreadableAsHex), TupleToJson(rel, newtup, unreadableAsHex), rel.Nspname,
                    rel.Relname);
            }
            finally
            {
                RelCache.RelationClose(rel, PgLockMode.None);
            }
        }
    }

    /// <summary>
    /// Converts the columns a remote change sent to a JSON object of their text.
    /// </summary>
    private static unsafe string? TupleToJson(WorfRelation rel, WorfTupleData? tuple, bool unreadableAsHex)
    {
        if (tuple is null)
        {
            return null;
        }

        TupleDescData* desc = rel.Rel->rd_att;
        StringBuilder json = new("{");
        for (int natt = 0; natt < desc->natts && natt < tuple.Kinds.Length; natt++)
        {
            FormData_pg_attribute* attr = NativeMethods.TupleDescAttr(desc, natt);
            if (attr->attisdropped || !tuple.Changed[natt])
            {
                continue;
            }

            if (json.Length > 1)
            {
                json.Append(", ");
            }

            Executor.EscapeJson(json, Worf.FromServer((sbyte*)&attr->attname));
            json.Append(": ");
            switch (tuple.Kinds[natt])
            {
                case 'n':
                    json.Append("null");
                    break;

                // Text is recorded as it came, and so is an enum's value in binary, its label, which is also its text.
                case 't':
                case 'b' when rel.RemoteType(natt)?.Definition == ProtoNative.EnumDefinition:
                    Executor.EscapeJson(json, Worf.LoggedText(tuple.Data[natt]!));
                    break;

                default:
                    Executor.EscapeJson(json, tuple.Converted[natt] ? ValueToText(attr->atttypid, tuple.Values[natt])
                        : BinaryToText(attr, tuple, natt, unreadableAsHex));
                    break;
            }
        }

        return json.Append('}').ToString();
    }

    /// <summary>
    /// Gets the text of a value sent in binary, or in the internal format of another type than its column's, read
    /// with the type it converts from, or else the column's own, without the column's domain constraints. A value this
    /// node can't read fails, or is recorded as its bytes in hex, as bytea shows them.
    /// </summary>
    private static unsafe string BinaryToText(FormData_pg_attribute* attr, WorfTupleData tuple, int natt, bool unreadableAsHex)
    {
        char kind = tuple.Kinds[natt];
        byte[] data = tuple.Data[natt]!;
        uint type = tuple.Conversions?[natt] is { } conversion ? conversion.ReadType : NativeMethods.getBaseType(attr->atttypid);
        if (type != 0 && !unreadableAsHex)
        {
            return ValueToText(type, WorfColumnConversion.Read(type, kind, data));
        }

        string? text = null;
        if (type != 0)
        {
            try
            {
                PgTransaction.RunInSubtransaction(() => text = ValueToText(type, WorfColumnConversion.Read(type, kind, data)));
            }
            catch (PgException)
            {
                // The bytes are recorded instead.
            }
        }

        return text ?? "\\x" + Convert.ToHexStringLower(data);
    }

    private static unsafe string ValueToText(uint type, ulong value)
    {
        uint typoutput;
        bool typisvarlena;
        NativeMethods.getTypeOutputInfo(type, &typoutput, &typisvarlena);
        return Worf.LoggedText(NativeMethods.OidOutputFunctionCall(typoutput, value));
    }
}
