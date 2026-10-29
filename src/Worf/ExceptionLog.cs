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
        action_counter integer NOT NULL,
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
    /// Records a failed change, or the change that made a transaction fail. A failure to record is reported and
    /// otherwise ignored.
    /// </summary>
    /// <param name="change">The change, and the transaction it belongs to.</param>
    /// <param name="behaviour">What apply did with it.</param>
    /// <param name="error">The error.</param>
    internal static void Record(WorfFailedChange change, WorfExceptionBehaviour behaviour, PgException error)
    {
        (string? oldTuple, string? newTuple) = (null, null);
        if (change.Message is not null)
        {
            try
            {
                PgTransaction.RunInSubtransaction(() => (oldTuple, newTuple) = ReadTuples(change.Message));
            }
            catch (PgException)
            {
                // The rows stay unrecorded when the change can't be read again, as when its table is missing.
            }
        }

        try
        {
            PgTransaction.RunInSubtransaction(() => _ = Spi.Execute(InsertSql,
                SpiParameter.Create(change.Subscription.Id),
                SpiParameter.Create(change.Subscription.Name),
                SpiParameter.Create(change.WriterNodeId),
                SpiParameter.Create(change.RemoteXid.ToString(CultureInfo.InvariantCulture)),
                SpiParameter.Create(new PgTimestampTz(change.CommitTime)),
                Worf.LsnParameter(change.CommitLsn),
                SpiParameter.Create((int)change.ActionCounter),
                SpiParameter.Create<string?>(change.Operation),
                SpiParameter.Create<string?>(change.Relation?.Nspname),
                SpiParameter.Create<string?>(change.Relation?.Relname),
                SpiParameter.Create<string?>(oldTuple),
                SpiParameter.Create<string?>(newTuple),
                SpiParameter.Create<string?>(change.QueuedMessage),
                SpiParameter.Create(behaviour switch
                {
                    WorfExceptionBehaviour.Discard => "discard",
                    WorfExceptionBehaviour.Transdiscard => "transdiscard",
                    _ => "sub_disable",
                }),
                SpiParameter.Create(error.SqlState),
                SpiParameter.Create(error.Message)));
        }
        catch (PgException)
        {
            PgLog.Warning("could not record the failed remote change in worf.exception_log");
        }
    }

    /// <summary>
    /// Reads a change's rows from its message again, as JSON objects of the text of the columns it sent.
    /// </summary>
    /// <param name="message">The INSERT, UPDATE or DELETE message.</param>
    /// <returns>The key of the row an update or delete changes, and the row an insert or update writes.</returns>
    private static unsafe (string? OldTuple, string? NewTuple) ReadTuples(byte[] message)
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
                TupleDescData* desc = rel.Rel->rd_att;
                return (TupleToJson(desc, oldtup), TupleToJson(desc, newtup));
            }
            finally
            {
                RelCache.RelationClose(rel, PgLockMode.None);
            }
        }
    }

    /// <summary>
    /// Converts the columns a remote change sent to a JSON object of their text. Values in the internal format point
    /// into the message, which has to stay pinned.
    /// </summary>
    private static unsafe string? TupleToJson(TupleDescData* desc, WorfTupleData? tuple)
    {
        if (tuple is null)
        {
            return null;
        }

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

                case 't':
                    Executor.EscapeJson(json, Worf.FromServer(tuple.Data[natt]!));
                    break;

                case 'b':
                    Executor.EscapeJson(json, ValueToText(attr, BinaryToValue(attr, tuple.Data[natt]!)));
                    break;

                default:
                    Executor.EscapeJson(json, ValueToText(attr, tuple.Values[natt]));
                    break;
            }
        }

        return json.Append('}').ToString();
    }

    /// <summary>
    /// Converts a value in its type's binary format to a datum.
    /// </summary>
    private static unsafe ulong BinaryToValue(FormData_pg_attribute* attr, byte[] data)
    {
        uint typreceive;
        uint typioparam;
        NativeMethods.getTypeBinaryInputInfo(attr->atttypid, &typreceive, &typioparam);

        StringInfoData buf;
        NativeMethods.initStringInfo(&buf);
        fixed (byte* bytes = data)
        {
            Worf.AppendBinaryStringInfo(&buf, bytes, data.Length);
        }

        return NativeMethods.OidReceiveFunctionCall(typreceive, &buf, typioparam, attr->atttypmod);
    }

    private static unsafe string ValueToText(FormData_pg_attribute* attr, ulong value)
    {
        uint typoutput;
        bool typisvarlena;
        NativeMethods.getTypeOutputInfo(attr->atttypid, &typoutput, &typisvarlena);
        return Worf.FromServer(NativeMethods.OidOutputFunctionCall(typoutput, value));
    }
}
