using System.Text;
using System.Text.Json;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Copies a table whose values convert from the provider's types through PostgreSQL's own <c>COPY FROM</c>, as the
/// other tables are copied, with each converted column's input function replaced by its conversion, so that values
/// convert as replication converts them sent as text, and an error names the row and column as COPY's do. Under
/// <c>worf.exception_behaviour</c> other than <c>error</c>, each row's conversions are tried first, and a row whose
/// value fails to convert is skipped and recorded in <c>worf.exception_log</c>, or disables the subscription once the
/// copy rolls back.
/// </summary>
internal static unsafe partial class CopyConversion
{
    // The operation the exception log records a row of the copy as.
    private const string Operation = "COPY";

    // The copy in progress: the connection its rows come from, the row being passed to COPY and how much of it is
    // left, each converted column's conversion and own input function by its number, and the copy's state.
    private static PgReplicationConnection? s_conn;
    private static byte[] s_row = new byte[8192];
    private static int s_rowStart;
    private static int s_rowEnd;
    private static WorfColumnConversion?[] s_conversions = [];
    private static FmgrInfo* s_inputs;
    private static CopyFromStateData* s_cstate;

    // How the rows are checked when worf.exception_behaviour skips or records them: the conversions of the copied
    // columns in order, the columns' names, the behaviour, what the exception log records with each row, and the
    // number of rows read so far.
    private static WorfColumnConversion?[]? s_checks;
    private static List<string> s_columns = [];
    private static WorfExceptionBehaviour s_behaviour;
    private static WorfFailedChange? s_change;
    private static ulong s_line;

    // A row that failed under sub_disable, which disables the subscription once the copy rolls back.
    private static (WorfFailedChange Change, PgException Error)? s_disabling;

    [PgNativeCallback(nameof(ReadRow))]
    private static partial copy_data_source_cb DataSource { get; }

    [PgNativeCallback(nameof(InputConverted))]
    private static partial PGFunction ConvertedInput { get; }

    /// <summary>
    /// Copies the rows of a <c>COPY ... TO STDOUT</c> started on the connection into a local table, converting the
    /// columns that need it. Runs in the copy's target transaction.
    /// </summary>
    /// <param name="conn">The provider connection, whose copy has started.</param>
    /// <param name="rel">The remote relation, mapped to the local table.</param>
    /// <param name="attnames">The copied columns, in order.</param>
    /// <param name="conversions">Each local column's conversion, by its number less one, or null where the column's
    /// own input reads the provider's text.</param>
    /// <param name="commitLsn">The position the copy is consistent with, which the exception log records.</param>
    /// <returns>The number of rows copied.</returns>
    internal static ulong Copy(PgReplicationConnection conn, WorfRelation rel, List<string> attnames,
        WorfColumnConversion?[] conversions, ulong commitLsn)
    {
        RelationData* relation = NativeMethods.table_open(rel.Reloid, RowExclusiveLock);
        try
        {
            ParseState* pstate = NativeMethods.make_parsestate(null);
            _ = NativeMethods.addRangeTableEntryForRelation(pstate, relation, AccessShareLock, null, false, false);
            List* attnamelist = null;
            foreach (string attname in attnames)
            {
                attnamelist = NativeMethods.lappend(attnamelist, NativeMethods.makeString(Worf.PStrDup(attname)));
            }

            s_conn = conn;
            s_rowStart = s_rowEnd = 0;
            s_conversions = conversions;
            s_checks = null;
            s_line = 0;
            CopyFromStateData* cstate = NativeMethods.BeginCopyFrom(pstate, relation, null, null, false, DataSource, attnamelist, null);
            s_cstate = cstate;

            // The converted columns' input functions convert, and keep the column type's own for nulls, which a
            // domain's input checks.
            s_inputs = (FmgrInfo*)NativeMethods.palloc0((ulong)(sizeof(FmgrInfo) * Math.Max(conversions.Length, 1)));
            for (int i = 0; i < conversions.Length; i++)
            {
                if (conversions[i] is not null)
                {
                    FmgrInfo* input = &cstate->in_functions[i];
                    s_inputs[i] = *input;
                    input->fn_addr = ConvertedInput;
                    input->fn_extra = (void*)(nint)i;
                }
            }

            // Unless worf.exception_behaviour stops at a failure, each row's conversions are tried before it's copied.
            if (Apply.ExceptionBehaviour != WorfExceptionBehaviour.Error)
            {
                WorfSubscription sub = Apply.MySubscription!;
                s_checks = [.. attnames.Select(attname => conversions[NativeMethods.attnameAttNum(relation, Worf.PStrDup(attname), false) - 1])];
                s_columns = attnames;
                s_behaviour = Apply.ExceptionBehaviour;
                s_change = new WorfFailedChange
                {
                    Subscription = sub,
                    WriterNodeId = sub.Origin.Id,
                    CommitTime = NativeMethods.GetCurrentTransactionStartTimestamp(),
                    CommitLsn = commitLsn,
                    Operation = Operation,
                    Relation = rel,
                };
            }

            NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());
            ulong copied = NativeMethods.CopyFrom(cstate);
            NativeMethods.PopActiveSnapshot();
            NativeMethods.EndCopyFrom(cstate);
            return copied;
        }
        finally
        {
            s_conn = null;
            s_cstate = null;
            s_inputs = null;
            s_conversions = [];
            s_checks = null;
            s_change = null;
            NativeMethods.table_close(relation, NoLock);
        }
    }

    /// <summary>
    /// Disables the subscription after a row of its copy failed under <c>sub_disable</c>, once the copy rolled back,
    /// and records the row, as apply does with a failed remote transaction.
    /// </summary>
    internal static void DisableAfterFailure()
    {
        if (s_disabling is not (WorfFailedChange change, PgException error))
        {
            return;
        }

        s_disabling = null;
        PgBackgroundWorker.RunTransaction(() =>
        {
            ExceptionLog.Record(change, WorfExceptionBehaviour.SubDisable, error);
            WorfSubscription sub = WorfNode.GetSubscription(change.Subscription.Id);
            sub.Enabled = false;
            WorfNode.AlterSubscription(sub);
        });
        PgLog.Log($"disabling subscription {change.Subscription.Name} after a row of table {change.Relation!.Nspname}.{change.Relation.Relname} failed to convert in its copy");
    }

    /// <summary>
    /// COPY's data source: passes it the provider's rows one at a time, so that its line numbers count the
    /// provider's rows, and skips those whose values fail to convert when worf.exception_behaviour says so.
    /// </summary>
    private static int ReadRow(void* outbuf, int minread, int maxread)
    {
        _ = minread;
        while (s_rowStart == s_rowEnd)
        {
            PgReplicationReceiveStatus status = s_conn!.Receive(TimeSpan.FromSeconds(1), out ReadOnlySpan<byte> message);
            if (status == PgReplicationReceiveStatus.Ended)
            {
                return 0;
            }

            if (status != PgReplicationReceiveStatus.Message)
            {
                PgInterrupts.Check();
                continue;
            }

            if (s_row.Length < message.Length)
            {
                s_row = new byte[Math.Max(message.Length, s_row.Length * 2)];
            }

            message.CopyTo(s_row);
            s_rowStart = 0;
            s_rowEnd = message.Length;

            // A row ends with its newline, which a message carries.
            if (message.Length > 0 && message[^1] == (byte)'\n')
            {
                s_line++;
                if (s_checks is not null && !CheckRow(new ReadOnlySpan<byte>(s_row, 0, s_rowEnd - 1)))
                {
                    s_rowStart = s_rowEnd = 0;
                    continue;
                }

                // COPY counts the rows it reads, and those skipped too.
                s_cstate->cur_lineno = s_line;
            }
        }

        int len = Math.Min(maxread, s_rowEnd - s_rowStart);
        new ReadOnlySpan<byte>(s_row, s_rowStart, len).CopyTo(new Span<byte>(outbuf, len));
        s_rowStart += len;
        return len;
    }

    /// <summary>
    /// Tries a row's conversions, and records a row whose value fails: skipped, or kept to disable the subscription
    /// once the copy fails on it and rolls back.
    /// </summary>
    /// <returns>Whether the row goes on to COPY.</returns>
    private static bool CheckRow(ReadOnlySpan<byte> row)
    {
        List<byte[]?> fields = ReadFields(row);
        if (fields.Count != s_checks!.Length)
        {
            return true;
        }

        try
        {
            PgTransaction.RunInSubtransaction(() =>
            {
                for (int i = 0; i < fields.Count; i++)
                {
                    if (s_checks[i] is { } conversion && fields[i] is byte[] field)
                    {
                        fixed (byte* text = field)
                        {
                            _ = conversion.ConvertText((sbyte*)text, out _);
                        }
                    }
                }
            }, PgSubtransactionMode.Atomic);
            return true;
        }
        catch (PgException error) when (!Apply.IsTransient(error))
        {
            WorfFailedChange change = new()
            {
                Subscription = s_change!.Subscription,
                WriterNodeId = s_change.WriterNodeId,
                CommitTime = s_change.CommitTime,
                CommitLsn = s_change.CommitLsn,
                ActionCounter = (uint)s_line,
                Operation = Operation,
                Relation = s_change.Relation,
                NewTuple = RowJson(fields),
            };
            // Under sub_disable, the row goes on to fail in COPY, which names it and its column.
            if (s_behaviour == WorfExceptionBehaviour.SubDisable)
            {
                s_disabling = (change, error);
                return true;
            }

            ExceptionLog.Record(change, WorfExceptionBehaviour.Discard, error);
            return false;
        }
    }

    /// <summary>
    /// Reads a row's fields as COPY's text format writes them, each with its terminating zero, or null.
    /// </summary>
    private static List<byte[]?> ReadFields(ReadOnlySpan<byte> row)
    {
        List<byte[]?> fields = [];
        List<byte> field = [];
        int start = 0;
        for (int i = 0; i <= row.Length; i++)
        {
            if (i < row.Length && row[i] != (byte)'\t')
            {
                if (row[i] != (byte)'\\' || i + 1 == row.Length)
                {
                    field.Add(row[i]);
                    continue;
                }

                byte c = row[++i];
                switch (c)
                {
                    case (byte)'b': field.Add(8); break;
                    case (byte)'f': field.Add(12); break;
                    case (byte)'n': field.Add(10); break;
                    case (byte)'r': field.Add(13); break;
                    case (byte)'t': field.Add(9); break;
                    case (byte)'v': field.Add(11); break;
                    case >= (byte)'0' and <= (byte)'7':
                    {
                        int value = c - '0';
                        for (int n = 1; n < 3 && i + 1 < row.Length && row[i + 1] is >= (byte)'0' and <= (byte)'7'; n++)
                        {
                            value = (value << 3) + (row[++i] - '0');
                        }

                        field.Add((byte)value);
                        break;
                    }

                    case (byte)'x' when i + 1 < row.Length && char.IsAsciiHexDigit((char)row[i + 1]):
                    {
                        int value = HexValue(row[++i]);
                        if (i + 1 < row.Length && char.IsAsciiHexDigit((char)row[i + 1]))
                        {
                            value = (value << 4) + HexValue(row[++i]);
                        }

                        field.Add((byte)value);
                        break;
                    }

                    default: field.Add(c); break;
                }

                continue;
            }

            // A field that's exactly \N is null.
            if (row[start..i].SequenceEqual("\\N"u8))
            {
                fields.Add(null);
            }
            else
            {
                field.Add(0);
                fields.Add([.. field]);
            }

            field.Clear();
            start = i + 1;
        }

        return fields;
    }

    private static int HexValue(byte digit) => digit <= (byte)'9' ? digit - '0' : (digit | 0x20) - 'a' + 10;

    /// <summary>
    /// Writes a row's fields as the exception log records a row: a JSON object of the columns' text.
    /// </summary>
    private static string RowJson(List<byte[]?> fields)
    {
        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream))
        {
            writer.WriteStartObject();
            for (int i = 0; i < fields.Count; i++)
            {
                if (fields[i] is byte[] field)
                {
                    writer.WriteString(s_columns[i], Worf.FromServer(field.AsSpan(0, field.Length - 1)));
                }
                else
                {
                    writer.WriteNull(s_columns[i]);
                }
            }

            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    /// <summary>
    /// A converted column's input function in the copy: converts the provider's text as replication does, and gives
    /// a null to the column type's own input.
    /// </summary>
    private static ulong InputConverted(FunctionCallInfoBaseData* fcinfo)
    {
        Span<NullableDatum> args = FunctionCallInfoBaseData.Dangerous_args(fcinfo, 3);
        int i = (int)(nint)fcinfo->flinfo->fn_extra;
        if (args[0].isnull || args[0].value == 0)
        {
            fcinfo->isnull = true;
            return NativeMethods.InputFunctionCall(&s_inputs[i], null, (uint)args[1].value, (int)args[2].value);
        }

        ulong value = s_conversions[i]!.ConvertText((sbyte*)args[0].value, out bool isnull);
        if (isnull)
        {
            throw new PgException(PgSqlStates.NotNullViolation, "a converted value is null");
        }

        return value;
    }

    private const int AccessShareLock = 1;
    private const int RowExclusiveLock = 3;
    private const int NoLock = 0;
}
