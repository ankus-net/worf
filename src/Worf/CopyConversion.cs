using System.Text;
using System.Text.Json;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Copies a table whose values convert from the provider's types through PostgreSQL's own <c>COPY FROM</c>, as the
/// other tables are copied. Each row's converted values are converted as replication converts a value sent as text,
/// and written as the column type's output writes them, which COPY's input reads back, and a value that fails names
/// its row and column as COPY's errors do. Under <c>worf.exception_behaviour</c> other than <c>error</c>, which copies
/// every table this way, a row that fails, whether a value fails to convert, its column's input refuses it or a
/// constraint does, is skipped and recorded in <c>worf.exception_log</c>, or disables the subscription once the copy
/// rolls back. A row whose value fails to convert is skipped as it's read; any other failure has the table copied
/// again a row at a time, each in its own <c>COPY</c> and subtransaction.
/// </summary>
internal static unsafe partial class CopyConversion
{
    // The operation the exception log records a row of the copy as.
    private const string Operation = "COPY";

    // The copy in progress: the connection its rows come from and whether they ended, the number of the last row
    // read, a row still to pass to COPY, how much of it is left and its number, and COPY's state.
    private static PgReplicationConnection? s_conn;
    private static bool s_ended;
    private static ulong s_line;
    private static byte[] s_row = new byte[8192];
    private static int s_rowStart;
    private static int s_rowEnd;
    private static ulong s_rowLine;
    private static CopyFromStateData* s_cstate;

    // Each copied column's conversion, or null, a converted column's name and its type's output function, the memory
    // context a row converts in, a value's text, which COPY's errors show, and the row as converted.
    private static WorfColumnConversion?[] s_conversions = [];
    private static sbyte** s_names;
    private static FmgrInfo* s_outputs;
    private static MemoryContextData* s_context;
    private static byte* s_text;
    private static int s_textSize;
    private static byte[] s_converted = new byte[8192];
    private static int s_convertedLength;

    // When worf.exception_behaviour skips or records rows: what the exception log records with each, or null, the
    // behaviour, the copied columns' names, and the row as received.
    private static WorfFailedChange? s_change;
    private static WorfExceptionBehaviour s_behaviour;
    private static List<string> s_columns = [];
    private static byte[] s_message = new byte[8192];

    // A row that failed under sub_disable, which disables the subscription once the copy rolls back.
    private static (WorfFailedChange Change, PgException Error)? s_disabling;

    // The row a copy of one row passes to COPY, its length, and how much of it COPY has read.
    private static int s_oneLength;
    private static int s_oneRead;

    [PgNativeCallback(nameof(ReadRows))]
    private static partial copy_data_source_cb DataSource { get; }

    [PgNativeCallback(nameof(ReadOneRow))]
    private static partial copy_data_source_cb OneRowSource { get; }

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
    /// <param name="restart">Starts the provider's copy again, for a copy a row at a time.</param>
    /// <returns>The number of rows copied.</returns>
    internal static ulong Copy(PgReplicationConnection conn, WorfRelation rel, List<string> attnames,
        WorfColumnConversion?[] conversions, ulong commitLsn, Action restart)
    {
        RelationData* relation = NativeMethods.table_open(rel.Reloid, RowExclusiveLock);
        MemoryContextData* context;
        fixed (byte* name = "worf copy conversion\0"u8)
        {
            // The name is static, as a memory context's has to be.
            context = NativeMethods.AllocSetContextCreateInternal(NativeGlobals.CurrentMemoryContext, (sbyte*)name, 0,
                8 * 1024, 8 * 1024 * 1024);
        }

        try
        {
            ParseState* pstate = NativeMethods.make_parsestate(null);
            _ = NativeMethods.addRangeTableEntryForRelation(pstate, relation, AccessShareLock, null, false, false);
            List* attnamelist = null;
            s_conversions = new WorfColumnConversion?[attnames.Count];
            s_names = (sbyte**)NativeMethods.palloc0((ulong)(sizeof(sbyte*) * attnames.Count));
            s_outputs = (FmgrInfo*)NativeMethods.palloc0((ulong)(sizeof(FmgrInfo) * attnames.Count));
            for (int i = 0; i < attnames.Count; i++)
            {
                sbyte* attname = Worf.PStrDup(attnames[i]);
                attnamelist = NativeMethods.lappend(attnamelist, NativeMethods.makeString(attname));
                int attnum = NativeMethods.attnameAttNum(relation, attname, false);
                if (attnum > 0 && conversions[attnum - 1] is { } conversion)
                {
                    uint typoutput;
                    bool typisvarlena;
                    NativeMethods.getTypeOutputInfo(NativeMethods.TupleDescAttr(relation->rd_att, attnum - 1)->atttypid, &typoutput,
                        &typisvarlena);
                    NativeMethods.fmgr_info(typoutput, &s_outputs[i]);
                    s_conversions[i] = conversion;
                    s_names[i] = attname;
                }
            }

            s_textSize = 256;
            s_text = (byte*)NativeMethods.palloc((ulong)s_textSize);
            s_conn = conn;
            s_ended = false;
            s_line = 0;
            s_rowStart = s_rowEnd = 0;
            s_context = context;
            s_change = null;

            // Unless worf.exception_behaviour stops at a failure, each row converts in its own subtransaction.
            if (Apply.ExceptionBehaviour != WorfExceptionBehaviour.Error)
            {
                WorfSubscription sub = Apply.MySubscription!;
                s_behaviour = Apply.ExceptionBehaviour;
                s_columns = attnames;
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

            bool snapshot = !NativeMethods.ActiveSnapshotSet();
            if (snapshot)
            {
                NativeMethods.PushActiveSnapshot(NativeMethods.GetTransactionSnapshot());
            }

            ulong copied;
            if (s_change is null)
            {
                copied = CopyAll(pstate, relation, attnamelist);
            }
            else
            {
                try
                {
                    copied = PgTransaction.RunInSubtransaction(() => CopyAll(pstate, relation, attnamelist), PgSubtransactionMode.Atomic);
                }
                catch (PgException error) when (!Apply.IsTransient(error) && s_disabling is null)
                {
                    // The rest of the provider's rows are read and passed over, and the copy starts again a row at a
                    // time.
                    PgLog.Log($"copying table {rel.Nspname}.{rel.Relname} again a row at a time, as a row failed: {error.Message}");
                    while (!s_ended)
                    {
                        s_rowStart = s_rowEnd = 0;
                        PgInterrupts.Check();
                        if (s_conn!.Receive(TimeSpan.FromSeconds(1), out _) == PgReplicationReceiveStatus.Ended)
                        {
                            s_ended = true;
                        }
                    }

                    restart();
                    copied = CopyEachRow(pstate, relation, attnamelist);
                }
            }

            if (snapshot)
            {
                NativeMethods.PopActiveSnapshot();
            }

            NativeMethods.free_parsestate(pstate);
            NativeMethods.MemoryContextDelete(context);
            return copied;
        }
        finally
        {
            s_conn = null;
            s_cstate = null;
            s_conversions = [];
            s_names = null;
            s_outputs = null;
            s_context = null;
            s_text = null;
            s_change = null;
            NativeMethods.table_close(relation, NoLock);
        }
    }

    /// <summary>
    /// Copies the provider's rows in one COPY.
    /// </summary>
    private static ulong CopyAll(ParseState* pstate, RelationData* relation, List* attnamelist)
    {
        CopyFromStateData* cstate = NativeMethods.BeginCopyFrom(pstate, relation, null, null, false, DataSource, attnamelist, null);
        s_cstate = cstate;
        ulong copied = NativeMethods.CopyFrom(cstate);
        NativeMethods.EndCopyFrom(cstate);

        // Text COPY can stop at an end-of-data line, and data after it would leave the connection mid-copy.
        byte extra;
        if (ReadRows(&extra, 1, 1) != 0)
        {
            throw new PgException(PgSqlStates.ProtocolViolation, "the copy sent data after its end-of-data marker");
        }

        return copied;
    }

    /// <summary>
    /// Copies the provider's rows a row at a time, each in its own COPY and subtransaction, and records a row that
    /// fails: skipped, or kept to disable the subscription as it fails the copy.
    /// </summary>
    private static ulong CopyEachRow(ParseState* pstate, RelationData* relation, List* attnamelist)
    {
        s_ended = false;
        s_line = 0;
        ulong copied = 0;
        while (true)
        {
            PgInterrupts.Check();
            PgReplicationReceiveStatus status = s_conn!.Receive(TimeSpan.FromSeconds(1), out ReadOnlySpan<byte> message);
            if (status == PgReplicationReceiveStatus.Ended)
            {
                s_ended = true;
                return copied;
            }

            if (status != PgReplicationReceiveStatus.Message)
            {
                continue;
            }

            s_line++;
            if (s_message.Length < message.Length)
            {
                s_message = new byte[Math.Max(message.Length, s_message.Length * 2)];
            }

            message.CopyTo(s_message);
            s_oneLength = message.Length;
            try
            {
                copied += PgTransaction.RunInSubtransaction(() =>
                {
                    s_oneRead = -1;
                    CopyFromStateData* cstate = NativeMethods.BeginCopyFrom(pstate, relation, null, null, false, OneRowSource, attnamelist, null);
                    s_cstate = cstate;
                    ulong one = NativeMethods.CopyFrom(cstate);
                    NativeMethods.EndCopyFrom(cstate);
                    return one;
                }, PgSubtransactionMode.Atomic);
            }
            catch (PgException error) when (!Apply.IsTransient(error))
            {
                WorfFailedChange change = Failed(s_message.AsSpan(0, s_oneLength));
                if (s_behaviour == WorfExceptionBehaviour.SubDisable)
                {
                    s_disabling = (change, error);
                    throw;
                }

                ExceptionLog.Record(change, WorfExceptionBehaviour.Discard, error);
            }
        }
    }

    /// <summary>
    /// COPY's data source for a copy of one row: the row, converted, numbered as the provider's row it is.
    /// </summary>
    private static int ReadOneRow(void* outbuf, int minread, int maxread)
    {
        _ = minread;
        if (s_oneRead < 0)
        {
            WriteRow(s_message.AsSpan(0, s_oneLength));
            s_cstate->cur_lineno = s_line;
            s_oneRead = 0;
        }

        int len = Math.Min(s_convertedLength - s_oneRead, maxread);
        s_converted.AsSpan(s_oneRead, len).CopyTo(new Span<byte>(outbuf, maxread));
        s_oneRead += len;
        return len;
    }

    /// <summary>
    /// Describes a row of the copy that failed, as the exception log records it.
    /// </summary>
    private static WorfFailedChange Failed(ReadOnlySpan<byte> message) => new()
    {
        Subscription = s_change!.Subscription,
        WriterNodeId = s_change.WriterNodeId,
        CommitTime = s_change.CommitTime,
        CommitLsn = s_change.CommitLsn,
        ActionCounter = (uint)s_line,
        Operation = Operation,
        Relation = s_change.Relation,
        NewTuple = RowJson(message),
    };

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
    /// COPY's data source: passes it the provider's rows that have arrived, converted, whole unless one is longer
    /// than COPY reads at once, and skips those whose values fail to convert when worf.exception_behaviour says so,
    /// keeping COPY's line numbers those of the provider's rows.
    /// </summary>
    private static int ReadRows(void* outbuf, int minread, int maxread)
    {
        _ = minread;
        Span<byte> output = new(outbuf, maxread);
        int written = 0;
        while (true)
        {
            // A row that didn't fit after others, or the rest of one longer than COPY reads at once.
            if (s_rowStart < s_rowEnd)
            {
                int len = s_rowEnd - s_rowStart;
                if (len > maxread - written)
                {
                    if (written > 0)
                    {
                        return written;
                    }

                    len = maxread;
                }

                if (written == 0 && s_rowStart == 0)
                {
                    StartRow(s_rowLine);
                }

                s_row.AsSpan(s_rowStart, len).CopyTo(output[written..]);
                s_rowStart += len;
                written += len;
                if (s_rowStart < s_rowEnd)
                {
                    return written;
                }

                continue;
            }

            if (s_ended)
            {
                return written;
            }

            PgReplicationReceiveStatus status = s_conn!.Receive(written == 0 ? TimeSpan.FromSeconds(1) : TimeSpan.Zero,
                out ReadOnlySpan<byte> message);
            if (status == PgReplicationReceiveStatus.Ended)
            {
                s_ended = true;
                return written;
            }

            if (status != PgReplicationReceiveStatus.Message)
            {
                if (written > 0)
                {
                    return written;
                }

                PgInterrupts.Check();
                continue;
            }

            // Each message is a row, which ends with its newline.
            s_line++;
            if (!ConvertRow(message))
            {
                continue;
            }

            ReadOnlySpan<byte> row = s_converted.AsSpan(0, s_convertedLength);
            if (row.Length <= maxread - written)
            {
                if (written == 0)
                {
                    StartRow(s_line);
                }

                row.CopyTo(output[written..]);
                written += row.Length;

                // COPY numbers the rows it reads in one call one after another, so where rows may be skipped, each
                // comes in a call of its own, numbered as the provider's row it is.
                if (s_change is not null)
                {
                    return written;
                }

                continue;
            }

            if (s_row.Length < row.Length)
            {
                s_row = new byte[Math.Max(row.Length, s_row.Length * 2)];
            }

            row.CopyTo(s_row);
            s_rowStart = 0;
            s_rowEnd = row.Length;
            s_rowLine = s_line;
        }
    }

    /// <summary>
    /// Numbers the row COPY reads next as the provider's row it is, once rows before it were skipped.
    /// </summary>
    private static void StartRow(ulong line)
    {
        if (s_change is not null)
        {
            s_cstate->cur_lineno = line;
        }
    }

    /// <summary>
    /// Converts a row, in its own subtransaction when worf.exception_behaviour skips or records rows, and records a
    /// row whose value fails: skipped, or kept to disable the subscription as it fails the copy.
    /// </summary>
    /// <returns>Whether the row goes on to COPY.</returns>
    private static bool ConvertRow(ReadOnlySpan<byte> message)
    {
        if (s_change is null)
        {
            WriteRow(message);
            return true;
        }

        if (s_message.Length < message.Length)
        {
            s_message = new byte[Math.Max(message.Length, s_message.Length * 2)];
        }

        message.CopyTo(s_message);
        int length = message.Length;
        ulong lineno = s_cstate->cur_lineno;
        try
        {
            PgTransaction.RunInSubtransaction(() => WriteRow(s_message.AsSpan(0, length)), PgSubtransactionMode.Atomic);
            return true;
        }
        catch (PgException error) when (!Apply.IsTransient(error))
        {
            ForgetValue(lineno);
            WorfFailedChange change = Failed(s_message.AsSpan(0, length));

            // Under sub_disable, the row converts again to fail the copy, naming its row and column.
            if (s_behaviour == WorfExceptionBehaviour.SubDisable)
            {
                s_disabling = (change, error);
                WriteRow(s_message.AsSpan(0, length));
                s_disabling = null;
                return true;
            }

            ExceptionLog.Record(change, WorfExceptionBehaviour.Discard, error);
            return false;
        }
    }

    /// <summary>
    /// Writes a row as COPY reads it, with its converted values converted and written as their type's output writes
    /// them. While a value converts, COPY's errors name its row, column and text, which a failure leaves them naming.
    /// </summary>
    private static void WriteRow(ReadOnlySpan<byte> message)
    {
        s_convertedLength = 0;
        ulong lineno = s_cstate->cur_lineno;
        MemoryContextData* oldcontext = Worf.MemoryContextSwitchTo(s_context);
        try
        {
            NativeMethods.MemoryContextReset(s_context);
            bool newline = message.Length > 0 && message[^1] == (byte)'\n';
            ReadOnlySpan<byte> row = newline ? message[..^1] : message;
            int field = 0;
            int start = 0;
            for (int i = 0; i <= row.Length; i++)
            {
                if (i < row.Length && row[i] != (byte)'\t')
                {
                    continue;
                }

                ReadOnlySpan<byte> raw = row[start..i];
                if (field < s_conversions.Length && s_conversions[field] is { } conversion && !raw.SequenceEqual("\\N"u8))
                {
                    _ = Unescape(raw);
                    s_cstate->cur_lineno = s_line;
                    s_cstate->cur_attname = s_names[field];
                    s_cstate->cur_attval = (sbyte*)s_text;
                    ulong value = conversion.ConvertText((sbyte*)s_text, out bool isnull);
                    if (isnull)
                    {
                        Append("\\N"u8);
                    }
                    else
                    {
                        sbyte* output = NativeMethods.OutputFunctionCall(&s_outputs[field], value);
                        AppendEscaped(new ReadOnlySpan<byte>(output, Worf.StrLen(output)));
                    }

                    ForgetValue(lineno);
                }
                else
                {
                    Append(raw);
                }

                if (i < row.Length)
                {
                    Append("\t"u8);
                }

                field++;
                start = i + 1;
            }

            if (newline)
            {
                Append("\n"u8);
            }
        }
        finally
        {
            _ = Worf.MemoryContextSwitchTo(oldcontext);
        }
    }

    /// <summary>
    /// Stops COPY's errors naming the value that converted, and gives them the line COPY is reading back.
    /// </summary>
    private static void ForgetValue(ulong lineno)
    {
        s_cstate->cur_lineno = lineno;
        s_cstate->cur_attname = null;
        s_cstate->cur_attval = null;
    }

    /// <summary>
    /// Reads a field as COPY's text format writes it into the value's text, with its terminating zero.
    /// </summary>
    /// <returns>Its length.</returns>
    private static int Unescape(ReadOnlySpan<byte> raw)
    {
        if (s_textSize <= raw.Length)
        {
            s_textSize = Math.Max(raw.Length + 1, s_textSize * 2);
            s_text = (byte*)NativeMethods.repalloc(s_text, (ulong)s_textSize);
        }

        int n = 0;
        for (int i = 0; i < raw.Length; i++)
        {
            byte c = raw[i];
            if (c != (byte)'\\' || i + 1 == raw.Length)
            {
                s_text[n++] = c;
                continue;
            }

            c = raw[++i];
            switch (c)
            {
                case (byte)'b': s_text[n++] = 8; break;
                case (byte)'f': s_text[n++] = 12; break;
                case (byte)'n': s_text[n++] = 10; break;
                case (byte)'r': s_text[n++] = 13; break;
                case (byte)'t': s_text[n++] = 9; break;
                case (byte)'v': s_text[n++] = 11; break;
                case >= (byte)'0' and <= (byte)'7':
                {
                    int value = c - '0';
                    for (int digits = 1; digits < 3 && i + 1 < raw.Length && raw[i + 1] is >= (byte)'0' and <= (byte)'7'; digits++)
                    {
                        value = (value << 3) + (raw[++i] - '0');
                    }

                    s_text[n++] = (byte)value;
                    break;
                }

                case (byte)'x' when i + 1 < raw.Length && char.IsAsciiHexDigit((char)raw[i + 1]):
                {
                    int value = HexValue(raw[++i]);
                    if (i + 1 < raw.Length && char.IsAsciiHexDigit((char)raw[i + 1]))
                    {
                        value = (value << 4) + HexValue(raw[++i]);
                    }

                    s_text[n++] = (byte)value;
                    break;
                }

                default: s_text[n++] = c; break;
            }
        }

        s_text[n] = 0;
        return n;
    }

    private static int HexValue(byte digit) => digit <= (byte)'9' ? digit - '0' : (digit | 0x20) - 'a' + 10;

    /// <summary>
    /// Adds bytes to the converted row.
    /// </summary>
    private static void Append(ReadOnlySpan<byte> bytes)
    {
        if (s_converted.Length < s_convertedLength + bytes.Length)
        {
            Array.Resize(ref s_converted, Math.Max(s_convertedLength + bytes.Length, s_converted.Length * 2));
        }

        bytes.CopyTo(s_converted.AsSpan(s_convertedLength));
        s_convertedLength += bytes.Length;
    }

    /// <summary>
    /// Adds a value to the converted row as COPY's text format writes it, with its backslashes and control characters
    /// escaped.
    /// </summary>
    private static void AppendEscaped(ReadOnlySpan<byte> value)
    {
        int start = 0;
        for (int i = 0; i < value.Length; i++)
        {
            byte escape = value[i] switch
            {
                (byte)'\\' => (byte)'\\',
                8 => (byte)'b',
                9 => (byte)'t',
                10 => (byte)'n',
                11 => (byte)'v',
                12 => (byte)'f',
                13 => (byte)'r',
                _ => 0,
            };
            if (escape != 0)
            {
                Append(value[start..i]);
                Append([(byte)'\\', escape]);
                start = i + 1;
            }
        }

        Append(value[start..]);
    }

    /// <summary>
    /// Writes a row as the exception log records a row: a JSON object of the columns' text.
    /// </summary>
    private static string RowJson(ReadOnlySpan<byte> message)
    {
        ReadOnlySpan<byte> row = message.Length > 0 && message[^1] == (byte)'\n' ? message[..^1] : message;
        using MemoryStream stream = new();
        using (Utf8JsonWriter writer = new(stream))
        {
            writer.WriteStartObject();
            int field = 0;
            int start = 0;
            for (int i = 0; i <= row.Length && field < s_columns.Count; i++)
            {
                if (i < row.Length && row[i] != (byte)'\t')
                {
                    continue;
                }

                ReadOnlySpan<byte> raw = row[start..i];
                if (raw.SequenceEqual("\\N"u8))
                {
                    writer.WriteNull(s_columns[field]);
                }
                else
                {
                    writer.WriteString(s_columns[field], Worf.FromServer(new ReadOnlySpan<byte>(s_text, Unescape(raw))));
                }

                field++;
                start = i + 1;
            }

            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private const int AccessShareLock = 1;
    private const int RowExclusiveLock = 3;
    private const int NoLock = 0;
}
