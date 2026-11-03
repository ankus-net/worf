using System.Text;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// Copies a table whose values convert from the provider's types through PostgreSQL's own <c>COPY FROM</c>, as the
/// other tables are copied. Each row's converted values are converted as replication converts a value sent as text,
/// and written as the column type's output writes them, which COPY's input reads back, and a value that fails names
/// its row and column as COPY's errors do. Under <c>worf.exception_behaviour</c> other than <c>error</c>, which copies
/// every table this way, a row that fails, whether a value fails to convert, its column's input refuses it or a
/// constraint does, is skipped and recorded in <c>worf.exception_log</c>, or disables the subscription once the copy
/// rolls back. Under <c>discard</c> and <c>transdiscard</c>, the table copies in batches, each in its own <c>COPY</c>
/// and subtransaction, which go on past each row that fails, its values converted first in subtransactions that write
/// nothing, so that a row whose value fails to convert is left out without a <c>COPY</c> failing. Under
/// <c>sub_disable</c>, the table copies in one <c>COPY</c>, in a subtransaction, and a row that fails, which COPY names,
/// is read again to record.
/// </summary>
internal static unsafe partial class CopyConversion
{
    // The operation the exception log records a row of the copy as.
    private const string Operation = "COPY";

    // The copy in progress: the connection its rows come from and whether they ended, the number of the last row
    // read, a row still to pass to COPY and how much of it is left, and COPY's state.
    private static PgReplicationConnection? s_conn;
    private static bool s_ended;
    private static ulong s_line;
    private static byte[] s_row = new byte[8192];
    private static int s_rowStart;
    private static int s_rowEnd;
    private static CopyFromStateData* s_cstate;

    // The rows of a copy in batches that failed: those of the batch under way, which are recorded in order once it's
    // copied, and those recorded since, in the copy's transaction, a run at a time, so that a copy that skips many
    // rows neither keeps them all nor takes a transaction ID for each, and how much of the rows' text they hold.
    private const int RecordRows = 1000;
    private const long RecordBytes = 8 * 1024 * 1024;
    private static readonly List<(ulong Line, WorfFailedChange Change, PgException Error)> s_batchFailed = [];
    private static readonly List<(WorfFailedChange Change, PgException Error)> s_toRecord = [];
    private static long s_toRecordBytes;

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

    // How many of the provider's rows a copy in batches copies in one subtransaction, and how much of their text at
    // most, past one row: as one fails, the rows before it are copied again and the copy goes on after it, or, when
    // COPY names no row, the batch is split in halves.
    private const int BatchRows = 1000;
    private const int BatchBytes = 8 * 1024 * 1024;

    // When worf.exception_behaviour skips or records rows: what the exception log records with each, or null, the
    // behaviour, and the copied columns' names.
    private static WorfFailedChange? s_change;
    private static WorfExceptionBehaviour s_behaviour;
    private static List<string> s_columns = [];

    // A row or merge that failed under sub_disable, which disables the subscription once the copy rolls back.
    private static (WorfFailedChange Change, PgException Error)? s_disabling;

    // A batch of the provider's rows: their bytes, where each starts and how long it is, and the first's number, which
    // the others follow. Then the rows that converted, which COPY reads: their text, where each starts and how long it
    // is, and which of the batch's rows it is, and the next and the end of those a copy of the batch passes to COPY,
    // and how much of the one under way COPY has read.
    private static byte[] s_batchData = new byte[64 * 1024];
    private static readonly List<(int Start, int Length)> s_batchRows = new(BatchRows);
    private static ulong s_batchLine;
    private static byte[] s_batchText = new byte[64 * 1024];
    private static readonly List<(int Start, int Length, int Row)> s_batchConverted = new(BatchRows);
    private static int s_batchNext;
    private static int s_batchEnd;
    private static int s_batchRead;

    // The line COPY was at as a copy ended, which a failure leaves naming the row that failed.
    private static ulong s_failedLine;

    [PgNativeCallback(nameof(ReadRows))]
    private static partial copy_data_source_cb DataSource { get; }

    [PgNativeCallback(nameof(ReadBatch))]
    private static partial copy_data_source_cb BatchSource { get; }

    /// <summary>
    /// Copies the rows of a <c>COPY ... TO STDOUT</c> started on the connection into a local table, or a table of its
    /// columns, converting the columns that need it. Runs in the copy's target transaction.
    /// </summary>
    /// <param name="conn">The provider connection, whose copy has started.</param>
    /// <param name="rel">The remote relation, mapped to the local table.</param>
    /// <param name="into">The table the rows go into: the local table, or one with its copied columns.</param>
    /// <param name="attnames">The copied columns, in order.</param>
    /// <param name="conversions">Each local column's conversion, by its number less one, or null where the column's
    /// own input reads the provider's text, or null when no column converts.</param>
    /// <param name="commitLsn">The position the copy is consistent with, which the exception log records.</param>
    /// <param name="restart">Starts the provider's copy again, for a copy in batches.</param>
    /// <returns>The number of rows copied.</returns>
    internal static ulong Copy(PgReplicationConnection conn, WorfRelation rel, uint into, List<string> attnames,
        WorfColumnConversion?[]? conversions, ulong commitLsn, Action restart)
    {
        RelationData* relation = NativeMethods.table_open(into, RowExclusiveLock);
        RelationData* table = into == rel.Reloid ? relation : NativeMethods.table_open(rel.Reloid, NoLock);
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
                int attnum = NativeMethods.attnameAttNum(table, attname, false);
                if (attnum > 0 && conversions?[attnum - 1] is { } conversion)
                {
                    uint typoutput;
                    bool typisvarlena;
                    NativeMethods.getTypeOutputInfo(NativeMethods.TupleDescAttr(table->rd_att, attnum - 1)->atttypid, &typoutput,
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

            // Unless worf.exception_behaviour stops at a failure, the rows that fail are recorded.
            s_batchFailed.Clear();
            s_toRecord.Clear();
            s_toRecordBytes = 0;
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

            // A copy that skips or records rows copies them all at once first, as fast as one that stops at a failure,
            // in a subtransaction, and only once a row fails, the copy starts again: under sub_disable to record the row
            // that failed, and otherwise, or when COPY named none, in batches, which find the rows that fail.
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
                catch (PgException error) when (!Apply.IsTransient(error))
                {
                    // The rest of the provider's rows are read and passed over before the copy starts again.
                    ulong failedLine = s_failedLine;
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
                    if (s_behaviour == WorfExceptionBehaviour.SubDisable && failedLine > 0)
                    {
                        if (FindRow(failedLine) is WorfFailedChange change)
                        {
                            s_disabling = (change, error);
                            throw;
                        }

                        restart();
                    }

                    PgLog.Log($"copying table {rel.Nspname}.{rel.Relname} again in batches, as a row failed: {error.Message}");
                    copied = CopyInBatches(pstate, relation, attnamelist);
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
            if (table != relation)
            {
                NativeMethods.table_close(table, NoLock);
            }

            NativeMethods.table_close(relation, NoLock);
        }
    }

    /// <summary>
    /// Copies the provider's rows in one COPY.
    /// </summary>
    private static ulong CopyAll(ParseState* pstate, RelationData* relation, List* attnamelist)
    {
        s_failedLine = 0;
        CopyFromStateData* cstate = NativeMethods.BeginCopyFrom(pstate, relation, null, null, false, DataSource, attnamelist, null);
        s_cstate = cstate;
        ulong copied;
        try
        {
            copied = NativeMethods.CopyFrom(cstate);
        }
        finally
        {
            // A failure leaves it naming the row that failed.
            s_failedLine = cstate->cur_lineno;
        }

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
    /// Reads the provider's rows up to one, which failed, and describes it as the exception log records it.
    /// </summary>
    /// <returns>The row, or null when the provider sent fewer rows.</returns>
    private static WorfFailedChange? FindRow(ulong line)
    {
        s_ended = false;
        ulong read = 0;
        while (true)
        {
            PgInterrupts.Check();
            PgReplicationReceiveStatus status = s_conn!.Receive(TimeSpan.FromSeconds(1), out ReadOnlySpan<byte> message);
            if (status == PgReplicationReceiveStatus.Ended)
            {
                s_ended = true;
                return null;
            }

            if (status == PgReplicationReceiveStatus.Message && ++read == line)
            {
                s_line = line;
                return Failed(message);
            }
        }
    }

    /// <summary>
    /// Copies the provider's rows in batches, each in its own COPY and subtransaction. A row that fails is recorded:
    /// skipped, or kept to disable the subscription as it fails the copy. Each batch takes a transaction ID, and each
    /// row that COPY fails one or two more, for the rows before it copied again and those after it; a row whose value
    /// fails to convert takes none.
    /// </summary>
    private static ulong CopyInBatches(ParseState* pstate, RelationData* relation, List* attnamelist)
    {
        s_ended = false;
        s_batchLine = 1;
        ulong copied = 0;
        while (!s_ended)
        {
            s_batchRows.Clear();
            int used = 0;
            while (s_batchRows.Count < BatchRows && used < BatchBytes)
            {
                PgInterrupts.Check();
                PgReplicationReceiveStatus status = s_conn!.Receive(TimeSpan.FromSeconds(1), out ReadOnlySpan<byte> message);
                if (status == PgReplicationReceiveStatus.Ended)
                {
                    s_ended = true;
                    break;
                }

                if (status == PgReplicationReceiveStatus.Message)
                {
                    Reserve(ref s_batchData, used, message.Length);

                    message.CopyTo(s_batchData.AsSpan(used));
                    s_batchRows.Add((used, message.Length));
                    used += message.Length;
                }
            }

            ConvertBatch();
            copied += CopyBatch(pstate, relation, attnamelist, 0, s_batchConverted.Count);

            // The batch's rows that failed are recorded in order, whether they failed to convert or COPY failed them, a
            // run at a time, as soon as a run is that large.
            s_batchFailed.Sort((x, y) => x.Line.CompareTo(y.Line));
            foreach ((_, WorfFailedChange change, PgException error) in s_batchFailed)
            {
                s_toRecord.Add((change, error));
                s_toRecordBytes += change.NewTuple?.Length ?? 0;
                if (s_toRecord.Count >= RecordRows || s_toRecordBytes >= RecordBytes)
                {
                    FlushFailures();
                }
            }

            s_batchFailed.Clear();
            if (s_ended)
            {
                FlushFailures();
            }

            s_batchLine += (ulong)s_batchRows.Count;
        }

        return copied;
    }

    /// <summary>
    /// Records the failed rows kept so far.
    /// </summary>
    private static void FlushFailures()
    {
        ExceptionLog.RecordCopied(s_toRecord);
        s_toRecord.Clear();
        s_toRecordBytes = 0;
    }

    /// <summary>
    /// Makes room in a buffer for more bytes after those used, doubling it, as far as an array goes.
    /// </summary>
    private static void Reserve(ref byte[] buffer, int used, int more)
    {
        long needed = (long)used + more;
        if (needed > buffer.Length)
        {
            Array.Resize(ref buffer, (int)Math.Min(Math.Max(needed, (long)buffer.Length * 2), Array.MaxLength));
        }
    }

    /// <summary>
    /// Converts a batch's rows as COPY reads them, as many at a time as convert in one subtransaction, which writes
    /// nothing and so takes no transaction ID. A row whose value fails to convert, from its data, is left out of the
    /// copy and kept to record, or, under <c>sub_disable</c>, fails it.
    /// </summary>
    private static void ConvertBatch()
    {
        s_batchConverted.Clear();
        int used = 0;
        int next = 0;
        string table = $"{s_change!.Relation!.Nspname}.{s_change.Relation.Relname}";
        bool converts = Array.Exists(s_conversions, conversion => conversion is not null);
        while (next < s_batchRows.Count)
        {
            try
            {
                PgTransaction.RunInSubtransaction(() =>
                {
                    for (; next < s_batchRows.Count; next++)
                    {
                        s_line = s_batchLine + (ulong)next;
                        if (converts)
                        {
                            using PgErrorContext context = PgErrorContext.Push($"converting row {s_line} of the copy of table {table}");
                            WriteRow(BatchRow(next));
                        }
                        else
                        {
                            WriteRow(BatchRow(next));
                        }

                        Reserve(ref s_batchText, used, s_convertedLength);

                        s_converted.AsSpan(0, s_convertedLength).CopyTo(s_batchText.AsSpan(used));
                        s_batchConverted.Add((used, s_convertedLength, next));
                        used += s_convertedLength;
                    }
                }, PgSubtransactionMode.Atomic);
            }
            catch (PgException error) when (!Apply.IsTransient(error))
            {
                WorfFailedChange change = Failed(BatchRow(next));
                if (s_behaviour == WorfExceptionBehaviour.SubDisable)
                {
                    s_disabling = (change, error);
                    throw;
                }

                s_batchFailed.Add((s_line, change, error));
                next++;
            }
        }
    }

    /// <summary>
    /// Copies some of a batch's converted rows in one COPY and subtransaction. When a row fails, which COPY's errors
    /// name, the rows before it are copied again, it's kept to record, and the copy goes on from the row after it; a
    /// failure that names none of them has them copied again in halves, down to a row of its own.
    /// </summary>
    private static ulong CopyBatch(ParseState* pstate, RelationData* relation, List* attnamelist, int start, int count)
    {
        ulong copied = 0;
        int end = start + count;
        while (start < end)
        {
            try
            {
                copied += CopyRange(pstate, relation, attnamelist, start, end);
                break;
            }
            catch (PgException error) when (!Apply.IsTransient(error))
            {
                // COPY numbers each row as the provider's, as ReadBatch tells it.
                int failed = -1;
                if (s_failedLine >= s_batchLine)
                {
                    int found = s_batchConverted.BinarySearch(start, end - start, (0, 0, (int)Math.Min(s_failedLine - s_batchLine, int.MaxValue)),
                        Comparer<(int Start, int Length, int Row)>.Create((x, y) => x.Row.CompareTo(y.Row)));
                    failed = found >= 0 ? found : -1;
                }

                if (failed < 0 && end - start > 1)
                {
                    int half = (end - start) / 2;
                    return copied + CopyBatch(pstate, relation, attnamelist, start, half) +
                        CopyBatch(pstate, relation, attnamelist, start + half, end - start - half);
                }

                failed = Math.Max(failed, start);
                int row = s_batchConverted[failed].Row;
                ulong line = s_batchLine + (ulong)row;
                s_line = line;
                WorfFailedChange change = Failed(BatchRow(row));
                if (s_behaviour == WorfExceptionBehaviour.SubDisable)
                {
                    s_disabling = (change, error);
                    throw;
                }

                if (failed > start)
                {
                    copied += CopyBatch(pstate, relation, attnamelist, start, failed - start);
                }

                s_batchFailed.Add((line, change, error));
                start = failed + 1;
            }
        }

        return copied;
    }

    /// <summary>
    /// Copies a batch's converted rows from one to before another in one COPY and subtransaction, and keeps the line
    /// that COPY was at, which a failure leaves naming the row that failed.
    /// </summary>
    private static ulong CopyRange(ParseState* pstate, RelationData* relation, List* attnamelist, int start, int end)
        => PgTransaction.RunInSubtransaction(() =>
        {
            s_batchNext = start;
            s_batchEnd = end;
            s_batchRead = 0;
            s_cstate = null;
            s_failedLine = 0;

            return Worf.InOwnContext(() =>
            {
                try
                {
                    CopyFromStateData* cstate = NativeMethods.BeginCopyFrom(pstate, relation, null, null, false, BatchSource, attnamelist, null);
                    s_cstate = cstate;
                    ulong rows = NativeMethods.CopyFrom(cstate);
                    NativeMethods.EndCopyFrom(cstate);
                    return rows;
                }
                finally
                {
                    // COPY's state goes with the context.
                    if (s_cstate is not null)
                    {
                        s_failedLine = s_cstate->cur_lineno;
                        s_cstate = null;
                    }
                }
            });
        }, PgSubtransactionMode.Atomic);

    /// <summary>
    /// A row of the batch.
    /// </summary>
    private static ReadOnlySpan<byte> BatchRow(int index) => s_batchData.AsSpan(s_batchRows[index].Start, s_batchRows[index].Length);

    /// <summary>
    /// COPY's data source for a copy of a batch: its converted rows, one at a time, so that COPY, which has counted the
    /// row it reads as it asks for its data, numbers it as the provider's row, which its errors then name, rather than
    /// by its place among the rows this copy passes it.
    /// </summary>
    private static int ReadBatch(void* outbuf, int minread, int maxread)
    {
        _ = minread;
        Span<byte> output = new(outbuf, maxread);
        int written = 0;
        if (s_batchNext < s_batchEnd)
        {
            (int rowStart, int length, int row) = s_batchConverted[s_batchNext];
            s_line = s_batchLine + (ulong)row;
            if (s_batchRead == 0 && s_cstate is not null)
            {
                s_cstate->cur_lineno = s_line;
            }

            int len = Math.Min(length - s_batchRead, maxread - written);
            s_batchText.AsSpan(rowStart + s_batchRead, len).CopyTo(output[written..]);
            s_batchRead += len;
            written += len;
            if (s_batchRead == length)
            {
                s_batchNext++;
                s_batchRead = 0;
            }
        }

        return written;
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
        ActionCounter = (long)Math.Min(s_line, long.MaxValue),
        Operation = Operation,
        Relation = s_change.Relation,
        NewTuple = RowJson(message),
    };

    /// <summary>
    /// Handles a merge of a table's copy that failed, as worf.exception_behaviour says: under <c>discard</c> and
    /// <c>transdiscard</c>, it is skipped and recorded, as the copy's transaction commits; under <c>sub_disable</c>, it
    /// disables the subscription and is recorded once the merge rolled back.
    /// </summary>
    /// <param name="rel">The table.</param>
    /// <param name="commitLsn">The position the copy is consistent with.</param>
    /// <param name="error">The failure.</param>
    /// <returns>Whether the merge is skipped, rather than failing the copy.</returns>
    internal static bool SkipMerge(WorfRelation rel, ulong commitLsn, PgException error)
    {
        WorfExceptionBehaviour behaviour = Apply.ExceptionBehaviour;
        if (behaviour == WorfExceptionBehaviour.Error)
        {
            return false;
        }

        WorfSubscription sub = Apply.MySubscription!;
        WorfFailedChange change = new()
        {
            Subscription = sub,
            WriterNodeId = sub.Origin.Id,
            CommitTime = NativeMethods.GetCurrentTransactionStartTimestamp(),
            CommitLsn = commitLsn,
            Operation = "MERGE",
            Relation = rel,
        };
        if (behaviour == WorfExceptionBehaviour.SubDisable)
        {
            s_disabling = (change, error);
            return false;
        }

        ExceptionLog.Record(change, behaviour, error);
        return true;
    }

    /// <summary>
    /// Disables the subscription after a row of its copy, or its merge, failed under <c>sub_disable</c>, once the copy
    /// rolled back, and records the row, as apply does with a failed remote transaction.
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
        PgLog.Log(change.Operation == "MERGE"
            ? $"disabling subscription {change.Subscription.Name} after the merge of table {change.Relation!.Nspname}.{change.Relation.Relname}'s copy failed"
            : $"disabling subscription {change.Subscription.Name} after a row of table {change.Relation!.Nspname}.{change.Relation.Relname} failed in its copy");
    }

    /// <summary>
    /// COPY's data source: passes it the provider's rows that have arrived, converted, whole unless one is longer
    /// than COPY reads at once. A row that fails fails the copy, which, when worf.exception_behaviour skips or records
    /// rows, runs in a subtransaction that the copy in batches takes over from.
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
            WriteRow(message);
            ReadOnlySpan<byte> row = s_converted.AsSpan(0, s_convertedLength);
            if (row.Length <= maxread - written)
            {
                row.CopyTo(output[written..]);
                written += row.Length;
                continue;
            }

            if (s_row.Length < row.Length)
            {
                s_row = new byte[Math.Max(row.Length, s_row.Length * 2)];
            }

            row.CopyTo(s_row);
            s_rowStart = 0;
            s_rowEnd = row.Length;
        }
    }

    /// <summary>
    /// Writes a row as COPY reads it, with its converted values converted and written as their type's output writes
    /// them. While a value converts, COPY's errors name its row, column and text, which a failure leaves them naming.
    /// </summary>
    private static void WriteRow(ReadOnlySpan<byte> message)
    {
        s_convertedLength = 0;
        ulong lineno = s_cstate is null ? 0 : s_cstate->cur_lineno;
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
                    if (s_cstate is not null)
                    {
                        s_cstate->cur_lineno = s_line;
                        s_cstate->cur_attname = s_names[field];
                        s_cstate->cur_attval = (sbyte*)s_text;
                    }

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
        if (s_cstate is null)
        {
            return;
        }

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
        // The text goes into the JSON as it is, rather than escaped as \u, which jsonb takes only for ASCII outside a
        // UTF-8 database, and bytes that aren't UTF-8, as a SQL_ASCII database holds, as their hex.
        ReadOnlySpan<byte> row = message.Length > 0 && message[^1] == (byte)'\n' ? message[..^1] : message;
        StringBuilder json = new("{");
        int field = 0;
        int start = 0;
        for (int i = 0; i <= row.Length && field < s_columns.Count; i++)
        {
            if (i < row.Length && row[i] != (byte)'\t')
            {
                continue;
            }

            json.Append(field == 0 ? "" : ", ");
            Executor.EscapeJson(json, s_columns[field]);
            json.Append(": ");
            ReadOnlySpan<byte> raw = row[start..i];
            if (raw.SequenceEqual("\\N"u8))
            {
                json.Append("null");
            }
            else
            {
                // The text is read once it's unescaped, which may move it.
                int length = Unescape(raw);
                Executor.EscapeJson(json, Worf.LoggedText(new ReadOnlySpan<byte>(s_text, length)));
            }

            field++;
            start = i + 1;
        }

        return json.Append('}').ToString();
    }

    private const int AccessShareLock = 1;
    private const int RowExclusiveLock = 3;
    private const int NoLock = 0;
}
