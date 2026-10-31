using System.Globalization;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The trigger on tables that use a managed sequence, which checks values local sessions write. A value passes when it
/// lies in a block this node holds and the sequence can never return it, or when a row with it already exists, which
/// the trigger then locks so the write can only reach that row.
/// </summary>
internal static class SequenceRangeGuard
{
    private const string CheckViolation = "23514";

    // The blocks this node holds under its current identity, by sequence, as of the version of the shared counter they
    // were read at, and the sequences whose blocks were received under another identity.
    private static readonly Dictionary<uint, (bool Ascending, List<(long Lo, long Hi, string State)> Blocks)> s_sequences = [];
    private static readonly HashSet<uint> s_moved = [];
    private static long s_version = -1;

    // The furthest position each sequence is known to have reached, which only moves forward within a block.
    private static readonly Dictionary<uint, long> s_reached = [];

    // The active blocks, by sequence and start, whose end this backend has woken the range worker for.
    private static readonly HashSet<(uint Seqoid, long Lo)> s_woken = [];

    /// <summary>
    /// Checks the values a local session writes into columns that use managed sequences.
    /// </summary>
    /// <param name="context">The trigger's context, whose arguments are each column, its sequence and its type.</param>
    /// <returns>The row, unchanged.</returns>
    [PgTrigger]
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_guard()
        RETURNS trigger LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static PgHeapTuple? Guard(PgTriggerContext context)
    {
        PgHeapTuple row = context.New!;
        PgHeapTuple? old = context.Old;
        IReadOnlyList<string> arguments = context.Arguments;
        Reload();

        for (int i = 0; i + 2 < arguments.Count; i += 3)
        {
            string column = arguments[i];
            uint seqoid = uint.Parse(arguments[i + 1], CultureInfo.InvariantCulture);
            uint typeOid = uint.Parse(arguments[i + 2], CultureInfo.InvariantCulture);
            if (Read(row, column, typeOid) is not long value || (old is not null && Read(old, column, typeOid) == value))
            {
                continue;
            }

            // After a restore, failover or point-in-time recovery, nothing passes until the range worker has reset the
            // node's blocks.
            if (s_moved.Contains(seqoid))
            {
                throw new PgException(CheckViolation,
                    $"this node's identity changed, so the sequence range blocks of column \"{column}\" in table \"{context.TableName}\" are no longer its own",
                    hint: "The range worker abandons them and requests new blocks; try again once it has.");
            }

            if (!Admits(seqoid, value) && !LockExisting(context, column, value))
            {
                throw new PgException(CheckViolation,
                    $"value {value} of column \"{column}\" in table \"{context.TableName}\" is not in a sequence range this node holds",
                    detail: "Only values this node's sequence has passed, or that rows already hold, can be written here.",
                    hint: "Let the column's default number the row, or reserve the values first with worf.reserve_sequence_values.");
            }
        }

        return row;
    }

    private static long? Read(PgHeapTuple row, string column, uint typeOid) => typeOid switch
    {
        21 => row.Get<short?>(column),
        23 => row.Get<int?>(column),
        _ => row.Get<long?>(column),
    };

    /// <summary>
    /// Checks whether a value lies in a block this node holds and its sequence can never return it.
    /// </summary>
    private static bool Admits(uint seqoid, long value)
    {
        // A sequence no longer under range management doesn't restrict its column.
        if (!s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence))
        {
            return true;
        }

        foreach ((long lo, long hi, string state) in sequence.Blocks)
        {
            if (value < lo || value > hi)
            {
                continue;
            }

            switch (state)
            {
                case "retired" or "imported":
                    return true;

                case "active":
                    WakeNearEnd(seqoid, sequence.Ascending, lo, hi, value);
                    if (s_reached.TryGetValue(seqoid, out long reached) && (sequence.Ascending ? reached >= value : reached <= value))
                    {
                        return true;
                    }

                    long? last = SequenceRanges.LastValue(seqoid);
                    if (last is long position && (sequence.Ascending ? position >= value : position <= value))
                    {
                        s_reached[seqoid] = position;
                        return true;
                    }

                    return false;

                default:
                    return false;
            }
        }

        return false;
    }

    /// <summary>
    /// Wakes the range worker once a sequence reaches the last twentieth of its active block, the least it leaves before
    /// switching, so that a sequence that runs through a block faster than the worker checks switches in time.
    /// </summary>
    private static void WakeNearEnd(uint seqoid, bool ascending, long lo, long hi, long value)
    {
        long tail = (long)(((decimal)hi - lo) / 20);
        if ((ascending ? value >= hi - tail : value <= lo + tail) && s_woken.Add((seqoid, lo)))
        {
            Worker.WakeRangeWorker();
        }
    }

    /// <summary>
    /// Locks the row that already holds a value, so that the write can only reach it. The lock is worf's, since the
    /// writer may not be allowed to lock rows; a unique index would tell the writer that the row exists anyway.
    /// </summary>
    private static bool LockExisting(PgTriggerContext context, string column, long value)
    {
        string table = Spi.QuoteQualifiedIdentifier(context.TableSchema, context.TableName);
        return Elevated.Run(() => Spi.ExecuteScalar<int?>("SELECT 1 FROM " + table + " WHERE " + Spi.QuoteIdentifier(column) +
            " = $1 FOR KEY SHARE", SpiParameter.Create(value)) is not null);
    }

    /// <summary>
    /// Reads the blocks this node holds again once the range worker or a function has changed them.
    /// </summary>
    private static void Reload()
    {
        long version = SequenceRanges.Version.Value;
        if (version == s_version)
        {
            return;
        }

        s_sequences.Clear();
        s_moved.Clear();
        s_reached.Clear();
        s_woken.Clear();
        ulong sysid;
        uint timeline;
        unsafe
        {
            sysid = NativeMethods.GetSystemIdentifier();
            timeline = NativeMethods.GetWALInsertionTimeLine();
        }

        // The blocks as last committed, since a switch that committed after this statement's snapshot may already
        // have given the sequence its new block.
        SpiResult result = Elevated.Run(() => Worf.SelectCatalog("""
            SELECT r.seqoid::oid, s.seqincrement > 0, b.lo, b.hi, b.state,
                b.node_id = l.node_id AND b.sysid = $1 AND b.timeline = $2
            FROM worf.sequence_range r
                JOIN pg_catalog.pg_sequence s ON s.seqrelid = r.seqoid
                CROSS JOIN worf.local_node l
                LEFT JOIN worf.sequence_range_block b ON b.seqoid = r.seqoid AND b.kind IN ('n', 'i')
                    AND b.state IN ('next', 'active', 'retired', 'imported')
            """, SpiParameter.Create((decimal)sysid), SpiParameter.Create((long)timeline)));
        for (int i = 0; i < result.Count; i++)
        {
            SpiRow row = result[i];
            uint seqoid = row.Get<uint>(0);
            if (!s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence))
            {
                sequence = (row.Get<bool>(1), []);
                s_sequences.Add(seqoid, sequence);
            }

            if (row.Get<long?>(2) is not long lo)
            {
                continue;
            }

            string state = row.Get<string>(4);
            if (row.Get<bool>(5))
            {
                sequence.Blocks.Add((lo, row.Get<long>(3), state));
            }
            else if (state is not "retired")
            {
                _ = s_moved.Add(seqoid);
            }
        }

        s_version = version;
    }
}
