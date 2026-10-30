using System.Globalization;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The guard on tables that use a managed sequence, which checks the values local sessions write. Its triggers run after
/// each row is written, so that they see the row as every BEFORE trigger left it, and their conditions call
/// <c>worf.sequence_range_check</c>, which fails the statement for a value outside this node's blocks. A value passes when
/// it lies in a block this node holds and the sequence can never return it.
/// </summary>
internal static class SequenceRangeGuard
{
    private const string CheckViolation = "23514";

    // The blocks this node holds under its current identity, by sequence, as of the versions of the shared and this
    // backend's counters they were read at, and the sequences whose blocks were received under another identity.
    private static readonly Dictionary<uint, (bool Ascending, List<(long Lo, long Hi, string State)> Blocks)> s_sequences = [];
    private static readonly HashSet<uint> s_moved = [];
    private static long s_version = -1;
    private static long s_localVersion = -1;

    // The furthest position each sequence is known to have reached, which only moves forward within a block.
    private static readonly Dictionary<uint, long> s_reached = [];

    /// <summary>
    /// The guard's trigger function. The triggers' conditions do the checking and are never true, so this never runs.
    /// </summary>
    /// <param name="context">The trigger's context.</param>
    /// <returns>Nothing, as an AFTER trigger returns.</returns>
    [PgTrigger]
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_guard()
        RETURNS trigger LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static PgHeapTuple? Guard(PgTriggerContext context) => null;

    /// <summary>
    /// Checks a value a new row holds in a column that uses a managed sequence.
    /// </summary>
    /// <param name="sequence">The sequence.</param>
    /// <param name="value">The value.</param>
    /// <returns>False, so that the trigger whose condition this is never runs; a value outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_check(sequence regclass, value bigint)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool CheckValue([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, long value)
        => Check((uint)sequence.DangerousGetBits(), value);

    /// <summary>
    /// Checks a value a new row holds in a numeric column that uses a managed sequence.
    /// </summary>
    /// <param name="sequence">The sequence.</param>
    /// <param name="value">The value.</param>
    /// <returns>False; a value outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_check(sequence regclass, value numeric)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool CheckNumeric([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, PgNumeric value)
        => Check((uint)sequence.DangerousGetBits(), value);

    /// <summary>
    /// Checks a value an update writes into a column that uses a managed sequence, when it changes the column.
    /// </summary>
    /// <param name="sequence">The sequence.</param>
    /// <param name="old">The column's value before.</param>
    /// <param name="value">Its value after.</param>
    /// <returns>False; a changed value outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_check(sequence regclass, old bigint, value bigint)
        RETURNS boolean CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool CheckUpdate([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, long? old, long? value)
        => value is long changed && old != changed && Check((uint)sequence.DangerousGetBits(), changed);

    /// <summary>
    /// Checks a value an update writes into a numeric column that uses a managed sequence, when it changes the column.
    /// </summary>
    /// <param name="sequence">The sequence.</param>
    /// <param name="old">The column's value before.</param>
    /// <param name="value">Its value after.</param>
    /// <returns>False; a changed value outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_check(sequence regclass, old numeric, value numeric)
        RETURNS boolean CALLED ON NULL INPUT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool CheckNumericUpdate([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, PgNumeric? old, PgNumeric? value)
        => value is PgNumeric changed && !(old is PgNumeric before && before.Equals(changed)) && Check((uint)sequence.DangerousGetBits(), changed);

    /// <summary>
    /// Checks a numeric value: only a whole number within bigint can be one of the sequence's values.
    /// </summary>
    private static bool Check(uint seqoid, PgNumeric value)
    {
        // A value too large for a decimal, or not finite, doesn't parse, and is no bigint either.
        if (!decimal.TryParse(value.Text, NumberStyles.Number, CultureInfo.InvariantCulture, out decimal exact) ||
            decimal.Truncate(exact) != exact || exact is < long.MinValue or > long.MaxValue)
        {
            throw Outside(seqoid, value.Text);
        }

        return Check(seqoid, (long)exact);
    }

    /// <summary>
    /// Checks a value, and fails unless it lies in a block this node holds and the sequence can never return it.
    /// </summary>
    private static bool Check(uint seqoid, long value)
    {
        Reload();

        // After a restore, failover or point-in-time recovery, nothing passes until the range worker has reset the
        // node's blocks.
        if (s_moved.Contains(seqoid))
        {
            throw new PgException(CheckViolation,
                $"this node's identity changed, so its sequence range blocks of sequence {SequenceRanges.GetInfo(seqoid).Name} are no longer its own",
                hint: "The range worker abandons them and requests new blocks; try again once it has.");
        }

        if (!Admits(seqoid, value))
        {
            throw Outside(seqoid, value.ToString(CultureInfo.InvariantCulture));
        }

        return false;
    }

    private static PgException Outside(uint seqoid, string value)
        => new(CheckViolation, $"value {value} of a column that uses sequence {SequenceRanges.GetInfo(seqoid).Name} is not in a sequence range this node holds",
            detail: "Only values this node's sequence has passed can be written here.",
            hint: "Let the column's default number the row, or reserve the values first with worf.reserve_sequence_values.");

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
    /// Reads the blocks this node holds again once the range worker, a function or this backend's own transaction has
    /// changed them.
    /// </summary>
    private static void Reload()
    {
        long version = SequenceRanges.Version.Value;
        long localVersion = SequenceRanges.LocalVersion;
        if (version == s_version && localVersion == s_localVersion)
        {
            return;
        }

        s_sequences.Clear();
        s_moved.Clear();
        s_reached.Clear();
        ulong sysid;
        uint timeline;
        unsafe
        {
            sysid = NativeMethods.GetSystemIdentifier();
            timeline = NativeMethods.GetWALInsertionTimeLine();
        }

        SpiResult result = Elevated.Run(() => Spi.Select("""
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
        s_localVersion = localVersion;
    }
}
