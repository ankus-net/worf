using System.Globalization;
using Ankus.Postgres;

namespace Worf;

/// <summary>
/// The guard on tables that use a managed sequence, which checks the values local sessions write. Its triggers run after
/// each row is written, so that they see the row as every BEFORE trigger left it, and their conditions call
/// <c>worf.sequence_range_check</c>, which fails the statement for a value outside this node's blocks. A value passes when
/// it lies in a block this node holds and the sequence can never return it. An update that moves a row to another
/// partition inserts it there, and that insert leaves the check to the update's own.
/// </summary>
internal static class SequenceRangeGuard
{
    private const string CheckViolation = "23514";

    // SESSION_REPLICATION_ROLE_REPLICA from commands/trigger.h.
    private const int SessionReplicationRoleReplica = 1;

    // BOOTSTRAP_SUPERUSERID from catalog/pg_authid_d.h and SECURITY_LOCAL_USERID_CHANGE from miscadmin.h.
    private const uint BootstrapSuperuserId = 10;
    private const int SecurityLocalUseridChange = 0x0001;

    // The blocks this node holds under its current identity, by sequence, as of the versions of the shared and this
    // backend's counters they were read at, and the sequences whose blocks were received under another identity.
    private static readonly Dictionary<uint, (bool Ascending, List<(long Lo, long Hi, string State)> Blocks)> s_sequences = [];
    private static readonly HashSet<uint> s_moved = [];
    private static long s_version = -1;
    private static long s_localVersion = -1;

    // The furthest position each sequence is known to have reached, which only moves forward within a block.
    private static readonly Dictionary<uint, long> s_reached = [];

    // The spans each sequence may still issue, for a session that restores rows, as of the start of the statement they
    // were read for.
    private static readonly Dictionary<uint, List<(long Lo, long Hi)>> s_issuable = [];
    private static long s_issuableAt;

    // pg_sequence_last_value, which reads a sequence's position without a query.
    private static nint s_lastValue;

    // The running statements whose only inserts are the rows they move to another partition, an UPDATE or a MERGE with
    // no INSERT action, by the start of their transaction, their subtransaction and their snapshot's command ID. A
    // statement leaves as it ends; one that an error ends stays behind in a subtransaction that no later statement runs
    // in, or in an earlier transaction.
    private static readonly List<(nint QueryDesc, long Started, uint Subxact, uint Cid)> s_moving = [];

    /// <summary>
    /// The guard's trigger function. The triggers' conditions do the checking and are never true, so this never runs.
    /// Its arguments list what the guard checks.
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
        => Check((uint)sequence.DangerousGetBits(), value, true);

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
        => Check((uint)sequence.DangerousGetBits(), value, true);

    /// <summary>
    /// Checks a value an update writes into a column that uses a managed sequence, which the guard's condition calls
    /// only when the update changes the column.
    /// </summary>
    /// <param name="sequence">The sequence.</param>
    /// <param name="value">The value.</param>
    /// <returns>False; a value outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_check_update(sequence regclass, value bigint)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool CheckUpdate([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, long value)
        => Check((uint)sequence.DangerousGetBits(), value, false);

    /// <summary>
    /// Checks a value an update writes into a numeric column that uses a managed sequence.
    /// </summary>
    /// <param name="sequence">The sequence.</param>
    /// <param name="value">The value.</param>
    /// <returns>False; a value outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_check_update(sequence regclass, value numeric)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool CheckNumericUpdate([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, PgNumeric value)
        => Check((uint)sequence.DangerousGetBits(), value, false);

    /// <summary>
    /// Checks whether a value a new row holds lies in a block of one of the managed sequences its column uses, as a
    /// column that uses more than one does, without failing.
    /// </summary>
    /// <param name="sequence">The sequence.</param>
    /// <param name="value">The value.</param>
    /// <returns>Whether it does.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_admits(sequence regclass, value bigint)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AdmitsValue([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, long value)
    {
        uint seqoid = (uint)sequence.DangerousGetBits();
        Reload();
        return !s_moved.Contains(seqoid) && Admits(seqoid, value);
    }

    /// <summary>
    /// Checks whether a numeric value a new row holds lies in a block of one of the managed sequences its column uses,
    /// without failing.
    /// </summary>
    /// <param name="sequence">The sequence.</param>
    /// <param name="value">The value.</param>
    /// <returns>Whether it does.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_admits(sequence regclass, value numeric)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AdmitsNumeric([PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, PgNumeric value)
        => Whole(value) is decimal exact && exact is >= long.MinValue and <= long.MaxValue &&
            AdmitsValue(sequence, (long)exact);

    /// <summary>
    /// Notes a statement as it starts, so that the guard knows one whose only inserts are rows it moves to another
    /// partition: an UPDATE, or a MERGE with no INSERT action, without a data-modifying WITH.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    internal static unsafe void StartStatement(QueryDesc* queryDesc)
    {
        PlannedStmt* statement = queryDesc->plannedstmt;
        if (queryDesc->snapshot is null || statement is null || statement->hasModifyingCTE)
        {
            return;
        }

        bool moves = queryDesc->operation == CmdType.CMD_UPDATE;
        if (queryDesc->operation == CmdType.CMD_MERGE && statement->planTree is not null && statement->planTree->type == NodeTag.T_ModifyTable)
        {
            moves = true;
            List* lists = ((ModifyTable*)statement->planTree)->mergeActionLists;
            for (int i = 0; moves && lists is not null && i < lists->length; i++)
            {
                List* actions = (List*)lists->elements[i].ptr_value;
                for (int j = 0; moves && actions is not null && j < actions->length; j++)
                {
                    moves = ((MergeAction*)actions->elements[j].ptr_value)->commandType != CmdType.CMD_INSERT;
                }
            }
        }

        if (moves)
        {
            long started = NativeMethods.GetCurrentTransactionStartTimestamp();
            _ = s_moving.RemoveAll(statement => statement.Started != started);
            s_moving.Add(((nint)queryDesc, started, NativeMethods.GetCurrentSubTransactionId(), queryDesc->snapshot->curcid));
        }
    }

    /// <summary>
    /// Forgets a statement as it ends, so that a statement that runs later with the same command ID, as one of the
    /// extended protocol's portals does after another's partial execution, isn't taken for it.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    internal static unsafe void EndStatement(QueryDesc* queryDesc)
    {
        if (s_moving.Count > 0)
        {
            _ = s_moving.RemoveAll(statement => statement.QueryDesc == (nint)queryDesc);
        }
    }

    /// <summary>
    /// Checks whether a new row is one a running statement moves from another partition. PostgreSQL then checks the
    /// partitioned table's update guard against the row's old and new values, so its insert leaves the check to that.
    /// A statement that runs inside it, as a trigger's does, has a later command ID.
    /// </summary>
    private static unsafe bool Moved()
    {
        if (s_moving.Count == 0 || !NativeMethods.ActiveSnapshotSet())
        {
            return false;
        }

        (long started, uint subxact, uint cid) = (NativeMethods.GetCurrentTransactionStartTimestamp(), NativeMethods.GetCurrentSubTransactionId(),
            NativeMethods.GetActiveSnapshot()->curcid);
        foreach ((_, long statementStarted, uint statementSubxact, uint statementCid) in s_moving)
        {
            if ((statementStarted, statementSubxact, statementCid) == (started, subxact, cid))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Reads a numeric value as a whole number, or null when it has a fraction or is too large for a decimal.
    /// </summary>
    private static decimal? Whole(PgNumeric value)
        => decimal.TryParse(value.Text, NumberStyles.Number, CultureInfo.InvariantCulture, out decimal exact) && decimal.Truncate(exact) == exact
            ? exact
            : null;

    /// <summary>
    /// Checks a numeric value: only a whole number within bigint can be one of the sequence's values.
    /// </summary>
    private static bool Check(uint seqoid, PgNumeric value, bool inserted)
    {
        // A value too large for a decimal, or not finite, doesn't parse, and is no bigint either.
        if (Whole(value) is not decimal exact || exact is < long.MinValue or > long.MaxValue)
        {
            if ((inserted && Moved()) || Restoring())
            {
                return false;
            }

            throw Outside(seqoid, value.Text);
        }

        return Check(seqoid, (long)exact, inserted);
    }

    /// <summary>
    /// Checks a value, and fails unless it lies in a block this node holds and the sequence can never return it.
    /// </summary>
    private static bool Check(uint seqoid, long value, bool inserted)
    {
        Reload();
        if (inserted && Moved())
        {
            return false;
        }

        // After a restore, failover or point-in-time recovery, nothing passes until the range worker has reset the
        // node's blocks.
        if (s_moved.Contains(seqoid))
        {
            throw new PgException(CheckViolation,
                $"this node's identity changed, so its sequence range blocks of sequence {SequenceRanges.GetInfo(seqoid).Name} are no longer its own",
                hint: "The range worker abandons them and requests new blocks; try again once it has.");
        }

        if (!Admits(seqoid, value) && (!Restoring() || MayIssue(seqoid, value)))
        {
            throw Outside(seqoid, value.ToString(CultureInfo.InvariantCulture));
        }

        return false;
    }

    /// <summary>
    /// Checks whether this session writes rows as a restore does: it sets worf.sequence_range_restore, or it runs with
    /// session_replication_role replica, as apply and sync workers do, whose rows and DDL come from other nodes.
    /// </summary>
    private static bool Restoring()
    {
        unsafe
        {
            return SequenceRanges.Restore || NativeGlobals.SessionReplicationRole == SessionReplicationRoleReplica;
        }
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

                    long? last = Position(seqoid);
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
    /// Checks the values a table's rows hold in a column that DDL brings under the guard, as when it sets the column's
    /// default or attaches the table as a partition. A value passes when the guard would pass it or it lies before
    /// where range management of the column's sequences started; in a session that restores rows, as an apply worker
    /// does for DDL from another node, a value also passes unless one of the sequences may still issue it, as far as
    /// this node knows. A numeric value that isn't a whole number within bigint, as NaN, is none of a sequence's. The
    /// check looks for the values that fail by their spans, so that an index on the column, or the table's constraints,
    /// spare it from reading every row.
    /// </summary>
    /// <param name="table">The table's qualified name.</param>
    /// <param name="inherited">Whether to check its inheritors' and partitions' rows too.</param>
    /// <param name="column">The column.</param>
    /// <param name="numeric">Whether the column is numeric.</param>
    /// <param name="sequences">The managed sequences the column uses.</param>
    internal static void CheckRows(string table, bool inherited, string column, bool numeric, List<uint> sequences)
    {
        Reload();
        List<(long Lo, long Hi)> refused = [];
        List<(long Lo, long Hi)> passed = [];
        foreach (uint seqoid in sequences)
        {
            // A sequence no longer under range management doesn't restrict its column.
            if (!s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence))
            {
                return;
            }

            (List<(long Lo, long Hi)> admitted, (long Lo, long Hi) managed, List<(long Lo, long Hi)> issuable) = Spans(seqoid, sequence);
            passed.AddRange(admitted);
            if (Restoring())
            {
                refused.AddRange(issuable);
            }
            else
            {
                refused.Add(managed);
            }
        }

        List<(long Lo, long Hi)> spans = Subtract(refused, passed);
        if (spans.Count == 0)
        {
            return;
        }

        string value = Spi.QuoteIdentifier(column);
        string condition = string.Join(" OR ", spans.Select(span => Within(value, span, numeric)));
        if (numeric)
        {
            condition = $"({condition}) AND {value} = trunc({value})";
        }

        // The table's constraints, as a partition's bound, may rule the spans out, and spare reading its rows, as they spare
        // PostgreSQL's own check of an attached partition's rows.
        string? outside = Elevated.Run(() =>
        {
            unsafe
            {
                fixed (byte* name = Worf.ToServerCString("constraint_exclusion"))
                fixed (byte* on = Worf.ToServerCString("on"))
                {
                    _ = NativeMethods.set_config_option((sbyte*)name, (sbyte*)on, GucContext.PGC_USERSET, GucSource.PGC_S_SESSION,
                        GucAction.GUC_ACTION_SAVE, true, 0, false);
                }
            }

            return Worf.ReadScalar<string?>($"SELECT {value}::text FROM {(inherited ? "" : "ONLY ")}{table} WHERE {condition} LIMIT 1");
        });
        if (outside is not null)
        {
            throw new PgException(CheckViolation,
                $"value {outside} in column \"{column}\" of table {table}, which uses sequence {SequenceRanges.GetInfo(sequences[0]).Name}, isn't one this node may write",
                detail: "The rows of a column that comes to use a managed sequence can hold values the guard passes, or values from before range management started.",
                hint: "Reserve the values first with worf.reserve_sequence_values, or change them. For other nodes' rows, set worf.sequence_range_restore.");
        }
    }

    /// <summary>
    /// Finds the spans of a sequence's values, as far as this node knows: the values the guard passes, which this
    /// node's blocks hold and its sequence has passed; the values from where range management started on; and those
    /// the sequence may still issue, on this node or another: ahead of this node's sequence in its blocks, in a span its
    /// root or pool blocks have yet to grant, or past every value granted so far.
    /// </summary>
    private static (List<(long Lo, long Hi)> Passed, (long Lo, long Hi) Managed, List<(long Lo, long Hi)> Issuable) Spans(uint seqoid,
        (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence)
    {
        bool ascending = sequence.Ascending;
        List<(long Lo, long Hi)> passed = [];
        List<(long Lo, long Hi)> issuable = [];
        foreach ((long lo, long hi, string state) in sequence.Blocks)
        {
            long? position = state == "active" ? Position(seqoid) : null;
            switch (state)
            {
                case "retired" or "imported":
                    passed.Add((lo, hi));
                    break;

                // The active block's values the sequence has passed, and those it has yet to issue.
                case "active" when position is long p && (ascending ? p >= lo : p <= hi):
                    passed.Add(ascending ? (lo, Math.Min(p, hi)) : (Math.Max(p, lo), hi));
                    if (ascending ? p < hi : p > lo)
                    {
                        issuable.Add(ascending ? (p + 1, hi) : (lo, p - 1));
                    }

                    break;

                default:
                    issuable.Add((lo, hi));
                    break;
            }
        }

        SpiResult range = Elevated.Run(() => Worf.Read("""
            SELECT r.root_start, r.watermark, f.lo, f.hi
            FROM worf.sequence_range r
                LEFT JOIN worf.sequence_range_block b ON b.seqoid = r.seqoid AND b.kind IN ('r', 'p') AND b.state = 'open'
                LEFT JOIN worf.sequence_range_free f ON f.block_id = b.block_id
            WHERE r.seqoid = $1
            """, 0, SpiParameter.Create(seqoid)));
        for (int i = 0; i < range.Count; i++)
        {
            if (range[i].Get<long?>(2) is long lo)
            {
                issuable.Add((lo, range[i].Get<long>(3)));
            }
        }

        // Before the root has a start, any value may be the sequence's.
        (long Lo, long Hi) managed = range.Count > 0 && range[0].Get<long?>(0) is long start
            ? (ascending ? (start, long.MaxValue) : (long.MinValue, start))
            : (long.MinValue, long.MaxValue);
        if (range.Count == 0 || range[0].Get<long?>(1) is not long watermark)
        {
            issuable.Add(managed);
        }
        else if (ascending ? watermark < long.MaxValue : watermark > long.MinValue)
        {
            issuable.Add(ascending ? (watermark + 1, long.MaxValue) : (long.MinValue, watermark - 1));
        }

        return (passed, managed, [.. issuable
            .Select(span => (Math.Max(span.Lo, managed.Lo), Math.Min(span.Hi, managed.Hi)))
            .Where(span => span.Item1 <= span.Item2)]);
    }

    /// <summary>
    /// Removes spans of values from others.
    /// </summary>
    private static List<(long Lo, long Hi)> Subtract(List<(long Lo, long Hi)> spans, List<(long Lo, long Hi)> removed)
    {
        List<(long Lo, long Hi)> left = [.. spans.Distinct()];
        foreach ((long lo, long hi) in removed)
        {
            List<(long Lo, long Hi)> next = [];
            foreach ((long Lo, long Hi) span in left)
            {
                if (hi < span.Lo || lo > span.Hi)
                {
                    next.Add(span);
                    continue;
                }

                if (lo > span.Lo)
                {
                    next.Add((span.Lo, lo - 1));
                }

                if (hi < span.Hi)
                {
                    next.Add((hi + 1, span.Hi));
                }
            }

            left = next;
        }

        return left;
    }

    /// <summary>
    /// Writes the condition that a value lies in a span. An integer column's span is open where it reaches bigint's
    /// limit, which its values can't pass, while a numeric column's is closed, since NaN and values beyond bigint's
    /// limits lie past it.
    /// </summary>
    private static string Within(string value, (long Lo, long Hi) span, bool numeric) => span switch
    {
        _ when numeric => string.Create(CultureInfo.InvariantCulture, $"{value} BETWEEN {span.Lo}::numeric AND {span.Hi}::numeric"),
        (long.MinValue, long.MaxValue) => $"{value} IS NOT NULL",
        (long.MinValue, long hi) => string.Create(CultureInfo.InvariantCulture, $"{value} <= {hi}"),
        (long lo, long.MaxValue) => string.Create(CultureInfo.InvariantCulture, $"{value} >= {lo}"),
        (long lo, long hi) => string.Create(CultureInfo.InvariantCulture, $"{value} BETWEEN {lo} AND {hi}"),
    };

    /// <summary>
    /// Checks whether a sequence may still issue a value, for a session that restores rows, as of the statement the
    /// sequence's spans were read for.
    /// </summary>
    private static bool MayIssue(uint seqoid, long value)
    {
        long statement;
        unsafe
        {
            statement = NativeMethods.GetCurrentStatementStartTimestamp();
        }

        if (statement != s_issuableAt)
        {
            s_issuable.Clear();
            s_issuableAt = statement;
        }

        if (!s_issuable.TryGetValue(seqoid, out List<(long Lo, long Hi)>? spans))
        {
            spans = s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence)
                ? Spans(seqoid, sequence).Issuable
                : [];
            s_issuable.Add(seqoid, spans);
        }

        return spans.Exists(span => value >= span.Lo && value <= span.Hi);
    }

    /// <summary>
    /// Reads a sequence's last value, or null when it hasn't issued one, as the bootstrap superuser, since PostgreSQL 15
    /// and 16 fail for a role without SELECT or USAGE on the sequence. A query per value would make a bulk insert
    /// crawl.
    /// </summary>
    private static unsafe long? Position(uint seqoid)
    {
        if (s_lastValue == 0)
        {
            FmgrInfo function;
            fixed (byte* name = "pg_sequence_last_value"u8)
            {
                NativeMethods.fmgr_info(NativeMethods.fmgr_internal_function((sbyte*)name), &function);
            }

            s_lastValue = (nint)function.fn_addr.DangerousGetAddress();
        }

        uint userId;
        int secContext;
        NativeMethods.GetUserIdAndSecContext(&userId, &secContext);
        NativeMethods.SetUserIdAndSecContext(BootstrapSuperuserId, secContext | SecurityLocalUseridChange);
        try
        {
            return PgFunctions.DangerousCall<long?>(s_lastValue, 0, [PgDatum.DangerousCreate(seqoid, (uint)PgBuiltInOid.OidOid, PgMemoryContext.Current)]);
        }
        finally
        {
            NativeMethods.SetUserIdAndSecContext(userId, secContext);
        }
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
        s_issuable.Clear();
        ulong sysid;
        uint timeline;
        unsafe
        {
            sysid = NativeMethods.GetSystemIdentifier();
            timeline = SequenceRanges.Timeline();
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
