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
internal static partial class SequenceRangeGuard
{
    private const string CheckViolation = "23514";

    // SESSION_REPLICATION_ROLE_REPLICA from commands/trigger.h.
    private const int SessionReplicationRoleReplica = 1;

    // RVR_MISSING_OK from catalog/namespace.h.
    private const uint RangeVarMissingOk = 1;

    // BOOTSTRAP_SUPERUSERID from catalog/pg_authid_d.h and SECURITY_LOCAL_USERID_CHANGE from miscadmin.h.
    private const uint BootstrapSuperuserId = 10;
    private const int SecurityLocalUseridChange = 0x0001;

    // ShareLock and ExclusiveLock from storage/lockdefs.h, and LOCKTAG_ADVISORY and DEFAULT_LOCKMETHOD from storage/lock.h.
    // A sequence's switch gate shows in pg_locks as an advisory lock with the sequence's OID as its object and 3 as its
    // objsubid, which the keys of pg_advisory_lock never have.
    private const int ShareLock = 5;
    private const int ExclusiveLock = 7;
    private const byte LockTagAdvisory = 10;
    private const byte DefaultLockMethod = 1;
    private const ushort GateKey = 3;

    // The switch gates this backend holds, by sequence, with the subtransaction that took each last, and the statements
    // that took them, by their query descriptors, or the COPY FROM they run, with the gates each took. The sequences
    // each table's defaults draw from, as of the blocks last read.
    private static readonly Dictionary<uint, uint> s_gates = [];
    private static readonly List<(nint Statement, uint Subxact, List<uint> Sequences)> s_gating = [];
    private static readonly Dictionary<uint, List<uint>> s_drawnBy = [];

    // Whether each table has the guard's triggers, which only tables numbered by a managed sequence have, as of the
    // versions of the shared and this backend's counters it was looked up at.
    private static readonly Dictionary<uint, bool> s_guarded = [];
    private static (long Version, long Local) s_guardedAt = (-1, -1);
    private static bool s_gatesRegistered;

    // The sequences this backend waits for a switch of, which it counts until it stops waiting, or exits, as at a FATAL
    // error, which skips the code after the wait.
    private static readonly Dictionary<uint, int> s_waitingFor = [];
    private static bool s_exitHooked;
    private static int s_backends;

    private const int MaxDrawers = 1024;

    // The keys the gates of each running COPY FROM are taken under, innermost last, which no query descriptor's address
    // can be.
    private static readonly List<nint> s_copyGates = [];
    private static nint s_lastCopyGate;

    /// <summary>
    /// Gets the shared table of the statements that hold each sequence's switch gate.
    /// </summary>
    internal static PgLwLock<WorfDrawers, WorfDrawer> Drawers { get; } = new("worf_sequence_range_drawers");

    /// <summary>
    /// Sets up the shared memory.
    /// </summary>
    internal static void ShmemInit() => PgSharedMemory.Initialize(Drawers, MaxDrawers, static () => default, static _ => default);

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
    // no INSERT action, by their subtransaction and their snapshot's command ID. A statement leaves as it ends, and one
    // that an error ends leaves with the subtransaction it ran in, or with its transaction, so that no later statement,
    // which a new subtransaction or command ID sets apart from it, is taken for it.
    private static readonly List<(nint QueryDesc, uint Subxact, uint Cid)> s_moving = [];
    private static bool s_movingRegistered;

    // The points in each sequence's active block, by sequence and value, at which this backend has woken the range worker.
    private static readonly HashSet<(uint Seqoid, long Mark)> s_woken = [];

    // The active blocks, by sequence and start, whose switch this backend has waited for, and until when it may.
    private static readonly Dictionary<(uint Seqoid, long Lo), long> s_waited = [];

    // The active blocks, by sequence and start, that this transaction has tried to switch away from itself, as it ran
    // them out, and the managed sequences that the COPY FROM statements running in this backend draw from, innermost
    // last.
    private static readonly HashSet<(uint Seqoid, long Lo)> s_switchedInTransaction = [];

    // The sequences this transaction switched itself, which it holds against every other switch until it ends.
    private static readonly HashSet<uint> s_switchedItself = [];
    private static readonly List<List<uint>> s_copying = [];

    // The active blocks, by sequence and start, reserved for a load, which the range worker uses up before it switches.
    private static readonly HashSet<(uint Seqoid, long Lo)> s_reservedActive = [];

    /// <summary>
    /// Gets the setting that a COPY FROM into a table whose defaults draw from a managed sequence reads for each row, in
    /// the condition <see cref="CopyCondition"/> adds, after the row's defaults drew their values and before COPY
    /// buffers the row, which the guard's triggers see only once a buffer of rows is written. Reading it checks the
    /// sequences as the guard checks a row's values: it waits for a switch near a block's end, and switches at the
    /// block's last value, so that the COPY's later rows take the next block's values.
    /// </summary>
    [PgGucString("worf.sequence_range_copy_check", "", "Checks the managed sequences a COPY FROM draws from, for each row.",
        Context = PgGucContext.Internal, Flags = PgGucOptions.NoShowAll | PgGucOptions.DisallowInFile)]
    internal static partial string CopyCheck { get; }

    // The setting's show hook, which reads run as, installed once in each process, and the empty value it shows.
    private static bool s_copyCheckInstalled;
    private static nint s_copyCheckValue;

    [PgNativeCallback(nameof(CheckCopied))]
    private static partial GucShowHook CopyCheckShow { get; }

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
    /// Checks a value a new row holds in a column whose default names its sequence as text, under the sequence the name
    /// finds as the row is written, as <see cref="Named"/> finds it.
    /// </summary>
    /// <param name="name">The name the default gives.</param>
    /// <param name="sequence">The managed sequence the name stands for.</param>
    /// <param name="value">The value.</param>
    /// <returns>False; a value outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_check(name text, sequence regclass, value bigint)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool CheckNamedValue([PgSqlType("text", Schema = "pg_catalog")] PgDatum name,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, long value)
        => Check(Named(name, sequence), value, true);

    /// <summary>
    /// Checks a numeric value a new row holds in a column whose default names its sequence as text.
    /// </summary>
    /// <param name="name">The name the default gives.</param>
    /// <param name="sequence">The managed sequence the name stands for.</param>
    /// <param name="value">The value.</param>
    /// <returns>False; a value outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_check(name text, sequence regclass, value numeric)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool CheckNamedNumeric([PgSqlType("text", Schema = "pg_catalog")] PgDatum name,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, PgNumeric value)
        => Check(Named(name, sequence), value, true);

    /// <summary>
    /// Checks a value an update writes into a column whose default names its sequence as text.
    /// </summary>
    /// <param name="name">The name the default gives.</param>
    /// <param name="sequence">The managed sequence the name stands for.</param>
    /// <param name="value">The value.</param>
    /// <returns>False; a value outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_check_update(name text, sequence regclass, value bigint)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool CheckNamedUpdate([PgSqlType("text", Schema = "pg_catalog")] PgDatum name,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, long value)
        => Check(Named(name, sequence), value, false);

    /// <summary>
    /// Checks a numeric value an update writes into a column whose default names its sequence as text.
    /// </summary>
    /// <param name="name">The name the default gives.</param>
    /// <param name="sequence">The managed sequence the name stands for.</param>
    /// <param name="value">The value.</param>
    /// <returns>False; a value outside this node's blocks fails.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_check_update(name text, sequence regclass, value numeric)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool CheckNamedNumericUpdate([PgSqlType("text", Schema = "pg_catalog")] PgDatum name,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, PgNumeric value)
        => Check(Named(name, sequence), value, false);

    /// <summary>
    /// Checks whether a value a new row holds lies in a block of the sequence a text default's name finds, without
    /// failing.
    /// </summary>
    /// <param name="name">The name the default gives.</param>
    /// <param name="sequence">The managed sequence the name stands for.</param>
    /// <param name="value">The value.</param>
    /// <returns>Whether it does.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_admits(name text, sequence regclass, value bigint)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AdmitsNamedValue([PgSqlType("text", Schema = "pg_catalog")] PgDatum name,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, long value)
    {
        uint seqoid = Named(name, sequence);
        return !s_moved.Contains(seqoid) && Admits(seqoid, value);
    }

    /// <summary>
    /// Checks whether a numeric value a new row holds lies in a block of the sequence a text default's name finds,
    /// without failing.
    /// </summary>
    /// <param name="name">The name the default gives.</param>
    /// <param name="sequence">The managed sequence the name stands for.</param>
    /// <param name="value">The value.</param>
    /// <returns>Whether it does.</returns>
    [PgFunction(Sql = """
        CREATE FUNCTION worf.sequence_range_admits(name text, sequence regclass, value numeric)
        RETURNS boolean STRICT VOLATILE LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
        """)]
    internal static bool AdmitsNamedNumeric([PgSqlType("text", Schema = "pg_catalog")] PgDatum name,
        [PgSqlType("regclass", Schema = "pg_catalog")] PgDatum sequence, PgNumeric value)
    {
        uint seqoid = Named(name, sequence);
        return Whole(value) is decimal exact && exact is >= long.MinValue and <= long.MaxValue && !s_moved.Contains(seqoid) &&
            Admits(seqoid, (long)exact);
    }

    /// <summary>
    /// Finds the sequence to check a value under that a default which names its sequence as text drew, as the row is
    /// written: the managed sequence the name finds under the session's search path, as nextval finds it, or otherwise,
    /// as when it finds nothing or a sequence that isn't managed, the managed sequence the name stands for. The name is
    /// looked up as nextval looks it up, without SQL, so that nothing on the session's search path runs.
    /// </summary>
    /// <param name="name">The name the default gives.</param>
    /// <param name="sequence">The managed sequence the name stands for.</param>
    /// <returns>The sequence.</returns>
    private static unsafe uint Named(PgDatum name, PgDatum sequence)
    {
        Reload();
        RangeVar* relation = NativeMethods.makeRangeVarFromNameList(NativeMethods.textToQualifiedNameList((varlena*)name.DangerousGetBits()));
        uint found = NativeMethods.RangeVarGetRelidExtended(relation, 0, RangeVarMissingOk, default, null);
        return found != 0 && s_sequences.ContainsKey(found) ? found : (uint)sequence.DangerousGetBits();
    }

    /// <summary>
    /// Notes a statement as it starts: it takes the switch gates of the sequences it draws from, and the guard knows one
    /// whose only inserts are rows it moves to another partition: an UPDATE, or a MERGE with no INSERT action, of a
    /// partitioned table, without a data-modifying WITH. A transaction takes on the callbacks that forget them only with
    /// the first such statement.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    /// <param name="written">The tables it writes to.</param>
    internal static unsafe void StartStatement(QueryDesc* queryDesc, List<uint> written)
    {
        GateStatement(queryDesc, written);
        PlannedStmt* statement = queryDesc->plannedstmt;
        if (queryDesc->snapshot is null || statement is null || statement->hasModifyingCTE)
        {
            return;
        }

        // Only a statement whose target is partitioned can move a row to another partition.
        if (statement->planTree is null || statement->planTree->type != NodeTag.T_ModifyTable || ((ModifyTable*)statement->planTree)->rootRelation == 0)
        {
            return;
        }

        bool moves = queryDesc->operation == CmdType.CMD_UPDATE;
        if (queryDesc->operation == CmdType.CMD_MERGE)
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
            if (!s_movingRegistered)
            {
                s_movingRegistered = true;
                _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, ForgetMoving);
                _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, ForgetMoving);
                _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, ForgetMoving);
                _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Abort,
                    static (aborted, _) => s_moving.RemoveAll(statement => statement.Subxact == aborted.Value));
            }

            s_moving.Add(((nint)queryDesc, NativeMethods.GetCurrentSubTransactionId(), queryDesc->snapshot->curcid));
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

        CloseGates((nint)queryDesc);
    }

    /// <summary>
    /// Takes the switch gates of the managed sequences a statement may draw from as it inserts rows: those the defaults
    /// and identity columns of the tables it inserts into draw from, and those of every table it writes to through a
    /// data-modifying WITH. A switch beside the transactions that hold a sequence moves it only while no statement holds
    /// its gate, so a statement's nextval never reads the bounds the sequence had before a switch with the position the
    /// switch moved it to: the statement takes the gate, and then hears of the switches committed, before it draws, and
    /// lets the gate go only between its rows, or as it waits for a switch.
    /// </summary>
    private static unsafe void GateStatement(QueryDesc* queryDesc, List<uint> written)
    {
        PlannedStmt* statement = queryDesc->plannedstmt;
        if (statement is null || written.Count == 0 ||
            !(queryDesc->operation == CmdType.CMD_INSERT || statement->hasModifyingCTE ||
                (queryDesc->operation == CmdType.CMD_MERGE && MergeInserts(statement))) ||
            Worf.CatalogOid("sequence_range") == 0 || !written.Exists(Guarded))
        {
            return;
        }

        Reload();
        if (s_sequences.Count == 0)
        {
            return;
        }

        List<uint> sequences = [.. written.SelectMany(DrawnBy)];
        OpenGates((nint)queryDesc, sequences);
        WaitBeforeDrawing(sequences);
    }

    /// <summary>
    /// Waits, before a statement that holds its gates draws from a sequence, for the switch, once the active block has
    /// reached its last twentieth, or the last value of one reserved for a load, or has no more values left than the
    /// statements that hold the gate, which may each draw one before they check again, so that however many statements
    /// draw at once, they don't run the block out. The sequence's position is read under the lock a SELECT from it takes,
    /// which no switch waits for, so the statement holds the sequence against no switch as it waits.
    /// </summary>
    private static void WaitBeforeDrawing(List<uint> sequences)
    {
        foreach (uint seqoid in sequences.Distinct())
        {
            WaitWhileShort(seqoid);
        }
    }

    /// <summary>
    /// Waits for a sequence's switches as long as its active block is short, by what other statements have left of it:
    /// a statement that waited for a switch checks the next block before it draws, since statements that went on
    /// meanwhile may have drawn most of it.
    /// </summary>
    private static void WaitWhileShort(uint seqoid)
    {
        while (s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) &&
            sequence.Blocks.FindIndex(block => block.State == "active") is int index and >= 0)
        {
            (long lo, long hi, _) = sequence.Blocks[index];
            long? position = ReadPosition(seqoid);
            if ((position is long p && (p < lo || p > hi)) || !Short(seqoid, sequence.Ascending, lo, hi, position, () => ReadPosition(seqoid)) ||
                !WaitForSwitch(seqoid, lo))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Checks whether a sequence's active block has reached its last twentieth, or the last value of one reserved for a
    /// load, or has no more values left than the statements drawing from it.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="ascending">Whether it ascends.</param>
    /// <param name="lo">The active block's lowest value.</param>
    /// <param name="hi">The active block's highest value.</param>
    /// <param name="position">The sequence's position, or null before it issues a value from the block.</param>
    /// <param name="reread">Reads the position again.</param>
    private static bool Short(uint seqoid, bool ascending, long lo, long hi, long? position, Func<long?> reread)
    {
        decimal tail = s_reservedActive.Contains((seqoid, lo)) ? 0 : ((decimal)hi - lo) / 20;
        decimal Left(long? at) => at is long p ? (ascending ? (decimal)hi - p : (decimal)p - lo) : (decimal)hi - lo + 1;

        // No more statements can draw than there are backends, so a block with as many values left needs no count.
        decimal left = Left(position);
        if (left <= tail || left >= Backends())
        {
            return left <= tail;
        }

        // The position is read again after the count, so that every value drawn since is either in it or one a counted
        // statement may draw: a statement draws only while it holds the gate, and one that takes it later checks, with
        // this one counted, before it draws. Read the other way round, a statement could draw the block's last value
        // and then let the gate go to wait for the switch between the reads, and neither would count it.
        int drawing = Drawing(seqoid).Statements;
        left = Left(reread());
        return left <= tail || left < drawing;
    }

    /// <summary>
    /// Reads a sequence's last value, or null when it hasn't issued one, as a SELECT from the sequence does, under the
    /// lock that takes, rather than nextval's.
    /// </summary>
    private static unsafe long? ReadPosition(uint seqoid)
    {
        RelationData* rel = NativeMethods.relation_open(seqoid, (int)PgLockMode.AccessShare);
        try
        {
            SysScanDescData* scan = NativeMethods.systable_beginscan(rel, 0, false, null, 0, null);
            try
            {
                HeapTupleData* tuple = NativeMethods.systable_getnext(scan);
                if (tuple is null)
                {
                    return null;
                }

                bool isnull;
                long lastValue = (long)NativeMethods.heap_getattr(tuple, 1, rel->rd_att, &isnull);
                bool called = NativeMethods.heap_getattr(tuple, 3, rel->rd_att, &isnull) != 0;
                return called ? lastValue : null;
            }
            finally
            {
                NativeMethods.systable_endscan(scan);
            }
        }
        finally
        {
            NativeMethods.relation_close(rel, (int)PgLockMode.None);
        }
    }

    /// <summary>
    /// Checks whether a table has the guard's triggers, from the system catalogs alone, so that a statement that writes
    /// to a table no managed sequence numbers reads none of worf's tables.
    /// </summary>
    private static bool Guarded(uint relid)
    {
        (long, long) version = (SequenceRanges.Version.Value, SequenceRanges.LocalVersion);
        if (version != s_guardedAt)
        {
            s_guarded.Clear();
            s_guardedAt = version;
        }

        if (!s_guarded.TryGetValue(relid, out bool guarded))
        {
            guarded = Elevated.Run(() => Worf.ReadScalar<bool>("""
                SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger
                    WHERE tgrelid = $1 AND tgfoid = 'worf.sequence_range_guard'::pg_catalog.regproc)
                """, SpiParameter.Create(relid)));
            s_guarded[relid] = guarded;
        }

        return guarded;
    }

    /// <summary>
    /// Checks whether a MERGE has an INSERT action.
    /// </summary>
    private static unsafe bool MergeInserts(PlannedStmt* statement)
    {
        if (statement->planTree is null || statement->planTree->type != NodeTag.T_ModifyTable)
        {
            return true;
        }

        List* lists = ((ModifyTable*)statement->planTree)->mergeActionLists;
        for (int i = 0; lists is not null && i < lists->length; i++)
        {
            List* actions = (List*)lists->elements[i].ptr_value;
            for (int j = 0; actions is not null && j < actions->length; j++)
            {
                if (((MergeAction*)actions->elements[j].ptr_value)->commandType == CmdType.CMD_INSERT)
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// Lists the managed sequences a table's defaults and identity columns draw from.
    /// </summary>
    private static List<uint> DrawnBy(uint relid)
    {
        if (relid == 0)
        {
            return [];
        }

        if (!s_drawnBy.TryGetValue(relid, out List<uint>? sequences))
        {
            WorfRows used = Elevated.Run(() => Worf.Read("""
                SELECT d.refobjid FROM pg_catalog.pg_attrdef a JOIN pg_catalog.pg_depend d
                    ON d.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass AND d.objid = a.oid AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
                WHERE a.adrelid = $1
                UNION
                SELECT d.objid FROM pg_catalog.pg_depend d
                WHERE d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refobjid = $1 AND d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                    AND d.deptype = 'i'
                """, 0, SpiParameter.Create(relid)));
            sequences = [.. used.Select(row => row.Get<uint>(0)).Where(s_sequences.ContainsKey)];
            s_drawnBy[relid] = sequences;
        }

        return sequences;
    }

    /// <summary>
    /// Takes the gates of a statement's sequences that this backend doesn't hold, in the sequences' order, and hears of
    /// the switches committed while it waited for them, so that the statement's first nextval reads their bounds.
    /// </summary>
    private static void OpenGates(nint statement, List<uint> sequences)
    {
        List<uint> taken = [];
        foreach (uint seqoid in sequences.Distinct().Order().Where(seqoid => !s_gates.ContainsKey(seqoid)))
        {
            TakeGate(seqoid);
            taken.Add(seqoid);
        }

        if (taken.Count > 0)
        {
            uint subxact;
            unsafe
            {
                subxact = NativeMethods.GetCurrentSubTransactionId();
            }

            s_gating.Add((statement, subxact, taken));
            Refresh();
        }
    }

    /// <summary>
    /// Lets go of the gates a statement took, as it ends.
    /// </summary>
    private static void CloseGates(nint statement)
    {
        int index = s_gating.FindLastIndex(entry => entry.Statement == statement);
        if (index >= 0)
        {
            foreach (uint seqoid in s_gating[index].Sequences)
            {
                LetGate(seqoid);
            }

            s_gating.RemoveAt(index);
        }
    }

    /// <summary>
    /// Lets every gate this backend holds go while a switch waits for one, at a row of a statement that holds them, and
    /// takes them again once the switch has committed, and hears of it, so that the next row's nextval reads its bounds.
    /// </summary>
    private static void PassGates()
    {
        if (s_gates.Count == 0 || SequenceRanges.SwitchesPending.Value == 0)
        {
            return;
        }

        List<uint> passing = [.. s_gates.Keys.Where(seqoid => Drawer(seqoid).Switching > 0).Order()];
        foreach (uint seqoid in passing)
        {
            LetGate(seqoid);
        }

        TakeGates(passing);
    }

    /// <summary>
    /// Lets every gate this backend holds go, as before it waits for a switch, which waits for the gates.
    /// </summary>
    /// <returns>The gates it held.</returns>
    private static List<uint> LetGates()
    {
        List<uint> held = [.. s_gates.Keys.Order()];
        foreach (uint seqoid in held)
        {
            LetGate(seqoid);
        }

        return held;
    }

    /// <summary>
    /// Takes gates again, and hears of the switches committed meanwhile.
    /// </summary>
    private static void TakeGates(List<uint> gates)
    {
        foreach (uint seqoid in gates)
        {
            TakeGate(seqoid);
        }

        if (gates.Count > 0)
        {
            Refresh();
        }
    }

    private static unsafe void TakeGate(uint seqoid)
    {
        HookExit();
        RegisterGates();
        LOCKTAG tag = GateTag(seqoid);
        _ = NativeMethods.LockAcquire(&tag, ShareLock, false, false);
        s_gates[seqoid] = NativeMethods.GetCurrentSubTransactionId();
        CountDrawer(seqoid, 1);
    }

    private static unsafe void LetGate(uint seqoid)
    {
        if (s_gates.Remove(seqoid))
        {
            LOCKTAG tag = GateTag(seqoid);
            _ = NativeMethods.LockRelease(&tag, ShareLock, false);
            CountDrawer(seqoid, -1);
        }
    }

    /// <summary>
    /// Reads the number of statements that hold a sequence's gate in this database, which may each draw a value before
    /// they check the sequence again, and the number that wait for it to switch.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <returns>The numbers.</returns>
    internal static (int Statements, int Waiting) Drawing(uint seqoid)
    {
        WorfDrawer drawer = Drawer(seqoid);
        return (drawer.Statements, drawer.Waiting);
    }

    /// <summary>
    /// Reads a sequence's entry in the shared table.
    /// </summary>
    private static WorfDrawer Drawer(uint seqoid)
    {
        uint dboid = DatabaseId();
        using PgLwLockShareGuard<WorfDrawers, WorfDrawer> guard = Drawers.Share();
        for (int probe = 0, i = Slot(dboid, seqoid); probe < MaxDrawers && guard[i].Dboid != 0; probe++, i = (i + 1) % MaxDrawers)
        {
            if (guard[i].Dboid == dboid && guard[i].Seqoid == seqoid)
            {
                return guard[i];
            }
        }

        return default;
    }

    /// <summary>
    /// Reads how many backends the server has room for.
    /// </summary>
    private static int Backends()
    {
        if (s_backends == 0)
        {
            unsafe
            {
                s_backends = NativeGlobals.MaxBackends;
            }
        }

        return s_backends;
    }

    /// <summary>
    /// Finds where a sequence's entry starts its search in the shared table. An entry stays once made, with its counts
    /// at zero, so that no search stops short at a freed one.
    /// </summary>
    private static int Slot(uint dboid, uint seqoid) => (int)((dboid * 2654435761UL ^ seqoid) % MaxDrawers);

    /// <summary>
    /// Counts a statement that takes or lets go of a sequence's gate, or that starts or stops waiting for it to switch.
    /// A sequence that finds no entry free goes uncounted.
    /// </summary>
    private static void CountDrawer(uint seqoid, int statements, int waiting = 0, int switching = 0)
    {
        uint dboid = DatabaseId();
        using PgLwLockExclusiveGuard<WorfDrawers, WorfDrawer> guard = Drawers.Exclusive();
        for (int probe = 0, i = Slot(dboid, seqoid); probe < MaxDrawers; probe++, i = (i + 1) % MaxDrawers)
        {
            if (guard[i].Dboid == dboid && guard[i].Seqoid == seqoid)
            {
                WorfDrawer drawer = guard[i];
                drawer.Statements = Math.Max(drawer.Statements + statements, 0);
                drawer.Waiting = Math.Max(drawer.Waiting + waiting, 0);
                drawer.Switching = Math.Max(drawer.Switching + switching, 0);
                guard[i] = drawer;
                return;
            }

            if (guard[i].Dboid == 0)
            {
                if (statements > 0 || waiting > 0 || switching > 0)
                {
                    guard[i] = new WorfDrawer
                    {
                        Dboid = dboid,
                        Seqoid = seqoid,
                        Statements = Math.Max(statements, 0),
                        Waiting = Math.Max(waiting, 0),
                        Switching = Math.Max(switching, 0),
                    };
                }

                return;
            }
        }
    }

    /// <summary>
    /// Shuts a sequence's switch gate for a switch beside the transactions that hold it, until its transaction ends:
    /// it waits for the statements that hold the gate to reach their next rows, while those that start wait for it,
    /// and fails with 55P03 once the time is up. Statements that hold the gate let it go at their next row while any
    /// switch waits for one.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="timeout">How long to wait, in milliseconds: a tenth of a second in the range worker, which checks
    /// the other sequences meanwhile, and a second in a switch worker.</param>
    internal static unsafe void ShutGate(uint seqoid, int timeout)
    {
        _ = PgAtomic.Add(SequenceRanges.SwitchesPending, 1L);
        CountDrawer(seqoid, 0, 0, 1);
        void Done()
        {
            CountDrawer(seqoid, 0, 0, -1);
            _ = PgAtomic.Add(SequenceRanges.SwitchesPending, -1L);
        }

        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, Done);
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, Done);
        _ = Worf.Execute($"SET LOCAL lock_timeout = {timeout.ToString(CultureInfo.InvariantCulture)}");
        LOCKTAG tag = GateTag(seqoid);
        _ = NativeMethods.LockAcquire(&tag, ExclusiveLock, false, false);
    }

    /// <summary>
    /// Names a sequence's switch gate.
    /// </summary>
    private static LOCKTAG GateTag(uint seqoid) => new()
    {
        locktag_field1 = DatabaseId(),
        locktag_field2 = seqoid,
        locktag_field3 = 0,
        locktag_field4 = GateKey,
        locktag_type = LockTagAdvisory,
        locktag_lockmethodid = DefaultLockMethod,
    };

    /// <summary>
    /// Keeps track of the gates through the transaction: a subtransaction that commits passes those it took to its
    /// parent, one that aborts lets them go, as PostgreSQL does their locks, and a transaction that prepares lets them
    /// all go first, so that a prepared transaction holds none.
    /// </summary>
    private static void RegisterGates()
    {
        if (s_gatesRegistered)
        {
            return;
        }

        s_gatesRegistered = true;
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, ForgetGates);
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, ForgetGates);
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.PrePrepare, static () => _ = LetGates());
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, ForgetGates);
        _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Commit, static (subxact, parent) =>
        {
            foreach (uint seqoid in s_gates.Where(gate => gate.Value == subxact.Value).Select(gate => gate.Key).ToList())
            {
                s_gates[seqoid] = parent.Value;
            }

            for (int i = 0; i < s_gating.Count; i++)
            {
                if (s_gating[i].Subxact == subxact.Value)
                {
                    s_gating[i] = s_gating[i] with { Subxact = parent.Value };
                }
            }
        });

        // The statements of a subtransaction that aborts end without ExecutorEnd.
        _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Abort, static (aborted, parent) =>
        {
            foreach (uint seqoid in s_gates.Where(gate => gate.Value == aborted.Value).Select(gate => gate.Key).ToList())
            {
                _ = s_gates.Remove(seqoid);
                CountDrawer(seqoid, -1);
            }

            _ = s_gating.RemoveAll(entry => entry.Subxact == aborted.Value);
        });
    }

    private static void ForgetGates()
    {
        foreach (uint seqoid in s_gates.Keys)
        {
            CountDrawer(seqoid, -1);
        }

        s_gates.Clear();
        s_gating.Clear();
        s_gatesRegistered = false;
    }

    /// <summary>
    /// Forgets the moving statements of a transaction as it ends.
    /// </summary>
    private static void ForgetMoving()
    {
        s_moving.Clear();
        s_movingRegistered = false;
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

        (uint subxact, uint cid) = (NativeMethods.GetCurrentSubTransactionId(), NativeMethods.GetActiveSnapshot()->curcid);
        foreach ((_, uint statementSubxact, uint statementCid) in s_moving)
        {
            if ((statementSubxact, statementCid) == (subxact, cid))
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
        // A switch beside this transaction, which holds the sequence, gives it new bounds, which nextval reads from the
        // catalog as this backend last heard of them, and a switch that waits for the gate moves it before the next row.
        PassGates();
        Refresh();
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
                    bool admitted = s_reached.TryGetValue(seqoid, out long reached) && (sequence.Ascending ? reached >= value : reached <= value);
                    if (!admitted && Position(seqoid) is long position && (sequence.Ascending ? position >= value : position <= value))
                    {
                        s_reached[seqoid] = position;
                        admitted = true;
                    }

                    if (admitted)
                    {
                        NearEnd(seqoid, sequence.Ascending, lo, hi, value);
                    }

                    return admitted;

                // A switch beside the transactions that hold the sequence moves it to the next block before it commits,
                // and a value it issued from the block meanwhile is this node's.
                case "next":
                    return Position(seqoid) is long issued && issued >= lo && issued <= hi && (sequence.Ascending ? issued >= value : issued <= value);

                default:
                    return false;
            }
        }

        return false;
    }

    /// <summary>
    /// Wakes the range worker once a sequence passes half its active block, the most it leaves before switching, and
    /// again once it reaches the last twentieth, the least, so that a sequence that runs through a block faster than the
    /// worker checks switches in time.
    /// </summary>
    /// <returns>Whether the value is in the block's last twentieth.</returns>
    private static bool WakeNearEnd(uint seqoid, bool ascending, long lo, long hi, long value)
    {
        long half = (long)(((decimal)hi - lo) / 2);
        long tail = (long)(((decimal)hi - lo) / 20);
        bool last = ascending ? value >= hi - tail : value <= lo + tail;
        long? mark = last ? (ascending ? hi - tail : lo + tail)
            : (ascending ? value >= hi - half : value <= lo + half) ? (ascending ? hi - half : lo + half)
            : null;
        if (mark is long point && s_woken.Add((seqoid, point)))
        {
            _ = PgAtomic.Add(SequenceRanges.CheckRequested, 1L);
            Worker.WakeRangeWorker();
        }

        return last;
    }

    /// <summary>
    /// Waits, once a sequence reaches the last twentieth of its active block, until the block is switched, so that
    /// inserts wait for the range worker to wake and switch, or for a next block on its way from the exchange worker,
    /// rather than run the block out. A backend waits up to twice the exchange timeout for each block in all, which
    /// covers an exchange that hangs and the check after it, however many of its statements and rows wait. It waits only
    /// while the worker can switch: the sequence draws from blocks, or has used one up and waits for the next, and no
    /// transaction holds the sequence against the switch, as one that switches it itself does, or DDL on it. A switch
    /// the range worker or a switch worker makes is no such transaction, nor is a lock still waited for, so a statement
    /// that finds one goes on waiting, and checks again at each of its rows.
    /// </summary>
    /// <returns>Whether the block was switched.</returns>
    private static bool WaitForSwitch(uint seqoid, long lo)
    {
        // The switch waits for the gates, which the statement lets go meanwhile, and the range worker switches the block
        // while statements wait for it, however many values it has left.
        List<uint> held = LetGates();
        CountWaiting(seqoid, 1);
        bool switched;
        try
        {
            switched = WaitForSwitchWithoutGates(seqoid, lo);
        }
        finally
        {
            CountWaiting(seqoid, -1);
        }

        TakeGates(held);
        return switched;
    }

    /// <summary>
    /// Counts this backend as waiting for a sequence's switch, or no longer, in the shared table and its own.
    /// </summary>
    private static void CountWaiting(uint seqoid, int change)
    {
        HookExit();
        CountDrawer(seqoid, 0, change);
        int waiting = s_waitingFor.GetValueOrDefault(seqoid) + change;
        if (waiting > 0)
        {
            s_waitingFor[seqoid] = waiting;
        }
        else
        {
            _ = s_waitingFor.Remove(seqoid);
        }
    }

    /// <summary>
    /// Takes this backend's counts out of the shared table as it exits, whatever it was doing.
    /// </summary>
    private static void HookExit()
    {
        if (s_exitHooked)
        {
            return;
        }

        s_exitHooked = true;
        _ = PgProcess.OnExit(static _ =>
        {
            foreach ((uint seqoid, int waiting) in s_waitingFor)
            {
                CountDrawer(seqoid, 0, -waiting);
            }

            foreach (uint seqoid in s_gates.Keys)
            {
                CountDrawer(seqoid, -1);
            }

            s_waitingFor.Clear();
            s_gates.Clear();
        });
    }

    /// <summary>
    /// Waits for a switch, with no gate held.
    /// </summary>
    private static bool WaitForSwitchWithoutGates(uint seqoid, long lo)
    {
        SpiParameter seq = SpiParameter.Create(seqoid);
        SpiParameter database = SpiParameter.Create(DatabaseId());
        SpiParameter switching = SpiParameter.Create(Switchers());
        long now = Worker.GetCurrentTimestamp();
        if (!s_waited.TryGetValue((seqoid, lo), out long deadline))
        {
            deadline = now + (SequenceRanges.ExchangeTimeout * 2000L);
            s_waited.Add((seqoid, lo), deadline);
        }

        long woken = 0;
        while (now < deadline)
        {
            if (!Elevated.Run(() => Worf.SelectCatalog("""
                SELECT EXISTS (SELECT 1 FROM worf.sequence_range WHERE seqoid = $1 AND state IN ('active', 'exhausted'))
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_catalog.pg_locks
                        WHERE locktype = 'relation' AND database = $2 AND relation = $1 AND granted AND pid <> ALL ($3::pg_catalog.int4[])
                            AND mode IN ('ShareLock', 'ShareRowExclusiveLock', 'ExclusiveLock', 'AccessExclusiveLock'))
                """, seq, database, switching)[0].Get<bool>(0)))
            {
                return false;
            }

            // The worker tries the switch again soon after one that gave up on the lock, and requests a block the
            // moment it can.
            if (now - woken >= 100_000)
            {
                woken = now;
                _ = PgAtomic.Add(SequenceRanges.CheckRequested, 1L);
                Worker.WakeRangeWorker();
            }

            PgInterrupts.Check();
            Worker.WaitLatch(10);
            Reload();
            if (!s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) ||
                !sequence.Blocks.Exists(block => block.State == "active" && block.Lo == lo))
            {
                // Nextval takes the sequence's new bounds from the catalog once this backend hears of the switch, which
                // its commit sends only after it tells backends of the new blocks, so they're read again at once.
                Refresh();
                ForgetBounds();
                return true;
            }

            now = Worker.GetCurrentTimestamp();
        }

        return false;
    }

    /// <summary>
    /// Lists the processes of this database's range worker and switch workers, whose locks on a sequence are its switches.
    /// </summary>
    private static int[] Switchers()
    {
        uint dboid = DatabaseId();
        List<int> pids = [];
        using PgLwLockShareGuard<WorfContext, WorfWorker> guard = Worker.WorfCtx.Share();
        for (int i = 0; i < guard.Length; i++)
        {
            if (guard[i].WorkerType is WorfWorkerType.Range or WorfWorkerType.Switch && guard[i].Dboid == dboid && guard[i].Proc != 0)
            {
                unsafe
                {
                    pids.Add(((PGPROC*)guard[i].Proc)->pid);
                }
            }
        }

        return [.. pids];
    }

    /// <summary>
    /// Switches a sequence to its next block in this transaction, once for each block, when the transaction holds the
    /// sequence, as one that drew from it does.
    /// </summary>
    private static void SwitchInTransaction(uint seqoid, long lo)
    {
        if (!s_switchedInTransaction.Add((seqoid, lo)))
        {
            return;
        }

        List<uint> held = LetGates();

        if (s_switchedInTransaction.Count == 1)
        {
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, ForgetSwitches);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, ForgetSwitches);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, ForgetSwitches);
        }

        // A transaction that switched the sequence itself holds it against the range worker's switches, which would
        // otherwise come as it waits, so it waits for the next block the worker brings meanwhile, and switches to it.
        long deadline = Worker.GetCurrentTimestamp() + (SequenceRanges.ExchangeTimeout * 2000L);
        long woken = 0;
        while (true)
        {
            if (SequenceRanges.SwitchInTransaction(seqoid))
            {
                _ = s_switchedItself.Add(seqoid);
                Reload();
                break;
            }

            long now = Worker.GetCurrentTimestamp();
            if (!s_switchedItself.Contains(seqoid) || now >= deadline)
            {
                break;
            }

            if (now - woken >= 100_000)
            {
                woken = now;
                _ = PgAtomic.Add(SequenceRanges.CheckRequested, 1L);
                Worker.WakeRangeWorker();
            }

            PgInterrupts.Check();
            Worker.WaitLatch(10);
        }

        TakeGates(held);
    }

    private static void ForgetSwitches()
    {
        s_switchedInTransaction.Clear();
        s_switchedItself.Clear();
    }

    /// <summary>
    /// Waits for the range worker's switch once a value reaches the last twentieth of its active block, or the last
    /// value of one reserved for a load, which the worker uses up, and switches in this transaction at the block's last
    /// value, so that the transaction's later rows take the next block's values.
    /// </summary>
    private static void NearEnd(uint seqoid, bool ascending, long lo, long hi, long value)
    {
        // What the block has left is what other statements have left it since this row's value, as far as they drew, read
        // under the lock nextval left this transaction holding.
        bool last = (ascending ? (decimal)hi - value : (decimal)value - lo) == 0;
        bool reserved = s_reservedActive.Contains((seqoid, lo));
        if (((reserved ? last : WakeNearEnd(seqoid, ascending, lo, hi, value)) || (!last && Short(seqoid, ascending, lo, hi, Position(seqoid) ?? value, () => Position(seqoid) ?? value))) &&
            WaitForSwitch(seqoid, lo))
        {
            WaitWhileShort(seqoid);
        }
        else if (last)
        {
            SwitchInTransaction(seqoid, lo);
        }
    }

    /// <summary>
    /// Builds the condition a COPY FROM into a table whose defaults draw from a managed sequence runs for each row, which
    /// reads <see cref="CopyCheck"/>, and starts checking the sequences, until <see cref="EndCopy"/>. The condition is
    /// always true and stable, so COPY still writes rows a buffer at a time.
    /// </summary>
    /// <param name="relid">The table.</param>
    /// <returns>The condition, unparsed, or null when no managed sequence numbers the table's rows.</returns>
    internal static unsafe Node* CopyCondition(uint relid)
    {
        s_copying.Add([]);
        s_copyGates.Add(--s_lastCopyGate);
        Reload();
        if (s_sequences.Count == 0 || relid == 0)
        {
            return null;
        }

        s_copying[^1].AddRange(DrawnBy(relid));
        if (s_copying[^1].Count == 0)
        {
            return null;
        }

        OpenGates(s_copyGates[^1], s_copying[^1]);
        WaitBeforeDrawing(s_copying[^1]);

        if (!s_copyCheckInstalled)
        {
            *GucOption.ShowHook("worf.sequence_range_copy_check") = CopyCheckShow;

            MemoryContextData* old = Worf.MemoryContextSwitchTo(NativeGlobals.TopMemoryContext);
            s_copyCheckValue = (nint)Worf.PStrDup("");
            _ = Worf.MemoryContextSwitchTo(old);
            s_copyCheckInstalled = true;
        }

        List* parsed = NativeMethods.raw_parser(Worf.PStrDup("SELECT pg_catalog.current_setting('worf.sequence_range_copy_check') IS NOT NULL"),
            RawParseMode.RAW_PARSE_DEFAULT);
        SelectStmt* select = (SelectStmt*)((RawStmt*)parsed->elements[0].ptr_value)->stmt;
        return ((ResTarget*)select->targetList->elements[0].ptr_value)->val;
    }

    /// <summary>
    /// Stops checking the sequences of the innermost COPY FROM that <see cref="CopyCondition"/> started.
    /// </summary>
    internal static void EndCopy()
    {
        CloseGates(s_copyGates[^1]);
        s_copyGates.RemoveAt(s_copyGates.Count - 1);
        s_copying.RemoveAt(s_copying.Count - 1);
    }

    /// <summary>
    /// Checks the sequences the innermost COPY FROM draws from, after a row's defaults drew their values.
    /// </summary>
    private static unsafe sbyte* CheckCopied()
    {
        if (s_copying.Count == 0 || s_copying[^1].Count == 0)
        {
            return (sbyte*)s_copyCheckValue;
        }

        PassGates();
        Refresh();
        Reload();
        foreach (uint seqoid in s_copying[^1])
        {
            if (!s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) ||
                !sequence.Blocks.Exists(block => block.State == "active") || Position(seqoid) is not long position)
            {
                continue;
            }

            (long lo, long hi, _) = sequence.Blocks.Find(block => block.State == "active");
            if (position >= lo && position <= hi)
            {
                NearEnd(seqoid, sequence.Ascending, lo, hi, position);
            }
        }

        return (sbyte*)s_copyCheckValue;
    }

    /// <summary>
    /// Hears of the catalog changes other backends committed, as a switch's new bounds for its sequence.
    /// </summary>
    private static void Refresh()
    {
        unsafe
        {
            NativeMethods.AcceptInvalidationMessages();
        }
    }

    /// <summary>
    /// Reads every sequence's bounds from the catalog again at its next nextval, as a switch committed whose
    /// invalidations this backend may not have heard yet.
    /// </summary>
    internal static void ForgetBounds()
    {
        unsafe
        {
            NativeMethods.CatalogCacheFlushCatalog(SequenceRelationId);
            NativeMethods.InvalidateCatalogSnapshot();
        }
    }

    private const uint SequenceRelationId = 2224;

    private static uint DatabaseId()
    {
        unsafe
        {
            return NativeGlobals.MyDatabaseId;
        }
    }

    /// <summary>
    /// Checks the values a table's rows hold in a column that DDL brings under the guard, as when it sets the column's
    /// default or attaches the table as a partition. A value passes when the guard would pass it or it lies before
    /// where range management of the column's sequences started; in a session that restores rows, as an apply worker
    /// does for DDL from another node, a value also passes unless one of the sequences may still issue it, as far as
    /// this node knows. A numeric value that isn't a whole number within bigint, as NaN, is none of a sequence's. The
    /// check looks for the values that fail by their spans, so that an index on the column, or a constraint on the column
    /// that rules the spans out, spares it from reading every row; otherwise it reads them all. It reads them as
    /// <see cref="StoredValues"/> does, whatever the table's row-level security, and running nothing of the user's.
    /// </summary>
    /// <param name="relid">The table.</param>
    /// <param name="inherited">Whether to check its inheritors' and partitions' rows too.</param>
    /// <param name="column">The column.</param>
    /// <param name="sequences">The managed sequences the column uses.</param>
    internal static void CheckRows(uint relid, bool inherited, string column, List<uint> sequences)
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

        // The table's constraints, as a partition's bound, may rule the spans out, and spare reading its rows, as they spare
        // PostgreSQL's own check of an attached partition's rows.
        string table = SequenceRanges.QualifiedName(relid);
        string? outside = StoredValues.FindWithin(relid, inherited, column, spans);
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

        WorfRows range = Elevated.Run(() => Worf.Read("""
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

        // A change to the blocks, as a switch, tells backends before its commit sends its invalidations, so the
        // sequences' bounds are read again too.
        ForgetBounds();
        s_sequences.Clear();
        s_drawnBy.Clear();
        s_reservedActive.Clear();
        s_moved.Clear();
        s_reached.Clear();
        s_issuable.Clear();
        s_woken.Clear();
        ulong sysid;
        uint timeline;
        unsafe
        {
            sysid = NativeMethods.GetSystemIdentifier();
            timeline = SequenceRanges.Timeline();
        }

        // The blocks as last committed, since a switch that committed after this statement's snapshot may already
        // have given the sequence its new block.
        WorfRows result = Elevated.Run(() => Worf.SelectCatalog("""
            SELECT r.seqoid::oid, s.seqincrement > 0, b.lo, b.hi, b.state,
                b.node_id = l.node_id AND b.sysid = $1 AND b.timeline = $2, b.reserved
            FROM worf.sequence_range r
                JOIN pg_catalog.pg_sequence s ON s.seqrelid = r.seqoid
                CROSS JOIN worf.local_node l
                LEFT JOIN worf.sequence_range_block b ON b.seqoid = r.seqoid AND b.kind IN ('n', 'i')
                    AND b.state IN ('next', 'active', 'retired', 'imported')
            """, SpiParameter.Create((decimal)sysid), SpiParameter.Create((long)timeline)));
        for (int i = 0; i < result.Count; i++)
        {
            WorfRow row = result[i];
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
                if (state == "active" && row.Get<bool>(6))
                {
                    _ = s_reservedActive.Add((seqoid, lo));
                }
            }
            else if (state is not "retired")
            {
                _ = s_moved.Add(seqoid);
            }
        }

        foreach ((uint Seqoid, long Lo) waited in s_waited.Keys.Where(waited =>
            !s_sequences.TryGetValue(waited.Seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) ||
            !sequence.Blocks.Exists(block => block.State == "active" && block.Lo == waited.Lo)).ToList())
        {
            _ = s_waited.Remove(waited);
        }

        s_version = version;
        s_localVersion = localVersion;
    }
}
