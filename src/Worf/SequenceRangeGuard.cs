using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
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

    /// <summary>
    /// Gets the detail of the failure of a call of nextval that waited for its grantor too long, which an apply worker
    /// applies again later, as it says nothing about the change's data.
    /// </summary>
    internal const string GrantorUnheard = "A server that restarted draws from a block its parent granted only once it has heard from the parent, which may have given the block's values out again meanwhile.";

    /// <summary>
    /// Gets the detail of the failure of a call of nextval whose sequence worf can't tell that waited for a grantor too
    /// long, which an apply worker applies again later.
    /// </summary>
    internal const string UnnamedGrantorUnheard = "worf can't tell which sequence the call draws from before it runs, as for a name computed as it runs, so the call waits for every grantor this server hasn't heard from.";

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

    // A sequence's door, an advisory lock beside its gate with 4 as its objsubid, which a statement passes as it takes
    // the gate, and a switch shuts as it waits for the gate, so that no statement takes the gate meanwhile.
    private const ushort DoorKey = 4;

    // The switch gates this backend holds, by sequence, with the subtransaction that took each last, and the statements
    // that took them, by their query descriptors, or the COPY FROM they run, with the gates each took. The sequences
    // each table's defaults draw from, as of the blocks last read.
    private static readonly Dictionary<uint, uint> s_gates = [];
    private static readonly List<(nint Statement, uint Subxact, List<uint> Sequences)> s_gating = [];
    private static readonly Dictionary<uint, List<uint>> s_drawnBy = [];

    // The managed sequences each table's columns draw from, by column, through a default or as identity columns.
    private static readonly Dictionary<uint, List<(short Column, uint Sequence)>> s_drawingColumns = [];

    // The statements and commands whose expressions are starting, innermost last.
    private static readonly List<WorfStartingStatement> s_starting = [];

    // How many calls of nextval each running statement's plan started as it started, by its query descriptor.
    private static readonly Dictionary<nint, int> s_planStarted = [];

    // The fields of the nodes of a plan that nodeToString writes a type's OID in.
    private static readonly HashSet<string> s_typeFields =
    [
        "vartype", "consttype", "paramtype", "aggtype", "aggtranstype", "wintype", "funcresulttype", "opresulttype", "resulttype",
        "array_typeid", "element_typeid", "row_typeid", "casetype", "coalescetype", "minmaxtype", "refcontainertype", "refelemtype",
        "refrestype", "firstColType", "typeId", "typid", "msftype", "exprBaseType",
    ];

    // The PL/pgSQL expression whose calls of nextval started last, which PL/pgSQL starts outside a statement, as of the
    // transaction it started in before, with the sequences its calls name once a call has looked, and how many have.
    private static (nint Expr, uint Lxid, (HashSet<uint> Called, int Calls, bool Unnamed)? Draws, int Looked) s_simple;

    // Where PL/pgSQL finds its instrumentation plugin, which it tells the function its error context calls.
    private static unsafe void** s_plpgsqlPlugin;

    // The text of the defaults a worker's rows of a table take, by the columns the rows carry, and of the CHECK
    // constraints and trigger conditions the rows start.
    private static readonly Dictionary<(uint Relid, string Sent), string> s_defaultsText = [];

    // The sequences the CHECK constraints of this database's domains draw from, and whether one of them draws from a
    // sequence it doesn't name, read again once a type changes.
    private static (HashSet<uint> Called, bool Unnamed)? s_domainDraws;

    // Whether the guard hears of the changes to tables and types that the text it holds by table reads, as a default
    // set, a partition attached or a domain's CHECK added.
    private static bool s_watchingTables;

#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    // TYPEOID and CONSTROID, the ids of the catalog caches of types and constraints, which only PostgreSQL 19's
    // bindings name.
    private const int TypeCacheId = 80;
    private const int ConstraintCacheId = 19;
#elif ANKUS_PG18
    private const int TypeCacheId = 82;
    private const int ConstraintCacheId = 19;
#endif

    [PgNativeCallback(nameof(TableInvalidated))]
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    private static partial RelcacheCallbackFunction TableCallback { get; }
#else
    private static partial RelSyncCallbackFunction TableCallback { get; }
#endif

    [PgNativeCallback(nameof(TypeInvalidated))]
    private static partial SyscacheCallbackFunction TypeCallback { get; }

    // Whether each table has the guard's triggers, which only tables numbered by a managed sequence have, as of the
    // versions of the shared and this backend's counters it was looked up at.
    private static readonly Dictionary<uint, bool> s_guarded = [];
    private static (long Version, long Local) s_guardedAt = (-1, -1);
    private static bool s_gatesRegistered;

    // The sequences this backend waits for a switch of, which it counts until it stops waiting, or exits, as at a FATAL
    // error, which skips the code after the wait.
    private static readonly Dictionary<uint, (int Count, long Lo)> s_waitingFor = [];
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

    // The node that granted each sequence's active block, its parent, when another node did, and the grantors this
    // backend has seen this server hear from since it started.
    private static readonly Dictionary<uint, uint> s_grantors = [];
    private static readonly HashSet<uint> s_confirmed = [];

    // Whether the guard is reading the blocks to find the grantors a statement's plan names.
    private static bool s_namingGrantors;

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
    private static readonly Dictionary<(uint Seqoid, long Lo), (long Deadline, bool Expired)> s_waited = [];

    // The active blocks, by sequence and start, that this transaction has tried to switch away from itself, as it ran
    // them out, each with the subtransaction that tried, and the managed sequences that the COPY FROM statements
    // running in this backend draw from, innermost last.
    private static readonly Dictionary<(uint Seqoid, long Lo), uint> s_switchedInTransaction = [];

    // The sequences this transaction switched itself, which it holds against every other switch until it ends, each with
    // the subtransaction that switched it, whose rollback to a savepoint lets the sequence go and undoes the switch.
    private static readonly Dictionary<uint, uint> s_switchedItself = [];
    private static bool s_switchesRegistered;
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
        _ = s_planStarted.Remove((nint)queryDesc);
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
        bool inserts = queryDesc->operation == CmdType.CMD_INSERT || (statement is not null && statement->hasModifyingCTE) ||
            (queryDesc->operation == CmdType.CMD_MERGE && statement is not null && MergeInserts(statement));
        bool updates = queryDesc->operation is CmdType.CMD_UPDATE or CmdType.CMD_MERGE;
        if (statement is null || written.Count == 0 || !(inserts || updates) || Worf.CatalogOid("sequence_range") == 0 || !written.Exists(Guarded))
        {
            return;
        }

        Reload();
        if (s_sequences.Count == 0)
        {
            return;
        }

        // An UPDATE, or a MERGE's, draws only as it sets a column to its default, which an identity column draws from
        // without calling nextval, so it holds the gates of the sequences of the columns it sets that its plan draws from.
        List<uint> sequences = inserts ? [.. written.SelectMany(DrawnBy)] : UpdatedDrawing(statement);
        if (sequences.Count == 0)
        {
            return;
        }

        OpenGates((nint)queryDesc, sequences);
        WaitBeforeDrawing(sequences);
    }

    /// <summary>
    /// Lists the managed sequences an UPDATE, or a MERGE's update actions, draws from: those of the columns it sets that
    /// draw from one by default or as identity columns, as a partition's draw from its ancestors', that its plan draws
    /// from, as setting such a column to its default does, rather than to a value.
    /// </summary>
    private static unsafe List<uint> UpdatedDrawing(PlannedStmt* statement)
    {
        List<uint> sequences = [];
        if (statement->planTree is null || statement->planTree->type != NodeTag.T_ModifyTable)
        {
            return sequences;
        }

        ModifyTable* modify = (ModifyTable*)statement->planTree;
        List* results = modify->resultRelations;
        for (int i = 0; results is not null && i < results->length; i++)
        {
            RangeTblEntry* rte = (RangeTblEntry*)statement->rtable->elements[results->elements[i].int_value - 1].ptr_value;
            List<(short Column, uint Sequence)> drawing = DrawingColumns(rte->relid);
            if (drawing.Count == 0)
            {
                continue;
            }

            List<nint> set = [];
            if (modify->operation == CmdType.CMD_UPDATE && modify->updateColnosLists is not null && i < modify->updateColnosLists->length)
            {
                set.Add((nint)modify->updateColnosLists->elements[i].ptr_value);
            }

            List* actions = modify->mergeActionLists is not null && i < modify->mergeActionLists->length
                ? (List*)modify->mergeActionLists->elements[i].ptr_value
                : null;
            for (int j = 0; actions is not null && j < actions->length; j++)
            {
                MergeAction* action = (MergeAction*)actions->elements[j].ptr_value;
                if (action->commandType == CmdType.CMD_UPDATE)
                {
                    set.Add((nint)action->updateColnos);
                }
            }

            for (int s = 0; s < set.Count; s++)
            {
                List* columns = (List*)set[s];
                for (int k = 0; columns is not null && k < columns->length; k++)
                {
                    int column = columns->elements[k].int_value;
                    sequences.AddRange(drawing.Where(draw => draw.Column == column).Select(draw => draw.Sequence));
                }
            }
        }

        if (sequences.Count == 0)
        {
            return sequences;
        }

        string text = NodeText((Node*)statement);
        (HashSet<uint> called, _, _) = Draws(text);
        return [.. sequences.Distinct().Where(seqoid => called.Contains(seqoid) ||
            text.Contains($"{{NEXTVALUEEXPR :seqid {seqoid.ToString(CultureInfo.InvariantCulture)} ", StringComparison.Ordinal))];
    }

    /// <summary>
    /// Lists the managed sequences a table's columns draw from, by column, through their defaults or as identity
    /// columns, a partition's identity columns drawing from its ancestors' sequences, by name, as an update through an
    /// ancestor sets them to its default on every version, which the plan of an update shows.
    /// </summary>
    private static List<(short Column, uint Sequence)> DrawingColumns(uint relid)
    {
        WatchTables();
        if (!s_drawingColumns.TryGetValue(relid, out List<(short Column, uint Sequence)>? drawing))
        {
            WorfRows used = Elevated.Run(() => Worf.Read("""
                SELECT a.adnum, d.refobjid FROM pg_catalog.pg_attrdef a JOIN pg_catalog.pg_depend d
                    ON d.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass AND d.objid = a.oid AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
                WHERE a.adrelid = $1
                UNION
                SELECT c.attnum, d.objid FROM pg_catalog.pg_depend d
                    JOIN pg_catalog.pg_attribute p ON p.attrelid = d.refobjid AND p.attnum = d.refobjsubid
                    JOIN pg_catalog.pg_attribute c ON c.attrelid = $1 AND c.attname = p.attname AND NOT c.attisdropped
                WHERE d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                    AND d.deptype = 'i' AND (d.refobjid = $1 OR d.refobjid IN (SELECT a.relid FROM pg_catalog.pg_partition_ancestors($1) a))
                """, 0, SpiParameter.Create(relid)));
            drawing = [.. used.Select(row => (row.Get<short>(0), row.Get<uint>(1))).Where(draw => s_sequences.ContainsKey(draw.Item2))];
            s_drawingColumns[relid] = drawing;
        }

        return drawing;
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
    /// meanwhile may have drawn most of it. It reads the blocks as last committed, with the gate held, since a switch
    /// may have committed while it waited for the gate, so that the position isn't taken for one past an old block.
    /// </summary>
    private static void WaitWhileShort(uint seqoid)
    {
        WaitForGrantor(seqoid);
        bool waitedForBlock = false;
        while (true)
        {
            Refresh();
            Reload();
            if (!s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence))
            {
                return;
            }

            if (sequence.Blocks.FindIndex(block => block.State == "active") is not (int index and >= 0))
            {
                // A sequence with no block to draw from, as one that just reset, waits for the block it asked for, once.
                if (waitedForBlock || !WaitForBlock(seqoid))
                {
                    return;
                }

                waitedForBlock = true;
                continue;
            }

            (long lo, long hi, _) = sequence.Blocks[index];
            long? position = ReadPosition(seqoid);

            // A position past the block, in the next one, as a switch to it that rolled back leaves it, has run the block
            // out, while its bounds are still the block's, as has one at its last value.
            bool past = position is long p && (sequence.Ascending ? p > hi : p < lo);
            if (position is long q && (q < lo || q > hi) && !past)
            {
                return;
            }

            if (!past && !Short(seqoid, sequence.Ascending, lo, hi, position, () => ReadPosition(seqoid)))
            {
                return;
            }

            if (!WaitForSwitch(seqoid, lo))
            {
                // A statement that would draw from a block run out, which no worker switched in time, switches it in its
                // transaction, as one that drew the block's last value does, taking the lock on the sequence that nextval
                // takes first.
                if (past || position == (sequence.Ascending ? hi : lo))
                {
                    unsafe
                    {
                        NativeMethods.LockRelationOid(seqoid, (int)PgLockMode.RowExclusive);
                    }

                    SwitchInTransaction(seqoid, lo);
                }

                return;
            }
        }
    }

    /// <summary>
    /// Waits, with the statement's gates let go, for a sequence that has no block to draw from, as one that just reset,
    /// to switch to the one its range worker asks for, while the sequence is active, or quarantined until its blocks
    /// come, up to twice <c>worf.sequence_range_timeout</c>, as for the next block at a block's end.
    /// </summary>
    /// <returns>Whether the sequence has a block to draw from.</returns>
    private static bool WaitForBlock(uint seqoid)
    {
        SpiParameter seq = SpiParameter.Create(seqoid);
        bool Coming() => Elevated.Run(() => Worf.SelectCatalog(
            "SELECT EXISTS (SELECT 1 FROM worf.sequence_range WHERE seqoid = $1 AND state IN ('active', 'quarantined'))", seq)[0].Get<bool>(0));
        if (!Coming())
        {
            return false;
        }

        List<uint> held = LetGates();
        try
        {
            long deadline = Worker.GetCurrentTimestamp() + (SequenceRanges.ExchangeTimeout * 2000L);
            long checkedComing = Worker.GetCurrentTimestamp();
            while (true)
            {
                _ = PgAtomic.Add(SequenceRanges.CheckRequested, 1L);
                Worker.WakeRangeWorker();
                PgInterrupts.Check();
                Worker.WaitLatch(50);
                Reload();
                if (s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) &&
                    sequence.Blocks.Exists(block => block.State == "active"))
                {
                    Refresh();
                    ForgetBounds();
                    return true;
                }

                long now = Worker.GetCurrentTimestamp();
                if (now >= deadline)
                {
                    return false;
                }

                if (now - checkedComing >= 1_000_000)
                {
                    checkedComing = now;
                    if (!Coming())
                    {
                        return false;
                    }
                }
            }
        }
        finally
        {
            TakeGates(held);
        }
    }

    /// <summary>
    /// Finds where PL/pgSQL looks for its instrumentation plugin, and gives it one that instruments nothing when no other
    /// library has, so that PL/pgSQL tells the plugin the function its error context calls as each of its functions
    /// starts, which <see cref="InPlpgsql"/> finds a running function's state through.
    /// </summary>
    internal static unsafe void WatchPlpgsql()
    {
        fixed (byte* name = "PLpgSQL_plugin\0"u8)
        {
            s_plpgsqlPlugin = NativeMethods.find_rendezvous_variable((sbyte*)name);
        }

        if (*s_plpgsqlPlugin is null)
        {
            *s_plpgsqlPlugin = NativeMemory.AllocZeroed((nuint)sizeof(PLpgSQL_plugin));
        }
    }

    /// <summary>
    /// Notes a statement whose plan starts, so that a call of nextval among its expressions knows the sequences its
    /// statement names, until <see cref="StartedExpressions"/>.
    /// </summary>
    /// <param name="statement">The statement's plan.</param>
    /// <param name="explained">Whether the statement is only explained, as EXPLAIN without ANALYZE starts it, which
    /// runs none of the calls it starts but those its partition pruning makes, so that only the sequences those name
    /// are waited for.</param>
    internal static unsafe void StartingExpressions(nint statement, bool explained)
    {
        if (!explained)
        {
            Starting(() => Draws(PlanText((Node*)statement)));
            return;
        }

        if (PruningDraws((Node*)statement) is not (HashSet<uint> Called, int Calls, bool Unnamed) pruning)
        {
            Starting(static () => ([], 0, false), explained: true);
            return;
        }

        Starting(() => (pruning.Called, Math.Max(Draws(PlanText((Node*)statement)).Calls, pruning.Calls), pruning.Unnamed));
    }

    /// <summary>
    /// Notes how many calls of nextval a statement's plan started as it started, which its run doesn't start again,
    /// and forgets the statement's start.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    internal static void StartedPlan(nint queryDesc)
    {
        if (s_starting.Count > 0 && s_starting[^1].Looked > 0)
        {
            s_planStarted[queryDesc] = s_starting[^1].Looked;
        }

        StartedExpressions();
    }

    /// <summary>
    /// Notes a COPY FROM, whose defaults and condition start before its rows, the CHECK constraints of the domains of the
    /// columns it reads with the first, and the CHECK constraints and trigger conditions of the tables it writes with the
    /// first row each takes, so that a call of nextval among them knows the sequences they name, until
    /// <see cref="StartedExpressions"/>.
    /// </summary>
    /// <param name="relid">The table the COPY writes.</param>
    /// <param name="columns">The columns the COPY reads, by number, or null for all of them.</param>
    /// <param name="defaults">Whether the COPY's DEFAULT option has it start every column's default.</param>
    /// <param name="condition">The text of the COPY's WHERE condition, as <see cref="CopyConditionText"/> reads it.</param>
    internal static void StartingCopy(uint relid, short[]? columns, bool defaults, string condition = "")
        => Starting(() => Draws(CopyDefaultsText(relid, columns, defaults) + " " + condition + " " + WrittenChecks([relid])));

    /// <summary>
    /// Notes a COPY FROM that worf runs itself, as a table's sync does, as <see cref="StartingCopy(uint, short[], bool, string)"/>
    /// does a user's.
    /// </summary>
    /// <param name="relation">The table the COPY writes.</param>
    /// <param name="names">The names of the columns it reads, or null for all of them.</param>
    internal static unsafe void StartingCopy(RelationData* relation, List* names)
    {
        short[]? columns = null;
        if (names is not null)
        {
            List* numbers = NativeMethods.CopyGetAttnums(relation->rd_att, relation, names);
            columns = new short[numbers is null ? 0 : numbers->length];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = (short)numbers->elements[i].int_value;
            }
        }

        StartingCopy(relation->rd_id, columns, false);
    }

    /// <summary>
    /// Notes a row a worker writes, whose columns the row doesn't carry take their defaults, and which starts the
    /// table's CHECK constraints and trigger conditions, so that a call of nextval among those knows the sequences they
    /// name, until <see cref="StartedExpressions"/>.
    /// </summary>
    /// <param name="relid">The table.</param>
    /// <param name="sent">Whether the row carries each column, by its index.</param>
    internal static void StartingDefaults(uint relid, bool[] sent)
    {
        short[] columns = [.. Enumerable.Range(0, sent.Length).Where(i => sent[i]).Select(i => (short)(i + 1))];
        string key = string.Join(',', columns);
        Starting(() =>
        {
            WatchTables();
            if (!s_defaultsText.TryGetValue((relid, key), out string? text))
            {
                text = CopyDefaultsText(relid, columns, false) + " " + WrittenChecks([relid]);
                s_defaultsText[(relid, key)] = text;
            }

            return Draws(text);
        });
    }

    /// <summary>
    /// Notes a utility statement that starts expressions outside a plan, so that a call of nextval among them knows the
    /// sequences they name, until <see cref="StartedExpressions"/>: ALTER TABLE, which starts the defaults of the columns
    /// it adds and the CHECK constraints it checks rows against, ALTER DOMAIN, which starts the CHECK it checks the
    /// domain's values against, EXECUTE, alone or explained or creating a table, which starts its parameters, and CALL,
    /// which starts its arguments. An ALTER TABLE that changes a column's type, through an expression the catalogs don't
    /// hold, starts no expressions it names.
    /// </summary>
    /// <param name="parsetree">The statement.</param>
    /// <returns>Whether it noted the statement.</returns>
    internal static unsafe bool StartingUtility(Node* parsetree)
    {
        switch (parsetree->type)
        {
            case NodeTag.T_AlterTableStmt:
                AlterTableStmt* alter = (AlterTableStmt*)parsetree;
                for (int i = 0; alter->cmds is not null && i < alter->cmds->length; i++)
                {
                    if (((AlterTableCmd*)alter->cmds->elements[i].ptr_value)->subtype == AlterTableType.AT_AlterColumnType)
                    {
                        return false;
                    }
                }

                RangeVar* relation = alter->relation;
                Starting(() => Draws(AlterTableText(relation)));
                return true;

            case NodeTag.T_AlterDomainStmt when ChecksDomain((AlterDomainStmt*)parsetree):
                List* typeName = ((AlterDomainStmt*)parsetree)->typeName;
                Starting(() => Draws(DomainChecks(NativeMethods.typenameTypeId(null, NativeMethods.makeTypeNameFromNameList(typeName)), [])));
                return true;

            case NodeTag.T_CallStmt when ((CallStmt*)parsetree)->funcexpr is not null:
                FuncExpr* call = ((CallStmt*)parsetree)->funcexpr;
                Starting(() => CallDraws(call));
                return true;

            default:
                ExecuteStmt* execute = Executed(parsetree);
                if (execute is null || execute->@params is null || Worf.CatalogOid("sequence_range_block") == 0)
                {
                    return false;
                }

                // EXECUTE analyzes its parameters again as it runs, so they're analyzed here first, where a call one
                // starts, as a constant coerced to a domain whose CHECK draws, names nothing.
                string text = Unnamed(() => ExecuteText(execute));
                Starting(() => Draws(text));
                return true;
        }
    }

    /// <summary>
    /// Notes a statement as it runs or finishes, so that a call of nextval among the expressions it starts then knows
    /// the sequences they name, until <see cref="StartedExpressions"/>: each row of a multi-row VALUES list, which
    /// starts as it's read, the CHECK constraints and trigger conditions of each table it writes its first row to, the
    /// RETURNING list, WITH CHECK OPTION and ON CONFLICT expressions and MERGE actions of each partition that takes its
    /// first row, and the CHECKs that draw of the domains its values' types are or hold, which a value converted as it
    /// runs may start.
    /// </summary>
    /// <param name="queryDesc">The statement.</param>
    /// <returns>Whether it noted the statement.</returns>
    internal static unsafe bool StartingRun(QueryDesc* queryDesc)
    {
        PlannedStmt* statement = queryDesc->plannedstmt;
        if (statement is null)
        {
            return false;
        }

        int started = s_planStarted.GetValueOrDefault((nint)queryDesc);
        Starting(() => RunDraws(statement, started));
        return true;
    }

    /// <summary>
    /// Forgets the statement noted last, once its expressions have started.
    /// </summary>
    internal static void StartedExpressions() => s_starting.RemoveAt(s_starting.Count - 1);

    /// <summary>
    /// Reads the text of a COPY FROM's WHERE condition, analyzed as COPY analyzes it, under a statement whose calls of
    /// nextval name nothing, as a constant coerced to a domain whose CHECK draws starts one.
    /// </summary>
    /// <param name="relid">The table the COPY writes, which this backend has locked.</param>
    /// <param name="condition">The condition, unanalyzed, or null.</param>
    /// <returns>The text, or an empty one.</returns>
    internal static unsafe string CopyConditionText(uint relid, Node* condition)
    {
        if (condition is null || relid == 0 || Worf.CatalogOid("sequence_range_block") == 0)
        {
            return "";
        }

        return Unnamed(() =>
        {
            RelationData* relation = NativeMethods.relation_open(relid, (int)PgLockMode.AccessShare);
            ParseState* pstate = NativeMethods.make_parsestate(null);
            try
            {
                ParseNamespaceItem* item = NativeMethods.addRangeTableEntryForRelation(pstate, relation, (int)PgLockMode.AccessShare, null, false, false);
                NativeMethods.addNSItemToQuery(pstate, item, false, true, true);
                return WithDomainChecks(NodeText(NativeMethods.transformExpr(pstate, (Node*)NativeMethods.copyObjectImpl(condition), ParseExprKind.EXPR_KIND_COPY_WHERE)));
            }
            finally
            {
                NativeMethods.free_parsestate(pstate);
                NativeMethods.relation_close(relation, (int)PgLockMode.None);
            }
        });
    }

    // Notes expressions starting, by how their calls of nextval are read, with the error context as they start.
    private static unsafe void Starting(Func<(HashSet<uint> Called, int Calls, bool Unnamed)> draws, bool explained = false)
        => s_starting.Add(new(draws, (nint)NativeGlobals.error_context_stack, explained));

    // Runs worf's own reading of a statement's expressions, as parse analysis, under a statement whose calls of nextval,
    // as a domain's CHECK coercing a constant starts, name nothing.
    private static T Unnamed<T>(Func<T> read)
    {
        Starting(static () => ([], 0, true));
        try
        {
            return read();
        }
        finally
        {
            StartedExpressions();
        }
    }

    // Whether an ALTER DOMAIN checks the domain's values against a CHECK, as adding or validating one does.
    private static unsafe bool ChecksDomain(AlterDomainStmt* alter)
#if ANKUS_PG19
        => alter->subtype is AlterDomainType.AD_AddConstraint or AlterDomainType.AD_ValidateConstraint;
#else
        => alter->subtype is (sbyte)'C' or (sbyte)'V';
#endif

    // The text of the expressions an ALTER TABLE may start: the defaults of the table and its inheritors, which hold
    // those of the columns it adds, their CHECK constraints, which hold those it adds, and the CHECKs of the domains of
    // their columns that have no default, as one it adds coerces a null to.
    private static unsafe string AlterTableText(RangeVar* relation)
    {
        uint relid = NativeMethods.RangeVarGetRelidExtended(relation, 0, RangeVarMissingOk, default, null);
        if (relid == 0)
        {
            return "";
        }

        WorfRows started = Elevated.Run(() => Worf.Read("""
            WITH RECURSIVE tree (relid) AS (
                SELECT $1::pg_catalog.oid
                UNION
                SELECT i.inhrelid FROM pg_catalog.pg_inherits i JOIN tree t ON i.inhparent = t.relid)
            SELECT d.adbin::pg_catalog.text FROM pg_catalog.pg_attrdef d JOIN tree t ON d.adrelid = t.relid
            UNION ALL
            SELECT c.conbin::pg_catalog.text FROM pg_catalog.pg_constraint c JOIN tree t ON c.conrelid = t.relid WHERE c.contype = 'c'
            UNION ALL
            SELECT pg_catalog.format('{COERCETODOMAIN :resulttype %s }', a.atttypid)
            FROM pg_catalog.pg_attribute a JOIN tree t ON a.attrelid = t.relid JOIN pg_catalog.pg_type y ON y.oid = a.atttypid
            WHERE a.attnum > 0 AND NOT a.attisdropped AND NOT a.atthasdef AND y.typtype = 'd'
            """, 0, SpiParameter.Create(relid)));
        return WithDomainChecks(string.Concat(started.Select(row => row.Get<string>(0) + " ")));
    }

    // The calls of nextval a CALL starts: its arguments', and, for one that leaves a parameter to its default, the
    // defaults', which name none when they draw, as the arguments may leave any of them out.
    private static unsafe (HashSet<uint> Called, int Calls, bool Unnamed) CallDraws(FuncExpr* call)
    {
        string text = NodeText((Node*)call);
        (HashSet<uint> called, int calls, bool unnamed) = Draws(WithDomainChecks(text));
        int given = call->args is null ? 0 : call->args->length;
        WorfRows procedure = Elevated.Run(() => Worf.Read(
            "SELECT p.pronargs::pg_catalog.int4, coalesce(p.proargdefaults::pg_catalog.text, '') FROM pg_catalog.pg_proc p WHERE p.oid = $1",
            1, SpiParameter.Create(call->funcid)));
        if (procedure.Count == 1 && (given < procedure[0].Get<int>(0) || text.Contains("{NAMEDARGEXPR ", StringComparison.Ordinal)) &&
            procedure[0].Get<string>(1).Contains("{FUNCEXPR :funcid 1574 ", StringComparison.Ordinal))
        {
            unnamed = true;
        }

        return (called, calls, unnamed);
    }

    // The EXECUTE a statement runs: itself, or the one it explains or creates a table from.
    private static unsafe ExecuteStmt* Executed(Node* parsetree)
    {
        Node* query = parsetree->type switch
        {
            NodeTag.T_ExplainStmt => ((ExplainStmt*)parsetree)->query,
            NodeTag.T_CreateTableAsStmt => ((CreateTableAsStmt*)parsetree)->query,
            _ => parsetree,
        };
        if (query is not null && query->type == NodeTag.T_Query)
        {
            query = ((Query*)query)->utilityStmt;
        }

        return query is not null && query->type == NodeTag.T_ExecuteStmt ? (ExecuteStmt*)query : null;
    }

    // The text of an EXECUTE's parameters, analyzed as EXECUTE analyzes them, and of the CHECKs of the domains they're
    // coerced to.
    private static unsafe string ExecuteText(ExecuteStmt* execute)
    {
        StringBuilder text = new();
        PreparedStatement* prepared = NativeMethods.FetchPreparedStatement(execute->name, false);
        ParseState* pstate = NativeMethods.make_parsestate(null);
        try
        {
            for (int i = 0; i < execute->@params->length; i++)
            {
                Node* parameter = (Node*)NativeMethods.copyObjectImpl(execute->@params->elements[i].ptr_value);
                _ = text.Append(NodeText(NativeMethods.transformExpr(pstate, parameter, ParseExprKind.EXPR_KIND_EXECUTE_PARAMETER))).Append(' ');
                if (prepared is not null && i < prepared->plansource->num_params && NativeMethods.get_typtype(prepared->plansource->param_types[i]) == (sbyte)'d')
                {
                    _ = text.Append("{COERCETODOMAIN :resulttype ").Append(prepared->plansource->param_types[i].ToString(CultureInfo.InvariantCulture)).Append(" } ");
                }
            }
        }
        finally
        {
            NativeMethods.free_parsestate(pstate);
        }

        return WithDomainChecks(text.ToString());
    }

    // The types an expression's text names, in the fields nodeToString writes a type's OID in: an expression's result,
    // a Var's, Const's or Param's type, and the types an array, row, CASE, COALESCE, subscript or subplan builds,
    // rather than the fields whose names end in "type" that hold an enumeration's value, as a join's type.
    private static HashSet<uint> NamedTypes(string text)
    {
        HashSet<uint> types = [];
        for (int at = text.IndexOf(" :", StringComparison.Ordinal); at >= 0; at = text.IndexOf(" :", at + 2, StringComparison.Ordinal))
        {
            int name = at + 2;
            int end = name;
            while (end < text.Length && (char.IsAsciiLetterOrDigit(text[end]) || text[end] == '_'))
            {
                end++;
            }

            int digits = end + 1;
            int last = digits;
            while (last < text.Length && char.IsAsciiDigit(text[last]))
            {
                last++;
            }

            if (end < text.Length && text[end] == ' ' && last > digits && s_typeFields.Contains(text[name..end]) &&
                uint.TryParse(text.AsSpan(digits, last - digits), NumberStyles.None, CultureInfo.InvariantCulture, out uint type) && type != 0)
            {
                _ = types.Add(type);
            }
        }

        return types;
    }

    // The ModifyTable nodes of a plan, at its top or as its subplans, as a data-modifying WITH's, with the tables each
    // writes and the partitioned table it routes rows into, if it does.
    private static unsafe List<(nint Modify, List<long> Written, uint Root)> Writing(PlannedStmt* statement)
    {
        List<(nint Modify, List<long> Written, uint Root)> writing = [];
        for (int i = -1; i < (statement->subplans is null ? 0 : statement->subplans->length); i++)
        {
            Plan* plan = i < 0 ? statement->planTree : (Plan*)statement->subplans->elements[i].ptr_value;
            if (plan is null || plan->type != NodeTag.T_ModifyTable)
            {
                continue;
            }

            ModifyTable* modify = (ModifyTable*)plan;
            List<long> written = [];
            for (int j = -1; j < (modify->resultRelations is null ? 0 : modify->resultRelations->length); j++)
            {
                int index = j < 0 ? (int)modify->rootRelation : modify->resultRelations->elements[j].int_value;
                if (index > 0 && statement->rtable is not null && index <= statement->rtable->length)
                {
                    written.Add(((RangeTblEntry*)statement->rtable->elements[index - 1].ptr_value)->relid);
                }
            }

            uint root = written.Count > 0 ? (uint)written[0] : 0;
            writing.Add(((nint)modify, written, root != 0 && NativeMethods.get_rel_relkind(root) == (sbyte)'p' ? root : 0));
        }

        return writing;
    }

    // The calls of nextval a statement starts as it runs: those of its plan its start didn't make, as each row of a
    // multi-row VALUES list starts its own, those of the CHECK constraints and trigger conditions of the tables it
    // writes and their partitions, those its ModifyTable nodes start for each leaf partition of the tables they route
    // rows into, and those of the CHECKs that draw of the domains the types its plan names are or hold. A call past
    // them, as one C code starts, names none.
    private static unsafe (HashSet<uint> Called, int Calls, bool Unnamed) RunDraws(PlannedStmt* statement, int started)
    {
        string text = PlanText((Node*)statement);
        (HashSet<uint> called, int calls, bool unnamed) = Draws(text);
        calls = Math.Max(calls - started, 0);
        List<(nint Modify, List<long> Written, uint Root)> writing = Writing(statement);
        List<(HashSet<uint> Called, int Calls, bool Unnamed)> parts = [Draws(WrittenChecks([.. writing.SelectMany(write => write.Written)]))];
        foreach ((nint node, _, uint root) in writing.Where(write => write.Root != 0))
        {
            ModifyTable* modify = (ModifyTable*)node;
            (HashSet<uint> Called, int Calls, bool Unnamed) routed = Draws(WithDomainChecks(string.Join(' ', NodeText((Node*)modify->returningLists),
                NodeText((Node*)modify->withCheckOptionLists), NodeText((Node*)modify->onConflictSet), NodeText(modify->onConflictWhere),
                NodeText((Node*)modify->mergeActionLists))));
            if (routed.Calls > 0)
            {
                int leaves = Elevated.Run(() => Worf.ReadScalar<int>(
                    "SELECT pg_catalog.count(*)::pg_catalog.int4 FROM pg_catalog.pg_partition_tree($1) p WHERE p.isleaf", SpiParameter.Create(root)));
                parts.Add((routed.Called, routed.Calls * Math.Max(leaves, 1), routed.Unnamed));
            }
        }

        long[] types = [.. NamedTypes(text).Select(type => (long)type)];
        if (types.Length > 0)
        {
            string drawing = string.Concat(Elevated.Run(() => Worf.Read("""
                WITH RECURSIVE r (typ) AS (
                    SELECT pg_catalog.unnest($1::pg_catalog.int8[])::pg_catalog.oid
                    UNION
                    SELECT n.typ FROM r JOIN pg_catalog.pg_type t ON t.oid = r.typ
                    CROSS JOIN LATERAL (
                        SELECT t.typbasetype WHERE t.typtype = 'd'
                        UNION ALL
                        SELECT t.typelem WHERE t.typelem <> 0 AND t.typsubscript = 'pg_catalog.array_subscript_handler'::pg_catalog.regproc
                        UNION ALL
                        SELECT f.atttypid FROM pg_catalog.pg_attribute f
                        WHERE t.typtype = 'c' AND f.attrelid = t.typrelid AND f.attnum > 0 AND NOT f.attisdropped
                        UNION ALL
                        SELECT g.rngsubtype FROM pg_catalog.pg_range g WHERE t.typtype = 'r' AND g.rngtypid = t.oid
                        UNION ALL
                        SELECT g.rngtypid FROM pg_catalog.pg_range g WHERE t.typtype = 'm' AND g.rngmultitypid = t.oid) n (typ))
                SELECT c.conbin::pg_catalog.text FROM r JOIN pg_catalog.pg_constraint c ON c.contypid = r.typ
                WHERE r.typ <> 0 AND c.contype = 'c' AND pg_catalog.strpos(c.conbin::pg_catalog.text, '{FUNCEXPR :funcid 1574 ') > 0
                """, 0, SpiParameter.Create(types))).Select(row => row.Get<string>(0) + " "));
            parts.Add(Draws(WithDomainChecks(drawing)));
        }

        foreach ((HashSet<uint> Called, int Calls, bool Unnamed) part in parts)
        {
            called.UnionWith(part.Called);
            calls += part.Calls;
            unnamed |= part.Unnamed;
        }

        return (called, calls, unnamed);
    }

    // The text of the CHECK constraints and trigger conditions of tables and their partitions, which start as each
    // table takes its first row, with those of the domains they coerce to.
    private static string WrittenChecks(long[] relids)
    {
        if (relids.Length == 0)
        {
            return "";
        }

        return WithDomainChecks(Elevated.Run(() => Worf.ReadScalar<string>("""
            WITH t (relid) AS (
                SELECT r.relid::pg_catalog.oid FROM pg_catalog.unnest($1::pg_catalog.int8[]) r (relid)
                UNION ALL
                SELECT p.relid FROM pg_catalog.unnest($1::pg_catalog.int8[]) r (relid)
                    CROSS JOIN LATERAL pg_catalog.pg_partition_tree(r.relid::pg_catalog.oid) p WHERE p.level > 0)
            SELECT coalesce(pg_catalog.string_agg(x.expr, ' '), '')
            FROM (
                SELECT c.conbin::pg_catalog.text FROM t JOIN pg_catalog.pg_constraint c ON c.conrelid = t.relid WHERE c.contype = 'c'
                UNION ALL
                SELECT g.tgqual::pg_catalog.text FROM t JOIN pg_catalog.pg_trigger g ON g.tgrelid = t.relid WHERE g.tgqual IS NOT NULL) x (expr)
            """, SpiParameter.Create(relids))) ?? "");
    }

    // The calls of nextval a plan's partition pruning, which its start runs even when only explained, may make, as a
    // CHECK of a domain it coerces a value to does, or null for none.
    private static unsafe (HashSet<uint> Called, int Calls, bool Unnamed)? PruningDraws(Node* plan)
    {
        if (Worf.CatalogOid("sequence_range_block") == 0)
        {
            return null;
        }

        const string Pruning = "{PARTITIONPRUNEINFO ";
        string text = NodeText(plan);
        StringBuilder pruning = new();
        for (int at = text.IndexOf(Pruning, StringComparison.Ordinal); at >= 0; at = text.IndexOf(Pruning, at + Pruning.Length, StringComparison.Ordinal))
        {
            _ = pruning.Append(NodeAt(text, at)).Append(' ');
        }

        if (pruning.Length == 0)
        {
            return null;
        }

        (HashSet<uint> Called, int Calls, bool Unnamed) draws = Draws(WithDomainChecks(pruning.ToString()));
        return draws.Calls > 0 || draws.Unnamed ? draws : null;
    }

    // The text of the node that starts at a position of an expression's text, to its closing brace.
    private static string NodeAt(string text, int at)
    {
        int depth = 0;
        for (int i = at; i < text.Length; i++)
        {
            char c = text[i];
            if (c == '\\')
            {
                i++;
            }
            else if (c is '{' or '(')
            {
                depth++;
            }
            else if (c is '}' or ')' && --depth == 0)
            {
                return text[at..(i + 1)];
            }
        }

        return text[at..];
    }

    // Registers, once, for the changes to tables, types and constraints after which the text the guard holds by table,
    // and the domains' draws, are read again.
    private static void WatchTables()
    {
        if (s_watchingTables)
        {
            return;
        }

        unsafe
        {
            NativeMethods.CacheRegisterRelcacheCallback(TableCallback, 0);
#if ANKUS_PG19
            NativeMethods.CacheRegisterSyscacheCallback(SysCacheIdentifier.TYPEOID, TypeCallback, 0);
            NativeMethods.CacheRegisterSyscacheCallback(SysCacheIdentifier.CONSTROID, TypeCallback, 0);
#else
            NativeMethods.CacheRegisterSyscacheCallback(TypeCacheId, TypeCallback, 0);
            NativeMethods.CacheRegisterSyscacheCallback(ConstraintCacheId, TypeCallback, 0);
#endif
        }

        s_watchingTables = true;
    }

    // Forgets what the guard holds for a changed table, or for every table, as a partition attached, which takes on its
    // parent's triggers and identity columns, or a default set.
    private static void TableInvalidated(ulong arg, uint relid)
    {
        _ = arg;
        if (relid == 0)
        {
            s_drawnBy.Clear();
            s_drawingColumns.Clear();
            s_defaultsText.Clear();
            s_guarded.Clear();
            return;
        }

        _ = s_guarded.Remove(relid);
        _ = s_drawnBy.Remove(relid);
        _ = s_drawingColumns.Remove(relid);
        foreach ((uint Relid, string Sent) key in s_defaultsText.Keys.Where(key => key.Relid == relid).ToList())
        {
            _ = s_defaultsText.Remove(key);
        }
    }

    // Forgets the defaults' text, which holds the CHECKs of their domains, and the domains' draws, once a type or a
    // constraint changes, as a domain's CHECK.
#if ANKUS_PG19
    private static void TypeInvalidated(ulong arg, SysCacheIdentifier cacheid, uint hashvalue)
#else
    private static void TypeInvalidated(ulong arg, int cacheid, uint hashvalue)
#endif
    {
        _ = arg;
        _ = cacheid;
        _ = hashvalue;
        s_defaultsText.Clear();
        s_domainDraws = null;
    }

    // The text of a plan, as nodeToString writes it, and of the CHECK constraints of the domains it coerces to, which
    // start with it from the type cache.
    private static unsafe string PlanText(Node* plan) => WithDomainChecks(NodeText(plan));

    // A node's text, as nodeToString writes it.
    private static unsafe string NodeText(Node* node)
    {
        sbyte* written = NativeMethods.nodeToString(node);
        try
        {
            return Worf.FromServer(written);
        }
        finally
        {
            NativeMethods.pfree(written);
        }
    }

    // The text of the defaults a COPY FROM starts, of the columns it doesn't read, and of the CHECK constraints of the
    // domains they're coerced to, and of those of the domains the values of the columns it reads are, or hold as array
    // elements, fields or range bounds, whose input functions start them as the first row comes.
    private static string CopyDefaultsText(uint relid, short[]? columns, bool defaults)
    {
        WorfRows started = Elevated.Run(() => Worf.Read("""
            WITH RECURSIVE r (typ, base) AS (
                SELECT a.atttypid, false FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
                    AND ($3 IS NULL OR a.attnum = ANY ($3::pg_catalog.int2[]))
                UNION ALL
                SELECT n.typ, n.base FROM r JOIN pg_catalog.pg_type t ON t.oid = r.typ
                CROSS JOIN LATERAL (
                    SELECT t.typbasetype, true WHERE t.typtype = 'd'
                    UNION ALL
                    SELECT t.typelem, false WHERE t.typtype <> 'd' AND t.typelem <> 0 AND t.typsubscript = 'pg_catalog.array_subscript_handler'::pg_catalog.regproc
                    UNION ALL
                    SELECT f.atttypid, false FROM pg_catalog.pg_attribute f
                    WHERE t.typtype = 'c' AND f.attrelid = t.typrelid AND f.attnum > 0 AND NOT f.attisdropped
                    UNION ALL
                    SELECT g.rngsubtype, false FROM pg_catalog.pg_range g WHERE t.typtype = 'r' AND g.rngtypid = t.oid
                    UNION ALL
                    SELECT g.rngtypid, false FROM pg_catalog.pg_range g WHERE t.typtype = 'm' AND g.rngmultitypid = t.oid) n (typ, base))
            SELECT coalesce(d.adbin::pg_catalog.text, t.typdefaultbin::pg_catalog.text, ''),
                CASE WHEN d.adbin IS NULL AND t.typtype = 'd' THEN pg_catalog.format('{COERCETODOMAIN :resulttype %s }', t.oid) ELSE '' END
            FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
                LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE ($2 OR $3 IS NOT NULL) AND a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
                AND ($2 OR NOT (a.attnum = ANY ($3::pg_catalog.int2[])))
                AND (d.adbin IS NOT NULL OR t.typdefaultbin IS NOT NULL)
            UNION ALL
            SELECT pg_catalog.format('{COERCETODOMAIN :resulttype %s }', r.typ), ''
            FROM r JOIN pg_catalog.pg_type t ON t.oid = r.typ WHERE t.typtype = 'd' AND NOT r.base
            """, 0, SpiParameter.Create(relid), SpiParameter.Create(defaults), SpiParameter.Create(columns)));
        return WithDomainChecks(string.Concat(started.Select(row => row.Get<string>(0) + " " + row.Get<string>(1) + " ")));
    }

    // Adds to an expression's text the text of the CHECK constraints of each domain it coerces to, and of those
    // domains' base domains, once for each coercion, as each starts its own, and of the domains those coerce to.
    private static string WithDomainChecks(string text)
    {
        StringBuilder all = new(text);
        StringBuilder found = new();
        Dictionary<uint, string> read = [];
        string pending = text;
        for (int depth = 0; depth < 8 && pending.Length > 0; depth++)
        {
            List<uint> domains = CoercedDomains(pending);
            _ = found.Clear();
            foreach (string checks in domains.Select(domain => DomainChecks(domain, read)))
            {
                _ = all.Append(' ').Append(checks);
                _ = found.Append(checks);
            }

            pending = found.ToString();
        }

        return all.ToString();
    }

    // The text of a domain's CHECK constraints, and of its base domains', read once for the expressions being read.
    private static string DomainChecks(uint domain, Dictionary<uint, string> read)
    {
        if (!read.TryGetValue(domain, out string? checks))
        {
            checks = string.Concat(Elevated.Run(() => Worf.Read("""
                WITH RECURSIVE d (oid) AS (
                    SELECT $1::pg_catalog.oid
                    UNION
                    SELECT t.typbasetype FROM pg_catalog.pg_type t JOIN d ON t.oid = d.oid WHERE t.typtype = 'd')
                SELECT c.conbin::pg_catalog.text FROM pg_catalog.pg_constraint c JOIN d ON c.contypid = d.oid WHERE c.contype = 'c'
                """, 0, SpiParameter.Create(domain))).Select(row => row.Get<string>(0) + " "));
            read[domain] = checks;
        }

        return checks;
    }

    // The domains an expression's text coerces to, once for each coercion: each CoerceToDomain's own result type, at
    // its node's top level rather than its argument's.
    private static List<uint> CoercedDomains(string text)
    {
        const string Coercion = "{COERCETODOMAIN ";
        List<uint> domains = [];
        for (int at = text.IndexOf(Coercion, StringComparison.Ordinal); at >= 0; at = text.IndexOf(Coercion, at + Coercion.Length, StringComparison.Ordinal))
        {
            int depth = 0;
            for (int i = at; i < text.Length; i++)
            {
                char c = text[i];
                if (c == '\\')
                {
                    i++;
                }
                else if (c is '{' or '(')
                {
                    depth++;
                }
                else if (c is '}' or ')')
                {
                    if (--depth == 0)
                    {
                        break;
                    }
                }
                else if (depth == 1 && string.CompareOrdinal(text, i, " :resulttype ", 0, 13) == 0)
                {
                    int start = i + 13;
                    int end = start;
                    while (end < text.Length && char.IsAsciiDigit(text[end]))
                    {
                        end++;
                    }

                    if (uint.TryParse(text.AsSpan(start, end - start), NumberStyles.None, CultureInfo.InvariantCulture, out uint domain))
                    {
                        domains.Add(domain);
                    }

                    break;
                }
            }
        }

        return domains;
    }

    /// <summary>
    /// Waits, as a call of nextval is about to run, until this server has heard from the grantor of the managed
    /// sequence's active block since it started, as <see cref="WaitForGrantor"/> does for the statements that hold the
    /// gates, so that nothing draws from that block meanwhile: the sequence the expressions starting around it name, or,
    /// for a call whose sequence they don't name, as one computed as it runs, every managed sequence's. Those are a
    /// statement's plan, or, for a PL/pgSQL function running within it, the expression PL/pgSQL starts outside a plan,
    /// or the expressions a COPY, a utility statement, a statement as it runs or a worker's row starts, as
    /// <see cref="StartingCopy(uint, short[], bool, string)"/>, <see cref="StartingUtility"/>,
    /// <see cref="StartingRun"/> and <see cref="StartingDefaults"/> note them. A call past the calls they name, as one
    /// started by code they don't hold, is one they don't name. worf's own statements, a standby, a read-only
    /// transaction, a statement only explained whose start prunes no partitions through a call, and Snowflake node
    /// numbers, whose IDs carry their times, don't wait; a statement that a worf worker runs for a user, as DDL it
    /// applies, does.
    /// </summary>
    internal static void BeforeNextval()
    {
        // worf's own reading of what a statement names starts no call of the statement's.
        if (s_namingGrantors)
        {
            return;
        }

        // Each call that starts counts against the calls the expressions starting around it name.
        bool plpgsql;
        nint simple;
        WorfStartingStatement? starting = null;
        unsafe
        {
            plpgsql = InPlpgsql(out simple);
            if (plpgsql && simple != 0)
            {
                uint lxid = ((PLpgSQL_expr*)simple)->expr_simple_lxid;
                if (s_simple.Expr != simple || s_simple.Lxid != lxid)
                {
                    s_simple = (simple, lxid, null, 0);
                }

                s_simple.Looked++;
            }
        }

        if (!plpgsql && s_starting.Count > 0)
        {
            starting = s_starting[^1];
            starting.Looked++;
        }

        bool quiet;
        unsafe
        {
            quiet = NativeGlobals.creating_extension || NativeGlobals.XactReadOnly;
        }

        if (quiet || starting is { Explained: true } || SequenceRanges.Recovering() || Worf.CatalogOid("sequence_range_block") == 0)
        {
            return;
        }

        s_namingGrantors = true;
        List<uint> waiting = [];
        bool unnamed;
        try
        {
            Reload();
            uint snowflake = Worf.CatalogOid("snowflake_node");
            List<uint> unconfirmed = [.. s_grantors.Where(entry => entry.Key != snowflake && !s_confirmed.Contains(entry.Value)).Select(entry => entry.Key)];
            if (unconfirmed.Count == 0 || Worf.InOwnSql)
            {
                return;
            }

            // C code starts a call of its own as it checks a value it was given against a domain, as a function that
            // reads a record or tests an input does, and such a call can come where a call the expressions name was
            // counted, which starts later or never, so a call they name waits for the sequences the domains' CHECKs draw
            // from too, after its own, or for every node when one of them doesn't name its sequence.
            HashSet<uint>? called = plpgsql ? SimpleDraws(simple) : StartingDraws(starting);
            (HashSet<uint> Called, bool Unnamed) domains = called is null ? ([], false) : DomainDraws();
            unnamed = called is null || domains.Unnamed;
            waiting = unnamed
                ? [.. unconfirmed.DistinctBy(seqoid => s_grantors[seqoid])]
                : [.. unconfirmed.Where(called!.Contains), .. unconfirmed.Where(seqoid => domains.Called.Contains(seqoid) && !called!.Contains(seqoid))];
        }
        finally
        {
            s_namingGrantors = false;
        }

        foreach (uint seqoid in waiting)
        {
            WaitForGrantor(seqoid, unnamed);
        }
    }

    /// <summary>
    /// Finds the sequences the CHECK constraints of this database's domains draw from, which C code starts as it checks a
    /// value against a domain, once until a type changes, and whether one of them draws from a sequence it doesn't name.
    /// </summary>
    private static (HashSet<uint> Called, bool Unnamed) DomainDraws()
    {
        WatchTables();
        if (s_domainDraws is not (HashSet<uint>, bool) draws)
        {
            string checks = Elevated.Run(() => Worf.ReadScalar<string?>("""
                SELECT pg_catalog.string_agg(c.conbin::pg_catalog.text, ' ') FROM pg_catalog.pg_constraint c
                WHERE c.contypid <> 0 AND c.contype = 'c' AND pg_catalog.strpos(c.conbin::pg_catalog.text, '{FUNCEXPR :funcid 1574 ') > 0
                """)) ?? "";
            (HashSet<uint> called, _, bool unnamed) = Draws(checks);
            draws = (called, unnamed);
            s_domainDraws = draws;
        }

        return draws;
    }

    /// <summary>
    /// Finds the sequences the nextval calls of the statement whose expressions start name, once for the statement, or
    /// null outside one, or for a call they don't name.
    /// </summary>
    private static HashSet<uint>? StartingDraws(WorfStartingStatement? starting)
        => starting is null || starting.Draws.Unnamed || starting.Looked > starting.Draws.Calls ? null : starting.Draws.Called;

    /// <summary>
    /// Finds the sequences the nextval calls of the PL/pgSQL expression starting name, once for the expression as it
    /// starts, or null for one PL/pgSQL doesn't start itself, or a call they don't name.
    /// </summary>
    private static unsafe HashSet<uint>? SimpleDraws(nint simple)
    {
        if (simple == 0)
        {
            return null;
        }

        s_simple.Draws ??= Draws(PlanText((Node*)((PLpgSQL_expr*)simple)->expr_simple_expr));
        return s_simple.Draws.Value.Unnamed || s_simple.Looked > s_simple.Draws.Value.Calls ? null : s_simple.Draws.Value.Called;
    }

    /// <summary>
    /// Checks whether a PL/pgSQL function runs within the statement whose expressions started last, or with none
    /// starting, as PostgreSQL's error context shows: PL/pgSQL calls the function it tells its instrumentation plugin of
    /// for each of its functions running, with the function's state. When PL/pgSQL is itself starting an expression of
    /// the function's, outside a plan, as it does a simple expression's once a transaction, it has set the function's
    /// parameters up for the expression, which isn't running yet.
    /// </summary>
    /// <param name="simple">The expression PL/pgSQL is starting, or zero.</param>
    /// <returns>Whether one runs.</returns>
    private static unsafe bool InPlpgsql(out nint simple)
    {
        simple = 0;
        PLpgSQL_plugin* plugin = s_plpgsqlPlugin is null ? null : (PLpgSQL_plugin*)*s_plpgsqlPlugin;
        if (plugin is null || plugin->error_callback.IsNull)
        {
            return false;
        }

        void* callback = plugin->error_callback.DangerousGetAddress();
        nint outer = s_starting.Count > 0 ? s_starting[^1].Context : 0;
        for (ErrorContextCallback* context = NativeGlobals.error_context_stack; context is not null && (nint)context != outer; context = context->previous)
        {
            if (context->callback.DangerousGetAddress() != callback)
            {
                continue;
            }

            ParamListInfoData* parameters = ((PLpgSQL_execstate*)context->arg)->paramLI;
            PLpgSQL_expr* expr = parameters is null ? null : (PLpgSQL_expr*)parameters->parserSetupArg;
            if (expr is not null && expr->expr_simple_expr is not null && !expr->expr_simple_in_use)
            {
                simple = (nint)expr;
            }

            return true;
        }

        return false;
    }

    /// <summary>
    /// Reads, from expressions' text, as nodeToString writes them, the sequences their calls of nextval name, as each
    /// call's argument, a constant, names one, and how many calls there are. A constant name given as text, which the
    /// call looks up as it runs, under the search path it runs with, names every managed sequence of that name in any
    /// schema, or the schema it gives. A call whose argument isn't a constant, as a name computed as it runs, makes the
    /// sequences they may draw from unnamed.
    /// </summary>
    private static (HashSet<uint> Called, int Calls, bool Unnamed) Draws(string text)
    {
        const string Call = "{FUNCEXPR :funcid 1574 ";
        const string Named = "{CONST :consttype 2205 ";
        const string Looked = "{FUNCEXPR :funcid 1079 ";
        const string Name = "{CONST :consttype 25 ";
        HashSet<uint> called = [];
        bool unnamed = false;
        int calls = 0;
        for (int at = text.IndexOf(Call, StringComparison.Ordinal); at >= 0; at = text.IndexOf(Call, at + Call.Length, StringComparison.Ordinal))
        {
            calls++;
            int args = Argument(text, at);
            if (args >= 0 && string.CompareOrdinal(text, args, Named, 0, Named.Length) == 0 && ConstBytes(text, args) is { Length: >= 4 } bytes)
            {
                _ = called.Add((uint)(bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24)));
            }
            else if (args >= 0 && string.CompareOrdinal(text, args, Looked, 0, Looked.Length) == 0 && Argument(text, args) is int inner and >= 0 &&
                string.CompareOrdinal(text, inner, Name, 0, Name.Length) == 0 && TextConst(ConstBytes(text, inner)) is string name &&
                ManagedNamed(name) is HashSet<uint> managed)
            {
                called.UnionWith(managed);
            }
            else
            {
                unnamed = true;
            }
        }

        return (called, calls, unnamed);
    }

    // Where the first argument of the node at a position of an expression's text starts, or -1.
    private static int Argument(string text, int node)
    {
        int args = text.IndexOf(":args (", node, StringComparison.Ordinal);
        if (args < 0)
        {
            return -1;
        }

        int first = args + ":args (".Length;
        while (first < text.Length && text[first] == ' ')
        {
            first++;
        }

        return first;
    }

    // The bytes of the value of the constant at a position of an expression's text, or null for a null one.
    private static byte[]? ConstBytes(string text, int node)
    {
        int value = text.IndexOf(":constvalue ", node, StringComparison.Ordinal);
        int open = value < 0 ? -1 : text.IndexOf('[', value);
        int close = open < 0 ? -1 : text.IndexOf(']', open);
        int next = text.IndexOf('{', node + 1);
        if (close < 0 || (next >= 0 && next < open))
        {
            return null;
        }

        string[] bytes = text[(open + 1)..close].Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return [.. bytes.Select(b => (byte)(int.Parse(b, CultureInfo.InvariantCulture) & 0xFF))];
    }

    // The characters of a text constant's bytes, as its varlena holds them, or null for one this can't tell, as one
    // compressed or out of line, or whose characters aren't ASCII, which the database's encoding may give otherwise.
    private static string? TextConst(byte[]? bytes)
    {
        if (bytes is null || bytes.Length < 1)
        {
            return null;
        }

        (int header, int length) = (bytes[0] & 0x01) == 0x01
            ? (1, bytes[0] == 0x01 ? -1 : (bytes[0] >> 1) & 0x7F)
            : (bytes[0] & 0x03) == 0 && bytes.Length >= 4 ? (4, (int)((uint)(bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24)) >> 2)) : (0, -1);
        if (length < header || length > bytes.Length || bytes.AsSpan(header, length - header).ContainsAnyInRange((byte)0x80, (byte)0xFF))
        {
            return null;
        }

        return Encoding.ASCII.GetString(bytes, header, length - header);
    }

    // The managed sequences a name given as text may look up, in whatever schema the search path finds it, or in the
    // schema it gives, compared without case, or null for a name nextval can't look up.
    private static HashSet<uint>? ManagedNamed(string name)
    {
        List<string>? parts = NameParts(name);
        if (parts is null || parts.Count is 0 or > 3)
        {
            return null;
        }

        string relname = parts[^1];
        string? schema = parts.Count > 1 ? parts[^2] : null;
        HashSet<uint> managed = [];
        foreach (uint seqoid in s_sequences.Keys)
        {
            string? found;
            string? foundSchema;
            unsafe
            {
                sbyte* rel = NativeMethods.get_rel_name(seqoid);
                found = rel is null ? null : Worf.FromServer(rel);
                sbyte* nsp = rel is null ? null : NativeMethods.get_namespace_name(NativeMethods.get_rel_namespace(seqoid));
                foundSchema = nsp is null ? null : Worf.FromServer(nsp);
            }

            if (string.Equals(found, relname, StringComparison.OrdinalIgnoreCase) &&
                (schema is null || string.Equals(foundSchema, schema, StringComparison.OrdinalIgnoreCase)))
            {
                _ = managed.Add(seqoid);
            }
        }

        return managed;
    }

    // The parts of a qualified name, as nextval splits a name given as text: by dots outside double quotes, an unquoted
    // part folded to lower case and cut to the length of a name, or null for a name it can't split.
    private static List<string>? NameParts(string name)
    {
        const int NameLength = 63;
        List<string> parts = [];
        StringBuilder part = new();
        int i = 0;
        while (true)
        {
            while (i < name.Length && char.IsWhiteSpace(name[i]))
            {
                i++;
            }

            _ = part.Clear();
            if (i < name.Length && name[i] == '"')
            {
                for (i++; ; i++)
                {
                    if (i >= name.Length)
                    {
                        return null;
                    }

                    if (name[i] == '"')
                    {
                        if (i + 1 < name.Length && name[i + 1] == '"')
                        {
                            _ = part.Append('"');
                            i++;
                            continue;
                        }

                        i++;
                        break;
                    }

                    _ = part.Append(name[i]);
                }
            }
            else
            {
                for (; i < name.Length && name[i] != '.' && !char.IsWhiteSpace(name[i]); i++)
                {
                    _ = part.Append(char.ToLowerInvariant(name[i]));
                }

                if (part.Length == 0)
                {
                    return null;
                }
            }

            parts.Add(part.Length > NameLength ? part.ToString(0, NameLength) : part.ToString());
            while (i < name.Length && char.IsWhiteSpace(name[i]))
            {
                i++;
            }

            if (i == name.Length)
            {
                return parts;
            }

            if (name[i] != '.')
            {
                return null;
            }

            i++;
        }
    }

    /// <summary>
    /// Waits, before a statement draws from a block the sequence's parent granted this node, until this server has
    /// taken in a message of the parent's since it started: a node that was away while its parent's fence lifted without
    /// its report, as when <c>worf.activate_sequence_range</c> stopped waiting for it, holds values the lift gave out
    /// again, and the parent's first message after asks it to reset, so it draws none of them. The statement lets its
    /// gates go and waits up to twice <c>worf.sequence_range_timeout</c>, as for the next block at a block's end, and
    /// then fails, naming the parent, as an error that ends the wait first, as <c>statement_timeout</c>'s, does.
    /// </summary>
    private static void WaitForGrantor(uint seqoid, bool unnamed = false)
    {
        Reload();
        if (!s_grantors.TryGetValue(seqoid, out uint grantor) || s_confirmed.Contains(grantor))
        {
            return;
        }

        if (!SequenceRangeHeard.Confirmed(grantor))
        {
            string sequence;
            unsafe
            {
                sequence = Worf.FromServer(NativeMethods.get_rel_name(seqoid));
            }

            string parent = Elevated.Run(() => Worf.ReadScalar<string?>("SELECT node_name::text FROM worf.node WHERE node_id = $1",
                SpiParameter.Create(grantor))) ?? grantor.ToString(CultureInfo.InvariantCulture);
            string waitingFor = $"waiting for node {parent}, which granted sequence {sequence}'s block, to answer this server since it started" +
                (unnamed ? ", as worf can't tell which sequence the call of nextval draws from before it runs" : "");
            using PgErrorContext context = PgErrorContext.Push(Worf.Shown(waitingFor));
            List<uint> held = LetGates();
            try
            {
                long deadline = Worker.GetCurrentTimestamp() + (SequenceRanges.ExchangeTimeout * 2000L);
                long woken = 0;
                while (!SequenceRangeHeard.Confirmed(grantor))
                {
                    // The wait makes the exchange with the grantor due, each second while it lasts.
                    long now = Worker.GetCurrentTimestamp();
                    if (now >= deadline)
                    {
                        throw unnamed
                            ? new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                                $"nextval can't draw until node {parent}, which granted sequence {sequence}'s block, answers this server, which started since",
                                detail: UnnamedGrantorUnheard,
                                hint: $"Make sure node {parent} is up and its exchanges with this node succeed, or name the sequence in the statement.")
                            : new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                                $"sequence {sequence} can't draw from the block node {parent} granted it until node {parent} answers this server, which started since",
                                detail: GrantorUnheard,
                                hint: $"Make sure node {parent} is up and its exchanges with this node succeed.");
                    }

                    if (now - woken >= 1_000_000)
                    {
                        woken = now;
                        SequenceRangeHeard.Want(grantor);
                        SequenceRangeExchangeWorker.Wake();
                    }

                    PgInterrupts.Check();
                    Worker.WaitLatch(100);
                }
            }
            finally
            {
                TakeGates(held);
            }
        }

        _ = s_confirmed.Add(grantor);
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
    /// to a table no managed sequence numbers reads none of worf's tables, as of the table's last change, as a partition
    /// attached, which takes on the guard.
    /// </summary>
    private static bool Guarded(uint relid)
    {
        WatchTables();
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
    /// Lists the managed sequences a table's defaults and identity columns draw from, a partition's identity columns
    /// drawing from its ancestors' sequences where PostgreSQL marks them identity columns, as from PostgreSQL 17 on it
    /// marks those of a partition it creates or attaches, though not those of a partition of a table it attaches.
    /// </summary>
    private static List<uint> DrawnBy(uint relid)
    {
        if (relid == 0)
        {
            return [];
        }

        // Before PostgreSQL 17 a partition shares no identity column with its ancestors: an insert into it draws from
        // its own identity column's sequence, if it has one.
#if ANKUS_PG15 || ANKUS_PG16
        const string Ancestors = "";
#else
        const string Ancestors = """
            UNION
            SELECT d.objid FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_partition_ancestors($1) p ON p.relid <> $1
                JOIN pg_catalog.pg_attribute pa ON pa.attrelid = p.relid AND pa.attname = a.attname AND NOT pa.attisdropped
                JOIN pg_catalog.pg_depend d ON d.refobjid = p.relid AND d.refobjsubid = pa.attnum AND d.deptype = 'i'
                    AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
            WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped AND a.attidentity <> ''
            """;
#endif
        WatchTables();
        if (!s_drawnBy.TryGetValue(relid, out List<uint>? sequences))
        {
            WorfRows used = Elevated.Run(() => Worf.Read($"""
                SELECT d.refobjid FROM pg_catalog.pg_attrdef a JOIN pg_catalog.pg_depend d
                    ON d.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass AND d.objid = a.oid AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
                WHERE a.adrelid = $1
                UNION
                SELECT d.objid FROM pg_catalog.pg_depend d
                WHERE d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
                    AND d.deptype = 'i' AND d.refobjid = $1
                {Ancestors}
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
    /// Lets the gates this backend holds go while a switch waits for them, at a row of a statement that holds them, and
    /// takes them again once the switch has committed, and hears of it, so that the next row's nextval reads its bounds.
    /// </summary>
    /// <returns>The sequences whose gates it let go, which the statement checks before it draws from them again.</returns>
    private static List<uint> PassGates()
    {
        if (s_gates.Count == 0 || SequenceRanges.SwitchesPending.Value == 0)
        {
            return [];
        }

        List<uint> passing = [.. s_gates.Keys.Where(seqoid => Drawer(seqoid).Switching > 0).Order()];
        foreach (uint seqoid in passing)
        {
            LetGate(seqoid);
        }

        TakeGates(passing);
        return passing;
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

        // The door, which a switch shuts, is passed on the way.
        LOCKTAG door = GateTag(seqoid, DoorKey);
        _ = NativeMethods.LockAcquire(&door, ShareLock, false, false);
        LOCKTAG tag = GateTag(seqoid);
        _ = NativeMethods.LockAcquire(&tag, ShareLock, false, false);
        _ = NativeMethods.LockRelease(&door, ShareLock, false);
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
    /// they check the sequence again, and the number that wait for it to switch, from an active block, or from any.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="activeLo">The active block's start, or null for any.</param>
    /// <returns>The numbers.</returns>
    internal static (int Statements, int Waiting) Drawing(uint seqoid, long? activeLo = null)
    {
        WorfDrawer drawer = Drawer(seqoid);
        return (drawer.Statements, activeLo is null || drawer.WaitingFor == activeLo ? drawer.Waiting : 0);
    }

    /// <summary>
    /// Counts the range worker's check of a sequence, or a switch worker's try at switching it, for the statements that
    /// wait for its switch.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    internal static void CountAttempt(uint seqoid)
    {
        uint dboid = DatabaseId();
        using PgLwLockExclusiveGuard<WorfDrawers, WorfDrawer> guard = Drawers.Exclusive();
        for (int probe = 0, i = Slot(dboid, seqoid); probe < MaxDrawers && guard[i].Dboid != 0; probe++, i = (i + 1) % MaxDrawers)
        {
            if (guard[i].Dboid == dboid && guard[i].Seqoid == seqoid)
            {
                WorfDrawer drawer = guard[i];
                drawer.Attempts++;
                guard[i] = drawer;
                return;
            }
        }
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
    /// Finds where a sequence's entry starts its search in the shared table. An entry stays once made, so that no search
    /// stops short at a freed one, but one whose counts are all zero is free for another sequence, so that sequences
    /// dropped never fill the table.
    /// </summary>
    private static int Slot(uint dboid, uint seqoid) => (int)((dboid * 2654435761UL ^ seqoid) % MaxDrawers);

    /// <summary>
    /// Counts a statement that takes or lets go of a sequence's gate, or that starts or stops waiting for it to switch
    /// from an active block, which counts only those waiting for the same block. A sequence that finds no entry free, as
    /// while more than the table holds are counted at once, goes uncounted.
    /// </summary>
    private static void CountDrawer(uint seqoid, int statements, int waiting = 0, int switching = 0, long waitingFor = 0)
    {
        uint dboid = DatabaseId();
        using PgLwLockExclusiveGuard<WorfDrawers, WorfDrawer> guard = Drawers.Exclusive();
        int free = -1;
        for (int probe = 0, i = Slot(dboid, seqoid); probe < MaxDrawers; probe++, i = (i + 1) % MaxDrawers)
        {
            if (guard[i].Dboid == dboid && guard[i].Seqoid == seqoid)
            {
                WorfDrawer drawer = guard[i];
                drawer.Statements = Math.Max(drawer.Statements + statements, 0);
                if (waiting > 0 && drawer.WaitingFor != waitingFor)
                {
                    drawer.Waiting = 0;
                    drawer.WaitingFor = waitingFor;
                }

                if (waiting > 0 || drawer.WaitingFor == waitingFor)
                {
                    drawer.Waiting = Math.Max(drawer.Waiting + waiting, 0);
                }

                drawer.Switching = Math.Max(drawer.Switching + switching, 0);
                guard[i] = drawer;
                return;
            }

            if (free < 0 && (guard[i].Dboid == 0 || guard[i] is { Statements: 0, Waiting: 0, Switching: 0 }))
            {
                free = i;
            }

            if (guard[i].Dboid == 0)
            {
                break;
            }
        }

        if (free >= 0 && (statements > 0 || waiting > 0 || switching > 0))
        {
            guard[free] = new WorfDrawer
            {
                Dboid = dboid,
                Seqoid = seqoid,
                Statements = Math.Max(statements, 0),
                Waiting = Math.Max(waiting, 0),
                WaitingFor = waiting > 0 ? waitingFor : 0,
                Switching = Math.Max(switching, 0),
            };
        }
    }

    /// <summary>
    /// Shuts a sequence's switch gate for a switch beside the transactions that hold it, until its transaction ends:
    /// it waits for the statements that hold the gate to reach their next rows, as for a lock, while those that start
    /// wait at the door, which it shuts first, and so do statements waiting for the switch. A statement that holds the
    /// gate while it waits for another lock holds the switch up until it has that lock, and one paused within its rows,
    /// as a COPY waiting for its client, until it goes on; a cycle of such waits, as when the transaction the statement
    /// waits for waits for the switch, is a deadlock that PostgreSQL's deadlock detector sees, as every wait in it is
    /// for a lock. Statements that hold the gate let it go at their next row while any switch waits for one.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    /// <param name="timeout">How long to wait, in milliseconds, after which the switch fails with 55P03: a tenth of a
    /// second in the range worker, which checks the other sequences meanwhile and leaves the switch to a switch worker;
    /// or a second in a switch worker, which waits for the gate first, as long as it takes, with
    /// <see cref="AwaitGate"/>, and again after such a failure.</param>
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
        LOCKTAG door = GateTag(seqoid, DoorKey);
        _ = NativeMethods.LockAcquire(&door, ExclusiveLock, false, false);
        LOCKTAG tag = GateTag(seqoid);
        _ = NativeMethods.LockAcquire(&tag, ExclusiveLock, false, false);
    }

    /// <summary>
    /// Waits, as for a lock, for as long as it takes, until the statements that hold
    /// a sequence's switch gate have let it go, with the door shut meanwhile, and then lets both go: a switch worker waits
    /// so before it locks the sequence's range row, so that a statement paused within its rows, as a COPY waiting for its
    /// client, holds up only the switch and the statements that wait for it, and not the exchanges and the range worker,
    /// which wait for that row. A cycle of waits through the gate is a deadlock that PostgreSQL's deadlock detector ends,
    /// as the switch worker looks for one last, in the statement, which can go on once it's retried, rather than in the
    /// switch, which the switch worker would make again into the same cycle.
    /// </summary>
    /// <param name="seqoid">The sequence.</param>
    internal static unsafe void AwaitGate(uint seqoid)
    {
        _ = PgAtomic.Add(SequenceRanges.SwitchesPending, 1L);
        CountDrawer(seqoid, 0, 0, 1);
        int nestLevel = NativeMethods.NewGUCNestLevel();
        LOCKTAG door = GateTag(seqoid, DoorKey);
        LOCKTAG tag = GateTag(seqoid);
        bool shut = false;
        bool taken = false;
        try
        {
            _ = Worf.Execute("SET LOCAL lock_timeout = 0");
            _ = Worf.Execute("SET LOCAL deadlock_timeout = '1h'");
            _ = NativeMethods.LockAcquire(&door, ExclusiveLock, false, false);
            shut = true;
            _ = NativeMethods.LockAcquire(&tag, ExclusiveLock, false, false);
            taken = true;
        }
        finally
        {
            if (taken)
            {
                _ = NativeMethods.LockRelease(&tag, ExclusiveLock, false);
            }

            if (shut)
            {
                _ = NativeMethods.LockRelease(&door, ExclusiveLock, false);
            }

            NativeMethods.AtEOXact_GUC(true, nestLevel);
            CountDrawer(seqoid, 0, 0, -1);
            _ = PgAtomic.Add(SequenceRanges.SwitchesPending, -1L);
        }
    }

    /// <summary>
    /// Waits, as for a lock, for a switch of a sequence that holds its door to end, if one does, so that a statement
    /// waiting for the switch shows in pg_locks as waiting for the switch's transaction, and a cycle through it is a
    /// deadlock PostgreSQL's deadlock detector sees.
    /// </summary>
    /// <returns>Whether a switch held the door.</returns>
    private static unsafe bool WaitAtDoor(uint seqoid)
    {
        LOCKTAG door = GateTag(seqoid, DoorKey);
        bool shut = NativeMethods.LockAcquire(&door, ShareLock, false, true) == LockAcquireResult.LOCKACQUIRE_NOT_AVAIL;
        if (shut)
        {
            _ = NativeMethods.LockAcquire(&door, ShareLock, false, false);
        }

        _ = NativeMethods.LockRelease(&door, ShareLock, false);
        return shut;
    }

    /// <summary>
    /// Names a sequence's switch gate.
    /// </summary>
    private static LOCKTAG GateTag(uint seqoid, ushort key = GateKey) => new()
    {
        locktag_field1 = DatabaseId(),
        locktag_field2 = seqoid,
        locktag_field3 = 0,
        locktag_field4 = key,
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
        // The row's value is in the block switched from, so the statement checks the new block before it draws again,
        // as one that takes its gates does: while it waited, uncounted, the others may have drawn most of it.
        List<uint> passed = PassGates();
        Refresh();
        Reload();
        foreach (uint passedSeqoid in passed)
        {
            WaitWhileShort(passedSeqoid);
        }

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
                // and so does one that rolled back, and a value it issued from the block meanwhile is this node's. The
                // sequence's bounds are still the active block's, past which nextval fails, so the row is the active
                // block's last: it waits for the switch, or switches in its transaction.
                case "next":
                    bool issuedHere = Position(seqoid) is long issued && issued >= lo && issued <= hi && (sequence.Ascending ? issued >= value : issued <= value);
                    if (issuedHere && sequence.Blocks.Find(block => block.State == "active") is (long activeLo, long activeHi, "active"))
                    {
                        NearEnd(seqoid, sequence.Ascending, activeLo, activeHi, sequence.Ascending ? activeHi : activeLo);
                    }

                    return issuedHere;

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
    /// rather than run the block out. While the next block is still to come, as over an exchange that may hang, a
    /// backend waits up to twice the exchange timeout for each block in all, which covers an exchange that hangs and the
    /// check after it, however many of its statements and rows wait. Once the next block is here, the switch is this
    /// node's alone, and a backend waits for it as for a lock, until it comes, or statement_timeout or a cancellation
    /// ends the wait, however long a busy node takes to come to the sequence, while a range or switch worker runs to make
    /// it; once a worker has tried and not switched it, as when a transaction holds its range row, the time is up as
    /// before. A switch under way, which waits for the statements that hold the sequence's gate, is waited for at its
    /// door, as for a lock, and that wait doesn't count toward the time. It waits only while the worker can switch: the sequence draws from blocks, or has used one up and waits for the next, and no
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
        CountWaiting(seqoid, lo, 1);
        bool switched;
        try
        {
            switched = WaitForSwitchWithoutGates(seqoid, lo);
        }
        finally
        {
            CountWaiting(seqoid, lo, -1);
        }

        TakeGates(held);
        return switched;
    }

    /// <summary>
    /// Counts this backend as waiting for a sequence's switch, or no longer, in the shared table and its own.
    /// </summary>
    private static void CountWaiting(uint seqoid, long lo, int change)
    {
        HookExit();
        CountDrawer(seqoid, 0, change, waitingFor: lo);
        int waiting = s_waitingFor.GetValueOrDefault(seqoid).Count + change;
        if (waiting > 0)
        {
            s_waitingFor[seqoid] = (waiting, lo);
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
            foreach ((uint seqoid, (int waiting, long lo)) in s_waitingFor)
            {
                CountDrawer(seqoid, 0, -waiting, waitingFor: lo);
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
        if (!s_waited.TryGetValue((seqoid, lo), out (long Deadline, bool Expired) waited))
        {
            waited = (now + (SequenceRanges.ExchangeTimeout * 2000L), false);
            s_waited.Add((seqoid, lo), waited);
        }

        // A wait for the block that ran out of time already, as an earlier row's, waits no more, unless the block has
        // switched since.
        if (waited.Expired)
        {
            Reload();
            return !s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) known) ||
                !known.Blocks.Exists(block => block.State == "active" && block.Lo == lo);
        }

        long deadline = waited.Deadline;
        WorfDrawer counted = Drawer(seqoid);
        long attempts = counted.Attempts;
        long woken = 0;
        long checkedHolders = now - 200_000;
        while (true)
        {
            // The holders are read a few times a second, as pg_locks takes every lock partition's lock to read them.
            if (now - checkedHolders >= 200_000)
            {
                checkedHolders = now;
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
            }

            // The worker tries the switch again soon after one that gave up on the lock, and requests a block the
            // moment it can.
            if (now - woken >= 100_000)
            {
                woken = now;
                _ = PgAtomic.Add(SequenceRanges.CheckRequested, 1L);
                Worker.WakeRangeWorker();
            }

            // A switch that holds the door is waited for as for a lock, however long it waits for the statements that
            // hold the gate, and the time is up only once one fails, as when the deadlock detector ends it.
            PgInterrupts.Check();
            long atDoor = Worker.GetCurrentTimestamp();
            bool shut = WaitAtDoor(seqoid);
            if (!shut)
            {
                Worker.WaitLatch(10);
            }

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
            if (shut)
            {
                deadline += now - atDoor;
            }

            // A try that began before this backend waited may not have seen it, so it waits for one more, as far as the
            // shared table counts the sequence's tries.
            if (now >= deadline &&
                !(counted.Dboid != 0 && sequence.Blocks.Exists(block => block.State == "next") && Switchers().Length > 0 && Drawer(seqoid).Attempts <= attempts + 1))
            {
                s_waited[(seqoid, lo)] = (deadline, true);
                return false;
            }
        }
    }

    /// <summary>
    /// Lists the processes of this database's range worker and switch workers, whose locks on a sequence are its switches.
    /// </summary>
    internal static int[] Switchers()
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
        uint subxact;
        unsafe
        {
            subxact = NativeMethods.GetCurrentSubTransactionId();
        }

        if (!s_switchedInTransaction.TryAdd((seqoid, lo), subxact))
        {
            return;
        }

        List<uint> held = LetGates();

        // A savepoint that rolls back undoes the switches made under it, so the transaction may try them again.
        if (s_switchedInTransaction.Count == 1 && !s_switchesRegistered)
        {
            s_switchesRegistered = true;
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, ForgetSwitches);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, ForgetSwitches);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, ForgetSwitches);
            _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Abort, static (aborted, parent) =>
            {
                foreach ((uint, long) block in s_switchedInTransaction.Where(entry => entry.Value == aborted.Value).Select(entry => entry.Key).ToList())
                {
                    _ = s_switchedInTransaction.Remove(block);
                }

                foreach (uint sequence in s_switchedItself.Where(entry => entry.Value == aborted.Value).Select(entry => entry.Key).ToList())
                {
                    _ = s_switchedItself.Remove(sequence);
                }
            });
            _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Commit, static (committed, parent) =>
            {
                foreach ((uint, long) block in s_switchedInTransaction.Where(entry => entry.Value == committed.Value).Select(entry => entry.Key).ToList())
                {
                    s_switchedInTransaction[block] = parent.Value;
                }

                foreach (uint sequence in s_switchedItself.Where(entry => entry.Value == committed.Value).Select(entry => entry.Key).ToList())
                {
                    s_switchedItself[sequence] = parent.Value;
                }
            });
        }

        // A transaction that switched the sequence itself holds it against the range worker's switches, which would
        // otherwise come as it waits, so it waits for the next block the worker brings meanwhile, and switches to it.
        long deadline = Worker.GetCurrentTimestamp() + (SequenceRanges.ExchangeTimeout * 2000L);
        long woken = 0;
        while (true)
        {
            if (SequenceRanges.SwitchInTransaction(seqoid))
            {
                _ = s_switchedItself.TryAdd(seqoid, subxact);
                Reload();
                break;
            }

            long now = Worker.GetCurrentTimestamp();
            if (!s_switchedItself.ContainsKey(seqoid) || now >= deadline)
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
        s_switchesRegistered = false;
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

        _ = PassGates();
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
        s_grantors.Clear();
        s_drawnBy.Clear();
        s_drawingColumns.Clear();
        s_defaultsText.Clear();
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
                b.node_id = l.node_id AND b.sysid = $1 AND b.timeline = $2, b.reserved,
                CASE WHEN b.from_node <> l.node_id AND EXISTS (SELECT 1 FROM worf.node n WHERE n.node_id = b.from_node) THEN b.from_node END
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

                if (state == "active" && row.Get<uint?>(7) is uint grantor)
                {
                    s_grantors[seqoid] = grantor;
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
