using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
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
    internal const string GrantorUnheard = "A server that restarted draws from a sequence's blocks only once the block's parent, and every other node it exchanges with that reports the sequence, has checked its report since: the parent may have given the block's values out again meanwhile, and any of them may have heard of draws a restore took back.";

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

    // The managed sequences the triggers of each table and its partitions name among their arguments, by the table and
    // the events the triggers fire on.
    private static readonly Dictionary<(uint Relid, int Events, int MovedInto), List<uint>> s_arguedBy = [];

    // TRIGGER_TYPE_INSERT and TRIGGER_TYPE_UPDATE, the bits of pg_trigger.tgtype for the events a trigger fires on.
    private const int TriggerTypeInsert = 1 << 2;

    // TRIGGEROID, the type a trigger function returns.
    private const uint TriggerTypeOid = 2279;
    private const int TriggerTypeUpdate = 1 << 4;

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

    // The fields that nodeToString writes a list of types' OIDs in, as a function's column definition list's, whose
    // input a record function runs, or a table function's columns'.
    private static readonly HashSet<string> s_typeListFields = ["funccoltypes", "coltypes", "colTypes"];

    // The PL/pgSQL expression whose calls of nextval started last, which PL/pgSQL starts outside a statement, as of the
    // transaction it started in before, with the sequences its calls name once a call has looked, and how many have.
    private static (nint Expr, uint Lxid, (HashSet<uint> Called, int Calls, bool Unnamed)? Draws, int Looked) s_simple;

    // Where PL/pgSQL finds its instrumentation plugin, which it tells the function its error context calls.
    private static unsafe void** s_plpgsqlPlugin;

    // The text of the defaults a worker's rows of a table take, by the columns the rows carry, and of the CHECK
    // constraints and trigger conditions the rows start.
    private static readonly Dictionary<(uint Relid, string Sent), string> s_defaultsText = [];

    // The sequences the CHECK constraints of this database's domains draw from, and whether one of them draws from a
    // sequence it doesn't name, under the search path they were read with, which the SQL functions they inline look
    // names up under, read again once a type, a constraint or a function changes.
    private static (HashSet<uint> Called, bool Unnamed, string Path)? s_domainDraws;

    // The sequences the CHECKs of the domains each PL/pgSQL function coerces its variables and result to draw from, by
    // the function as PL/pgSQL compiled it, once for each set of argument types a polymorphic function is called with,
    // read again once a type, a constraint or a function changes.
    private static readonly Dictionary<(nint Compiled, uint Function, uint Xmin), (HashSet<uint> Called, bool Unnamed)?> s_plpgsqlCasts = [];

    // Whether each function a statement called may check a domain the statement doesn't name, by the function, read
    // again once a function changes.
    private static readonly Dictionary<long, bool> s_checksUnnamed = [];

    // The sequences the calls of nextval of statements name, by the statements' text and the search path, read again once
    // a table, a type, a constraint or a function changes, or the managed sequences do.
    private static readonly Dictionary<(string Text, string Path, uint User), HashSet<uint>> s_namedByText = [];

    // The managed sequences the CHECK constraints, trigger conditions, defaults and column domains of the tables each
    // native subscription writes draw from, by the subscription, or null where one of them doesn't name its sequence,
    // read again once a table, a type, a constraint, a function or the subscription's tables change.
    private static readonly Dictionary<uint, HashSet<uint>?> s_subscriptionDraws = [];

    // The trigger functions the fmgr hook watches as they start in sessions that write as replicas do, and the managed
    // sequences named among the arguments of the triggers that call each, read again once a table changes.
    private static readonly HashSet<uint> s_hookedTriggers = [];
    private static readonly Dictionary<(uint Function, uint Subscription), List<uint>> s_triggerArguments = [];
    private static long s_managedSignature;

    // PostgreSQL's own functions that check a value against a type a statement may give only as a value, as text or an
    // OID, by their OIDs in its catalog, the same from 15 on: pg_input_is_valid and pg_input_error_info, which 16 added,
    // and the input and receive functions of records, domains, arrays, ranges and multiranges.
    private static readonly HashSet<long> s_inputCheckers = [6210, 6211, 2290, 2402, 2597, 2598, 750, 2400, 3834, 3836, 4231, 4233];

    // Whether the guard hears of the changes to tables and types that the text it holds by table reads, as a default
    // set, a partition attached or a domain's CHECK added.
    private static bool s_watchingTables;

#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    // TYPEOID, CONSTROID, PROCOID, NAMESPACEOID and SUBSCRIPTIONRELMAP, the ids of the catalog caches of types,
    // constraints, functions, schemas and subscriptions' tables, which only PostgreSQL 19's bindings name.
    private const int TypeCacheId = 80;
    private const int ConstraintCacheId = 19;
    private const int ProcedureCacheId = 45;
    private const int NamespaceCacheId = 36;
    private const int SubscriptionRelCacheId = 66;
#elif ANKUS_PG18
    private const int TypeCacheId = 82;
    private const int ConstraintCacheId = 19;
    private const int ProcedureCacheId = 47;
    private const int NamespaceCacheId = 38;
    private const int SubscriptionRelCacheId = 68;
#endif

    [PgNativeCallback(nameof(TableInvalidated))]
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
    private static partial RelcacheCallbackFunction TableCallback { get; }
#else
    private static partial RelSyncCallbackFunction TableCallback { get; }
#endif

    [PgNativeCallback(nameof(TypeInvalidated))]
    private static partial SyscacheCallbackFunction TypeCallback { get; }

    // Whether each table has the guard's triggers, which only tables numbered by a managed sequence have, and a trigger
    // that takes arguments, which may name a sequence it draws from, as of the versions of the shared and this backend's
    // counters it was looked up at.
    private static readonly Dictionary<uint, (bool Guarded, bool Argued)> s_guarded = [];
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

    // The node each sequence's draws wait for since this server started: the node that granted its active block, its
    // parent, when another node did, until it has checked this node's report of the sequence since then, and otherwise
    // any other node this node exchanges with directly that reports the sequence, and so may have heard of its draws,
    // as an owner's neighbors hear of the draws from its own blocks, until it has too; each one this node subscribes to,
    // or which exchanges with it through a slot of this node, as one that only answers that node's calls knows it.
    private static readonly Dictionary<uint, uint> s_grantors = [];
    // The sequences whose draws wait for no node, as the tables last read hold, which the reads leave empty, as they
    // name only nodes that haven't checked this node's report of a sequence since this server started.
    private static readonly HashSet<uint> s_confirmedSequences = [];

    // The sequences whose node waited for, as a start's draws wait, is a neighbor that may have heard of their draws,
    // rather than the grantor of their active block.
    private static readonly HashSet<uint> s_heardBy = [];

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

    // The managed sequences this transaction's statements draw from without holding their switch gates, as a SELECT of
    // nextval and a PL/pgSQL function's expressions do, with the rows, statements or calls left before each is checked
    // again for the switch, and whether the transaction's end forgets them.
    private static readonly Dictionary<uint, long> s_drawingUngated = [];
    private static bool s_drawingRegistered;
    private static bool s_countingDown;

    // The plan nodes of the statements that draw so, with the functions that produce their rows, which the guard counts
    // down for before each row, by the statement.
    private static readonly Dictionary<nint, (ExecProcNodeMtd Function, int Calls)> s_rowFunctions = [];
    private static readonly Dictionary<nint, (uint Subxact, List<nint> Nodes)> s_countedNodes = [];
    private static readonly Dictionary<(uint Function, uint Xmin), int> s_functionCalls = [];
    private static bool s_countedRegistered;

    [PgNativeCallback(nameof(CountedRow))]
    private static partial ExecProcNodeMtd CountedRowCallback { get; }

    [PgNativeCallback(nameof(PlpgsqlStatement))]
    private static partial PLpgSQL_plugin_stmt_begCallback PlpgsqlStatementCallback { get; }

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
        CountDownDraws();
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
        ForgetCounted((nint)queryDesc);

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
        // An INSERT's ON CONFLICT DO UPDATE updates the rows it conflicts with, which fires their update triggers.
        bool upserts = queryDesc->operation == CmdType.CMD_INSERT && statement is not null && statement->planTree is not null &&
            statement->planTree->type == NodeTag.T_ModifyTable && ((ModifyTable*)statement->planTree)->onConflictAction == OnConflictAction.ONCONFLICT_UPDATE;
        bool updates = queryDesc->operation is CmdType.CMD_UPDATE or CmdType.CMD_MERGE || upserts;
        // A statement that writes only to tables neither guarded nor with a trigger that takes arguments, among their
        // partitions', reads none of worf's tables.
        if (statement is null || written.Count == 0 || !(inserts || updates) || Worf.CatalogOid("sequence_range") == 0 ||
            !written.Exists(table => Triggers(table) is var triggers && (triggers.Guarded || triggers.Argued)))
        {
            return;
        }

        Reload();
        if (s_sequences.Count == 0)
        {
            return;
        }

        // An UPDATE, or a MERGE's, draws only as it sets a column to its default, which an identity column draws from
        // without calling nextval, so it holds the gates of the sequences of the columns it sets that its plan draws from,
        // and as its tables' update triggers, or their partitions' insert triggers, which fire as a row moves to another
        // partition, draw from the sequences they name. An insert draws from those its tables' defaults draw from, and
        // those their triggers, and their partitions', name, whether or not a guard checks the tables.
        List<uint> sequences = inserts ? [.. written.SelectMany(DrawnBy)] : [];
        if (updates)
        {
            if (!upserts && written.Exists(Guarded))
            {
                sequences.AddRange(UpdatedDrawing(statement));
            }

            sequences.AddRange(written.SelectMany(table => ArguedBy(table, TriggerTypeUpdate, upserts ? 0 : TriggerTypeInsert)));
        }

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
                SequenceRanges.RequestCheck(seqoid);
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

        // The plugin worf gives counts the statements PL/pgSQL starts down too.
        if (*s_plpgsqlPlugin is null)
        {
            PLpgSQL_plugin* plugin = (PLpgSQL_plugin*)NativeMemory.AllocZeroed((nuint)sizeof(PLpgSQL_plugin));
            plugin->stmt_beg = PlpgsqlStatementCallback;
            *s_plpgsqlPlugin = plugin;
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
    /// <param name="source">The query string the statement came in, or zero.</param>
    /// <param name="parameters">Whether the statement has parameters, whose values its plan may hold.</param>
    internal static unsafe void StartingExpressions(nint statement, bool explained, nint source = 0, bool parameters = false)
    {
        if (!explained)
        {
            PlannedStmt* planned = (PlannedStmt*)statement;
            int location = planned->stmt_location;
            int length = planned->stmt_len;
            s_starting.Add(new(() => Draws(PlanText((Node*)statement)), (nint)NativeGlobals.error_context_stack, false,
                source == 0 || parameters ? null : () => StatementText(source, location, length)));
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

        // A statement that draws from a managed sequence without its switch gate, as a SELECT of nextval does, counts
        // down for its sequences' checks before each row its plan's nodes produce, so that one statement that draws more
        // values than the block has left waits for the switch, as separate statements do.
        // Each row counts for as many draws as the statement has calls of nextval, as a row may call each of them once.
        if (s_starting.Count > 0 && s_starting[^1].Switched.Count > 0 && !s_starting[^1].Explained)
        {
            unsafe
            {
                QueryDesc* query = (QueryDesc*)queryDesc;
                List<nint> nodes = [];
                CountRows(query->planstate, nodes, Math.Max(s_starting[^1].Draws.Calls, 1));
                if (nodes.Count > 0)
                {
                    RegisterCounted();
                    s_countedNodes[queryDesc] = (NativeMethods.GetCurrentSubTransactionId(), nodes);
                }
            }
        }

        StartedExpressions();
    }

    // Has the guard count down before each row a plan node, and those under it, produce: its children, its initPlans
    // and subplans, as an ARRAY or scalar subquery runs, and the plans an Append, a MergeAppend or a subquery scan runs.
    private static unsafe void CountRows(PlanState* node, List<nint> nodes, int calls)
    {
        if (node is null || s_rowFunctions.ContainsKey((nint)node))
        {
            return;
        }

        s_rowFunctions[(nint)node] = (node->ExecProcNodeReal, calls);
        node->ExecProcNodeReal = CountedRowCallback;
        nodes.Add((nint)node);
        CountRows(node->lefttree, nodes, calls);
        CountRows(node->righttree, nodes, calls);
        foreach (List* subplans in new[] { node->initPlan, node->subPlan })
        {
            for (int i = 0; subplans is not null && i < subplans->length; i++)
            {
                CountRows(((SubPlanState*)subplans->elements[i].ptr_value)->planstate, nodes, calls);
            }
        }

        switch (node->type)
        {
            case NodeTag.T_AppendState:
                for (int i = 0; i < ((AppendState*)node)->as_nplans; i++)
                {
                    CountRows(((AppendState*)node)->appendplans[i], nodes, calls);
                }

                break;
            case NodeTag.T_MergeAppendState:
                for (int i = 0; i < ((MergeAppendState*)node)->ms_nplans; i++)
                {
                    CountRows(((MergeAppendState*)node)->mergeplans[i], nodes, calls);
                }

                break;
            case NodeTag.T_SubqueryScanState:
                CountRows(((SubqueryScanState*)node)->subplan, nodes, calls);
                break;
            default:
                break;
        }
    }

    // Produces a plan node's next row, after counting down for the checks of the sequences the statement draws from.
    private static unsafe TupleTableSlot* CountedRow(PlanState* node)
    {
        (ExecProcNodeMtd function, int calls) = s_rowFunctions[(nint)node];
        CountDownDraws(calls);
        return function.Invoke(node);
    }

    // Counts a PL/pgSQL statement down, as PL/pgSQL starts it, for as many draws as its function has calls of nextval,
    // as a loop's assignment draws each time it runs, which starts its expression only once a transaction.
    private static unsafe void PlpgsqlStatement(PLpgSQL_execstate* estate, PLpgSQL_stmt* stmt)
    {
        _ = stmt;
        if (s_drawingUngated.Count == 0 || s_countingDown || s_namingGrantors || Worf.InOwnSql || estate is null || estate->func is null)
        {
            return;
        }

        if (FunctionCalls(estate->func) is int calls and > 0)
        {
            CountDownDraws(calls);
        }
    }

    // How many calls of nextval a PL/pgSQL function's source has, once until it's replaced, or, for a DO block's, which
    // has no source of its own, the statement that runs it.
    private static unsafe int FunctionCalls(PLpgSQL_function* function)
    {
        if (function->fn_oid == 0)
        {
            return NextvalCalls(NativeGlobals.debug_query_string is null ? "" : Worf.FromServer(NativeGlobals.debug_query_string));
        }

#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
        (uint, uint) key = (function->fn_oid, function->fn_xmin);
#else
        (uint, uint) key = (function->fn_oid, function->cfunc.fn_xmin);
#endif
        if (!s_functionCalls.TryGetValue(key, out int calls))
        {
            uint oid = function->fn_oid;
            calls = NextvalCalls(Worf.ReadScalar<string?>("SELECT prosrc FROM pg_catalog.pg_proc WHERE oid = $1", SpiParameter.Create(oid)) ?? "");
            s_functionCalls[key] = calls;
        }

        return calls;
    }

    // How many calls of nextval a text has, as its name does.
    private static int NextvalCalls(string text)
    {
        int calls = 0;
        for (int at = text.IndexOf("nextval", StringComparison.OrdinalIgnoreCase); at >= 0; at = text.IndexOf("nextval", at + 7, StringComparison.OrdinalIgnoreCase))
        {
            calls++;
        }

        return calls;
    }

    // Forgets the nodes a statement counts down for, as it ends, or as the transaction or subtransaction it ran in
    // aborts, which ends it without ExecutorEnd.
    private static void ForgetCounted(nint queryDesc)
    {
        if (s_countedNodes.Remove(queryDesc, out (uint Subxact, List<nint> Nodes) counted))
        {
            foreach (nint node in counted.Nodes)
            {
                _ = s_rowFunctions.Remove(node);
            }
        }
    }

    private static void RegisterCounted()
    {
        if (s_countedRegistered)
        {
            return;
        }

        s_countedRegistered = true;
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, static () =>
        {
            s_countedNodes.Clear();
            s_rowFunctions.Clear();
            s_countedRegistered = false;
        });
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, static () => s_countedRegistered = false);
        _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, static () => s_countedRegistered = false);
        _ = PgTransaction.RegisterSubtransactionCallback(PgSubtransactionEvent.Abort, static (aborted, _) =>
        {
            foreach (nint queryDesc in s_countedNodes.Where(entry => entry.Value.Subxact >= aborted.Value).Select(entry => entry.Key).ToList())
            {
                ForgetCounted(queryDesc);
            }
        });
    }

    // Notes the managed sequences a statement draws from without their switch gates, after their first check, until the
    // transaction ends.
    private static void DrawUngated(List<uint> sequences)
    {
        if (!s_drawingRegistered)
        {
            s_drawingRegistered = true;
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Commit, ForgetUngated);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Abort, ForgetUngated);
            _ = PgTransaction.RegisterCallback(PgTransactionEvent.Prepare, ForgetUngated);
        }

        foreach (uint seqoid in sequences)
        {
            s_drawingUngated[seqoid] = RowsUntilShort(seqoid);
        }
    }

    private static void ForgetUngated()
    {
        s_drawingUngated.Clear();
        s_drawingRegistered = false;
    }

    // Counts a row, statement or call down, by the draws it may take, for each sequence drawn from without its gate,
    // and checks it for the switch, as before its first draw, once its count runs out, which half the values left
    // before the block's last twentieth, in the sequence's steps, sets, as other sessions may draw too.
    private static void CountDownDraws(long draws = 1)
    {
        if (s_drawingUngated.Count == 0 || s_countingDown || s_namingGrantors || Worf.InOwnSql)
        {
            return;
        }

        s_countingDown = true;
        try
        {
            foreach (uint seqoid in s_drawingUngated.Keys.ToList())
            {
                if (s_drawingUngated[seqoid] >= draws)
                {
                    s_drawingUngated[seqoid] -= draws;
                    continue;
                }

                if (!s_gates.ContainsKey(seqoid))
                {
                    WaitWhileShort(seqoid);
                }

                s_drawingUngated[seqoid] = RowsUntilShort(seqoid);
            }
        }
        finally
        {
            s_countingDown = false;
        }
    }

    // How many draws a sequence may take before it is checked again: half its steps left before the active block's last
    // twentieth, or none once there.
    private static long RowsUntilShort(uint seqoid)
    {
        Reload();
        if (!s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) sequence) ||
            sequence.Blocks.FindIndex(block => block.State == "active") is not (int index and >= 0) || ReadPosition(seqoid) is not long position)
        {
            return 0;
        }

        (long lo, long hi, _) = sequence.Blocks[index];
        decimal tail = ((decimal)hi - lo) / 20;
        decimal left = sequence.Ascending ? hi - tail - position : position - lo - tail;
        decimal step = Math.Max(Math.Abs((decimal)SequenceRanges.GetInfo(seqoid).Increment), 1);
        return left <= 0 ? 0 : (long)Math.Min(left / step / 2, int.MaxValue);
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

        // The table's triggers that its rows fire, whose C code draws from a sequence it names, wait for its grantor,
        // as worf's apply does as it writes a row.
        WritingRow(relation->rd_id, false);
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
                Starting(() => Draws(WithDomainChecks(DomainChecks(NativeMethods.typenameTypeId(null, NativeMethods.makeTypeNameFromNameList(typeName)), []))));
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

    // A statement's own text, as its place in the query string it came in gives it: from its location, which is -1 when
    // unknown, for its length, which is 0 when it runs to the end.
    private static unsafe string StatementText(nint source, int location, int length)
    {
        byte* start = (byte*)source + Math.Max(location, 0);
        int bytes = length > 0 ? length : MemoryMarshal.CreateReadOnlySpanFromNullTerminated(start).Length;
        return Encoding.Latin1.GetString(new ReadOnlySpan<byte>(start, bytes));
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
            Draws(WithDomainChecks(procedure[0].Get<string>(1))) is { Calls: > 0 } or { Unnamed: true })
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
    // rather than the fields whose names end in "type" that hold an enumeration's value, as a join's type, and in the
    // fields it writes a list of them in, as a function's column definition list's.
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

            if (end + 3 < text.Length && string.CompareOrdinal(text, end, " (o ", 0, 4) == 0 && s_typeListFields.Contains(text[name..end]))
            {
                int close = text.IndexOf(')', end);
                types.UnionWith(text[(end + 4)..(close < 0 ? text.Length : close)].Split(' ', StringSplitOptions.RemoveEmptyEntries)
                    .Select(listed => uint.TryParse(listed, NumberStyles.None, CultureInfo.InvariantCulture, out uint listedType) ? listedType : 0)
                    .Where(listedType => listedType != 0));
                continue;
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
            parts.Add(Draws(WithDomainChecks(TypesDomainChecks(types))));
        }

        foreach ((HashSet<uint> Called, int Calls, bool Unnamed) part in parts)
        {
            called.UnionWith(part.Called);
            calls += part.Calls;
            unnamed |= part.Unnamed;
        }

        return (called, calls, unnamed);
    }

    // The text of the CHECK constraints that may draw of the domains some types are or hold, as array elements, fields
    // or range bounds, as the type cache holds them.
    private static string TypesDomainChecks(long[] types)
    {
        if (types.Length == 0)
        {
            return "";
        }

        Dictionary<uint, string> read = [];
        return string.Concat(Elevated.Run(() => Worf.Read($"""
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
                SELECT DISTINCT c.contypid FROM r JOIN pg_catalog.pg_constraint c ON c.contypid = r.typ
                WHERE r.typ <> 0 AND c.contype = 'c' AND c.conbin::pg_catalog.text ~ '{MayDrawPattern}'
                """, 0, SpiParameter.Create(types))).Select(row => DomainChecks(row.Get<uint>(0), read)));
    }

    // The text of the CHECK constraints and trigger conditions of tables and their partitions, which start as each
    // table takes its first row, with those of the domains they coerce to, and the calls of the tables' trigger
    // functions, which may check values against domains.
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
                SELECT g.tgqual::pg_catalog.text FROM t JOIN pg_catalog.pg_trigger g ON g.tgrelid = t.relid WHERE g.tgqual IS NOT NULL
                UNION ALL
                SELECT pg_catalog.format('{TRIGGER :funcid %s }', g.tgfoid) FROM t JOIN pg_catalog.pg_trigger g ON g.tgrelid = t.relid) x (expr)
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
            NativeMethods.CacheRegisterSyscacheCallback(SysCacheIdentifier.PROCOID, TypeCallback, 0);
            NativeMethods.CacheRegisterSyscacheCallback(SysCacheIdentifier.NAMESPACEOID, TypeCallback, 0);
            NativeMethods.CacheRegisterSyscacheCallback(SysCacheIdentifier.SUBSCRIPTIONRELMAP, TypeCallback, 0);
#else
            NativeMethods.CacheRegisterSyscacheCallback(TypeCacheId, TypeCallback, 0);
            NativeMethods.CacheRegisterSyscacheCallback(ConstraintCacheId, TypeCallback, 0);
            NativeMethods.CacheRegisterSyscacheCallback(ProcedureCacheId, TypeCallback, 0);
            NativeMethods.CacheRegisterSyscacheCallback(NamespaceCacheId, TypeCallback, 0);
            NativeMethods.CacheRegisterSyscacheCallback(SubscriptionRelCacheId, TypeCallback, 0);
#endif
        }

        s_watchingTables = true;
    }

    // Forgets what the guard holds for a changed table, or for every table, as a partition attached, which takes on its
    // parent's triggers and identity columns, or a default set.
    private static void TableInvalidated(ulong arg, uint relid)
    {
        _ = arg;
        s_namedByText.Clear();

        // What a table's triggers draw counts its partitions' too, which a change to a partition changes.
        s_drawnBy.Clear();
        s_arguedBy.Clear();
        s_guarded.Clear();
        s_subscriptionDraws.Clear();
        s_triggerArguments.Clear();
        if (relid == 0)
        {
            s_drawingColumns.Clear();
            s_defaultsText.Clear();
            return;
        }

        _ = s_drawingColumns.Remove(relid);
        foreach ((uint Relid, string Sent) key in s_defaultsText.Keys.Where(key => key.Relid == relid).ToList())
        {
            _ = s_defaultsText.Remove(key);
        }
    }

    // Forgets the defaults' text, which holds the CHECKs of their domains and the bodies of the SQL functions they
    // inline, and the domains' draws, once a type, a constraint or a function changes, as a domain's CHECK, and what
    // statements' text named, once a schema does, as one renamed, which names look sequences up in, and the draws of
    // native subscriptions, once their tables do.
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
        s_plpgsqlCasts.Clear();
        s_checksUnnamed.Clear();
        s_namedByText.Clear();
        s_subscriptionDraws.Clear();
    }

    // The text of a plan, as nodeToString writes it, and of the CHECK constraints of the domains it coerces to, which
    // start with it from the type cache. The planner has inlined the SQL functions the plan's own expressions call.
    private static unsafe string PlanText(Node* plan) => WithDomainChecks(NodeText(plan), planned: true);

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
    // domains' base domains, once for each coercion, as each starts its own, and of the domains those coerce to, with
    // the bodies of the SQL functions each of those texts calls that the planner inlines as the expression starts, and
    // of those the bodies call in turn. A plan's own text has its functions inlined already. A domain whose CHECKs call
    // no nextval, no function the planner may inline and coerce to no domain names the same sequences, through the
    // functions it calls, however many coercions start it, so its text goes once: a multi-row VALUES list of thousands
    // of values coerced to it would otherwise add its text thousands of times, megabytes that each backend that plans
    // the statement builds and collects.
    private static string WithDomainChecks(string text, bool planned = false)
    {
        StringBuilder all = new(text);
        StringBuilder found = new();
        Dictionary<uint, string> read = [];
        Dictionary<uint, bool> counted = [];
        HashSet<uint> added = [];
        Dictionary<string, string?> inlined = [];
        string pending = text;
        for (int depth = 0; depth < 8 && pending.Length > 0 && all.Length < 4 << 20; depth++)
        {
            _ = found.Clear();
            if (!planned || depth > 0)
            {
                foreach (string body in InlinedBodies(pending, inlined))
                {
                    _ = all.Append(' ').Append(body);
                    _ = found.Append(body).Append(' ');
                }
            }

            foreach (uint domain in CoercedDomains(pending))
            {
                string checks = DomainChecks(domain, read);
                if (!counted.TryGetValue(domain, out bool counts))
                {
                    counts = NextvalCall().IsMatch(checks) || UserFunctionCall().IsMatch(checks) || checks.Contains("{COERCETODOMAIN ", StringComparison.Ordinal);
                    counted[domain] = counts;
                }

                if (counts || added.Add(domain))
                {
                    _ = all.Append(' ').Append(checks);
                    _ = found.Append(checks);
                }
            }

            pending = found.ToString();
        }

        return all.ToString();
    }

    // The bodies of the SQL functions an expression's text calls, as functions or through operators, that the planner
    // inlines, once for each call, as each call's expression starts the body's own. PostgreSQL's own functions draw
    // nothing.
    private static List<string> InlinedBodies(string text, Dictionary<string, string?> inlined)
    {
        List<(uint Function, int At)> calls = [];
        foreach (Match call in UserFunctionCall().Matches(text))
        {
            calls.Add((uint.Parse(call.Groups[1].ValueSpan, CultureInfo.InvariantCulture), call.Index));
        }

        if (calls.Count == 0)
        {
            return [];
        }

        // The functions the planner may inline: SQL ones that run with no settings or rights of their own and return one
        // value of a known type, with their bodies, and whether a body may call a volatile function where the function
        // says it doesn't, which the planner won't inline.
        WorfRows candidates = Elevated.Run(() => Worf.Read("""
            SELECT p.oid, p.prosqlbody::pg_catalog.text, p.provolatile = 'v'
            FROM pg_catalog.pg_proc p
            WHERE p.oid = ANY ($1::pg_catalog.int8[]::pg_catalog.oid[]) AND p.prolang = 14 AND p.prokind = 'f' AND NOT p.prosecdef
                AND NOT p.proretset AND p.prorettype <> 'pg_catalog.record'::pg_catalog.regtype AND p.proconfig IS NULL
            """, 0, SpiParameter.Create(calls.Select(call => (long)call.Function).Distinct().ToArray())));
        Dictionary<uint, (string? SqlBody, bool Volatile)> inlinable = [];
        for (int i = 0; i < candidates.Count; i++)
        {
            inlinable[candidates[i].Get<uint>(0)] = (candidates[i].Get<string?>(1), candidates[i].Get<bool>(2));
        }

        List<string> bodies = [];
        foreach ((uint function, int at) in calls.Where(call => inlinable.ContainsKey(call.Function)))
        {
            string call = NodeAt(text, at);
            if (!inlined.TryGetValue(call, out string? body))
            {
                body = InlinedBody(function, call, inlinable[function].SqlBody);
                if (body is not null && !inlinable[function].Volatile && NextvalCall().IsMatch(body))
                {
                    body = null;
                }

                inlined[call] = body;
            }

            if (body is not null)
            {
                bodies.Add(body);
            }
        }

        return bodies;
    }

    // A call, as a function or through an operator, of a function that isn't PostgreSQL's own.
    [GeneratedRegex(@"\{(?:FUNCEXPR :funcid|OPEXPR :opno [0-9]+ :opfuncid) ([1-9][0-9]{4,}) ", RegexOptions.CultureInvariant)]
    private static partial Regex UserFunctionCall();

    // The text of the expression a SQL function's body gives, which the planner puts in place of a call of it, read as
    // the planner reads it, for the call whose text is given, or null when the planner keeps the call: when the body is
    // no single SELECT of one expression from nothing. A body worf can't read, which the planner would fail on as it
    // inlines it, is taken as a call of nextval that names nothing.
    private static string? InlinedBody(uint function, string call, string? sqlBody)
    {
        string? body = null;
        try
        {
            PgTransaction.RunInSubtransaction(() => body = InlinedExpression(function, call, sqlBody) is string inlined ? WithArguments(inlined, call) : null);
        }
        catch (PgException)
        {
            body = "{FUNCEXPR :funcid 1574 :args ({UNREAD})}";
        }

        return body;
    }

    // Reads a SQL function's body as InlinedBody does.
    private static unsafe string? InlinedExpression(uint function, string call, string? sqlBody)
    {
        Query* query;
        if (sqlBody is not null)
        {
            Node* stored = (Node*)NativeMethods.stringToNode(Worf.PStrDup(sqlBody));
            List* statements = stored->type == NodeTag.T_List ? (List*)((List*)stored)->elements[0].ptr_value : NativeMethods.lappend(null, stored);
            if (statements is null || statements->length != 1)
            {
                return null;
            }

            query = (Query*)statements->elements[0].ptr_value;
        }
        else
        {
#if ANKUS_PG19
            HeapTupleData* tuple = NativeMethods.SearchSysCache1(SysCacheIdentifier.PROCOID, function);
#else
            HeapTupleData* tuple = NativeMethods.SearchSysCache1(ProcedureCacheId, function);
#endif
            if (tuple is null)
            {
                return null;
            }

            try
            {
                string source = Elevated.Run(() => Worf.ReadScalar<string>("SELECT p.prosrc FROM pg_catalog.pg_proc p WHERE p.oid = $1", SpiParameter.Create(function))) ?? "";
                sbyte* text = Worf.PStrDup(source);
                List* raw = NativeMethods.pg_parse_query(text);
                if (raw is null || raw->length != 1)
                {
                    return null;
                }

                Node* expression = (Node*)NativeMethods.stringToNode(Worf.PStrDup(call));
                uint collation = expression->type == NodeTag.T_FuncExpr ? ((FuncExpr*)expression)->inputcollid : ((OpExpr*)expression)->inputcollid;
                SQLFunctionParseInfo* parameters = NativeMethods.prepare_sql_fn_parse_info(tuple, expression, collation);
                ParseState* pstate = NativeMethods.make_parsestate(null);
                pstate->p_sourcetext = text;
                NativeMethods.sql_fn_parser_setup(pstate, parameters);
                query = NativeMethods.transformTopLevelStmt(pstate, (RawStmt*)raw->elements[0].ptr_value);
                NativeMethods.free_parsestate(pstate);
            }
            finally
            {
                NativeMethods.ReleaseSysCache(tuple);
            }
        }

        bool single = query is not null && query->type == NodeTag.T_Query && query->commandType == CmdType.CMD_SELECT && !query->hasAggs &&
            !query->hasWindowFuncs && !query->hasTargetSRFs && !query->hasSubLinks && query->cteList is null && query->rtable is null &&
            (query->jointree is null || (query->jointree->fromlist is null && query->jointree->quals is null)) && query->groupClause is null &&
            query->groupingSets is null && query->havingQual is null && query->windowClause is null && query->distinctClause is null &&
            query->sortClause is null && query->limitOffset is null && query->limitCount is null && query->setOperations is null &&
            query->targetList is not null && query->targetList->length == 1;
        if (!single)
        {
            return null;
        }

        // A body read from its source looks its sequences' names up under the search path the planner had as it
        // inlined it, which the type cache keeps for a domain's CHECK until the domain changes, so each sequence it names
        // stands for every managed one of that name.
        string body = NodeText((Node*)((TargetEntry*)query->targetList->elements[0].ptr_value)->expr);
        if (sqlBody is null)
        {
            const string Named = "{CONST :consttype 2205 ";
            StringBuilder marked = new(body);
            IEnumerable<byte[]> names = NextvalCall().Matches(body)
                .Select(call => Argument(body, call.Index))
                .Where(argument => argument >= 0 && string.CompareOrdinal(body, argument, Named, 0, Named.Length) == 0)
                .Select(argument => ConstBytes(body, argument))
                .OfType<byte[]>()
                .Where(bytes => bytes.Length >= 4);
            foreach (byte[] bytes in names)
            {
                _ = marked.Append(CultureInfo.InvariantCulture, $" {Relname}{(uint)(bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24))} }}");
            }

            body = marked.ToString();
        }

        return body;
    }

    // Marks, in an expression's text, a sequence that stands for every managed sequence of its name.
    private const string Relname = "{WORFRELNAME :seqoid ";

    // A body with a call's arguments in place of its parameters, as the planner puts them, so that a call of nextval
    // given one names the sequence the call gives. A call that names its arguments, which the planner orders later,
    // leaves its parameters as they are, naming nothing, as does a parameter left to its default.
    private static unsafe string WithArguments(string body, string call)
    {
        Node* expression = (Node*)NativeMethods.stringToNode(Worf.PStrDup(call));
        List* args = expression->type == NodeTag.T_FuncExpr ? ((FuncExpr*)expression)->args : ((OpExpr*)expression)->args;
        List<string> given = [];
        for (int i = 0; args is not null && i < args->length; i++)
        {
            Node* arg = (Node*)args->elements[i].ptr_value;
            if (arg is null || arg->type == NodeTag.T_NamedArgExpr)
            {
                return body;
            }

            given.Add(NodeText(arg));
        }

        const string Param = "{PARAM :paramkind 0 :paramid ";
        StringBuilder result = new();
        int from = 0;
        for (int at = body.IndexOf(Param, StringComparison.Ordinal); at >= 0; at = body.IndexOf(Param, from, StringComparison.Ordinal))
        {
            int digits = at + Param.Length;
            int end = digits;
            while (end < body.Length && char.IsAsciiDigit(body[end]))
            {
                end++;
            }

            string node = NodeAt(body, at);
            _ = result.Append(body, from, at - from);
            _ = int.TryParse(body.AsSpan(digits, end - digits), NumberStyles.None, CultureInfo.InvariantCulture, out int id) && id >= 1 && id <= given.Count
                ? result.Append(given[id - 1]) : result.Append(node);
            from = at + node.Length;
        }

        return result.Append(body, from, body.Length - from).ToString();
    }

    // The text of a domain's CHECK constraints, and of its base domains', read once for the expressions being read, as
    // the type cache holds them, planned, with the SQL functions they call inlined as they were when the cache read
    // them: the type cache keeps them until the domain or its constraints change, though a function they inlined is
    // replaced, and checks values with them. A function the planner didn't inline runs its body as it is when called.
    private static string DomainChecks(uint domain, Dictionary<uint, string> read)
    {
        if (!read.TryGetValue(domain, out string? checks))
        {
            StringBuilder text = new();
            unsafe
            {
                // The reference holds the callback its memory context runs as it's reset, so it lives in that context.
                DomainConstraintRef* reference = (DomainConstraintRef*)NativeMethods.palloc((nuint)sizeof(DomainConstraintRef));
                NativeMethods.InitDomainConstraintRef(domain, reference, NativeGlobals.CurrentMemoryContext, false);
                for (int i = 0; reference->constraints is not null && i < reference->constraints->length; i++)
                {
                    DomainConstraintState* constraint = (DomainConstraintState*)reference->constraints->elements[i].ptr_value;
                    if (constraint->constrainttype == DomainConstraintType.DOM_CONSTRAINT_CHECK && constraint->check_expr is not null)
                    {
                        _ = text.Append(NodeText((Node*)constraint->check_expr)).Append(' ');
                    }
                }
            }

            checks = text.ToString();
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
        nint inUse;
        nint estate;
        WorfStartingStatement? starting = null;
        unsafe
        {
            plpgsql = InPlpgsql(out simple, out inUse, out estate);
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
        List<uint> switching = [];
        bool unnamed = false;
        try
        {
            Reload();
            if (Worf.InOwnSql)
            {
                return;
            }

            // A statement that holds no switch gates, as a SELECT of nextval doesn't, waits once, before it draws from a
            // managed sequence its expressions name, for the switch from a block that has reached its last twentieth, as
            // one that holds them does, so that statements that each draw a value don't run the block out between the
            // range worker's checks.
            // A PL/pgSQL expression starts once a transaction, as the function runs a statement of its own; a sequence whose
            // switch gate the statement holds waits as the gate is taken.
            uint snowflake = Worf.CatalogOid("snowflake_node");
            HashSet<uint>? named = s_sequences.Count == 0 ? null : plpgsql ? SimpleNamed(simple) : starting is null ? null : StatementNamed(starting);
            if (named is not null)
            {
                switching = [.. named.Where(seqoid => seqoid != snowflake && s_sequences.ContainsKey(seqoid) && !s_gates.ContainsKey(seqoid) &&
                    (plpgsql || starting!.Switched.Add(seqoid)))];
            }

            // The sequences whose grantors haven't answered, in the order of their OIDs, so that a statement that waits
            // for several waits for them, and names the first, in the same order however the grantors were read.
            List<uint> unconfirmed = [.. s_grantors.Where(entry => entry.Key != snowflake && !s_confirmedSequences.Contains(entry.Key)).Select(entry => entry.Key).Order()];
            if (unconfirmed.Count > 0)
            {
                // Within PL/pgSQL, a call starts as an expression of the function's starts, or C code starts it within an
                // expression that runs, or as PL/pgSQL coerces a value it assigns or returns with no expression, as a
                // variable it returns. Outside any statement, a native subscription's apply starts calls as it writes
                // its tables' rows.
                HashSet<uint>? called = !plpgsql ? (starting is not null ? StartingDraws(starting) : SubscriptionDraws())
                    : simple != 0 ? SimpleDraws(simple)
                    : inUse != 0 ? ExpressionDraws(inUse)
                    : [];

                // PL/pgSQL coerces the values it assigns its variables, and returns, to their types, after the
                // expression that gave them, which still counts as the one starting, so a domain's CHECK among those
                // types draws as though that expression named it.
                if (called is not null && plpgsql && PlpgsqlCasts(estate) is (HashSet<uint> castCalled, bool castUnnamed))
                {
                    called = castUnnamed ? null : [.. called, .. castCalled];
                }

                // A call worf can't tell the sequence of waits for every sequence whose grantor hasn't answered.
                unnamed = called is null;
                waiting = unnamed ? unconfirmed : [.. unconfirmed.Where(called!.Contains)];
            }
        }
        finally
        {
            s_namingGrantors = false;
        }

        foreach (uint seqoid in waiting)
        {
            WaitForGrantor(seqoid, unnamed);
        }

        foreach (uint seqoid in switching)
        {
            WaitWhileShort(seqoid);
        }

        if (switching.Count > 0)
        {
            DrawUngated(switching);
        }
    }

    /// <summary>
    /// Finds the sequences the CHECK constraints of this database's domains draw from, with the SQL functions they
    /// inline, which C code starts as it checks a value against a domain, once until a type, a constraint or a function
    /// changes, or the search path does, and whether one of them draws from a sequence it doesn't name.
    /// </summary>
    private static (HashSet<uint> Called, bool Unnamed) DomainDraws()
    {
        WatchTables();
        string path = SearchPath();
        if (s_domainDraws is not (HashSet<uint> Called, bool Unnamed, string Path) draws || draws.Path != path)
        {
            Dictionary<uint, string> read = [];
            string checks = string.Concat(Elevated.Run(() => Worf.Read($"""
                SELECT DISTINCT c.contypid FROM pg_catalog.pg_constraint c
                WHERE c.contypid <> 0 AND c.contype = 'c' AND c.conbin::pg_catalog.text ~ '{MayDrawPattern}'
                """, 0)).Select(row => DomainChecks(row.Get<uint>(0), read)));
            (HashSet<uint> called, _, bool unnamed) = Draws(WithDomainChecks(checks), unnamedDomains: false);
            draws = (called, unnamed, path);
            s_domainDraws = draws;
        }

        return (draws.Called, draws.Unnamed);
    }

    // The search path, as the session has it now.
    private static unsafe string SearchPath()
    {
        fixed (byte* name = "search_path\0"u8)
        {
            return Worf.FromServer(NativeMethods.GetConfigOption((sbyte*)name, false, false));
        }
    }

    // A pattern for the text of an expression that may call nextval: one that calls it, or calls a function that isn't
    // PostgreSQL's own, as a SQL function the planner inlines may.
    private const string MayDrawPattern = ":(op)?funcid (1574|[1-9][0-9]{4,}) ";

    /// <summary>
    /// Finds the sequences the nextval calls of the statement whose expressions start name, once for the statement, or
    /// null outside one, or for a call they don't name.
    /// </summary>
    private static HashSet<uint>? StartingDraws(WorfStartingStatement? starting)
        => starting is null || starting.Draws.Unnamed || starting.Looked > starting.Draws.Calls ? null : starting.Draws.Called;

    /// <summary>
    /// Finds the sequences the calls of nextval of the statement starting name, kept by the statement's text, the
    /// search path and the role it runs as, unless it has parameters, until a table, a type, a constraint, a function or
    /// a schema changes, or the managed sequences do, so that a statement run again doesn't read its plan's text again.
    /// </summary>
    private static HashSet<uint> StatementNamed(WorfStartingStatement starting)
    {
        if (starting.Text is not string text)
        {
            return starting.Draws.Called;
        }

        // The search path's "$user" stands for the role the statement runs as.
        WatchTables();
        uint user;
        unsafe
        {
            user = NativeMethods.GetUserId();
        }

        (string Text, string Path, uint User) key = (text, SearchPath(), user);
        if (!s_namedByText.TryGetValue(key, out HashSet<uint>? named))
        {
            named = starting.Draws.Called;
            if (s_namedByText.Count >= 1000)
            {
                s_namedByText.Clear();
            }

            s_namedByText[key] = named;
        }

        return named;
    }

    /// <summary>
    /// Finds the sequences the nextval calls of the PL/pgSQL expression starting name, whether or not they name every
    /// call's, once for the expression as it starts, or null for one PL/pgSQL doesn't start itself.
    /// </summary>
    private static unsafe HashSet<uint>? SimpleNamed(nint simple)
    {
        if (simple == 0)
        {
            return null;
        }

        s_simple.Draws ??= Draws(PlanText((Node*)((PLpgSQL_expr*)simple)->expr_simple_expr));
        return s_simple.Draws.Value.Called;
    }

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
    /// Checks, as PostgreSQL looks a function up to call it, whether the fmgr hook should watch it start: a trigger
    /// function, in PostgreSQL's apply or table sync of a subscription, which start no statement whose tables' triggers
    /// the guard reads, so that a trigger whose C code draws from a sequence it names among its arguments, as contrib's
    /// autoinc does, waits for the sequence's grantor first. worf's own apply and sync check each row's table as they
    /// write it, as <see cref="WritingRow"/> does.
    /// </summary>
    /// <param name="fnOid">The function.</param>
    /// <returns>Whether to watch it.</returns>
    internal static bool HooksTrigger(uint fnOid)
    {
        unsafe
        {
            if (NativeGlobals.MySubscription is null || NativeGlobals.SessionReplicationRole != SessionReplicationRoleReplica ||
                NativeMethods.get_func_rettype(fnOid) != TriggerTypeOid)
            {
                return false;
            }
        }

        _ = s_hookedTriggers.Add(fnOid);
        return true;
    }

    /// <summary>
    /// Waits, as worf's apply or sync writes a row, for the grantors of the managed sequences named among the arguments
    /// of the triggers of the row's table, and its partitions', that fire on its insert, or on its update, as a trigger
    /// whose C code draws from a sequence it names, as contrib's autoinc does, enabled ALWAYS, may draw from one.
    /// </summary>
    /// <param name="relid">The table.</param>
    /// <param name="update">Whether the row updates one, rather than being inserted.</param>
    internal static void WritingRow(uint relid, bool update)
    {
        bool quiet;
        unsafe
        {
            quiet = NativeGlobals.creating_extension || NativeGlobals.XactReadOnly;
        }

        if (quiet || s_namingGrantors || SequenceRanges.Recovering() || Worf.CatalogOid("sequence_range_block") == 0)
        {
            return;
        }

        List<uint> waiting = [];
        s_namingGrantors = true;
        try
        {
            Reload();
            if (s_grantors.Count == 0 || !Triggers(relid).Argued)
            {
                return;
            }

            uint snowflake = Worf.CatalogOid("snowflake_node");
            waiting = [.. (update ? ArguedBy(relid, TriggerTypeUpdate, TriggerTypeInsert) : ArguedBy(relid, TriggerTypeInsert))
                .Where(seqoid => seqoid != snowflake && s_grantors.ContainsKey(seqoid) && !s_confirmedSequences.Contains(seqoid)).Order()];
        }
        finally
        {
            s_namingGrantors = false;
        }

        foreach (uint seqoid in waiting)
        {
            WaitForGrantor(seqoid);
        }
    }

    /// <summary>
    /// Waits, as a trigger function the fmgr hook watches starts, for the grantors of the managed sequences named among
    /// the arguments of the triggers that call it and fire as the subscription applies: those enabled ALWAYS or REPLICA,
    /// of the subscription's tables and their partitions, as its C code may draw from them.
    /// </summary>
    /// <param name="fnOid">The function.</param>
    internal static void StartingTrigger(uint fnOid)
    {
        bool quiet;
        unsafe
        {
            quiet = NativeGlobals.creating_extension || NativeGlobals.XactReadOnly;
        }

        if (!s_hookedTriggers.Contains(fnOid) || quiet || s_namingGrantors || SequenceRanges.Recovering() || Worf.CatalogOid("sequence_range_block") == 0)
        {
            return;
        }

        List<uint> waiting = [];
        s_namingGrantors = true;
        try
        {
            Reload();
            if (Worf.InOwnSql || s_grantors.Count == 0)
            {
                return;
            }

            WatchTables();
            uint subscription;
            unsafe
            {
                subscription = NativeGlobals.MySubscription is null ? 0 : NativeGlobals.MySubscription->oid;
            }

            if (!s_triggerArguments.TryGetValue((fnOid, subscription), out List<uint>? named))
            {
                WorfRows arguments = Elevated.Run(() => Worf.Read("""
                    SELECT DISTINCT a.arg FROM pg_catalog.pg_trigger t
                        CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.string_to_array(pg_catalog.encode(t.tgargs, 'escape'), '\000')) a (arg)
                    WHERE t.tgfoid = $1 AND t.tgnargs > 0 AND a.arg <> '' AND NOT t.tgisinternal AND t.tgenabled IN ('A', 'R')
                        AND t.tgrelid IN (
                            SELECT r.srrelid FROM pg_catalog.pg_subscription_rel r WHERE r.srsubid = $2
                            UNION ALL
                            SELECT p.relid FROM pg_catalog.pg_subscription_rel r CROSS JOIN LATERAL pg_catalog.pg_partition_tree(r.srrelid) p
                            WHERE r.srsubid = $2)
                    """, 0, SpiParameter.Create(fnOid), SpiParameter.Create(subscription)));
                named = [.. arguments.Select(argument => ManagedNamed(argument.Get<string>(0))).OfType<HashSet<uint>>().SelectMany(each => each).Distinct()];
                s_triggerArguments[(fnOid, subscription)] = named;
            }

            uint snowflake = Worf.CatalogOid("snowflake_node");
            waiting = [.. named.Where(seqoid => seqoid != snowflake && s_grantors.ContainsKey(seqoid) && !s_confirmedSequences.Contains(seqoid)).Order()];
        }
        finally
        {
            s_namingGrantors = false;
        }

        foreach (uint seqoid in waiting)
        {
            WaitForGrantor(seqoid);
        }
    }

    /// <summary>
    /// Finds the sequences the calls of nextval of a PL/pgSQL expression that runs name, with those of the CHECKs of the
    /// domains its C code may check values against, or null for a call they don't name.
    /// </summary>
    private static unsafe HashSet<uint>? ExpressionDraws(nint expr)
    {
        (HashSet<uint> called, _, bool unnamed) = Draws(PlanText((Node*)((PLpgSQL_expr*)expr)->expr_simple_expr));
        return unnamed ? null : called;
    }

    /// <summary>
    /// Finds the sequences that the CHECK constraints, trigger conditions, defaults and column domains of the tables a
    /// native subscription writes draw from, in its apply or table sync, where calls of nextval start as it writes
    /// their rows, or null outside one, or where one of them doesn't name its sequence.
    /// </summary>
    private static unsafe HashSet<uint>? SubscriptionDraws()
    {
        Subscription* subscription = NativeGlobals.MySubscription;
        if (subscription is null)
        {
            return null;
        }

        uint oid = subscription->oid;
        WatchTables();
        if (!s_subscriptionDraws.TryGetValue(oid, out HashSet<uint>? draws))
        {
            long[] relids = [.. Elevated.Run(() => Worf.Read("SELECT srrelid FROM pg_catalog.pg_subscription_rel WHERE srsubid = $1", 0, SpiParameter.Create(oid)))
                .Select(row => (long)row.Get<uint>(0))];
            string text = WrittenChecks(relids) + " " + string.Concat(relids.Select(relid => CopyDefaultsText((uint)relid, null, true)));
            (HashSet<uint> called, _, bool unnamed) = Draws(text);
            draws = unnamed ? null : called;
            s_subscriptionDraws[oid] = draws;
        }

        return draws;
    }

    /// <summary>
    /// Checks whether a PL/pgSQL function runs within the statement whose expressions started last, or with none
    /// starting, as PostgreSQL's error context shows: PL/pgSQL calls the function it tells its instrumentation plugin of
    /// for each of its functions running, with the function's state. When PL/pgSQL is itself starting an expression of
    /// the function's, outside a plan, as it does a simple expression's once a transaction, it has set the function's
    /// parameters up for the expression, which isn't running yet.
    /// </summary>
    /// <param name="simple">The expression PL/pgSQL is starting, or zero.</param>
    /// <param name="inUse">The simple expression of the function's that runs, within which C code starts the call, or
    /// zero.</param>
    /// <param name="estate">The state of the PL/pgSQL function that runs, or zero.</param>
    /// <returns>Whether one runs.</returns>
    private static unsafe bool InPlpgsql(out nint simple, out nint inUse, out nint estate)
    {
        simple = 0;
        inUse = 0;
        estate = 0;
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

            estate = (nint)context->arg;
            ParamListInfoData* parameters = ((PLpgSQL_execstate*)context->arg)->paramLI;
            PLpgSQL_expr* expr = parameters is null ? null : (PLpgSQL_expr*)parameters->parserSetupArg;
            if (expr is not null && expr->expr_simple_expr is not null)
            {
                if (expr->expr_simple_in_use)
                {
                    inUse = (nint)expr;
                }
                else
                {
                    simple = (nint)expr;
                }
            }

            return true;
        }

        return false;
    }

    /// <summary>
    /// Finds the sequences the CHECK constraints draw from of the domains a PL/pgSQL function's variables and result
    /// are, or hold, which PL/pgSQL coerces values to as it assigns and returns them, once for the function until a type,
    /// a constraint or a function changes, and whether one of them draws from a sequence it doesn't name.
    /// </summary>
    /// <param name="estate">The function's state.</param>
    /// <returns>The sequences, or null when none of those domains' CHECKs draws.</returns>
    private static unsafe (HashSet<uint> Called, bool Unnamed)? PlpgsqlCasts(nint estate)
    {
        // A function's are kept for its compiled state, of which a polymorphic function has one for each set of argument
        // types, with its OID and the version of its catalog row, as a function compiled anew may take the place of an
        // old one's in memory; and a DO block's, which has no OID, and whose compiled state a later one's may take the
        // place of, aren't kept.
        PLpgSQL_execstate* state = (PLpgSQL_execstate*)estate;
        (nint Compiled, uint Function, uint Xmin)? function = state->func is null || state->func->fn_oid == 0 ? null
#if ANKUS_PG15 || ANKUS_PG16 || ANKUS_PG17
            : ((nint)state->func, state->func->fn_oid, state->func->fn_xmin);
#else
            : ((nint)state->func, state->func->fn_oid, state->func->cfunc.fn_xmin);
#endif
        WatchTables();
        if (function is (nint, uint, uint) key && s_plpgsqlCasts.TryGetValue(key, out (HashSet<uint> Called, bool Unnamed)? known))
        {
            return known;
        }

        // A variable, as a promise is, and a record, start as PL/pgSQL's rows do, with the type they're declared as.
        HashSet<long> types = [state->fn_rettype];
        for (int i = 0; i < state->ndatums; i++)
        {
            PLpgSQL_datum* datum = state->datums[i];
            if (datum is null || datum->dtype is not (PLpgSQL_datum_type.PLPGSQL_DTYPE_VAR or PLpgSQL_datum_type.PLPGSQL_DTYPE_PROMISE or
                PLpgSQL_datum_type.PLPGSQL_DTYPE_REC))
            {
                continue;
            }

            PLpgSQL_rec* declared = (PLpgSQL_rec*)datum;
            if (declared->datatype is not null)
            {
                _ = types.Add(declared->datatype->typoid);
            }

            if (datum->dtype == PLpgSQL_datum_type.PLPGSQL_DTYPE_REC)
            {
                _ = types.Add(declared->rectypeid);
            }
        }

        string checks = TypesDomainChecks([.. types.Where(type => type != 0)]);
        (HashSet<uint> Called, bool Unnamed)? casts = null;
        if (checks.Length > 0)
        {
            (HashSet<uint> called, _, bool unnamed) = Draws(WithDomainChecks(checks));
            casts = (called, unnamed);
        }

        if (function is (nint, uint, uint) kept)
        {
            s_plpgsqlCasts[kept] = casts;
        }

        return casts;
    }

    /// <summary>
    /// Reads, from expressions' text, as nodeToString writes them, the sequences their calls of nextval name, as each
    /// call's argument, a constant, names one, and how many calls there are, as a function or through an operator. A
    /// constant name given as text, which the call looks up as it runs, under the search path it runs with, names every
    /// managed sequence of that name in any schema, or the schema it gives. A call whose argument isn't a constant, as a
    /// name computed as it runs, makes the sequences they may draw from unnamed. Expressions that call a function that
    /// may check a value against a domain they don't name, whose CHECK's calls of nextval C code starts where a call they
    /// name was counted, name the sequences every domain's CHECK draws from too, or none when one of those doesn't name
    /// its sequence.
    /// </summary>
    /// <param name="text">The expressions' text.</param>
    /// <param name="unnamedDomains">Whether to read the draws of domains the expressions don't name.</param>
    private static (HashSet<uint> Called, int Calls, bool Unnamed) Draws(string text, bool unnamedDomains = true)
    {
        const string Named = "{CONST :consttype 2205 ";
        const string Looked = "{FUNCEXPR :funcid 1079 ";
        const string Name = "{CONST :consttype 25 ";
        HashSet<uint> called = [];
        bool unnamed = false;
        int calls = 0;
        foreach (int at in NextvalCall().Matches(text).Select(call => call.Index))
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

        // A sequence a SQL function's body names, read from its source, stands for every managed one of its name.
        for (int at = text.IndexOf(Relname, StringComparison.Ordinal); at >= 0; at = text.IndexOf(Relname, at + Relname.Length, StringComparison.Ordinal))
        {
            int digits = at + Relname.Length;
            int end = digits;
            while (end < text.Length && char.IsAsciiDigit(text[end]))
            {
                end++;
            }

            if (uint.TryParse(text.AsSpan(digits, end - digits), NumberStyles.None, CultureInfo.InvariantCulture, out uint named))
            {
                called.UnionWith(ManagedOfName(named));
            }
        }

        if (unnamedDomains && ChecksUnnamedDomains(text))
        {
            (HashSet<uint> domainCalled, bool domainUnnamed) = DomainDraws();
            called.UnionWith(domainCalled);
            unnamed |= domainUnnamed;
        }

        return (called, calls, unnamed);
    }

    // A call of nextval in an expression's text, as a function or through an operator.
    [GeneratedRegex(@"\{FUNCEXPR :funcid 1574 |\{OPEXPR :opno [0-9]+ :opfuncid 1574 ", RegexOptions.CultureInvariant)]
    private static partial Regex NextvalCall();

    // A function an expression's text calls, as a function, an operator, an aggregate or a window function.
    [GeneratedRegex(":(?:funcid|opfuncid|aggfnoid|winfnoid) ([0-9]+) ", RegexOptions.CultureInvariant)]
    private static partial Regex CalledFunction();

    // Whether an expression's text calls a function that may check a value against a domain the text doesn't name:
    // one of PostgreSQL's that takes the type by name, as pg_input_is_valid does, or one in a procedural language other
    // than SQL and PL/pgSQL, whose code converts values it makes as it likes. The statements a SQL function runs, when it
    // isn't inlined, are read as they start, and PL/pgSQL's own checks as it makes them; a library's C code, which may
    // do anything, worf doesn't see into, as it doesn't see C code that calls nextval itself.
    private static bool ChecksUnnamedDomains(string text)
    {
        // PostgreSQL's own functions, but for those that check a value against a type given as a value, check only the
        // domains a statement names, so a statement that calls none of the others, as most that draw call only nextval,
        // reads no catalog. A function of a user's is asked about once, until a function changes: it may check unnamed
        // domains as one written in a procedural language other than SQL and PL/pgSQL, whose code worf can't read, or as
        // an internal function whose source is one of PostgreSQL's that do.
        WatchTables();
        long[] functions = [.. CalledFunction().Matches(text).Select(call => long.Parse(call.Groups[1].ValueSpan, CultureInfo.InvariantCulture)).Distinct()
            .Where(function => function >= 16384 || s_inputCheckers.Contains(function))];
        if (functions.Any(s_inputCheckers.Contains) || functions.Any(function => s_checksUnnamed.TryGetValue(function, out bool checks) && checks))
        {
            return true;
        }

        long[] unread = [.. functions.Where(function => !s_checksUnnamed.ContainsKey(function))];
        if (unread.Length == 0)
        {
            return false;
        }

        WorfRows checking = Elevated.Run(() => Worf.Read("""
            SELECT f.oid, EXISTS (
                SELECT 1 FROM pg_catalog.pg_proc p
                WHERE p.oid = f.oid::pg_catalog.oid
                    AND (p.prolang NOT IN (12, 13, 14) AND p.pronamespace <> 'worf'::pg_catalog.regnamespace
                            AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_language l WHERE l.oid = p.prolang AND l.lanname = 'plpgsql')
                        OR p.prolang = 12 AND p.prosrc IN ('pg_input_is_valid', 'pg_input_error_info', 'record_in', 'record_recv', 'domain_in',
                            'domain_recv', 'array_in', 'array_recv', 'range_in', 'range_recv', 'multirange_in', 'multirange_recv')))
            FROM pg_catalog.unnest($1::pg_catalog.int8[]) f (oid)
            """, 0, SpiParameter.Create(unread)));
        bool any = false;
        foreach (WorfRow row in checking)
        {
            bool checks = row.Get<bool>(1);
            s_checksUnnamed[row.Get<long>(0)] = checks;
            any |= checks;
        }

        return any;
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

    // The managed sequences whose relation has the name a sequence's has, in any schema.
    private static HashSet<uint> ManagedOfName(uint sequence)
    {
        HashSet<uint> managed = [];
        unsafe
        {
            sbyte* name = NativeMethods.get_rel_name(sequence);
            string? relname = name is null ? null : Worf.FromServer(name);
            foreach (uint seqoid in s_sequences.Keys)
            {
                sbyte* found = NativeMethods.get_rel_name(seqoid);
                if (relname is not null && found is not null && Worf.FromServer(found) == relname)
                {
                    _ = managed.Add(seqoid);
                }
            }
        }

        return managed;
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
    /// Waits, before a statement draws from a sequence's active block, until the block's parent, when another node
    /// granted it, and every other node this node exchanges with directly that reports the sequence have checked this
    /// node's report of it since this server started: a node that was away while its parent's fence lifted without its
    /// report, as when <c>worf.activate_sequence_range</c> stopped waiting for it, holds values the lift gave out again,
    /// and a node restored from a backup may draw again values a neighbor heard it draw, its own blocks' too, as an
    /// owner's; the first message after from such a node asks it to reset, so it draws none of them. The statement lets its
    /// gates go and waits up to twice <c>worf.sequence_range_timeout</c>, as for the next block at a block's end, and
    /// then fails, naming the parent, as an error that ends the wait first, as <c>statement_timeout</c>'s, does.
    /// </summary>
    private static void WaitForGrantor(uint seqoid, bool unnamed = false)
    {
        Reload();
        if (!s_grantors.TryGetValue(seqoid, out uint grantor) || s_confirmedSequences.Contains(seqoid))
        {
            return;
        }

        {
            string sequence;
            unsafe
            {
                sequence = Worf.FromServer(NativeMethods.get_rel_name(seqoid));
            }

            // The node waited for, its name, and why it's waited for, which change as the nodes ahead of it answer.
            string parent = "";
            bool heardBy = false;
            string role = "";
            void Describe(uint waitFor)
            {
                parent = Elevated.Run(() => Worf.ReadScalar<string?>("""
                    SELECT coalesce((SELECT node_name::text FROM worf.node WHERE node_id = $1),
                        (SELECT max(node_name::text) FROM worf.sequence_range_node WHERE node_id = $1))
                    """, SpiParameter.Create(waitFor))) ?? waitFor.ToString(CultureInfo.InvariantCulture);
                heardBy = s_heardBy.Contains(seqoid);
                role = heardBy ? $"which may have heard of sequence {sequence}'s draws" : $"which granted sequence {sequence}'s block";
            }

            Describe(grantor);
            string waitingFor = $"waiting for node {parent}, {role}, to answer this server since it started" +
                (unnamed ? ", as worf can't tell which sequence the call of nextval draws from before it runs" : "");
            using PgErrorContext context = PgErrorContext.Push(Worf.Shown(waitingFor));
            List<uint> held = LetGates();
            try
            {
                long deadline = Worker.GetCurrentTimestamp() + (SequenceRanges.ExchangeTimeout * 2000L);
                long woken = 0;
                while (true)
                {
                    Refresh();
                    Reload();
                    if (!s_grantors.TryGetValue(seqoid, out uint current) || s_confirmedSequences.Contains(seqoid))
                    {
                        break;
                    }

                    if (current != grantor)
                    {
                        grantor = current;
                        Describe(grantor);
                        woken = 0;
                    }

                    // The wait makes the exchange with the node waited for due, each second while it lasts.
                    long now = Worker.GetCurrentTimestamp();
                    if (now >= deadline)
                    {
                        throw unnamed
                            ? new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                                $"nextval can't draw until node {parent}, {role}, answers this server, which started since",
                                detail: UnnamedGrantorUnheard,
                                hint: $"Make sure node {parent} is up and its exchanges with this node succeed, or name the sequence in the statement.")
                            : new PgException(PgSqlStates.ObjectNotInPrerequisiteState,
                                heardBy
                                    ? $"sequence {sequence} can't draw until node {parent}, which may have heard of its draws, answers this server, which started since"
                                    : $"sequence {sequence} can't draw from the block node {parent} granted it until node {parent} answers this server, which started since",
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
    private static bool Guarded(uint relid) => Triggers(relid).Guarded;

    /// <summary>
    /// Checks, from the system catalogs alone, as <see cref="Guarded"/> does, whether a table has the guard's triggers,
    /// and whether it has a trigger that takes arguments, which may name a sequence the trigger draws from.
    /// </summary>
    private static (bool Guarded, bool Argued) Triggers(uint relid)
    {
        WatchTables();
        (long, long) version = (SequenceRanges.Version.Value, SequenceRanges.LocalVersion);
        if (version != s_guardedAt)
        {
            s_guarded.Clear();
            s_guardedAt = version;
        }

        if (!s_guarded.TryGetValue(relid, out (bool Guarded, bool Argued) triggers))
        {
            WorfRows found = Elevated.Run(() => Worf.Read("""
                SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = $1 AND tgfoid = 'worf.sequence_range_guard'::pg_catalog.regproc),
                    EXISTS (SELECT 1 FROM pg_catalog.pg_trigger t
                        WHERE t.tgrelid IN (SELECT $1 UNION ALL SELECT p.relid FROM pg_catalog.pg_partition_tree($1) p)
                            AND NOT t.tgisinternal AND t.tgenabled <> 'D' AND t.tgnargs > 0)
                """, 1, SpiParameter.Create(relid)));
            triggers = (found[0].Get<bool>(0), found[0].Get<bool>(1));
            s_guarded[relid] = triggers;
        }

        return triggers;
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
    /// marks those of a partition it creates or attaches, though not those of a partition of a table it attaches; and
    /// those its triggers name among their arguments, as contrib's autoinc names the sequence its C code draws from
    /// with no call worf sees.
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
            sequences.AddRange(ArguedBy(relid, TriggerTypeInsert).Where(seqoid => !sequences.Contains(seqoid)));
            s_drawnBy[relid] = sequences;
        }

        return sequences;
    }

    /// <summary>
    /// Lists the managed sequences that the triggers of a table and its partitions, which fire on any of some events,
    /// name among their arguments, as contrib's autoinc names the sequence its C code draws from with no call worf
    /// sees: a trigger's arguments that name a managed sequence, which its code may look up under whatever search path
    /// the statement has, stand for every managed sequence of that name. An insert routed to a partition, or a row an
    /// update moves to one, fires the partition's triggers, and only a partition's insert triggers fire as an update moves
    /// a row to it: an update of a table that isn't partitioned fires no insert trigger.
    /// </summary>
    /// <param name="relid">The table.</param>
    /// <param name="events">The events, as the bits of <c>pg_trigger.tgtype</c>.</param>
    /// <param name="movedInto">The events of the table's partitions alone, as the insert a row an update moves fires.</param>
    private static List<uint> ArguedBy(uint relid, int events, int movedInto = 0)
    {
        WatchTables();
        if (!s_arguedBy.TryGetValue((relid, events, movedInto), out List<uint>? sequences))
        {
            WorfRows arguments = Elevated.Run(() => Worf.Read("""
                SELECT DISTINCT a.arg FROM pg_catalog.pg_trigger t
                    CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.string_to_array(pg_catalog.encode(t.tgargs, 'escape'), '\000')) a (arg)
                WHERE t.tgrelid IN (SELECT $1 UNION ALL SELECT p.relid FROM pg_catalog.pg_partition_tree($1) p)
                    AND NOT t.tgisinternal AND t.tgenabled <> 'D' AND t.tgnargs > 0 AND a.arg <> ''
                    AND ((t.tgtype & $2) <> 0 OR (t.tgrelid <> $1 AND (t.tgtype & $3) <> 0))
                """, 0, SpiParameter.Create(relid), SpiParameter.Create(events), SpiParameter.Create(movedInto)));
            sequences = [.. arguments.Select(argument => ManagedNamed(argument.Get<string>(0))).OfType<HashSet<uint>>().SelectMany(named => named).Distinct()];
            s_arguedBy[(relid, events, movedInto)] = sequences;
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
        // worf's own apply and sync workers write other nodes' rows, which the guard's triggers, which fire in every
        // session so that another apply's rows are checked, leave alone, as they did when the triggers fired only in
        // sessions that write their own. Another session with session_replication_role replica writes what it likes, as
        // before, but a value it drew from this node's active block, as PostgreSQL's apply of another subscription fills
        // an identity column, waits for the block's grantor to have answered this server since it started.
        if (Worker.MyWorfWorker >= 0 && Worker.MyWorker.WorkerType is WorfWorkerType.Apply or WorfWorkerType.Sync)
        {
            return false;
        }

        bool replica;
        unsafe
        {
            replica = NativeGlobals.SessionReplicationRole == SessionReplicationRoleReplica;
        }

        if (replica && !SequenceRanges.Restore)
        {
            Reload();
            if (s_sequences.TryGetValue(seqoid, out (bool Ascending, List<(long Lo, long Hi, string State)> Blocks) held) &&
                held.Blocks.Exists(block => block.State == "active" && value >= block.Lo && value <= block.Hi))
            {
                WaitForGrantor(seqoid);
            }

            return false;
        }

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

        if (!Admits(seqoid, value, inserted) && (!Restoring() || MayIssue(seqoid, value)))
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
    private static bool Admits(uint seqoid, long value, bool inserted = false)
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
                    // A value a new row takes from the block, which code whose call of nextval worf doesn't see may have
                    // drawn, as C code that calls it itself, or PostgreSQL's apply of another subscription filling an
                    // identity column, waits for the block's grantor to have answered this server since it started, as a
                    // statement that draws does. A value an update sets is the statement's own.
                    if (inserted)
                    {
                        WaitForGrantor(seqoid);
                    }

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
            SequenceRanges.RequestCheck(seqoid);
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
                SequenceRanges.RequestCheck(seqoid);
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
                SequenceRanges.RequestCheck(seqoid);
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
        s_confirmedSequences.Clear();
        s_heardBy.Clear();
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
                coalesce(
                    CASE WHEN b.from_node <> l.node_id AND (EXISTS (SELECT 1 FROM worf.node n WHERE n.node_id = b.from_node)
                            OR EXISTS (SELECT 1 FROM worf.sequence_range_link k WHERE k.node_id = b.from_node))
                        AND NOT EXISTS (SELECT 1 FROM worf.sequence_range_node c WHERE c.seqoid = r.seqoid AND c.node_id = b.from_node AND c.confirmed_start = $3)
                        THEN b.from_node END,
                    (SELECT c.node_id FROM worf.sequence_range_node c
                    WHERE c.seqoid = r.seqoid AND c.node_id <> l.node_id AND c.confirmed_start IS DISTINCT FROM $3
                        AND (EXISTS (SELECT 1 FROM worf.subscription s WHERE s.sub_origin = c.node_id AND s.sub_target = l.node_id)
                            OR EXISTS (SELECT 1 FROM worf.sequence_range_link k WHERE k.node_id = c.node_id))
                    ORDER BY c.node_name LIMIT 1)),
                b.from_node
            FROM worf.sequence_range r
                JOIN pg_catalog.pg_sequence s ON s.seqrelid = r.seqoid
                CROSS JOIN worf.local_node l
                LEFT JOIN worf.sequence_range_block b ON b.seqoid = r.seqoid AND b.kind IN ('n', 'i')
                    AND b.state IN ('next', 'active', 'retired', 'imported')
            """, SpiParameter.Create((decimal)sysid), SpiParameter.Create((long)timeline), SpiParameter.Create(SequenceRangeHeard.Started)));
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

                if (state == "active" && row.Get<uint?>(7) is uint waitFor)
                {
                    s_grantors[seqoid] = waitFor;
                    if (row.Get<uint?>(8) != waitFor)
                    {
                        _ = s_heardBy.Add(seqoid);
                    }
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

        // The sequences a statement's text names by name are the managed ones of that name, which change with them.
        long signature = s_sequences.Keys.Aggregate(17L, (hash, seqoid) => unchecked((hash * 31) + seqoid)) ^ s_sequences.Count;
        if (signature != s_managedSignature)
        {
            s_managedSignature = signature;
            s_namedByText.Clear();
        }

        s_version = version;
        s_localVersion = localVersion;
    }
}
